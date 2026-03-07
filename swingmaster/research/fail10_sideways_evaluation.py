from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import roc_auc_score

from swingmaster.regime.production import DEFAULT_REGIME_VERSION
from swingmaster.research.fail10_sideways_training import (
    COMPARISON_ID,
    MODEL_ID_CATBOOST,
    MODEL_ID_HGB,
    load_fail10_sideways_dataset,
)

EVAL_ID = "FAIL10_SIDEWAYS_MODEL_EVAL_V1"
EVAL_FILE_NAME = f"{EVAL_ID}.json"
FEATURE_LIST_FILE_NAME = "FAIL10_SIDEWAYS_FEATURE_LIST_V1.json"

THRESHOLDS = (0.50, 0.60, 0.70)
TOP_BUCKETS: tuple[tuple[str, float], ...] = (("top_10pct", 0.10), ("top_20pct", 0.20))


@dataclass(frozen=True)
class Fail10SidewaysEvalSummary:
    n_valid: int
    n_test: int
    pos_rate_valid: float
    pos_rate_test: float
    auc_valid_catboost: float | None
    auc_test_catboost: float | None
    auc_valid_hgb: float | None
    auc_test_hgb: float | None
    precision_at_0_60_test_catboost: float | None
    lift_at_0_60_test_catboost: float | None
    top_10pct_fail10_rate_test_catboost: float | None
    avg_growth_top_10pct_test_catboost: float | None
    gt_50_rate_top_10pct_test_catboost: float | None
    precision_at_0_60_test_hgb: float | None
    lift_at_0_60_test_hgb: float | None
    top_10pct_fail10_rate_test_hgb: float | None
    avg_growth_top_10pct_test_hgb: float | None
    gt_50_rate_top_10pct_test_hgb: float | None
    previous_selected_production_candidate: str | None
    evaluation_recommended_candidate: str
    output_path: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_auc(y_true: np.ndarray, y_score: np.ndarray) -> float | None:
    if y_true.size == 0:
        return None
    if np.unique(y_true).size < 2:
        return None
    return float(roc_auc_score(y_true, y_score))


def _to_nullable(value: float | np.floating[Any] | None) -> float | None:
    if value is None:
        return None
    if np.isnan(value):
        return None
    return float(value)


def _threshold_label(threshold: float) -> str:
    return f"{threshold:.2f}"


def _compute_threshold_metrics(
    *,
    y: np.ndarray,
    score: np.ndarray,
    growth: np.ndarray,
    base_rate: float,
    thresholds: tuple[float, ...] = THRESHOLDS,
) -> dict[str, dict[str, float | int | None]]:
    out: dict[str, dict[str, float | int | None]] = {}
    n = int(y.size)
    for t in thresholds:
        key = _threshold_label(t)
        mask = score >= t
        selected_count = int(mask.sum())
        selection_rate = float(selected_count / n) if n > 0 else 0.0
        if selected_count == 0:
            out[key] = {
                "selected_count": 0,
                "selection_rate": selection_rate,
                "precision": None,
                "lift": None,
                "avg_growth_selected": None,
                "median_growth_selected": None,
            }
            continue
        y_selected = y[mask]
        g_selected = growth[mask]
        precision = float(np.mean(y_selected))
        lift = float(precision / base_rate) if base_rate > 0.0 else None
        out[key] = {
            "selected_count": selected_count,
            "selection_rate": selection_rate,
            "precision": precision,
            "lift": lift,
            "avg_growth_selected": _to_nullable(np.mean(g_selected)),
            "median_growth_selected": _to_nullable(np.median(g_selected)),
        }
    return out


def _compute_top_bucket_metrics(
    *,
    y: np.ndarray,
    score: np.ndarray,
    growth: np.ndarray,
    buckets: tuple[tuple[str, float], ...] = TOP_BUCKETS,
) -> dict[str, dict[str, float | int | None]]:
    n = int(y.size)
    if n == 0:
        return {
            name: {
                "selected_count": 0,
                "selection_rate": 0.0,
                "fail10_rate": None,
                "avg_growth_bucket": None,
                "median_growth_bucket": None,
                "gt_50_rate_bucket": None,
            }
            for name, _ in buckets
        }
    order = np.argsort(-score, kind="mergesort")
    out: dict[str, dict[str, float | int | None]] = {}
    for name, frac in buckets:
        k = int(np.ceil(n * frac))
        k = max(1, min(k, n))
        idx = order[:k]
        y_bucket = y[idx]
        g_bucket = growth[idx]
        out[name] = {
            "selected_count": k,
            "selection_rate": float(k / n),
            "fail10_rate": _to_nullable(np.mean(y_bucket)),
            "avg_growth_bucket": _to_nullable(np.mean(g_bucket)),
            "median_growth_bucket": _to_nullable(np.median(g_bucket)),
            "gt_50_rate_bucket": _to_nullable(np.mean(g_bucket > 50.0)),
        }
    return out


def _load_feature_list(model_dir: Path) -> list[str]:
    path = model_dir / FEATURE_LIST_FILE_NAME
    payload = json.loads(path.read_text(encoding="utf-8"))
    cols = payload.get("feature_columns")
    if not isinstance(cols, list) or not all(isinstance(x, str) for x in cols):
        raise RuntimeError("FAIL10_SIDEWAYS_FEATURE_LIST_INVALID")
    return cols


def _load_previous_selected_candidate(model_dir: Path) -> str | None:
    path = model_dir / f"{COMPARISON_ID}.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    value = payload.get("selected_production_candidate")
    if isinstance(value, str) and value:
        return value
    return None


def _recommend_candidate(
    *,
    previous_selected: str | None,
    auc_test_catboost: float | None,
    auc_test_hgb: float | None,
    top10_fail10_catboost: float | None,
    top10_fail10_hgb: float | None,
    avg_growth_top10_catboost: float | None,
    avg_growth_top10_hgb: float | None,
    gt50_top10_catboost: float | None,
    gt50_top10_hgb: float | None,
    tolerance: float = 1e-12,
) -> tuple[str, dict[str, str]]:
    def _cmp(a: float | None, b: float | None, *, lower_is_better: bool = False) -> int:
        if a is None and b is None:
            return 0
        if a is None:
            return -1 if not lower_is_better else 1
        if b is None:
            return 1 if not lower_is_better else -1
        d = a - b
        if abs(d) <= tolerance:
            return 0
        if lower_is_better:
            return 1 if d < 0 else -1
        return 1 if d > 0 else -1

    auc_cmp = _cmp(auc_test_catboost, auc_test_hgb)
    if auc_cmp > 0:
        baseline = MODEL_ID_CATBOOST
    elif auc_cmp < 0:
        baseline = MODEL_ID_HGB
    elif previous_selected in {MODEL_ID_CATBOOST, MODEL_ID_HGB}:
        baseline = previous_selected
    else:
        baseline = MODEL_ID_CATBOOST

    practical_order: list[tuple[str, float | None, float | None, bool]] = [
        ("top_10pct_fail10_rate_test", top10_fail10_catboost, top10_fail10_hgb, False),
        ("avg_growth_top_10pct_test", avg_growth_top10_catboost, avg_growth_top10_hgb, True),
        ("gt_50_rate_top_10pct_test", gt50_top10_catboost, gt50_top10_hgb, True),
    ]
    for metric_name, c_val, h_val, lower_is_better in practical_order:
        cmp_val = _cmp(c_val, h_val, lower_is_better=lower_is_better)
        if cmp_val > 0:
            return MODEL_ID_CATBOOST, {
                "baseline_from_auc_test": baseline,
                "practical_winner_metric": metric_name,
                "decision": "practical_overrides_auc",
            }
        if cmp_val < 0:
            return MODEL_ID_HGB, {
                "baseline_from_auc_test": baseline,
                "practical_winner_metric": metric_name,
                "decision": "practical_overrides_auc",
            }

    return baseline, {
        "baseline_from_auc_test": baseline,
        "practical_winner_metric": "none",
        "decision": "keep_auc_or_previous_due_to_tie",
    }


def _evaluate_single_model(
    *,
    y_valid: np.ndarray,
    y_test: np.ndarray,
    growth_valid: np.ndarray,
    growth_test: np.ndarray,
    score_valid: np.ndarray,
    score_test: np.ndarray,
) -> dict[str, Any]:
    pos_rate_valid = float(np.mean(y_valid)) if y_valid.size else 0.0
    pos_rate_test = float(np.mean(y_test)) if y_test.size else 0.0
    threshold_valid = _compute_threshold_metrics(
        y=y_valid,
        score=score_valid,
        growth=growth_valid,
        base_rate=pos_rate_valid,
    )
    threshold_test = _compute_threshold_metrics(
        y=y_test,
        score=score_test,
        growth=growth_test,
        base_rate=pos_rate_test,
    )
    top_valid = _compute_top_bucket_metrics(y=y_valid, score=score_valid, growth=growth_valid)
    top_test = _compute_top_bucket_metrics(y=y_test, score=score_test, growth=growth_test)
    return {
        "auc_valid": _safe_auc(y_valid, score_valid),
        "auc_test": _safe_auc(y_test, score_test),
        "threshold_metrics_valid": threshold_valid,
        "threshold_metrics_test": threshold_test,
        "top_bucket_metrics_valid": top_valid,
        "top_bucket_metrics_test": top_test,
        "return_metrics_valid": {
            "avg_growth_all": _to_nullable(np.mean(growth_valid) if growth_valid.size else np.nan),
            "median_growth_all": _to_nullable(np.median(growth_valid) if growth_valid.size else np.nan),
        },
        "return_metrics_test": {
            "avg_growth_all": _to_nullable(np.mean(growth_test) if growth_test.size else np.nan),
            "median_growth_all": _to_nullable(np.median(growth_test) if growth_test.size else np.nan),
        },
        "big_winner_valid": {
            "gt_50_rate_all": _to_nullable(np.mean(growth_valid > 50.0) if growth_valid.size else np.nan),
            "gt_50_rate_top_10pct": top_valid["top_10pct"]["gt_50_rate_bucket"],
            "gt_50_rate_top_20pct": top_valid["top_20pct"]["gt_50_rate_bucket"],
        },
        "big_winner_test": {
            "gt_50_rate_all": _to_nullable(np.mean(growth_test > 50.0) if growth_test.size else np.nan),
            "gt_50_rate_top_10pct": top_test["top_10pct"]["gt_50_rate_bucket"],
            "gt_50_rate_top_20pct": top_test["top_20pct"]["gt_50_rate_bucket"],
        },
    }


def _load_models(model_dir: Path) -> tuple[CatBoostClassifier, Any]:
    cat_path = model_dir / f"{MODEL_ID_CATBOOST}.cbm"
    hgb_path = model_dir / f"{MODEL_ID_HGB}.joblib"
    if not cat_path.exists():
        raise RuntimeError("FAIL10_SIDEWAYS_CATBOOST_ARTIFACT_MISSING")
    if not hgb_path.exists():
        raise RuntimeError("FAIL10_SIDEWAYS_HGB_ARTIFACT_MISSING")
    cat_model = CatBoostClassifier()
    cat_model.load_model(str(cat_path))
    hgb_model = joblib.load(str(hgb_path))
    return cat_model, hgb_model


def _build_eval_frame(
    conn: sqlite3.Connection,
    *,
    regime_version: str,
    feature_order_from_artifact: list[str],
) -> pd.DataFrame:
    ds = load_fail10_sideways_dataset(conn, regime_version=regime_version)
    if ds.frame.empty:
        raise RuntimeError("FAIL10_SIDEWAYS_DATASET_EMPTY")
    if tuple(feature_order_from_artifact) != ds.feature_columns:
        raise RuntimeError("FAIL10_SIDEWAYS_FEATURE_ORDER_MISMATCH")
    return ds.frame


def evaluate_fail10_sideways_models(
    conn: sqlite3.Connection,
    *,
    model_dir: Path,
    out_dir: Path | None = None,
    regime_version: str = DEFAULT_REGIME_VERSION,
    computed_at: str | None = None,
) -> Fail10SidewaysEvalSummary:
    feature_order = _load_feature_list(model_dir)
    frame = _build_eval_frame(conn, regime_version=regime_version, feature_order_from_artifact=feature_order)

    valid_df = frame[frame["split_bucket"] == "valid"].copy()
    test_df = frame[frame["split_bucket"] == "test"].copy()
    if valid_df.empty or test_df.empty:
        raise RuntimeError("FAIL10_SIDEWAYS_SPLIT_EMPTY")

    x_valid = valid_df[feature_order]
    x_test = test_df[feature_order]
    y_valid = valid_df["label_fail10"].astype(int).to_numpy()
    y_test = test_df["label_fail10"].astype(int).to_numpy()
    growth_valid = pd.to_numeric(
        valid_df["post60_growth_pct_close_ew_exit_to_peak"], errors="coerce"
    ).to_numpy(dtype=float)
    growth_test = pd.to_numeric(
        test_df["post60_growth_pct_close_ew_exit_to_peak"], errors="coerce"
    ).to_numpy(dtype=float)

    cat_model, hgb_model = _load_models(model_dir)
    cat_valid = np.asarray(cat_model.predict_proba(x_valid)[:, 1], dtype=float)
    cat_test = np.asarray(cat_model.predict_proba(x_test)[:, 1], dtype=float)
    hgb_valid = np.asarray(hgb_model.predict_proba(x_valid)[:, 1], dtype=float)
    hgb_test = np.asarray(hgb_model.predict_proba(x_test)[:, 1], dtype=float)

    cat_eval = _evaluate_single_model(
        y_valid=y_valid,
        y_test=y_test,
        growth_valid=growth_valid,
        growth_test=growth_test,
        score_valid=cat_valid,
        score_test=cat_test,
    )
    hgb_eval = _evaluate_single_model(
        y_valid=y_valid,
        y_test=y_test,
        growth_valid=growth_valid,
        growth_test=growth_test,
        score_valid=hgb_valid,
        score_test=hgb_test,
    )

    previous_selected = _load_previous_selected_candidate(model_dir)
    recommended, decision_meta = _recommend_candidate(
        previous_selected=previous_selected,
        auc_test_catboost=cat_eval["auc_test"],
        auc_test_hgb=hgb_eval["auc_test"],
        top10_fail10_catboost=cat_eval["top_bucket_metrics_test"]["top_10pct"]["fail10_rate"],
        top10_fail10_hgb=hgb_eval["top_bucket_metrics_test"]["top_10pct"]["fail10_rate"],
        avg_growth_top10_catboost=cat_eval["top_bucket_metrics_test"]["top_10pct"]["avg_growth_bucket"],
        avg_growth_top10_hgb=hgb_eval["top_bucket_metrics_test"]["top_10pct"]["avg_growth_bucket"],
        gt50_top10_catboost=cat_eval["big_winner_test"]["gt_50_rate_top_10pct"],
        gt50_top10_hgb=hgb_eval["big_winner_test"]["gt_50_rate_top_10pct"],
    )

    ts = computed_at or _utc_now_iso()
    dest_dir = out_dir if out_dir is not None else model_dir
    dest_dir.mkdir(parents=True, exist_ok=True)
    output_path = dest_dir / EVAL_FILE_NAME
    payload = {
        "evaluation_id": EVAL_ID,
        "regime": "SIDEWAYS",
        "target": "FAIL10",
        "regime_version": regime_version,
        "computed_at": ts,
        "model_dir": str(model_dir.resolve()),
        "dataset": {
            "n_valid": int(valid_df.shape[0]),
            "n_test": int(test_df.shape[0]),
            "pos_rate_valid": float(np.mean(y_valid)),
            "pos_rate_test": float(np.mean(y_test)),
            "feature_count": len(feature_order),
            "feature_columns": feature_order,
        },
        "models": {
            MODEL_ID_CATBOOST: cat_eval,
            MODEL_ID_HGB: hgb_eval,
        },
        "comparison_test_practical": {
            "auc_test": {
                MODEL_ID_CATBOOST: cat_eval["auc_test"],
                MODEL_ID_HGB: hgb_eval["auc_test"],
            },
            "precision_at_0_60_test": {
                MODEL_ID_CATBOOST: cat_eval["threshold_metrics_test"]["0.60"]["precision"],
                MODEL_ID_HGB: hgb_eval["threshold_metrics_test"]["0.60"]["precision"],
            },
            "lift_at_0_60_test": {
                MODEL_ID_CATBOOST: cat_eval["threshold_metrics_test"]["0.60"]["lift"],
                MODEL_ID_HGB: hgb_eval["threshold_metrics_test"]["0.60"]["lift"],
            },
            "top_10pct_fail10_rate_test": {
                MODEL_ID_CATBOOST: cat_eval["top_bucket_metrics_test"]["top_10pct"]["fail10_rate"],
                MODEL_ID_HGB: hgb_eval["top_bucket_metrics_test"]["top_10pct"]["fail10_rate"],
            },
            "avg_growth_top_10pct_test": {
                MODEL_ID_CATBOOST: cat_eval["top_bucket_metrics_test"]["top_10pct"]["avg_growth_bucket"],
                MODEL_ID_HGB: hgb_eval["top_bucket_metrics_test"]["top_10pct"]["avg_growth_bucket"],
            },
            "gt_50_rate_top_10pct_test": {
                MODEL_ID_CATBOOST: cat_eval["big_winner_test"]["gt_50_rate_top_10pct"],
                MODEL_ID_HGB: hgb_eval["big_winner_test"]["gt_50_rate_top_10pct"],
            },
        },
        "previous_selected_production_candidate": previous_selected,
        "evaluation_recommended_candidate": recommended,
        "decision_meta": decision_meta,
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return Fail10SidewaysEvalSummary(
        n_valid=int(valid_df.shape[0]),
        n_test=int(test_df.shape[0]),
        pos_rate_valid=float(np.mean(y_valid)),
        pos_rate_test=float(np.mean(y_test)),
        auc_valid_catboost=cat_eval["auc_valid"],
        auc_test_catboost=cat_eval["auc_test"],
        auc_valid_hgb=hgb_eval["auc_valid"],
        auc_test_hgb=hgb_eval["auc_test"],
        precision_at_0_60_test_catboost=cat_eval["threshold_metrics_test"]["0.60"]["precision"],
        lift_at_0_60_test_catboost=cat_eval["threshold_metrics_test"]["0.60"]["lift"],
        top_10pct_fail10_rate_test_catboost=cat_eval["top_bucket_metrics_test"]["top_10pct"]["fail10_rate"],
        avg_growth_top_10pct_test_catboost=cat_eval["top_bucket_metrics_test"]["top_10pct"]["avg_growth_bucket"],
        gt_50_rate_top_10pct_test_catboost=cat_eval["big_winner_test"]["gt_50_rate_top_10pct"],
        precision_at_0_60_test_hgb=hgb_eval["threshold_metrics_test"]["0.60"]["precision"],
        lift_at_0_60_test_hgb=hgb_eval["threshold_metrics_test"]["0.60"]["lift"],
        top_10pct_fail10_rate_test_hgb=hgb_eval["top_bucket_metrics_test"]["top_10pct"]["fail10_rate"],
        avg_growth_top_10pct_test_hgb=hgb_eval["top_bucket_metrics_test"]["top_10pct"]["avg_growth_bucket"],
        gt_50_rate_top_10pct_test_hgb=hgb_eval["big_winner_test"]["gt_50_rate_top_10pct"],
        previous_selected_production_candidate=previous_selected,
        evaluation_recommended_candidate=recommended,
        output_path=str(output_path.resolve()),
    )
