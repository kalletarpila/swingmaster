from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

from swingmaster.episode_exit_features.production import FEATURE_COLUMNS
from swingmaster.regime.production import DEFAULT_REGIME_VERSION

MODEL_ID_CATBOOST = "FAIL10_BULL_CATBOOST_V1"
MODEL_ID_HGB = "FAIL10_BULL_HGB_V1"
COMPARISON_ID = "FAIL10_BULL_MODEL_COMPARISON_V1"


@dataclass(frozen=True)
class Fail10BullDataset:
    frame: pd.DataFrame
    feature_columns: tuple[str, ...]


@dataclass(frozen=True)
class Fail10BullMetrics:
    n_train: int
    n_valid: int
    n_test: int
    pos_rate_train: float
    pos_rate_valid: float
    pos_rate_test: float
    feature_count: int
    auc_valid_catboost: float
    auc_test_catboost: float
    auc_valid_hgb: float
    auc_test_hgb: float
    selected_production_candidate: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table_name: str, col: str) -> bool:
    if not _table_exists(conn, table_name):
        return False
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(r[1]) == col for r in rows)


def _required_schema_check(conn: sqlite3.Connection) -> None:
    required_tables = ["rc_pipeline_episode", "rc_episode_exit_features", "rc_episode_regime"]
    for table in required_tables:
        if not _table_exists(conn, table):
            raise RuntimeError(f"TABLE_MISSING_{table}")
    required_ep_cols = ["episode_id", "ticker", "entry_window_exit_date", "post60_growth_pct_close_ew_exit_to_peak"]
    for col in required_ep_cols:
        if not _column_exists(conn, "rc_pipeline_episode", col):
            raise RuntimeError(f"COLUMN_MISSING_rc_pipeline_episode_{col}")
    required_reg_cols = ["episode_id", "regime_version", "ew_exit_regime_combined"]
    for col in required_reg_cols:
        if not _column_exists(conn, "rc_episode_regime", col):
            raise RuntimeError(f"COLUMN_MISSING_rc_episode_regime_{col}")
    required_feat_cols = ["episode_id", "as_of_date", *FEATURE_COLUMNS]
    for col in required_feat_cols:
        if not _column_exists(conn, "rc_episode_exit_features", col):
            raise RuntimeError(f"COLUMN_MISSING_rc_episode_exit_features_{col}")


def _build_split_bucket(year: int) -> str | None:
    if 2020 <= year <= 2022:
        return "train"
    if year == 2023:
        return "valid"
    if 2024 <= year <= 2025:
        return "test"
    return None


def load_fail10_bull_dataset(conn: sqlite3.Connection, *, regime_version: str) -> Fail10BullDataset:
    _required_schema_check(conn)
    selected_cols = ", ".join(f"f.{col}" for col in FEATURE_COLUMNS)
    rows = conn.execute(
        f"""
        SELECT
          p.episode_id,
          p.ticker,
          p.entry_window_exit_date,
          p.post60_growth_pct_close_ew_exit_to_peak,
          {selected_cols}
        FROM rc_pipeline_episode p
        JOIN rc_episode_exit_features f
          ON f.episode_id = p.episode_id
         AND f.as_of_date = p.entry_window_exit_date
        JOIN rc_episode_regime r
          ON r.episode_id = p.episode_id
         AND r.regime_version = ?
        WHERE p.entry_window_exit_date IS NOT NULL
          AND r.ew_exit_regime_combined = 'BULL'
        """,
        (regime_version,),
    ).fetchall()
    cols = [
        "episode_id",
        "ticker",
        "entry_window_exit_date",
        "post60_growth_pct_close_ew_exit_to_peak",
        *FEATURE_COLUMNS,
    ]
    frame = pd.DataFrame.from_records(rows, columns=cols)
    if frame.empty:
        return Fail10BullDataset(frame=frame, feature_columns=tuple(FEATURE_COLUMNS))
    frame["entry_window_exit_date"] = pd.to_datetime(frame["entry_window_exit_date"], errors="coerce")
    frame = frame[frame["entry_window_exit_date"].notna()].copy()
    frame["exit_year"] = frame["entry_window_exit_date"].dt.year.astype(int)
    frame["split_bucket"] = frame["exit_year"].map(_build_split_bucket)
    frame = frame[frame["split_bucket"].notna()].copy()
    frame["label_fail10"] = (
        pd.to_numeric(frame["post60_growth_pct_close_ew_exit_to_peak"], errors="coerce") < 10.0
    ).astype(float)
    frame = frame[frame["post60_growth_pct_close_ew_exit_to_peak"].notna()].copy()
    for col in FEATURE_COLUMNS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.sort_values(["entry_window_exit_date", "ticker"], kind="stable").reset_index(drop=True)
    return Fail10BullDataset(frame=frame, feature_columns=tuple(FEATURE_COLUMNS))


def _split_counts(frame: pd.DataFrame, split: str) -> tuple[int, float]:
    part = frame[frame["split_bucket"] == split]
    n = len(part)
    if n == 0:
        return 0, 0.0
    y = part["label_fail10"].astype(int)
    return n, float(y.mean())


def _fit_catboost(x: pd.DataFrame, y: pd.Series) -> CatBoostClassifier:
    model = CatBoostClassifier(
        iterations=500,
        depth=5,
        learning_rate=0.05,
        loss_function="Logloss",
        eval_metric="AUC",
        random_seed=42,
        allow_writing_files=False,
        thread_count=1,
        verbose=False,
    )
    model.fit(x, y.astype(int))
    return model


def _fit_hgb(x: pd.DataFrame, y: pd.Series) -> HistGradientBoostingClassifier:
    model = HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_depth=3,
        max_iter=300,
        min_samples_leaf=20,
        random_state=42,
    )
    model.fit(x, y.astype(int))
    return model


def _auc(y_true: Sequence[int], y_score: Sequence[float]) -> float:
    return float(roc_auc_score(np.asarray(y_true, dtype=int), np.asarray(y_score, dtype=float)))


def select_preferred_candidate(
    *,
    auc_test_catboost: float,
    auc_test_hgb: float,
    auc_valid_catboost: float,
    auc_valid_hgb: float,
    tolerance: float = 1e-12,
) -> str:
    diff_test = auc_test_catboost - auc_test_hgb
    if abs(diff_test) > tolerance:
        return MODEL_ID_CATBOOST if diff_test > 0 else MODEL_ID_HGB
    diff_valid = auc_valid_catboost - auc_valid_hgb
    if abs(diff_valid) > tolerance:
        return MODEL_ID_CATBOOST if diff_valid > 0 else MODEL_ID_HGB
    return MODEL_ID_CATBOOST


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def train_and_compare_fail10_bull(
    conn: sqlite3.Connection,
    *,
    out_dir: Path,
    regime_version: str = DEFAULT_REGIME_VERSION,
    computed_at: str | None = None,
) -> Fail10BullMetrics:
    ds = load_fail10_bull_dataset(conn, regime_version=regime_version)
    frame = ds.frame
    if frame.empty:
        raise RuntimeError("FAIL10_BULL_DATASET_EMPTY")

    train_df = frame[frame["split_bucket"] == "train"].copy()
    valid_df = frame[frame["split_bucket"] == "valid"].copy()
    test_df = frame[frame["split_bucket"] == "test"].copy()

    if train_df.empty or valid_df.empty or test_df.empty:
        raise RuntimeError("FAIL10_BULL_SPLIT_EMPTY")

    x_train = train_df[list(ds.feature_columns)]
    y_train = train_df["label_fail10"].astype(int)
    x_valid = valid_df[list(ds.feature_columns)]
    y_valid = valid_df["label_fail10"].astype(int)
    x_test = test_df[list(ds.feature_columns)]
    y_test = test_df["label_fail10"].astype(int)

    cat_model = _fit_catboost(x_train, y_train)
    hgb_model = _fit_hgb(x_train, y_train)

    cat_valid = cat_model.predict_proba(x_valid)[:, 1]
    cat_test = cat_model.predict_proba(x_test)[:, 1]
    hgb_valid = hgb_model.predict_proba(x_valid)[:, 1]
    hgb_test = hgb_model.predict_proba(x_test)[:, 1]

    auc_valid_cat = _auc(y_valid.tolist(), cat_valid.tolist())
    auc_test_cat = _auc(y_test.tolist(), cat_test.tolist())
    auc_valid_hgb = _auc(y_valid.tolist(), hgb_valid.tolist())
    auc_test_hgb = _auc(y_test.tolist(), hgb_test.tolist())

    selected = select_preferred_candidate(
        auc_test_catboost=auc_test_cat,
        auc_test_hgb=auc_test_hgb,
        auc_valid_catboost=auc_valid_cat,
        auc_valid_hgb=auc_valid_hgb,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    ts = computed_at or _utc_now_iso()
    cat_path = out_dir / f"{MODEL_ID_CATBOOST}.cbm"
    hgb_path = out_dir / f"{MODEL_ID_HGB}.joblib"
    features_path = out_dir / "FAIL10_BULL_FEATURE_LIST_V1.json"
    cat_meta_path = out_dir / f"{MODEL_ID_CATBOOST}.meta.json"
    hgb_meta_path = out_dir / f"{MODEL_ID_HGB}.meta.json"
    comparison_path = out_dir / f"{COMPARISON_ID}.json"

    cat_model.save_model(str(cat_path))
    joblib.dump(hgb_model, str(hgb_path))

    _write_json(features_path, {"feature_columns": list(ds.feature_columns), "feature_count": len(ds.feature_columns)})
    _write_json(
        cat_meta_path,
        {
            "model_id": MODEL_ID_CATBOOST,
            "target": "FAIL10",
            "regime": "BULL",
            "regime_version": regime_version,
            "train_years": [2020, 2021, 2022],
            "valid_years": [2023],
            "test_years": [2024, 2025],
            "auc_valid": auc_valid_cat,
            "auc_test": auc_test_cat,
            "artifact_path": str(cat_path.resolve()),
            "trained_at_utc": ts,
        },
    )
    _write_json(
        hgb_meta_path,
        {
            "model_id": MODEL_ID_HGB,
            "target": "FAIL10",
            "regime": "BULL",
            "regime_version": regime_version,
            "train_years": [2020, 2021, 2022],
            "valid_years": [2023],
            "test_years": [2024, 2025],
            "auc_valid": auc_valid_hgb,
            "auc_test": auc_test_hgb,
            "artifact_path": str(hgb_path.resolve()),
            "trained_at_utc": ts,
        },
    )

    n_train, pos_rate_train = _split_counts(frame, "train")
    n_valid, pos_rate_valid = _split_counts(frame, "valid")
    n_test, pos_rate_test = _split_counts(frame, "test")

    _write_json(
        comparison_path,
        {
            "comparison_id": COMPARISON_ID,
            "target": "FAIL10",
            "regime": "BULL",
            "regime_version": regime_version,
            "n_train": n_train,
            "n_valid": n_valid,
            "n_test": n_test,
            "pos_rate_train": pos_rate_train,
            "pos_rate_valid": pos_rate_valid,
            "pos_rate_test": pos_rate_test,
            "feature_count": len(ds.feature_columns),
            "auc_valid_catboost": auc_valid_cat,
            "auc_test_catboost": auc_test_cat,
            "auc_valid_hgb": auc_valid_hgb,
            "auc_test_hgb": auc_test_hgb,
            "selected_production_candidate": selected,
            "trained_at_utc": ts,
            "catboost_meta_path": str(cat_meta_path.resolve()),
            "hgb_meta_path": str(hgb_meta_path.resolve()),
            "feature_list_path": str(features_path.resolve()),
        },
    )

    return Fail10BullMetrics(
        n_train=n_train,
        n_valid=n_valid,
        n_test=n_test,
        pos_rate_train=pos_rate_train,
        pos_rate_valid=pos_rate_valid,
        pos_rate_test=pos_rate_test,
        feature_count=len(ds.feature_columns),
        auc_valid_catboost=auc_valid_cat,
        auc_test_catboost=auc_test_cat,
        auc_valid_hgb=auc_valid_hgb,
        auc_test_hgb=auc_test_hgb,
        selected_production_candidate=selected,
    )

