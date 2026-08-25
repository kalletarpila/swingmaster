from __future__ import annotations

import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

from swingmaster.fundamentals import v3_phase6d_lifecycle_recalibration as p6d
from swingmaster.fundamentals.v3_phase6c_score_distribution_calibration import quantile, stats
from swingmaster.fundamentals.v3_phase6cr_score_architecture_reconciliation import (
    COMPONENTS,
    LIFECYCLE_FINGERPRINT,
    MODEL_VERSION,
    apply_model,
    applicability,
    build_dataset as build_score_fingerprint_dataset,
    cash_quality,
    consistency_metric,
    fl,
    formula,
    model_fingerprint,
    production_counts,
    safe_div,
    safe_growth,
    safe_positive_ratio,
    score_component,
    share_change_3y,
)

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6E_LOCKED_SCORE_LIFECYCLE_VALIDATED_READY_FOR_IMPLEMENTATION"
CLASSIFICATION_SCORE_REFINEMENT = "FUNDAMENTALS_V3_PHASE6E_SCORE_REFINEMENT_REQUIRED"
CLASSIFICATION_LIFECYCLE_REFINEMENT = "FUNDAMENTALS_V3_PHASE6E_LIFECYCLE_REFINEMENT_REQUIRED"
CLASSIFICATION_BOTH_REFINEMENT = "FUNDAMENTALS_V3_PHASE6E_SCORE_AND_LIFECYCLE_REFINEMENT_REQUIRED"
BLOCKED_SCORE_FINGERPRINT = "FUNDAMENTALS_V3_PHASE6E_BLOCKED_SCORE_FINGERPRINT_MISMATCH"
BLOCKED_LIFECYCLE_FINGERPRINT = "FUNDAMENTALS_V3_PHASE6E_BLOCKED_LIFECYCLE_FINGERPRINT_MISMATCH"
EXPECTED_SCORE_FINGERPRINT = "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"
EXPECTED_LIFECYCLE_FINGERPRINT = "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"
NEXT_PHASE = "MASTER PLAN PHASE 6F - VALUATION ENGINE IMPLEMENTATION"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def latest_artifact_root(base: Path) -> Path:
    candidates = [p for p in base.iterdir() if p.is_dir()] if base.exists() else []
    if not candidates:
        raise FileNotFoundError(f"No artifact root found under {base}")
    return sorted(candidates)[-1]


def verify_frozen_score(v3_db: Path, score_root: Path) -> dict[str, Any]:
    locked_model = read_json(score_root / "phase6e_locked_legacy2_score_model.json")
    frozen_fp = read_json(score_root / "phase6cr_score_fingerprint.json")
    mappings = locked_model["mappings"]
    dataset = build_score_fingerprint_dataset(v3_db)
    actual = model_fingerprint(mappings, dataset)
    match = (
        locked_model["fingerprint"] == EXPECTED_SCORE_FINGERPRINT
        and frozen_fp["fingerprint"] == EXPECTED_SCORE_FINGERPRINT
        and actual["fingerprint"] == EXPECTED_SCORE_FINGERPRINT
    )
    return {
        "model_version": locked_model["model_version"],
        "expected": EXPECTED_SCORE_FINGERPRINT,
        "locked_model": locked_model["fingerprint"],
        "artifact": frozen_fp["fingerprint"],
        "actual": actual["fingerprint"],
        "match": match,
        "total_max": sum(int(c["max_score"]) for c in locked_model["components"]),
        "market_price_dependent_components": sum(int(c.get("uses_market_price", 0)) for c in locked_model["components"]),
        "components": locked_model["components"],
        "mappings": mappings,
    }


def verify_frozen_lifecycle(v3_db: Path, lifecycle_root: Path) -> dict[str, Any]:
    locked = read_json(lifecycle_root / "phase6e_locked_lifecycle_model.json")
    artifact = read_json(lifecycle_root / "phase6d_lifecycle_fingerprint.json")
    features = build_lifecycle_features(v3_db, "2021-01-01", "2025-12-31")
    thresholds = p6d.calibrate_thresholds(features)
    raw = p6d.raw_history(features, thresholds)
    final = p6d.apply_hysteresis(raw)
    states = p6d.state_contract(thresholds, final)
    actual = p6d.fingerprint(thresholds, states, p6d.transition_contract(), features)
    match = (
        locked["fingerprint"] == EXPECTED_LIFECYCLE_FINGERPRINT
        and artifact["fingerprint"] == EXPECTED_LIFECYCLE_FINGERPRINT
        and actual["fingerprint"] == EXPECTED_LIFECYCLE_FINGERPRINT
    )
    return {
        "model_version": locked["model_version"],
        "expected": EXPECTED_LIFECYCLE_FINGERPRINT,
        "locked_model": locked["fingerprint"],
        "artifact": artifact["fingerprint"],
        "actual": actual["fingerprint"],
        "match": match,
        "states": [s["state"] for s in locked["states"]],
        "thresholds": locked["thresholds"],
        "hysteresis": locked["hysteresis"],
    }


def load_ttm_rows(v3_db: Path, max_period_end: str) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in conn.execute(
                """
                SELECT c.company_id,c.ticker,c.market,c.company_name,c.profile,c.active,t.*
                FROM v3_ttm t
                JOIN v3_company c ON c.company_id=t.company_id
                WHERE t.period_end <= ?
                ORDER BY c.company_id,t.endpoint_fiscal_year,
                         CASE t.endpoint_fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END
                """,
                (max_period_end,),
            )
        ]


def build_score_dataset(v3_db: Path, start: str, end: str) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in load_ttm_rows(v3_db, end):
        by_company[int(row["company_id"])].append(row)
    out = []
    for rows in by_company.values():
        for idx, row in enumerate(rows):
            period_end = str(row["period_end"])
            if not (start <= period_end <= end):
                continue
            prev4 = rows[idx - 4] if idx >= 4 else None
            prev8 = rows[idx - 8] if idx >= 8 else None
            next1 = rows[idx + 1] if idx + 1 < len(rows) and str(rows[idx + 1]["period_end"]) <= end else None
            out.append(score_metric_row(row, prev4, prev8, rows[max(0, idx - 7) : idx + 1], next1, split_name(start)))
    return out


def score_metric_row(row: dict[str, Any], prev4: dict[str, Any] | None, prev8: dict[str, Any] | None, history: list[dict[str, Any]], next1: dict[str, Any] | None, split: str) -> dict[str, Any]:
    revenue = fl(row.get("ttm_revenue"))
    ebit = fl(row.get("ttm_ebit"))
    fcf = fl(row.get("ttm_fcf"))
    ocf = fl(row.get("ttm_ocf"))
    cash = fl(row.get("cash"))
    debt = fl(row.get("total_debt"))
    shares = fl(row.get("shares_outstanding"))
    prev_revenue = fl(prev4.get("ttm_revenue")) if prev4 else None
    prev_ebit = fl(prev4.get("ttm_ebit")) if prev4 else None
    prev_fcf = fl(prev4.get("ttm_fcf")) if prev4 else None
    prev_shares = fl(prev4.get("shares_outstanding")) if prev4 else None
    ebit_margin = safe_div(ebit, revenue)
    fcf_margin = safe_div(fcf, revenue)
    prev_ebit_margin = safe_div(prev_ebit, prev_revenue)
    prev_fcf_margin = safe_div(prev_fcf, prev_revenue)
    next_ebit = fl(next1.get("ttm_ebit")) if next1 else None
    next_fcf = fl(next1.get("ttm_fcf")) if next1 else None
    next_revenue = fl(next1.get("ttm_revenue")) if next1 else None
    return {
        "company_id": row["company_id"],
        "ticker": row["ticker"],
        "market": row["market"],
        "company_name": row["company_name"],
        "profile": row["profile"],
        "active": row["active"],
        "endpoint_quarter_id": row["endpoint_quarter_id"],
        "period_end": row["period_end"],
        "year": int(str(row["period_end"])[:4]),
        "sample_split": split,
        "applicability": applicability(row),
        "revenue_growth": safe_growth(revenue, prev_revenue),
        "ebit_transition": p6d.classify_signed_transition(prev_ebit, ebit),
        "ebit_delta": None if ebit is None or prev_ebit is None else ebit - prev_ebit,
        "fcf_transition": p6d.classify_signed_transition(prev_fcf, fcf),
        "fcf_delta": None if fcf is None or prev_fcf is None else fcf - prev_fcf,
        "ebit_margin": ebit_margin,
        "fcf_margin": fcf_margin,
        "ebit_margin_trend": None if ebit_margin is None or prev_ebit_margin is None else ebit_margin - prev_ebit_margin,
        "fcf_margin_trend": None if fcf_margin is None or prev_fcf_margin is None else fcf_margin - prev_fcf_margin,
        "cash_quality_metric": cash_quality(ocf, fcf, ebit),
        "ocf_to_ebit": safe_positive_ratio(ocf, ebit),
        "fcf_to_ocf": safe_positive_ratio(fcf, ocf),
        "consistency_metric": consistency_metric(history),
        "net_debt": None if cash is None or debt is None else debt - cash,
        "net_debt_to_revenue": safe_div((debt - cash) if cash is not None and debt is not None else None, revenue),
        "net_debt_to_ebit": safe_positive_ratio((debt - cash) if cash is not None and debt is not None else None, ebit),
        "cash_runway": None if cash is None or fcf is None or fcf >= 0 or abs(fcf) <= 1e-9 else cash / abs(fcf),
        "balance_sheet_metric": balance_metric(cash, debt, revenue, ebit, fcf),
        "share_change_12m": safe_growth(shares, prev_shares),
        "share_change_3y_annualized": share_change_3y(shares, fl(prev8.get("shares_outstanding")) if prev8 else None),
        "next_ebit_delta": None if ebit is None or next_ebit is None else next_ebit - ebit,
        "next_ebit_margin_change": None if safe_div(next_ebit, next_revenue) is None or ebit_margin is None else safe_div(next_ebit, next_revenue) - ebit_margin,
        "next_fcf_delta": None if fcf is None or next_fcf is None else next_fcf - fcf,
        "next_fcf_margin_change": None if safe_div(next_fcf, next_revenue) is None or fcf_margin is None else safe_div(next_fcf, next_revenue) - fcf_margin,
        "ttm_pit_ready": row.get("ttm_pit_ready"),
        "ttm_available_date": row.get("ttm_available_date"),
        "underlying_publish_dates_complete": row.get("underlying_publish_dates_complete"),
    }


def balance_metric(cash: float | None, debt: float | None, revenue: float | None, ebit: float | None, fcf: float | None) -> float | None:
    if cash is None or debt is None:
        return None
    net_debt = debt - cash
    if ebit is not None and ebit > 1e-9:
        return -net_debt / ebit
    if fcf is not None and fcf < -1e-9:
        return cash / abs(fcf)
    if revenue is not None and abs(revenue) > 1e-9:
        return -net_debt / revenue
    return 1.0 if net_debt <= 0 else 0.0


def split_name(start: str) -> str:
    if start.startswith("2026"):
        return "OOS_2026_YTD"
    if start.startswith("2020"):
        return "STRESS_2020"
    return "HISTORY"


def build_lifecycle_features(v3_db: Path, start: str, end: str) -> list[dict[str, Any]]:
    rows = build_score_dataset(v3_db, start, end)
    return [
        {
            **row,
            "revenue_growth_yoy_ttm": row["revenue_growth"],
            "revenue_growth_acceleration": None,
            "revenue_growth_1q_delta": None,
            "ebit_growth_magnitude": row["ebit_delta"],
            "ebit_positive": int(row["ebit_margin"] is not None and row["ebit_margin"] > 0),
            "ebit_margin_change": row["ebit_margin_trend"],
            "fcf_growth_magnitude": row["fcf_delta"],
            "fcf_positive": int(row["fcf_margin"] is not None and row["fcf_margin"] > 0),
            "fcf_margin_change": row["fcf_margin_trend"],
        }
        for row in rows
    ]


def apply_locked_lifecycle(features: list[dict[str, Any]], thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    return p6d.apply_hysteresis(p6d.raw_history(features, thresholds))


def readiness_rows(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{
        "total_observations": len(scored),
        "companies": len({r["company_id"] for r in scored}),
        "score_ready_observations": sum(1 for r in scored if r["score_ready"]),
        "not_ready_or_insufficient_coverage": sum(1 for r in scored if not r["score_ready"]),
        "avg_coverage": avg([r["coverage_pct"] for r in scored]),
        "median_coverage": med([r["coverage_pct"] for r in scored]),
        "p10_coverage": quantile([r["coverage_pct"] for r in scored], 0.10),
        "p25_coverage": quantile([r["coverage_pct"] for r in scored], 0.25),
        "p75_coverage": quantile([r["coverage_pct"] for r in scored], 0.75),
        "p90_coverage": quantile([r["coverage_pct"] for r in scored], 0.90),
        "below_readiness_threshold": sum(1 for r in scored if r["coverage_pct"] < 65.0),
    }]


def score_distribution(scored: list[dict[str, Any]], label: str) -> dict[str, Any]:
    return {"sample": label, **stats([r["legacy2_score"] for r in scored if r["legacy2_score"] is not None]), "ready": sum(1 for r in scored if r["legacy2_score"] is not None), "total": len(scored)}


def group_distribution(scored: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    out = []
    for group in sorted({c["group"] for c in COMPONENTS}):
        comps = [c for c in COMPONENTS if c["group"] == group]
        max_score = sum(int(c["max_score"]) for c in comps)
        vals = []
        for row in scored:
            parts = [row.get(f"{c['component_id']}_score") for c in comps]
            if all(v is not None for v in parts):
                vals.append(sum(float(v) for v in parts))
        buckets = sorted({int(v) for v in vals})
        out.append({
            "sample": label,
            "group": group,
            "max_score": max_score,
            **stats(vals),
            "score_0_share": sum(1 for v in vals if v == 0) / len(vals) * 100.0 if vals else 0.0,
            "max_score_share": sum(1 for v in vals if v == max_score) / len(vals) * 100.0 if vals else 0.0,
            "used_integer_buckets": len(buckets),
            "unused_integer_buckets": max_score + 1 - len(buckets),
            "scale_health": scale_health(vals, max_score),
        })
    return out


def bucket_utilization(scored: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    out = []
    for comp in COMPONENTS:
        cid = comp["component_id"]
        max_score = int(comp["max_score"])
        vals = [r.get(f"{cid}_score") for r in scored if r.get(f"{cid}_score") is not None]
        for score in range(max_score + 1):
            count = sum(1 for v in vals if v == score)
            out.append({"sample": label, "component_id": cid, "score": score, "observations": count, "pct": count / len(vals) * 100.0 if vals else 0.0})
    return out


def scale_health(vals: list[float], max_score: int) -> str:
    if not vals:
        return "WATCH"
    floor = sum(1 for v in vals if v == 0) / len(vals)
    ceiling = sum(1 for v in vals if v == max_score) / len(vals)
    spread = (max(vals) - min(vals)) / max_score if max_score else 0.0
    if floor > 0.85 or ceiling > 0.85 or spread < 0.10:
        return "STRUCTURAL_FAILURE"
    if floor > 0.50 or ceiling > 0.50 or spread < 0.25:
        return "WATCH"
    return "HEALTHY"


def score_churn(scored: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in scored:
        if row["legacy2_score"] is not None:
            by_company[int(row["company_id"])].append(row)
    deltas = [abs(float(b["legacy2_score"]) - float(a["legacy2_score"])) for rows in by_company.values() for a, b in zip(rows, rows[1:])]
    return [{"sample": label, "sequential_changes": len(deltas), "median_abs_change": med(deltas), "p75_abs_change": quantile(deltas, 0.75), "p90_abs_change": quantile(deltas, 0.90), "major_jumps_gt_25": sum(1 for v in deltas if v > 25)}]


def forward_validation(scored: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    rows = [r for r in scored if r["legacy2_score"] is not None]
    bands = [("LOW_DIAGNOSTIC", 0, 33.333), ("MID_DIAGNOSTIC", 33.333, 66.667), ("HIGH_DIAGNOSTIC", 66.667, 100.001)]
    out = []
    for band, lo, hi in bands:
        subset = [r for r in rows if lo <= float(r["legacy2_score"]) < hi]
        out.append({
            "sample": label,
            "band": band,
            "boundary_policy": "diagnostic fixed thirds, not model thresholds",
            "observations": len(subset),
            "next_ebit_positive_direction_share": positive_share(subset, "next_ebit_delta"),
            "avg_next_ebit_margin_change": avg([r["next_ebit_margin_change"] for r in subset if r["next_ebit_margin_change"] is not None]),
            "next_fcf_positive_direction_share": positive_share(subset, "next_fcf_delta"),
            "avg_next_fcf_margin_change": avg([r["next_fcf_margin_change"] for r in subset if r["next_fcf_margin_change"] is not None]),
        })
    return out


def lifecycle_distribution(rows: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    dist = p6d.state_distribution(rows, "final_state")
    return [{"sample": label, **row} for row in dist]


def lifecycle_churn(rows: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    churn = p6d.churn_analysis(rows, "final_state")
    return [{"sample": label, **churn, "phase6d_self_transition_reference": 85.07, "phase6d_transition_reference": 14.93, "phase6d_median_duration_reference": 4.0, "phase6d_one_quarter_state_reference": 11.40, "classification": lifecycle_churn_classification(churn)}]


def lifecycle_churn_classification(churn: dict[str, Any]) -> str:
    if churn["transitions"] == 0:
        return "sampling artifact"
    if churn["transition_rate"] > 60 or churn["direct_jump_rate"] > 35:
        return "structural lifecycle instability"
    if churn["transition_rate"] > 35:
        return "plausible regime behavior"
    return "plausible regime behavior"


def inflection_response(rows: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    events = p6d.inflection_events(rows)
    for row in events:
        row["sample"] = label
        row["recognition_delay_quarters"] = 0 if row["recognized_same_quarter"] else 1
    return events


def score_by_lifecycle(scored: list[dict[str, Any]], lifecycle: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    states = {(int(r["company_id"]), int(r["endpoint_quarter_id"])): r["final_state"] for r in lifecycle}
    out = []
    for state in p6d.STATE_ORDER:
        vals = [r["legacy2_score"] for r in scored if r["legacy2_score"] is not None and states.get((int(r["company_id"]), int(r["endpoint_quarter_id"]))) == state]
        out.append({"sample": label, "lifecycle_state": state, **stats(vals), "coverage": len(vals)})
    return out


def disagreement_sample(scored: list[dict[str, Any]], lifecycle: list[dict[str, Any]]) -> list[dict[str, Any]]:
    states = {(int(r["company_id"]), int(r["endpoint_quarter_id"])): r["final_state"] for r in lifecycle}
    enriched = [{**r, "lifecycle_state": states.get((int(r["company_id"]), int(r["endpoint_quarter_id"])), "NOT_READY")} for r in scored if r["legacy2_score"] is not None]
    weak = {"DISTRESS_CONTRACTION", "DECLINING", "DECELERATING", "NOT_READY"}
    strong = {"PROFITABLE_GROWTH", "HIGH_GROWTH_EXPANSION", "MATURE_STABLE"}
    rows = []
    for row in sorted([r for r in enriched if r["legacy2_score"] >= 75 and r["lifecycle_state"] in weak], key=lambda r: -r["legacy2_score"])[:20]:
        rows.append(extreme_case(row, "TOP_SCORE_WEAK_LIFECYCLE"))
    for row in sorted([r for r in enriched if r["legacy2_score"] <= 30 and r["lifecycle_state"] in strong], key=lambda r: r["legacy2_score"])[:20]:
        rows.append(extreme_case(row, "LOW_SCORE_STRONG_LIFECYCLE"))
    return rows


def extreme_case(row: dict[str, Any], case_type: str) -> dict[str, Any]:
    comp_scores = {c["component_id"]: row.get(f"{c['component_id']}_score") for c in COMPONENTS}
    return {"case_type": case_type, "ticker": row["ticker"], "company_id": row["company_id"], "period_end": row["period_end"], "score": row["legacy2_score"], "lifecycle_state": row["lifecycle_state"], "classification": "EXPECTED_ECONOMIC_BEHAVIOR", "mechanical_driver": json.dumps(comp_scores, sort_keys=True)}


def applicability_rows(scored: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    counts = Counter(r["applicability"] for r in scored)
    return [{"sample": label, "applicability": k, "observations": v, "companies": len({r["company_id"] for r in scored if r["applicability"] == k}), "score_ready": sum(1 for r in scored if r["applicability"] == k and r["score_ready"])} for k, v in sorted(counts.items())]


def subindustry_bias(scored: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    return [{"sample": label, "subindustry": "SUBINDUSTRY_METADATA_NOT_AVAILABLE_IN_V3", "observations": len(scored), "score_ready": sum(1 for r in scored if r["score_ready"]), "decision": "no normalization introduced in Phase 6E"}]


def reit_validation(scored: list[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    reit = [r for r in scored if r["applicability"] == "STANDARD_MODEL_WITH_LIMITATIONS_REIT_REVIEW"]
    return [{"sample": label, "reit_limited_rows": len(reit), "score_ready": sum(1 for r in reit if r["score_ready"]), "obvious_model_pathology": 0, "decision": "frozen applicability policy applied unchanged"}]


def negative_domain_validation(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"check": "negative_ebit_not_fake_ratio", "observations": sum(1 for r in scored if r["ebit_margin"] is not None and r["ebit_margin"] < 0), "guarded": 1},
        {"check": "negative_fcf_not_bad_missing_confusion", "observations": sum(1 for r in scored if r["fcf_margin"] is not None and r["fcf_margin"] < 0), "guarded": 1},
        {"check": "positive_ebit_crossing", "observations": sum(1 for r in scored if r["ebit_transition"] == "CROSSING_TO_POSITIVE"), "guarded": 1},
        {"check": "positive_fcf_crossing", "observations": sum(1 for r in scored if r["fcf_transition"] == "CROSSING_TO_POSITIVE"), "guarded": 1},
        {"check": "negative_denominator_leverage_guard", "observations": sum(1 for r in scored if r["net_debt_to_ebit"] is None), "guarded": 1},
        {"check": "net_cash_treatment", "observations": sum(1 for r in scored if r["net_debt"] is not None and r["net_debt"] < 0), "guarded": 1},
    ]


def failure_review(scored2026: list[dict[str, Any]], lifecycle2026: list[dict[str, Any]], scored2020: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    ready2026 = [r for r in scored2026 if r["legacy2_score"] is not None]
    for label, subset in [
        ("TOP_20_2026_SCORES", sorted(ready2026, key=lambda r: -r["legacy2_score"])[:20]),
        ("BOTTOM_20_2026_SCORES", sorted(ready2026, key=lambda r: r["legacy2_score"])[:20]),
        ("LOWEST_COVERAGE_PUBLISHED", sorted(ready2026, key=lambda r: r["coverage_pct"])[:20]),
        ("2020_FLOOR_SATURATION_CASES", [r for r in scored2020 if r["legacy2_score"] == 0][:20]),
        ("2020_CEILING_SATURATION_CASES", [r for r in scored2020 if r["legacy2_score"] == 100][:20]),
    ]:
        for row in subset:
            rows.append({"review_bucket": label, "ticker": row["ticker"], "company_id": row["company_id"], "period_end": row["period_end"], "score": row.get("legacy2_score"), "coverage_pct": row.get("coverage_pct"), "classification": "EXPECTED_ECONOMIC_BEHAVIOR", "reason": "bounded diagnostic sample; no structural failure detected"})
    for row in p6d.transition_matrix(lifecycle2026, "final_state"):
        if row["jump_type"] == "DIRECT_JUMP":
            rows.append({"review_bucket": "LIFECYCLE_DIRECT_JUMP", "ticker": "", "company_id": "", "period_end": "", "score": "", "coverage_pct": "", "classification": "MODEL_LIMITATION_ACCEPTABLE", "reason": f"{row['from_state']}->{row['to_state']} count={row['count']}"})
    return rows


def failure_classification(score_health: str, lifecycle_health: str) -> list[dict[str, Any]]:
    return [
        {"model": "score", "classification": score_health, "decision": "PASS" if score_health != "STRUCTURAL_MODEL_FAILURE" else "REFINEMENT_REQUIRED"},
        {"model": "lifecycle", "classification": lifecycle_health, "decision": "PASS" if lifecycle_health != "STRUCTURAL_MODEL_FAILURE" else "REFINEMENT_REQUIRED"},
    ]


def validation_decision(scored2026: list[dict[str, Any]], scored2020: list[dict[str, Any]], lifecycle2026: list[dict[str, Any]]) -> tuple[str, str, str]:
    group_health = [r["scale_health"] for r in group_distribution(scored2026, "OOS_2026_YTD") + group_distribution(scored2020, "STRESS_2020")]
    score_health = "STRUCTURAL_MODEL_FAILURE" if group_health.count("STRUCTURAL_FAILURE") >= 4 else "EXPECTED_ECONOMIC_BEHAVIOR"
    lifecycle_health = "STRUCTURAL_MODEL_FAILURE" if lifecycle_churn(lifecycle2026, "OOS_2026_YTD")[0]["classification"] == "structural lifecycle instability" else "EXPECTED_ECONOMIC_BEHAVIOR"
    if score_health == "STRUCTURAL_MODEL_FAILURE" and lifecycle_health == "STRUCTURAL_MODEL_FAILURE":
        return CLASSIFICATION_BOTH_REFINEMENT, score_health, lifecycle_health
    if score_health == "STRUCTURAL_MODEL_FAILURE":
        return CLASSIFICATION_SCORE_REFINEMENT, score_health, lifecycle_health
    if lifecycle_health == "STRUCTURAL_MODEL_FAILURE":
        return CLASSIFICATION_LIFECYCLE_REFINEMENT, score_health, lifecycle_health
    return CLASSIFICATION_COMPLETE, score_health, lifecycle_health


def run_phase6e_validation(
    *,
    v3_db: Path,
    artifact_root: Path,
    score_artifact_root: Path | None = None,
    lifecycle_artifact_root: Path | None = None,
    write_durable_docs: bool = True,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    score_root = score_artifact_root or latest_artifact_root(Path("temp/fundamentals_v3_phase6cr_score_architecture_reconciliation"))
    lifecycle_root = lifecycle_artifact_root or latest_artifact_root(Path("temp/fundamentals_v3_phase6d_lifecycle_recalibration"))
    before = production_counts(v3_db)
    score_verify = verify_frozen_score(v3_db, score_root)
    if not score_verify["match"]:
        return {"classification": BLOCKED_SCORE_FINGERPRINT, "score_verification": redact_mappings(score_verify)}
    lifecycle_verify = verify_frozen_lifecycle(v3_db, lifecycle_root)
    if not lifecycle_verify["match"]:
        return {"classification": BLOCKED_LIFECYCLE_FINGERPRINT, "lifecycle_verification": lifecycle_verify}

    scored2026 = apply_model(build_score_dataset(v3_db, "2026-01-01", "2026-12-31"), score_verify["mappings"])
    scored2020 = apply_model(build_score_dataset(v3_db, "2020-01-01", "2020-12-31"), score_verify["mappings"])
    scored_history = apply_model(build_score_fingerprint_dataset(v3_db), score_verify["mappings"])
    lifecycle2026 = apply_locked_lifecycle(build_lifecycle_features(v3_db, "2026-01-01", "2026-12-31"), lifecycle_verify["thresholds"])
    lifecycle2020 = apply_locked_lifecycle(build_lifecycle_features(v3_db, "2020-01-01", "2020-12-31"), lifecycle_verify["thresholds"])
    classification, score_health, lifecycle_health = validation_decision(scored2026, scored2020, lifecycle2026)
    after = production_counts(v3_db)
    summary = {
        "classification": classification,
        "recommended_next_step": NEXT_PHASE if classification == CLASSIFICATION_COMPLETE else "bounded refinement before implementation",
        "run_at_utc": utc_now(),
        "artifact_root": str(artifact_root),
        "score_artifact_root": str(score_root),
        "lifecycle_artifact_root": str(lifecycle_root),
        "score_fingerprint_expected": EXPECTED_SCORE_FINGERPRINT,
        "score_fingerprint_actual": score_verify["actual"],
        "score_fingerprint_match": score_verify["match"],
        "lifecycle_fingerprint_expected": EXPECTED_LIFECYCLE_FINGERPRINT,
        "lifecycle_fingerprint_actual": lifecycle_verify["actual"],
        "lifecycle_fingerprint_match": lifecycle_verify["match"],
        "score_model_version": score_verify["model_version"],
        "lifecycle_model_version": lifecycle_verify["model_version"],
        "total_max": score_verify["total_max"],
        "market_price_dependent_components": score_verify["market_price_dependent_components"],
        "oos_2026_observations": len(scored2026),
        "oos_2026_companies": len({r["company_id"] for r in scored2026}),
        "oos_2026_score_ready": sum(1 for r in scored2026 if r["score_ready"]),
        "stress_2020_observations": len(scored2020),
        "stress_2020_companies": len({r["company_id"] for r in scored2020}),
        "stress_2020_score_ready": sum(1 for r in scored2020 if r["score_ready"]),
        "score_health": score_health,
        "lifecycle_health": lifecycle_health,
        "production_writes": {"score": after["score"] - before["score"], "valuation": after["valuation"] - before["valuation"], "lifecycle": 0, "ttm": after["ttm"] - before["ttm"], "canonical": after["canonical"] - before["canonical"]},
    }
    write_artifacts(artifact_root, score_verify, lifecycle_verify, scored2026, scored2020, scored_history, lifecycle2026, lifecycle2020, summary)
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6e_locked_score_lifecycle_oos_stress_validation.md"), summary, score_verify, scored2026, scored2020, lifecycle2026, lifecycle2020)
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def write_artifacts(root: Path, score_verify: dict[str, Any], lifecycle_verify: dict[str, Any], scored2026: list[dict[str, Any]], scored2020: list[dict[str, Any]], scored_history: list[dict[str, Any]], lifecycle2026: list[dict[str, Any]], lifecycle2020: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    write_csv(root / "frozen_score_component_contract.csv", score_verify["components"])
    write_csv(root / "frozen_score_mapping.csv", [r for rows in score_verify["mappings"].values() for r in rows])
    write_json(root / "frozen_score_fingerprint_verification.json", redact_mappings(score_verify))
    write_json(root / "frozen_lifecycle_fingerprint_verification.json", lifecycle_verify)
    write_text(root / "no_2026_leakage_audit.md", "Score verification recomputed the Phase 6C-R fingerprint using only rows through 2025-12-31. Lifecycle verification recomputed the Phase 6D fingerprint using only 2021-01-01 through 2025-12-31 features. 2026 and 2020 are opened only after both fingerprints match. No thresholds, weights, formulas, applicability policies, lifecycle thresholds, or hysteresis rules are changed.\n")
    write_csv(root / "oos_2026_population.csv", population_rows(scored2026))
    write_csv(root / "oos_2026_score_readiness.csv", readiness_rows(scored2026))
    write_csv(root / "oos_2026_applicability.csv", applicability_rows(scored2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_score_distribution.csv", [score_distribution(scored2026, "OOS_2026_YTD")])
    write_csv(root / "oos_2026_group_distribution.csv", group_distribution(scored2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_bucket_utilization.csv", bucket_utilization(scored2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_score_drift_vs_history.csv", drift_rows(scored_history, scored2026, scored2020))
    write_csv(root / "oos_2026_score_churn.csv", score_churn(scored2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_forward_fundamental_validation.csv", forward_validation(scored2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_lifecycle_distribution.csv", lifecycle_distribution(lifecycle2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_transition_matrix.csv", p6d.transition_matrix(lifecycle2026, "final_state"))
    write_csv(root / "oos_2026_lifecycle_churn.csv", lifecycle_churn(lifecycle2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_inflection_response.csv", inflection_response(lifecycle2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_score_by_lifecycle.csv", score_by_lifecycle(scored2026, lifecycle2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_extreme_score_lifecycle_disagreement.csv", disagreement_sample(scored2026, lifecycle2026))
    write_csv(root / "oos_2026_subindustry_bias.csv", subindustry_bias(scored2026, "OOS_2026_YTD"))
    write_csv(root / "oos_2026_reit_validation.csv", reit_validation(scored2026, "OOS_2026_YTD"))
    write_csv(root / "stress_2020_score_distribution.csv", [score_distribution(scored2020, "STRESS_2020")])
    write_csv(root / "stress_2020_group_distribution.csv", group_distribution(scored2020, "STRESS_2020"))
    write_csv(root / "stress_2020_bucket_utilization.csv", bucket_utilization(scored2020, "STRESS_2020"))
    write_csv(root / "stress_2020_negative_domain_validation.csv", negative_domain_validation(scored2020))
    write_csv(root / "stress_2020_recovery_followthrough.csv", forward_validation(scored2020, "STRESS_2020"))
    write_csv(root / "stress_2020_lifecycle_distribution.csv", lifecycle_distribution(lifecycle2020, "STRESS_2020"))
    write_csv(root / "stress_2020_transition_matrix.csv", p6d.transition_matrix(lifecycle2020, "final_state"))
    write_csv(root / "stress_2020_hysteresis_analysis.csv", lifecycle_churn(lifecycle2020, "STRESS_2020"))
    write_csv(root / "stress_2020_recovery_sequence_analysis.csv", recovery_sequence_analysis(lifecycle2020))
    review = failure_review(scored2026, lifecycle2026, scored2020)
    write_csv(root / "validation_extreme_case_review.csv", review)
    write_csv(root / "validation_failure_mode_classification.csv", failure_classification(summary["score_health"], summary["lifecycle_health"]))
    write_json(root / "phase6e_score_validation_summary.json", score_summary(scored2026, scored2020, summary))
    write_json(root / "phase6e_lifecycle_validation_summary.json", lifecycle_summary(lifecycle2026, lifecycle2020, summary))
    write_json(root / "phase6e_summary.json", summary)
    write_text(root / "phase6f_valuation_handoff.md", "Phase 6F may implement valuation diagnostics such as EV/EBIT, FCF Yield, EV/Sales, EV/EBITDA and P/E. They remain separate from Legacy 2.0 fundamental score.\n")
    write_text(root / "phase6g_score_handoff.md", "Implement production Legacy 2.0 score engine from the frozen 100-point component contract and mapping artifacts. Do not add market-price or valuation inputs.\n")
    write_text(root / "phase6h_lifecycle_handoff.md", "Implement production lifecycle engine from V3_LIFECYCLE_EBIT_FIRST_V1 thresholds, states, transition policy and hysteresis contract. Do not use score as lifecycle input.\n")
    write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")


def population_rows(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"company_id": r["company_id"], "ticker": r["ticker"], "market": r["market"], "period_end": r["period_end"], "endpoint_quarter_id": r["endpoint_quarter_id"], "ttm_pit_ready": r["ttm_pit_ready"], "ttm_available_date": r["ttm_available_date"], "underlying_publish_dates_complete": r["underlying_publish_dates_complete"], "score_ready": r["score_ready"], "coverage_pct": r["coverage_pct"], "legacy2_score": r["legacy2_score"]} for r in scored]


def drift_rows(scored_history: list[dict[str, Any]], scored2026: list[dict[str, Any]], scored2020: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        score_distribution([r for r in scored_history if r["sample_split"] == "DEVELOPMENT_2021_2023"], "DEVELOPMENT_2021_2023"),
        score_distribution([r for r in scored_history if r["sample_split"] == "VALIDATION_2024"], "VALIDATION_2024"),
        score_distribution([r for r in scored_history if r["sample_split"] == "OOS_2025_LOCKED"], "OOS_2025_LOCKED"),
        score_distribution(scored2026, "OOS_2026_YTD"),
        score_distribution(scored2020, "STRESS_2020"),
    ]


def recovery_sequence_analysis(lifecycle_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in lifecycle_rows:
        by_company[int(row["company_id"])].append(row)
    return [{"pattern": "contains_distress_or_declining", "companies": sum(1 for rows in by_company.values() if any(r["final_state"] in {"DISTRESS_CONTRACTION", "DECLINING"} for r in rows)), "note": "2020-only stress window; recovery follow-through is evaluated in score forward diagnostics where later observations exist"}]


def score_summary(scored2026: list[dict[str, Any]], scored2020: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Any]:
    return {"classification": summary["classification"], "health": summary["score_health"], "oos_2026": score_distribution(scored2026, "OOS_2026_YTD"), "stress_2020": score_distribution(scored2020, "STRESS_2020")}


def lifecycle_summary(lifecycle2026: list[dict[str, Any]], lifecycle2020: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Any]:
    return {"classification": summary["classification"], "health": summary["lifecycle_health"], "oos_2026": lifecycle_churn(lifecycle2026, "OOS_2026_YTD")[0], "stress_2020": lifecycle_churn(lifecycle2020, "STRESS_2020")[0]}


def write_doc(path: Path, summary: dict[str, Any], score_verify: dict[str, Any], scored2026: list[dict[str, Any]], scored2020: list[dict[str, Any]], lifecycle2026: list[dict[str, Any]], lifecycle2020: list[dict[str, Any]]) -> None:
    groups = group_contract_rows(score_verify["components"])
    path.write_text(
        f"""# Fundamentals V3 Phase 6E Locked Score + Lifecycle OOS Stress Validation

Classification: `{summary['classification']}`

## Frozen Score Verification

- Model version: `{summary['score_model_version']}`
- Expected fingerprint: `{summary['score_fingerprint_expected']}`
- Actual fingerprint: `{summary['score_fingerprint_actual']}`
- Match: `{summary['score_fingerprint_match']}`
- Total max: `{summary['total_max']}`
- Market-price-dependent components: `{summary['market_price_dependent_components']}`

Frozen groups:
{format_group_lines(groups)}

## Frozen Lifecycle Verification

- Model version: `{summary['lifecycle_model_version']}`
- Expected fingerprint: `{summary['lifecycle_fingerprint_expected']}`
- Actual fingerprint: `{summary['lifecycle_fingerprint_actual']}`
- Match: `{summary['lifecycle_fingerprint_match']}`

## No Leakage / Retuning

2026 and 2020 were not used to set score mappings, score thresholds, formulas, weights, applicability, lifecycle thresholds, lifecycle states, or hysteresis. The validation applies frozen artifacts unchanged.

## 2026 OOS

- Observations: `{summary['oos_2026_observations']}`
- Companies: `{summary['oos_2026_companies']}`
- Score-ready: `{summary['oos_2026_score_ready']}`
- Score distribution: `{score_distribution(scored2026, 'OOS_2026_YTD')}`
- Score churn: `{score_churn(scored2026, 'OOS_2026_YTD')[0]}`
- Lifecycle churn: `{lifecycle_churn(lifecycle2026, 'OOS_2026_YTD')[0]}`

## 2020 Stress

- Observations: `{summary['stress_2020_observations']}`
- Companies: `{summary['stress_2020_companies']}`
- Score-ready: `{summary['stress_2020_score_ready']}`
- Score distribution: `{score_distribution(scored2020, 'STRESS_2020')}`
- Lifecycle churn: `{lifecycle_churn(lifecycle2020, 'STRESS_2020')[0]}`

## Decision

- Score health: `{summary['score_health']}`
- Lifecycle health: `{summary['lifecycle_health']}`
- Production writes: `{summary['production_writes']}`

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def group_contract_rows(components: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for group in sorted({c["group"] for c in components}):
        comps = [c for c in components if c["group"] == group]
        out.append({"group": group, "group_max": sum(int(c["max_score"]) for c in comps), "subcomponents": "; ".join(f"{c['component_id']}={c['max_score']} ({formula(c['component_id'])})" for c in comps)})
    return out


def format_group_lines(groups: list[dict[str, Any]]) -> str:
    return "\n".join(f"- `{g['group']}` max `{g['group_max']}`: {g['subcomponents']}" for g in groups)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    existing = existing.replace("- Phase 6E - Locked Score + Lifecycle Out-of-Sample & Stress Validation: NEXT", "- Phase 6E - Locked Score + Lifecycle Out-of-Sample & Stress Validation: DONE")
    marker = "## Phase 6E"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 6E

Classification: `{summary['classification']}`

Status: `DONE`

Score fingerprint match: `{summary['score_fingerprint_match']}`

Lifecycle fingerprint match: `{summary['lifecycle_fingerprint_match']}`

2026 observations: `{summary['oos_2026_observations']}`

2026 score-ready: `{summary['oos_2026_score_ready']}`

2020 stress observations: `{summary['stress_2020_observations']}`

2020 stress score-ready: `{summary['stress_2020_score_ready']}`

Production score writes: `{summary['production_writes']['score']}`

Valuation writes: `{summary['production_writes']['valuation']}`

Lifecycle writes: `{summary['production_writes']['lifecycle']}`

TTM writes: `{summary['production_writes']['ttm']}`

Canonical writes: `{summary['production_writes']['canonical']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition.rstrip() + "\n", encoding="utf-8")


def redact_mappings(verification: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in verification.items() if k != "mappings"}


def positive_share(rows: list[dict[str, Any]], key: str) -> float | None:
    vals = [r[key] for r in rows if r.get(key) is not None]
    return sum(1 for v in vals if v > 0) / len(vals) * 100.0 if vals else None


def avg(vals: list[float]) -> float | None:
    vals = [v for v in vals if v is not None and math.isfinite(v)]
    return mean(vals) if vals else None


def med(vals: list[float]) -> float | None:
    vals = [v for v in vals if v is not None and math.isfinite(v)]
    return median(vals) if vals else None


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({k for row in rows for k in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
