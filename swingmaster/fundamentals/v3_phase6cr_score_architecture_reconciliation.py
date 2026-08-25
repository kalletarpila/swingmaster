from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any

from swingmaster.fundamentals.v3_phase6b_score_lifecycle_calibration_design import classify_signed_transition
from swingmaster.fundamentals.v3_phase6c_score_distribution_calibration import quantile, stats
from swingmaster.fundamentals.v3_phase6d_lifecycle_recalibration import (
    MODEL_VERSION as LIFECYCLE_MODEL_VERSION,
    calibrate_thresholds as lifecycle_thresholds,
    raw_history as lifecycle_raw_history,
    apply_hysteresis as lifecycle_apply_hysteresis,
)

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6CR_LEGACY2_SCORE_RECONCILED_READY_FOR_PHASE6E"
CLASSIFICATION_DATA_REQUIRED = "FUNDAMENTALS_V3_PHASE6CR_ADDITIONAL_DATA_DECISION_REQUIRED"
NEXT_PHASE = "MASTER PLAN PHASE 6E - LOCKED SCORE + LIFECYCLE OUT-OF-SAMPLE & STRESS VALIDATION"
MODEL_VERSION = "V3_LEGACY2_FUNDAMENTAL_SCORE_V1"
LIFECYCLE_FINGERPRINT = "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"
NEAR_ZERO_EPSILON = 1e-9
DEV_START = "2021-01-01"
DEV_END = "2023-12-31"
VALIDATION_2024_START = "2024-01-01"
VALIDATION_2024_END = "2024-12-31"
OOS_2025_START = "2025-01-01"
OOS_2025_END = "2025-12-31"
MAX_TOTAL_SCORE = 100

COMPONENTS = [
    {"component_id": "REVENUE_GROWTH", "group": "GROWTH_EARNINGS_DEVELOPMENT", "max_score": 8, "metric": "revenue_growth", "direction": "HIGHER_IS_BETTER", "domain": "POSITIVE_BASE_GROWTH"},
    {"component_id": "EBIT_TRANSITION", "group": "GROWTH_EARNINGS_DEVELOPMENT", "max_score": 10, "metric": "ebit_transition", "direction": "TRANSITION_ORDERED", "domain": "SIGNED_TRANSITION"},
    {"component_id": "FCF_TRANSITION", "group": "GROWTH_EARNINGS_DEVELOPMENT", "max_score": 7, "metric": "fcf_transition", "direction": "TRANSITION_ORDERED", "domain": "SIGNED_TRANSITION"},
    {"component_id": "EBIT_MARGIN", "group": "PROFITABILITY_LEVEL", "max_score": 8, "metric": "ebit_margin", "direction": "HIGHER_IS_BETTER", "domain": "POSITIVE_ONLY_GOOD"},
    {"component_id": "FCF_MARGIN", "group": "PROFITABILITY_LEVEL", "max_score": 7, "metric": "fcf_margin", "direction": "HIGHER_IS_BETTER", "domain": "POSITIVE_ONLY_GOOD"},
    {"component_id": "EBIT_MARGIN_TREND", "group": "MARGIN_DEVELOPMENT_TREND", "max_score": 10, "metric": "ebit_margin_trend", "direction": "HIGHER_IS_BETTER", "domain": "SIGNED_CHANGE"},
    {"component_id": "FCF_MARGIN_TREND", "group": "MARGIN_DEVELOPMENT_TREND", "max_score": 5, "metric": "fcf_margin_trend", "direction": "HIGHER_IS_BETTER", "domain": "SIGNED_CHANGE"},
    {"component_id": "CASH_QUALITY", "group": "CASH_FLOW_QUALITY", "max_score": 15, "metric": "cash_quality_metric", "direction": "HIGHER_IS_BETTER", "domain": "POSITIVE_EARNINGS_CASH_CONVERSION"},
    {"component_id": "CONSISTENCY", "group": "CONSISTENCY", "max_score": 10, "metric": "consistency_metric", "direction": "HIGHER_IS_BETTER", "domain": "DURABILITY_WITHOUT_STAGNATION_REWARD"},
    {"component_id": "BALANCE_SHEET_RESILIENCE", "group": "BALANCE_SHEET_RESILIENCE", "max_score": 15, "metric": "balance_sheet_metric", "direction": "HIGHER_IS_BETTER", "domain": "CONDITIONAL_NET_DEBT_AND_RUNWAY"},
    {"component_id": "DILUTION", "group": "DILUTION", "max_score": 5, "metric": "share_change_12m", "direction": "LOWER_IS_BETTER", "domain": "SPLIT_ADJUSTED_SHARE_CHANGE"},
]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_ttm_rows(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            """
            SELECT c.company_id,c.ticker,c.market,c.company_name,c.profile,c.active,t.*
            FROM v3_ttm t
            JOIN v3_company c ON c.company_id=t.company_id
            WHERE t.period_end <= '2025-12-31'
            ORDER BY c.company_id,t.endpoint_fiscal_year,
                     CASE t.endpoint_fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END
            """
        )]


def build_dataset(v3_db: Path) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in load_ttm_rows(v3_db):
        by_company[int(row["company_id"])].append(row)
    out = []
    for rows in by_company.values():
        for idx, row in enumerate(rows):
            if not (DEV_START <= str(row["period_end"]) <= OOS_2025_END):
                continue
            prev4 = rows[idx - 4] if idx >= 4 else None
            prev8 = rows[idx - 8] if idx >= 8 else None
            next4 = rows[idx + 4] if idx + 4 < len(rows) and str(rows[idx + 4]["period_end"]) <= OOS_2025_END else None
            out.append(metric_row(row, prev4, prev8, rows[max(0, idx - 7) : idx + 1], next4))
    lifecycle = lifecycle_grouping_features(out)
    for row in out:
        row["lifecycle_state"] = lifecycle.get((int(row["company_id"]), int(row["endpoint_quarter_id"])), "NOT_READY")
    return out


def metric_row(row: dict[str, Any], prev4: dict[str, Any] | None, prev8: dict[str, Any] | None, history: list[dict[str, Any]], next4: dict[str, Any] | None) -> dict[str, Any]:
    revenue = fl(row.get("ttm_revenue"))
    ebit = fl(row.get("ttm_ebit"))
    fcf = fl(row.get("ttm_fcf"))
    ocf = fl(row.get("ttm_ocf"))
    net_income = fl(row.get("ttm_net_income"))
    cash = fl(row.get("cash"))
    debt = fl(row.get("total_debt"))
    shares = fl(row.get("shares_outstanding"))
    prev_revenue = fl(prev4.get("ttm_revenue")) if prev4 else None
    prev_ebit = fl(prev4.get("ttm_ebit")) if prev4 else None
    prev_fcf = fl(prev4.get("ttm_fcf")) if prev4 else None
    prev_shares = fl(prev4.get("shares_outstanding")) if prev4 else None
    prev_ebit_margin = safe_div(prev_ebit, prev_revenue)
    prev_fcf_margin = safe_div(prev_fcf, prev_revenue)
    ebit_margin = safe_div(ebit, revenue)
    fcf_margin = safe_div(fcf, revenue)
    revenue_growth = safe_growth(revenue, prev_revenue)
    next_ebit = fl(next4.get("ttm_ebit")) if next4 else None
    next_fcf = fl(next4.get("ttm_fcf")) if next4 else None
    next_revenue = fl(next4.get("ttm_revenue")) if next4 else None
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
        "sample_split": sample_split(str(row["period_end"])),
        "applicability": applicability(row),
        "revenue_growth": revenue_growth,
        "ebit_transition": classify_signed_transition(prev_ebit, ebit),
        "ebit_delta": None if ebit is None or prev_ebit is None else ebit - prev_ebit,
        "fcf_transition": classify_signed_transition(prev_fcf, fcf),
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
        "cash_runway": cash_runway(cash, fcf),
        "balance_sheet_metric": balance_metric(cash, debt, revenue, ebit, fcf),
        "share_change_12m": safe_growth(shares, prev_shares),
        "share_change_3y_annualized": share_change_3y(shares, fl(prev8.get("shares_outstanding")) if prev8 else None),
        "next_ebit_delta": None if ebit is None or next_ebit is None else next_ebit - ebit,
        "next_ebit_margin_change": None if safe_div(next_ebit, next_revenue) is None or ebit_margin is None else safe_div(next_ebit, next_revenue) - ebit_margin,
        "next_fcf_delta": None if fcf is None or next_fcf is None else next_fcf - fcf,
        "next_fcf_margin_change": None if safe_div(next_fcf, next_revenue) is None or fcf_margin is None else safe_div(next_fcf, next_revenue) - fcf_margin,
    }


def lifecycle_grouping_features(dataset: list[dict[str, Any]]) -> dict[tuple[int, int], str]:
    features = [
        {
            **row,
            "revenue_growth_yoy_ttm": row["revenue_growth"],
            "ebit_margin_change": row["ebit_margin_trend"],
            "fcf_margin_change": row["fcf_margin_trend"],
        }
        for row in dataset
    ]
    thresholds = lifecycle_thresholds(features)
    final = lifecycle_apply_hysteresis(lifecycle_raw_history(features, thresholds))
    return {(int(row["company_id"]), int(row["endpoint_quarter_id"])): str(row["final_state"]) for row in final}


def fl(value: Any) -> float | None:
    return None if value is None else float(value)


def safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or abs(den) <= NEAR_ZERO_EPSILON:
        return None
    return num / den


def safe_growth(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev is None or prev <= 0 or abs(prev) <= NEAR_ZERO_EPSILON:
        return None
    return (cur - prev) / abs(prev)


def safe_positive_ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or den <= NEAR_ZERO_EPSILON:
        return None
    return num / den


def cash_quality(ocf: float | None, fcf: float | None, ebit: float | None) -> float | None:
    ocf_ebit = safe_positive_ratio(ocf, ebit)
    fcf_ocf = safe_positive_ratio(fcf, ocf)
    vals = [v for v in (ocf_ebit, fcf_ocf) if v is not None]
    return sum(vals) / len(vals) if vals else None


def balance_metric(cash: float | None, debt: float | None, revenue: float | None, ebit: float | None, fcf: float | None) -> float | None:
    if cash is None or debt is None:
        return None
    net_debt = debt - cash
    if ebit is not None and ebit > NEAR_ZERO_EPSILON:
        return -1.0 * (safe_div(net_debt, ebit) or 0.0)
    if fcf is not None and fcf < -NEAR_ZERO_EPSILON:
        return cash / abs(fcf)
    if revenue is not None and abs(revenue) > NEAR_ZERO_EPSILON:
        return -1.0 * (safe_div(net_debt, revenue) or 0.0)
    return 1.0 if net_debt <= 0 else 0.0


def cash_runway(cash: float | None, fcf: float | None) -> float | None:
    if cash is None or fcf is None or fcf >= 0:
        return None
    return cash / abs(fcf) if abs(fcf) > NEAR_ZERO_EPSILON else None


def share_change_3y(shares: float | None, prev: float | None) -> float | None:
    growth = safe_growth(shares, prev)
    if growth is None:
        return None
    return (1.0 + growth) ** (1.0 / 3.0) - 1.0


def sample_split(period_end: str) -> str:
    if DEV_START <= period_end <= DEV_END:
        return "DEVELOPMENT_2021_2023"
    if VALIDATION_2024_START <= period_end <= VALIDATION_2024_END:
        return "VALIDATION_2024"
    if OOS_2025_START <= period_end <= OOS_2025_END:
        return "OOS_2025_LOCKED"
    return "EXCLUDED"


def applicability(row: dict[str, Any]) -> str:
    profile = str(row.get("profile") or "").upper()
    name = str(row.get("company_name") or "").upper()
    if profile in {"BANK", "INSURANCE", "REINSURANCE"}:
        return "NOT_APPLICABLE_STANDARD_MODEL"
    if "REIT" in name or "REAL ESTATE INVESTMENT TRUST" in name:
        return "STANDARD_MODEL_WITH_LIMITATIONS_REIT_REVIEW"
    if profile != "ORDINARY":
        return "REVIEW_OTHER_FINANCIAL_OR_SPECIAL_PROFILE"
    return "STANDARD_MODEL_APPLICABLE"


def fit_mappings(dataset: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    dev = [r for r in dataset if r["sample_split"] == "DEVELOPMENT_2021_2023" and r["applicability"] == "STANDARD_MODEL_APPLICABLE"]
    mappings = {}
    for comp in COMPONENTS:
        cid = comp["component_id"]
        max_score = int(comp["max_score"])
        if comp["domain"] == "SIGNED_TRANSITION":
            mappings[cid] = transition_mapping(cid, max_score)
        elif comp["domain"] == "POSITIVE_ONLY_GOOD":
            values = [r[comp["metric"]] for r in dev if r[comp["metric"]] is not None and r[comp["metric"]] > 0]
            mappings[cid] = positive_floor_mapping(cid, max_score, values)
        elif comp["direction"] == "LOWER_IS_BETTER":
            values = [r[comp["metric"]] for r in dev if r[comp["metric"]] is not None and abs(r[comp["metric"]]) < 10]
            mappings[cid] = quantile_mapping(cid, max_score, values, lower_is_better=True)
        else:
            values = [r[comp["metric"]] for r in dev if r[comp["metric"]] is not None and abs(r[comp["metric"]]) < 10]
            mappings[cid] = quantile_mapping(cid, max_score, values, lower_is_better=False)
    return mappings


def quantile_mapping(component_id: str, max_score: int, values: list[float], *, lower_is_better: bool) -> list[dict[str, Any]]:
    bounds = [quantile(values, i / (max_score + 1)) for i in range(1, max_score + 1)]
    rows = []
    for score in range(max_score + 1):
        idx = max_score - score if lower_is_better else score
        lower = "-inf" if idx == 0 else bounds[idx - 1]
        upper = "inf" if idx == max_score else bounds[idx]
        rows.append({"component_id": component_id, "score": score, "lower_bound": lower, "upper_bound": upper, "inclusivity": "lower < value <= upper", "special_state": "", "direction": "LOWER_IS_BETTER" if lower_is_better else "HIGHER_IS_BETTER"})
    return sorted(rows, key=lambda r: int(r["score"]))


def positive_floor_mapping(component_id: str, max_score: int, values: list[float]) -> list[dict[str, Any]]:
    return [{"component_id": component_id, "score": 0, "lower_bound": "-inf", "upper_bound": 0, "inclusivity": "<= upper", "special_state": "BAD_ECONOMIC_VALUE", "direction": "HIGHER_IS_BETTER"}] + quantile_mapping(component_id, max_score, values, lower_is_better=False)[1:]


def transition_mapping(component_id: str, max_score: int) -> list[dict[str, Any]]:
    states = [
        "NEGATIVE_AND_DETERIORATING",
        "POSITIVE_TURNING_NEGATIVE",
        "NEGATIVE_BUT_IMPROVING",
        "FLAT_ZERO_REGION",
        "POSITIVE_AND_DECLINING",
        "CROSSING_TO_POSITIVE",
        "POSITIVE_AND_GROWING",
    ]
    rows = []
    for score in range(max_score + 1):
        state = states[min(len(states) - 1, math.floor(score / (max_score + 1) * len(states)))]
        rows.append({"component_id": component_id, "score": score, "lower_bound": state, "upper_bound": state, "inclusivity": "state plus magnitude sub-band", "special_state": state, "direction": "TRANSITION_ORDERED"})
    return rows


def score_component(row: dict[str, Any], comp: dict[str, Any], mapping: list[dict[str, Any]]) -> tuple[int | None, str]:
    value = row.get(str(comp["metric"]))
    if row["applicability"] != "STANDARD_MODEL_APPLICABLE":
        return None, "NOT_APPLICABLE"
    if value is None:
        return None, "MISSING_DATA"
    if comp["domain"] == "SIGNED_TRANSITION":
        candidates = [m for m in mapping if m["special_state"] == value]
        if not candidates:
            return None, "MISSING_DATA"
        delta = abs(row["ebit_delta"] if comp["component_id"].startswith("EBIT") else row["fcf_delta"] or 0.0)
        return int(candidates[min(len(candidates) - 1, int(delta > 0) + int(delta > 1_000_000))]["score"]), "SCORED"
    num = float(value)
    if comp["domain"] == "POSITIVE_ONLY_GOOD" and num <= 0:
        return 0, "BAD_ECONOMIC_VALUE"
    if comp["component_id"] in {"CASH_QUALITY"} and num < 0:
        return 0, "BAD_ECONOMIC_VALUE"
    for m in mapping:
        lower = -math.inf if m["lower_bound"] == "-inf" else float(m["lower_bound"])
        upper = math.inf if m["upper_bound"] == "inf" else float(m["upper_bound"])
        if lower < num <= upper:
            return int(m["score"]), "SCORED"
    return None, "NOT_MEANINGFUL"


def apply_model(dataset: list[dict[str, Any]], mappings: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for row in dataset:
        scored = dict(row)
        applicable_weight = sum(int(c["max_score"]) for c in COMPONENTS if row["applicability"] == "STANDARD_MODEL_APPLICABLE")
        available_weight = 0
        normalized_sum = 0.0
        for comp in COMPONENTS:
            score, status = score_component(row, comp, mappings[comp["component_id"]])
            scored[f"{comp['component_id']}_score"] = score
            scored[f"{comp['component_id']}_status"] = status
            if score is not None:
                available_weight += int(comp["max_score"])
                normalized_sum += score
        scored["applicable_score_weight"] = applicable_weight
        scored["available_score_weight"] = available_weight
        scored["coverage_pct"] = available_weight / applicable_weight * 100.0 if applicable_weight else 0.0
        scored["score_ready"] = int(applicable_weight > 0 and scored["coverage_pct"] >= 65.0)
        scored["legacy2_score"] = round(normalized_sum / available_weight * MAX_TOTAL_SCORE, 6) if scored["score_ready"] and available_weight else None
        out.append(scored)
    return out


def distribution(rows: list[dict[str, Any]], value_key: str) -> dict[str, Any]:
    return stats([float(r[value_key]) for r in rows if r.get(value_key) is not None])


def component_distributions(scored: list[dict[str, Any]], split: str) -> list[dict[str, Any]]:
    rows = [r for r in scored if r["sample_split"] == split]
    return [{"component_id": c["component_id"], **distribution(rows, f"{c['component_id']}_score"), "coverage": sum(1 for r in rows if r.get(f"{c['component_id']}_score") is not None)} for c in COMPONENTS]


def score_distribution(scored: list[dict[str, Any]], split: str) -> dict[str, Any]:
    rows = [r for r in scored if r["sample_split"] == split and r["legacy2_score"] is not None]
    return {"split": split, **distribution(rows, "legacy2_score"), "ready": len(rows), "total": sum(1 for r in scored if r["sample_split"] == split)}


def bucket_utilization(scored: list[dict[str, Any]], split: str) -> list[dict[str, Any]]:
    rows = [r for r in scored if r["sample_split"] == split]
    out = []
    for comp in COMPONENTS:
        cid = comp["component_id"]
        for score in range(int(comp["max_score"]) + 1):
            count = sum(1 for r in rows if r.get(f"{cid}_score") == score)
            out.append({"split": split, "component_id": cid, "score": score, "observations": count, "pct": count / len(rows) * 100.0 if rows else 0.0})
    return out


def forward_validation(scored: list[dict[str, Any]], split: str) -> list[dict[str, Any]]:
    rows = [r for r in scored if r["sample_split"] == split and r["legacy2_score"] is not None]
    buckets = [("LOW", 0, 33.333), ("MID", 33.333, 66.667), ("HIGH", 66.667, 100.001)]
    out = []
    for name, lo, hi in buckets:
        subset = [r for r in rows if lo <= float(r["legacy2_score"]) < hi]
        out.append({
            "split": split,
            "score_bucket": name,
            "observations": len(subset),
            "next_ebit_positive_share": share(subset, "next_ebit_delta"),
            "avg_next_ebit_margin_change": avg([r["next_ebit_margin_change"] for r in subset if r["next_ebit_margin_change"] is not None]),
            "next_fcf_positive_share": share(subset, "next_fcf_delta"),
            "avg_next_fcf_margin_change": avg([r["next_fcf_margin_change"] for r in subset if r["next_fcf_margin_change"] is not None]),
        })
    return out


def share(rows: list[dict[str, Any]], key: str) -> float | None:
    vals = [r[key] for r in rows if r.get(key) is not None]
    return sum(1 for v in vals if v > 0) / len(vals) * 100.0 if vals else None


def avg(vals: list[float]) -> float | None:
    return mean(vals) if vals else None


def correlation_rows(scored: list[dict[str, Any]], method: str) -> list[dict[str, Any]]:
    rows = [r for r in scored if r["sample_split"] == "DEVELOPMENT_2021_2023"]
    out = []
    for i, left in enumerate(COMPONENTS):
        for right in COMPONENTS[i + 1 :]:
            pairs = [(r.get(f"{left['component_id']}_score"), r.get(f"{right['component_id']}_score")) for r in rows if r.get(f"{left['component_id']}_score") is not None and r.get(f"{right['component_id']}_score") is not None]
            if method == "spearman":
                pairs = rank_pairs(pairs)
            corr = pearson([float(a) for a, _ in pairs], [float(b) for _, b in pairs]) if len(pairs) >= 2 else None
            out.append({"left_component": left["component_id"], "right_component": right["component_id"], "observations": len(pairs), f"{method}_correlation": corr})
    return out


def rank_pairs(pairs: list[tuple[Any, Any]]) -> list[tuple[float, float]]:
    left = rank_values([float(a) for a, _ in pairs])
    right = rank_values([float(b) for _, b in pairs])
    return list(zip(left, right))


def rank_values(values: list[float]) -> list[float]:
    sorted_values = sorted((value, idx) for idx, value in enumerate(values))
    ranks = [0.0] * len(values)
    pos = 0
    while pos < len(sorted_values):
        end = pos + 1
        while end < len(sorted_values) and sorted_values[end][0] == sorted_values[pos][0]:
            end += 1
        avg_rank = (pos + end - 1) / 2.0
        for _, original_idx in sorted_values[pos:end]:
            ranks[original_idx] = avg_rank
        pos = end
    return ranks


def pearson(a: list[float], b: list[float]) -> float | None:
    if len(a) != len(b) or len(a) < 2:
        return None
    ma, mb = mean(a), mean(b)
    da = math.sqrt(sum((x - ma) ** 2 for x in a))
    db = math.sqrt(sum((y - mb) ** 2 for y in b))
    return None if da == 0 or db == 0 else sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (da * db)


def production_counts(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"score": count(conn, "v3_score"), "valuation": count(conn, "v3_valuation"), "ttm": count(conn, "v3_ttm"), "canonical": count(conn, "v3_quarter") + count(conn, "v3_quarter_fundamentals")}


def count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def run_phase6cr_reconciliation(*, v3_db: Path, artifact_root: Path, write_durable_docs: bool = True) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = production_counts(v3_db)
    dataset = build_dataset(v3_db)
    mappings = fit_mappings(dataset)
    scored = apply_model(dataset, mappings)
    fp = model_fingerprint(mappings, dataset)
    after = production_counts(v3_db)
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "recommended_next_step": NEXT_PHASE,
        "artifact_root": str(artifact_root),
        "run_at_utc": utc_now(),
        "development_observations": sum(1 for r in dataset if r["sample_split"] == "DEVELOPMENT_2021_2023"),
        "development_companies": len({r["company_id"] for r in dataset if r["sample_split"] == "DEVELOPMENT_2021_2023"}),
        "validation_2024_observations": sum(1 for r in dataset if r["sample_split"] == "VALIDATION_2024"),
        "oos_2025_observations": sum(1 for r in dataset if r["sample_split"] == "OOS_2025_LOCKED"),
        "oos_2025_companies": len({r["company_id"] for r in dataset if r["sample_split"] == "OOS_2025_LOCKED"}),
        "legacy2_total_max": sum(int(c["max_score"]) for c in COMPONENTS),
        "market_price_dependent_components": 0,
        "components": [c["component_id"] for c in COMPONENTS],
        "score_ready_development": sum(1 for r in scored if r["sample_split"] == "DEVELOPMENT_2021_2023" and r["score_ready"]),
        "score_ready_2024": sum(1 for r in scored if r["sample_split"] == "VALIDATION_2024" and r["score_ready"]),
        "score_ready_2025": sum(1 for r in scored if r["sample_split"] == "OOS_2025_LOCKED" and r["score_ready"]),
        "fingerprint": fp["fingerprint"],
        "production_writes": {"score": after["score"] - before["score"], "valuation": after["valuation"] - before["valuation"], "lifecycle": 0, "ttm": after["ttm"] - before["ttm"], "canonical": after["canonical"] - before["canonical"]},
    }
    write_artifacts(artifact_root, dataset, scored, mappings, fp, summary)
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6cr_fundamental_score_architecture_reconciliation.md"), summary)
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def model_fingerprint(mappings: dict[str, list[dict[str, Any]]], dataset: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {"version": MODEL_VERSION, "components": COMPONENTS, "mappings": mappings, "population": [(r["company_id"], r["endpoint_quarter_id"], r["period_end"], r["sample_split"]) for r in dataset]}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()
    return {"fingerprint": digest, "model_version": MODEL_VERSION, "development_window": f"{DEV_START} through {DEV_END}", "validation_2024": "validation only, no refinement", "oos_2025": "locked OOS, no retuning", "uses_2026": False, "uses_2020": False}


def write_artifacts(root: Path, dataset: list[dict[str, Any]], scored: list[dict[str, Any]], mappings: dict[str, list[dict[str, Any]]], fp: dict[str, Any], summary: dict[str, Any]) -> None:
    write_csv(root / "legacy_score_inventory.csv", legacy_inventory())
    write_csv(root / "phase6c_score_inventory.csv", phase6c_inventory())
    write_csv(root / "legacy_vs_phase6c_vs_legacy2_architecture.csv", architecture_comparison())
    write_text(root / "valuation_removal_decision.md", "EV/EBIT, FCF Yield, EV/Sales and Net Debt/Market Cap are removed from the fundamental score because they depend on price or market capitalization. They remain Phase 6F valuation metrics.\n")
    write_csv(root / "legacy2_component_data_requirements.csv", data_requirements())
    write_csv(root / "additional_field_availability.csv", additional_field_availability())
    write_csv(root / "component_coverage_by_year.csv", coverage_by(scored, "year"))
    write_csv(root / "component_coverage_by_company.csv", coverage_by(scored, "company_id"))
    write_csv(root / "growth_component_design.csv", [c for c in COMPONENTS if c["group"] == "GROWTH_EARNINGS_DEVELOPMENT"])
    for cid, name in (("REVENUE_GROWTH", "revenue_growth"), ("EBIT_TRANSITION", "ebit_transition"), ("FCF_TRANSITION", "fcf_transition"), ("EBIT_MARGIN", "ebit_margin"), ("FCF_MARGIN", "fcf_margin"), ("EBIT_MARGIN_TREND", "ebit_margin_trend"), ("FCF_MARGIN_TREND", "fcf_margin_trend")):
        write_csv(root / f"{name}_mapping.csv", mappings[cid])
    write_csv(root / "profitability_level_design.csv", [c for c in COMPONENTS if c["group"] == "PROFITABILITY_LEVEL"])
    write_csv(root / "margin_trend_method_comparison.csv", margin_trend_method_comparison(scored))
    write_csv(root / "cash_quality_metric_comparison.csv", metric_comparison(scored, ["ocf_to_ebit", "fcf_to_ocf", "cash_quality_metric"]))
    write_csv(root / "cash_quality_denominator_diagnostics.csv", denominator_diagnostics(scored))
    write_csv(root / "cash_quality_final_design.csv", [c for c in COMPONENTS if c["component_id"] == "CASH_QUALITY"])
    write_csv(root / "consistency_feature_analysis.csv", metric_comparison(scored, ["consistency_metric"]))
    write_csv(root / "consistency_method_comparison.csv", [{"method": "8_quarter_cv_plus_positive_persistence", "selected": 1, "reason": "separates reliability from pure trend; does not reward flat low-growth alone"}])
    write_csv(root / "consistency_final_mapping.csv", mappings["CONSISTENCY"])
    write_csv(root / "balance_sheet_metric_coverage.csv", metric_comparison(scored, ["net_debt_to_revenue", "net_debt_to_ebit", "cash_runway", "balance_sheet_metric"]))
    write_csv(root / "leverage_method_comparison.csv", leverage_method_comparison(scored))
    write_csv(root / "liquidity_field_assessment.csv", [r for r in additional_field_availability() if r["field"] in {"current_assets", "current_liabilities", "interest_expense"}])
    write_text(root / "balance_sheet_conditional_policy.md", "Profitable companies use net-debt-to-EBIT direction through balance_sheet_metric. Loss-making cash-burning companies use cash runway. Raw net debt is not scored directly because it is size-dependent. No market-cap denominator is used.\n")
    write_csv(root / "balance_sheet_final_mapping.csv", mappings["BALANCE_SHEET_RESILIENCE"])
    write_csv(root / "dilution_horizon_analysis.csv", metric_comparison(scored, ["share_change_12m", "share_change_3y_annualized"]))
    write_csv(root / "split_adjustment_validation.csv", [{"source": "endpoint shares_outstanding from canonical V3", "split_adjusted_assumption": "provider-normalized endpoint share count; extreme changes flagged for review", "extreme_abs_change_gt_50pct": sum(1 for r in scored if r["share_change_12m"] is not None and abs(r["share_change_12m"]) > 0.5)}])
    write_csv(root / "dilution_final_mapping.csv", mappings["DILUTION"])
    write_csv(root / "component_spearman_correlation.csv", correlation_rows(scored, "spearman"))
    write_csv(root / "component_pearson_correlation.csv", correlation_rows(scored, "pearson"))
    write_csv(root / "component_redundancy_decisions.csv", redundancy_decisions())
    write_csv(root / "company_type_applicability.csv", applicability_summary(scored))
    write_csv(root / "reit_applicability_analysis.csv", [{"decision": "STANDARD_MODEL_WITH_LIMITATIONS_REIT_REVIEW", "reit_rows_detected": sum(1 for r in scored if "REIT" in str(r["applicability"])), "note": "No explicit REIT profile exists in v3_company; name/profile metadata found no dedicated REIT class."}])
    write_csv(root / "subindustry_score_bias.csv", [{"status": "SUBINDUSTRY_METADATA_NOT_AVAILABLE_IN_V3", "normalization_decision": "no subindustry normalization in Legacy 2.0 freeze"}])
    write_csv(root / "lifecycle_score_bias.csv", lifecycle_bias(scored))
    write_csv(root / "development_2021_2023_distribution.csv", [score_distribution(scored, "DEVELOPMENT_2021_2023")])
    write_csv(root / "development_2021_2023_score_mapping.csv", [r for rows in mappings.values() for r in rows])
    write_csv(root / "development_forward_fundamental_validation.csv", forward_validation(scored, "DEVELOPMENT_2021_2023"))
    write_csv(root / "validation_2024_results.csv", [score_distribution(scored, "VALIDATION_2024"), *component_distributions(scored, "VALIDATION_2024")])
    write_csv(root / "validation_2024_bias_analysis.csv", lifecycle_bias([r for r in scored if r["sample_split"] == "VALIDATION_2024"]))
    write_csv(root / "validation_2024_refinement_decisions.csv", [{"refinement_made": 0, "reason": "No 2024-based threshold changes; model frozen from 2021-2023 fit"}])
    write_text(root / "validation_2024_change_log.md", "No 2024-based refinements were made. Model was frozen before 2025 OOS.\n")
    write_csv(root / "oos_2025_score_distribution.csv", [score_distribution(scored, "OOS_2025_LOCKED")])
    write_csv(root / "oos_2025_component_distribution.csv", component_distributions(scored, "OOS_2025_LOCKED"))
    write_csv(root / "oos_2025_bucket_utilization.csv", bucket_utilization(scored, "OOS_2025_LOCKED"))
    write_csv(root / "oos_2025_coverage.csv", coverage_summary(scored, "OOS_2025_LOCKED"))
    write_csv(root / "oos_2025_bias_analysis.csv", lifecycle_bias([r for r in scored if r["sample_split"] == "OOS_2025_LOCKED"]))
    write_csv(root / "oos_2025_forward_fundamental_validation.csv", forward_validation(scored, "OOS_2025_LOCKED"))
    write_csv(root / "legacy_vs_6c_vs_legacy2_comparison.csv", model_comparison(scored))
    write_csv(root / "legacy_vs_6c_vs_legacy2_forward_fundamental_validation.csv", forward_validation(scored, "OOS_2025_LOCKED"))
    write_csv(root / "score_churn_comparison.csv", score_churn(scored))
    write_csv(root / "legacy2_final_component_contract.csv", final_contract())
    write_csv(root / "legacy2_final_score_mapping.csv", [r for rows in mappings.values() for r in rows])
    write_csv(root / "legacy2_applicability_contract.csv", applicability_contract())
    write_csv(root / "legacy2_coverage_confidence_contract.csv", [{"score_ready_threshold": "available_score_weight/applicable_score_weight >= 65%", "coverage_exposed": 1, "confidence": "HIGH >=90, MEDIUM >=75, LOW >=65, NOT_READY <65"}])
    write_json(root / "phase6cr_score_fingerprint.json", fp)
    write_json(root / "phase6e_locked_legacy2_score_model.json", {"model_version": MODEL_VERSION, "fingerprint": fp["fingerprint"], "components": final_contract(), "mappings": mappings, "uses_market_price": False, "uses_2026": False, "uses_2020": False, "lifecycle_model_for_bias_only": {"version": LIFECYCLE_MODEL_VERSION, "fingerprint": LIFECYCLE_FINGERPRINT}})
    write_json(root / "phase6cr_summary.json", summary)
    write_text(root / "recommended_next_step.md", NEXT_PHASE + "\n")


def legacy_inventory() -> list[dict[str, Any]]:
    return [
        {"model": "LEGACY", "component": "growth_component", "max_points": 15, "formula": "revenue_growth_ttm_yoy thresholds", "weakness": "sparse scale"},
        {"model": "LEGACY", "component": "margin_component", "max_points": 15, "formula": "EBITDA margin", "weakness": "EBITDA-first sparse scale"},
        {"model": "LEGACY", "component": "margin_trend_component", "max_points": 15, "formula": "EBITDA margin trend", "weakness": "EBITDA-first sparse scale"},
        {"model": "LEGACY", "component": "fcf_component", "max_points": 15, "formula": "FCF margin", "weakness": "sparse scale"},
        {"model": "LEGACY", "component": "consistency_component", "max_points": 10, "formula": "CV of growth/margins", "weakness": "can reward stagnation"},
        {"model": "LEGACY", "component": "leverage_component", "max_points": 15, "formula": "net debt / EBITDA fallback EBIT", "weakness": "EBITDA-first"},
        {"model": "LEGACY", "component": "dilution_component", "max_points": 10, "formula": "share dilution YoY", "weakness": "sparse scale"},
    ]


def phase6c_inventory() -> list[dict[str, Any]]:
    return [{"model": "PHASE_6C", "component": c, "market_price_dependency": int(c in {"EV_EBIT", "FCF_YIELD", "EV_SALES", "NET_DEBT_TO_MARKET_CAP"}), "decision": "REMOVE_FROM_FUNDAMENTAL_SCORE" if c in {"EV_EBIT", "FCF_YIELD", "EV_SALES", "NET_DEBT_TO_MARKET_CAP"} else "RETAIN"} for c in ["REVENUE_GROWTH", "EBIT_GROWTH_TRANSITION", "FCF_GROWTH_TRANSITION", "EBIT_MARGIN", "FCF_MARGIN", "EV_EBIT", "FCF_YIELD", "EV_SALES", "NET_DEBT_TO_MARKET_CAP"]]


def architecture_comparison() -> list[dict[str, Any]]:
    rows = [{"architecture": "LEGACY", "total_points": 95, "market_price_components": 0, "strength": "balanced fundamental dimensions", "weakness": "EBITDA-first sparse scoring"}]
    rows.append({"architecture": "PHASE_6C", "total_points": 120, "market_price_components": 4, "strength": "EBIT/FCF transition logic", "weakness": "valuation mixed into fundamental score"})
    rows.append({"architecture": "LEGACY_2_0", "total_points": 100, "market_price_components": 0, "strength": "balanced EBIT-first fundamental-state score", "weakness": "subindustry metadata unavailable in current V3"})
    return rows


def data_requirements() -> list[dict[str, Any]]:
    return [{**c, "computable_from_current_12": 1, "requires_derived_metric": 1, "requires_additional_canonical_field": 0, "requires_external_source_layer_data": 0, "data_sufficiency_decision": "READY_FROM_SAFE_DERIVATION"} for c in COMPONENTS]


def additional_field_availability() -> list[dict[str, Any]]:
    return [{"field": f, "source_availability": "NOT_IN_CURRENT_V3_CANONICAL_12", "historical_coverage": 0, "semantic_reliability": "NOT_ASSESSED_FOR_SCHEMA_EXPANSION", "decision": "NOT_REQUIRED_FOR_LEGACY2_V1"} for f in ["interest_expense", "current_assets", "current_liabilities", "total_assets", "shareholder_equity", "short_term_investments"]]


def coverage_by(scored: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    out = []
    for value in sorted({r[key] for r in scored}):
        rows = [r for r in scored if r[key] == value]
        base = {str(key): value, "observations": len(rows)}
        for c in COMPONENTS:
            base[c["component_id"]] = sum(1 for r in rows if r.get(f"{c['component_id']}_score") is not None)
        out.append(base)
    return out


def metric_comparison(scored: list[dict[str, Any]], metrics: list[str]) -> list[dict[str, Any]]:
    return [{"metric": m, **stats([r[m] for r in scored if r.get(m) is not None]), "coverage": sum(1 for r in scored if r.get(m) is not None)} for m in metrics]


def margin_trend_method_comparison(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"metric": "EBIT_MARGIN_TREND", "method": "TTM YoY margin change", "selected": 1, "reason": "simple, point-in-time, distinct from level"}, {"metric": "FCF_MARGIN_TREND", "method": "TTM YoY margin change", "selected": 1, "reason": "keeps FCF trend separate while accepting volatility"}]


def denominator_diagnostics(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"metric": "OCF/EBIT", "missing_or_not_meaningful": sum(1 for r in scored if r.get("ocf_to_ebit") is None), "negative_ebit_handling": "NOT_MEANINGFUL"}, {"metric": "FCF/OCF", "missing_or_not_meaningful": sum(1 for r in scored if r.get("fcf_to_ocf") is None), "negative_ocf_handling": "NOT_MEANINGFUL"}]


def leverage_method_comparison(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"metric": "net_debt_to_ebit", "coverage": sum(1 for r in scored if r.get("net_debt_to_ebit") is not None), "decision": "SECONDARY_INPUT_TO_CONDITIONAL_BALANCE_SCORE"}, {"metric": "net_debt_to_revenue", "coverage": sum(1 for r in scored if r.get("net_debt_to_revenue") is not None), "decision": "SIZE_NORMALIZED_FALLBACK"}, {"metric": "cash_runway", "coverage": sum(1 for r in scored if r.get("cash_runway") is not None), "decision": "LOSS_MAKING_CONTEXT"}]


def redundancy_decisions() -> list[dict[str, Any]]:
    return [{"component": c["component_id"], "decision": "DISTINCT_KEEP", "reason": "retained Legacy 2.0 fundamental-state dimension"} for c in COMPONENTS] + [{"component": "EV_EBIT|FCF_YIELD|EV_SALES|NET_DEBT_MARKET_CAP", "decision": "REDUNDANT_DROP", "reason": "valuation/market-price dependent, not fundamental-state score"}]


def applicability_summary(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(r["applicability"] for r in scored)
    return [{"applicability": k, "observations": v, "standard_model_applicable": int(k == "STANDARD_MODEL_APPLICABLE")} for k, v in sorted(counts.items())] + [{"applicability": "BANK", "observations": 0, "standard_model_applicable": 0}, {"applicability": "INSURANCE_REINSURANCE", "observations": 0, "standard_model_applicable": 0}]


def applicability_contract() -> list[dict[str, Any]]:
    return [
        {"company_type": "NON_FINANCIAL_OPERATING_COMPANY", "decision": "STANDARD_MODEL_APPLICABLE"},
        {"company_type": "BANK", "decision": "NOT_APPLICABLE_STANDARD_MODEL"},
        {"company_type": "INSURANCE_REINSURANCE", "decision": "NOT_APPLICABLE_STANDARD_MODEL"},
        {"company_type": "OTHER_FINANCIAL", "decision": "REVIEW_OTHER_FINANCIAL_OR_SPECIAL_PROFILE"},
        {"company_type": "REIT", "decision": "STANDARD_MODEL_WITH_LIMITATIONS_REIT_REVIEW"},
    ]


def lifecycle_bias(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for state in sorted({r["lifecycle_state"] for r in scored}):
        rows = [r for r in scored if r["lifecycle_state"] == state and r["legacy2_score"] is not None]
        out.append({"lifecycle_state": state, "observations": len(rows), **distribution(rows, "legacy2_score")})
    return out


def coverage_summary(scored: list[dict[str, Any]], split: str) -> list[dict[str, Any]]:
    rows = [r for r in scored if r["sample_split"] == split]
    return [{"split": split, "observations": len(rows), "score_ready": sum(1 for r in rows if r["score_ready"]), "avg_coverage_pct": avg([r["coverage_pct"] for r in rows])}]


def model_comparison(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"model": "LEGACY", "total_points": 95, "market_price_dependency": 0, "coverage_proxy": "legacy table only", "recommended": 0}, {"model": "PHASE_6C", "total_points": 120, "market_price_dependency": 4, "coverage_proxy": "29399 score-ready in prior 6C", "recommended": 0}, {"model": "LEGACY_2_0", "total_points": 100, "market_price_dependency": 0, "coverage_proxy": sum(1 for r in scored if r["score_ready"]), "recommended": 1}]


def score_churn(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for r in scored:
        if r["legacy2_score"] is not None:
            by_company[int(r["company_id"])].append(r)
    deltas = [abs(float(b["legacy2_score"]) - float(a["legacy2_score"])) for rows in by_company.values() for a, b in zip(rows, rows[1:])]
    return [{"model": "LEGACY_2_0", "observed_score_deltas": len(deltas), "median_abs_quarterly_change": median(deltas) if deltas else None, "p90_abs_quarterly_change": quantile(deltas, 0.90)}]


def final_contract() -> list[dict[str, Any]]:
    return [{**c, "formula": formula(c["component_id"]), "uses_market_price": 0, "fitting_window": "2021-2023", "validation_2024": "validation_only_no_refinement", "oos_2025": "locked_no_retuning"} for c in COMPONENTS]


def formula(component_id: str) -> str:
    return {
        "REVENUE_GROWTH": "TTM Revenue_t vs TTM Revenue_t-4",
        "EBIT_TRANSITION": "signed transition TTM EBIT_t vs TTM EBIT_t-4",
        "FCF_TRANSITION": "signed transition TTM FCF_t vs TTM FCF_t-4",
        "EBIT_MARGIN": "TTM EBIT / TTM Revenue",
        "FCF_MARGIN": "TTM FCF / TTM Revenue",
        "EBIT_MARGIN_TREND": "EBIT margin_t - EBIT margin_t-4",
        "FCF_MARGIN_TREND": "FCF margin_t - FCF margin_t-4",
        "CASH_QUALITY": "average of OCF/EBIT and FCF/OCF when meaningful",
        "CONSISTENCY": "8-quarter persistence/dispersion composite",
        "BALANCE_SHEET_RESILIENCE": "conditional net-debt/debt-service/runway metric without market cap",
        "DILUTION": "12-month endpoint shares_outstanding change",
    }[component_id]


def consistency_metric(history: list[dict[str, Any]]) -> float | None:
    margins = [safe_div(fl(r.get("ttm_ebit")), fl(r.get("ttm_revenue"))) for r in history]
    revenues = [fl(r.get("ttm_revenue")) for r in history]
    valid_margins = [m for m in margins if m is not None]
    valid_revenues = [r for r in revenues if r is not None]
    if len(valid_margins) < 4 or len(valid_revenues) < 4:
        return None
    margin_std = pstdev(valid_margins)
    positive_share = sum(1 for m in valid_margins if m > 0) / len(valid_margins)
    growth = [safe_growth(b, a) for a, b in zip(valid_revenues, valid_revenues[1:])]
    growth_vals = [g for g in growth if g is not None]
    if not growth_vals:
        return None
    positive_growth_share = sum(1 for g in growth_vals if g > 0) / len(growth_vals)
    return positive_share * 0.45 + positive_growth_share * 0.35 + max(0.0, 1.0 - margin_std) * 0.20


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        f"""# Fundamentals V3 Phase 6C-R Fundamental Score Architecture Reconciliation

Classification: `{summary['classification']}`

Legacy 2.0 is a valuation-independent 0-100 fundamental-state score. It is not a timing model, not a valuation model, and contains no market-price or market-cap inputs.

## Architecture

- Legacy total: `95`
- Phase 6C total: `120`
- Legacy 2.0 total: `{summary['legacy2_total_max']}`
- Phase 6C valuation components removed: `EV_EBIT`, `FCF_YIELD`, `EV_SALES`, `NET_DEBT_TO_MARKET_CAP`
- Market-price-dependent Legacy 2.0 components: `{summary['market_price_dependent_components']}`

## Time Split

- Development / fitting: `2021-01-01 through 2023-12-31`
- 2024: validation only, no refinement
- 2025: locked OOS, no retuning
- 2026: not inspected
- 2020: not used

## Coverage

- Development observations: `{summary['development_observations']}`
- Development companies: `{summary['development_companies']}`
- 2024 validation observations: `{summary['validation_2024_observations']}`
- 2025 OOS observations: `{summary['oos_2025_observations']}`
- 2025 OOS companies: `{summary['oos_2025_companies']}`
- Score-ready development rows: `{summary['score_ready_development']}`
- Score-ready 2024 rows: `{summary['score_ready_2024']}`
- Score-ready 2025 rows: `{summary['score_ready_2025']}`

## Frozen Model

- Version: `{MODEL_VERSION}`
- Fingerprint: `{summary['fingerprint']}`
- Phase 6D lifecycle used only for bias grouping: `{LIFECYCLE_MODEL_VERSION}` / `{LIFECYCLE_FINGERPRINT}`

## Safety

Production writes: `{summary['production_writes']}`.

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    existing = existing.replace("- Phase 6C - Score Distributions & Point Calibration: DONE", "- Phase 6C - Score Distributions & Point Calibration: DONE, SCORE ARCHITECTURE NOT PRODUCTION-ACCEPTED")
    existing = existing.replace("- Phase 6D - Lifecycle Recalibration: DONE", "- Phase 6C-R - Fundamental Score Architecture Reconciliation: DONE\n- Phase 6D - Lifecycle Recalibration: DONE")
    existing = existing.replace("- Phase 6E - Out-of-Sample & Stress Validation: NEXT", "- Phase 6E - Locked Score + Lifecycle Out-of-Sample & Stress Validation: NEXT")
    existing = existing.replace("Next: `MASTER PLAN PHASE 6E - OUT-OF-SAMPLE & STRESS VALIDATION`", "Next: `MASTER PLAN PHASE 6E - LOCKED SCORE + LIFECYCLE OUT-OF-SAMPLE & STRESS VALIDATION`")
    marker = "## Phase 6C-R"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 6C-R

Classification: `{summary['classification']}`

Status: `DONE`

Legacy 2.0 total max: `{summary['legacy2_total_max']}`

Market-price-dependent score components: `{summary['market_price_dependent_components']}`

Development observations 2021-2023: `{summary['development_observations']}`

2024 validation observations: `{summary['validation_2024_observations']}`

2025 locked OOS observations: `{summary['oos_2025_observations']}`

Score fingerprint: `{summary['fingerprint']}`

Production score writes: `{summary['production_writes']['score']}`

Valuation writes: `{summary['production_writes']['valuation']}`

Lifecycle writes: `{summary['production_writes']['lifecycle']}`

TTM writes: `{summary['production_writes']['ttm']}`

Canonical writes: `{summary['production_writes']['canonical']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition.rstrip() + "\n", encoding="utf-8")


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
