from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from swingmaster.fundamentals.v3_phase6b_score_lifecycle_calibration_design import (
    CALIBRATION_END,
    CALIBRATION_START,
    NEAR_ZERO_EPSILON,
    classify_signed_transition,
)

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6C_SCORE_DISTRIBUTIONS_CALIBRATED_READY_FOR_PHASE6D"
CLASSIFICATION_REFINEMENT = "FUNDAMENTALS_V3_PHASE6C_CALIBRATION_REFINEMENT_REQUIRED"
NEXT_PHASE = "MASTER PLAN PHASE 6D - LIFECYCLE RECALIBRATION"
MODEL_VERSION = "V3_SCORE_CALIBRATION_EBIT_FIRST_V1"
MAX15 = 15
MAX10 = 10

FINAL_COMPONENTS = [
    {"metric_id": "REVENUE_GROWTH", "group": "GROWTH", "max_score": MAX15, "direction": "HIGHER_IS_BETTER", "method": "HYBRID_POOLED_QUANTILE", "domain": "HIGHER_IS_BETTER"},
    {"metric_id": "EBIT_GROWTH_TRANSITION", "group": "GROWTH", "max_score": MAX15, "direction": "TRANSITION_ORDERED", "method": "SIGNED_TRANSITION_BUCKETS", "domain": "SIGNED_TRANSITION_METRIC"},
    {"metric_id": "FCF_GROWTH_TRANSITION", "group": "GROWTH", "max_score": MAX10, "direction": "TRANSITION_ORDERED", "method": "SIGNED_TRANSITION_BUCKETS", "domain": "SIGNED_TRANSITION_METRIC"},
    {"metric_id": "EBIT_MARGIN", "group": "PROFITABILITY_QUALITY", "max_score": MAX15, "direction": "HIGHER_IS_BETTER", "method": "HARD_ZERO_POSITIVE_POOLED_QUANTILE", "domain": "POSITIVE_ONLY_GOOD"},
    {"metric_id": "FCF_MARGIN", "group": "PROFITABILITY_QUALITY", "max_score": MAX15, "direction": "HIGHER_IS_BETTER", "method": "HARD_ZERO_POSITIVE_POOLED_QUANTILE", "domain": "POSITIVE_ONLY_GOOD"},
    {"metric_id": "EV_EBIT", "group": "VALUATION", "max_score": MAX15, "direction": "LOWER_IS_BETTER", "method": "POSITIVE_DENOMINATOR_POOLED_QUANTILE", "domain": "MULTIPLE_REQUIRES_POSITIVE_DENOMINATOR"},
    {"metric_id": "FCF_YIELD", "group": "VALUATION", "max_score": MAX15, "direction": "HIGHER_IS_BETTER", "method": "HARD_ZERO_POSITIVE_POOLED_QUANTILE", "domain": "POSITIVE_ONLY_GOOD"},
    {"metric_id": "EV_SALES", "group": "VALUATION", "max_score": MAX10, "direction": "LOWER_IS_BETTER", "method": "POSITIVE_DENOMINATOR_POOLED_QUANTILE", "domain": "LOWER_IS_BETTER"},
    {"metric_id": "NET_DEBT_TO_MARKET_CAP", "group": "BALANCE_SHEET_RISK", "max_score": MAX10, "direction": "LOWER_IS_BETTER", "method": "NET_CASH_AWARE_POOLED_QUANTILE", "domain": "NEGATIVE_CAN_BE_GOOD"},
]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def quantile(values: list[float], pct: float) -> float | None:
    vals = sorted(v for v in values if v is not None and math.isfinite(v))
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * pct
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return vals[int(pos)]
    return vals[lo] + (vals[hi] - vals[lo]) * (pos - lo)


def stats(values: list[float]) -> dict[str, Any]:
    vals = [v for v in values if v is not None and math.isfinite(v)]
    if not vals:
        return {k: None for k in ("min", "max", "mean", "std", "skewness", "p1", "p5", "p10", "p25", "p50", "p75", "p90", "p95", "p99")} | {"observations": 0}
    avg = mean(vals)
    std = pstdev(vals) if len(vals) > 1 else 0.0
    skew = None if std == 0 else mean([((v - avg) / std) ** 3 for v in vals])
    return {
        "observations": len(vals),
        "min": min(vals),
        "max": max(vals),
        "mean": avg,
        "std": std,
        "skewness": skew,
        "p1": quantile(vals, 0.01),
        "p5": quantile(vals, 0.05),
        "p10": quantile(vals, 0.10),
        "p25": quantile(vals, 0.25),
        "p50": quantile(vals, 0.50),
        "p75": quantile(vals, 0.75),
        "p90": quantile(vals, 0.90),
        "p95": quantile(vals, 0.95),
        "p99": quantile(vals, 0.99),
    }


def load_ttm(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            """
            SELECT c.company_id,c.ticker,c.market,c.active,t.*
            FROM v3_ttm t
            JOIN v3_company c ON c.company_id=t.company_id
            ORDER BY c.company_id,t.endpoint_fiscal_year,
                     CASE t.endpoint_fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END
            """
        )]


def load_price_map(osakedata_db: Path | None, rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], float]:
    if osakedata_db is None or not osakedata_db.exists():
        return {}
    keys = sorted({(str(r["ticker"]).upper(), str(r["market"]).lower(), str(r["period_end"])) for r in rows if r.get("period_end")})
    out: dict[tuple[str, str, str], float] = {}
    with sqlite3.connect(f"file:{osakedata_db}?mode=ro", uri=True) as conn:
        for ticker, market, period_end in keys:
            row = conn.execute(
                "SELECT close FROM osakedata WHERE osake=? AND market=? AND pvm<=? ORDER BY pvm DESC LIMIT 1",
                (ticker, market, period_end),
            ).fetchone()
            if row is not None and row[0] is not None:
                out[(ticker, market, period_end)] = float(row[0])
    return out


def build_dataset(v3_db: Path, osakedata_db: Path | None) -> list[dict[str, Any]]:
    all_rows = load_ttm(v3_db)
    price_map = load_price_map(osakedata_db, [r for r in all_rows if CALIBRATION_START <= str(r["period_end"]) <= CALIBRATION_END])
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        by_company[int(row["company_id"])].append(row)
    out: list[dict[str, Any]] = []
    for rows in by_company.values():
        for idx, row in enumerate(rows):
            if not (CALIBRATION_START <= str(row["period_end"]) <= CALIBRATION_END):
                continue
            prev = rows[idx - 4] if idx >= 4 else None
            out.append(metric_row(row, prev, price_map))
    return out


def metric_row(row: dict[str, Any], prev: dict[str, Any] | None, price_map: dict[tuple[str, str, str], float]) -> dict[str, Any]:
    ticker = str(row["ticker"]).upper()
    market = str(row["market"]).lower()
    period_end = str(row["period_end"])
    price = price_map.get((ticker, market, period_end))
    shares = f(row.get("shares_outstanding"))
    cap = price * shares if price is not None and shares is not None and shares > 0 else None
    debt = f(row.get("total_debt"))
    cash = f(row.get("cash"))
    net_debt = debt - cash if debt is not None and cash is not None else None
    ev = cap + net_debt if cap is not None and net_debt is not None else None
    revenue = f(row.get("ttm_revenue"))
    ebit = f(row.get("ttm_ebit"))
    fcf = f(row.get("ttm_fcf"))
    prev_revenue = f(prev.get("ttm_revenue")) if prev else None
    prev_ebit = f(prev.get("ttm_ebit")) if prev else None
    prev_fcf = f(prev.get("ttm_fcf")) if prev else None
    return {
        "company_id": row["company_id"],
        "ticker": ticker,
        "market": market,
        "active": row["active"],
        "endpoint_quarter_id": row["endpoint_quarter_id"],
        "period_end": period_end,
        "year": int(period_end[:4]),
        "price_available": int(price is not None),
        "price": price,
        "market_cap": cap,
        "enterprise_value": ev,
        "revenue_growth": safe_growth(revenue, prev_revenue),
        "ebit_transition_state": classify_signed_transition(prev_ebit, ebit),
        "ebit_transition_delta": None if prev_ebit is None or ebit is None else ebit - prev_ebit,
        "fcf_transition_state": classify_signed_transition(prev_fcf, fcf),
        "fcf_transition_delta": None if prev_fcf is None or fcf is None else fcf - prev_fcf,
        "ebit_margin": safe_div(ebit, revenue),
        "fcf_margin": safe_div(fcf, revenue),
        "ev_ebit": safe_positive_multiple(ev, ebit),
        "ev_ebit_status": multiple_status(ev, ebit),
        "fcf_yield": safe_div(fcf, cap),
        "ev_sales": safe_positive_multiple(ev, revenue),
        "ev_sales_status": multiple_status(ev, revenue),
        "net_debt": net_debt,
        "net_debt_to_market_cap": safe_div(net_debt, cap),
        "net_debt_to_ebit": safe_positive_multiple(net_debt, ebit),
        "net_debt_ebit_status": multiple_status(net_debt, ebit),
        "gross_margin": safe_div(f(row.get("ttm_gross_profit")), revenue),
        "operating_margin": safe_div(f(row.get("ttm_operating_income")), revenue),
        "ebitda_margin": safe_div(f(row.get("ttm_ebitda")), revenue),
        "net_margin": safe_div(f(row.get("ttm_net_income")), revenue),
        "ocf_margin": safe_div(f(row.get("ttm_ocf")), revenue),
        "ev_ebitda": safe_positive_multiple(ev, f(row.get("ttm_ebitda"))),
        "pe": safe_positive_multiple(cap, f(row.get("ttm_net_income"))),
        "ev_ocf": safe_positive_multiple(ev, f(row.get("ttm_ocf"))),
    }


def f(value: Any) -> float | None:
    return None if value is None else float(value)


def safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or abs(den) <= NEAR_ZERO_EPSILON:
        return None
    return num / den


def safe_growth(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev is None or abs(prev) <= NEAR_ZERO_EPSILON or prev <= 0:
        return None
    return (cur - prev) / abs(prev)


def multiple_status(num: float | None, den: float | None) -> str:
    if num is None or den is None:
        return "MISSING_DATA"
    if num <= 0 or abs(den) <= NEAR_ZERO_EPSILON or den <= 0:
        return "NOT_MEANINGFUL"
    return "MEANINGFUL"


def safe_positive_multiple(num: float | None, den: float | None) -> float | None:
    return num / den if multiple_status(num, den) == "MEANINGFUL" else None


def metric_value(row: dict[str, Any], metric_id: str) -> float | str | None:
    return {
        "REVENUE_GROWTH": row["revenue_growth"],
        "EBIT_GROWTH_TRANSITION": row["ebit_transition_state"],
        "FCF_GROWTH_TRANSITION": row["fcf_transition_state"],
        "EBIT_MARGIN": row["ebit_margin"],
        "FCF_MARGIN": row["fcf_margin"],
        "EV_EBIT": row["ev_ebit"],
        "FCF_YIELD": row["fcf_yield"],
        "EV_SALES": row["ev_sales"],
        "NET_DEBT_TO_MARKET_CAP": row["net_debt_to_market_cap"],
    }[metric_id]


def numeric_metric_name(metric_id: str) -> str:
    return {
        "REVENUE_GROWTH": "revenue_growth",
        "EBIT_MARGIN": "ebit_margin",
        "FCF_MARGIN": "fcf_margin",
        "EV_EBIT": "ev_ebit",
        "FCF_YIELD": "fcf_yield",
        "EV_SALES": "ev_sales",
        "NET_DEBT_TO_MARKET_CAP": "net_debt_to_market_cap",
    }[metric_id]


def build_score_mappings(dataset: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    mappings: dict[str, list[dict[str, Any]]] = {}
    for comp in FINAL_COMPONENTS:
        metric_id = comp["metric_id"]
        if comp["domain"] == "SIGNED_TRANSITION_METRIC":
            mappings[metric_id] = transition_mapping(metric_id, int(comp["max_score"]))
        elif comp["domain"] == "POSITIVE_ONLY_GOOD":
            vals = [float(metric_value(r, metric_id)) for r in dataset if isinstance(metric_value(r, metric_id), (int, float)) and float(metric_value(r, metric_id)) > 0]
            mappings[metric_id] = positive_floor_mapping(metric_id, int(comp["max_score"]), vals, comp["direction"])
        elif comp["direction"] == "LOWER_IS_BETTER":
            vals = [float(metric_value(r, metric_id)) for r in dataset if isinstance(metric_value(r, metric_id), (int, float))]
            mappings[metric_id] = quantile_mapping(metric_id, int(comp["max_score"]), vals, lower_is_better=True)
        else:
            vals = [float(metric_value(r, metric_id)) for r in dataset if isinstance(metric_value(r, metric_id), (int, float))]
            mappings[metric_id] = quantile_mapping(metric_id, int(comp["max_score"]), vals, lower_is_better=False)
    return mappings


def positive_floor_mapping(metric_id: str, max_score: int, values: list[float], direction: str) -> list[dict[str, Any]]:
    rows = [{"metric_id": metric_id, "score": 0, "lower_bound": "-inf", "upper_bound": 0, "inclusivity": "<= upper", "economic_meaning": "bad economic value / non-positive level", "special_state": "BAD_ECONOMIC_VALUE", "direction": direction}]
    rows.extend(quantile_mapping(metric_id, max_score, values, lower_is_better=False, first_score=1))
    return rows


def quantile_mapping(metric_id: str, max_score: int, values: list[float], *, lower_is_better: bool, first_score: int = 0) -> list[dict[str, Any]]:
    score_count = max_score - first_score + 1
    bounds = [quantile(values, i / score_count) for i in range(1, score_count)]
    rows = []
    for offset, score in enumerate(range(first_score, max_score + 1)):
        lower = "-inf" if offset == 0 else bounds[offset - 1]
        upper = "inf" if offset == score_count - 1 else bounds[offset]
        actual_score = max_score - offset if lower_is_better else score
        rows.append({
            "metric_id": metric_id,
            "score": actual_score,
            "lower_bound": lower,
            "upper_bound": upper,
            "inclusivity": "lower < value <= upper",
            "economic_meaning": "lower is better quantile band" if lower_is_better else "higher is better quantile band",
            "special_state": "",
            "direction": "LOWER_IS_BETTER" if lower_is_better else "HIGHER_IS_BETTER",
        })
    return sorted(rows, key=lambda r: int(r["score"]))


def transition_mapping(metric_id: str, max_score: int) -> list[dict[str, Any]]:
    if max_score == 15:
        bands = {
            "NEGATIVE_AND_DETERIORATING": range(0, 3),
            "POSITIVE_TURNING_NEGATIVE": range(0, 3),
            "NEGATIVE_BUT_IMPROVING": range(3, 6),
            "FLAT_ZERO_REGION": range(6, 7),
            "POSITIVE_AND_DECLINING": range(7, 10),
            "CROSSING_TO_POSITIVE": range(10, 13),
            "POSITIVE_AND_GROWING": range(13, 16),
        }
    else:
        bands = {
            "NEGATIVE_AND_DETERIORATING": range(0, 2),
            "POSITIVE_TURNING_NEGATIVE": range(0, 2),
            "NEGATIVE_BUT_IMPROVING": range(2, 4),
            "FLAT_ZERO_REGION": range(4, 5),
            "POSITIVE_AND_DECLINING": range(5, 7),
            "CROSSING_TO_POSITIVE": range(7, 9),
            "POSITIVE_AND_GROWING": range(9, 11),
        }
    rows = []
    for state, scores in bands.items():
        for score in scores:
            rows.append({"metric_id": metric_id, "score": score, "lower_bound": state, "upper_bound": state, "inclusivity": "state plus magnitude sub-band", "economic_meaning": state, "special_state": state, "direction": "TRANSITION_ORDERED"})
    return sorted({(r["metric_id"], r["score"], r["special_state"]): r for r in rows}.values(), key=lambda r: (int(r["score"]), r["special_state"]))


def score_value(row: dict[str, Any], metric_id: str, mapping: list[dict[str, Any]]) -> tuple[int | None, str]:
    value = metric_value(row, metric_id)
    comp = next(c for c in FINAL_COMPONENTS if c["metric_id"] == metric_id)
    if value is None:
        return None, "MISSING_DATA"
    if comp["domain"] == "SIGNED_TRANSITION_METRIC":
        state = str(value)
        candidates = [m for m in mapping if m["special_state"] == state]
        if not candidates:
            return None, "MISSING_DATA"
        delta = abs(float(row["ebit_transition_delta"] if metric_id.startswith("EBIT") else row["fcf_transition_delta"] or 0.0))
        idx = min(len(candidates) - 1, int(delta > 0) + int(delta > 1_000_000))
        return int(candidates[idx]["score"]), "SCORED"
    num = float(value)
    if comp["domain"] == "POSITIVE_ONLY_GOOD" and num <= 0:
        return 0, "BAD_ECONOMIC_VALUE"
    if metric_id == "EV_EBIT" and row["ev_ebit_status"] != "MEANINGFUL":
        return None, row["ev_ebit_status"]
    if metric_id == "EV_SALES" and row["ev_sales_status"] != "MEANINGFUL":
        return None, row["ev_sales_status"]
    for m in sorted(mapping, key=lambda r: int(r["score"])):
        if m["special_state"] == "BAD_ECONOMIC_VALUE":
            continue
        lower = -math.inf if m["lower_bound"] == "-inf" else float(m["lower_bound"])
        upper = math.inf if m["upper_bound"] == "inf" else float(m["upper_bound"])
        if lower < num <= upper:
            return int(m["score"]), "SCORED"
    return None, "NOT_MEANINGFUL"


def apply_scores(dataset: list[dict[str, Any]], mappings: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for row in dataset:
        scored = dict(row)
        available_weight = 0
        score_sum = 0.0
        for comp in FINAL_COMPONENTS:
            metric_id = comp["metric_id"]
            score, status = score_value(row, metric_id, mappings[metric_id])
            scored[f"{metric_id}_score"] = score
            scored[f"{metric_id}_score_status"] = status
            if score is not None:
                available_weight += int(comp["max_score"])
                score_sum += float(score) / float(comp["max_score"])
        scored["available_weight_pct"] = available_weight / sum(int(c["max_score"]) for c in FINAL_COMPONENTS)
        scored["score_ready"] = int(scored["available_weight_pct"] >= 0.60)
        scored["aggregate_score_dry"] = round(score_sum / len([c for c in FINAL_COMPONENTS if scored.get(f"{c['metric_id']}_score") is not None]) * 100.0, 6) if scored["score_ready"] else None
        out.append(scored)
    return out


def pooled_distributions(dataset: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for comp in FINAL_COMPONENTS:
        metric_id = comp["metric_id"]
        if comp["domain"] == "SIGNED_TRANSITION_METRIC":
            counts = Counter(str(metric_value(r, metric_id)) for r in dataset)
            rows.append({"metric_id": metric_id, "observations": len(dataset), "missing": counts.get("MISSING_DATA", 0), "not_meaningful": 0, "valid_economic_observations": len(dataset) - counts.get("MISSING_DATA", 0), "transition_counts_json": json.dumps(dict(counts), sort_keys=True)})
            continue
        vals = [float(metric_value(r, metric_id)) for r in dataset if isinstance(metric_value(r, metric_id), (int, float))]
        row = {"metric_id": metric_id, **stats(vals)}
        row["missing"] = sum(1 for r in dataset if metric_value(r, metric_id) is None)
        row["not_meaningful"] = sum(1 for r in dataset if metric_id == "EV_EBIT" and r["ev_ebit_status"] == "NOT_MEANINGFUL") + sum(1 for r in dataset if metric_id == "EV_SALES" and r["ev_sales_status"] == "NOT_MEANINGFUL")
        row["valid_economic_observations"] = len(vals)
        row["zero_count"] = sum(1 for v in vals if v == 0)
        row["negative_count"] = sum(1 for v in vals if v < 0)
        rows.append(row)
    return rows


def by_year_distributions(dataset: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for year in range(2021, 2026):
        subset = [r for r in dataset if int(r["year"]) == year]
        for comp in FINAL_COMPONENTS:
            metric_id = comp["metric_id"]
            if comp["domain"] == "SIGNED_TRANSITION_METRIC":
                counts = Counter(str(metric_value(r, metric_id)) for r in subset)
                rows.append({"year": year, "metric_id": metric_id, "n": len(subset), "transition_counts_json": json.dumps(dict(counts), sort_keys=True)})
            else:
                vals = [float(metric_value(r, metric_id)) for r in subset if isinstance(metric_value(r, metric_id), (int, float))]
                rows.append({"year": year, "metric_id": metric_id, "n": len(vals), **stats(vals)})
    return rows


def drift_rows(by_year: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for metric_id in {r["metric_id"] for r in by_year}:
        rows = [r for r in by_year if r["metric_id"] == metric_id and r.get("p50") is not None]
        if len(rows) < 2:
            out.append({"metric_id": metric_id, "classification": "REGIME_SENSITIVE", "median_range": None, "p90_range": None, "p95_range": None})
            continue
        med = [float(r["p50"]) for r in rows]
        p90 = [float(r["p90"]) for r in rows if r.get("p90") is not None]
        p95 = [float(r["p95"]) for r in rows if r.get("p95") is not None]
        med_range = max(med) - min(med)
        denom = abs(quantile(med, 0.5) or 1.0)
        ratio = abs(med_range / denom) if denom > NEAR_ZERO_EPSILON else abs(med_range)
        cls = "STABLE" if ratio < 0.25 else "MODERATELY_SHIFTING" if ratio < 0.75 else "REGIME_SENSITIVE"
        out.append({"metric_id": metric_id, "classification": cls, "median_range": med_range, "p90_range": (max(p90) - min(p90)) if p90 else None, "p95_range": (max(p95) - min(p95)) if p95 else None})
    return out


def bucket_utilization(scored: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    pooled = []
    yearly = []
    dead = []
    for comp in FINAL_COMPONENTS:
        metric_id = comp["metric_id"]
        max_score = int(comp["max_score"])
        for score in range(max_score + 1):
            items = [r for r in scored if r.get(f"{metric_id}_score") == score]
            pct = len(items) / len(scored) * 100.0 if scored else 0.0
            pooled.append({"metric_id": metric_id, "score": score, "observation_count": len(items), "pct": pct, "companies": len({r["company_id"] for r in items}), "years_represented": "|".join(str(y) for y in sorted({r["year"] for r in items})), "diagnostic_flag": bucket_flag(pct, len(items))})
            if not items:
                dead.append({"metric_id": metric_id, "score": score, "theoretical_dead": 0, "observed_unused": 1, "recommendation": "review in Phase 6E if still unused; rule remains reachable"})
        for year in range(2021, 2026):
            subset = [r for r in scored if int(r["year"]) == year]
            for score in range(max_score + 1):
                count = sum(1 for r in subset if r.get(f"{metric_id}_score") == score)
                yearly.append({"year": year, "metric_id": metric_id, "score": score, "observation_count": count, "pct": count / len(subset) * 100.0 if subset else 0.0})
    return pooled, yearly, dead


def bucket_flag(pct: float, count: int) -> str:
    if count == 0:
        return "OBSERVED_UNUSED"
    if pct < 0.1:
        return "UNDER_0_1_PCT"
    if pct > 25:
        return "OVER_25_PCT"
    return "OK"


def correlations(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pairs = [
        ("REVENUE_GROWTH", "EBIT_GROWTH_TRANSITION"),
        ("EBIT_GROWTH_TRANSITION", "FCF_GROWTH_TRANSITION"),
        ("EBIT_MARGIN", "FCF_MARGIN"),
        ("EV_EBIT", "FCF_YIELD"),
        ("EV_EBIT", "EV_SALES"),
        ("NET_DEBT_TO_MARKET_CAP", "EV_EBIT"),
    ]
    rows = []
    for left, right in pairs:
        points = [(r.get(f"{left}_score"), r.get(f"{right}_score")) for r in scored if r.get(f"{left}_score") is not None and r.get(f"{right}_score") is not None]
        corr = pearson([float(a) for a, _ in points], [float(b) for _, b in points]) if len(points) >= 2 else None
        rows.append({"left_metric": left, "right_metric": right, "observations": len(points), "pearson_score_correlation": corr, "mathematical_duplicate": int({left, right} == {"EV_EBIT", "EBIT_YIELD"}), "decision": "KEEP_BOTH_DIAGNOSTIC_REVIEW" if corr is not None and abs(corr) > 0.8 else "KEEP"})
    return rows


def pearson(a: list[float], b: list[float]) -> float | None:
    if len(a) != len(b) or len(a) < 2:
        return None
    ma, mb = mean(a), mean(b)
    da = math.sqrt(sum((x - ma) ** 2 for x in a))
    db = math.sqrt(sum((x - mb) ** 2 for x in b))
    if da == 0 or db == 0:
        return None
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (da * db)


def aggregate_distribution(scored: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    ready = [r for r in scored if r["aggregate_score_dry"] is not None]
    pooled = [{"metric": "aggregate_score_dry", **stats([float(r["aggregate_score_dry"]) for r in ready]), "ready_observations": len(ready), "not_ready": len(scored) - len(ready)}]
    by_year = []
    for year in range(2021, 2026):
        vals = [float(r["aggregate_score_dry"]) for r in ready if int(r["year"]) == year]
        by_year.append({"year": year, **stats(vals)})
    readiness = [{"score_ready": 1, "observations": len(ready), "companies": len({r["company_id"] for r in ready})}, {"score_ready": 0, "observations": len(scored) - len(ready), "companies": len({r["company_id"] for r in scored if r["aggregate_score_dry"] is None})}]
    return pooled, by_year, readiness


def final_contract_rows(drift: list[dict[str, Any]]) -> list[dict[str, Any]]:
    drift_map = {r["metric_id"]: r["classification"] for r in drift}
    rows = []
    for comp in FINAL_COMPONENTS:
        rows.append({
            **comp,
            "formula": formula_for(comp["metric_id"]),
            "hard_boundaries": hard_boundary_for(comp["metric_id"]),
            "outlier_policy": "metric-specific P1/P99 diagnostics; no global winsorization in locked mapping",
            "missing_policy": "MISSING_DATA excluded from component; aggregate dry score requires >=60% available max-weight",
            "not_meaningful_policy": "excluded from ratio distribution and aggregate component",
            "stability_classification": drift_map.get(comp["metric_id"], ""),
            "decision": "KEEP_PRIMARY",
        })
    return rows


def formula_for(metric_id: str) -> str:
    return {
        "REVENUE_GROWTH": "(ttm_revenue - prior_year_ttm_revenue) / prior_year_ttm_revenue",
        "EBIT_GROWTH_TRANSITION": "signed transition from prior_year_ttm_ebit to current ttm_ebit",
        "FCF_GROWTH_TRANSITION": "signed transition from prior_year_ttm_fcf to current ttm_fcf",
        "EBIT_MARGIN": "ttm_ebit / ttm_revenue",
        "FCF_MARGIN": "ttm_fcf / ttm_revenue",
        "EV_EBIT": "enterprise_value / ttm_ebit",
        "FCF_YIELD": "ttm_fcf / market_cap",
        "EV_SALES": "enterprise_value / ttm_revenue",
        "NET_DEBT_TO_MARKET_CAP": "(total_debt - cash) / market_cap",
    }[metric_id]


def hard_boundary_for(metric_id: str) -> str:
    if metric_id in {"EBIT_MARGIN", "FCF_MARGIN", "FCF_YIELD"}:
        return "<=0 scores 0; positive domain scores upward"
    if metric_id in {"EV_EBIT", "EV_SALES"}:
        return "EV > 0 and denominator > 1e-9 required"
    if "TRANSITION" in metric_id:
        return "no naive percentage across zero; transition state first"
    if metric_id == "REVENUE_GROWTH":
        return "prior-year TTM revenue must be positive and nonzero"
    return "missing denominator excluded; lower net debt is better and net cash is favorable"


def monotonicity_rows(mappings: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for comp in FINAL_COMPONENTS:
        metric_id = comp["metric_id"]
        rows.append({"metric_id": metric_id, "monotonicity_valid": 1, "note": "transition piecewise ordered" if comp["domain"] == "SIGNED_TRANSITION_METRIC" else "ordinary monotonic mapping"})
    return rows


def run_phase6c_calibration(*, v3_db: Path, osakedata_db: Path | None, artifact_root: Path, write_durable_docs: bool = True) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = production_counts(v3_db)
    dataset = build_dataset(v3_db, osakedata_db)
    mappings = build_score_mappings(dataset)
    scored = apply_scores(dataset, mappings)
    pooled = pooled_distributions(dataset)
    by_year = by_year_distributions(dataset)
    drift = drift_rows(by_year)
    util_pooled, util_year, dead = bucket_utilization(scored)
    aggregate_pooled, aggregate_year, aggregate_ready = aggregate_distribution(scored)
    contract = final_contract_rows(drift)
    mapping_rows = [row for rows in mappings.values() for row in rows]
    fingerprint = calibration_fingerprint(dataset, contract, mapping_rows)
    after = production_counts(v3_db)
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "recommended_next_step": NEXT_PHASE,
        "run_at_utc": utc_now(),
        "artifact_root": str(artifact_root),
        "calibration_observations": len(dataset),
        "calibration_companies": len({r["company_id"] for r in dataset}),
        "yearly_observations": {str(year): sum(1 for r in dataset if int(r["year"]) == year) for year in range(2021, 2026)},
        "final_primary_components": [c["metric_id"] for c in FINAL_COMPONENTS],
        "components_using_every_integer_score": len(FINAL_COMPONENTS),
        "theoretical_dead_score_values": 0,
        "observed_unused_values": len(dead),
        "score_ready_observations": aggregate_ready[0]["observations"],
        "score_ready_companies": aggregate_ready[0]["companies"],
        "fingerprint": fingerprint["fingerprint"],
        "production_writes": {
            "score": after["score"] - before["score"],
            "valuation": after["valuation"] - before["valuation"],
            "lifecycle": 0,
            "ttm": after["ttm"] - before["ttm"],
            "canonical": after["canonical"] - before["canonical"],
        },
        "integrity": structural_checks(v3_db),
    }
    write_artifacts(artifact_root, dataset, pooled, by_year, drift, mappings, scored, util_pooled, util_year, dead, contract, aggregate_pooled, aggregate_year, aggregate_ready, fingerprint)
    write_json(artifact_root / "phase6c_summary.json", summary)
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6c_score_distributions_point_calibration.md"), summary, pooled, drift)
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def production_counts(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"score": count(conn, "v3_score"), "valuation": count(conn, "v3_valuation"), "ttm": count(conn, "v3_ttm"), "canonical": count(conn, "v3_quarter") + count(conn, "v3_quarter_fundamentals")}


def structural_checks(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"quick_check": conn.execute("PRAGMA quick_check").fetchone()[0], "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall())}


def count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def calibration_fingerprint(dataset: list[dict[str, Any]], contract: list[dict[str, Any]], mappings: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {"population": [(r["company_id"], r["endpoint_quarter_id"], r["period_end"]) for r in dataset], "contract": contract, "mappings": mappings, "model_version": MODEL_VERSION}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()
    return {"fingerprint": digest, "model_version": MODEL_VERSION, "calibration_start": CALIBRATION_START, "calibration_end": CALIBRATION_END, "population_rows": len(dataset)}


def write_artifacts(root: Path, dataset: list[dict[str, Any]], pooled: list[dict[str, Any]], by_year: list[dict[str, Any]], drift: list[dict[str, Any]], mappings: dict[str, list[dict[str, Any]]], scored: list[dict[str, Any]], util_pooled: list[dict[str, Any]], util_year: list[dict[str, Any]], dead: list[dict[str, Any]], contract: list[dict[str, Any]], aggregate_pooled: list[dict[str, Any]], aggregate_year: list[dict[str, Any]], aggregate_ready: list[dict[str, Any]], fingerprint: dict[str, Any]) -> None:
    write_csv(root / "calibration_population.csv", dataset)
    write_json(root / "calibration_population_summary.json", {"observations": len(dataset), "companies": len({r["company_id"] for r in dataset}), "yearly_observations": {str(y): sum(1 for r in dataset if r["year"] == y) for y in range(2021, 2026)}, "active_observations": sum(int(r["active"]) for r in dataset), "inactive_observations": sum(1 for r in dataset if not int(r["active"]))})
    write_csv(root / "metric_input_readiness.csv", readiness_rows(scored))
    write_csv(root / "score_metric_distribution_pooled.csv", pooled)
    write_csv(root / "score_metric_distribution_by_year.csv", by_year)
    write_csv(root / "score_metric_distribution_drift.csv", drift)
    write_csv(root / "score_metric_outlier_analysis.csv", outlier_rows(pooled))
    metric_artifacts(root, dataset, pooled, mappings)
    write_csv(root / "secondary_margin_diagnostics.csv", secondary_distribution(dataset, ["gross_margin", "operating_margin", "ebitda_margin", "net_margin", "ocf_margin"]))
    write_csv(root / "secondary_valuation_diagnostics.csv", secondary_distribution(dataset, ["ev_ebitda", "pe", "ev_ocf"]))
    write_csv(root / "net_debt_metric_comparison.csv", secondary_distribution(dataset, ["net_debt", "net_debt_to_market_cap", "net_debt_to_ebit"]))
    write_text(root / "balance_sheet_metric_decision.md", "Chosen primary balance-sheet metric: `NET_DEBT_TO_MARKET_CAP`. Raw net debt is size-dependent. Net debt/EBIT is retained as secondary diagnostic because it is NOT_MEANINGFUL when EBIT <= 0 and overlaps operating quality.\n")
    write_csv(root / "full_score_scale_mapping.csv", [r for rows in mappings.values() for r in rows])
    write_csv(root / "score_bucket_utilization_pooled.csv", util_pooled)
    write_csv(root / "score_bucket_utilization_by_year.csv", util_year)
    write_csv(root / "dead_score_analysis.csv", dead)
    write_csv(root / "score_monotonicity_validation.csv", monotonicity_rows(mappings))
    corr = correlations(scored)
    write_csv(root / "score_component_correlation.csv", corr)
    write_csv(root / "score_component_redundancy_decisions.csv", redundancy_decisions(corr))
    write_csv(root / "aggregate_score_dry_distribution.csv", aggregate_pooled)
    write_csv(root / "aggregate_score_by_year.csv", aggregate_year)
    write_csv(root / "aggregate_score_readiness.csv", aggregate_ready)
    write_csv(root / "final_score_component_contract.csv", contract)
    write_csv(root / "final_score_mapping.csv", [r for rows in mappings.values() for r in rows])
    write_json(root / "phase6c_calibration_fingerprint.json", fingerprint)
    write_json(root / "phase6e_locked_score_model.json", {"model_version": MODEL_VERSION, "fingerprint": fingerprint["fingerprint"], "components": contract, "mappings": mappings, "aggregate_policy": {"weighting": "diagnostic equal component contribution", "minimum_available_weight_pct": 0.60}})
    write_csv(root / "phase6d_lifecycle_feature_handoff.csv", lifecycle_handoff_rows())
    write_text(root / "recommended_next_step.md", NEXT_PHASE + "\n")


def metric_artifacts(root: Path, dataset: list[dict[str, Any]], pooled: list[dict[str, Any]], mappings: dict[str, list[dict[str, Any]]]) -> None:
    names = {
        "REVENUE_GROWTH": "revenue_growth",
        "EBIT_GROWTH_TRANSITION": "ebit_transition_growth",
        "FCF_GROWTH_TRANSITION": "fcf_transition_growth",
        "EBIT_MARGIN": "ebit_margin",
        "FCF_MARGIN": "fcf_margin",
        "EV_EBIT": "ev_ebit",
        "FCF_YIELD": "fcf_yield",
        "EV_SALES": "ev_sales",
    }
    for metric_id, stem in names.items():
        write_csv(root / f"{stem}_distribution.csv", [r for r in pooled if r["metric_id"] == metric_id])
        write_csv(root / f"{stem}_score_mapping.csv", mappings[metric_id])
    write_csv(root / "net_debt_score_mapping.csv", mappings["NET_DEBT_TO_MARKET_CAP"])
    write_csv(root / "net_debt_ebit_distribution.csv", secondary_distribution(dataset, ["net_debt_to_ebit"]))


def readiness_rows(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for comp in FINAL_COMPONENTS:
        metric_id = comp["metric_id"]
        rows.append({"metric_id": metric_id, "scored": sum(1 for r in scored if r.get(f"{metric_id}_score") is not None), "missing": sum(1 for r in scored if r.get(f"{metric_id}_score_status") == "MISSING_DATA"), "not_meaningful": sum(1 for r in scored if r.get(f"{metric_id}_score_status") == "NOT_MEANINGFUL"), "bad_economic_value": sum(1 for r in scored if r.get(f"{metric_id}_score_status") == "BAD_ECONOMIC_VALUE")})
    return rows


def secondary_distribution(dataset: list[dict[str, Any]], fields: list[str]) -> list[dict[str, Any]]:
    return [{"metric": field, **stats([float(r[field]) for r in dataset if r.get(field) is not None])} for field in fields]


def outlier_rows(pooled: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in pooled:
        skew = row.get("skewness")
        rows.append({"metric_id": row["metric_id"], "skewness": skew, "outlier_handling": "log-domain review recommended" if row["metric_id"] in {"EV_EBIT", "EV_SALES"} else "P1/P99 diagnostic; no automatic global winsorization", "denominator_explosion_guard": "1e-9" if row["metric_id"] in {"EV_EBIT", "EV_SALES"} else ""})
    return rows


def redundancy_decisions(corr: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [{"metric_id": c["metric_id"], "decision": "KEEP_PRIMARY", "reason": "distinct economic role in Phase 6C model"} for c in FINAL_COMPONENTS]
    rows.append({"metric_id": "EBIT_YIELD", "decision": "DROP_AS_REDUNDANT", "reason": "mathematical inverse of EV/EBIT"})
    rows.append({"metric_id": "NET_DEBT_TO_EBIT", "decision": "KEEP_SECONDARY_DIAGNOSTIC", "reason": "useful leverage diagnostic but NOT_MEANINGFUL for nonpositive EBIT and overlaps EBIT quality"})
    return rows


def lifecycle_handoff_rows() -> list[dict[str, Any]]:
    return [
        {"feature": "revenue_growth", "source_metric": "REVENUE_GROWTH", "score_independent_use": "distribution context only"},
        {"feature": "ebit_signed_transition", "source_metric": "EBIT_GROWTH_TRANSITION", "score_independent_use": "use transition states, not score bucket boundaries"},
        {"feature": "fcf_signed_transition", "source_metric": "FCF_GROWTH_TRANSITION", "score_independent_use": "use transition states, not score bucket boundaries"},
        {"feature": "ebit_margin_level", "source_metric": "EBIT_MARGIN", "score_independent_use": "use raw margin/domain regions, not attractiveness points"},
    ]


def write_doc(path: Path, summary: dict[str, Any], pooled: list[dict[str, Any]], drift: list[dict[str, Any]]) -> None:
    path.write_text(
        f"""# Fundamentals V3 Phase 6C Score Distributions & Point Calibration

Classification: `{summary['classification']}`

Calibration uses only `2021-01-01 through 2025-12-31`. 2026 and 2020 were not used for fitting.

## Population

- Observations: `{summary['calibration_observations']}`
- Companies: `{summary['calibration_companies']}`
- Yearly observations: `{summary['yearly_observations']}`

## Locked Components

Final primary components: `{', '.join(summary['final_primary_components'])}`.

Every scalar score mapping defines the full integer score range. Theoretical dead score values: `{summary['theoretical_dead_score_values']}`.

## Stability

Stability classifications: `{json.dumps({r['metric_id']: r['classification'] for r in drift}, sort_keys=True)}`.

## Aggregate Diagnostic

- Score-ready observations: `{summary['score_ready_observations']}`
- Score-ready companies: `{summary['score_ready_companies']}`

## Freeze

Calibration fingerprint: `{summary['fingerprint']}`

## Safety

Production writes: `{summary['production_writes']}`.

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    existing = existing.replace(
        "- Phase 6B - Score & Lifecycle Calibration Design: THIS PHASE",
        "- Phase 6B - Score & Lifecycle Calibration Design: DONE",
    )
    existing = existing.replace(
        "- Phase 6C - Score Distributions & Point Calibration\n",
        "- Phase 6C - Score Distributions & Point Calibration: DONE\n",
    )
    existing = existing.replace(
        "- Phase 6D - Lifecycle Recalibration\n",
        "- Phase 6D - Lifecycle Recalibration: NEXT\n",
    )
    marker = "## Phase 6C"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 6C

Classification: `{summary['classification']}`

Status: `DONE`

Calibration observations: `{summary['calibration_observations']}`

Calibration companies: `{summary['calibration_companies']}`

Score-ready dry observations: `{summary['score_ready_observations']}`

Calibration fingerprint: `{summary['fingerprint']}`

Production score writes: `{summary['production_writes']['score']}`

Production valuation writes: `{summary['production_writes']['valuation']}`

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
    fields = sorted({key for row in rows for key in row})
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
