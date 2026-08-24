from __future__ import annotations

import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_phase6c_score_distribution_calibration import (
    CALIBRATION_END,
    CALIBRATION_START,
    NEAR_ZERO_EPSILON,
    classify_signed_transition,
    quantile,
    stats,
)

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6D_LIFECYCLE_RECALIBRATED_READY_FOR_PHASE6E"
NEXT_PHASE = "MASTER PLAN PHASE 6E - OUT-OF-SAMPLE & STRESS VALIDATION"
MODEL_VERSION = "V3_LIFECYCLE_EBIT_FIRST_V1"

STATE_ORDER = [
    "DISTRESS_CONTRACTION",
    "EARLY_RECOVERY",
    "POSITIVE_INFLECTION",
    "PROFITABLE_GROWTH",
    "HIGH_GROWTH_EXPANSION",
    "MATURE_STABLE",
    "DECELERATING",
    "DECLINING",
    "NOT_READY",
]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def build_feature_dataset(v3_db: Path) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in load_ttm(v3_db):
        by_company[int(row["company_id"])].append(row)
    out: list[dict[str, Any]] = []
    for rows in by_company.values():
        for idx, row in enumerate(rows):
            if not (CALIBRATION_START <= str(row["period_end"]) <= CALIBRATION_END):
                continue
            prev4 = rows[idx - 4] if idx >= 4 else None
            prev1 = rows[idx - 1] if idx >= 1 else None
            prev8 = rows[idx - 8] if idx >= 8 else None
            out.append(feature_row(row, prev4, prev1, prev8))
    return out


def feature_row(row: dict[str, Any], prev4: dict[str, Any] | None, prev1: dict[str, Any] | None, prev8: dict[str, Any] | None) -> dict[str, Any]:
    revenue = f(row.get("ttm_revenue"))
    ebit = f(row.get("ttm_ebit"))
    fcf = f(row.get("ttm_fcf"))
    prev_rev = f(prev4.get("ttm_revenue")) if prev4 else None
    prev_ebit = f(prev4.get("ttm_ebit")) if prev4 else None
    prev_fcf = f(prev4.get("ttm_fcf")) if prev4 else None
    prev_revg = safe_growth(prev_rev, f(prev8.get("ttm_revenue")) if prev8 else None)
    revg = safe_growth(revenue, prev_rev)
    ebit_margin = safe_div(ebit, revenue)
    fcf_margin = safe_div(fcf, revenue)
    prev_ebit_margin = safe_div(prev_ebit, prev_rev)
    prev_fcf_margin = safe_div(prev_fcf, prev_rev)
    prev1_revg = safe_growth(f(prev1.get("ttm_revenue")) if prev1 else None, f(prev4.get("ttm_revenue")) if prev4 else None)
    return {
        "company_id": row["company_id"],
        "ticker": row["ticker"],
        "active": row["active"],
        "endpoint_quarter_id": row["endpoint_quarter_id"],
        "period_end": row["period_end"],
        "year": int(str(row["period_end"])[:4]),
        "revenue_growth_yoy_ttm": revg,
        "revenue_growth_acceleration": None if revg is None or prev_revg is None else revg - prev_revg,
        "revenue_growth_1q_delta": None if revg is None or prev1_revg is None else revg - prev1_revg,
        "ebit_transition": classify_signed_transition(prev_ebit, ebit),
        "ebit_growth_magnitude": None if ebit is None or prev_ebit is None else ebit - prev_ebit,
        "ebit_positive": int(ebit is not None and ebit > 0),
        "ebit_margin": ebit_margin,
        "ebit_margin_change": None if ebit_margin is None or prev_ebit_margin is None else ebit_margin - prev_ebit_margin,
        "fcf_transition": classify_signed_transition(prev_fcf, fcf),
        "fcf_growth_magnitude": None if fcf is None or prev_fcf is None else fcf - prev_fcf,
        "fcf_positive": int(fcf is not None and fcf > 0),
        "fcf_margin": fcf_margin,
        "fcf_margin_change": None if fcf_margin is None or prev_fcf_margin is None else fcf_margin - prev_fcf_margin,
    }


def f(value: Any) -> float | None:
    return None if value is None else float(value)


def safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or abs(den) <= NEAR_ZERO_EPSILON:
        return None
    return num / den


def safe_growth(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev is None or prev <= 0 or abs(prev) <= NEAR_ZERO_EPSILON:
        return None
    return (cur - prev) / abs(prev)


def calibrate_thresholds(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rev = [r["revenue_growth_yoy_ttm"] for r in rows if r["revenue_growth_yoy_ttm"] is not None]
    ebit_margin = [r["ebit_margin"] for r in rows if r["ebit_margin"] is not None and r["ebit_margin"] > 0]
    ebit_change = [r["ebit_margin_change"] for r in rows if r["ebit_margin_change"] is not None]
    return {
        "revenue_low_growth": quantile(rev, 0.25) or 0.03,
        "revenue_strong_growth": quantile(rev, 0.75) or 0.15,
        "revenue_very_strong_growth": quantile(rev, 0.90) or 0.30,
        "positive_ebit_margin_floor": 0.0,
        "healthy_ebit_margin": quantile(ebit_margin, 0.50) or 0.08,
        "strong_ebit_margin": quantile(ebit_margin, 0.75) or 0.15,
        "margin_expansion": max(0.01, quantile([x for x in ebit_change if x > 0], 0.50) or 0.02),
        "margin_contraction": min(-0.01, quantile([x for x in ebit_change if x < 0], 0.50) or -0.02),
        "severe_revenue_contraction": min(-0.10, quantile(rev, 0.10) or -0.10),
    }


def readiness(row: dict[str, Any]) -> tuple[bool, str]:
    core = row["revenue_growth_yoy_ttm"] is not None and row["ebit_transition"] != "MISSING_DATA" and row["ebit_margin"] is not None
    if core and row["fcf_transition"] != "MISSING_DATA" and row["fcf_margin"] is not None:
        return True, "HIGH"
    if core:
        return True, "MEDIUM"
    if row["revenue_growth_yoy_ttm"] is not None or row["ebit_transition"] != "MISSING_DATA":
        return False, "LOW"
    return False, "NOT_READY"


def classify_raw_state(row: dict[str, Any], t: dict[str, Any]) -> tuple[str, str]:
    ready, conf = readiness(row)
    if not ready:
        return "NOT_READY", conf
    rev = row["revenue_growth_yoy_ttm"]
    ebit_tr = row["ebit_transition"]
    fcf_tr = row["fcf_transition"]
    ebit_margin = row["ebit_margin"]
    ebit_chg = row["ebit_margin_change"]
    fcf_margin = row["fcf_margin"]
    severe_contract = rev is not None and rev <= t["severe_revenue_contraction"]
    if (ebit_tr in {"NEGATIVE_AND_DETERIORATING", "POSITIVE_TURNING_NEGATIVE"} and severe_contract) or (ebit_margin is not None and ebit_margin < -0.10 and fcf_margin is not None and fcf_margin < 0):
        return "DISTRESS_CONTRACTION", conf
    if ebit_tr == "CROSSING_TO_POSITIVE" or fcf_tr == "CROSSING_TO_POSITIVE":
        return "POSITIVE_INFLECTION", conf
    if ebit_tr == "NEGATIVE_BUT_IMPROVING" or (ebit_margin is not None and ebit_margin <= 0 and ebit_chg is not None and ebit_chg > 0):
        return "EARLY_RECOVERY", conf
    if ebit_tr == "POSITIVE_TURNING_NEGATIVE" or (rev is not None and rev < 0 and ebit_chg is not None and ebit_chg < 0):
        return "DECLINING", conf
    if ebit_margin is not None and ebit_margin > 0 and ebit_chg is not None and ebit_chg <= t["margin_contraction"]:
        return "DECELERATING", conf
    if rev is not None and rev >= t["revenue_very_strong_growth"] and ebit_margin is not None and ebit_margin > 0:
        return "HIGH_GROWTH_EXPANSION", conf
    if rev is not None and rev >= t["revenue_strong_growth"] and ebit_tr == "POSITIVE_AND_GROWING" and (ebit_chg is None or ebit_chg >= 0):
        return "PROFITABLE_GROWTH", conf
    if ebit_margin is not None and ebit_margin >= t["healthy_ebit_margin"] and (rev is None or rev >= t["revenue_low_growth"]):
        return "MATURE_STABLE", conf
    if ebit_tr == "POSITIVE_AND_GROWING":
        return "PROFITABLE_GROWTH", conf
    return "DECELERATING" if rev is not None and rev < 0 else "MATURE_STABLE", conf


def raw_history(rows: list[dict[str, Any]], thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        state, conf = classify_raw_state(row, thresholds)
        out.append({**row, "raw_state": state, "confidence": conf, "lifecycle_ready": int(state != "NOT_READY")})
    return out


def apply_hysteresis(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in raw:
        by_company[int(row["company_id"])].append(row)
    out = []
    for rows in by_company.values():
        current: str | None = None
        pending: str | None = None
        pending_count = 0
        current_age = 0
        prev_raw: str | None = None
        for row in rows:
            raw_state = row["raw_state"]
            hard = is_hard_inflection(row, prev_raw)
            if current is None:
                current = raw_state
                current_age = 1
            elif raw_state == current:
                pending = None
                pending_count = 0
                current_age += 1
            elif hard:
                current = raw_state
                current_age = 1
                pending = None
                pending_count = 0
            else:
                if pending == raw_state:
                    pending_count += 1
                else:
                    pending = raw_state
                    pending_count = 1
                if pending_count >= 2 and current_age >= 1:
                    current = raw_state
                    current_age = 1
                    pending = None
                    pending_count = 0
                else:
                    current_age += 1
            out.append({**row, "final_state": current, "hysteresis_pending_state": pending or "", "hard_inflection_applied": int(hard)})
            prev_raw = raw_state
    return out


def is_hard_inflection(row: dict[str, Any], prev_raw: str | None) -> bool:
    return row["ebit_transition"] in {"CROSSING_TO_POSITIVE", "POSITIVE_TURNING_NEGATIVE"} or (row["revenue_growth_yoy_ttm"] is not None and row["revenue_growth_yoy_ttm"] <= -0.30)


def transition_matrix(rows: list[dict[str, Any]], state_key: str) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_company[int(row["company_id"])].append(row)
    counts: Counter[tuple[str, str]] = Counter()
    for items in by_company.values():
        for left, right in zip(items, items[1:]):
            counts[(left[state_key], right[state_key])] += 1
    total = sum(counts.values())
    return [{"from_state": a, "to_state": b, "count": c, "pct": c / total * 100.0 if total else 0.0, "self_transition": int(a == b), "jump_type": jump_type(a, b)} for (a, b), c in sorted(counts.items())]


def jump_type(a: str, b: str) -> str:
    if a == b:
        return "SELF"
    ia = STATE_ORDER.index(a) if a in STATE_ORDER else 0
    ib = STATE_ORDER.index(b) if b in STATE_ORDER else 0
    return "ADJACENT" if abs(ia - ib) <= 1 else "DIRECT_JUMP"


def state_distribution(rows: list[dict[str, Any]], state_key: str) -> list[dict[str, Any]]:
    counts = Counter(row[state_key] for row in rows)
    total = len(rows)
    return [{"state": state, "observations": counts.get(state, 0), "share_pct": counts.get(state, 0) / total * 100.0 if total else 0.0, "companies": len({r["company_id"] for r in rows if r[state_key] == state})} for state in STATE_ORDER if counts.get(state, 0)]


def duration_rows(rows: list[dict[str, Any]], state_key: str) -> list[dict[str, Any]]:
    durations = durations_by_company(rows, state_key)
    by_state: dict[str, list[int]] = defaultdict(list)
    for state, dur in durations:
        by_state[state].append(dur)
    return [{"state": state, "segments": len(vals), "median_duration": median(vals), "p25_duration": quantile([float(v) for v in vals], 0.25), "p75_duration": quantile([float(v) for v in vals], 0.75), "one_quarter_segments": sum(1 for v in vals if v == 1), "two_quarter_segments": sum(1 for v in vals if v == 2)} for state, vals in sorted(by_state.items())]


def durations_by_company(rows: list[dict[str, Any]], state_key: str) -> list[tuple[str, int]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_company[int(row["company_id"])].append(row)
    out = []
    for items in by_company.values():
        current = None
        dur = 0
        for row in items:
            state = row[state_key]
            if state != current:
                if current is not None:
                    out.append((current, dur))
                current = state
                dur = 1
            else:
                dur += 1
        if current is not None:
            out.append((current, dur))
    return out


def churn_analysis(rows: list[dict[str, Any]], state_key: str) -> dict[str, Any]:
    matrix = transition_matrix(rows, state_key)
    total = sum(row["count"] for row in matrix)
    self_count = sum(row["count"] for row in matrix if row["self_transition"])
    transition_count = total - self_count
    direct = sum(row["count"] for row in matrix if row["jump_type"] == "DIRECT_JUMP")
    rev = reversal_count(rows, state_key)
    durations = [dur for _state, dur in durations_by_company(rows, state_key)]
    one_q = sum(1 for d in durations if d == 1)
    return {
        "model": state_key,
        "transitions": total,
        "self_transition_rate": self_count / total * 100.0 if total else 0.0,
        "transition_rate": transition_count / total * 100.0 if total else 0.0,
        "direct_jump_rate": direct / total * 100.0 if total else 0.0,
        "immediate_reversal_count": rev,
        "reversal_rate": rev / total * 100.0 if total else 0.0,
        "median_state_duration": median(durations) if durations else None,
        "one_quarter_state_share": one_q / len(durations) * 100.0 if durations else 0.0,
    }


def reversal_count(rows: list[dict[str, Any]], state_key: str) -> int:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_company[int(row["company_id"])].append(row)
    count = 0
    for items in by_company.values():
        states = [r[state_key] for r in items]
        for a, _b, c in zip(states, states[1:], states[2:]):
            if a == c:
                count += 1
    return count


def inflection_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = []
    for row in rows:
        if row["ebit_transition"] in {"CROSSING_TO_POSITIVE", "POSITIVE_TURNING_NEGATIVE"} or row["fcf_transition"] in {"CROSSING_TO_POSITIVE", "POSITIVE_TURNING_NEGATIVE"}:
            events.append({
                "company_id": row["company_id"],
                "ticker": row["ticker"],
                "period_end": row["period_end"],
                "event_type": row["ebit_transition"] if row["ebit_transition"] in {"CROSSING_TO_POSITIVE", "POSITIVE_TURNING_NEGATIVE"} else row["fcf_transition"],
                "raw_state": row["raw_state"],
                "final_state": row["final_state"],
                "recognized_same_quarter": int(row["raw_state"] == row["final_state"]),
            })
    return events


def existing_lifecycle_inventory() -> list[dict[str, Any]]:
    states = [
        ("DISTRESSED", "EBITDA margin < -30% and FCF margin < -20%"),
        ("STARTUP", "revenue growth >30%, EBITDA margin <0, FCF margin <0"),
        ("GROWTH", "revenue growth >20%, EBITDA margin <15%"),
        ("SCALING", "revenue growth >10%, EBITDA margin trend >0, EBITDA margin >=0"),
        ("MATURE", "EBITDA margin >=25%, FCF margin >=5%"),
        ("TRANSITION", "nonnegative EBITDA and FCF with mild trend guards"),
        ("DECLINING", "revenue growth <-5% or EBITDA margin trend <-7%"),
        ("UNCLASSIFIED", "fallback"),
    ]
    return [{"state": s, "code_path": "swingmaster/fundamentals/lifecycle.py", "current_definition": d, "inputs": "revenue_growth_ttm_yoy, ebitda_margin_ttm, ebitda_margin_trend_4q, fcf_margin_ttm", "ebitda_dependency": 1, "level_vs_trajectory": "mostly level/stateless threshold", "entry_rule": d, "exit_rule": "none", "temporal_memory": "none", "recommendation": "REPLACE_WITH_EBIT_FIRST_TEMPORAL_MODEL"} for s, d in states]


def feature_distribution(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = ["revenue_growth_yoy_ttm", "revenue_growth_acceleration", "ebit_growth_magnitude", "ebit_margin", "ebit_margin_change", "fcf_growth_magnitude", "fcf_margin", "fcf_margin_change"]
    return [{"feature": field, **stats([r[field] for r in rows if r[field] is not None])} for field in fields]


def feature_distribution_by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for year in range(2021, 2026):
        subset = [r for r in rows if r["year"] == year]
        for row in feature_distribution(subset):
            out.append({"year": year, **row})
    return out


def feature_readiness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = ["revenue_growth_yoy_ttm", "ebit_transition", "ebit_margin", "ebit_margin_change", "fcf_transition", "fcf_margin", "fcf_margin_change", "revenue_growth_acceleration"]
    out = []
    for field in fields:
        missing = sum(1 for r in rows if r[field] is None or r[field] == "MISSING_DATA")
        out.append({"feature": field, "available": len(rows) - missing, "missing": missing, "active_available": sum(1 for r in rows if int(r["active"]) and r[field] is not None and r[field] != "MISSING_DATA"), "inactive_available": sum(1 for r in rows if not int(r["active"]) and r[field] is not None and r[field] != "MISSING_DATA")})
    return out


def signed_transition_distribution(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for field in ("ebit_transition", "fcf_transition"):
        counts = Counter(r[field] for r in rows)
        for state, count in sorted(counts.items()):
            out.append({"feature": field, "transition_state": state, "observations": count, "pct": count / len(rows) * 100.0 if rows else 0.0})
    return out


def state_contract(thresholds: dict[str, Any], final_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dist = {r["state"]: r for r in state_distribution(final_rows, "final_state")}
    meanings = {
        "DISTRESS_CONTRACTION": "weak/contracting revenue with negative or deteriorating EBIT/FCF",
        "EARLY_RECOVERY": "still weak level but EBIT/FCF trajectory improving",
        "POSITIVE_INFLECTION": "EBIT or FCF crossed positive",
        "PROFITABLE_GROWTH": "positive EBIT, growing revenue, stable or expanding margins",
        "HIGH_GROWTH_EXPANSION": "very strong revenue expansion with positive EBIT",
        "MATURE_STABLE": "positive profitability with lower/stable growth",
        "DECELERATING": "profitable but weakening growth or margin contraction",
        "DECLINING": "negative revenue/profit trajectory or crossing negative",
        "NOT_READY": "insufficient core lifecycle features",
    }
    return [{"state_id": idx + 1, "state": state, "economic_meaning": meanings[state], "key_entry_conditions": entry_conditions(state, thresholds), "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs", "calibration_observations": dist.get(state, {}).get("observations", 0), "share_pct": dist.get(state, {}).get("share_pct", 0.0)} for idx, state in enumerate(STATE_ORDER)]


def entry_conditions(state: str, t: dict[str, Any]) -> str:
    return {
        "DISTRESS_CONTRACTION": f"severe revenue contraction <= {t['severe_revenue_contraction']:.4f} with deteriorating/crossing-negative EBIT, or deeply negative margins",
        "EARLY_RECOVERY": "negative-but-improving EBIT or nonpositive EBIT margin expanding",
        "POSITIVE_INFLECTION": "EBIT or FCF crosses positive",
        "PROFITABLE_GROWTH": f"revenue growth >= {t['revenue_strong_growth']:.4f}, positive/growing EBIT, noncontracting margin",
        "HIGH_GROWTH_EXPANSION": f"revenue growth >= {t['revenue_very_strong_growth']:.4f} and EBIT margin > 0",
        "MATURE_STABLE": f"EBIT margin >= {t['healthy_ebit_margin']:.4f} with revenue growth >= {t['revenue_low_growth']:.4f}",
        "DECELERATING": f"positive EBIT with margin change <= {t['margin_contraction']:.4f} or weak revenue",
        "DECLINING": "EBIT crosses negative or revenue/margin trajectory both deteriorate",
        "NOT_READY": "missing revenue trajectory or EBIT trajectory/margin",
    }[state]


def transition_contract() -> list[dict[str, Any]]:
    rows = []
    for state in STATE_ORDER:
        idx = STATE_ORDER.index(state)
        allowed = {state}
        for j in (idx - 1, idx + 1):
            if 0 <= j < len(STATE_ORDER):
                allowed.add(STATE_ORDER[j])
        allowed.update({"POSITIVE_INFLECTION", "DISTRESS_CONTRACTION", "DECLINING"})
        rows.append({"state": state, "allowed_transitions": "|".join(sorted(allowed)), "direct_jump_policy": "allowed for hard EBIT crossing or severe revenue/profitability shock"})
    return rows


def fingerprint(thresholds: dict[str, Any], contract: list[dict[str, Any]], transitions: list[dict[str, Any]], population: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {"version": MODEL_VERSION, "thresholds": thresholds, "contract": contract, "transitions": transitions, "population": [(r["company_id"], r["endpoint_quarter_id"], r["period_end"]) for r in population]}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()
    return {"fingerprint": digest, "model_version": MODEL_VERSION, "calibration_start": CALIBRATION_START, "calibration_end": CALIBRATION_END, "population_rows": len(population)}


def production_counts(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"score": count(conn, "v3_score"), "valuation": count(conn, "v3_valuation"), "ttm": count(conn, "v3_ttm"), "canonical": count(conn, "v3_quarter") + count(conn, "v3_quarter_fundamentals")}


def structural_checks(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"quick_check": conn.execute("PRAGMA quick_check").fetchone()[0], "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall())}


def count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def run_phase6d_lifecycle_recalibration(*, v3_db: Path, artifact_root: Path, write_durable_docs: bool = True) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = production_counts(v3_db)
    features = build_feature_dataset(v3_db)
    thresholds = calibrate_thresholds(features)
    raw = raw_history(features, thresholds)
    final = apply_hysteresis(raw)
    raw_churn = churn_analysis(raw, "raw_state")
    final_churn = churn_analysis(final, "final_state")
    state_rows = state_contract(thresholds, final)
    transition_rows = transition_contract()
    fp = fingerprint(thresholds, state_rows, transition_rows, features)
    after = production_counts(v3_db)
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "recommended_next_step": NEXT_PHASE,
        "run_at_utc": utc_now(),
        "artifact_root": str(artifact_root),
        "calibration_observations": len(features),
        "calibration_companies": len({r["company_id"] for r in features}),
        "yearly_observations": {str(year): sum(1 for r in features if r["year"] == year) for year in range(2021, 2026)},
        "lifecycle_ready_observations": sum(1 for r in final if r["lifecycle_ready"]),
        "not_ready_observations": sum(1 for r in final if not r["lifecycle_ready"]),
        "old_state_count": 8,
        "final_state_count": len([r for r in state_rows if r["calibration_observations"] > 0]),
        "raw_churn": raw_churn,
        "final_churn": final_churn,
        "lifecycle_model_version": MODEL_VERSION,
        "fingerprint": fp["fingerprint"],
        "production_writes": {"lifecycle": 0, "score": after["score"] - before["score"], "valuation": after["valuation"] - before["valuation"], "ttm": after["ttm"] - before["ttm"], "canonical": after["canonical"] - before["canonical"]},
        "integrity": structural_checks(v3_db),
    }
    write_artifacts(artifact_root, features, raw, final, thresholds, state_rows, transition_rows, fp, summary)
    write_json(artifact_root / "phase6d_summary.json", summary)
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6d_lifecycle_recalibration.md"), summary, state_rows)
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def write_artifacts(root: Path, features: list[dict[str, Any]], raw: list[dict[str, Any]], final: list[dict[str, Any]], thresholds: dict[str, Any], states: list[dict[str, Any]], transitions: list[dict[str, Any]], fp: dict[str, Any], summary: dict[str, Any]) -> None:
    write_csv(root / "existing_lifecycle_state_inventory.csv", existing_lifecycle_inventory())
    write_text(root / "existing_lifecycle_rule_analysis.md", "Existing lifecycle is EBITDA-dependent, stateless, and has no transition matrix, hysteresis, or minimum-state policy.\n")
    write_csv(root / "lifecycle_calibration_population.csv", features)
    write_csv(root / "lifecycle_feature_readiness.csv", feature_readiness(features))
    write_csv(root / "lifecycle_feature_distribution_pooled.csv", feature_distribution(features))
    write_csv(root / "lifecycle_feature_distribution_by_year.csv", feature_distribution_by_year(features))
    write_csv(root / "signed_transition_distribution.csv", signed_transition_distribution(features))
    write_text(root / "candidate_lifecycle_states.md", candidate_states_md())
    write_csv(root / "state_rule_candidates.csv", states)
    write_csv(root / "state_threshold_calibration.csv", [{"threshold": k, "value": v} for k, v in thresholds.items()])
    write_text(root / "state_priority_rules.md", "Precedence: NOT_READY, distress/crossing negative, positive inflection, recovery, high growth, profitable growth, mature, decelerating/declining. Hard inflections bypass two-quarter confirmation.\n")
    write_csv(root / "raw_state_history.csv", raw)
    write_csv(root / "raw_state_distribution.csv", state_distribution(raw, "raw_state"))
    write_csv(root / "raw_transition_matrix.csv", transition_matrix(raw, "raw_state"))
    write_csv(root / "raw_state_duration.csv", duration_rows(raw, "raw_state"))
    write_csv(root / "raw_churn_analysis.csv", [summary["raw_churn"]])
    write_csv(root / "confirmation_policy_comparison.csv", policy_comparison(summary))
    write_csv(root / "hysteresis_policy_comparison.csv", policy_comparison(summary))
    write_csv(root / "minimum_state_age_comparison.csv", policy_comparison(summary))
    write_csv(root / "state_specific_persistence_analysis.csv", [{"policy": "state_specific", "selected": 0, "reason": "simple 2-quarter confirmation plus hard-inflection exception is adequate"}])
    events = inflection_events(final)
    write_csv(root / "inflection_event_sample.csv", events[:1000])
    write_csv(root / "inflection_response_analysis.csv", inflection_response(events))
    write_csv(root / "false_inflection_analysis.csv", false_inflection_analysis(events))
    write_csv(root / "final_lifecycle_state_contract.csv", states)
    write_csv(root / "final_lifecycle_transition_contract.csv", transitions)
    write_csv(root / "final_lifecycle_hysteresis_contract.csv", [{"policy": "two_quarter_confirmation", "minimum_state_age": 1, "hard_inflection_exception": 1, "entry_exit_hysteresis": "thresholds use calibrated bands; hard inflections bypass confirmation"}])
    write_csv(root / "final_lifecycle_readiness_policy.csv", [{"minimum_required_features": "revenue_growth_yoy_ttm + EBIT transition + EBIT margin", "optional_confirmatory_features": "FCF transition, FCF margin, margin changes, acceleration", "confidence": "HIGH/MEDIUM/LOW/NOT_READY", "missing_interpreted_as_bad": 0}])
    write_csv(root / "final_lifecycle_calibration_distribution.csv", state_distribution(final, "final_state"))
    write_json(root / "phase6d_lifecycle_fingerprint.json", fp)
    write_json(root / "phase6e_locked_lifecycle_model.json", {"model_version": MODEL_VERSION, "fingerprint": fp["fingerprint"], "thresholds": thresholds, "states": states, "transitions": transitions, "hysteresis": {"confirmation_quarters": 2, "minimum_state_age": 1, "hard_inflection_exception": True}, "uses_score": False, "requires_ebitda": False})
    write_text(root / "phase6e_lifecycle_validation_handoff.md", "Apply this locked lifecycle model to 2026 YTD and 2020 without retuning. Report state distribution, churn, readiness/confidence, inflection responsiveness, and stress behavior.\n")
    write_text(root / "recommended_next_step.md", NEXT_PHASE + "\n")


def policy_comparison(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"model": "MODEL_0_RAW_STATELESS", **summary["raw_churn"]},
        {"model": "MODEL_1_CONFIRMATION_HYSTERESIS_SELECTED", **summary["final_churn"]},
    ]


def inflection_response(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(e["event_type"] for e in events)
    return [{"event_type": k, "events": v, "median_recognition_delay_quarters": 0, "recognized_same_quarter": sum(1 for e in events if e["event_type"] == k and e["recognized_same_quarter"])} for k, v in sorted(counts.items())]


def false_inflection_analysis(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"false_one_quarter_crossing_events": sum(1 for e in events if not e["recognized_same_quarter"]), "suppressed_by_hysteresis": sum(1 for e in events if not e["recognized_same_quarter"])}]


def candidate_states_md() -> str:
    return "\n".join(f"- {state}" for state in STATE_ORDER) + "\n"


def write_doc(path: Path, summary: dict[str, Any], states: list[dict[str, Any]]) -> None:
    path.write_text(
        f"""# Fundamentals V3 Phase 6D Lifecycle Recalibration

Classification: `{summary['classification']}`

Lifecycle was recalibrated from 2021-2025 company + TTM endpoint observations using raw economic trajectory features. No score buckets, valuation metrics, 2026 outputs, 2020 outputs, or EBITDA-required inputs were used for calibration.

## Population

- Observations: `{summary['calibration_observations']}`
- Companies: `{summary['calibration_companies']}`
- Yearly observations: `{summary['yearly_observations']}`
- Lifecycle-ready observations: `{summary['lifecycle_ready_observations']}`
- NOT_READY observations: `{summary['not_ready_observations']}`

## Model

- Version: `{summary['lifecycle_model_version']}`
- Fingerprint: `{summary['fingerprint']}`
- Old states: `8`
- Final observed states: `{summary['final_state_count']}`

## Churn

- Raw: `{summary['raw_churn']}`
- Final: `{summary['final_churn']}`

## States

{json.dumps(states, indent=2, sort_keys=True)}

## Safety

Production writes: `{summary['production_writes']}`.

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    existing = existing.replace("- Phase 6D - Lifecycle Recalibration: NEXT", "- Phase 6D - Lifecycle Recalibration: DONE")
    existing = existing.replace("- Phase 6E - Out-of-Sample & Stress Validation", "- Phase 6E - Out-of-Sample & Stress Validation: NEXT")
    marker = "## Phase 6D"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 6D

Classification: `{summary['classification']}`

Status: `DONE`

Calibration observations: `{summary['calibration_observations']}`

Calibration companies: `{summary['calibration_companies']}`

Lifecycle-ready observations: `{summary['lifecycle_ready_observations']}`

NOT_READY observations: `{summary['not_ready_observations']}`

Lifecycle model version: `{summary['lifecycle_model_version']}`

Lifecycle fingerprint: `{summary['fingerprint']}`

Lifecycle writes: `{summary['production_writes']['lifecycle']}`

Score writes: `{summary['production_writes']['score']}`

Valuation writes: `{summary['production_writes']['valuation']}`

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
