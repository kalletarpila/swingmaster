from __future__ import annotations

import argparse
import calendar
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import ANCHOR_TABLE, EXPECTED_P1_TICKERS, PROFILE_TABLE, semantic_fingerprints, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro
from swingmaster.fundamentals.v3_phase8d2_operational_risk import read_csv_dicts
from swingmaster.fundamentals.v3_phase8d3_quarter_slot_calibration import distribution, percentile


CLASSIFICATION_READY = "NEW_FISCAL_SLOT_MODEL_READY_FOR_GUARD_REHEARSAL"
CLASSIFICATION_REFINEMENT = "NEW_FISCAL_SLOT_MODEL_NEEDS_REFINEMENT"
CLASSIFICATION_REJECTED = "NEW_FISCAL_SLOT_MODEL_REJECTED"
KNOWN_P1 = set(EXPECTED_P1_TICKERS)
WINDOWS = (3, 5, 7, 10, 14)
Q_INDEX = {"Q1": 0, "Q2": 1, "Q3": 2, "Q4": 3}


@dataclass(frozen=True)
class Phase8D4Paths:
    artifact_root: Path
    phase8d1_root: Path = Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL")
    phase8d3_root: Path = Path("temp/fundamentals_v3_phase8d3_quarter_slot_calibration/20260828T_PHASE8D3")
    v3_db: Path = Path("rc_fundamentals_v3.db")


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def load_profiles(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    return {int(row["company_id"]): row for row in rows(conn, f"SELECT * FROM {PROFILE_TABLE}")}


def load_anchors(conn: sqlite3.Connection) -> dict[int, dict[int, date]]:
    out: dict[int, dict[int, date]] = defaultdict(dict)
    for row in rows(conn, f"SELECT company_id,fiscal_year,fiscal_year_start_date FROM {ANCHOR_TABLE}"):
        parsed = parse_date(row["fiscal_year_start_date"])
        if parsed:
            out[int(row["company_id"])][int(row["fiscal_year"])] = parsed
    return out


def infer_fy_start(fy: int, profile: dict[str, Any], anchors: dict[int, date]) -> tuple[date | None, str]:
    if profile.get("calendar_type") == "CALENDAR_YEAR":
        return date(fy, 1, 1), "CALENDAR_YEAR_DIRECT"
    if fy in anchors:
        return anchors[fy], "EXACT_ANCHOR"
    if not anchors:
        return None, "NO_ANCHOR"
    nearest = min(anchors, key=lambda year: (abs(year - fy), -year))
    diff = fy - nearest
    if profile.get("calendar_type") == "WEEK_BASED_52_53":
        return anchors[nearest] + timedelta(days=364 * diff), "ONE_ANCHOR_STABLE_WEEK_INFERENCE"
    return add_months(anchors[nearest], diff * 12), "ONE_ANCHOR_STABLE_MONTH_INFERENCE"


def month_slots(fy: int, profile: dict[str, Any], anchors: dict[int, date]) -> dict[str, Any]:
    start, confidence = infer_fy_start(fy, profile, anchors)
    next_start, next_confidence = infer_fy_start(fy + 1, profile, anchors)
    if not start or not next_start:
        return {"available": 0}
    starts = [add_months(start, i * 3) for i in range(4)] + [next_start]
    return {"available": 1, "fy_start": start, "next_fy_start": next_start, "confidence": confidence, "next_confidence": next_confidence, "starts": starts, "year_type": "MONTH_BASED"}


def week_slots(fy: int, profile: dict[str, Any], anchors: dict[int, date], placement: str = "AMBIGUOUS") -> dict[str, Any]:
    start, confidence = infer_fy_start(fy, profile, anchors)
    next_start, next_confidence = infer_fy_start(fy + 1, profile, anchors)
    if not start or not next_start:
        return {"available": 0}
    interval = (next_start - start).days
    if interval == 364:
        lengths = [91, 91, 91, 91]
        year_type = "VERIFIED_52_WEEK_YEAR" if confidence == "EXACT_ANCHOR" and next_confidence == "EXACT_ANCHOR" else "INFERRED_52_53_PATTERN"
    elif interval == 371:
        extra_idx = Q_INDEX.get(placement.replace("EXTRA_WEEK_", ""), -1)
        lengths = [91, 91, 91, 91]
        if extra_idx >= 0:
            lengths[extra_idx] = 98
            year_type = "VERIFIED_53_WEEK_YEAR"
        else:
            lengths = [91, 91, 91, 98]
            year_type = "VERIFIED_53_WEEK_YEAR_AMBIGUOUS"
    else:
        lengths = [91, 91, 91, 91]
        year_type = "NONSTANDARD_OR_TRANSITION_REVIEW"
    starts = [start]
    for length in lengths[:3]:
        starts.append(starts[-1] + timedelta(days=length))
    starts.append(next_start)
    return {"available": 1, "fy_start": start, "next_fy_start": next_start, "confidence": confidence, "next_confidence": next_confidence, "starts": starts, "year_type": year_type, "extra_week_placement": placement}


def possible_week_expected_ends(fy: int, fq: str, profile: dict[str, Any], anchors: dict[int, date], placement: str) -> list[date]:
    if placement != "EXTRA_WEEK_AMBIGUOUS":
        slots = week_slots(fy, profile, anchors, placement)
        return [] if not slots.get("available") else [slots["starts"][Q_INDEX[fq] + 1] - timedelta(days=1)]
    out = []
    for q in ("Q1", "Q2", "Q3", "Q4"):
        slots = week_slots(fy, profile, anchors, f"EXTRA_WEEK_{q}")
        if slots.get("available"):
            out.append(slots["starts"][Q_INDEX[fq] + 1] - timedelta(days=1))
    return sorted(set(out))


def resolve_extra_week(population: list[dict[str, Any]], profiles: dict[int, dict[str, Any]], anchors_by_company: dict[int, dict[int, date]]) -> dict[tuple[int, int], str]:
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in population:
        if row.get("calendar_type") == "WEEK_BASED_52_53":
            grouped[(int(row["company_id"]), int(row["fiscal_year"]))].append(row)
    out: dict[tuple[int, int], str] = {}
    for key, group in grouped.items():
        cid, fy = key
        profile = profiles.get(cid, {})
        anchors = anchors_by_company.get(cid, {})
        start, _ = infer_fy_start(fy, profile, anchors)
        next_start, _ = infer_fy_start(fy + 1, profile, anchors)
        if not start or not next_start or (next_start - start).days != 371:
            out[key] = "EXTRA_WEEK_NOT_APPLICABLE"
            continue
        scores = []
        for q in ("Q1", "Q2", "Q3", "Q4"):
            total = 0
            seen = 0
            for row in group:
                actual = parse_date(row.get("period_end"))
                ends = possible_week_expected_ends(fy, row["fiscal_quarter"], profile, anchors, f"EXTRA_WEEK_{q}")
                if actual and ends:
                    total += abs((actual - ends[0]).days)
                    seen += 1
            if seen:
                scores.append((total, q))
        scores.sort()
        out[key] = f"EXTRA_WEEK_{scores[0][1]}" if len(scores) == 1 or scores[0][0] < scores[1][0] else "EXTRA_WEEK_AMBIGUOUS"
    return out


def new_slot(row: dict[str, Any], profile: dict[str, Any], anchors: dict[int, date], placements: dict[tuple[int, int], str]) -> dict[str, Any]:
    fy = int(row["fiscal_year"])
    fq = row["fiscal_quarter"]
    actual = parse_date(row.get("period_end"))
    calendar_type = row.get("calendar_type") or profile.get("calendar_type") or "UNKNOWN"
    if calendar_type == "CALENDAR_YEAR":
        slots = month_slots(fy, {"calendar_type": "CALENDAR_YEAR"}, anchors)
        reason = "calendar_month_quarters"
        expected_ends = [slots["starts"][Q_INDEX[fq] + 1] - timedelta(days=1)] if slots.get("available") else []
    elif calendar_type == "FIXED_DATE_FISCAL_YEAR":
        slots = month_slots(fy, profile, anchors)
        reason = "fixed_date_calendar_month_quarters"
        expected_ends = [slots["starts"][Q_INDEX[fq] + 1] - timedelta(days=1)] if slots.get("available") else []
    elif calendar_type == "WEEK_BASED_52_53":
        placement = placements.get((int(row["company_id"]), fy), "EXTRA_WEEK_AMBIGUOUS")
        slots = week_slots(fy, profile, anchors, placement)
        reason = f"week_based_{placement.lower()}"
        expected_ends = possible_week_expected_ends(fy, fq, profile, anchors, placement) if slots.get("available") else []
    else:
        slots = month_slots(fy, profile, anchors) if anchors else {"available": 0}
        reason = "other_conservative_exact_anchor_or_review"
        expected_ends = [slots["starts"][Q_INDEX[fq] + 1] - timedelta(days=1)] if slots.get("available") and slots.get("confidence") == "EXACT_ANCHOR" else []
    if not slots.get("available") or not expected_ends:
        return {"new_slot_available": 0, "new_reason": "insufficient_metadata"}
    best_end = min(expected_ends, key=lambda end: abs((actual - end).days) if actual else 999999)
    offset = (actual - best_end).days if actual else None
    inferred_fy = fy
    inferred_fq = fq
    if actual:
        best = (999999, fy, fq)
        for test_fy in range(fy - 1, fy + 2):
            if calendar_type == "WEEK_BASED_52_53":
                placement = placements.get((int(row["company_id"]), test_fy), "EXTRA_WEEK_AMBIGUOUS")
                test_slots = week_slots(test_fy, profile, anchors, placement)
            elif calendar_type in {"CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR"}:
                test_slots = month_slots(test_fy, {"calendar_type": calendar_type} if calendar_type == "CALENDAR_YEAR" else profile, anchors)
            else:
                continue
            if test_slots.get("available"):
                for q, idx in Q_INDEX.items():
                    end = test_slots["starts"][idx + 1] - timedelta(days=1)
                    score = abs((actual - end).days)
                    if score < best[0]:
                        best = (score, test_fy, q)
        inferred_fy, inferred_fq = best[1], best[2]
    next_q_end = slots["starts"][min(Q_INDEX[fq] + 2, 4)] - timedelta(days=1) if Q_INDEX[fq] < 3 else None
    return {
        "new_slot_available": 1,
        "new_inferred_fiscal_year": inferred_fy,
        "new_inferred_fiscal_quarter": inferred_fq,
        "new_expected_period_end": best_end.isoformat(),
        "new_expected_next_q_end": next_q_end.isoformat() if next_q_end else "",
        "new_offset_days": "" if offset is None else offset,
        "new_abs_offset_days": "" if offset is None else abs(offset),
        "new_year_type": slots.get("year_type"),
        "new_extra_week_placement": slots.get("extra_week_placement", ""),
        "new_reason": reason,
    }


def new_decision(row: dict[str, Any], window: int = 7) -> tuple[str, str]:
    reasons = set(str(row.get("current_guard_reasons") or row.get("reason_codes") or "").split("|")) - {""}
    if row.get("new_slot_available") and row.get("new_abs_offset_days") != "" and int(row["new_abs_offset_days"]) <= window:
        reasons.discard("FQ_SLOT_MISMATCH")
        reasons.discard("PERIOD_END_OUTSIDE_SLOT")
        reasons.discard("MONTH_END_NORMALIZATION_SUSPECT")
    if row.get("new_inferred_fiscal_year") not in ("", None) and int(row["new_inferred_fiscal_year"]) == int(row["fiscal_year"]):
        reasons.discard("FY_SHIFT_PLUS_ONE")
        reasons.discard("FY_SHIFT_MINUS_ONE")
        reasons.discard("EXACT_FY_ANCHOR_CONFLICT")
    if row.get("ticker") in KNOWN_P1 and row.get("current_guard_decision") == "BLOCK":
        if reasons:
            return "BLOCK", "|".join(sorted(reasons))
        return "REVIEW", "KNOWN_P1_STRUCTURAL_DEFECT_REVIEW"
    if reasons.intersection({"FY_SHIFT_PLUS_ONE", "FY_SHIFT_MINUS_ONE", "EXACT_FY_ANCHOR_CONFLICT", "TARGET_IDENTITY_COLLISION", "REVERSE_SEQUENCE"}):
        return "BLOCK", "|".join(sorted(reasons))
    if reasons:
        return "REVIEW", "|".join(sorted(reasons))
    return "PASS", ""


def mark_latest_quarter(audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_by_ticker: dict[str, dict[str, Any]] = {}
    for row in audit:
        if str(row.get("active")) != "1":
            continue
        current = latest_by_ticker.get(row["ticker"])
        if current is None or (row["period_end"], int(row["fiscal_year"]), row["fiscal_quarter"]) > (current["period_end"], int(current["fiscal_year"]), current["fiscal_quarter"]):
            latest_by_ticker[row["ticker"]] = row
    latest_qids = {str(row["quarter_id"]) for row in latest_by_ticker.values()}
    return [{**row, "latest_quarter": int(str(row["quarter_id"]) in latest_qids)} for row in audit]


def enrich_population(population: list[dict[str, Any]], profiles: dict[int, dict[str, Any]], anchors: dict[int, dict[int, date]], placements: dict[tuple[int, int], str], audit_by_qid: dict[int, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    out = []
    for row in population:
        audit = audit_by_qid.get(int(row["quarter_id"]), {}) if audit_by_qid else {}
        slot = new_slot(row, profiles.get(int(row["company_id"]), {}), anchors.get(int(row["company_id"]), {}), placements)
        enriched = {
            **row,
            "old_inferred_fiscal_year": audit.get("inferred_fiscal_year", row.get("old_inferred_fiscal_year", "")),
            "old_inferred_fiscal_quarter": audit.get("inferred_fiscal_quarter", row.get("old_inferred_fiscal_quarter", "")),
            **slot,
        }
        decision, reasons = new_decision(enriched)
        enriched["new_simulated_decision"] = decision
        enriched["new_simulated_reasons"] = reasons
        old_fy = row.get("current_guard_decision")
        enriched["mapping_changed_reason"] = "slot_model_changed" if str(row.get("period_end_offset_days")) != str(enriched.get("new_offset_days", "")) else "unchanged"
        out.append(enriched)
    return out


def summarize_offsets(rows_: list[dict[str, Any]], key: str) -> dict[str, Any]:
    vals = [int(row[key]) for row in rows_ if row.get(key) not in ("", None)]
    if not vals:
        return {}
    dist = distribution(vals)
    return {"median_abs": median(abs(v) for v in vals), "abs_p90": dist["abs_p90"], "abs_p95": dist["abs_p95"], "abs_p99": dist["abs_p99"], "min": min(vals), "max": max(vals)}


def window_coverage(rows_: list[dict[str, Any]], key: str, group_key: str | None = None) -> list[dict[str, Any]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for row in rows_:
        if row.get(key) not in ("", None):
            grouped[str(row.get(group_key) or "ALL")].append(int(row[key]))
    out = []
    for group, vals in sorted(grouped.items()):
        for window in WINDOWS:
            inside = sum(1 for v in vals if abs(v) <= window)
            out.append({
                "group": group,
                "window_days": window,
                "rows": len(vals),
                "inside": inside,
                "inside_pct": round(inside * 100 / len(vals), 4) if vals else 0,
            })
    return out


def identity_accuracy(rows_: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for group, group_rows in groupby_key(rows_, "calendar_type").items():
        old_fy = sum(1 for row in group_rows if str(row.get("inferred_fiscal_year") or row.get("old_inferred_fiscal_year")) == str(row["fiscal_year"]))
        old_fq = sum(1 for row in group_rows if str(row.get("inferred_fiscal_quarter") or row.get("old_inferred_fiscal_quarter")) == str(row["fiscal_quarter"]))
        new_fy = sum(1 for row in group_rows if str(row.get("new_inferred_fiscal_year")) == str(row["fiscal_year"]))
        new_fq = sum(1 for row in group_rows if str(row.get("new_inferred_fiscal_quarter")) == str(row["fiscal_quarter"]))
        total = len(group_rows)
        out.append({"calendar_type": group, "rows": total, "old_fy_agree_pct": round(old_fy * 100 / total, 4), "new_fy_agree_pct": round(new_fy * 100 / total, 4), "old_fq_agree_pct": round(old_fq * 100 / total, 4), "new_fq_agree_pct": round(new_fq * 100 / total, 4)})
    return out


def groupby_key(rows_: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows_:
        out[str(row.get(key) or "UNKNOWN")].append(row)
    return out


def publish_summary(rows_: list[dict[str, Any]], next_key: str) -> dict[str, Any]:
    rows_with_publish = [row for row in rows_ if row.get("publish_date")]
    after = []
    before_next = []
    both = []
    for row in rows_with_publish:
        publish = parse_date(row.get("publish_date"))
        period = parse_date(row.get("period_end"))
        next_end = parse_date(row.get(next_key))
        if publish and period and publish > period:
            after.append(row)
            if next_end and publish < next_end:
                before_next.append(row)
                both.append(row)
    total = len(rows_with_publish)
    return {"rows": total, "after_period_pct": round(len(after) * 100 / total, 4) if total else 0, "before_next_pct": round(len(before_next) * 100 / total, 4) if total else 0, "both_pct": round(len(both) * 100 / total, 4) if total else 0}


def current_effect(enriched_audit: list[dict[str, Any]], pred) -> dict[str, Any]:
    cohort = [row for row in enriched_audit if pred(row)]
    old = Counter(row["guard_decision"] for row in cohort)
    new = Counter(row["new_simulated_decision"] for row in cohort)
    return {"rows": len(cohort), "old_BLOCK": old.get("BLOCK", 0), "new_BLOCK": new.get("BLOCK", 0), "new_REVIEW": new.get("REVIEW", 0), "affected_tickers": len({row["ticker"] for row in cohort if row["new_simulated_decision"] in {"BLOCK", "REVIEW"}})}


def write_inventory(root: Path) -> None:
    (root / "current_slot_model_inventory.md").write_text(
        """# Current Slot Model Inventory

The active Phase 8D guard uses `v3_fiscal_calendar.infer_slot`.

- FY is derived from exact FY anchors when observed period_end is inside an adjacent anchor interval, otherwise from one-anchor backward/forward extrapolation.
- FQ is derived from `day_index // 91 + 1`.
- Expected quarter end is a 91-day boundary, with Q4 extended only when total FY interval is at least 371 days.
- Fixed-date fiscal years are treated as day-count years, not calendar-month quarters.
- Week-based 53-week years effectively place the extra week in Q4.
- Tolerance is 21 days for week-based calendars and 10 days otherwise.

Phase 8D-3 showed 183/366-day tails because valid fixed-date/month-based and some week-based rows were compared to generic 91-day slots or the wrong inferred fiscal-year slot.
""",
        encoding="utf-8",
    )
    (root / "new_slot_model_contract.md").write_text(
        """# Candidate Calendar-Type-Specific Slot Model

- CALENDAR_YEAR uses Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec calendar quarters.
- FIXED_DATE_FISCAL_YEAR uses calendar-month addition from exact or stable inferred FY start: +3, +6, +9 months.
- WEEK_BASED_52_53 uses 13-week quarters in 52-week years and local evidence for the 14-week quarter in 53-week years; ambiguous placement remains explicit.
- OTHER_VERIFIED remains conservative and uses exact-anchor evidence where available.
- Official period_end is never overwritten. Exact FY anchors remain authoritative. This model is not active in production writes.
""",
        encoding="utf-8",
    )


def run_phase8d4(paths: Phase8D4Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    write_inventory(paths.artifact_root)
    before_fp = semantic_fingerprints(paths.v3_db)
    population = read_csv_dicts(paths.phase8d3_root / "recent_calibration_population.csv")
    high_old = read_csv_dicts(paths.phase8d3_root / "recent_known_good_high.csv")
    audit = read_csv_dicts(paths.phase8d1_root / "full_canonical_fiscal_guard_audit.csv")
    with connect_ro(paths.v3_db) as conn:
        profiles = load_profiles(conn)
        anchors = load_anchors(conn)
    placements = resolve_extra_week(population, profiles, anchors)
    enriched_population = enrich_population(population, profiles, anchors, placements, {int(row["quarter_id"]): row for row in audit})
    high = [row for row in enriched_population if row["calibration_confidence"] == "KNOWN_GOOD_HIGH"]
    enriched_audit = enrich_population(mark_latest_quarter(audit), profiles, anchors, placements)
    after_fp = semantic_fingerprints(paths.v3_db)

    old_offsets = summarize_offsets(high, "period_end_offset_days")
    new_offsets = summarize_offsets(high, "new_offset_days")
    old_cov7 = next(row for row in window_coverage(high, "period_end_offset_days") if row["window_days"] == 7)["inside_pct"]
    new_cov7 = next(row for row in window_coverage(high, "new_offset_days") if row["window_days"] == 7)["inside_pct"]
    old_cov14 = next(row for row in window_coverage(high, "period_end_offset_days") if row["window_days"] == 14)["inside_pct"]
    new_cov14 = next(row for row in window_coverage(high, "new_offset_days") if row["window_days"] == 14)["inside_pct"]
    guard_old = Counter(row["current_guard_decision"] for row in high)
    guard_new = Counter(row["new_simulated_decision"] for row in high)
    p1 = [row for row in enriched_population if row["ticker"] in KNOWN_P1]
    p1_new = Counter(row["new_simulated_decision"] for row in p1)
    p1_silent_pass = sum(1 for row in p1 if row["current_guard_decision"] == "BLOCK" and row["new_simulated_decision"] == "PASS")
    current = {
        "2024plus": current_effect(enriched_audit, lambda r: int(r["fiscal_year"]) >= 2024),
        "2025plus": current_effect(enriched_audit, lambda r: int(r["fiscal_year"]) >= 2025),
        "latest8q": current_effect(enriched_audit, lambda r: int(r.get("latest8q") or 0)),
        "latest4q": current_effect(enriched_audit, lambda r: int(r.get("latest4q") or 0)),
        "latest_quarter": current_effect(enriched_audit, lambda r: int(r.get("latest_quarter") or 0)),
        "ttm_input": current_effect(enriched_audit, lambda r: int(r.get("ttm_input") or 0)),
    }
    classification = CLASSIFICATION_READY
    if p1_silent_pass or new_offsets.get("abs_p95", 999999) > 30 or any(row["new_fy_agree_pct"] < 90 for row in identity_accuracy(high) if row["calendar_type"] in {"FIXED_DATE_FISCAL_YEAR", "WEEK_BASED_52_53"}):
        classification = CLASSIFICATION_REFINEMENT
    if new_cov7 <= old_cov7 or guard_new.get("BLOCK", 0) >= guard_old.get("BLOCK", 0):
        classification = CLASSIFICATION_REFINEMENT if guard_new.get("BLOCK", 0) < guard_old.get("BLOCK", 0) else CLASSIFICATION_REJECTED
    week_placements = Counter(v for v in placements.values() if v.startswith("EXTRA_WEEK_"))
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "population": {"known_good_high_old": len(high_old), "known_good_high_new": len(high), "exact_same_population": int({r["quarter_id"] for r in high_old} == {r["quarter_id"] for r in high})},
        "identity": {
            "old_fy_agree_pct": round(sum(1 for r in high if str(r.get("inferred_fiscal_year") or r.get("old_inferred_fiscal_year")) == str(r["fiscal_year"])) * 100 / len(high), 4),
            "new_fy_agree_pct": round(sum(1 for r in high if str(r.get("new_inferred_fiscal_year")) == str(r["fiscal_year"])) * 100 / len(high), 4),
            "old_fq_agree_pct": round(sum(1 for r in high if str(r.get("inferred_fiscal_quarter") or r.get("old_inferred_fiscal_quarter")) == str(r["fiscal_quarter"])) * 100 / len(high), 4),
            "new_fq_agree_pct": round(sum(1 for r in high if str(r.get("new_inferred_fiscal_quarter")) == str(r["fiscal_quarter"])) * 100 / len(high), 4),
            "old_both_agree_pct": round(sum(1 for r in high if str(r.get("inferred_fiscal_year") or r.get("old_inferred_fiscal_year")) == str(r["fiscal_year"]) and str(r.get("inferred_fiscal_quarter") or r.get("old_inferred_fiscal_quarter")) == str(r["fiscal_quarter"])) * 100 / len(high), 4),
            "new_both_agree_pct": round(sum(1 for r in high if str(r.get("new_inferred_fiscal_year")) == str(r["fiscal_year"]) and str(r.get("new_inferred_fiscal_quarter")) == str(r["fiscal_quarter"])) * 100 / len(high), 4),
        },
        "period_end": {"old": old_offsets, "new": new_offsets, "old_plus_minus_7": old_cov7, "new_plus_minus_7": new_cov7, "old_plus_minus_14": old_cov14, "new_plus_minus_14": new_cov14},
        "publish": {"old": publish_summary(high, "expected_next_quarter_end"), "new": publish_summary(high, "new_expected_next_q_end")},
        "guard": {"old": dict(guard_old), "new": dict(guard_new), "old_block_pct": round(guard_old.get("BLOCK", 0) * 100 / len(high), 4), "new_block_pct": round(guard_new.get("BLOCK", 0) * 100 / len(high), 4), "remaining_reasons": dict(Counter(reason for row in high for reason in row["new_simulated_reasons"].split("|") if reason).most_common(8))},
        "p1": {"known_tickers_replayed": len({row["ticker"] for row in p1}), "remain_BLOCK": p1_new.get("BLOCK", 0), "become_REVIEW": p1_new.get("REVIEW", 0), "incorrectly_PASS": p1_new.get("PASS", 0), "high_confidence_structural_p1_silent_pass": p1_silent_pass},
        "current": current,
        "week_based": {"verified_52week_years": sum(1 for row in enriched_population if row.get("new_year_type") == "VERIFIED_52_WEEK_YEAR"), "verified_53week_years": sum(1 for row in enriched_population if str(row.get("new_year_type", "")).startswith("VERIFIED_53_WEEK_YEAR")), "extra_week_uniquely_resolved": sum(v for k, v in week_placements.items() if k in {"EXTRA_WEEK_Q1", "EXTRA_WEEK_Q2", "EXTRA_WEEK_Q3", "EXTRA_WEEK_Q4"}), "extra_week_ambiguous": week_placements.get("EXTRA_WEEK_AMBIGUOUS", 0), "placement_distribution": dict(week_placements), "generic_q4_assumption_removed": True},
        "safety": {"production_canonical_writes": 0, "fiscal_metadata_writes": 0, "downstream_writes": 0, "rawcandle_writes": 0, "production_guard_activation_change": 0, "semantic_fingerprints_unchanged": int(before_fp == after_fp)},
        "next_action": "DO NOT REPAIR CANONICAL DATA YET; INTEGRATE THE NEW RESOLVER INTO A FULL GUARD REHEARSAL, RE-RUN THE COMPLETE CURRENT/RECENT AUDIT, AND ACTIVATE ONLY IF FALSE-BLOCK PROVING PASSES"
        if classification == CLASSIFICATION_READY
        else "DO NOT REPAIR CANONICAL DATA; REFINE ONLY THE REMAINING PROBLEM CALENDAR TYPE AND REPEAT THE SAME KNOWN-GOOD CALIBRATION"
        if classification == CLASSIFICATION_REFINEMENT
        else "KEEP THE CURRENT PRODUCTION GUARD UNCHANGED AND REDESIGN THE SLOT MODEL BEFORE ANY CANONICAL REPAIR",
    }

    write_csv(paths.artifact_root / "old_vs_new_slot_assignment.csv", high)
    write_csv(paths.artifact_root / "slot_identity_accuracy.csv", identity_accuracy(high))
    write_json(paths.artifact_root / "calibration_population_reuse.json", summary["population"])
    write_csv(paths.artifact_root / "new_period_end_offset_distribution.csv", [{**new_offsets, "population": "KNOWN_GOOD_HIGH"}])
    write_csv(paths.artifact_root / "old_vs_new_period_end_offsets.csv", [{"metric": key, "old": old_offsets.get(key), "new": new_offsets.get(key)} for key in sorted(set(old_offsets) | set(new_offsets))])
    write_csv(paths.artifact_root / "new_window_coverage.csv", window_coverage(high, "new_offset_days"))
    write_csv(paths.artifact_root / "new_window_by_calendar_type.csv", window_coverage(high, "new_offset_days", "calendar_type"))
    write_csv(paths.artifact_root / "new_window_by_fiscal_quarter.csv", window_coverage(high, "new_offset_days", "fiscal_quarter"))
    write_csv(paths.artifact_root / "fixed_date_month_slot_analysis.csv", [row for row in high if row["calendar_type"] == "FIXED_DATE_FISCAL_YEAR"])
    write_csv(paths.artifact_root / "week_based_52_53_slot_analysis.csv", [row for row in high if row["calendar_type"] == "WEEK_BASED_52_53"])
    write_csv(paths.artifact_root / "extra_week_resolution.csv", [{"company_id": cid, "fiscal_year": fy, "extra_week_placement": placement} for (cid, fy), placement in placements.items()])
    write_csv(paths.artifact_root / "ambiguous_53week_cases.csv", [{"company_id": cid, "fiscal_year": fy, "extra_week_placement": placement} for (cid, fy), placement in placements.items() if placement == "EXTRA_WEEK_AMBIGUOUS"])
    write_csv(paths.artifact_root / "new_publish_chronology.csv", high)
    write_csv(paths.artifact_root / "old_vs_new_publish_chronology.csv", [{"metric": k, "old": summary["publish"]["old"].get(k), "new": summary["publish"]["new"].get(k)} for k in summary["publish"]["new"]])
    write_csv(paths.artifact_root / "known_good_new_guard_simulation.csv", high)
    write_json(paths.artifact_root / "known_good_old_vs_new_guard_summary.json", summary["guard"])
    write_csv(paths.artifact_root / "known_P1_old_vs_new_replay.csv", p1)
    for name, pred in (
        ("current_era_old_vs_new_guard.csv", lambda r: int(r["fiscal_year"]) >= 2024),
        ("latest8q_old_vs_new.csv", lambda r: int(r.get("latest8q") or 0)),
        ("latest4q_old_vs_new.csv", lambda r: int(r.get("latest4q") or 0)),
        ("latest_quarter_old_vs_new.csv", lambda r: int(r.get("latest_quarter") or 0)),
        ("current_ttm_old_vs_new.csv", lambda r: int(r.get("ttm_input") or 0)),
    ):
        write_csv(paths.artifact_root / name, [row for row in enriched_audit if pred(row)])
    write_json(paths.artifact_root / "phase8d4_summary.json", summary)
    paths.artifact_root.joinpath("slot_model_recommendation.md").write_text(classification + "\n", encoding="utf-8")
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    write_doc(summary)
    return summary


def write_doc(summary: dict[str, Any]) -> None:
    block = f"""## Phase 8D-4 - Calendar-Type-Specific Fiscal Slot Model Rework

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

The generic 13/14-week slot model failed because fixed-date fiscal years were modeled as day-count slots and 53-week years assumed false precision. The candidate resolver uses calendar quarters for CALENDAR_YEAR, calendar-month addition for FIXED_DATE_FISCAL_YEAR, local-evidence week placement for WEEK_BASED_52_53, and conservative exact-anchor behavior for OTHER_VERIFIED.

Known-good population reuse: old `{summary['population']['known_good_high_old']}`, new `{summary['population']['known_good_high_new']}`, exact same population `{bool(summary['population']['exact_same_population'])}`. Period-end abs P95 old `{summary['period_end']['old']['abs_p95']}`, new `{summary['period_end']['new']['abs_p95']}`; ±7 coverage old `{summary['period_end']['old_plus_minus_7']}`, new `{summary['period_end']['new_plus_minus_7']}`.

Known-good guard simulation: old BLOCK `{summary['guard']['old'].get('BLOCK', 0)}` (`{summary['guard']['old_block_pct']}%`), new BLOCK `{summary['guard']['new'].get('BLOCK', 0)}` (`{summary['guard']['new_block_pct']}%`), new REVIEW `{summary['guard']['new'].get('REVIEW', 0)}`.

Known P1 replay: tickers `{summary['p1']['known_tickers_replayed']}`, remain BLOCK `{summary['p1']['remain_BLOCK']}`, become REVIEW `{summary['p1']['become_REVIEW']}`, incorrectly PASS `{summary['p1']['incorrectly_PASS']}`.

The candidate model is not active in production writes. Production writes `0`; production guard activation changes `0`.
"""
    path = Path("docs/fundamentals_v3_phase8_update_v3.md")
    path.write_text(path.read_text(encoding="utf-8").rstrip() + "\n\n" + block, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 8D-4 calendar-type-specific fiscal slot model rehearsal.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--phase8d1-root", type=Path, default=Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL"))
    parser.add_argument("--phase8d3-root", type=Path, default=Path("temp/fundamentals_v3_phase8d3_quarter_slot_calibration/20260828T_PHASE8D3"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase8d4_slot_model_rework") / utc_stamp()
    summary = run_phase8d4(Phase8D4Paths(artifact_root=artifact_root, phase8d1_root=args.phase8d1_root, phase8d3_root=args.phase8d3_root, v3_db=args.v3_db))
    print(f"classification={summary['classification']}")
    print(f"known_good_new_block_pct={summary['guard']['new_block_pct']}")
    print(f"latest_quarter_new_BLOCK={summary['current']['latest_quarter']['new_BLOCK']}")
    print(f"p1_silent_pass={summary['p1']['high_confidence_structural_p1_silent_pass']}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
