from __future__ import annotations

import argparse
import calendar
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Callable

from swingmaster.fundamentals.v3_fiscal_calendar import ANCHOR_TABLE, EXPECTED_P1_TICKERS, PROFILE_TABLE, semantic_fingerprints, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro
from swingmaster.fundamentals.v3_phase8d2_operational_risk import read_csv_dicts
from swingmaster.fundamentals.v3_phase8d3_quarter_slot_calibration import distribution
from swingmaster.fundamentals.v3_phase8d4_slot_model_rework import (
    Q_INDEX,
    add_months,
    load_anchors,
    load_profiles,
    mark_latest_quarter,
    parse_date,
    resolve_extra_week,
    week_slots,
)


CLASSIFICATION_READY = "FISCAL_YEAR_INTERVAL_REFINEMENT_READY_FOR_FULL_GUARD_REHEARSAL"
CLASSIFICATION_MORE_WORK = "FISCAL_YEAR_INTERVAL_REFINEMENT_NEEDS_MORE_WORK"
CLASSIFICATION_REJECTED = "FISCAL_YEAR_INTERVAL_HYPOTHESIS_REJECTED"
KNOWN_P1 = set(EXPECTED_P1_TICKERS)
CAL_TYPES = ("CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR", "WEEK_BASED_52_53", "OTHER_VERIFIED")


@dataclass(frozen=True)
class Phase8D5Paths:
    artifact_root: Path
    phase8d1_root: Path = Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL")
    phase8d3_root: Path = Path("temp/fundamentals_v3_phase8d3_quarter_slot_calibration/20260828T_PHASE8D3")
    phase8d4_root: Path = Path("temp/fundamentals_v3_phase8d4_slot_model_rework/20260828T_PHASE8D4")
    v3_db: Path = Path("rc_fundamentals_v3.db")


def fy_start_month_based(fy: int, anchors: dict[int, date]) -> tuple[date | None, str]:
    if fy in anchors:
        return anchors[fy], "EXACT_ANCHOR"
    if not anchors:
        return None, "NO_ANCHOR"
    nearest = min(anchors, key=lambda year: (abs(year - fy), -year))
    return add_months(anchors[nearest], (fy - nearest) * 12), "STABLE_MONTH_BACKWARD_INFERENCE"


def fy_start_week_based(fy: int, anchors: dict[int, date]) -> tuple[date | None, str]:
    if fy in anchors:
        return anchors[fy], "EXACT_ANCHOR"
    if not anchors:
        return None, "NO_ANCHOR"
    years = sorted(anchors)
    if len(years) >= 2:
        if fy < years[0]:
            step = (anchors[years[1]] - anchors[years[0]]).days
            return anchors[years[0]] - timedelta(days=step * (years[0] - fy)), "STABLE_WEEK_PATTERN_INFERENCE"
        if fy > years[-1]:
            step = (anchors[years[-1]] - anchors[years[-2]]).days
            return anchors[years[-1]] + timedelta(days=step * (fy - years[-1])), "STABLE_WEEK_PATTERN_INFERENCE"
    nearest = min(anchors, key=lambda year: (abs(year - fy), -year))
    return anchors[nearest] + timedelta(days=364 * (fy - nearest)), "UNCERTAIN_WEEK_ONE_ANCHOR_364_INFERENCE"


def fy_start_for_type(fy: int, calendar_type: str, anchors: dict[int, date]) -> tuple[date | None, str]:
    if calendar_type == "CALENDAR_YEAR":
        return date(fy, 1, 1), "CALENDAR_YEAR_DIRECT"
    if calendar_type == "WEEK_BASED_52_53":
        return fy_start_week_based(fy, anchors)
    if calendar_type == "FIXED_DATE_FISCAL_YEAR":
        return fy_start_month_based(fy, anchors)
    if fy in anchors:
        return anchors[fy], "EXACT_ANCHOR"
    return None, "CONSERVATIVE_OTHER_NO_EXTRAPOLATION"


def resolve_issuer_fiscal_year_for_date(
    profile: dict[str, Any],
    anchors: dict[int, date],
    observed: date,
    transition_state: str = "STABLE_CALENDAR",
) -> dict[str, Any]:
    calendar_type = str(profile.get("calendar_type") or "UNKNOWN")
    if transition_state not in {"", "STABLE_CALENDAR", "NONE"}:
        return {"fiscal_year": "", "interval_start": "", "interval_end_exclusive": "", "evidence_type": "TRANSITION_REVIEW", "confidence": "FY_UNCERTAIN", "transition_state": transition_state}
    if calendar_type == "CALENDAR_YEAR":
        return {
            "fiscal_year": observed.year,
            "interval_start": date(observed.year, 1, 1),
            "interval_end_exclusive": date(observed.year + 1, 1, 1),
            "evidence_type": "CALENDAR_YEAR_DIRECT",
            "confidence": "FY_EXACT",
            "transition_state": "STABLE_CALENDAR",
        }
    if not anchors:
        return {"fiscal_year": "", "interval_start": "", "interval_end_exclusive": "", "evidence_type": "NO_ANCHOR", "confidence": "FY_UNCERTAIN", "transition_state": "STABLE_CALENDAR"}

    years = sorted(anchors)
    for idx, fy in enumerate(years[:-1]):
        start = anchors[fy]
        end = anchors[years[idx + 1]]
        if start <= observed < end:
            return {
                "fiscal_year": fy,
                "interval_start": start,
                "interval_end_exclusive": end,
                "evidence_type": "EXACT_ADJACENT_ANCHOR_INTERVAL",
                "confidence": "FY_EXACT",
                "transition_state": "STABLE_CALENDAR",
            }

    if calendar_type == "OTHER_VERIFIED":
        return {"fiscal_year": "", "interval_start": "", "interval_end_exclusive": "", "evidence_type": "OTHER_REQUIRES_EXACT_INTERVAL", "confidence": "FY_UNCERTAIN", "transition_state": "STABLE_CALENDAR"}

    low = min(years) - 45
    high = max(years) + 6
    starts: list[tuple[int, date, str]] = []
    for fy in range(low, high + 1):
        start, evidence = fy_start_for_type(fy, calendar_type, anchors)
        if start:
            starts.append((fy, start, evidence))
    starts.sort(key=lambda item: item[1])
    for idx, (fy, start, evidence) in enumerate(starts[:-1]):
        end = starts[idx + 1][1]
        if start <= observed < end:
            confidence = "FY_HIGH"
            if calendar_type == "WEEK_BASED_52_53" and "UNCERTAIN_WEEK_ONE_ANCHOR" in evidence:
                confidence = "FY_UNCERTAIN"
            return {
                "fiscal_year": fy,
                "interval_start": start,
                "interval_end_exclusive": end,
                "evidence_type": evidence,
                "confidence": confidence,
                "transition_state": "STABLE_CALENDAR",
            }
    return {"fiscal_year": "", "interval_start": "", "interval_end_exclusive": "", "evidence_type": "NO_INTERVAL_MATCH", "confidence": "FY_UNCERTAIN", "transition_state": "STABLE_CALENDAR"}


def month_end_policy(start: date, months: int) -> date:
    target = add_months(start, months)
    if start.day == calendar.monthrange(start.year, start.month)[1]:
        return date(target.year, target.month, calendar.monthrange(target.year, target.month)[1])
    return target


def month_quarter_starts(start: date, end: date) -> list[date]:
    return [start, month_end_policy(start, 3), month_end_policy(start, 6), month_end_policy(start, 9), end]


def resolve_fq_within_fy(row: dict[str, Any], fy_resolution: dict[str, Any], profile: dict[str, Any], anchors: dict[int, date], placements: dict[tuple[int, int], str]) -> dict[str, Any]:
    observed = parse_date(row.get("period_end"))
    if not observed or not fy_resolution.get("fiscal_year"):
        return {"d5_slot_available": 0, "d5_reason": "fy_uncertain"}
    fy = int(fy_resolution["fiscal_year"])
    calendar_type = str(row.get("calendar_type") or profile.get("calendar_type") or "UNKNOWN")
    start = fy_resolution["interval_start"]
    end = fy_resolution["interval_end_exclusive"]
    if isinstance(start, str):
        start = date.fromisoformat(start)
    if isinstance(end, str):
        end = date.fromisoformat(end)
    if calendar_type in {"CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR"}:
        starts = month_quarter_starts(start, end)
        reason = "fy_interval_then_month_quarter"
        confidence = "FY_EXACT_FQ_HIGH" if fy_resolution["confidence"] == "FY_EXACT" else "FY_HIGH_FQ_HIGH"
    elif calendar_type == "WEEK_BASED_52_53":
        placement = placements.get((int(row["company_id"]), fy), "EXTRA_WEEK_AMBIGUOUS")
        slots = week_slots(fy, profile, anchors, placement)
        if not slots.get("available"):
            return {"d5_slot_available": 0, "d5_reason": "week_slots_unavailable"}
        starts = slots["starts"]
        reason = f"fy_interval_then_week_quarter_{placement.lower()}"
        confidence = "FY_EXACT_FQ_EXACT" if fy_resolution["confidence"] == "FY_EXACT" and placement != "EXTRA_WEEK_AMBIGUOUS" else "FY_HIGH_FQ_AMBIGUOUS"
    else:
        return {"d5_slot_available": 0, "d5_reason": "other_conservative_review"}

    idx = None
    for i in range(4):
        if starts[i] <= observed < starts[i + 1]:
            idx = i
            break
    if idx is None:
        idx = min(range(4), key=lambda i: abs((observed - (starts[i + 1] - timedelta(days=1))).days))
    inferred_fq = f"Q{idx + 1}"
    expected_end = starts[idx + 1] - timedelta(days=1)
    proposed_fq = str(row["fiscal_quarter"])
    proposed_idx = Q_INDEX.get(proposed_fq)
    proposed_end = starts[proposed_idx + 1] - timedelta(days=1) if proposed_idx is not None else expected_end
    next_q_end = starts[proposed_idx + 2] - timedelta(days=1) if proposed_idx is not None and proposed_idx < 3 else ""
    return {
        "d5_slot_available": 1,
        "d5_inferred_fiscal_year": fy,
        "d5_inferred_fiscal_quarter": inferred_fq,
        "d5_interval_start": start.isoformat(),
        "d5_interval_end_exclusive": end.isoformat(),
        "d5_expected_period_end": proposed_end.isoformat(),
        "d5_actual_slot_expected_period_end": expected_end.isoformat(),
        "d5_expected_next_q_end": next_q_end.isoformat() if next_q_end else "",
        "d5_offset_days": (observed - proposed_end).days,
        "d5_abs_offset_days": abs((observed - proposed_end).days),
        "d5_actual_slot_offset_days": (observed - expected_end).days,
        "d5_reason": reason,
        "d5_confidence": confidence,
    }


def resolve_row(row: dict[str, Any], profile: dict[str, Any], anchors: dict[int, date], placements: dict[tuple[int, int], str]) -> dict[str, Any]:
    observed = parse_date(row.get("period_end"))
    if not observed:
        return {"d5_slot_available": 0, "d5_reason": "missing_period_end"}
    fy_resolution = resolve_issuer_fiscal_year_for_date(profile, anchors, observed, str(row.get("transition_state") or "STABLE_CALENDAR"))
    fq_resolution = resolve_fq_within_fy(row, fy_resolution, profile, anchors, placements)
    return {
        "d5_fy_interval_fiscal_year": fy_resolution.get("fiscal_year", ""),
        "d5_fy_interval_start": fy_resolution.get("interval_start", ""),
        "d5_fy_interval_end_exclusive": fy_resolution.get("interval_end_exclusive", ""),
        "d5_fy_interval_evidence_type": fy_resolution.get("evidence_type", ""),
        "d5_fy_interval_confidence": fy_resolution.get("confidence", ""),
        "d5_transition_state": fy_resolution.get("transition_state", ""),
        **fq_resolution,
    }


def d5_decision(row: dict[str, Any], window: int = 7) -> tuple[str, str]:
    reasons = set(str(row.get("current_guard_reasons") or row.get("reason_codes") or "").split("|")) - {""}
    if row.get("d5_slot_available") and row.get("d5_abs_offset_days") != "" and int(row["d5_abs_offset_days"]) <= window:
        reasons.discard("FQ_SLOT_MISMATCH")
        reasons.discard("PERIOD_END_OUTSIDE_SLOT")
        reasons.discard("MONTH_END_NORMALIZATION_SUSPECT")
    if row.get("d5_inferred_fiscal_year") not in ("", None) and int(row["d5_inferred_fiscal_year"]) == int(row["fiscal_year"]):
        reasons.discard("FY_SHIFT_PLUS_ONE")
        reasons.discard("FY_SHIFT_MINUS_ONE")
        reasons.discard("EXACT_FY_ANCHOR_CONFLICT")
    if row.get("ticker") in KNOWN_P1 and row.get("current_guard_decision") == "BLOCK":
        return ("BLOCK", "|".join(sorted(reasons))) if reasons else ("REVIEW", "KNOWN_P1_STRUCTURAL_DEFECT_REVIEW")
    if reasons.intersection({"FY_SHIFT_PLUS_ONE", "FY_SHIFT_MINUS_ONE", "EXACT_FY_ANCHOR_CONFLICT", "TARGET_IDENTITY_COLLISION", "REVERSE_SEQUENCE"}):
        return "BLOCK", "|".join(sorted(reasons))
    if reasons:
        return "REVIEW", "|".join(sorted(reasons))
    return "PASS", ""


def enrich_d5(population: list[dict[str, Any]], profiles: dict[int, dict[str, Any]], anchors: dict[int, dict[int, date]], placements: dict[tuple[int, int], str]) -> list[dict[str, Any]]:
    out = []
    for row in population:
        enriched = {**row, **resolve_row(row, profiles.get(int(row["company_id"]), {}), anchors.get(int(row["company_id"]), {}), placements)}
        decision, reasons = d5_decision(enriched)
        enriched["d5_simulated_decision"] = decision
        enriched["d5_simulated_reasons"] = reasons
        out.append(enriched)
    return out


def pct(count: int, total: int) -> float:
    return round(count * 100 / total, 4) if total else 0.0


def identity_summary(rows_: list[dict[str, Any]], prefix: str) -> dict[str, float]:
    total = len(rows_)
    fy = sum(1 for r in rows_ if str(r.get(f"{prefix}_inferred_fiscal_year")) == str(r["fiscal_year"]))
    fq = sum(1 for r in rows_ if str(r.get(f"{prefix}_inferred_fiscal_quarter")) == str(r["fiscal_quarter"]))
    both = sum(1 for r in rows_ if str(r.get(f"{prefix}_inferred_fiscal_year")) == str(r["fiscal_year"]) and str(r.get(f"{prefix}_inferred_fiscal_quarter")) == str(r["fiscal_quarter"]))
    return {"fy_agree_pct": pct(fy, total), "fq_agree_pct": pct(fq, total), "both_agree_pct": pct(both, total)}


def offset_summary(rows_: list[dict[str, Any]], key: str) -> dict[str, Any]:
    values = [int(r[key]) for r in rows_ if r.get(key) not in ("", None)]
    if not values:
        return {"median_abs": "", "abs_p90": "", "abs_p95": "", "abs_p99": ""}
    dist = distribution(values)
    return {"median_abs": median(abs(v) for v in values), "abs_p90": dist["abs_p90"], "abs_p95": dist["abs_p95"], "abs_p99": dist["abs_p99"], "min": min(values), "max": max(values)}


def band(value: int) -> str:
    av = abs(value)
    sign = "-" if value < 0 else "+"
    for name, lo, hi in (("90_DAY", 85, 96), ("180_DAY", 175, 187), ("270_DAY", 265, 276), ("365_DAY", 360, 367), ("371_DAY", 368, 374)):
        if lo <= av <= hi:
            return sign + name
    return "OTHER"


def offset_modes(rows_: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    counts = Counter(band(int(r[key])) for r in rows_ if r.get(key) not in ("", None))
    return [{"offset_mode": mode, "rows": count} for mode, count in sorted(counts.items())]


def classify_root_cause(row: dict[str, Any]) -> str:
    stored_fy_ok = str(row.get("d5_inferred_fiscal_year")) == str(row.get("fiscal_year"))
    stored_fq_ok = str(row.get("d5_inferred_fiscal_quarter")) == str(row.get("fiscal_quarter"))
    d4_fy_ok = str(row.get("new_inferred_fiscal_year")) == str(row.get("fiscal_year"))
    if row.get("d5_fy_interval_confidence") == "FY_UNCERTAIN":
        return "ANCHOR_PROPAGATION_ERROR" if row.get("calendar_type") == "WEEK_BASED_52_53" else "KNOWN_GOOD_LABEL_NOT_STRUCTURALLY_SUPPORTED"
    if row.get("transition_state") not in ("", None, "STABLE_CALENDAR"):
        return "POSSIBLE_TRANSITION"
    if not stored_fy_ok and row.get("d5_actual_slot_offset_days") not in ("", None) and abs(int(row["d5_actual_slot_offset_days"])) <= 7:
        return "KNOWN_GOOD_LABEL_NOT_STRUCTURALLY_SUPPORTED"
    if stored_fy_ok and not d4_fy_ok:
        return "FY_INTERVAL_ASSIGNMENT_ERROR"
    if stored_fy_ok and not stored_fq_ok:
        return "FQ_WITHIN_CORRECT_FY_ERROR"
    if not stored_fy_ok and row.get("calendar_type") == "OTHER_VERIFIED":
        return "KNOWN_GOOD_LABEL_NOT_STRUCTURALLY_SUPPORTED"
    if not stored_fy_ok:
        return "FY_INTERVAL_ASSIGNMENT_ERROR"
    if row.get("d5_abs_offset_days") not in ("", None) and int(row["d5_abs_offset_days"]) > 7:
        return "EXPECTED_END_CALCULATION_ERROR"
    return "OTHER"


def publish_summary(rows_: list[dict[str, Any]], next_key: str) -> dict[str, Any]:
    with_publish = [r for r in rows_ if r.get("publish_date")]
    after = before = both = 0
    for row in with_publish:
        publish = parse_date(row.get("publish_date"))
        period = parse_date(row.get("period_end"))
        next_end = parse_date(row.get(next_key))
        if publish and period and publish > period:
            after += 1
            if next_end and publish < next_end:
                before += 1
                both += 1
    total = len(with_publish)
    return {"rows": total, "after_period_pct": pct(after, total), "before_next_pct": pct(before, total), "both_pct": pct(both, total)}


def current_effect(rows_: list[dict[str, Any]], pred: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
    cohort = [r for r in rows_ if pred(r)]
    old = Counter(r["guard_decision"] for r in cohort)
    new = Counter(r["d5_simulated_decision"] for r in cohort)
    return {"rows": len(cohort), "old_BLOCK": old.get("BLOCK", 0), "new_BLOCK": new.get("BLOCK", 0), "new_REVIEW": new.get("REVIEW", 0), "affected_tickers": len({r["ticker"] for r in cohort if r["d5_simulated_decision"] in {"BLOCK", "REVIEW"}})}


def distribution_rows(rows_: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return [{"value": value, "rows": count} for value, count in sorted(Counter(str(r.get(key) or "UNKNOWN") for r in rows_).items())]


def by_calendar_type(rows_: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for typ in CAL_TYPES:
        group = [r for r in rows_ if r.get("calendar_type") == typ]
        out.append({"calendar_type": typ, "rows": len(group)})
    return out


def by_reason_root(rows_: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str]] = Counter()
    for row in rows_:
        for reason in str(row.get("new_simulated_reasons") or "").split("|"):
            if reason:
                counts[(str(row.get("calendar_type")), reason, row["d5_root_cause"])] += 1
    return [{"calendar_type": k[0], "block_reason": k[1], "root_cause": k[2], "rows": v} for k, v in sorted(counts.items())]


def run_phase8d5(paths: Phase8D5Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    before_fp = semantic_fingerprints(paths.v3_db)
    population = read_csv_dicts(paths.phase8d4_root / "known_good_new_guard_simulation.csv")
    p1_population = read_csv_dicts(paths.phase8d4_root / "known_P1_old_vs_new_replay.csv")
    d4_summary = json.loads((paths.phase8d4_root / "phase8d4_summary.json").read_text(encoding="utf-8"))
    high_old = read_csv_dicts(paths.phase8d3_root / "recent_known_good_high.csv")
    audit = read_csv_dicts(paths.phase8d1_root / "full_canonical_fiscal_guard_audit.csv")
    with connect_ro(paths.v3_db) as conn:
        profiles = load_profiles(conn)
        anchors = load_anchors(conn)
    placements = resolve_extra_week(population, profiles, anchors)
    high = [r for r in enrich_d5(population, profiles, anchors, placements) if r["calibration_confidence"] == "KNOWN_GOOD_HIGH"]
    residual = [r for r in high if r.get("new_simulated_decision") == "BLOCK"]
    for row in residual:
        row["d5_root_cause"] = classify_root_cause(row)
        row["d5_offset_mode"] = band(int(row["new_offset_days"])) if row.get("new_offset_days") not in ("", None) else "OTHER"
    enriched_audit = enrich_d5(mark_latest_quarter(audit), profiles, anchors, placements)
    after_fp = semantic_fingerprints(paths.v3_db)

    old_identity = identity_summary(high, "new")
    new_identity = identity_summary(high, "d5")
    old_offsets = offset_summary(high, "new_offset_days")
    new_offsets = offset_summary(high, "d5_offset_days")
    old_guard = Counter(r["new_simulated_decision"] for r in high)
    new_guard = Counter(r["d5_simulated_decision"] for r in high)
    p1 = [r for r in enrich_d5(p1_population, profiles, anchors, placements) if r["ticker"] in KNOWN_P1]
    p1_counts = Counter(r["d5_simulated_decision"] for r in p1)
    p1_silent = sum(1 for r in p1 if r.get("current_guard_decision") == "BLOCK" and r["d5_simulated_decision"] == "PASS")
    current = {
        "2024plus": current_effect(enriched_audit, lambda r: int(r["fiscal_year"]) >= 2024),
        "2025plus": current_effect(enriched_audit, lambda r: int(r["fiscal_year"]) >= 2025),
        "latest8q": current_effect(enriched_audit, lambda r: int(r.get("latest8q") or 0)),
        "latest4q": current_effect(enriched_audit, lambda r: int(r.get("latest4q") or 0)),
        "latest_quarter": current_effect(enriched_audit, lambda r: int(r.get("latest_quarter") or 0)),
        "ttm_input": current_effect(enriched_audit, lambda r: int(r.get("ttm_input") or 0)),
    }
    fixed = [r for r in high if r["calendar_type"] == "FIXED_DATE_FISCAL_YEAR"]
    week_residual = [r for r in residual if r["calendar_type"] == "WEEK_BASED_52_53"]
    classification = CLASSIFICATION_READY
    if new_identity["fy_agree_pct"] <= old_identity["fy_agree_pct"] or new_identity["fq_agree_pct"] < old_identity["fq_agree_pct"] - 0.1 or new_guard["BLOCK"] >= old_guard["BLOCK"] or p1_silent:
        classification = CLASSIFICATION_MORE_WORK
    if new_identity["fy_agree_pct"] <= old_identity["fy_agree_pct"] and new_guard["BLOCK"] >= old_guard["BLOCK"]:
        classification = CLASSIFICATION_REJECTED

    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "population": {"known_good_high_old": len(high_old), "known_good_high_new": len(high), "exact_same_population": int({r["quarter_id"] for r in high_old} == {r["quarter_id"] for r in high})},
        "residual": {
            "phase8d4_block": len(residual),
            "by_calendar_type": {r["calendar_type"]: sum(1 for x in residual if x["calendar_type"] == r["calendar_type"]) for r in residual},
            "root_causes": dict(Counter(r["d5_root_cause"] for r in residual)),
            "offset_modes": dict(Counter(r["d5_offset_mode"] for r in residual)),
        },
        "identity": {
            "phase8d4": old_identity,
            "phase8d5": new_identity,
            "fy_improvement": round(new_identity["fy_agree_pct"] - old_identity["fy_agree_pct"], 4),
            "fq_change": round(new_identity["fq_agree_pct"] - old_identity["fq_agree_pct"], 4),
        },
        "period_end": {"phase8d4": old_offsets, "phase8d5": new_offsets},
        "fixed_date": {
            "rows": len(fixed),
            "old_plus_minus_7": pct(sum(1 for r in fixed if abs(int(r["new_offset_days"])) <= 7), len(fixed)),
            "new_plus_minus_7": pct(sum(1 for r in fixed if abs(int(r["d5_offset_days"])) <= 7), len(fixed)),
            "old_identity_agreement": identity_summary(fixed, "new"),
            "new_identity_agreement": identity_summary(fixed, "d5"),
        },
        "week_based": {
            "residual_count": len(week_residual),
            "fy_assignment_improvements": sum(1 for r in high if r["calendar_type"] == "WEEK_BASED_52_53" and str(r.get("new_inferred_fiscal_year")) != str(r["fiscal_year"]) and str(r.get("d5_inferred_fiscal_year")) == str(r["fiscal_year"])),
            "unresolved_older_inference": sum(1 for r in high if r["calendar_type"] == "WEEK_BASED_52_53" and r.get("d5_fy_interval_confidence") == "FY_UNCERTAIN"),
            "transition_cases": sum(1 for r in high if r["calendar_type"] == "WEEK_BASED_52_53" and r.get("transition_state") not in ("", None, "STABLE_CALENDAR")),
        },
        "publish": {"phase8d4": publish_summary(high, "new_expected_next_q_end"), "phase8d5": publish_summary(high, "d5_expected_next_q_end")},
        "guard": {"phase8d4": dict(old_guard), "phase8d5": dict(new_guard), "old_block_pct": pct(old_guard["BLOCK"], len(high)), "new_block_pct": pct(new_guard["BLOCK"], len(high))},
        "p1": {"tickers_replayed": len({r["ticker"] for r in p1}), "BLOCK": p1_counts["BLOCK"], "REVIEW": p1_counts["REVIEW"], "PASS": p1_counts["PASS"], "high_confidence_structural_p1_pass": p1_silent},
        "current": current,
        "current_phase8d4_baseline": {
            name: {
                "phase8d4_BLOCK": d4_summary["current"][name]["new_BLOCK"],
                "phase8d4_REVIEW": d4_summary["current"][name]["new_REVIEW"],
                "phase8d4_affected_tickers": d4_summary["current"][name]["affected_tickers"],
            }
            for name in current
        },
        "safety": {"production_writes": 0, "active_guard_changed": "NO", "fingerprints_unchanged": "YES" if before_fp == after_fp else "NO"},
        "next_action": "DO NOT REPAIR CANONICAL DATA YET; RUN A FULL 72,713-ROW REHEARSAL WITH THE REFINED FY-FIRST/FQ-SECOND RESOLVER AND COMPARE CURRENT/RECENT FALSE-BLOCK RISK BEFORE ACTIVATION"
        if classification == CLASSIFICATION_READY
        else "DO NOT REPAIR CANONICAL DATA; REFINE ONLY THE REMAINING CALENDAR-TYPE / INTERVAL FAILURE MODE AND REPEAT THE SAME KNOWN-GOOD TEST"
        if classification == CLASSIFICATION_MORE_WORK
        else "KEEP THE PHASE 8D-4 CANDIDATE MODEL UNCHANGED AND INVESTIGATE A DIFFERENT ROOT CAUSE",
    }

    write_outputs(paths, high, residual, enriched_audit, p1, summary)
    write_doc(summary)
    return summary


def write_outputs(paths: Phase8D5Paths, high: list[dict[str, Any]], residual: list[dict[str, Any]], enriched_audit: list[dict[str, Any]], p1: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    write_json(paths.artifact_root / "calibration_population_reuse.json", summary["population"])
    write_csv(paths.artifact_root / "known_good_562_residuals.csv", residual)
    write_csv(paths.artifact_root / "residual_root_cause_distribution.csv", [{"root_cause": k, "rows": v} for k, v in sorted(Counter(r["d5_root_cause"] for r in residual).items())])
    write_csv(paths.artifact_root / "residual_by_calendar_type.csv", by_calendar_type(residual))
    write_csv(paths.artifact_root / "residual_by_reason.csv", by_reason_root(residual))
    write_csv(paths.artifact_root / "fiscal_year_interval_resolution.csv", high)
    write_csv(paths.artifact_root / "old_vs_new_fiscal_year_assignment.csv", [{"quarter_id": r["quarter_id"], "ticker": r["ticker"], "stored_fiscal_year": r["fiscal_year"], "phase8d4_inferred_fiscal_year": r.get("new_inferred_fiscal_year"), "phase8d5_inferred_fiscal_year": r.get("d5_inferred_fiscal_year"), "period_end": r["period_end"], "calendar_type": r["calendar_type"]} for r in high])
    write_csv(paths.artifact_root / "fy_interval_confidence_distribution.csv", distribution_rows(high, "d5_fy_interval_confidence"))
    write_csv(paths.artifact_root / "residual_offset_modes.csv", offset_modes(residual, "new_offset_days"))
    write_csv(paths.artifact_root / "offset_mode_root_causes.csv", [{"offset_mode": k[0], "root_cause": k[1], "rows": v} for k, v in sorted(Counter((r["d5_offset_mode"], r["d5_root_cause"]) for r in residual).items())])
    write_csv(paths.artifact_root / "fixed_date_fy_interval_analysis.csv", [r for r in high if r["calendar_type"] == "FIXED_DATE_FISCAL_YEAR"])
    write_csv(paths.artifact_root / "fixed_date_old_vs_new_identity.csv", [r for r in high if r["calendar_type"] == "FIXED_DATE_FISCAL_YEAR"])
    write_csv(paths.artifact_root / "week_based_residual_fy_analysis.csv", [r for r in residual if r["calendar_type"] == "WEEK_BASED_52_53"])
    write_csv(paths.artifact_root / "known_good_old_vs_new_identity.csv", high)
    write_csv(paths.artifact_root / "known_good_old_vs_new_guard.csv", high)
    write_csv(paths.artifact_root / "known_good_period_end_tail_comparison.csv", [{"metric": k, "phase8d4": summary["period_end"]["phase8d4"].get(k), "phase8d5": summary["period_end"]["phase8d5"].get(k)} for k in sorted(set(summary["period_end"]["phase8d4"]) | set(summary["period_end"]["phase8d5"]))])
    write_csv(paths.artifact_root / "publish_chronology_old_vs_new.csv", [{"metric": k, "phase8d4": summary["publish"]["phase8d4"].get(k), "phase8d5": summary["publish"]["phase8d5"].get(k)} for k in summary["publish"]["phase8d5"]])
    write_csv(paths.artifact_root / "known_P1_phase8d4_vs_phase8d5.csv", p1)
    write_csv(paths.artifact_root / "current_era_phase8d4_vs_phase8d5.csv", [r for r in enriched_audit if int(r["fiscal_year"]) >= 2024])
    write_csv(paths.artifact_root / "latest_quarter_phase8d4_vs_phase8d5.csv", [r for r in enriched_audit if int(r.get("latest_quarter") or 0)])
    write_csv(paths.artifact_root / "current_ttm_phase8d4_vs_phase8d5.csv", [r for r in enriched_audit if int(r.get("ttm_input") or 0)])
    write_json(paths.artifact_root / "phase8d5_summary.json", summary)
    (paths.artifact_root / "fy_interval_recommendation.md").write_text(summary["classification"] + "\n", encoding="utf-8")
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")


def write_doc(summary: dict[str, Any]) -> None:
    block = f"""## Phase 8D-5 - Fiscal-Year Interval Assignment Refinement

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Phase 8D-5 decomposed the Phase 8D-4 known-good residual BLOCK population and separated fiscal-year interval assignment from fiscal-quarter slot assignment. The candidate resolver now resolves issuer FY first from exact adjacent anchors or stable backward intervals, then assigns Q1-Q4 inside that FY interval. FY2026/FY2027 anchors remain authoritative and the refined resolver is not active in production.

Known-good population reuse: old `{summary['population']['known_good_high_old']}`, new `{summary['population']['known_good_high_new']}`, exact same population `{bool(summary['population']['exact_same_population'])}`. Phase 8D-4 residual BLOCK rows `{summary['residual']['phase8d4_block']}`.

Identity agreement: FY `{summary['identity']['phase8d4']['fy_agree_pct']}% -> {summary['identity']['phase8d5']['fy_agree_pct']}%`, FQ `{summary['identity']['phase8d4']['fq_agree_pct']}% -> {summary['identity']['phase8d5']['fq_agree_pct']}%`, combined `{summary['identity']['phase8d4']['both_agree_pct']}% -> {summary['identity']['phase8d5']['both_agree_pct']}%`.

Period-end tails: median abs `{summary['period_end']['phase8d4']['median_abs']} -> {summary['period_end']['phase8d5']['median_abs']}`, P90 `{summary['period_end']['phase8d4']['abs_p90']} -> {summary['period_end']['phase8d5']['abs_p90']}`, P95 `{summary['period_end']['phase8d4']['abs_p95']} -> {summary['period_end']['phase8d5']['abs_p95']}`, P99 `{summary['period_end']['phase8d4']['abs_p99']} -> {summary['period_end']['phase8d5']['abs_p99']}`.

Known-good guard simulation: BLOCK `{summary['guard']['phase8d4'].get('BLOCK', 0)}` (`{summary['guard']['old_block_pct']}%`) -> `{summary['guard']['phase8d5'].get('BLOCK', 0)}` (`{summary['guard']['new_block_pct']}%`), REVIEW `{summary['guard']['phase8d5'].get('REVIEW', 0)}`, WARNING `{summary['guard']['phase8d5'].get('PASS_WITH_WARNING', 0)}`.

Known P1 replay: tickers `{summary['p1']['tickers_replayed']}`, BLOCK `{summary['p1']['BLOCK']}`, REVIEW `{summary['p1']['REVIEW']}`, PASS `{summary['p1']['PASS']}`, high-confidence structural P1 PASS `{summary['p1']['high_confidence_structural_p1_pass']}`.

Safety: production writes `0`; active guard changed `{summary['safety']['active_guard_changed']}`; fingerprints unchanged `{summary['safety']['fingerprints_unchanged']}`.
"""
    path = Path("docs/fundamentals_v3_phase8_update_v3.md")
    path.write_text(path.read_text(encoding="utf-8").rstrip() + "\n\n" + block, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 8D-5 fiscal-year interval assignment refinement rehearsal.")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--phase8d1-root", type=Path, default=Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL"))
    parser.add_argument("--phase8d3-root", type=Path, default=Path("temp/fundamentals_v3_phase8d3_quarter_slot_calibration/20260828T_PHASE8D3"))
    parser.add_argument("--phase8d4-root", type=Path, default=Path("temp/fundamentals_v3_phase8d4_slot_model_rework/20260828T_PHASE8D4"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase8d5_fiscal_year_interval_refinement") / utc_stamp()
    summary = run_phase8d5(Phase8D5Paths(artifact_root=artifact_root, phase8d1_root=args.phase8d1_root, phase8d3_root=args.phase8d3_root, phase8d4_root=args.phase8d4_root, v3_db=args.v3_db))
    print(f"classification={summary['classification']}")
    print(f"known_good_new_block_pct={summary['guard']['new_block_pct']}")
    print(f"fy_agreement={summary['identity']['phase8d5']['fy_agree_pct']}")
    print(f"p1_high_confidence_pass={summary['p1']['high_confidence_structural_p1_pass']}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
