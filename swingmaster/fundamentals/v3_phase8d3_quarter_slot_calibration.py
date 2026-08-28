from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable

from swingmaster.fundamentals.v3_fiscal_calendar import ANCHOR_TABLE, EXPECTED_P1_TICKERS, PROFILE_TABLE, semantic_fingerprints, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro
from swingmaster.fundamentals.v3_phase8d2_operational_risk import read_csv_dicts


CLASSIFICATION = "FUNDAMENTALS_V3_PHASE8D3_QUARTER_SLOT_CALIBRATION_COMPLETE"
RECOMMEND_CONFIRMED = "CURRENT_GUARD_SLOT_MODEL_CONFIRMED"
RECOMMEND_CALIBRATE = "CALIBRATE_PERIOD_END_WINDOW"
RECOMMEND_REWORK = "REWORK_FISCAL_SLOT_MODEL"
KNOWN_P1 = set(EXPECTED_P1_TICKERS)
WINDOWS = (3, 5, 7, 10, 14)


@dataclass(frozen=True)
class Phase8D3Paths:
    artifact_root: Path
    phase8d1_root: Path = Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL")
    v3_db: Path = Path("rc_fundamentals_v3.db")


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    fraction = rank - lo
    return ordered[lo] + (ordered[hi] - ordered[lo]) * fraction


def distribution(values: list[int]) -> dict[str, Any]:
    if not values:
        return {}
    abs_values = [abs(v) for v in values]
    return {
        "rows": len(values),
        "min": min(values),
        "p1": percentile(values, 1),
        "p5": percentile(values, 5),
        "p10": percentile(values, 10),
        "p25": percentile(values, 25),
        "median": median(values),
        "p75": percentile(values, 75),
        "p90": percentile(values, 90),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values),
        "mean": mean(values),
        "mean_absolute_deviation": mean(abs_values),
        "abs_p90": percentile(abs_values, 90),
        "abs_p95": percentile(abs_values, 95),
        "abs_p99": percentile(abs_values, 99),
    }


def coverage(values: list[int], window: int) -> dict[str, Any]:
    inside = sum(1 for v in values if abs(v) <= window)
    total = len(values)
    return {
        "window_days": window,
        "rows_inside": inside,
        "inside_pct": round(inside * 100 / total, 4) if total else 0,
        "rows_outside": total - inside,
        "outside_pct": round((total - inside) * 100 / total, 4) if total else 0,
    }


def window_needed(values: list[int], pct: float) -> int | None:
    if not values:
        return None
    ordered = sorted(abs(v) for v in values)
    idx = min(len(ordered) - 1, int(round((pct / 100) * (len(ordered) - 1))))
    return ordered[idx]


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def load_profiles(conn: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    return {int(row["company_id"]): row for row in rows(conn, f"SELECT * FROM {PROFILE_TABLE}")}


def load_anchors(conn: sqlite3.Connection) -> dict[int, dict[int, date]]:
    out: dict[int, dict[int, date]] = defaultdict(dict)
    for row in rows(conn, f"SELECT company_id,fiscal_year,fiscal_year_start_date FROM {ANCHOR_TABLE}"):
        parsed = _parse_date(row["fiscal_year_start_date"])
        if parsed:
            out[int(row["company_id"])][int(row["fiscal_year"])] = parsed
    return out


def estimate_fy_start(fiscal_year: int, profile: dict[str, Any], anchors: dict[int, date]) -> tuple[date | None, str]:
    if fiscal_year in anchors:
        return anchors[fiscal_year], "EXACT_ANCHOR"
    if not anchors:
        return None, "NO_ANCHOR"
    nearest = min(anchors, key=lambda year: (abs(year - fiscal_year), -year))
    ref = anchors[nearest]
    diff = fiscal_year - nearest
    if profile.get("calendar_type") == "WEEK_BASED_52_53":
        return ref + timedelta(days=364 * diff), "ONE_ANCHOR_INFERRED"
    try:
        return date(ref.year + diff, ref.month, ref.day), "ONE_ANCHOR_INFERRED"
    except ValueError:
        return date(ref.year + diff, ref.month, 28), "ONE_ANCHOR_INFERRED"


def expected_slot(row: dict[str, Any], profile: dict[str, Any], anchors: dict[int, date]) -> dict[str, Any]:
    fy = int(row["fiscal_year"])
    fq_idx = {"Q1": 0, "Q2": 1, "Q3": 2, "Q4": 3}[row["fiscal_quarter"]]
    start, start_confidence = estimate_fy_start(fy, profile, anchors)
    next_start, next_confidence = estimate_fy_start(fy + 1, profile, anchors)
    if not start or not next_start:
        return {"slot_available": 0}
    q_starts = [start, start + timedelta(days=91), start + timedelta(days=182), start + timedelta(days=273), next_start]
    return {
        "slot_available": 1,
        "expected_fy_start": start.isoformat(),
        "expected_next_fy_start": next_start.isoformat(),
        "fy_start_confidence": start_confidence,
        "next_fy_start_confidence": next_confidence,
        "expected_quarter_start": q_starts[fq_idx].isoformat(),
        "expected_quarter_end": (q_starts[fq_idx + 1] - timedelta(days=1)).isoformat(),
        "expected_next_quarter_end": (q_starts[min(fq_idx + 2, 4)] - timedelta(days=1)).isoformat() if fq_idx < 3 else "",
        "verified_year_length_days": (next_start - start).days if start_confidence == "EXACT_ANCHOR" and next_confidence == "EXACT_ANCHOR" else "",
    }


def latest_six_candidates(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    data = rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date AS period_end,q.publish_date,f.accepted_source_provider,
               f.revenue,f.operating_income,f.net_income,p.calendar_type,p.profile_parse_status,
               a26.fiscal_year_start_date AS fy2026_anchor,a27.fiscal_year_start_date AS fy2027_anchor
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        LEFT JOIN v3_company_fiscal_calendar_profile p ON p.company_id=c.company_id
        LEFT JOIN v3_company_fiscal_year_calendar a26 ON a26.company_id=c.company_id AND a26.fiscal_year=2026
        LEFT JOIN v3_company_fiscal_year_calendar a27 ON a27.company_id=c.company_id AND a27.fiscal_year=2027
        WHERE c.active=1 AND q.period_end_date IS NOT NULL
        ORDER BY c.ticker,q.period_end_date DESC,q.fiscal_year DESC,q.fiscal_quarter DESC
        """,
    )
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in data:
        grouped[int(row["company_id"])].append(row)
    return [row for group in grouped.values() for row in group[:6]]


def sequence_quality(rows_for_company: list[dict[str, Any]]) -> str:
    ordered = sorted(rows_for_company, key=lambda r: (int(r["fiscal_year"]), r["fiscal_quarter"]))
    prev = None
    for row in ordered:
        period = _parse_date(row["period_end"])
        if prev and period and period < prev:
            return "REVERSE_PERIOD_SEQUENCE"
        if period:
            prev = period
    return "COHERENT"


def build_calibration_population(conn: sqlite3.Connection, audit_by_qid: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = latest_six_candidates(conn)
    profiles = load_profiles(conn)
    anchors = load_anchors(conn)
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        by_company[int(row["company_id"])].append(row)
    quality_by_company = {cid: sequence_quality(group) for cid, group in by_company.items()}
    out = []
    for row in candidates:
        audit = audit_by_qid.get(int(row["quarter_id"]), {})
        profile = profiles.get(int(row["company_id"]), {})
        slot = expected_slot(row, profile, anchors.get(int(row["company_id"]), {}))
        known_p1 = row["ticker"] in KNOWN_P1
        source = row.get("accepted_source_provider") or "UNKNOWN"
        sequence = quality_by_company[int(row["company_id"])]
        if known_p1 or sequence != "COHERENT" or not slot.get("slot_available"):
            confidence = "EXCLUDED_STRUCTURAL_RISK"
        elif source == "YAHOO" and row.get("publish_date"):
            confidence = "KNOWN_GOOD_HIGH"
        elif source in {"YAHOO", "V2", "LEGACY", "SEC"}:
            confidence = "KNOWN_GOOD_MEDIUM"
        else:
            confidence = "KNOWN_GOOD_MEDIUM"
        actual = _parse_date(row["period_end"])
        expected = _parse_date(slot.get("expected_quarter_end"))
        offset = (actual - expected).days if actual and expected else None
        publish = _parse_date(row.get("publish_date"))
        next_end = _parse_date(slot.get("expected_next_quarter_end"))
        lag = (publish - actual).days if publish and actual else None
        if not publish or not actual:
            chronology = "INSUFFICIENT"
        elif publish <= actual:
            chronology = "PUBLISH_BEFORE_OR_ON_PERIOD_END"
        elif next_end and publish < next_end:
            chronology = "PUBLISH_CHRONOLOGY_STRONG_PASS"
        elif next_end:
            chronology = "PUBLISH_AFTER_NEXT_ESTIMATED_Q_END"
        else:
            chronology = "INSUFFICIENT"
        out.append({
            **row,
            "source": source,
            "source_lineage": source,
            "known_p1_status": int(known_p1),
            "sequence_quality": sequence,
            "calibration_confidence": confidence,
            "current_guard_decision": audit.get("guard_decision", ""),
            "current_guard_reasons": audit.get("reason_codes", ""),
            "current_guard_block_kind": audit.get("block_kind", ""),
            "current_guard_block_confidence": audit.get("block_confidence", ""),
            **slot,
            "period_end_offset_days": "" if offset is None else offset,
            "abs_period_end_offset_days": "" if offset is None else abs(offset),
            "reporting_lag_days": "" if lag is None else lag,
            "publish_chronology": chronology,
            "publish_before_next_q_end_plus7": int(bool(publish and next_end and publish < next_end + timedelta(days=7))),
            "publish_before_next_q_end_plus14": int(bool(publish and next_end and publish < next_end + timedelta(days=14))),
        })
    return out


def enrich_audit_offsets(conn: sqlite3.Connection, audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    profiles = load_profiles(conn)
    anchors = load_anchors(conn)
    latest_by_ticker: dict[str, dict[str, Any]] = {}
    for row in audit:
        if str(row.get("active")) != "1":
            continue
        current = latest_by_ticker.get(row["ticker"])
        if current is None or (row["period_end"], int(row["fiscal_year"]), row["fiscal_quarter"]) > (current["period_end"], int(current["fiscal_year"]), current["fiscal_quarter"]):
            latest_by_ticker[row["ticker"]] = row
    latest_qids = {str(row["quarter_id"]) for row in latest_by_ticker.values()}
    out = []
    for row in audit:
        profile = profiles.get(int(row["company_id"]), {})
        slot = expected_slot(row, profile, anchors.get(int(row["company_id"]), {}))
        actual = _parse_date(row.get("period_end"))
        expected = _parse_date(slot.get("expected_quarter_end"))
        offset = (actual - expected).days if actual and expected else None
        out.append({
            **row,
            **{f"sim_{key}": value for key, value in slot.items()},
            "period_end_offset_days": "" if offset is None else offset,
            "abs_period_end_offset_days": "" if offset is None else abs(offset),
            "current_guard_decision": row.get("guard_decision", ""),
            "current_guard_reasons": row.get("reason_codes", ""),
            "latest_quarter": int(str(row["quarter_id"]) in latest_qids),
        })
    return out


def period_rows(population: list[dict[str, Any]]) -> list[dict[str, Any]]:
    high = [row for row in population if row["calibration_confidence"] == "KNOWN_GOOD_HIGH" and row["period_end_offset_days"] != ""]
    values = [int(row["period_end_offset_days"]) for row in high]
    return [{**distribution(values), "population": "KNOWN_GOOD_HIGH"}]


def group_distribution(population: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for row in population:
        if row["calibration_confidence"] == "KNOWN_GOOD_HIGH" and row["period_end_offset_days"] != "":
            grouped[str(row.get(key) or "UNKNOWN")].append(int(row["period_end_offset_days"]))
    out = []
    for value, vals in sorted(grouped.items()):
        dist = distribution(vals)
        out.append({
            key: value,
            "rows": len(vals),
            "median_abs_offset": median(abs(v) for v in vals),
            "abs_p90": dist["abs_p90"],
            "abs_p95": dist["abs_p95"],
            "abs_p99": dist["abs_p99"],
            "plus_minus_7_coverage_pct": coverage(vals, 7)["inside_pct"],
            "plus_minus_14_coverage_pct": coverage(vals, 14)["inside_pct"],
            "median_signed_offset": dist["median"],
        })
    return out


def publish_lag_rows(population: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lags = [int(row["reporting_lag_days"]) for row in population if row["calibration_confidence"] == "KNOWN_GOOD_HIGH" and row["reporting_lag_days"] != ""]
    return [{**{k: v for k, v in distribution(lags).items() if k in {"rows", "p10", "p25", "median", "p75", "p90", "p95", "max"}}, "population": "KNOWN_GOOD_HIGH"}]


def chronology_summary(population: list[dict[str, Any]]) -> dict[str, Any]:
    high = [row for row in population if row["calibration_confidence"] == "KNOWN_GOOD_HIGH" and row.get("publish_date")]
    after_period = [row for row in high if row["reporting_lag_days"] != "" and int(row["reporting_lag_days"]) > 0]
    strict = [row for row in after_period if row["publish_chronology"] == "PUBLISH_CHRONOLOGY_STRONG_PASS"]
    plus7 = [row for row in after_period if int(row["publish_before_next_q_end_plus7"])]
    plus14 = [row for row in after_period if int(row["publish_before_next_q_end_plus14"])]
    total = len(high)
    return {
        "rows_with_publish_date": total,
        "publish_after_period_end_pct": round(len(after_period) * 100 / total, 4) if total else 0,
        "publish_before_next_estimated_q_end_pct": round(len(strict) * 100 / total, 4) if total else 0,
        "both_pct": round(len(strict) * 100 / total, 4) if total else 0,
        "both_plus7_pct": round(len(plus7) * 100 / total, 4) if total else 0,
        "both_plus14_pct": round(len(plus14) * 100 / total, 4) if total else 0,
        "outliers": total - len(strict),
    }


def simulated_decision(row: dict[str, Any], window: int) -> str:
    if row["period_end_offset_days"] == "":
        return row.get("current_guard_decision") or row.get("guard_decision") or "PASS_WITH_WARNING"
    reasons = set(str(row.get("current_guard_reasons") or row.get("reason_codes") or "").split("|")) - {""}
    if abs(int(row["period_end_offset_days"])) <= window:
        reasons.discard("FQ_SLOT_MISMATCH")
        reasons.discard("PERIOD_END_OUTSIDE_SLOT")
        reasons.discard("MONTH_END_NORMALIZATION_SUSPECT")
    if reasons.intersection({"FY_SHIFT_PLUS_ONE", "FY_SHIFT_MINUS_ONE", "EXACT_FY_ANCHOR_CONFLICT", "TARGET_IDENTITY_COLLISION", "REVERSE_SEQUENCE"}):
        return "BLOCK"
    if reasons:
        return "REVIEW" if "PUBLISH_SEQUENCE_MISMATCH" in reasons else "PASS_WITH_WARNING"
    return "PASS"


def simulate(population: list[dict[str, Any]], audit: list[dict[str, Any]], window: int) -> dict[str, Any]:
    high = [row for row in population if row["calibration_confidence"] == "KNOWN_GOOD_HIGH"]
    high_counts = Counter(simulated_decision(row, window) for row in high)
    def cohort(name: str, pred) -> dict[str, int]:
        return Counter(simulated_decision(row, window) for row in audit if pred(row))
    p1_rows = [row for row in population if row["ticker"] in KNOWN_P1]
    p1_counts = Counter(simulated_decision(row, window) for row in p1_rows)
    return {
        "model": f"plus_minus_{window}",
        "known_good_rows": len(high),
        "known_good_BLOCK": high_counts.get("BLOCK", 0),
        "known_good_BLOCK_pct": round(high_counts.get("BLOCK", 0) * 100 / len(high), 4) if high else 0,
        "known_good_REVIEW": high_counts.get("REVIEW", 0),
        "known_good_PASS_WARN": high_counts.get("PASS", 0) + high_counts.get("PASS_WITH_WARNING", 0),
        "latest_quarter_BLOCK": cohort("latest_quarter", lambda r: int(r.get("latest_quarter", 0))).get("BLOCK", 0),
        "latest4Q_BLOCK": cohort("latest4q", lambda r: int(r.get("latest4q", 0))).get("BLOCK", 0),
        "latest8Q_BLOCK": cohort("latest8q", lambda r: int(r.get("latest8q", 0))).get("BLOCK", 0),
        "current_TTM_input_BLOCK": cohort("ttm", lambda r: int(r.get("ttm_input", 0))).get("BLOCK", 0),
        "known_P1_BLOCK_or_REVIEW": p1_counts.get("BLOCK", 0) + p1_counts.get("REVIEW", 0),
        "known_P1_incorrect_PASS": p1_counts.get("PASS", 0),
    }


def week_based_analysis(population: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    high_week = [row for row in population if row["calibration_confidence"] == "KNOWN_GOOD_HIGH" and row["calendar_type"] == "WEEK_BASED_52_53"]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in high_week:
        grouped[(row["ticker"], str(row["fiscal_year"]))].append(row)
    lengths = []
    placements = []
    for (ticker, fy), group in grouped.items():
        ordered = sorted(group, key=lambda r: r["fiscal_quarter"])
        prev_end = None
        year_length = ""
        extra = []
        for row in ordered:
            period = _parse_date(row["period_end"])
            if row.get("verified_year_length_days"):
                year_length = row["verified_year_length_days"]
            if period:
                if prev_end:
                    days = (period - prev_end).days
                else:
                    start = _parse_date(row.get("expected_fy_start"))
                    days = (period - start).days + 1 if start else 0
                bucket = "14 weeks" if 94 <= days <= 101 else "13 weeks" if 87 <= days <= 93 else "12 weeks" if 80 <= days <= 86 else "other"
                if bucket == "14 weeks":
                    extra.append(row["fiscal_quarter"])
                lengths.append({"ticker": ticker, "fiscal_year": fy, "fiscal_quarter": row["fiscal_quarter"], "observed_length_days": days, "length_bucket": bucket, "verified_year_length_days": year_length or "inferred"})
                prev_end = period
        placements.append({"ticker": ticker, "fiscal_year": fy, "verified_year_length_days": year_length or "inferred", "extra_week_placement": extra[0] if len(extra) == 1 else "ambiguous"})
    return lengths, placements


def write_doc(summary: dict[str, Any]) -> None:
    block = f"""## Phase 8D-3 - Empirical Fiscal Quarter-End / Publish-Date Calibration

Status: `{CLASSIFICATION}`

Artifact root: `{summary['artifact_root']}`

Calibration population: active tickers `{summary['population']['active_tickers']}`, recent rows considered `{summary['population']['recent_rows_considered']}`, KNOWN_GOOD_HIGH `{summary['population']['known_good_high_rows']}`, KNOWN_GOOD_MEDIUM `{summary['population']['known_good_medium_rows']}`, excluded structural risk `{summary['population']['excluded_rows']}`.

Period-end offsets for KNOWN_GOOD_HIGH: median signed `{summary['period_end_offsets']['median_signed_offset']}`, median absolute `{summary['period_end_offsets']['median_abs_offset']}`, abs P90 `{summary['period_end_offsets']['abs_p90']}`, abs P95 `{summary['period_end_offsets']['abs_p95']}`, abs P99 `{summary['period_end_offsets']['abs_p99']}`. Window coverage: ±7 `{summary['window_coverage']['plus_minus_7_pct']}%`, ±14 `{summary['window_coverage']['plus_minus_14_pct']}%`.

Publish chronology: rows with publish_date `{summary['publish_chronology']['rows_with_publish_date']}`, publish after period_end `{summary['publish_chronology']['publish_after_period_end_pct']}%`, strict next-quarter chronology `{summary['publish_chronology']['both_pct']}%`, +7 tolerance `{summary['publish_chronology']['both_plus7_pct']}%`, +14 tolerance `{summary['publish_chronology']['both_plus14_pct']}%`.

Current guard on KNOWN_GOOD_HIGH: PASS `{summary['current_guard_on_known_good']['PASS']}`, PASS_WITH_WARNING `{summary['current_guard_on_known_good']['PASS_WITH_WARNING']}`, REVIEW `{summary['current_guard_on_known_good']['REVIEW']}`, BLOCK `{summary['current_guard_on_known_good']['BLOCK']}` (`{summary['current_guard_on_known_good']['block_pct']}%`).

Recommendation: `{summary['recommendation']}`. Guard behavior was not changed; production writes were `0`.
"""
    path = Path("docs/fundamentals_v3_phase8_update_v3.md")
    path.write_text(path.read_text(encoding="utf-8").rstrip() + "\n\n" + block, encoding="utf-8")


def run_phase8d3(paths: Phase8D3Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    audit = read_csv_dicts(paths.phase8d1_root / "full_canonical_fiscal_guard_audit.csv")
    audit_by_qid = {int(row["quarter_id"]): row for row in audit}
    before_fp = semantic_fingerprints(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        population = build_calibration_population(conn, audit_by_qid)
        enriched_audit = enrich_audit_offsets(conn, audit)
        active_tickers = rows(conn, "SELECT COUNT(*) AS rows FROM v3_company WHERE active=1")[0]["rows"]
    after_fp = semantic_fingerprints(paths.v3_db)

    high = [row for row in population if row["calibration_confidence"] == "KNOWN_GOOD_HIGH"]
    medium = [row for row in population if row["calibration_confidence"] == "KNOWN_GOOD_MEDIUM"]
    excluded = [row for row in population if row["calibration_confidence"] == "EXCLUDED_STRUCTURAL_RISK"]
    offset_values = [int(row["period_end_offset_days"]) for row in high if row["period_end_offset_days"] != ""]
    window_rows = [coverage(offset_values, w) for w in WINDOWS]
    guard_counts = Counter(row["current_guard_decision"] for row in high)
    simulations = [simulate(population, enriched_audit, window) for window in (7, 14, max(window_needed(offset_values, 99) or 14, 14))]
    week_lengths, extra_placement = week_based_analysis(population)
    chronology = chronology_summary(population)
    source_counts = Counter(row["source"] for row in population)
    p99_window = window_needed(offset_values, 99) or 14
    known_good_block_rate = round(guard_counts.get("BLOCK", 0) * 100 / len(high), 4) if high else 0
    recommendation = RECOMMEND_CALIBRATE if known_good_block_rate > 1 and simulations[1]["known_good_BLOCK"] < guard_counts.get("BLOCK", 0) else RECOMMEND_CONFIRMED
    if p99_window and p99_window > 45:
        recommendation = RECOMMEND_REWORK
    summary = {
        "classification": CLASSIFICATION,
        "artifact_root": str(paths.artifact_root),
        "population": {
            "active_tickers": active_tickers,
            "recent_rows_considered": len(population),
            "known_good_high_rows": len(high),
            "known_good_medium_rows": len(medium),
            "excluded_rows": len(excluded),
            "yahoo_current_source_rows": source_counts.get("YAHOO", 0),
            "other_source_rows": sum(v for k, v in source_counts.items() if k not in {"YAHOO", "UNKNOWN"}),
            "unknown_source_rows": source_counts.get("UNKNOWN", 0),
        },
        "period_end_offsets": {
            "median_signed_offset": median(offset_values) if offset_values else None,
            "median_abs_offset": median(abs(v) for v in offset_values) if offset_values else None,
            "abs_p90": percentile([abs(v) for v in offset_values], 90),
            "abs_p95": percentile([abs(v) for v in offset_values], 95),
            "abs_p99": percentile([abs(v) for v in offset_values], 99),
            "min": min(offset_values) if offset_values else None,
            "max": max(offset_values) if offset_values else None,
        },
        "window_coverage": {f"plus_minus_{row['window_days']}_pct": row["inside_pct"] for row in window_rows} | {"needed_95": window_needed(offset_values, 95), "needed_99": p99_window},
        "publish_chronology": chronology,
        "current_guard_on_known_good": {
            "PASS": guard_counts.get("PASS", 0),
            "PASS_WITH_WARNING": guard_counts.get("PASS_WITH_WARNING", 0),
            "REVIEW": guard_counts.get("REVIEW", 0),
            "BLOCK": guard_counts.get("BLOCK", 0),
            "block_pct": known_good_block_rate,
            "top_block_reasons": dict(Counter(reason for row in high if row["current_guard_decision"] == "BLOCK" for reason in row["current_guard_reasons"].split("|") if reason).most_common(8)),
        },
        "simulations": simulations,
        "week_based": {
            "verified_52week_years": sum(1 for row in extra_placement if str(row["verified_year_length_days"]) == "364"),
            "verified_53week_years": sum(1 for row in extra_placement if str(row["verified_year_length_days"]) == "371"),
            "observed_13week_quarters": sum(1 for row in week_lengths if row["length_bucket"] == "13 weeks"),
            "observed_14week_quarters": sum(1 for row in week_lengths if row["length_bucket"] == "14 weeks"),
            "extra_week_placement_by_q": dict(Counter(row["extra_week_placement"] for row in extra_placement)),
            "generic_extra_week_rule_safe": False,
        },
        "recommendation": recommendation,
        "rationale": "Known-good recent rows are blocked by the current guard at a material rate, and wider window simulation reduces slot-driven false blocks without changing exact FY-anchor authority." if recommendation == RECOMMEND_CALIBRATE else "The empirical window distribution does not support a simple bounded calibration.",
        "next_action": "DO NOT REPAIR CANONICAL DATA YET; IMPLEMENT AND REHEARSE THE EMPIRICALLY SUPPORTED GUARD CALIBRATION, THEN RE-RUN THE FULL CURRENT/RECENT AUDIT" if recommendation == RECOMMEND_CALIBRATE else "KEEP THE CURRENT GUARD; PROCEED TO CURRENT FISCAL-IDENTITY REPAIR PLANNING" if recommendation == RECOMMEND_CONFIRMED else "DO NOT REPAIR CANONICAL DATA; REWORK ONLY THE FISCAL-SLOT MODEL AND REPEAT THIS CALIBRATION",
        "safety": {"production_writes": 0, "guard_changes": 0, "fingerprints_unchanged": int(before_fp == after_fp), "rawcandle_writes": 0},
    }

    write_csv(paths.artifact_root / "recent_calibration_population.csv", population)
    write_csv(paths.artifact_root / "recent_known_good_high.csv", high)
    write_csv(paths.artifact_root / "recent_known_good_medium.csv", medium)
    write_csv(paths.artifact_root / "recent_excluded_structural_risk.csv", excluded)
    write_json(paths.artifact_root / "calibration_population_summary.json", summary["population"])
    write_csv(paths.artifact_root / "recent_expected_quarter_slots.csv", population)
    write_csv(paths.artifact_root / "period_end_offset_distribution.csv", period_rows(population))
    write_csv(paths.artifact_root / "period_end_window_coverage.csv", window_rows)
    write_csv(paths.artifact_root / "period_end_by_calendar_type.csv", group_distribution(population, "calendar_type"))
    write_csv(paths.artifact_root / "period_end_by_fiscal_quarter.csv", group_distribution(population, "fiscal_quarter"))
    write_csv(paths.artifact_root / "week_based_quarter_length_analysis.csv", week_lengths)
    write_csv(paths.artifact_root / "verified_53week_extra_week_placement.csv", [row for row in extra_placement if row["verified_year_length_days"] == 371])
    write_csv(paths.artifact_root / "publish_lag_distribution.csv", publish_lag_rows(population))
    write_csv(paths.artifact_root / "publish_chronology_analysis.csv", [{"publish_chronology": k, "rows": v} for k, v in Counter(row["publish_chronology"] for row in high).items()])
    write_csv(paths.artifact_root / "publish_chronology_outliers.csv", [row for row in high if row["publish_chronology"] != "PUBLISH_CHRONOLOGY_STRONG_PASS"])
    write_csv(paths.artifact_root / "known_good_current_guard_results.csv", high)
    write_csv(paths.artifact_root / "known_good_guard_block_reasons.csv", [{"reason_code": k, "rows": v} for k, v in summary["current_guard_on_known_good"]["top_block_reasons"].items()])
    write_csv(paths.artifact_root / "simulated_window_models_summary.csv", simulations)
    write_csv(paths.artifact_root / "simulated_known_good_results.csv", [{**row, "simulated_plus_minus_7": simulated_decision(row, 7), "simulated_plus_minus_14": simulated_decision(row, 14)} for row in high])
    write_csv(paths.artifact_root / "simulated_current_era_effect.csv", simulations)
    write_csv(paths.artifact_root / "simulated_known_P1_replay.csv", [{**row, "simulated_plus_minus_7": simulated_decision(row, 7), "simulated_plus_minus_14": simulated_decision(row, 14)} for row in population if row["ticker"] in KNOWN_P1])
    write_json(paths.artifact_root / "phase8d3_summary.json", summary)
    paths.artifact_root.joinpath("calibration_recommendation.md").write_text(summary["recommendation"] + "\n", encoding="utf-8")
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    write_doc(summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Empirically calibrate V3 fiscal quarter slot windows from recent data.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--phase8d1-root", type=Path, default=Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase8d3_quarter_slot_calibration") / utc_stamp()
    summary = run_phase8d3(Phase8D3Paths(artifact_root=artifact_root, phase8d1_root=args.phase8d1_root, v3_db=args.v3_db))
    print(f"classification={summary['classification']}")
    print(f"known_good_high={summary['population']['known_good_high_rows']}")
    print(f"current_guard_block_pct={summary['current_guard_on_known_good']['block_pct']}")
    print(f"recommendation={summary['recommendation']}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
