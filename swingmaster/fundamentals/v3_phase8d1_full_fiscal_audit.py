from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from swingmaster.fundamentals.v3_fiscal_calendar import (
    ANCHOR_TABLE,
    EXPECTED_P1_TICKERS,
    PROFILE_TABLE,
    FiscalCalendarWriteCandidate,
    baseline_summary,
    infer_slot,
    semantic_fingerprints,
    utc_stamp,
    validate_canonical_write_candidate,
)
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro, integrity
from swingmaster.fundamentals.v3_phase8d_prevention_guards import anchor_immutability


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE8D1_FULL_FISCAL_AUDIT_COMPLETE"
RECOMMEND_KEEP = "KEEP_PHASE8D_GUARD_UNCHANGED"
RECOMMEND_CALIBRATE = "CALIBRATE_LONG_DISTANCE_INFERENCE_TO_REVIEW"
RECOMMEND_MORE_MODELING = "GUARD_REQUIRES_MORE_MODELING_BEFORE_USE"


@dataclass(frozen=True)
class Phase8D1Paths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")


def _quarter_ordinal(fiscal_year: int, fiscal_quarter: str) -> int:
    return fiscal_year * 4 + {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}[fiscal_quarter]


def _bucket_distance(distance: int | None, direction: str) -> str:
    if distance is None:
        return "unknown"
    if distance == 0:
        return "0 exact/current anchor interval"
    if direction == "FORWARD":
        return "forward"
    if distance == 1:
        return "1 year"
    if distance == 2:
        return "2 years"
    if distance == 3:
        return "3 years"
    if 4 <= distance <= 5:
        return "4-5 years"
    if 6 <= distance <= 10:
        return "6-10 years"
    return ">10 years"


def _evidence_class(distance: int | None, direction: str, exact_interval: bool, has_adjacent: bool, calendar_type: str, parse_status: str, reasons: str) -> str:
    if parse_status != "PARSED" or distance is None:
        return "INSUFFICIENT_METADATA"
    if "TRANSITION" in reasons or "STUB_PERIOD_REVIEW" in reasons:
        return "TRANSITION_AWARE"
    if exact_interval and has_adjacent:
        return "ADJACENT_EXACT_ANCHORS"
    if exact_interval:
        return "EXACT_ANCHOR_INTERVAL"
    if direction == "FORWARD":
        return "FORWARD_INFERENCE"
    if direction == "BACKWARD":
        if distance == 1:
            return "BACKWARD_INFERENCE_1FY"
        if distance == 2:
            return "BACKWARD_INFERENCE_2FY"
        if distance == 3:
            return "BACKWARD_INFERENCE_3FY"
        if 4 <= distance <= 5:
            return "BACKWARD_INFERENCE_4_5FY"
        if 6 <= distance <= 10:
            return "BACKWARD_INFERENCE_6_10FY"
        return "BACKWARD_INFERENCE_GT10FY"
    if calendar_type in {"CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR"}:
        return "EXACT_ANCHOR_INTERVAL"
    return "INSUFFICIENT_METADATA"


def _confidence(decision: str, evidence_class: str, calendar_type: str, distance: int | None, reasons: str) -> str:
    if decision != "BLOCK":
        return ""
    if evidence_class in {"EXACT_ANCHOR_INTERVAL", "ADJACENT_EXACT_ANCHORS"}:
        return "PROVEN_HIGH"
    if any(code in reasons for code in ("TARGET_IDENTITY_COLLISION", "REVERSE_SEQUENCE")):
        return "PROVEN_HIGH"
    if distance is not None and distance <= 2 and calendar_type in {"CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR", "WEEK_BASED_52_53"}:
        return "STRUCTURAL_HIGH"
    if distance is not None and distance <= 5 and calendar_type in {"CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR"}:
        return "STRUCTURAL_MEDIUM"
    return "INFERENCE_RISK"


def _latest_quarter_ids(all_rows: list[dict[str, Any]], limit: int) -> set[int]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in all_rows:
        grouped[int(row["company_id"])].append(row)
    out: set[int] = set()
    for company_rows in grouped.values():
        ordered = sorted(company_rows, key=lambda r: (r["period_end"] or "", _quarter_ordinal(int(r["fiscal_year"]), str(r["fiscal_quarter"]))), reverse=True)
        out.update(int(row["quarter_id"]) for row in ordered[:limit])
    return out


def _downstream_quarter_sets(conn: sqlite3.Connection) -> dict[str, set[int]]:
    ttm_rows = rows(conn, "SELECT endpoint_quarter_id,q1_quarter_id,q2_quarter_id,q3_quarter_id,q4_quarter_id FROM v3_ttm")
    ttm_inputs = {int(row[col]) for row in ttm_rows for col in ("endpoint_quarter_id", "q1_quarter_id", "q2_quarter_id", "q3_quarter_id", "q4_quarter_id") if row.get(col) is not None}
    return {
        "ttm": ttm_inputs,
        "score": {int(row["as_of_quarter_id"]) for row in rows(conn, "SELECT as_of_quarter_id FROM v3_score")},
        "lifecycle": {int(row["endpoint_quarter_id"]) for row in rows(conn, "SELECT endpoint_quarter_id FROM v3_lifecycle")},
        "valuation": {int(row["endpoint_quarter_id"]) for row in rows(conn, "SELECT endpoint_quarter_id FROM v3_valuation")},
    }


def enumerate_canonical_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date AS period_end,q.publish_date,p.calendar_type,p.profile_parse_status
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_company_fiscal_calendar_profile p ON p.company_id=c.company_id
        WHERE q.period_end_date IS NOT NULL
        ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter
        """,
    )


def classify_row(conn: sqlite3.Connection, row: dict[str, Any], downstream_sets: dict[str, set[int]], latest8: set[int], latest4: set[int]) -> dict[str, Any]:
    anchors = rows(conn, f"SELECT fiscal_year,fiscal_year_start_date FROM {ANCHOR_TABLE} WHERE company_id=? ORDER BY fiscal_year", (row["company_id"],))
    anchor_years = [int(anchor["fiscal_year"]) for anchor in anchors]
    nearest_anchor_year = min(anchor_years, key=lambda y: (abs(y - int(row["fiscal_year"])), -y)) if anchor_years else None
    distance = abs(int(row["fiscal_year"]) - nearest_anchor_year) if nearest_anchor_year is not None else None
    direction = "EXACT" if distance == 0 else "BACKWARD" if nearest_anchor_year and int(row["fiscal_year"]) < nearest_anchor_year else "FORWARD" if nearest_anchor_year else "UNKNOWN"
    calendar = {
        "profile": dict(conn.execute(f"SELECT * FROM {PROFILE_TABLE} WHERE company_id=?", (row["company_id"],)).fetchone() or {}),
        "anchors": anchors,
    } if anchors else None
    slot = infer_slot(calendar, row["period_end"], int(row["fiscal_year"]))
    exact_interval = slot.get("confidence") == "EXACT_ANCHOR"
    has_adjacent = bool(exact_interval and nearest_anchor_year is not None and nearest_anchor_year + 1 in anchor_years)
    candidate = FiscalCalendarWriteCandidate(
        company_id=int(row["company_id"]),
        fiscal_year=int(row["fiscal_year"]),
        fiscal_quarter=str(row["fiscal_quarter"]),
        period_end_date=row["period_end"],
        publish_date=row["publish_date"],
        source_context="PHASE8D1_FULL_CANONICAL_REPLAY",
    )
    decision = validate_canonical_write_candidate(conn, candidate)
    reasons = "|".join(decision.reason_codes)
    evidence = _evidence_class(distance, direction, exact_interval, has_adjacent, row.get("calendar_type") or decision.calendar_type or "UNKNOWN", row.get("profile_parse_status") or "MISSING", reasons)
    block_kind = "EXACT_ANCHOR_PROVEN_CONFLICT" if decision.decision == "BLOCK" and exact_interval else "BACKWARD_INFERENCE_BLOCK" if decision.decision == "BLOCK" and direction == "BACKWARD" else ""
    qid = int(row["quarter_id"])
    return {
        **row,
        "active_state": "active" if int(row["active"]) else "inactive",
        "calendar_type": row.get("calendar_type") or decision.calendar_type or "UNKNOWN",
        "profile_parse_status": row.get("profile_parse_status") or "MISSING",
        "nearest_exact_anchor_start": slot.get("exact_anchor_used") or "",
        "anchor_fiscal_year": nearest_anchor_year or "",
        "fiscal_year_distance_from_verified_anchor": "" if distance is None else distance,
        "inference_direction": direction,
        "inference_distance_bucket": _bucket_distance(distance, direction),
        "inferred_fiscal_year": decision.inferred_fiscal_year or "",
        "inferred_fiscal_quarter": decision.inferred_fiscal_quarter or "",
        "slot_confidence": decision.slot_confidence,
        "structural_evidence_class": evidence,
        "mathematically_direct_profile": int((row.get("calendar_type") or decision.calendar_type) in {"CALENDAR_YEAR", "FIXED_DATE_FISCAL_YEAR"}),
        "transition_state": decision.calendar_regime,
        "guard_decision": decision.decision,
        "reason_codes": reasons,
        "block_kind": block_kind,
        "block_confidence": _confidence(decision.decision, evidence, row.get("calendar_type") or decision.calendar_type or "UNKNOWN", distance, reasons),
        "latest8q": int(qid in latest8),
        "latest4q": int(qid in latest4),
        "ttm_input": int(qid in downstream_sets["ttm"]),
        "score_source": int(qid in downstream_sets["score"]),
        "lifecycle_source": int(qid in downstream_sets["lifecycle"]),
        "valuation_source": int(qid in downstream_sets["valuation"]),
    }


def _breakdown(data: Iterable[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    grouped: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in data:
        grouped[row.get(key, "")].append(row)
    out = []
    for value, group in sorted(grouped.items(), key=lambda item: str(item[0])):
        counts = Counter(row["guard_decision"] for row in group)
        reasons = Counter(reason for row in group for reason in str(row["reason_codes"]).split("|") if reason)
        total = len(group)
        out.append({
            key: value,
            "rows": total,
            "PASS": counts.get("PASS", 0),
            "PASS_WITH_WARNING": counts.get("PASS_WITH_WARNING", 0),
            "REVIEW": counts.get("REVIEW", 0),
            "BLOCK": counts.get("BLOCK", 0),
            "block_pct": round(counts.get("BLOCK", 0) * 100 / total, 4) if total else 0,
            "major_reason_codes": "|".join(code for code, _ in reasons.most_common(5)),
        })
    return out


def _company_breakdown(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in data:
        grouped[int(row["company_id"])].append(row)
    out = []
    for group in grouped.values():
        counts = Counter(row["guard_decision"] for row in group)
        blocked = [row for row in group if row["guard_decision"] == "BLOCK"]
        reasons = Counter(reason for row in blocked for reason in str(row["reason_codes"]).split("|") if reason)
        total = len(group)
        out.append({
            "company_id": group[0]["company_id"],
            "ticker": group[0]["ticker"],
            "active": group[0]["active"],
            "calendar_type": group[0]["calendar_type"],
            "canonical_rows": total,
            "blocked_rows": counts.get("BLOCK", 0),
            "review_rows": counts.get("REVIEW", 0),
            "warning_rows": counts.get("PASS_WITH_WARNING", 0),
            "block_pct": round(counts.get("BLOCK", 0) * 100 / total, 4) if total else 0,
            "oldest_blocked_fy": min((int(row["fiscal_year"]) for row in blocked), default=""),
            "newest_blocked_fy": max((int(row["fiscal_year"]) for row in blocked), default=""),
            "dominant_reason": reasons.most_common(1)[0][0] if reasons else "",
            "exact_anchor_coverage": sum(1 for row in group if row["structural_evidence_class"] in {"EXACT_ANCHOR_INTERVAL", "ADJACENT_EXACT_ANCHORS"}),
        })
    return sorted(out, key=lambda r: (-int(r["blocked_rows"]), -float(r["block_pct"]), str(r["ticker"])))


def _current_analysis(data: list[dict[str, Any]], predicate) -> list[dict[str, Any]]:
    return _breakdown([row for row in data if predicate(row)], "guard_decision")


def _material_block_review(data: list[dict[str, Any]], company_summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    top_count = {row["ticker"] for row in company_summary[:25]}
    top_pct = {row["ticker"] for row in sorted(company_summary, key=lambda r: (-float(r["block_pct"]), -int(r["blocked_rows"]), str(r["ticker"])))[:25]}
    seen: set[int] = set()
    review = []
    representative_bucket_seen: set[str] = set()
    for row in data:
        cohorts = []
        if row["block_kind"] == "EXACT_ANCHOR_PROVEN_CONFLICT":
            cohorts.append("exact_anchor_proven")
        if int(row["fiscal_year"]) >= 2024:
            cohorts.append("fy_2024_plus")
        if row["latest8q"]:
            cohorts.append("latest8q")
        if row["ttm_input"]:
            cohorts.append("ttm_input")
        if row["score_source"] or row["lifecycle_source"] or row["valuation_source"]:
            cohorts.append("current_downstream")
        if row["ticker"] in EXPECTED_P1_TICKERS:
            cohorts.append("known_p1")
        if row["ticker"] in top_count:
            cohorts.append("top25_block_count")
        if row["ticker"] in top_pct:
            cohorts.append("top25_block_pct")
        if row["inference_distance_bucket"] not in representative_bucket_seen:
            representative_bucket_seen.add(row["inference_distance_bucket"])
            cohorts.append("representative_distance_bucket")
        if cohorts and int(row["quarter_id"]) not in seen:
            review.append({**row, "review_cohorts": "|".join(cohorts)})
            seen.add(int(row["quarter_id"]))
    return review


def recommend(summary: dict[str, Any]) -> str:
    exact_blocks = summary["block_kinds"].get("EXACT_ANCHOR_PROVEN_CONFLICT", 0)
    current_blocks = summary["current_2024plus"].get("BLOCK", 0) + summary["latest8q"].get("BLOCK", 0) + summary["ttm_input"].get("BLOCK", 0)
    inference_risk_blocks = summary["block_confidence"].get("INFERENCE_RISK", 0)
    if current_blocks == 0 and inference_risk_blocks > exact_blocks:
        return RECOMMEND_CALIBRATE
    if exact_blocks == 0 and inference_risk_blocks > 1000:
        return RECOMMEND_MORE_MODELING
    return RECOMMEND_KEEP


def run_phase8d1(paths: Phase8D1Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    before_counts = baseline_summary(paths.v3_db)
    before_fp = semantic_fingerprints(paths.v3_db)
    before_anchor = anchor_immutability(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        all_rows = enumerate_canonical_rows(conn)
        latest8 = _latest_quarter_ids(all_rows, 8)
        latest4 = _latest_quarter_ids(all_rows, 4)
        downstream_sets = _downstream_quarter_sets(conn)
        audit = [classify_row(conn, row, downstream_sets, latest8, latest4) for row in all_rows]
        phase_integrity = integrity(conn)
    after_counts = baseline_summary(paths.v3_db)
    after_fp = semantic_fingerprints(paths.v3_db)
    after_anchor = anchor_immutability(paths.v3_db)

    decisions = Counter(row["guard_decision"] for row in audit)
    block_kinds = Counter(row["block_kind"] for row in audit if row["block_kind"])
    block_confidence = Counter(row["block_confidence"] for row in audit if row["block_confidence"])
    reason_counts = Counter(reason for row in audit for reason in str(row["reason_codes"]).split("|") if reason)
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "artifact_root": str(paths.artifact_root),
        "baseline_commit": "ec482c5",
        "phase8c_commit": "5a76a4c",
        "phase8d_commit": "5144bf4",
        "baseline_before": before_counts,
        "baseline_after": after_counts,
        "rows_audited": len(audit),
        "decision_counts": dict(decisions),
        "block_kinds": dict(block_kinds),
        "block_confidence": dict(block_confidence),
        "reason_counts": dict(reason_counts),
        "current_2024plus": dict(Counter(row["guard_decision"] for row in audit if int(row["fiscal_year"]) >= 2024)),
        "latest8q": dict(Counter(row["guard_decision"] for row in audit if row["latest8q"])),
        "latest4q": dict(Counter(row["guard_decision"] for row in audit if row["latest4q"])),
        "ttm_input": dict(Counter(row["guard_decision"] for row in audit if row["ttm_input"])),
        "downstream_source": dict(Counter(row["guard_decision"] for row in audit if row["score_source"] or row["lifecycle_source"] or row["valuation_source"])),
        "known_p1": dict(Counter(row["guard_decision"] for row in audit if row["ticker"] in EXPECTED_P1_TICKERS)),
        "safety": {
            "companies_changed": int(before_counts["companies"] != after_counts["companies"]),
            "canonical_changed": int(before_fp["canonical"] != after_fp["canonical"]),
            "fundamentals_changed": int(before_counts["fundamentals_rows"] != after_counts["fundamentals_rows"]),
            "lineage_changed": int(before_counts["migration_audit_rows"] != after_counts["migration_audit_rows"]),
            "ttm_changed": int(before_fp["ttm"] != after_fp["ttm"]),
            "score_changed": int(before_fp["score"] != after_fp["score"]),
            "lifecycle_changed": int(before_fp["lifecycle"] != after_fp["lifecycle"]),
            "valuation_changed": int(before_fp["valuation"] != after_fp["valuation"]),
            "fiscal_anchors_changed": int(before_anchor != after_anchor),
            "rawcandle_writes": 0,
        },
        "fingerprints_identical": {key: before_fp[key] == after_fp[key] for key in before_fp} | {"fiscal_anchors": before_anchor == after_anchor},
        "integrity": {"quick_check": phase_integrity["quick_check"], "fk_rows": phase_integrity["foreign_key_check"], "duplicate_canonical_fy_fq": phase_integrity["duplicate_fy_fq"], "orphans": phase_integrity["orphans"]},
        "phase8_status": "IN PROGRESS",
    }
    summary["recommendation"] = recommend(summary)
    summary["calibration_applied"] = False

    company_summary = _company_breakdown(audit)
    blocked = [row for row in audit if row["guard_decision"] == "BLOCK"]
    write_json(paths.artifact_root / "full_audit_baseline.json", before_counts)
    write_json(paths.artifact_root / "pre_audit_semantic_fingerprints.json", {"fingerprints": before_fp, "anchors": before_anchor})
    write_csv(paths.artifact_root / "full_canonical_fiscal_guard_audit.csv", audit)
    write_json(paths.artifact_root / "full_canonical_fiscal_guard_summary.json", summary)
    write_csv(paths.artifact_root / "guard_evidence_strength_distribution.csv", _breakdown(audit, "structural_evidence_class"))
    write_csv(paths.artifact_root / "guard_by_inference_distance.csv", _breakdown(audit, "inference_distance_bucket"))
    write_csv(paths.artifact_root / "guard_by_calendar_type.csv", _breakdown(audit, "calendar_type"))
    write_csv(paths.artifact_root / "guard_by_fiscal_year.csv", _breakdown(audit, "fiscal_year"))
    write_csv(paths.artifact_root / "guard_by_company.csv", company_summary)
    write_csv(paths.artifact_root / "all_blocked_rows.csv", blocked)
    write_csv(paths.artifact_root / "exact_anchor_proven_blocks.csv", [row for row in blocked if row["block_kind"] == "EXACT_ANCHOR_PROVEN_CONFLICT"])
    write_csv(paths.artifact_root / "backward_inference_blocks.csv", [row for row in blocked if row["block_kind"] == "BACKWARD_INFERENCE_BLOCK"])
    write_csv(paths.artifact_root / "block_confidence_distribution.csv", [{"block_confidence": k, "rows": v} for k, v in block_confidence.items()])
    write_csv(paths.artifact_root / "block_reason_distribution.csv", [{"reason_code": k, "rows": v} for k, v in reason_counts.items()])
    write_csv(paths.artifact_root / "current_2024plus_guard_analysis.csv", _breakdown([row for row in audit if int(row["fiscal_year"]) >= 2024], "guard_decision"))
    write_csv(paths.artifact_root / "latest8q_guard_analysis.csv", _breakdown([row for row in audit if row["latest8q"]], "guard_decision"))
    write_csv(paths.artifact_root / "latest4q_guard_analysis.csv", _breakdown([row for row in audit if row["latest4q"]], "guard_decision"))
    write_csv(paths.artifact_root / "current_ttm_input_guard_analysis.csv", _breakdown([row for row in audit if row["ttm_input"]], "guard_decision"))
    write_csv(paths.artifact_root / "current_downstream_guard_risk.csv", _breakdown([row for row in audit if row["score_source"] or row["lifecycle_source"] or row["valuation_source"]], "guard_decision"))
    write_csv(paths.artifact_root / "material_block_review.csv", _material_block_review(blocked, company_summary))
    write_csv(paths.artifact_root / "top_block_companies.csv", company_summary[:25] + sorted(company_summary, key=lambda r: (-float(r["block_pct"]), -int(r["blocked_rows"]), str(r["ticker"])))[:25])
    write_csv(paths.artifact_root / "possible_transition_cases.csv", [row for row in audit if row["structural_evidence_class"] == "TRANSITION_AWARE" or row["block_confidence"] == "INFERENCE_RISK"])
    write_csv(paths.artifact_root / "known_P1_guard_replay.csv", [row for row in audit if row["ticker"] in EXPECTED_P1_TICKERS])
    write_json(paths.artifact_root / "post_audit_semantic_fingerprints.json", {"fingerprints": after_fp, "anchors": after_anchor})
    write_json(paths.artifact_root / "phase8d1_integrity.json", phase_integrity)
    write_json(paths.artifact_root / "phase8d1_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(summary["recommendation"] + "\n", encoding="utf-8")
    if any(summary["safety"].values()):
        raise RuntimeError("PHASE8D1_READ_ONLY_SAFETY_VIOLATION")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 8D-1 full V3 fiscal-calendar guard audit.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase8d1_full_fiscal_audit") / utc_stamp()
    summary = run_phase8d1(Phase8D1Paths(artifact_root=artifact_root, v3_db=args.v3_db))
    print(f"classification={summary['classification']}")
    print(f"rows_audited={summary['rows_audited']}")
    print(f"decision_counts={json.dumps(summary['decision_counts'], sort_keys=True)}")
    print(f"block_kinds={json.dumps(summary['block_kinds'], sort_keys=True)}")
    print(f"recommendation={summary['recommendation']}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
