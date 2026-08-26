from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv, sha_file


CLASSIFICATION_PLAN_READY = "FUNDAMENTALS_V3_PHASE8A10A_SEQUENCE_COLLISIONS_RESOLVED_REPAIR_PLAN_READY"
CLASSIFICATION_EVIDENCE_REQUIRED = "FUNDAMENTALS_V3_PHASE8A10A_SEQUENCE_COLLISIONS_EXTERNAL_EVIDENCE_REQUIRED"

CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
VALUE_FIELDS = (
    "revenue",
    "operating_income",
    "ebit",
    "ebitda",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)
FLOW_FIELDS = ("revenue", "operating_income", "ebit", "ebitda", "free_cashflow")
INSTANT_FIELDS = ("cash", "total_debt", "shares_outstanding")


@dataclass(frozen=True)
class Phase8A10APaths:
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    a9_root: Path = Path("temp/fundamentals_v3_phase8a9_period_end_apply/20260826T052001Z")
    verified_csv: Path = Path("temp/phase8_period_end_R1_verified.csv")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def file_snapshot(v3_db: Path, rawcandle_db: Path) -> dict[str, Any]:
    def one(path: Path) -> dict[str, Any]:
        return {
            "exists": path.exists(),
            "size": path.stat().st_size if path.exists() else None,
            "mtime_ns": path.stat().st_mtime_ns if path.exists() else None,
            "sha256": sha_file(path) if path.exists() else None,
        }

    return {"v3_db": one(v3_db), "rawcandle_db": one(rawcandle_db)}


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    tables = ["v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation"]
    return {table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}")) for table in tables}


def q_number(fiscal_quarter: str) -> int:
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(str(fiscal_quarter), 0)


def fiscal_ordinal(fiscal_year: int, fiscal_quarter: str) -> int:
    return fiscal_year * 4 + q_number(fiscal_quarter)


def expected_next_identity(fiscal_year: int, fiscal_quarter: str) -> tuple[int, str]:
    q = q_number(fiscal_quarter)
    if q == 4:
        return fiscal_year + 1, "Q1"
    return fiscal_year, f"Q{q + 1}"


def transition_class(prev: dict[str, Any], current: dict[str, Any]) -> str:
    expected_fy, expected_fq = expected_next_identity(int(prev["fiscal_year"]), str(prev["fiscal_quarter"]))
    if int(current["fiscal_year"]) == expected_fy and current["fiscal_quarter"] == expected_fq:
        return "VALID"
    if fiscal_ordinal(int(current["fiscal_year"]), current["fiscal_quarter"]) <= fiscal_ordinal(int(prev["fiscal_year"]), prev["fiscal_quarter"]):
        return "REVERSE_OR_DUPLICATE_LABEL"
    return "MISSING_OR_SKIPPED_QUARTER"


def period_gap_class(days: int | None) -> str:
    if days is None:
        return "FIRST"
    if days <= 0:
        return "NEGATIVE_OR_ZERO"
    if days < 50:
        return "VERY_SHORT"
    if 75 <= days <= 105:
        return "NORMAL_13_14_WEEK"
    if 50 <= days < 75 or 106 <= days <= 130:
        return "REVIEW"
    return "SEVERE_LONG_GAP"


def reporting_lag_class(days: int | None) -> str:
    if days is None:
        return "UNKNOWN"
    if days < 0:
        return "NEGATIVE"
    if days < 7:
        return "VERY_SHORT"
    if days <= 120:
        return "NORMAL"
    if days <= 240:
        return "LONG"
    return "EXTREME"


def days_between(start: str | None, end: str | None) -> int | None:
    if not start or not end:
        return None
    return (date.fromisoformat(end) - date.fromisoformat(start)).days


def is_52_53_week_case(ticker: str, verified_period_end: str, method: str | None = None) -> bool:
    return method == "52_53_WEEK_CALENDAR" or (ticker in {"CRUS", "MNRO", "RBC", "SKY"} and not verified_period_end.endswith(("03-31", "06-30", "09-30", "12-31")))


def value_comparison(left: dict[str, Any], right: dict[str, Any]) -> tuple[str, str]:
    compared = 0
    same = 0
    different: list[str] = []
    for field in VALUE_FIELDS:
        a = left.get(field)
        b = right.get(field)
        if a is None or b is None:
            continue
        compared += 1
        if abs(float(a) - float(b)) <= max(1.0, abs(float(a)), abs(float(b))) * 0.01:
            same += 1
        else:
            different.append(field)
    if compared == 0:
        return "NO_COMPARABLE_NON_NULL_FIELDS", ""
    if same == compared:
        return "IDENTICAL_OR_NEAR_IDENTICAL", ""
    if same >= max(1, compared - 2):
        return "NEAR_IDENTICAL_WITH_LIMITED_DIFFERENCES", ",".join(different)
    return "DIFFERENT", ",".join(different)


def freeze_cases(a9_root: Path) -> list[dict[str, Any]]:
    case_rows = read_csv(a9_root / "retained_r1_reaudit.csv")
    if len(case_rows) != 15:
        raise RuntimeError(f"expected 15 retained R1 rows, found {len(case_rows)}")
    out = []
    for idx, row in enumerate(case_rows, 1):
        out.append(
            {
                "issue_id": f"P8A10A-R1-{idx:03d}",
                "company_id": row["company_id"],
                "ticker": row["ticker"],
                "company_name": "",
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "current_period_end": row["current_period_end"],
                "externally_verified_period_end": row["new_period_end"],
                "guard_result": row["sequence_guard"],
                "collision_sequence_type": row["sequence_guard"],
                "latest_state_impact": "",
                "downstream_impact": "TTM window/value risk until structural repair is approved",
                "quarter_id": row["quarter_id"],
                "request_id": row["request_id"],
                "duplicate_period_end_rows": row.get("duplicate_period_end_rows", ""),
            }
        )
    return out


def complete_timelines(conn: sqlite3.Connection, cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    company_ids = sorted({int(row["company_id"]) for row in cases})
    placeholders = ",".join("?" for _ in company_ids)
    timeline = rows(
        conn,
        f"""
        SELECT c.company_id,c.ticker,c.company_name,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date AS period_end,q.publish_date,
               f.revenue,f.operating_income,f.ebit,f.ebitda,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.accepted_source_provider,
               CASE WHEN {" AND ".join(f"f.{field} IS NOT NULL" for field in CORE_FIELDS)} AND f.shares_outstanding > 0 THEN 1 ELSE 0 END AS core_ready,
               CASE WHEN t.ttm_id IS NULL THEN 0 ELSE 1 END AS ttm_endpoint_presence,
               CASE WHEN s.score_id IS NULL THEN 0 ELSE 1 END AS score_presence,
               CASE WHEN l.lifecycle_id IS NULL THEN 0 ELSE 1 END AS lifecycle_presence,
               CASE WHEN v.valuation_id IS NULL THEN 0 ELSE 1 END AS valuation_presence
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        LEFT JOIN v3_ttm t ON t.endpoint_quarter_id=q.quarter_id
        LEFT JOIN v3_score s ON s.as_of_quarter_id=q.quarter_id
        LEFT JOIN v3_lifecycle l ON l.endpoint_quarter_id=q.quarter_id
        LEFT JOIN v3_valuation v ON v.endpoint_quarter_id=q.quarter_id
        WHERE c.company_id IN ({placeholders}) AND q.period_end_date >= '2018-01-01'
        ORDER BY c.ticker,q.period_end_date,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
        """,
        company_ids,
    )
    by_company: dict[int, list[dict[str, Any]]] = {}
    for row in timeline:
        by_company.setdefault(int(row["company_id"]), []).append(row)
    out: list[dict[str, Any]] = []
    for group in by_company.values():
        fiscal_ordered = sorted(group, key=lambda r: fiscal_ordinal(int(r["fiscal_year"]), r["fiscal_quarter"]))
        prev_by_qid: dict[int, dict[str, Any]] = {}
        for idx, row in enumerate(fiscal_ordered):
            if idx:
                prev_by_qid[int(row["quarter_id"])] = fiscal_ordered[idx - 1]
        for row in group:
            prev = prev_by_qid.get(int(row["quarter_id"]))
            gap = days_between(prev.get("period_end") if prev else None, row.get("period_end"))
            lag = days_between(row.get("period_end"), row.get("publish_date"))
            out.append(
                {
                    **row,
                    "fiscal_ordinal": fiscal_ordinal(int(row["fiscal_year"]), row["fiscal_quarter"]),
                    "previous_period_end_by_fyfq": prev.get("period_end") if prev else "",
                    "period_gap_days_by_fyfq": gap if gap is not None else "",
                    "period_gap_class": period_gap_class(gap),
                    "publish_lag_days": lag if lag is not None else "",
                    "reporting_lag_class": reporting_lag_class(lag),
                }
            )
    return out


def timeline_summary(timeline: list[dict[str, Any]], cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    case_company_ids = {int(row["company_id"]) for row in cases}
    out = []
    for company_id in sorted(case_company_ids):
        company_rows = [row for row in timeline if int(row["company_id"]) == company_id]
        transitions = []
        ordered = sorted(company_rows, key=lambda r: fiscal_ordinal(int(r["fiscal_year"]), r["fiscal_quarter"]))
        for prev, current in zip(ordered, ordered[1:]):
            transitions.append(transition_class(prev, current))
        duplicate_periods = sum(
            1
            for count in Counter(row["period_end"] for row in company_rows if row["period_end"]).values()
            if count > 1
        )
        out.append(
            {
                "company_id": company_id,
                "ticker": company_rows[0]["ticker"] if company_rows else "",
                "company_name": company_rows[0]["company_name"] if company_rows else "",
                "quarters": len(company_rows),
                "first_period_end": min((row["period_end"] for row in company_rows if row["period_end"]), default=""),
                "latest_period_end": max((row["period_end"] for row in company_rows if row["period_end"]), default=""),
                "missing_or_skipped_transitions": transitions.count("MISSING_OR_SKIPPED_QUARTER"),
                "reverse_or_duplicate_label_transitions": transitions.count("REVERSE_OR_DUPLICATE_LABEL"),
                "duplicate_period_end_groups": duplicate_periods,
                "ttm_endpoints": sum(int(row["ttm_endpoint_presence"]) for row in company_rows),
                "score_rows": sum(int(row["score_presence"]) for row in company_rows),
                "latest_state_affected": int(any(row["period_end"] >= "2025-01-01" for row in company_rows)),
            }
        )
    return out


def lineage_reconstruction(conn: sqlite3.Connection, cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    qids = sorted({int(row["quarter_id"]) for row in cases})
    out = []
    for case in cases:
        audits = rows(
            conn,
            """
            SELECT audit_id,source,source_key,audit_type,decision,evidence_json
            FROM v3_migration_audit
            WHERE quarter_id=?
            ORDER BY audit_id
            """,
            (int(case["quarter_id"]),),
        )
        for audit in audits:
            evidence = json.loads(audit["evidence_json"] or "{}")
            value_meta = evidence.get("value_metadata", {})
            out.append(
                {
                    "issue_id": case["issue_id"],
                    "ticker": case["ticker"],
                    "fiscal_year": case["fiscal_year"],
                    "fiscal_quarter": case["fiscal_quarter"],
                    "quarter_id": case["quarter_id"],
                    "audit_id": audit["audit_id"],
                    "source": audit["source"],
                    "source_key": audit["source_key"],
                    "audit_type": audit["audit_type"],
                    "decision": audit["decision"],
                    "work_unit_key": evidence.get("work_unit_key", ""),
                    "metadata_outcomes": ",".join(evidence.get("metadata_outcomes", [])),
                    "row_outcomes": ",".join(evidence.get("row_outcomes", [])),
                    "raw_evidence_ref": evidence.get("raw_evidence_ref", ""),
                    "derivation_method": value_meta.get("derivation_method", ""),
                    "fiscal_identity_source": value_meta.get("fiscal_identity_source", ""),
                    "phase_q_type": value_meta.get("phase3c2_q_type", ""),
                    "identity_classification": value_meta.get("identity_classification", ""),
                    "economic_period_conclusion": "local source period conflicts with current canonical period_end"
                    if "PERIOD_DATE_CONFLICT" in evidence.get("metadata_outcomes", [])
                    else "local source accepted under current canonical identity",
                }
            )
    missing = sorted(set(qids) - {int(row["quarter_id"]) for row in out})
    for qid in missing:
        case = next(row for row in cases if int(row["quarter_id"]) == qid)
        out.append({**case, "audit_id": "", "source": "NONE", "economic_period_conclusion": "no local migration audit rows found"})
    return out


def row_by_qid(timeline: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    return {int(row["quarter_id"]): row for row in timeline}


def colliding_row(case: dict[str, Any], timeline: list[dict[str, Any]]) -> dict[str, Any] | None:
    company_rows = [row for row in timeline if int(row["company_id"]) == int(case["company_id"])]
    for row in company_rows:
        if int(row["quarter_id"]) != int(case["quarter_id"]) and row["period_end"] == case["externally_verified_period_end"]:
            return row
    return None


def classify_case(case: dict[str, Any], target: dict[str, Any], collision: dict[str, Any] | None, verified: dict[str, str]) -> dict[str, Any]:
    ticker = case["ticker"]
    method = verified.get("Verification Method", "")
    is_5352 = is_52_53_week_case(ticker, case["externally_verified_period_end"], method)
    if case["guard_result"] == "COLLISION":
        if collision:
            comparison, conflicts = value_comparison(target, collision)
            existing_identity = f"FY{collision['fiscal_year']} {collision['fiscal_quarter']}"
        else:
            comparison, conflicts, existing_identity = "NO_COLLISION_ROW_FOUND", "", ""
        if comparison == "IDENTICAL_OR_NEAR_IDENTICAL":
            primary = "DUPLICATE_CANONICAL_QUARTER"
            disposition = "MERGE_INTO_EXISTING_CANONICAL_QUARTER"
            collision_type = "DUPLICATE_SAME_ECONOMIC_QUARTER"
        elif is_5352:
            primary = "52_53_WEEK_CALENDAR_HANDLING"
            disposition = "SHIFT_MULTI_QUARTER_SEGMENT"
            collision_type = "MULTI_ROW_SHIFT"
        else:
            primary = "SHIFTED_MULTI_QUARTER_SEQUENCE"
            disposition = "SHIFT_MULTI_QUARTER_SEGMENT"
            collision_type = "MULTI_ROW_SHIFT"
        confidence = "MEDIUM"
        source_conclusion = "colliding row and target need multi-row relabel/merge review before any write"
    else:
        comparison, conflicts, existing_identity = "NO_PERIOD_END_COLLISION", "", ""
        primary = "SHIFTED_MULTI_QUARTER_SEQUENCE"
        disposition = "SHIFT_MULTI_QUARTER_SEGMENT"
        collision_type = "SEQUENCE_CONFLICT"
        confidence = "MEDIUM"
        source_conclusion = "verified period_end conflicts with adjacent FY/FQ chronology"
        if is_5352:
            primary = "52_53_WEEK_CALENDAR_HANDLING"
    canonical_fy_wrong = int(str(target["period_end"])[:4] != str(case["externally_verified_period_end"])[:4])
    canonical_q_wrong = int(case["guard_result"] == "COLLISION" or case["guard_result"] == "SEQUENCE_CONFLICT")
    neighboring_shift = int(disposition == "SHIFT_MULTI_QUARTER_SEGMENT")
    latest_impact = int(target["period_end"] >= "2025-01-01" or case["externally_verified_period_end"] >= "2025-01-01")
    return {
        "issue_id": case["issue_id"],
        "ticker": ticker,
        "company_id": case["company_id"],
        "quarter_id": case["quarter_id"],
        "fiscal_year": case["fiscal_year"],
        "fiscal_quarter": case["fiscal_quarter"],
        "current_period_end": target["period_end"],
        "verified_period_end": case["externally_verified_period_end"],
        "existing_colliding_or_adjacent_row": existing_identity,
        "collision_transformation_type": collision_type,
        "primary_root_cause": primary,
        "secondary_root_causes": "WRONG_SOURCE_PERIOD_MAPPING,MIGRATION_LINEAGE_ERROR",
        "confidence": confidence,
        "canonical_fy_wrong": canonical_fy_wrong,
        "canonical_q_wrong": canonical_q_wrong,
        "duplicate_economic_quarter_exists": int(case["guard_result"] == "COLLISION"),
        "neighboring_sequence_shifted": neighboring_shift,
        "source_lineage_conclusion": source_conclusion,
        "publish_date_corroboration": "publish_date missing" if not target.get("publish_date") else "publish_date reviewed as chronology evidence",
        "latest_state_impact": latest_impact,
        "value_comparison_to_collision": comparison,
        "conflicting_non_null_fields": conflicts,
        "is_52_53_week_calendar": int(is_5352),
        "proposed_disposition": disposition,
        "production_ready": "NO",
    }


def period_end_collision_analysis(cases: list[dict[str, Any]], timeline: list[dict[str, Any]], verified_by_id: dict[str, str]) -> list[dict[str, Any]]:
    by_qid = row_by_qid(timeline)
    out = []
    for case in cases:
        if case["guard_result"] != "COLLISION":
            continue
        target = by_qid[int(case["quarter_id"])]
        collision = colliding_row(case, timeline)
        comparison, conflicts = value_comparison(target, collision) if collision else ("NO_COLLISION_ROW_FOUND", "")
        out.append(
            {
                "issue_id": case["issue_id"],
                "ticker": case["ticker"],
                "target_quarter_id": case["quarter_id"],
                "target_fy": case["fiscal_year"],
                "target_fq": case["fiscal_quarter"],
                "target_current_period_end": target["period_end"],
                "verified_period_end": case["externally_verified_period_end"],
                "colliding_quarter_id": collision.get("quarter_id", "") if collision else "",
                "colliding_fy": collision.get("fiscal_year", "") if collision else "",
                "colliding_fq": collision.get("fiscal_quarter", "") if collision else "",
                "both_rows_populated": int(bool(collision) and any(target.get(field) is not None for field in VALUE_FIELDS) and any(collision.get(field) is not None for field in VALUE_FIELDS)),
                "value_comparison": comparison,
                "conflicting_non_null_fields": conflicts,
                "same_economic_quarter": "REQUIRES_MERGE_REVIEW" if comparison != "DIFFERENT" else "NO_VALUES_DIFFER",
                "likely_duplicate": int(comparison in {"IDENTICAL_OR_NEAR_IDENTICAL", "NEAR_IDENTICAL_WITH_LIMITED_DIFFERENCES"}),
                "one_row_label_shifted": 1,
                "transformation_type": classify_case(case, target, collision, verified_by_id[case["request_id"]])["collision_transformation_type"],
            }
        )
    return out


def sequence_conflict_analysis(cases: list[dict[str, Any]], timeline: list[dict[str, Any]], verified_by_id: dict[str, str]) -> list[dict[str, Any]]:
    by_qid = row_by_qid(timeline)
    out = []
    for case in cases:
        if case["guard_result"] != "SEQUENCE_CONFLICT":
            continue
        company_rows = sorted(
            [row for row in timeline if int(row["company_id"]) == int(case["company_id"])],
            key=lambda r: fiscal_ordinal(int(r["fiscal_year"]), r["fiscal_quarter"]),
        )
        pos = next(i for i, row in enumerate(company_rows) if int(row["quarter_id"]) == int(case["quarter_id"]))
        prev = company_rows[pos - 1] if pos else None
        nxt = company_rows[pos + 1] if pos + 1 < len(company_rows) else None
        verified = case["externally_verified_period_end"]
        cause = "period_end_before_prior_quarter" if prev and prev["period_end"] >= verified else "period_end_after_next_quarter" if nxt and verified >= nxt["period_end"] else "nonlocal_sequence_conflict"
        target = by_qid[int(case["quarter_id"])]
        out.append(
            {
                "issue_id": case["issue_id"],
                "ticker": case["ticker"],
                "quarter_id": case["quarter_id"],
                "fiscal_year": case["fiscal_year"],
                "fiscal_quarter": case["fiscal_quarter"],
                "current_period_end": target["period_end"],
                "verified_period_end": verified,
                "previous_four": json.dumps(company_rows[max(0, pos - 4) : pos], sort_keys=True, default=str),
                "next_four": json.dumps(company_rows[pos + 1 : pos + 5], sort_keys=True, default=str),
                "conflict_mode": cause,
                "smallest_safe_structural_correction": "SHIFT_MULTI_QUARTER_SEGMENT",
                "production_ready": "NO",
                "is_52_53_week_calendar": int(is_52_53_week_case(case["ticker"], verified, verified_by_id[case["request_id"]].get("Verification Method", ""))),
            }
        )
    return out


def publish_context(cases: list[dict[str, Any]], timeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    by_company: dict[int, list[dict[str, Any]]] = {}
    for row in timeline:
        by_company.setdefault(int(row["company_id"]), []).append(row)
    case_qids = {int(row["quarter_id"]) for row in cases}
    for group in by_company.values():
        ordered = sorted(group, key=lambda r: fiscal_ordinal(int(r["fiscal_year"]), r["fiscal_quarter"]))
        prev_publish = None
        for row in ordered:
            if int(row["quarter_id"]) not in case_qids:
                prev_publish = row.get("publish_date") or prev_publish
                continue
            lag = days_between(row.get("period_end"), row.get("publish_date"))
            anomaly = []
            if lag is not None and lag < 0:
                anomaly.append("PUBLISH_BEFORE_PERIOD")
            if prev_publish and row.get("publish_date") and row["publish_date"] < prev_publish:
                anomaly.append("PUBLISH_BEFORE_PRIOR_PUBLISH")
            if lag is not None and lag > 240:
                anomaly.append("EXTREME_LAG")
            out.append(
                {
                    "ticker": row["ticker"],
                    "quarter_id": row["quarter_id"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end": row["period_end"],
                    "publish_date": row.get("publish_date") or "",
                    "publish_lag_days": lag if lag is not None else "",
                    "reporting_lag_class": reporting_lag_class(lag),
                    "previous_publish_date": prev_publish or "",
                    "publish_context_anomaly": ",".join(anomaly) if anomaly else "NONE",
                    "publish_date_conclusion": "weak/no corroboration" if not row.get("publish_date") else "supports chronology review but not identity by itself",
                }
            )
            prev_publish = row.get("publish_date") or prev_publish
    return out


def proposed_transformations(root_causes: list[dict[str, Any]], timeline: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_qid = row_by_qid(timeline)
    plans = []
    conflicts = []
    impact = []
    for row in root_causes:
        target = by_qid[int(row["quarter_id"])]
        ttm_count = int(target["ttm_endpoint_presence"])
        score_count = int(target["score_presence"])
        lifecycle_count = int(target["lifecycle_presence"])
        valuation_count = int(target["valuation_presence"])
        plans.append(
            {
                "issue_id": row["issue_id"],
                "ticker": row["ticker"],
                "disposition": row["proposed_disposition"],
                "current_company_id": row["company_id"],
                "current_quarter_id": row["quarter_id"],
                "current_fiscal_year": row["fiscal_year"],
                "current_fiscal_quarter": row["fiscal_quarter"],
                "current_period_end": row["current_period_end"],
                "proposed_fiscal_year": "REQUIRES_SEGMENT_PLAN",
                "proposed_fiscal_quarter": "REQUIRES_SEGMENT_PLAN",
                "proposed_period_end": row["verified_period_end"],
                "merge_target": row["existing_colliding_or_adjacent_row"] if row["duplicate_economic_quarter_exists"] else "",
                "delete_source": "NO_DELETE_IN_A10A",
                "affected_canonical_row_ids": row["quarter_id"],
                "affected_lineage_source_rows": "v3_migration_audit rows for affected quarter and neighbors",
                "fields_that_would_move": ",".join(VALUE_FIELDS),
                "publish_date_treatment": "preserve separately; review during apply planning",
                "downstream_endpoints_affected": ttm_count + score_count + lifecycle_count + valuation_count,
                "production_ready": row["production_ready"],
            }
        )
        if row["conflicting_non_null_fields"]:
            conflicts.append(
                {
                    "issue_id": row["issue_id"],
                    "ticker": row["ticker"],
                    "field_conflicts": row["conflicting_non_null_fields"],
                    "action": "do not merge/delete without field-level preservation plan",
                }
            )
        impact.append(
            {
                "issue_id": row["issue_id"],
                "ticker": row["ticker"],
                "canonical_rows_affected_minimum": 1,
                "ttm_endpoint_rows": ttm_count,
                "score_rows": score_count,
                "lifecycle_rows": lifecycle_count,
                "valuation_rows": valuation_count,
                "latest_state_impact": row["latest_state_impact"],
            }
        )
    return plans, conflicts, impact


def write_rule_docs(root: Path) -> None:
    (root / "global_fiscal_sequence_audit_rules.md").write_text(
        """# Global Fiscal Sequence Audit Rules

P1: duplicate company/FY/FQ, reversed fiscal ordinal, same economic quarter mapped to multiple FY/FQ labels, or a verified row whose correction would collide with another canonical row.

P2: missing expected FY/Q transition, skipped quarter labels, unusual rollover, or a shifted segment suspected from neighboring periods.

P3: old isolated missing quarter with no current TTM/score/lifecycle/valuation endpoint.
""",
        encoding="utf-8",
    )
    (root / "global_period_end_audit_rules.md").write_text(
        """# Global Period-End Audit Rules

Compute period_gap_days by canonical FY/FQ order. Initial bands from the 15-case study: normal 75-105 days, review 50-74 or 106-130 days, severe <=0, <50, >160, and annual-like 300-430 day jumps between adjacent FY/FQ labels.

Allow VALID_52_53_WEEK for Saturday/Sunday fiscal closes and 91/98-day 13/14-week quarters when the issuer uses a 52/53-week calendar.
""",
        encoding="utf-8",
    )
    (root / "global_publish_date_audit_rules.md").write_text(
        """# Global Publish-Date Audit Rules

Audit publish chronology independently from fiscal identity. Flag publish_date before period_end, publish_date before prior FY/FQ publication, duplicate publish dates across apparently different economic quarters, and publication chronology that supports a different fiscal sequence.
""",
        encoding="utf-8",
    )
    (root / "global_reporting_lag_audit_rules.md").write_text(
        """# Global Reporting-Lag Audit Rules

Compute reporting_lag_days = publish_date - period_end. Initial bands for Phase 8A10B calibration: NEGATIVE <0, VERY_SHORT 0-6, NORMAL 7-120, LONG 121-240, EXTREME >240. Phase 8A10B must recalibrate these empirically over all retained V3 rows before production gating.
""",
        encoding="utf-8",
    )


def write_handoff(root: Path, summary: dict[str, Any]) -> None:
    (root / "phase8a10b_full_v3_audit_handoff.md").write_text(
        f"""# Phase 8A10B Full V3 Audit Handoff

Input learning set: 15 retained Phase 8A9 R1 cases.

Root-cause summary: `{json.dumps(summary["root_cause_counts"], sort_keys=True)}`.

Required domains:

- fiscal label continuity
- period_end continuity
- publish_date continuity
- period_end to publish_date lag
- duplicate period_end / duplicate economic quarter detection
- 52/53-week calendar exception handling

Severity model:

- P1 structural blocker: duplicate economic quarter, reversed period sequence, wrong FY/FQ mapping, duplicate period_end ambiguity, or TTM-corrupting sequence error.
- P2 material review: unusual quarter gaps, long reporting lag, missing quarter, suspicious but unproven mapping.
- P3 informational: legitimate 52/53-week variation, small month-end vs actual close difference, old metadata anomaly with no current impact.

Do not rebuild downstream until P1 retained-company structural blockers are repaired or explicitly dispositioned.
""",
        encoding="utf-8",
    )
    (root / "next_action.md").write_text(
        "USER EXTERNAL RESEARCH - ONLY UNRESOLVED STRUCTURAL CASES\n\nThen: PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT\n",
        encoding="utf-8",
    )


def run_phase8a10a(paths: Phase8A10APaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    before_files = file_snapshot(paths.v3_db, paths.rawcandle_db)
    verified_rows = read_csv(paths.verified_csv)
    verified_by_id = {row["Request ID"]: row for row in verified_rows}
    cases = freeze_cases(paths.a9_root)
    with connect_ro(paths.v3_db) as conn:
        before_counts = table_counts(conn)
        case_ids = [int(row["company_id"]) for row in cases]
        names = {
            int(row["company_id"]): row["company_name"]
            for row in rows(conn, f"SELECT company_id,company_name FROM v3_company WHERE company_id IN ({','.join('?' for _ in case_ids)})", case_ids)
        }
        latest = {
            int(row["company_id"]): row["latest_period_end"]
            for row in rows(conn, f"SELECT company_id,MAX(period_end_date) AS latest_period_end FROM v3_quarter WHERE company_id IN ({','.join('?' for _ in case_ids)}) GROUP BY company_id", case_ids)
        }
        for case in cases:
            case["company_name"] = names.get(int(case["company_id"]), "")
            case["latest_state_impact"] = int(case["current_period_end"] == latest.get(int(case["company_id"])) or case["externally_verified_period_end"] >= "2025-01-01")
        timeline = complete_timelines(conn, cases)
        lineage = lineage_reconstruction(conn, cases)
        collision = period_end_collision_analysis(cases, timeline, verified_by_id)
        sequence = sequence_conflict_analysis(cases, timeline, verified_by_id)
        publish = publish_context(cases, timeline)
        by_qid = row_by_qid(timeline)
        root_causes = [
            classify_case(case, by_qid[int(case["quarter_id"])], colliding_row(case, timeline), verified_by_id[case["request_id"]])
            for case in cases
        ]
        plans, conflicts, impact = proposed_transformations(root_causes, timeline)
        after_counts = table_counts(conn)
    after_files = file_snapshot(paths.v3_db, paths.rawcandle_db)

    write_csv(paths.artifact_root / "phase8a10a_frozen_r1_cases.csv", cases)
    write_csv(paths.artifact_root / "affected_company_full_fiscal_timelines.csv", timeline)
    write_csv(paths.artifact_root / "affected_company_timeline_summary.csv", timeline_summary(timeline, cases))
    write_csv(paths.artifact_root / "source_lineage_reconstruction.csv", lineage)
    write_csv(paths.artifact_root / "period_end_collision_analysis.csv", collision)
    write_csv(paths.artifact_root / "sequence_conflict_analysis.csv", sequence)
    write_csv(paths.artifact_root / "publish_date_context_analysis.csv", publish)
    write_csv(paths.artifact_root / "r1_structural_root_causes.csv", root_causes)
    write_csv(paths.artifact_root / "proposed_canonical_transformations.csv", plans)
    write_csv(paths.artifact_root / "proposed_transformation_conflicts.csv", conflicts)
    write_csv(paths.artifact_root / "proposed_downstream_impact.csv", impact)

    root_counts = dict(Counter(row["primary_root_cause"] for row in root_causes))
    threshold = {
        "period_gap_days": {"normal": [75, 105], "review": [[50, 74], [106, 130]], "severe": ["<=0", "<50", ">160", "300-430 adjacent annual-like"]},
        "publish_gap_days": {"normal": [60, 130], "review": [[30, 59], [131, 210]], "severe": ["<0 chronological reversal", ">210"]},
        "reporting_lag_days": {"negative": "<0", "very_short": [0, 6], "normal": [7, 120], "long": [121, 240], "extreme": ">240"},
        "empirical_basis": "15 affected-company fiscal timelines; Phase 8A10B must recalibrate across all retained V3 rows",
    }
    summary = {
        "classification": CLASSIFICATION_PLAN_READY if all(row["production_ready"] == "YES" for row in root_causes) else CLASSIFICATION_EVIDENCE_REQUIRED,
        "frozen_r1": len(cases),
        "unique_tickers": len({row["ticker"] for row in cases}),
        "collision_rows": sum(1 for row in cases if row["guard_result"] == "COLLISION"),
        "sequence_conflict_rows": sum(1 for row in cases if row["guard_result"] == "SEQUENCE_CONFLICT"),
        "root_cause_counts": root_counts,
        "production_ready_rows": sum(1 for row in root_causes if row["production_ready"] == "YES"),
        "production_writes": int(before_files["v3_db"] != after_files["v3_db"]),
        "rawcandle_writes": int(before_files["rawcandle_db"] != after_files["rawcandle_db"]),
        "counts_before": before_counts,
        "counts_after": after_counts,
        "counts_unchanged": before_counts == after_counts,
        "derived_writes": {
            "ttm": int(before_counts["v3_ttm"] != after_counts["v3_ttm"]),
            "score": int(before_counts["v3_score"] != after_counts["v3_score"]),
            "lifecycle": int(before_counts["v3_lifecycle"] != after_counts["v3_lifecycle"]),
            "valuation": int(before_counts["v3_valuation"] != after_counts["v3_valuation"]),
        },
        "threshold_proposal": threshold,
        "artifact_root": str(paths.artifact_root),
        "next_action": "USER EXTERNAL RESEARCH - ONLY UNRESOLVED STRUCTURAL CASES",
    }
    write_json(paths.artifact_root / "r1_root_cause_summary.json", summary)
    write_json(paths.artifact_root / "phase8a10b_threshold_proposal.json", threshold)
    write_rule_docs(paths.artifact_root)
    write_handoff(paths.artifact_root, summary)
    if summary["production_writes"] or summary["rawcandle_writes"] or not summary["counts_unchanged"]:
        raise RuntimeError("read-only safety check failed")
    return summary
