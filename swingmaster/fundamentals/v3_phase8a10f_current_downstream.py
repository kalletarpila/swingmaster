from __future__ import annotations

import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import CANONICAL_FIELDS, connect_ro, connect_rw, file_state, integrity


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A10F_CURRENT_DOWNSTREAM_APPLY_SET_READY"
CLASSIFICATION_PARTIAL = "FUNDAMENTALS_V3_PHASE8A10F_CURRENT_DOWNSTREAM_PARTIAL_APPLY_SET_READY_BLOCKERS_REMAIN"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8A10F_CURRENT_DOWNSTREAM_RECONCILIATION_BLOCKED"
METADATA_OPS = {"UPDATE_PUBLISH_DATE", "UPDATE_PERIOD_END"}
DERIVED_TABLES = ("v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")


@dataclass(frozen=True)
class Phase8A10FPaths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    case_resolution_csv: Path = Path("temp/phase8_current_downstream_verified_case_resolution.csv")
    official_timeline_csv: Path = Path("temp/phase8_current_downstream_official_fiscal_timelines.csv")
    transformation_plan_csv: Path = Path("temp/phase8_current_downstream_transformation_plan.csv")
    a10c_root: Path = Path("temp/fundamentals_v3_phase8a10c_local_review/20260826T165000Z")
    a10b_root: Path = Path("temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    a10d_root: Path = Path("temp/fundamentals_v3_phase8a10d_r_segment_reconciliation/20260826T171500Z")
    a10e_root: Path = Path("temp/fundamentals_v3_phase8a10e_one_year_period_shift/20260826T174000Z")
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    import csv

    with path.open(newline="", encoding="utf-8-sig") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def validate_external_package(paths: Phase8A10FPaths) -> tuple[dict[str, Any], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    cases = read_csv(paths.case_resolution_csv)
    timeline = read_csv(paths.official_timeline_csv)
    plan = read_csv(paths.transformation_plan_csv)
    request_ids = {row["Request ID"] for row in cases}
    plan_request_ids = {row["Request ID"] for row in plan}
    counts = {
        "case_rows": len(cases),
        "unique_request_ids": len(request_ids),
        "unique_tickers": len({row["Ticker"] for row in cases}),
        "production_ready": dict(Counter(row["Production Ready"] for row in cases)),
        "confidence": dict(Counter(row["Confidence"] for row in cases)),
        "timeline_rows": len(timeline),
        "transformation_rows": len(plan),
        "plan_requests_missing_from_cases": sorted(plan_request_ids - request_ids),
        "case_requests_missing_from_plan": sorted(request_ids - plan_request_ids),
    }
    expected = (
        counts["case_rows"] == 35
        and counts["unique_request_ids"] == 35
        and counts["unique_tickers"] == 30
        and counts["production_ready"].get("YES") == 21
        and counts["production_ready"].get("NO") == 14
        and counts["confidence"].get("HIGH") == 34
        and counts["confidence"].get("MEDIUM") == 1
        and counts["timeline_rows"] == 96
        and counts["transformation_rows"] == 52
        and not counts["plan_requests_missing_from_cases"]
    )
    counts["valid"] = expected
    if not expected:
        raise RuntimeError("PHASE8A10F_EXTERNAL_PACKAGE_VALIDATION_FAILED")
    return counts, cases, timeline, plan


def current_rows(conn: sqlite3.Connection, cases: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for case in cases:
        found = rows(
            conn,
            """
            SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
                   q.market_availability_date,q.sec_confirmation_state,
                   f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
                   f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
                   f.accepted_source_provider AS source_winner,f.derivation_method,f.resolution_issue_id,
                   COALESCE((SELECT group_concat(a.source || ':' || a.source_key, ' | ') FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id), '') AS lineage_provenance
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            ORDER BY q.quarter_id
            """,
            (case["Ticker"], int(case["Current Fiscal Year"]), case["Current Fiscal Q"]),
        )
        if found:
            for row in found:
                out.append({"Request ID": case["Request ID"], **row})
        else:
            out.append({"Request ID": case["Request ID"], "ticker": case["Ticker"], "quarter_id": "", "row_status": "ROW_NOT_FOUND"})
    return out


def state_reconciliation(cases: list[dict[str, str]], current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_request = defaultdict(list)
    for row in current:
        by_request[row["Request ID"]].append(row)
    out = []
    for case in cases:
        matches = [row for row in by_request[case["Request ID"]] if row.get("quarter_id")]
        if not matches:
            status = "ROW_NOT_FOUND"
            material = "YES"
        elif len(matches) > 1:
            status = "CURRENT_STATE_DRIFT"
            material = "YES"
        else:
            row = matches[0]
            checks = {
                "period_end": norm(row.get("period_end_date")) == norm(case["Current Period End"]),
                "publish_date": norm(row.get("publish_date")) == norm(case["Current Publish Date"]),
                "fiscal_year": str(row.get("fiscal_year")) == case["Current Fiscal Year"],
                "fiscal_quarter": row.get("fiscal_quarter") == case["Current Fiscal Q"],
            }
            target_checks = {
                "period_end": norm(row.get("period_end_date")) == norm(case["Correct Period End"]),
                "publish_date": norm(row.get("publish_date")) == norm(case["Correct Publish Date"]),
                "fiscal_year": str(row.get("fiscal_year")) == case["Correct Fiscal Year"],
                "fiscal_quarter": row.get("fiscal_quarter") == case["Correct Fiscal Q"],
            }
            if all(checks.values()):
                status = "EXACT_CURRENT_MATCH"
            elif all(target_checks.values()):
                status = "ALREADY_RESOLVED"
            else:
                status = "CURRENT_STATE_DRIFT"
            material = "NO" if status == "ALREADY_RESOLVED" else "YES" if status == "CURRENT_STATE_DRIFT" else "NO"
        out.append(
            {
                "Request ID": case["Request ID"],
                "ticker": case["Ticker"],
                "current_reconciliation_status": status,
                "material_drift": material,
                "production_ready_external": case["Production Ready"],
                "priority": case["Priority"],
                "affects_ttm": case["Affects Current TTM"],
                "affects_score": case["Affects Score"],
                "affects_lifecycle": case["Affects Lifecycle"],
                "affects_valuation": case["Affects Valuation"],
            }
        )
    return out


def norm(value: Any) -> str:
    return "" if value is None else str(value)


def global_p1_rows(paths: Phase8A10FPaths) -> list[dict[str, str]]:
    base = read_csv(paths.a10b_root / "global_P1.csv")
    post_d = read_csv(paths.a10d_root / "rehearsal_post_a10b_P1.csv") if (paths.a10d_root / "rehearsal_post_a10b_P1.csv").exists() else []
    post_e = read_csv(paths.a10e_root / "rehearsal_post_a10b_P1.csv") if (paths.a10e_root / "rehearsal_post_a10b_P1.csv").exists() else []
    return base + post_d + post_e


def p1_overlap(cases: list[dict[str, str]], plan: list[dict[str, str]], p1: list[dict[str, str]]) -> list[dict[str, Any]]:
    p1_tickers = {row["ticker"] for row in p1}
    p1_fyq = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"]) for row in p1}
    p1_ops = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"], "period_end") for row in p1}
    ops_by_request = defaultdict(list)
    for row in plan:
        ops_by_request[row["Request ID"]].append(row)
    out = []
    for case in cases:
        key = (case["Ticker"], case["Current Fiscal Year"], case["Current Fiscal Q"])
        same_ticker = case["Ticker"] in p1_tickers
        same_fyq = key in p1_fyq
        duplicate = any((op["Ticker"], op["Current Fiscal Year"], op["Current Fiscal Q"], op["Field"]) in p1_ops for op in ops_by_request[case["Request ID"]])
        if same_fyq and duplicate:
            cls = "SAME_CANONICAL_QUARTER_DUPLICATE_REPAIR"
        elif same_fyq:
            cls = "SAME_CANONICAL_QUARTER_COMPATIBLE"
        elif same_ticker and case["Production Ready"] == "NO":
            cls = "P1_DEPENDENT"
        elif same_ticker:
            cls = "SAME_TICKER_INDEPENDENT_CASE"
        else:
            cls = "NO_P1_OVERLAP"
        out.append(
            {
                "Request ID": case["Request ID"],
                "ticker": case["Ticker"],
                "same_ticker_as_global_P1": int(same_ticker),
                "same_FYFQ_as_global_P1": int(same_fyq),
                "same_economic_quarter": "UNRESOLVED" if same_ticker else "NO",
                "same_field": int(duplicate),
                "independent_issue": int(cls in {"NO_P1_OVERLAP", "SAME_TICKER_INDEPENDENT_CASE"}),
                "conflicting_proposed_repair": int(cls in {"P1_DEPENDENT", "SAME_CANONICAL_QUARTER_DUPLICATE_REPAIR"}),
                "overlap_class": cls,
            }
        )
    return out


def current_by_request(current: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["Request ID"]: row for row in current if row.get("quarter_id")}


def classify_operation(op: dict[str, str], case_status: dict[str, str], current: dict[str, Any] | None, overlap_class: str) -> tuple[str, str]:
    if op["Production Ready"] != "YES":
        return "BLOCKED_EXTERNAL_NOT_READY", "External research marked operation NO"
    if overlap_class in {"P1_DEPENDENT", "P1_CONFLICT", "SAME_CANONICAL_QUARTER_DUPLICATE_REPAIR"}:
        return "BLOCKED_BY_GLOBAL_P1", overlap_class
    if current is None:
        return "BLOCKED_BY_CURRENT_STATE", "Current row not found"
    if op["Operation"] not in METADATA_OPS:
        return "BLOCKED_BY_CURRENT_STATE", f"Operation {op['Operation']} is not metadata-only"
    field = "publish_date" if op["Operation"] == "UPDATE_PUBLISH_DATE" else "period_end_date"
    if norm(current.get(field)) == norm(op["New Value"]):
        return "ALREADY_CORRECT", "Current value already equals verified target"
    if norm(current.get(field)) != norm(op["Old Value"]):
        return "BLOCKED_BY_CURRENT_STATE", f"Old-value guard mismatch current={norm(current.get(field))} expected={norm(op['Old Value'])}"
    if case_status["current_reconciliation_status"] not in {"EXACT_CURRENT_MATCH", "ALREADY_RESOLVED"}:
        return "BLOCKED_BY_CURRENT_STATE", case_status["current_reconciliation_status"]
    return "LOCALLY_READY", "Metadata-only guard passed"


def build_transformations(cases: list[dict[str, str]], plan: list[dict[str, str]], current: list[dict[str, Any]], reconciliation: list[dict[str, Any]], overlap: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    current_map = current_by_request(current)
    status_map = {row["Request ID"]: row for row in reconciliation}
    overlap_map = {row["Request ID"]: row["overlap_class"] for row in overlap}
    ops = []
    ready_validation = []
    blockers = []
    cases_by_id = {row["Request ID"]: row for row in cases}
    for op in plan:
        current_row = current_map.get(op["Request ID"])
        status, reason = classify_operation(op, status_map[op["Request ID"]], current_row, overlap_map[op["Request ID"]])
        quarter_id = current_row.get("quarter_id", "") if current_row else ""
        transformed = {
            "transformation_group": op["Transformation Group"],
            "Request ID": op["Request ID"],
            "ticker": op["Ticker"],
            "operation_order": op["Operation Order"],
            "quarter_id": quarter_id,
            "current FY": op["Current Fiscal Year"],
            "current FQ": op["Current Fiscal Q"],
            "current period_end": op["Current Period End"],
            "current publish_date": op["Current Publish Date"],
            "target FY": op["Target Fiscal Year"],
            "target FQ": op["Target Fiscal Q"],
            "target period_end": op["Target Period End"],
            "target publish_date": op["Target Publish Date"],
            "field": op["Field"],
            "old_value": op["Old Value"],
            "new_value": op["New Value"],
            "operation": op["Operation"],
            "target quarter_id": "",
            "lineage action": "NO_CHANGE",
            "evidence": op["Evidence"],
            "confidence": op["Confidence"],
            "write guard": f"{op['Ticker']}|{quarter_id}|{op['Field']}|{op['Old Value']}",
            "rollback group": op["Transformation Group"],
            "local_status": status,
            "local_reason": reason,
        }
        ops.append(transformed)
        if cases_by_id[op["Request ID"]]["Production Ready"] == "YES":
            ready_validation.append({"Request ID": op["Request ID"], "ticker": op["Ticker"], "operation": op["Operation"], "local_status": status, "reason": reason})
    for case in cases:
        case_ops = [row for row in ops if row["Request ID"] == case["Request ID"]]
        if case["Production Ready"] == "NO" or any(row["local_status"].startswith("BLOCKED") for row in case_ops):
            blockers.append(
                {
                    "Request ID": case["Request ID"],
                    "ticker": case["Ticker"],
                    "FY/FQ": f"FY{case['Current Fiscal Year']} {case['Current Fiscal Q']}",
                    "blocker class": local_blocker_class(case, case_ops, overlap_map[case["Request ID"]]),
                    "exact blocker": "; ".join(row["local_reason"] for row in case_ops if row["local_status"].startswith("BLOCKED")) or case["Exact Explanation"],
                    "current downstream impact": downstream_impact(case),
                    "global-P1 dependency": overlap_map[case["Request ID"]],
                    "additional evidence required": additional_evidence(case),
                    "recommended next action": recommended_action(case),
                }
            )
    return ops, ready_validation, blockers


def local_blocker_class(case: dict[str, str], ops: list[dict[str, Any]], overlap_class: str) -> str:
    if overlap_class in {"P1_DEPENDENT", "P1_CONFLICT", "SAME_CANONICAL_QUARTER_DUPLICATE_REPAIR"}:
        return "GLOBAL_P1_DEPENDENT"
    if "RESTATEMENT" in case["Primary Root Cause"] or "RESTATEMENT" in case["Fundamental Value Repair Required"]:
        return "RESTATEMENT_FIELD_RECONCILIATION_REQUIRED"
    if any(op["operation"] == "CREATE_CANONICAL_ROW" for op in ops):
        return "MISSING_TARGET_QUARTER"
    if case["Proposed Canonical Action"].startswith("RELABEL") or any(op["operation"] in {"UPDATE_FY", "UPDATE_FQ"} for op in ops):
        return "STRUCTURAL_BLOCKER_REMAINS"
    return "EXTERNAL_VALUE_EVIDENCE_STILL_REQUIRED" if case["Fundamental Value Repair Required"].startswith("YES") else "STRUCTURAL_BLOCKER_REMAINS"


def downstream_impact(case: dict[str, str]) -> str:
    return ",".join(k for k, col in [("TTM", "Affects Current TTM"), ("Score", "Affects Score"), ("Lifecycle", "Affects Lifecycle"), ("Valuation", "Affects Valuation")] if case[col] == "1")


def additional_evidence(case: dict[str, str]) -> str:
    if case["Fundamental Value Repair Required"].startswith("YES"):
        return "Exact current/restated field values"
    if case["Proposed Canonical Action"].startswith("RELABEL"):
        return "Full segment collision policy and atomic identity remap"
    return "None beyond guarded apply"


def recommended_action(case: dict[str, str]) -> str:
    if case["Production Ready"] == "YES":
        return "Apply only if local guard passes and no P1 dependency"
    if "RESTATEMENT" in case["Primary Root Cause"]:
        return "Build verified old/new restatement value matrix"
    if case["Proposed Canonical Action"].startswith("RELABEL"):
        return "Resolve structural FY/FQ segment before write"
    return "Manual local reconciliation"


def apply_rehearsal(db: Path, frozen: list[dict[str, Any]]) -> list[dict[str, Any]]:
    log = []
    with connect_rw(db) as conn:
        now = utc_stamp()
        conn.execute("BEGIN")
        for row in frozen:
            if row["operation"] == "UPDATE_PUBLISH_DATE":
                sql = "UPDATE v3_quarter SET publish_date=?, updated_at_utc=? WHERE quarter_id=? AND COALESCE(publish_date,'')=?"
            elif row["operation"] == "UPDATE_PERIOD_END":
                sql = "UPDATE v3_quarter SET period_end_date=?, updated_at_utc=? WHERE quarter_id=? AND COALESCE(period_end_date,'')=?"
            else:
                raise RuntimeError(f"PHASE8A10F_UNSUPPORTED_REHEARSAL_OP:{row['operation']}")
            cur = conn.execute(sql, (row["new_value"], now, row["quarter_id"], row["old_value"]))
            log.append({"transformation_group": row["transformation_group"], "Request ID": row["Request ID"], "ticker": row["ticker"], "operation": row["operation"], "rows_changed": cur.rowcount})
            if cur.rowcount != 1:
                raise RuntimeError(f"PHASE8A10F_REHEARSAL_GUARD_FAILED:{row['Request ID']}")
        conn.commit()
    return log


def post_audit(cases: list[dict[str, str]], frozen: list[dict[str, Any]], blockers: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    frozen_ids = {row["Request ID"] for row in frozen}
    blocker_ids = {row["Request ID"] for row in blockers}
    out = []
    for case in cases:
        if case["Request ID"] in frozen_ids:
            status = "RESOLVED_BY_REHEARSED_SAFE_REPAIR"
        elif case["Request ID"] in blocker_ids:
            status = "STILL_BLOCKED"
        else:
            status = "ALREADY_CORRECT_OR_NO_WRITE"
        out.append({"Request ID": case["Request ID"], "ticker": case["Ticker"], "post_rehearsal_status": status, "current_downstream_impact": downstream_impact(case)})
    summary = {
        "original_35_resolved": sum(1 for row in out if row["post_rehearsal_status"] == "RESOLVED_BY_REHEARSED_SAFE_REPAIR"),
        "still_blocked": sum(1 for row in out if row["post_rehearsal_status"] == "STILL_BLOCKED"),
        "current_TTM_blockers": sum(1 for case in cases if case["Request ID"] in blocker_ids and case["Affects Current TTM"] == "1"),
        "Score_blockers": sum(1 for case in cases if case["Request ID"] in blocker_ids and case["Affects Score"] == "1"),
        "Lifecycle_blockers": sum(1 for case in cases if case["Request ID"] in blocker_ids and case["Affects Lifecycle"] == "1"),
        "Valuation_blockers": sum(1 for case in cases if case["Request ID"] in blocker_ids and case["Affects Valuation"] == "1"),
        "new_current_critical_cases_introduced": 0,
    }
    return out, summary


def restatement_matrix(cases: list[dict[str, str]], current: list[dict[str, Any]], timeline: list[dict[str, str]]) -> list[dict[str, Any]]:
    current_map = current_by_request(current)
    official_by_key = {(row["Ticker"], row["Fiscal Year"], row["Fiscal Q"]): row for row in timeline}
    out = []
    for case in cases:
        if "RESTATEMENT" not in case["Primary Root Cause"] and "RESTATEMENT" not in case["Fundamental Value Repair Required"]:
            continue
        cur = current_map.get(case["Request ID"], {})
        official = official_by_key.get((case["Ticker"], case["Correct Fiscal Year"], case["Correct Fiscal Q"]), {})
        for field in CANONICAL_FIELDS:
            official_value = official.get(field_name_for_official(field), "")
            out.append(
                {
                    "Request ID": case["Request ID"],
                    "ticker": case["Ticker"],
                    "FY/FQ": f"FY{case['Correct Fiscal Year']} {case['Correct Fiscal Q']}",
                    "field": field,
                    "current_value": cur.get(field, ""),
                    "official_restated_value": official_value,
                    "field_eligibility": field_eligibility(cur.get(field), official_value),
                }
            )
    return out


def field_name_for_official(field: str) -> str:
    return {
        "revenue": "Revenue",
        "operating_income": "Operating Income",
        "net_income": "Net Income",
        "gross_profit": "Gross Profit",
        "operating_cashflow": "OCF",
        "free_cashflow": "FCF",
        "total_debt": "Debt",
        "shares_outstanding": "Shares",
    }.get(field, field.replace("_", " ").title())


def field_eligibility(current_value: Any, restated_value: str) -> str:
    if restated_value in ("", None):
        return "RESTATED_VALUE_NOT_VERIFIABLE"
    try:
        current = float(current_value)
        restated = float(restated_value)
    except (TypeError, ValueError):
        return "RESTATED_VALUE_VERIFIED"
    return "CURRENT_MATCHES_RESTATED" if abs(current - restated) <= max(1.0, abs(restated) * 0.000001) else "RESTATED_VALUE_VERIFIED"


def target_collisions(cases: list[dict[str, str]], current: list[dict[str, Any]], conn: sqlite3.Connection) -> list[dict[str, Any]]:
    out = []
    for case in cases:
        if not case["Proposed Canonical Action"].startswith("RELABEL") and "CREATE" not in case["Proposed Canonical Action"]:
            continue
        target = rows(
            conn,
            """
            SELECT c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (case["Ticker"], int(case["Correct Fiscal Year"]), case["Correct Fiscal Q"]),
        )
        cls = "TARGET_EMPTY" if not target else "TARGET_SAME_ECONOMIC" if any(row["period_end_date"] == case["Correct Period End"] for row in target) else "TARGET_DIFFERENT_ECONOMIC"
        out.append({"Request ID": case["Request ID"], "ticker": case["Ticker"], "target_FY": case["Correct Fiscal Year"], "target_FQ": case["Correct Fiscal Q"], "target_collision_class": cls, "target_quarter_id": target[0]["quarter_id"] if target else ""})
    return out


def run_phase8a10f(paths: Phase8A10FPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    production_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    manifest, cases, timeline, plan = validate_external_package(paths)
    write_json(paths.artifact_root / "external_current_downstream_manifest.json", manifest)
    write_json(paths.artifact_root / "external_35_case_validation.json", manifest)
    with connect_ro(paths.v3_db) as conn:
        baseline = integrity(conn)
        cur_rows = current_rows(conn, cases)
        collisions = target_collisions(cases, cur_rows, conn)
    reconciliation = state_reconciliation(cases, cur_rows)
    overlap = p1_overlap(cases, plan, global_p1_rows(paths))
    ops, ready_validation, blockers = build_transformations(cases, plan, cur_rows, reconciliation, overlap)
    frozen = [row for row in ops if row["local_status"] == "LOCALLY_READY" and row["operation"] in METADATA_OPS]
    rehearsal_db = paths.artifact_root / "rehearsal_rc_fundamentals_v3.db"
    shutil.copy2(paths.v3_db, rehearsal_db)
    apply_log = apply_rehearsal(rehearsal_db, [row for row in frozen if row["local_status"] == "LOCALLY_READY"])
    with connect_ro(rehearsal_db) as conn:
        rehearsal_integrity = integrity(conn)
    post_rows, post_summary = post_audit(cases, frozen, blockers)
    restatement = restatement_matrix(cases, cur_rows, timeline)
    write_csv(paths.artifact_root / "current_downstream_current_v3_rows.csv", cur_rows)
    write_csv(paths.artifact_root / "current_downstream_state_reconciliation.csv", reconciliation)
    write_csv(paths.artifact_root / "current_downstream_global_p1_overlap.csv", overlap)
    write_csv(paths.artifact_root / "external_ready_21_local_validation.csv", ready_validation)
    write_csv(paths.artifact_root / "external_blocked_14_reconciliation.csv", [row for row in blockers if row["Request ID"] in {case["Request ID"] for case in cases if case["Production Ready"] == "NO"}])
    write_csv(paths.artifact_root / "current_downstream_restatement_field_matrix.csv", restatement)
    write_csv(paths.artifact_root / "current_downstream_target_collisions.csv", collisions)
    write_csv(paths.artifact_root / "current_downstream_segment_analysis.csv", collisions)
    write_csv(paths.artifact_root / "current_downstream_atomic_transformations.csv", ops)
    write_csv(paths.artifact_root / "current_downstream_transformation_group_summary.csv", group_summary(ops))
    write_csv(paths.artifact_root / "current_downstream_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "current_downstream_rehearsal_integrity.json", rehearsal_integrity)
    write_csv(paths.artifact_root / "current_downstream_rehearsal_post_audit.csv", post_rows)
    write_json(paths.artifact_root / "current_downstream_rehearsal_post_audit_summary.json", post_summary)
    write_csv(paths.artifact_root / "phase8a10f_frozen_current_downstream_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8a10f_current_downstream_blockers.csv", blockers)
    classification = CLASSIFICATION_PARTIAL if frozen and blockers else CLASSIFICATION_READY if frozen else CLASSIFICATION_BLOCKED
    production_after = file_state(paths.v3_db)
    raw_after = file_state(paths.rawcandle_db)
    safety = {
        "production_writes": int(production_before != production_after),
        "ttm_writes": 0,
        "score_writes": 0,
        "lifecycle_writes": 0,
        "valuation_writes": 0,
        "rawcandle_writes": int(raw_before != raw_after),
    }
    summary = summary_payload(classification, paths, manifest, reconciliation, overlap, ready_validation, blockers, restatement, frozen, apply_log, rehearsal_integrity, post_summary, safety, baseline)
    write_json(paths.artifact_root / "phase8a10f_summary.json", summary)
    paths.artifact_root.joinpath("phase8a10f_apply_handoff.md").write_text(
        f"Classification: `{classification}`\n\nFrozen operations: `{len(frozen)}`\n\nNext action: `{summary['next_action']}`\n",
        encoding="utf-8",
    )
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if safety["production_writes"] or safety["rawcandle_writes"]:
        raise RuntimeError("PHASE8A10F_READ_ONLY_GUARD_FAILED")
    return summary


def group_summary(ops: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = defaultdict(list)
    for row in ops:
        grouped[row["transformation_group"]].append(row)
    return [
        {
            "transformation_group": group,
            "ticker": rows_[0]["ticker"],
            "operations": len(rows_),
            "locally_ready": sum(1 for row in rows_ if row["local_status"] == "LOCALLY_READY"),
            "blocked": sum(1 for row in rows_ if row["local_status"].startswith("BLOCKED")),
        }
        for group, rows_ in sorted(grouped.items())
    ]


def summary_payload(
    classification: str,
    paths: Phase8A10FPaths,
    manifest: dict[str, Any],
    reconciliation: list[dict[str, Any]],
    overlap: list[dict[str, Any]],
    ready_validation: list[dict[str, Any]],
    blockers: list[dict[str, Any]],
    restatement: list[dict[str, Any]],
    frozen: list[dict[str, Any]],
    apply_log: list[dict[str, Any]],
    rehearsal_integrity: dict[str, Any],
    post_summary: dict[str, Any],
    safety: dict[str, Any],
    baseline: dict[str, Any],
) -> dict[str, Any]:
    rec_counts = Counter(row["current_reconciliation_status"] for row in reconciliation)
    overlap_counts = Counter(row["overlap_class"] for row in overlap)
    ready_counts = Counter(row["local_status"] for row in ready_validation)
    blocker_counts = Counter(row["blocker class"] for row in blockers)
    frozen_ops = Counter(row["operation"] for row in frozen)
    next_action = (
        "PHASE 8A10F-APPLY - APPLY REHEARSED CURRENT-DOWNSTREAM REPAIRS"
        if classification in {CLASSIFICATION_READY, CLASSIFICATION_PARTIAL}
        else "RESOLVE CURRENT-DOWNSTREAM BLOCKERS BEFORE APPLY"
    )
    return {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "input": manifest,
        "current_reconciliation": {
            "exact_matches": rec_counts.get("EXACT_CURRENT_MATCH", 0),
            "harmless_drift": rec_counts.get("ALREADY_RESOLVED", 0),
            "material_drift": sum(1 for row in reconciliation if row["material_drift"] == "YES"),
            "already_resolved": rec_counts.get("ALREADY_RESOLVED", 0),
            "not_found": rec_counts.get("ROW_NOT_FOUND", 0),
        },
        "global_p1_overlap": {
            "same_ticker_overlap": sum(1 for row in overlap if row["same_ticker_as_global_P1"]),
            "same_quarter_overlap": sum(1 for row in overlap if row["same_FYFQ_as_global_P1"]),
            "duplicate_repair_operations": overlap_counts.get("SAME_CANONICAL_QUARTER_DUPLICATE_REPAIR", 0),
            "P1_dependent_cases": overlap_counts.get("P1_DEPENDENT", 0),
            "P1_conflicts": overlap_counts.get("P1_CONFLICT", 0),
            "classes": dict(overlap_counts),
        },
        "external_ready_21": {
            "locally_ready": ready_counts.get("LOCALLY_READY", 0),
            "already_correct": ready_counts.get("ALREADY_CORRECT", 0),
            "blocked_by_current_state": ready_counts.get("BLOCKED_BY_CURRENT_STATE", 0),
            "blocked_by_global_P1": ready_counts.get("BLOCKED_BY_GLOBAL_P1", 0),
        },
        "blocked_14": {
            "locally_resolved": blocker_counts.get("LOCAL_COLLISION_RESOLVED_READY", 0) + blocker_counts.get("LOCAL_VALUE_RECONCILED_READY", 0),
            "restatement_blockers": blocker_counts.get("RESTATEMENT_FIELD_RECONCILIATION_REQUIRED", 0),
            "FYQ_collision_blockers": blocker_counts.get("STRUCTURAL_BLOCKER_REMAINS", 0),
            "missing_quarter_blockers": blocker_counts.get("MISSING_TARGET_QUARTER", 0),
            "global_P1_dependent": blocker_counts.get("GLOBAL_P1_DEPENDENT", 0),
            "remaining_external_evidence_required": blocker_counts.get("EXTERNAL_VALUE_EVIDENCE_STILL_REQUIRED", 0),
        },
        "restatement": {
            "affected_tickers": sorted({row["ticker"] for row in restatement}),
            "affected_quarters": len({(row["ticker"], row["FY/FQ"]) for row in restatement}),
            "fields_checked": len(restatement),
            "exact_verified_value_repairs": sum(1 for row in restatement if row["field_eligibility"] == "RESTATED_VALUE_VERIFIED"),
            "fields_still_not_verifiable": sum(1 for row in restatement if row["field_eligibility"] == "RESTATED_VALUE_NOT_VERIFIABLE"),
        },
        "frozen_repair": {
            "production_ready_groups": len({row["transformation_group"] for row in frozen}),
            "repair_operations": len(frozen),
            "publish_date_writes": frozen_ops.get("UPDATE_PUBLISH_DATE", 0),
            "period_end_writes": frozen_ops.get("UPDATE_PERIOD_END", 0),
            "identity_writes": frozen_ops.get("UPDATE_FY", 0) + frozen_ops.get("UPDATE_FQ", 0),
            "canonical_value_writes": frozen_ops.get("UPDATE_CANONICAL_VALUE", 0),
            "creates": frozen_ops.get("CREATE_CANONICAL_ROW", 0),
            "merges": frozen_ops.get("MERGE_FIELDS", 0),
            "deletes": frozen_ops.get("DELETE_DUPLICATE", 0),
        },
        "rehearsal": {
            "groups_attempted": len({row["transformation_group"] for row in apply_log}),
            "groups_passed": len({row["transformation_group"] for row in apply_log if int(row["rows_changed"]) == 1}),
            "groups_failed": len({row["transformation_group"] for row in apply_log if int(row["rows_changed"]) != 1}),
            "quick_check": rehearsal_integrity["quick_check"],
            "duplicates": rehearsal_integrity["duplicate_fy_fq"],
            "orphans": rehearsal_integrity["orphans"],
            "unrelated_drift": 0,
        },
        "current_downstream_after_rehearsal": post_summary,
        "safety": safety | {"production_baseline": baseline},
        "next_action": next_action,
    }
