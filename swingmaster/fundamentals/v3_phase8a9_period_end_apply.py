from __future__ import annotations

import csv
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a6_safe_apply import DERIVED_STALE, read_csv, sha_file
from swingmaster.fundamentals.v3_phase8a7_canonical_closure import preflight


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A9_R1_PERIOD_END_CLOSED_READY_FOR_COMBINED_DOWNSTREAM_REBUILD"
CLASSIFICATION_RESIDUAL = "FUNDAMENTALS_V3_PHASE8A9_SEQUENCE_COLLISION_R1_RETAINED"

ORIGINAL_QUEUE_COLUMNS = [
    "Candidate Publish Date",
    "Candidate Value",
    "Current Impact",
    "Current Publish Date",
    "Current Value",
    "Evidence Already Available",
    "Exact Missing Fact",
    "Exact Research Question",
    "Field",
    "Fiscal Q",
    "Fiscal Year",
    "Issue Type",
    "Period End",
    "Preferred Source",
    "Request ID",
    "Ticker",
    "Why R1",
]


@dataclass(frozen=True)
class Phase8A9Paths:
    artifact_root: Path
    v3_db: Path
    verified_csv: Path = Path("temp/phase8_period_end_R1_verified.csv")
    a8_external_queue_csv: Path = Path(
        "temp/fundamentals_v3_phase8a8_r1_resolution/20260826T045013Z/external_research_queue_R1.csv"
    )


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def make_backup(db_path: Path, artifact_root: Path) -> dict[str, Any]:
    backup_dir = artifact_root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{db_path.stem}_phase8a9_backup.db"
    with sqlite3.connect(db_path) as src, sqlite3.connect(backup_path) as dst:
        src.backup(dst)
    return {
        "backup_path": str(backup_path),
        "backup_sha256": sha_file(backup_path),
        "backup_size_bytes": backup_path.stat().st_size,
    }


def validate_verified_input(verified_rows: list[dict[str, str]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    request_ids = [row.get("Request ID", "") for row in verified_rows]
    keys = [(row.get("Ticker", ""), row.get("Fiscal Year", ""), row.get("Fiscal Q", "")) for row in verified_rows]
    source_counts = [int(row.get("Source Count") or 0) for row in verified_rows]
    summary = {
        "rows": len(verified_rows),
        "unique_quarters": len(set(keys)),
        "status_counts": dict(Counter(row.get("Status", "") for row in verified_rows)),
        "confidence_counts": dict(Counter(row.get("Confidence", "") for row in verified_rows)),
        "issue_type_counts": dict(Counter(row.get("Issue Type", "") for row in verified_rows)),
        "source_count_2plus": sum(1 for value in source_counts if value >= 2),
        "source_count_1": sum(1 for value in source_counts if value == 1),
        "candidate_equals_verified": sum(
            1 for row in verified_rows if row.get("Candidate Value") == row.get("Verified Period End")
        ),
        "verified_period_end_complete": sum(1 for row in verified_rows if row.get("Verified Period End")),
        "identity_conflicts": sum(1 for row in verified_rows if row.get("Status") == "IDENTITY_CONFLICT"),
        "duplicate_request_ids": len(request_ids) - len(set(request_ids)),
    }
    expected = {
        "rows": 18,
        "unique_quarters": 18,
        "source_count_2plus": 12,
        "source_count_1": 6,
        "candidate_equals_verified": 18,
        "verified_period_end_complete": 18,
        "identity_conflicts": 0,
        "duplicate_request_ids": 0,
    }
    for key, expected_value in expected.items():
        diagnostics.append(
            {
                "check": key,
                "expected": expected_value,
                "actual": summary[key],
                "status": "PASS" if summary[key] == expected_value else "FAIL",
            }
        )
    counter_expectations = {
        "status_counts": {"DIFFERENT": 18},
        "confidence_counts": {"HIGH": 18},
        "issue_type_counts": {"PERIOD_END": 18},
    }
    for key, expected_value in counter_expectations.items():
        diagnostics.append(
            {
                "check": key,
                "expected": expected_value,
                "actual": summary[key],
                "status": "PASS" if summary[key] == expected_value else "FAIL",
            }
        )
    if any(row["status"] != "PASS" for row in diagnostics):
        raise RuntimeError(f"verified period-end input failed reconciliation: {diagnostics}")
    return summary, diagnostics


def reconcile_original_columns(
    verified_rows: list[dict[str, str]], external_queue_rows: list[dict[str, str]]
) -> list[dict[str, Any]]:
    by_id = {row["Request ID"]: row for row in external_queue_rows}
    out: list[dict[str, Any]] = []
    for row in verified_rows:
        source = by_id.get(row["Request ID"])
        mismatches = []
        missing = source is None
        if source is not None:
            for column in ORIGINAL_QUEUE_COLUMNS:
                if row.get(column, "") != source.get(column, ""):
                    mismatches.append(column)
        out.append(
            {
                "request_id": row["Request ID"],
                "ticker": row["Ticker"],
                "fiscal_year": row["Fiscal Year"],
                "fiscal_quarter": row["Fiscal Q"],
                "source_row_found": int(not missing),
                "original_columns_checked": len(ORIGINAL_QUEUE_COLUMNS),
                "mismatch_count": len(mismatches),
                "mismatched_columns": ",".join(mismatches),
                "status": "PASS" if not missing and not mismatches else "FAIL",
            }
        )
    if any(row["status"] != "PASS" for row in out):
        raise RuntimeError("verified period-end input no longer matches Phase 8A8 external queue identity columns")
    return out


def current_v3_reconciliation(conn: sqlite3.Connection, verified_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in verified_rows:
        found = rows(
            conn,
            """
            SELECT c.company_id,c.ticker,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Q"]),
        )
        current = found[0] if len(found) == 1 else {}
        old_ok = current.get("period_end_date") == row["Period End"]
        out.append(
            {
                "request_id": row["Request ID"],
                "ticker": row["Ticker"],
                "company_id": current.get("company_id", ""),
                "active": current.get("active", ""),
                "quarter_id": current.get("quarter_id", ""),
                "fiscal_year": row["Fiscal Year"],
                "fiscal_quarter": row["Fiscal Q"],
                "old_period_end": row["Period End"],
                "current_period_end": current.get("period_end_date", ""),
                "new_period_end": row["Verified Period End"],
                "target_row_count": len(found),
                "status": "PASS" if len(found) == 1 and old_ok else "STALE_PERIOD_END_WRITE_GUARD_FAILED",
            }
        )
    return out


def _quarter_rank(row: dict[str, Any]) -> tuple[int, int]:
    return (int(row["fiscal_year"]), {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(str(row["fiscal_quarter"]), 9))


def sequence_guards(conn: sqlite3.Connection, reconciled: list[dict[str, Any]], verified_by_id: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in reconciled:
        if row["status"] != "PASS":
            out.append({**row, "sequence_guard": "STALE_PERIOD_END_WRITE_GUARD_FAILED", "sequence_guard_ok": 0})
            continue
        company_id = int(row["company_id"])
        quarter_id = int(row["quarter_id"])
        all_quarters = rows(
            conn,
            """
            SELECT quarter_id,fiscal_year,fiscal_quarter,period_end_date
            FROM v3_quarter
            WHERE company_id=?
            ORDER BY fiscal_year, CASE fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
            """,
            (company_id,),
        )
        updated = [
            {**q, "period_end_date": row["new_period_end"]} if int(q["quarter_id"]) == quarter_id else q
            for q in all_quarters
        ]
        ordered = sorted(updated, key=_quarter_rank)
        position = next(idx for idx, q in enumerate(ordered) if int(q["quarter_id"]) == quarter_id)
        previous_q = ordered[position - 1] if position else None
        next_q = ordered[position + 1] if position + 1 < len(ordered) else None
        duplicate_targets = [q for q in ordered if int(q["quarter_id"]) != quarter_id and q["period_end_date"] == row["new_period_end"]]
        chronological_ok = True
        if previous_q and previous_q["period_end_date"] and previous_q["period_end_date"] >= row["new_period_end"]:
            chronological_ok = False
        if next_q and next_q["period_end_date"] and row["new_period_end"] >= next_q["period_end_date"]:
            chronological_ok = False
        verified = verified_by_id[row["request_id"]]
        if duplicate_targets:
            guard = "COLLISION"
        elif not chronological_ok:
            guard = "SEQUENCE_CONFLICT"
        elif verified.get("Verification Method") == "52_53_WEEK_CALENDAR":
            guard = "VALID_52_53_WEEK"
        else:
            guard = "VALID"
        out.append(
            {
                **row,
                "previous_fy": previous_q.get("fiscal_year", "") if previous_q else "",
                "previous_fq": previous_q.get("fiscal_quarter", "") if previous_q else "",
                "previous_period_end": previous_q.get("period_end_date", "") if previous_q else "",
                "next_fy": next_q.get("fiscal_year", "") if next_q else "",
                "next_fq": next_q.get("fiscal_quarter", "") if next_q else "",
                "next_period_end": next_q.get("period_end_date", "") if next_q else "",
                "duplicate_period_end_count": len(duplicate_targets),
                "duplicate_period_end_rows": ";".join(
                    f"FY{q['fiscal_year']} {q['fiscal_quarter']} {q['period_end_date']}" for q in duplicate_targets
                ),
                "sequence_guard": guard,
                "sequence_guard_ok": int(guard in {"VALID", "VALID_52_53_WEEK"}),
            }
        )
    return out


def freeze_repairs(guarded: list[dict[str, Any]], verified_by_id: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    repairs: list[dict[str, Any]] = []
    for idx, row in enumerate((r for r in guarded if int(r["sequence_guard_ok"]) == 1), 1):
        verified = verified_by_id[row["request_id"]]
        repairs.append(
            {
                "repair_id": f"P8A9-PERIOD-END-{idx:03d}",
                "company_id": row["company_id"],
                "quarter_id": row["quarter_id"],
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "old_period_end": row["old_period_end"],
                "new_period_end": row["new_period_end"],
                "status": verified["Status"],
                "confidence": verified["Confidence"],
                "source_count": verified["Source Count"],
                "source_1": verified.get("Primary Source", ""),
                "source_2": verified.get("Secondary Source", ""),
                "sequence_guard": row["sequence_guard"],
                "old_value_guard_expectation": row["old_period_end"],
            }
        )
    return repairs


def apply_repairs(conn: sqlite3.Connection, repairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    audit_rows: list[dict[str, Any]] = []
    conn.execute("BEGIN")
    try:
        for repair in repairs:
            current = scalar(
                conn,
                "SELECT period_end_date FROM v3_quarter WHERE quarter_id=? AND company_id=? AND fiscal_year=? AND fiscal_quarter=?",
                (
                    repair["quarter_id"],
                    repair["company_id"],
                    int(repair["fiscal_year"]),
                    repair["fiscal_quarter"],
                ),
            )
            if current != repair["old_period_end"]:
                audit_rows.append({**repair, "apply_status": "STALE_PERIOD_END_WRITE_GUARD_FAILED", "rows_changed": 0})
                continue
            cur = conn.execute(
                "UPDATE v3_quarter SET period_end_date=? WHERE quarter_id=? AND period_end_date=?",
                (repair["new_period_end"], repair["quarter_id"], repair["old_period_end"]),
            )
            audit_rows.append({**repair, "apply_status": "APPLIED", "rows_changed": cur.rowcount})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return audit_rows


def same_company_period_end_duplicate_count(conn: sqlite3.Connection) -> int:
    return int(
        scalar(
            conn,
            "SELECT COUNT(*) FROM (SELECT company_id,period_end_date,COUNT(*) c FROM v3_quarter WHERE period_end_date IS NOT NULL GROUP BY company_id,period_end_date HAVING c>1)",
        )
    )


def post_integrity(conn: sqlite3.Connection, before: dict[str, Any], after: dict[str, Any], before_period_duplicates: int) -> dict[str, Any]:
    row_counts = before["row_counts"]
    after_counts = after["row_counts"]
    current_period_duplicates = same_company_period_end_duplicate_count(conn)
    return {
        "quick_check": after["quick_check"],
        "companies_before": row_counts["v3_company"],
        "companies_after": after_counts["v3_company"],
        "canonical_rows_before": row_counts["v3_quarter"],
        "canonical_rows_after": after_counts["v3_quarter"],
        "fundamentals_rows_before": row_counts["v3_quarter_fundamentals"],
        "fundamentals_rows_after": after_counts["v3_quarter_fundamentals"],
        "ttm_rows_before": row_counts["v3_ttm"],
        "ttm_rows_after": after_counts["v3_ttm"],
        "score_rows_before": row_counts["v3_score"],
        "score_rows_after": after_counts["v3_score"],
        "lifecycle_rows_before": row_counts["v3_lifecycle"],
        "lifecycle_rows_after": after_counts["v3_lifecycle"],
        "valuation_rows_before": row_counts["v3_valuation"],
        "valuation_rows_after": after_counts["v3_valuation"],
        "duplicate_canonical_identities": scalar(conn, "SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) c FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING c>1)"),
        "same_company_period_end_duplicates_before": before_period_duplicates,
        "same_company_period_end_duplicates_after": current_period_duplicates,
        "same_company_period_end_duplicate_delta": current_period_duplicates - before_period_duplicates,
        "orphan_fundamentals": scalar(conn, "SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL"),
        "model_fingerprints_unchanged": before["fingerprints"]["score"] == after["fingerprints"]["score"]
        and before["fingerprints"]["lifecycle"] == after["fingerprints"]["lifecycle"],
        "fundamentals_unchanged": before["fingerprints"]["fundamentals"] == after["fingerprints"]["fundamentals"],
        "ttm_unchanged": before["fingerprints"]["ttm"] == after["fingerprints"]["ttm"],
        "score_unchanged": before["fingerprints"]["score"] == after["fingerprints"]["score"],
        "lifecycle_unchanged": before["fingerprints"]["lifecycle"] == after["fingerprints"]["lifecycle"],
        "valuation_unchanged": before["fingerprints"]["valuation"] == after["fingerprints"]["valuation"],
    }


def run_phase8a9(paths: Phase8A9Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    verified_rows = read_csv(paths.verified_csv)
    external_queue_rows = read_csv(paths.a8_external_queue_csv)
    verified_by_id = {row["Request ID"]: row for row in verified_rows}

    input_summary, input_checks = validate_verified_input(verified_rows)
    original_reconciliation = reconcile_original_columns(verified_rows, external_queue_rows)
    write_json(paths.artifact_root / "input_reconciliation_summary.json", input_summary)
    write_csv(paths.artifact_root / "input_reconciliation_checks.csv", input_checks)
    write_csv(paths.artifact_root / "original_column_reconciliation.csv", original_reconciliation)

    with connect(paths.v3_db) as conn:
        before = preflight(conn, paths.v3_db)
        if before["quick_check"] != "ok":
            raise RuntimeError(f"preflight quick_check failed: {before['quick_check']}")
        before_period_duplicates = same_company_period_end_duplicate_count(conn)
        write_json(paths.artifact_root / "preflight_snapshot.json", before)
        backup = make_backup(paths.v3_db, paths.artifact_root)
        write_json(paths.artifact_root / "backup_manifest.json", backup)

        current_reconciliation = current_v3_reconciliation(conn, verified_rows)
        guarded = sequence_guards(conn, current_reconciliation, verified_by_id)
        repairs = freeze_repairs(guarded, verified_by_id)
        retained_r1 = [row for row in guarded if int(row["sequence_guard_ok"]) != 1]
        write_csv(paths.artifact_root / "current_v3_reconciliation.csv", current_reconciliation)
        write_csv(paths.artifact_root / "period_end_sequence_guard.csv", guarded)
        write_csv(paths.artifact_root / "frozen_period_end_repair_set.csv", repairs)
        write_csv(paths.artifact_root / "retained_r1_reaudit.csv", retained_r1)

        apply_audit = apply_repairs(conn, repairs)
        after = preflight(conn, paths.v3_db)
        integrity = post_integrity(conn, before, after, before_period_duplicates)

    write_csv(paths.artifact_root / "apply_audit.csv", apply_audit)
    write_json(paths.artifact_root / "post_integrity.json", integrity)
    summary = {
        "classification": CLASSIFICATION_READY if not retained_r1 and len(repairs) == 18 else CLASSIFICATION_RESIDUAL,
        "db_path": str(paths.v3_db),
        "artifact_root": str(paths.artifact_root),
        "verified_rows": len(verified_rows),
        "frozen_repairs": len(repairs),
        "rows_applied": sum(1 for row in apply_audit if row["apply_status"] == "APPLIED"),
        "write_guard_failures": sum(1 for row in apply_audit if row["apply_status"] != "APPLIED"),
        "retained_r1": len(retained_r1),
        "retained_r1_by_guard": dict(Counter(row["sequence_guard"] for row in retained_r1)),
        "derived_data_status": DERIVED_STALE,
        "downstream_rebuild_run": 0,
        "backup": backup,
        "counts": {
            "companies": integrity["companies_after"],
            "canonical": integrity["canonical_rows_after"],
            "ttm": integrity["ttm_rows_after"],
            "score": integrity["score_rows_after"],
            "lifecycle": integrity["lifecycle_rows_after"],
            "valuation": integrity["valuation_rows_after"],
        },
        "next_action": "RESOLVE_SEQUENCE_COLLISION_R1_BEFORE_COMBINED_DOWNSTREAM_REBUILD"
        if retained_r1
        else "COMBINED_DOWNSTREAM_REBUILD",
    }
    write_json(paths.artifact_root / "phase8a9_summary.json", summary)
    return summary
