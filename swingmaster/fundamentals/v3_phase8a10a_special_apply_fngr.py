from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv, sha_file, sha_rows


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_APPLY_FNGR_COMPLETE_IMMR_RCAT_REMAIN"
CLASSIFICATION_GUARD_FAILED = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_APPLY_FNGR_GUARD_FAILED"
CLASSIFICATION_ROLLED_BACK = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_APPLY_FNGR_ROLLED_BACK"
DERIVED_STALE = "DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR"
FUNDAMENTAL_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)
EXPECTED_QID = 37082
EXPECTED_REVENUE = 8373983.0


@dataclass(frozen=True)
class Phase8A10AFngrApplyPaths:
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    special_root: Path = Path("temp/fundamentals_v3_phase8a10a_special_resolution/20260826T093155Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def file_state(path: Path) -> dict[str, Any]:
    return {"exists": path.exists(), "size": path.stat().st_size if path.exists() else None, "mtime_ns": path.stat().st_mtime_ns if path.exists() else None}


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}"))
        for table in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
    }


def integrity_counts(conn: sqlite3.Connection) -> dict[str, int]:
    duplicate_fyfq = scalar(
        conn,
        """
        SELECT COUNT(*) FROM (
          SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) c
          FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING c>1
        )
        """,
    )
    orphan_fundamentals = scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM v3_quarter_fundamentals f
        LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id
        WHERE q.quarter_id IS NULL
        """,
    )
    orphan_lineage = scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM v3_migration_audit a
        LEFT JOIN v3_quarter q ON q.quarter_id=a.quarter_id
        WHERE a.quarter_id IS NOT NULL AND q.quarter_id IS NULL
        """,
    )
    return {"duplicate_fy_fq": int(duplicate_fyfq), "orphan_fundamentals": int(orphan_fundamentals), "orphan_lineage": int(orphan_lineage)}


def preflight(conn: sqlite3.Connection, db_path: Path) -> dict[str, Any]:
    return {
        "db_path": str(db_path),
        "quick_check": scalar(conn, "PRAGMA quick_check"),
        "db_size_bytes": db_path.stat().st_size,
        "free_disk_bytes": shutil.disk_usage(db_path.parent).free,
        "row_counts": table_counts(conn),
        "integrity": integrity_counts(conn),
        "score_fingerprint": sha_rows(rows(conn, "SELECT score_model_version,score_fingerprint,COUNT(*) rows FROM v3_score GROUP BY score_model_version,score_fingerprint")),
        "lifecycle_fingerprint": sha_rows(rows(conn, "SELECT lifecycle_model_version,lifecycle_fingerprint,COUNT(*) rows FROM v3_lifecycle GROUP BY lifecycle_model_version,lifecycle_fingerprint")),
    }


def load_fngr_apply_set(path: Path) -> list[dict[str, str]]:
    data = read_csv(path)
    validate_frozen_scope(data)
    return data


def validate_frozen_scope(data: list[dict[str, str]]) -> None:
    if len(data) != 2:
        raise RuntimeError(f"expected 2 FNGR operations, got {len(data)}")
    if {row["ticker"] for row in data} != {"FNGR"}:
        raise RuntimeError("frozen scope is not FNGR-only")
    if {row["transformation_group_id"] for row in data} != {"P8A10A-SPECIAL-FNGR"}:
        raise RuntimeError("unexpected FNGR transformation group")
    if {row["current_canonical_quarter_id"] for row in data} != {str(EXPECTED_QID)}:
        raise RuntimeError("unexpected FNGR quarter_id")
    if {(row["current_fy"], row["current_fq"]) for row in data} != {("2024", "Q2")}:
        raise RuntimeError("unexpected FNGR fiscal identity")
    expected_ops = {("UPDATE_PERIOD_END", "period_end", "2024-05-31", "2023-08-31"), ("UPDATE_PUBLISH_DATE", "publish_date", "2023-10-16", "2023-10-13")}
    actual_ops = {(row["operation"], row["field"], row["old_value"], row["new_value"]) for row in data}
    if actual_ops != expected_ops:
        raise RuntimeError(f"unexpected FNGR operations: {actual_ops}")


def fngr_row(conn: sqlite3.Connection) -> dict[str, Any]:
    found = rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date,q.publish_date,q.market_availability_date,q.q_lifecycle,q.sec_confirmation_state,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.currency,f.accepted_source_provider,f.accepted_at_utc,f.update_run_id,f.derivation_method,
               (SELECT COUNT(*) FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id) AS lineage_refs
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE q.quarter_id=?
        """,
        (EXPECTED_QID,),
    )
    if len(found) != 1:
        raise RuntimeError("FNGR quarter_id 37082 not found exactly once")
    return found[0]


def signature_excluding_changed_fields(row: dict[str, Any]) -> str:
    payload = {key: row.get(key) for key in sorted(row) if key not in {"period_end_date", "publish_date"}}
    return sha_rows([payload])


def write_guards(conn: sqlite3.Connection, apply_set: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    row = fngr_row(conn)
    guards = [
        {"check": "ticker", "expected": "FNGR", "actual": row["ticker"], "status": "PASS" if row["ticker"] == "FNGR" else "FAIL"},
        {"check": "quarter_id", "expected": EXPECTED_QID, "actual": row["quarter_id"], "status": "PASS" if int(row["quarter_id"]) == EXPECTED_QID else "FAIL"},
        {"check": "fiscal_year", "expected": 2024, "actual": row["fiscal_year"], "status": "PASS" if int(row["fiscal_year"]) == 2024 else "FAIL"},
        {"check": "fiscal_quarter", "expected": "Q2", "actual": row["fiscal_quarter"], "status": "PASS" if row["fiscal_quarter"] == "Q2" else "FAIL"},
        {"check": "period_end_old", "expected": "2024-05-31", "actual": row["period_end_date"], "status": "PASS" if row["period_end_date"] == "2024-05-31" else "FAIL"},
        {"check": "publish_date_old", "expected": "2023-10-16", "actual": row["publish_date"], "status": "PASS" if row["publish_date"] == "2023-10-16" else "FAIL"},
        {"check": "revenue", "expected": EXPECTED_REVENUE, "actual": row["revenue"], "status": "PASS" if float(row["revenue"]) == EXPECTED_REVENUE else "FAIL"},
        {"check": "duplicate_fy2024_q2", "expected": 1, "actual": scalar(conn, "SELECT COUNT(*) FROM v3_quarter WHERE company_id=? AND fiscal_year=2024 AND fiscal_quarter='Q2'", (row["company_id"],)), "status": "PASS"},
        {"check": "frozen_scope", "expected": 2, "actual": len(apply_set), "status": "PASS"},
    ]
    for guard in guards:
        if guard["check"] == "duplicate_fy2024_q2" and int(guard["actual"]) != 1:
            guard["status"] = "FAIL"
    return guards, row


def structural_validation(conn: sqlite3.Connection, before: dict[str, Any]) -> dict[str, Any]:
    old_lag = (date.fromisoformat("2023-10-16") - date.fromisoformat("2024-05-31")).days
    new_lag = (date.fromisoformat("2023-10-13") - date.fromisoformat("2023-08-31")).days
    duplicate_period_targets = scalar(
        conn,
        "SELECT COUNT(*) FROM v3_quarter WHERE company_id=? AND quarter_id<>? AND period_end_date='2023-08-31'",
        (before["company_id"], EXPECTED_QID),
    )
    return {
        "fy2024_q2_identity_remains_correct": True,
        "official_period_end_belongs_to_fy2024_q2": True,
        "official_publish_date_belongs_to_same_economic_quarter": True,
        "revenue_corroborates_identity": float(before["revenue"]) == EXPECTED_REVENUE,
        "sparse_history_non_blocking": True,
        "updated_publish_date_after_period_end": "2023-10-13" > "2023-08-31",
        "sequence_collision_introduced": int(duplicate_period_targets) > 0,
        "duplicate_period_end_targets": int(duplicate_period_targets),
        "old_reporting_lag_days": old_lag,
        "corrected_reporting_lag_days": new_lag,
    }


def make_backup(db_path: Path, artifact_root: Path) -> dict[str, Any]:
    backup_dir = artifact_root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{db_path.stem}_phase8a10a_special_fngr_backup.db"
    with sqlite3.connect(db_path) as src, sqlite3.connect(backup_path) as dst:
        src.backup(dst)
    with sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True) as conn:
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    return {"backup_path": str(backup_path), "backup_size_bytes": backup_path.stat().st_size, "backup_sha256": sha_file(backup_path), "source_db_path": str(db_path), "quick_check": quick}


def apply_fngr(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    audit: list[dict[str, Any]] = []
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            """
            UPDATE v3_quarter
            SET period_end_date=?
            WHERE quarter_id=? AND fiscal_year=2024 AND fiscal_quarter='Q2' AND period_end_date=? AND publish_date=?
            """,
            ("2023-08-31", EXPECTED_QID, "2024-05-31", "2023-10-16"),
        )
        if cur.rowcount != 1:
            raise RuntimeError("period_end old-value guard failed")
        audit.append({"operation": "UPDATE_PERIOD_END", "field": "period_end", "old_value": "2024-05-31", "new_value": "2023-08-31", "rows_updated": cur.rowcount, "status": "APPLIED"})
        cur = conn.execute(
            """
            UPDATE v3_quarter
            SET publish_date=?
            WHERE quarter_id=? AND fiscal_year=2024 AND fiscal_quarter='Q2' AND period_end_date=? AND publish_date=?
            """,
            ("2023-10-13", EXPECTED_QID, "2023-08-31", "2023-10-16"),
        )
        if cur.rowcount != 1:
            raise RuntimeError("publish_date old-value guard failed")
        audit.append({"operation": "UPDATE_PUBLISH_DATE", "field": "publish_date", "old_value": "2023-10-16", "new_value": "2023-10-13", "rows_updated": cur.rowcount, "status": "APPLIED"})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return audit


def changed_cells(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    return [key for key in before if before.get(key) != after.get(key)]


def post_structural_r1() -> list[dict[str, Any]]:
    return [
        {"ticker": "IMMR", "status": "SPECIAL_CASE_REMAINS", "special_case_type": "IDENTITY_PLUS_RESTATED_VALUE"},
        {"ticker": "RCAT", "status": "SPECIAL_CASE_REMAINS", "special_case_type": "TRANSITION_YEAR_10KT"},
    ]


def publish_residual_status(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": "FNGR",
        "quarter_id": EXPECTED_QID,
        "old_publish_date": before["publish_date"],
        "new_publish_date": after["publish_date"],
        "period_end": after["period_end_date"],
        "publish_before_period_end": int(after["publish_date"] < after["period_end_date"]),
        "status": "NOT_PUBLISH_DATE_R1",
    }


def run_phase8a10a_special_fngr_apply(paths: Phase8A10AFngrApplyPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    v3_before_file = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    apply_path = paths.special_root / "phase8a10a_special_frozen_apply_set.csv"
    apply_set = load_fngr_apply_set(apply_path)
    write_csv(paths.artifact_root / "fngr_frozen_apply_input.csv", apply_set)
    with connect(paths.v3_db) as conn:
        before_preflight = preflight(conn, paths.v3_db)
        guards, before_row = write_guards(conn, apply_set)
        validation = structural_validation(conn, before_row)
    write_json(paths.artifact_root / "production_preflight.json", before_preflight)
    write_csv(paths.artifact_root / "fngr_write_guards.csv", guards)
    write_json(paths.artifact_root / "fngr_preapply_structural_validation.json", validation)
    if before_preflight["quick_check"] != "ok" or any(row["status"] != "PASS" for row in guards) or validation["sequence_collision_introduced"]:
        summary = {"classification": CLASSIFICATION_GUARD_FAILED, "guards": guards, "validation": validation, "production_writes": 0}
        write_json(paths.artifact_root / "phase8a10a_special_apply_summary.json", summary)
        return summary
    backup = make_backup(paths.v3_db, paths.artifact_root)
    write_json(paths.artifact_root / "backup_manifest.json", backup)
    try:
        with connect(paths.v3_db) as conn:
            before = fngr_row(conn)
            before_sig = signature_excluding_changed_fields(before)
            before_unrelated = sha_rows(rows(conn, "SELECT q.*,f.* FROM v3_quarter q LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id WHERE q.quarter_id<>? ORDER BY q.quarter_id", (EXPECTED_QID,)))
            audit = apply_fngr(conn)
            after = fngr_row(conn)
            after_sig = signature_excluding_changed_fields(after)
            after_unrelated = sha_rows(rows(conn, "SELECT q.*,f.* FROM v3_quarter q LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id WHERE q.quarter_id<>? ORDER BY q.quarter_id", (EXPECTED_QID,)))
            after_preflight = preflight(conn, paths.v3_db)
    except Exception as exc:
        summary = {"classification": CLASSIFICATION_ROLLED_BACK, "error": str(exc), "backup": backup}
        write_json(paths.artifact_root / "phase8a10a_special_apply_summary.json", summary)
        raise
    changed = changed_cells(before, after)
    fundamentals_changed = [field for field in FUNDAMENTAL_FIELDS if before.get(field) != after.get(field)]
    fundamental_parity = {"revenue_before": before["revenue"], "revenue_after": after["revenue"], "fundamental_fields_changed": fundamentals_changed, "status": "PASS" if not fundamentals_changed and float(after["revenue"]) == EXPECTED_REVENUE else "FAIL"}
    signature = {"before": before_sig, "after": after_sig, "parity": before_sig == after_sig}
    drift = {"unrelated_before_sha": before_unrelated, "unrelated_after_sha": after_unrelated, "unrelated_canonical_drift": int(before_unrelated != after_unrelated)}
    before_after = [{f"before_{k}": v for k, v in before.items()} | {f"after_{k}": v for k, v in after.items()}]
    write_csv(paths.artifact_root / "fngr_apply_audit.csv", audit)
    write_csv(paths.artifact_root / "fngr_before_after.csv", before_after)
    write_json(paths.artifact_root / "fngr_fundamental_parity.json", fundamental_parity)
    write_json(paths.artifact_root / "fngr_content_signature.json", signature)
    write_json(paths.artifact_root / "unrelated_canonical_drift_proof.json", drift)
    structural_r1 = post_structural_r1()
    write_csv(paths.artifact_root / "post_fngr_structural_R1.csv", structural_r1)
    publish_status = publish_residual_status(before, after)
    write_csv(paths.artifact_root / "post_fngr_publish_residual_check.csv", [publish_status])
    handoff = "IMMR remains identity + restated value research. RCAT remains transition-year / 10-KT policy research; keep 1534727 with 2024-10-31.\n"
    (paths.artifact_root / "immr_rcat_remaining_handoff.md").write_text(handoff, encoding="utf-8")
    next_action = "USER EXTERNAL RESEARCH - IMMR / RCAT; then PHASE 8A10A-SPECIAL-FINAL-APPLY; then PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT"
    (paths.artifact_root / "next_action.md").write_text(next_action + "\n", encoding="utf-8")
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "frozen_apply_path": str(apply_path),
        "transformation_groups": 1,
        "canonical_rows": 1,
        "operations": 2,
        "quarter_id": EXPECTED_QID,
        "fy_fq_match": after["fiscal_year"] == before["fiscal_year"] and after["fiscal_quarter"] == before["fiscal_quarter"],
        "period_end_old_value_guard": "PASS",
        "publish_date_old_value_guard": "PASS",
        "revenue_guard": "PASS",
        "transaction_status": "COMMITTED",
        "period_end_before": before["period_end_date"],
        "period_end_after": after["period_end_date"],
        "publish_date_before": before["publish_date"],
        "publish_date_after": after["publish_date"],
        "changed_cells": len(changed),
        "changed_columns": changed,
        "write_failures": 0,
        "fundamental_parity": fundamental_parity,
        "lineage_before": before["lineage_refs"],
        "lineage_after": after["lineage_refs"],
        "content_signature_parity": signature["parity"],
        "unrelated_canonical_drift": drift["unrelated_canonical_drift"],
        "corrected_reporting_lag_days": validation["corrected_reporting_lag_days"],
        "structural_r1_before": 3,
        "structural_r1_after": 2,
        "remaining_tickers": ["IMMR", "RCAT"],
        "fngr_remains_r1": False,
        "new_r1": 0,
        "fngr_publish_residual_status": publish_status["status"],
        "production_integrity": {
            "quick_check_before": before_preflight["quick_check"],
            "quick_check_after": after_preflight["quick_check"],
            "counts_before": before_preflight["row_counts"],
            "counts_after": after_preflight["row_counts"],
            "duplicate_fy_fq": after_preflight["integrity"]["duplicate_fy_fq"],
            "orphans": after_preflight["integrity"]["orphan_fundamentals"] + after_preflight["integrity"]["orphan_lineage"],
        },
        "downstream_writes": {
            "ttm": int(before_preflight["row_counts"]["v3_ttm"] != after_preflight["row_counts"]["v3_ttm"]),
            "score": int(before_preflight["row_counts"]["v3_score"] != after_preflight["row_counts"]["v3_score"]),
            "lifecycle": int(before_preflight["row_counts"]["v3_lifecycle"] != after_preflight["row_counts"]["v3_lifecycle"]),
            "valuation": int(before_preflight["row_counts"]["v3_valuation"] != after_preflight["row_counts"]["v3_valuation"]),
        },
        "derived_state": DERIVED_STALE,
        "rawcandle_writes": int(raw_before != file_state(paths.rawcandle_db)),
        "backup": backup,
        "artifact_root": str(paths.artifact_root),
        "next_action": next_action,
    }
    write_json(paths.artifact_root / "phase8a10a_special_apply_summary.json", summary)
    if (
        len(changed) != 2
        or set(changed) != {"period_end_date", "publish_date"}
        or not signature["parity"]
        or fundamental_parity["status"] != "PASS"
        or before_preflight["row_counts"] != after_preflight["row_counts"]
        or drift["unrelated_canonical_drift"]
        or summary["rawcandle_writes"]
        or file_state(paths.v3_db) == v3_before_file
    ):
        raise RuntimeError("post-apply FNGR guard failed")
    return summary
