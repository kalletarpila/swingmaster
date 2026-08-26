from __future__ import annotations

import csv
import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a6_safe_apply import sha_file, sha_rows


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE8A10A_PUBLISH_VERIFIED_REPAIRS_COMPLETE"
CLASSIFICATION_COMPLETE_EXCEPT_STRUCTURAL = "FUNDAMENTALS_V3_PHASE8A10A_PUBLISH_REPAIRS_COMPLETE_EXCEPT_STRUCTURAL_BLOCKERS"
CLASSIFICATION_REPAIR_GUARD_FAILURES = "FUNDAMENTALS_V3_PHASE8A10A_PUBLISH_REPAIR_GUARD_FAILURES_REMAIN"
CLASSIFICATION_GUARD_FAILED = "FUNDAMENTALS_V3_PHASE8A10A_PUBLISH_APPLY_GUARD_FAILED"
CLASSIFICATION_ROLLED_BACK = "FUNDAMENTALS_V3_PHASE8A10A_PUBLISH_APPLY_ROLLED_BACK"
CLASSIFICATION_STALE = "STALE_PUBLISH_DATE_WRITE_GUARD_FAILED"
CLASSIFICATION_STRUCTURAL_BLOCK = "BLOCKED_BY_STRUCTURAL_IDENTITY_R1"
DERIVED_STALE = "DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR"
EXPECTED_ROWS = 17
STRUCTURAL_R1_REMAINING = {"IMMR", "RCAT"}
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
FUNDAMENTAL_AUDIT_FIELDS = (
    *FUNDAMENTAL_FIELDS,
    "currency",
    "accepted_source_provider",
    "accepted_at_utc",
    "update_run_id",
    "derivation_method",
    "resolution_issue_id",
    "created_at_utc",
    "updated_at_utc",
)
REQUIRED_COLUMNS = {
    "Ticker",
    "Fiscal Year",
    "Fiscal Q",
    "Period End",
    "Current Publish Date",
    "Current Canonical Quarter ID",
    "Verified Publish Date",
    "Status",
    "Confidence",
    "Source Count",
    "Primary Source",
    "Primary Source Type",
    "Verification Method",
}


@dataclass(frozen=True)
class Phase8A10APublishApplyPaths:
    artifact_root: Path
    v3_db: Path
    verified_csv: Path | None = None
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def find_verified_csv(temp_root: Path = Path("temp")) -> Path:
    exact = temp_root / "phase8_publish_date_residual_17_verified.csv"
    if exact.exists():
        return exact
    matches = sorted(temp_root.glob("phase8_publish_date_residual_17_verified*.csv"))
    if matches:
        return matches[0]
    raise FileNotFoundError("verified 17-row publish residual CSV not found under temp")


def read_verified_csv(path: Path) -> list[dict[str, str]]:
    if "external_check" in path.name:
        raise RuntimeError(f"Refusing external research queue as authoritative verified input: {path}")
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"Verified publish CSV missing columns: {sorted(missing)}")
        return list(reader)


def validate_iso_date(value: str) -> bool:
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def validate_verified_input(data: list[dict[str, str]], source_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    statuses = Counter(row["Status"].strip().upper() for row in data)
    confidence = Counter(row["Confidence"].strip().upper() for row in data)
    source_counts = Counter(int(row["Source Count"]) for row in data if row.get("Source Count", "").strip().isdigit())
    unique_keys = {(row["Ticker"], row["Fiscal Year"], row["Fiscal Q"]) for row in data}
    validations = [
        ("row_count", EXPECTED_ROWS, len(data)),
        ("DIFFERENT", EXPECTED_ROWS, statuses.get("DIFFERENT", 0)),
        ("MATCH", 0, statuses.get("MATCH", 0)),
        ("UNCERTAIN", 0, statuses.get("UNCERTAIN", 0)),
        ("NOT_FOUND", 0, statuses.get("NOT_FOUND", 0)),
        ("IDENTITY_CONFLICT", 0, statuses.get("IDENTITY_CONFLICT", 0)),
        ("HIGH", EXPECTED_ROWS, confidence.get("HIGH", 0)),
        ("MEDIUM", 0, confidence.get("MEDIUM", 0)),
        ("LOW", 0, confidence.get("LOW", 0)),
        ("source_count_ge_2", 6, sum(count for source_count, count in source_counts.items() if source_count >= 2)),
        ("source_count_eq_1", 11, source_counts.get(1, 0)),
        ("verified_publish_date_complete", EXPECTED_ROWS, sum(1 for row in data if row.get("Verified Publish Date"))),
        ("verified_publish_date_iso", EXPECTED_ROWS, sum(1 for row in data if validate_iso_date(row.get("Verified Publish Date", "")))),
        ("unique_ticker_fy_fq", EXPECTED_ROWS, len(unique_keys)),
    ]
    recon = [{"metric": metric, "expected": expected, "actual": actual, "status": "PASS" if expected == actual else "FAIL"} for metric, expected, actual in validations]
    manifest = {
        "source_path": str(source_path),
        "source_sha256": sha_file(source_path),
        "rows": len(data),
        "status_counts": dict(statuses),
        "confidence_counts": dict(confidence),
        "source_count_distribution": dict(source_counts),
        "unique_ticker_fy_fq": len(unique_keys),
        "validation_status": "PASS" if all(row["status"] == "PASS" for row in recon) else "FAIL",
    }
    return manifest, recon


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}"))
        for table in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
    }


def integrity_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "duplicate_fy_fq": int(
            scalar(
                conn,
                """
                SELECT COUNT(*) FROM (
                  SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) c
                  FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING c>1
                )
                """,
            )
        ),
        "orphan_fundamentals": int(
            scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM v3_quarter_fundamentals f
                LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id
                WHERE q.quarter_id IS NULL
                """,
            )
        ),
        "orphan_migration_audit": int(
            scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM v3_migration_audit a
                LEFT JOIN v3_quarter q ON q.quarter_id=a.quarter_id
                WHERE a.quarter_id IS NOT NULL AND q.quarter_id IS NULL
                """,
            )
        ),
    }


def file_state(path: Path) -> dict[str, Any]:
    return {
        "exists": path.exists(),
        "size": path.stat().st_size if path.exists() else None,
        "mtime_ns": path.stat().st_mtime_ns if path.exists() else None,
    }


def table_fingerprint(conn: sqlite3.Connection, table: str, id_column: str) -> str:
    return sha_rows(rows(conn, f"SELECT * FROM {table} ORDER BY {id_column}"))


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
        "derived_fingerprints": {
            "v3_ttm": table_fingerprint(conn, "v3_ttm", "ttm_id"),
            "v3_score": table_fingerprint(conn, "v3_score", "score_id"),
            "v3_lifecycle": table_fingerprint(conn, "v3_lifecycle", "lifecycle_id"),
            "v3_valuation": table_fingerprint(conn, "v3_valuation", "valuation_id"),
        },
    }


def current_publish_row(conn: sqlite3.Connection, ticker: str, fiscal_year: int, fiscal_quarter: str) -> dict[str, Any] | None:
    found = rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date,q.publish_date,q.market_availability_date,q.q_lifecycle,q.sec_confirmation_state,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.currency,f.accepted_source_provider,f.accepted_at_utc,f.update_run_id,f.derivation_method,
               f.resolution_issue_id,f.created_at_utc AS fundamentals_created_at_utc,
               f.updated_at_utc AS fundamentals_updated_at_utc,
               (SELECT COUNT(*) FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id) AS lineage_refs
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
        """,
        (ticker, fiscal_year, fiscal_quarter),
    )
    return found[0] if len(found) == 1 else None


def row_signature(row: dict[str, Any], *, excluding_publish_date: bool) -> str:
    excluded = {"publish_date"} if excluding_publish_date else set()
    return sha_rows([{key: row.get(key) for key in sorted(row) if key not in excluded}])


def fundamental_signature(row: dict[str, Any]) -> str:
    return sha_rows([{field: row.get(field) for field in FUNDAMENTAL_AUDIT_FIELDS if field in row}])


def reconcile_input_to_current(conn: sqlite3.Connection, data: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    guards: list[dict[str, Any]] = []
    chronology: list[dict[str, Any]] = []
    frozen: list[dict[str, Any]] = []
    for index, row in enumerate(data, 1):
        ticker = row["Ticker"].strip()
        fiscal_year = int(row["Fiscal Year"])
        fiscal_quarter = row["Fiscal Q"].strip()
        current = current_publish_row(conn, ticker, fiscal_year, fiscal_quarter)
        verified_publish = row["Verified Publish Date"].strip()
        expected_qid = int(row["Current Canonical Quarter ID"])
        structural_blocked = ticker in STRUCTURAL_R1_REMAINING
        mismatches: list[str] = []
        if current is None:
            mismatches.append("current_identity_not_found")
        else:
            checks = {
                "quarter_id": int(current["quarter_id"]) == expected_qid,
                "period_end": (current["period_end_date"] or "") == row["Period End"],
                "old_publish_date": (current["publish_date"] or "") == row["Current Publish Date"],
                "fiscal_year": int(current["fiscal_year"]) == fiscal_year,
                "fiscal_quarter": current["fiscal_quarter"] == fiscal_quarter,
            }
            mismatches.extend(key for key, ok in checks.items() if not ok)
        status = "PASS"
        if structural_blocked:
            status = CLASSIFICATION_STRUCTURAL_BLOCK
        elif mismatches:
            status = CLASSIFICATION_STALE if "old_publish_date" in mismatches else "GUARD_FAILED"
        period_end = current["period_end_date"] if current else row["Period End"]
        lag_days = (date.fromisoformat(verified_publish) - date.fromisoformat(period_end)).days if validate_iso_date(verified_publish) and validate_iso_date(period_end) else ""
        chronology_status = "PASS" if lag_days != "" and int(lag_days) >= 0 else "FAIL"
        if ticker == "BRTX" and fiscal_year == 2020 and fiscal_quarter in {"Q2", "Q3"} and verified_publish == "2021-04-12":
            duplicate_publish_exception = "BRTX_FY2020_Q2_Q3_SAME_DAY_ACCEPTED"
        else:
            duplicate_publish_exception = ""
        first_public_rule = "PASS"
        earnings = row.get("Earnings Release Date", "").strip()
        sec = row.get("SEC Filing Date", "").strip()
        known_dates = [d for d in (earnings, sec) if validate_iso_date(d)]
        if known_dates and verified_publish != min(known_dates):
            first_public_rule = "FAIL"
        chronology.append(
            {
                "repair_id": f"P8A10A-PUBLISH-{index:03d}",
                "ticker": ticker,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "period_end": period_end,
                "verified_publish_date": verified_publish,
                "reporting_lag_days": lag_days,
                "same_day_multi_quarter_exception": duplicate_publish_exception,
                "first_public_disclosure_rule": first_public_rule,
                "status": chronology_status if first_public_rule == "PASS" else "FAIL",
            }
        )
        guards.append(
            {
                "repair_id": f"P8A10A-PUBLISH-{index:03d}",
                "ticker": ticker,
                "company_id": current["company_id"] if current else "",
                "quarter_id": current["quarter_id"] if current else "",
                "expected_quarter_id": expected_qid,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "expected_period_end": row["Period End"],
                "current_period_end": current["period_end_date"] if current else "",
                "old_publish_date_expected": row["Current Publish Date"],
                "current_publish_date_db": current["publish_date"] if current else "",
                "new_publish_date": verified_publish,
                "old_value_guard": "PASS" if status == "PASS" else status,
                "structural_identity_guard": "PASS" if not structural_blocked else CLASSIFICATION_STRUCTURAL_BLOCK,
                "mismatches": ",".join(mismatches),
                "signature_before_ex_publish": row_signature(current, excluding_publish_date=True) if current else "",
                "fundamental_signature_before": fundamental_signature(current) if current else "",
                "lineage_refs_before": current["lineage_refs"] if current else "",
            }
        )
        if status == "PASS" and chronology_status == "PASS" and first_public_rule == "PASS":
            frozen.append(
                {
                    "repair_id": f"P8A10A-PUBLISH-{index:03d}",
                    "company_id": current["company_id"],
                    "quarter_id": current["quarter_id"],
                    "ticker": ticker,
                    "fiscal_year": fiscal_year,
                    "fiscal_quarter": fiscal_quarter,
                    "period_end": period_end,
                    "old_publish_date": current["publish_date"],
                    "new_publish_date": verified_publish,
                    "reporting_lag_days": lag_days,
                    "status": row["Status"],
                    "confidence": row["Confidence"],
                    "source_count": row["Source Count"],
                    "primary_source": row.get("Primary Source", ""),
                    "secondary_source": row.get("Secondary Source", ""),
                    "verification_method": row.get("Verification Method", ""),
                    "old_value_guard": "PASS",
                    "structural_identity_guard": "PASS",
                }
            )
    return guards, chronology, frozen


def canonical_snapshot(conn: sqlite3.Connection, affected_qids: set[int], *, invert: bool) -> list[dict[str, Any]]:
    if affected_qids:
        placeholders = ",".join("?" for _ in affected_qids)
        op = "NOT IN" if invert else "IN"
        return rows(
            conn,
            f"""
            SELECT q.*,f.*
            FROM v3_quarter q LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE q.quarter_id {op} ({placeholders})
            ORDER BY q.quarter_id
            """,
            sorted(affected_qids),
        )
    return rows(conn, "SELECT q.*,f.* FROM v3_quarter q LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id ORDER BY q.quarter_id")


def publish_residual_rows(conn: sqlite3.Connection, *, today: date) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,q.market_availability_date
        FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
        WHERE (q.publish_date IS NOT NULL AND q.period_end_date IS NOT NULL AND q.publish_date < q.period_end_date)
           OR (q.publish_date IS NOT NULL AND q.publish_date > ?)
           OR (q.market_availability_date IS NOT NULL AND q.publish_date IS NOT NULL AND q.market_availability_date < q.publish_date)
        ORDER BY c.ticker,q.period_end_date
        """,
        (today.isoformat(),),
    )


def publish_residual_tier(row: dict[str, Any], *, today: date) -> str:
    publish = row.get("publish_date")
    period = row.get("period_end_date")
    market = row.get("market_availability_date")
    if publish and period and publish < period:
        return "R1_PUBLISH_BEFORE_PERIOD_END"
    if publish and publish > today.isoformat():
        return "R1_PUBLISH_IN_FUTURE"
    if market and publish and market < publish:
        return "R2_MARKET_AVAILABILITY_STALE_AFTER_PUBLISH_REPAIR"
    return "R3_OTHER_HEURISTIC_FLAG"


def make_backup(db_path: Path, artifact_root: Path, source_csv: Path) -> dict[str, Any]:
    backup_dir = artifact_root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{db_path.stem}_phase8a10a_publish_apply_backup.db"
    source_db_sha = sha_file(db_path)
    with sqlite3.connect(db_path) as src, sqlite3.connect(backup_path) as dst:
        src.backup(dst)
    with sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True) as conn:
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
    return {
        "backup_path": str(backup_path),
        "backup_size_bytes": backup_path.stat().st_size,
        "backup_sha256": sha_file(backup_path),
        "source_db_path": str(db_path),
        "source_db_sha256": source_db_sha,
        "source_csv_path": str(source_csv),
        "source_csv_sha256": sha_file(source_csv),
        "quick_check": quick_check,
    }


def apply_publish_dates(conn: sqlite3.Connection, frozen: list[dict[str, Any]]) -> list[dict[str, Any]]:
    audit: list[dict[str, Any]] = []
    conn.execute("BEGIN IMMEDIATE")
    try:
        for plan in frozen:
            cur = conn.execute(
                """
                UPDATE v3_quarter
                SET publish_date=?
                WHERE quarter_id=?
                  AND company_id=?
                  AND fiscal_year=?
                  AND fiscal_quarter=?
                  AND COALESCE(period_end_date,'')=?
                  AND COALESCE(publish_date,'')=?
                """,
                (
                    plan["new_publish_date"],
                    int(plan["quarter_id"]),
                    int(plan["company_id"]),
                    int(plan["fiscal_year"]),
                    plan["fiscal_quarter"],
                    plan["period_end"],
                    plan["old_publish_date"],
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"publish_date old-value guard failed for quarter_id={plan['quarter_id']}")
            audit.append({**plan, "field": "publish_date", "rows_updated": cur.rowcount, "status_after_apply": "APPLIED"})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return audit


def final_parity(conn: sqlite3.Connection, frozen: list[dict[str, Any]], guards: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    guard_by_qid = {int(row["quarter_id"]): row for row in guards if row["quarter_id"] != ""}
    parity: list[dict[str, Any]] = []
    before_after: list[dict[str, Any]] = []
    fundamental_rows: list[dict[str, Any]] = []
    lineage_rows: list[dict[str, Any]] = []
    changed_cells = 0
    for plan in frozen:
        qid = int(plan["quarter_id"])
        current = current_publish_row(conn, plan["ticker"], int(plan["fiscal_year"]), plan["fiscal_quarter"])
        guard = guard_by_qid[qid]
        unchanged_signature = row_signature(current, excluding_publish_date=True) if current else ""
        fundamental_after = fundamental_signature(current) if current else ""
        lineage_after = current["lineage_refs"] if current else ""
        publish_changed = current is not None and guard["current_publish_date_db"] != current["publish_date"]
        changed_cells += int(publish_changed)
        ok = (
            current is not None
            and int(current["quarter_id"]) == qid
            and current["publish_date"] == plan["new_publish_date"]
            and current["period_end_date"] == plan["period_end"]
            and int(current["fiscal_year"]) == int(plan["fiscal_year"])
            and current["fiscal_quarter"] == plan["fiscal_quarter"]
            and unchanged_signature == guard["signature_before_ex_publish"]
        )
        parity.append(
            {
                "repair_id": plan["repair_id"],
                "ticker": plan["ticker"],
                "quarter_id": qid,
                "expected_publish_date": plan["new_publish_date"],
                "actual_publish_date": current["publish_date"] if current else "",
                "expected_period_end": plan["period_end"],
                "actual_period_end": current["period_end_date"] if current else "",
                "fy_fq_unchanged": int(current is not None and int(current["fiscal_year"]) == int(plan["fiscal_year"]) and current["fiscal_quarter"] == plan["fiscal_quarter"]),
                "period_end_unchanged": int(current is not None and current["period_end_date"] == plan["period_end"]),
                "only_publish_date_changed": int(unchanged_signature == guard["signature_before_ex_publish"]),
                "status": "PASS" if ok else "FAIL",
            }
        )
        before_after.append(
            {
                "repair_id": plan["repair_id"],
                "ticker": plan["ticker"],
                "quarter_id": qid,
                "publish_date_before": guard["current_publish_date_db"],
                "publish_date_after": current["publish_date"] if current else "",
                "period_end_before": plan["period_end"],
                "period_end_after": current["period_end_date"] if current else "",
            }
        )
        fundamental_rows.append(
            {
                "repair_id": plan["repair_id"],
                "ticker": plan["ticker"],
                "quarter_id": qid,
                "signature_before": guard["fundamental_signature_before"],
                "signature_after": fundamental_after,
                "status": "PASS" if guard["fundamental_signature_before"] == fundamental_after else "FAIL",
            }
        )
        lineage_rows.append(
            {
                "repair_id": plan["repair_id"],
                "ticker": plan["ticker"],
                "quarter_id": qid,
                "lineage_refs_before": guard["lineage_refs_before"],
                "lineage_refs_after": lineage_after,
                "status": "PASS" if str(guard["lineage_refs_before"]) == str(lineage_after) else "FAIL",
            }
        )
    fundamental_parity = {
        "rows_checked": len(fundamental_rows),
        "rows_passing": sum(1 for row in fundamental_rows if row["status"] == "PASS"),
        "fundamental_fields_changed": sum(1 for row in fundamental_rows if row["status"] != "PASS"),
        "details": fundamental_rows,
    }
    lineage_parity = {
        "rows_checked": len(lineage_rows),
        "rows_passing": sum(1 for row in lineage_rows if row["status"] == "PASS"),
        "lineage_rows_changed": sum(1 for row in lineage_rows if row["status"] != "PASS"),
        "details": lineage_rows,
    }
    return parity, before_after, fundamental_parity, lineage_parity | {"changed_publish_cells": changed_cells}


def write_handoffs(root: Path) -> None:
    (root / "immr_rcat_structural_handoff.md").write_text(
        "Remaining structural R1 cases are exactly `IMMR` and `RCAT`. This phase intentionally applies no publish-date, fiscal identity, period_end, value, or lineage changes to either ticker.\n",
        encoding="utf-8",
    )
    (root / "next_action.md").write_text(
        "USER EXTERNAL RESEARCH - IMMR / RCAT; then PHASE 8A10A-SPECIAL-FINAL-APPLY; then PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT\n",
        encoding="utf-8",
    )


def guard_failed_summary(root: Path, classification: str, reason: str, guards: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    summary = {
        "classification": classification,
        "reason": reason,
        "production_writes": 0,
        "artifact_root": str(root),
        "guard_failures": [row for row in guards or [] if row.get("old_value_guard") != "PASS" or row.get("structural_identity_guard") != "PASS"],
    }
    write_json(root / "phase8a10a_publish_apply_summary.json", summary)
    return summary


def run_phase8a10a_publish_apply(paths: Phase8A10APublishApplyPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    source_csv = paths.verified_csv or find_verified_csv()
    data = read_verified_csv(source_csv)
    raw_before = file_state(paths.rawcandle_db)
    manifest, input_recon = validate_verified_input(data, source_csv)
    write_json(paths.artifact_root / "verified_publish_input_manifest.json", manifest)
    write_csv(paths.artifact_root / "verified_publish_input_reconciliation.csv", input_recon)
    if manifest["validation_status"] != "PASS":
        return guard_failed_summary(paths.artifact_root, CLASSIFICATION_REPAIR_GUARD_FAILURES, "verified input validation failed")

    with connect(paths.v3_db) as conn:
        before = preflight(conn, paths.v3_db)
        residual_before = publish_residual_rows(conn, today=date(2026, 8, 26))
        guards, chronology, frozen = reconcile_input_to_current(conn, data)
        affected_qids = {int(row["quarter_id"]) for row in frozen}
        unrelated_before = canonical_snapshot(conn, affected_qids, invert=True)
        affected_before = canonical_snapshot(conn, affected_qids, invert=False)
    write_json(paths.artifact_root / "production_preflight.json", before)
    write_csv(paths.artifact_root / "publish_write_guards.csv", guards)
    write_csv(paths.artifact_root / "publish_current_value_guards.csv", guards)
    write_csv(
        paths.artifact_root / "publish_structural_identity_guards.csv",
        [
            {
                "repair_id": row["repair_id"],
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "structural_identity_guard": row["structural_identity_guard"],
                "status": "PASS" if row["structural_identity_guard"] == "PASS" else "FAIL",
            }
            for row in guards
        ],
    )
    write_csv(paths.artifact_root / "publish_chronology_sanity.csv", chronology)
    write_csv(paths.artifact_root / "publish_frozen_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8a10a_publish_verified_frozen_apply_set.csv", frozen)
    if before["quick_check"] != "ok":
        return guard_failed_summary(paths.artifact_root, CLASSIFICATION_REPAIR_GUARD_FAILURES, "production quick_check failed", guards)
    if any(row["old_value_guard"] != "PASS" or row["structural_identity_guard"] != "PASS" for row in guards):
        classification = CLASSIFICATION_COMPLETE_EXCEPT_STRUCTURAL if any(row["structural_identity_guard"] != "PASS" for row in guards) else CLASSIFICATION_STALE
        return guard_failed_summary(paths.artifact_root, classification, "write guard failed", guards)
    if len(frozen) != EXPECTED_ROWS or any(row["status"] != "PASS" for row in chronology):
        return guard_failed_summary(paths.artifact_root, CLASSIFICATION_REPAIR_GUARD_FAILURES, "frozen apply or chronology sanity failed", guards)
    original_qids = {int(row["quarter_id"]) for row in frozen}
    original_residual_before = [row for row in residual_before if int(row["quarter_id"]) in original_qids]
    if len(original_residual_before) != EXPECTED_ROWS:
        return guard_failed_summary(
            paths.artifact_root,
            CLASSIFICATION_REPAIR_GUARD_FAILURES,
            f"verified 17 publish residual baseline expected 17 matching original cases, got {len(original_residual_before)}",
            guards,
        )

    backup = make_backup(paths.v3_db, paths.artifact_root, source_csv)
    write_json(paths.artifact_root / "backup_manifest.json", backup)
    try:
        with connect(paths.v3_db) as conn:
            apply_audit = apply_publish_dates(conn, frozen)
            after = preflight(conn, paths.v3_db)
            parity, before_after, fundamental_parity, lineage_parity = final_parity(conn, frozen, guards)
            drift = {
                "affected_rows_before": len(affected_before),
                "affected_rows_after": len(canonical_snapshot(conn, {int(row["quarter_id"]) for row in frozen}, invert=False)),
                "unrelated_before_sha": sha_rows(unrelated_before),
                "unrelated_after_sha": sha_rows(canonical_snapshot(conn, {int(row["quarter_id"]) for row in frozen}, invert=True)),
            }
            drift["unrelated_canonical_drift"] = int(drift["unrelated_before_sha"] != drift["unrelated_after_sha"])
            residual_after = publish_residual_rows(conn, today=date(2026, 8, 26))
            original_residual_after = [row for row in residual_after if int(row["quarter_id"]) in original_qids]
            original_residual_after_tiered = [
                {**row, "retained_publish_tier": publish_residual_tier(row, today=date(2026, 8, 26))}
                for row in original_residual_after
            ]
            unrelated_residual_before_qids = {int(row["quarter_id"]) for row in residual_before if int(row["quarter_id"]) not in original_qids}
            unrelated_residual_after_qids = {int(row["quarter_id"]) for row in residual_after if int(row["quarter_id"]) not in original_qids}
    except Exception as exc:
        summary = {"classification": CLASSIFICATION_ROLLED_BACK, "error": str(exc), "backup": backup, "artifact_root": str(paths.artifact_root)}
        write_json(paths.artifact_root / "phase8a10a_publish_apply_summary.json", summary)
        raise

    write_csv(paths.artifact_root / "publish_apply_audit.csv", apply_audit)
    write_csv(paths.artifact_root / "publish_apply_parity.csv", parity)
    write_csv(paths.artifact_root / "publish_before_after.csv", before_after)
    write_csv(
        paths.artifact_root / "publish_content_signature_parity.csv",
        [
            {
                "repair_id": row["repair_id"],
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "only_publish_date_changed": row["only_publish_date_changed"],
                "status": row["status"],
            }
            for row in parity
        ],
    )
    write_json(paths.artifact_root / "publish_fundamental_parity.json", fundamental_parity)
    write_json(paths.artifact_root / "publish_lineage_parity.json", lineage_parity)
    write_json(paths.artifact_root / "unrelated_canonical_drift_proof.json", drift)
    write_csv(paths.artifact_root / "post_publish_residual_reaudit.csv", residual_after)
    write_csv(paths.artifact_root / "post_publish_original_17_retained_flags.csv", original_residual_after_tiered)
    retained_r1 = sum(1 for row in original_residual_after_tiered if str(row["retained_publish_tier"]).startswith("R1_"))
    retained_r2 = sum(1 for row in original_residual_after_tiered if str(row["retained_publish_tier"]).startswith("R2_"))
    retained_r3 = sum(1 for row in original_residual_after_tiered if str(row["retained_publish_tier"]).startswith("R3_"))
    residual_summary = {
        "raw_publish_audit_flags_before": len(residual_before),
        "raw_publish_audit_flags_after": len(residual_after),
        "original_17_residual_before": len(original_residual_before),
        "original_17_residual_after": len(original_residual_after),
        "original_17_resolved": EXPECTED_ROWS - len(original_residual_after),
        "original_17_publish_R1_after": retained_r1,
        "original_17_publish_R2_after": retained_r2,
        "original_17_publish_R3_after": retained_r3,
        "publish_residual_closed_for_verified_17": retained_r1 == 0,
        "new_unrelated_publish_R1": len(unrelated_residual_after_qids - unrelated_residual_before_qids),
        "structural_r1_remaining": sorted(STRUCTURAL_R1_REMAINING),
    }
    write_json(paths.artifact_root / "post_publish_residual_summary.json", residual_summary)
    write_csv(paths.artifact_root / "post_publish_apply_residuals.csv", residual_after)
    write_json(
        paths.artifact_root / "post_publish_apply_audit_summary.json",
        {
            **residual_summary,
            "raw_publish_audit_flags": len(residual_after),
            "retained_publish_R1": retained_r1,
            "retained_publish_R2": retained_r2,
            "retained_publish_R3": retained_r3,
            "original_17_still_residual": len(original_residual_after),
            "new_publish_R1": len(unrelated_residual_after_qids - unrelated_residual_before_qids),
        },
    )
    write_handoffs(paths.artifact_root)

    raw_after = file_state(paths.rawcandle_db)
    downstream_writes = {
        "ttm": int(before["row_counts"]["v3_ttm"] != after["row_counts"]["v3_ttm"] or before["derived_fingerprints"]["v3_ttm"] != after["derived_fingerprints"]["v3_ttm"]),
        "score": int(before["row_counts"]["v3_score"] != after["row_counts"]["v3_score"] or before["derived_fingerprints"]["v3_score"] != after["derived_fingerprints"]["v3_score"]),
        "lifecycle": int(before["row_counts"]["v3_lifecycle"] != after["row_counts"]["v3_lifecycle"] or before["derived_fingerprints"]["v3_lifecycle"] != after["derived_fingerprints"]["v3_lifecycle"]),
        "valuation": int(before["row_counts"]["v3_valuation"] != after["row_counts"]["v3_valuation"] or before["derived_fingerprints"]["v3_valuation"] != after["derived_fingerprints"]["v3_valuation"]),
    }
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "artifact_root": str(paths.artifact_root),
        "verified_input": {"path": str(source_csv), **manifest},
        "guards": {"rows": len(guards), "passed": sum(1 for row in guards if row["old_value_guard"] == "PASS"), "stale_failures": 0, "structural_blocks": 0},
        "chronology": {"rows": len(chronology), "passed": sum(1 for row in chronology if row["status"] == "PASS"), "brtx_duplicate_publish_date_exception_accepted": True},
        "apply": {"transaction_status": "COMMITTED", "rows_updated": sum(int(row["rows_updated"]) for row in apply_audit), "changed_publish_cells": lineage_parity["changed_publish_cells"], "write_failures": 0},
        "integrity": {
            "quick_check_before": before["quick_check"],
            "quick_check_after": after["quick_check"],
            "counts_before": before["row_counts"],
            "counts_after": after["row_counts"],
            "duplicate_fy_fq_after": after["integrity"]["duplicate_fy_fq"],
            "orphans_after": after["integrity"]["orphan_fundamentals"] + after["integrity"]["orphan_migration_audit"],
            "unrelated_canonical_drift": drift["unrelated_canonical_drift"],
            "fundamental_fields_changed": fundamental_parity["fundamental_fields_changed"],
            "lineage_rows_changed": lineage_parity["lineage_rows_changed"],
        },
        "residual": residual_summary
        | {
            "retained_publish_R1": retained_r1,
            "retained_publish_R2": retained_r2,
            "retained_publish_R3": retained_r3,
            "raw_publish_audit_flags": len(residual_after),
        },
        "special_cases": {"remaining": sorted(STRUCTURAL_R1_REMAINING), "IMMR_changed": False, "RCAT_changed": False},
        "downstream_writes": downstream_writes,
        "derived_state": DERIVED_STALE,
        "rawcandle": {"before": raw_before, "after": raw_after, "writes": int(raw_before != raw_after)},
        "backup": backup,
        "next_action": "USER EXTERNAL RESEARCH - IMMR / RCAT; then PHASE 8A10A-SPECIAL-FINAL-APPLY; then PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT",
    }
    write_json(paths.artifact_root / "phase8a10a_publish_apply_summary.json", summary)
    if (
        after["quick_check"] != "ok"
        or before["row_counts"] != after["row_counts"]
        or any(row["status"] != "PASS" for row in parity)
        or fundamental_parity["fundamental_fields_changed"]
        or lineage_parity["lineage_rows_changed"]
        or drift["unrelated_canonical_drift"]
        or retained_r1 != 0
        or len(unrelated_residual_after_qids - unrelated_residual_before_qids) != 0
        or lineage_parity["changed_publish_cells"] != EXPECTED_ROWS
        or any(downstream_writes.values())
        or raw_before != raw_after
    ):
        raise RuntimeError("post-apply publish_date integrity guard failed")
    return summary
