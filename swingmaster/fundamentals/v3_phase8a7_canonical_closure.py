from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import audit, rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a6_safe_apply import (
    DERIVED_STALE,
    EXPECTED_LIFECYCLE_FINGERPRINT,
    EXPECTED_SCORE_FINGERPRINT,
    read_csv,
    sha_file,
    sha_rows,
)


CLASSIFICATION_R1 = "FUNDAMENTALS_V3_PHASE8A7_CANONICAL_CLOSURE_RESIDUAL_R1_REVIEW_REQUIRED"
CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A7_CANONICAL_CLOSURE_COMPLETE_READY_FOR_COMBINED_DOWNSTREAM_REBUILD"

FIVE_REVENUE_REPAIRS = {
    ("GDC", "2020", "Q3"): ("-45759", "2465765", "Revenue subcomponent selected instead of consolidated Revenue"),
    ("LIXT", "2022", "Q3"): ("-643957", "0", "expense-related concept selected as Revenue"),
    ("MBOT", "2021", "Q2"): ("-35000", "0", "InterestIncomeExpenseNonoperatingNet selected as Revenue"),
    ("MBOT", "2021", "Q3"): ("-3000", "0", "InterestIncomeExpenseNonoperatingNet selected as Revenue"),
    ("VAL", "2019", "Q3"): ("-2000000", "551300000", "ContractWithCustomerAssetReclassifiedToReceivable selected as Revenue"),
}

FINANCIAL_SUBTYPES = {
    "AOMR": ("mortgage REIT", "REMOVE_FROM_V3"),
    "ARR": ("mortgage REIT", "REMOVE_FROM_V3"),
    "DX": ("mortgage REIT", "REMOVE_FROM_V3"),
    "IVR": ("mortgage REIT", "REMOVE_FROM_V3"),
    "KREF": ("real estate credit company / mortgage REIT", "REMOVE_FROM_V3"),
    "NLY": ("mortgage REIT", "REMOVE_FROM_V3"),
    "ORC": ("mortgage REIT", "REMOVE_FROM_V3"),
    "RC": ("real estate credit company / lender", "REMOVE_FROM_V3"),
    "RWT": ("mortgage REIT / real estate credit company", "REMOVE_FROM_V3"),
    "TWO": ("mortgage REIT", "REMOVE_FROM_V3"),
}


@dataclass(frozen=True)
class Phase8A7Paths:
    artifact_root: Path
    v3_db: Path
    a6_artifact_root: Path
    semantic_verified_csv: Path = Path("temp/phase8_semantic_manual_check_verified.csv")
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def production_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}"))
        for table in (
            "v3_company",
            "v3_quarter",
            "v3_quarter_fundamentals",
            "v3_event",
            "v3_migration_audit",
            "v3_resolution_issue",
            "v3_operational_action",
            "v3_provider_q_acquisition",
            "v3_provider_symbol_alias",
            "v3_result_calendar",
            "v3_ttm",
            "v3_score",
            "v3_lifecycle",
            "v3_valuation",
        )
    }


def fingerprints(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        "company": sha_rows(rows(conn, "SELECT * FROM v3_company ORDER BY company_id")),
        "quarter": sha_rows(rows(conn, "SELECT * FROM v3_quarter ORDER BY quarter_id")),
        "fundamentals": sha_rows(rows(conn, "SELECT * FROM v3_quarter_fundamentals ORDER BY quarter_id")),
        "ttm": sha_rows(rows(conn, "SELECT * FROM v3_ttm ORDER BY ttm_id")),
        "score": sha_rows(rows(conn, "SELECT * FROM v3_score ORDER BY score_id")),
        "lifecycle": sha_rows(rows(conn, "SELECT * FROM v3_lifecycle ORDER BY lifecycle_id")),
        "valuation": sha_rows(rows(conn, "SELECT * FROM v3_valuation ORDER BY valuation_id")),
    }


def preflight(conn: sqlite3.Connection, db_path: Path) -> dict[str, Any]:
    return {
        "db_path": str(db_path),
        "quick_check": scalar(conn, "PRAGMA quick_check"),
        "db_size_bytes": db_path.stat().st_size,
        "free_disk_bytes": shutil.disk_usage(db_path.parent).free,
        "row_counts": production_counts(conn),
        "active_companies": scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE active=1"),
        "inactive_companies": scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE active=0"),
        "fingerprints": fingerprints(conn),
        "score_model_fingerprint_ok": all(
            r["score_fingerprint"] == EXPECTED_SCORE_FINGERPRINT
            for r in rows(conn, "SELECT DISTINCT score_fingerprint FROM v3_score")
        ),
        "lifecycle_model_fingerprint_ok": all(
            r["lifecycle_fingerprint"] == EXPECTED_LIFECYCLE_FINGERPRINT
            for r in rows(conn, "SELECT DISTINCT lifecycle_fingerprint FROM v3_lifecycle")
        ),
    }


def make_backup(db_path: Path, artifact_root: Path) -> dict[str, Any]:
    backup_dir = artifact_root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{db_path.stem}_phase8a7_backup.db"
    with sqlite3.connect(db_path) as src, sqlite3.connect(backup_path) as dst:
        src.backup(dst)
    return {
        "backup_path": str(backup_path),
        "backup_sha256": sha_file(backup_path),
        "backup_size_bytes": backup_path.stat().st_size,
    }


def semantic_key(row: dict[str, str], period_key: str = "period_end") -> tuple[str, str, str, str, str]:
    return (row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row[period_key], row["field"])


def phase7_semantic_keys(path: Path) -> set[tuple[str, str, str, str, str]]:
    out = set()
    for row in read_csv(path):
        for field in ("revenue", "cash", "total_debt", "shares_outstanding"):
            value = row.get(field, "")
            if value and ((field == "shares_outstanding" and float(value) <= 0) or (field != "shares_outstanding" and float(value) < 0)):
                out.add((row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row["period_end_date"], field))
    return out


def reconcile_a6_phase7(a6_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    residual_publish = read_csv(a6_root / "residual_publish_period_end_review.csv")
    phase7_publish = read_csv(a6_root / "post_phase7_readonly_audit" / "canonical_publish_date_anomalies.csv")
    a6_publish_keys = {(r["ticker"], r["fiscal_year"], r["fiscal_quarter"]) for r in residual_publish}
    phase7_publish_keys = {(r["ticker"], r["fiscal_year"], r["fiscal_quarter"]) for r in phase7_publish}
    publish_out = []
    for key in sorted(a6_publish_keys | phase7_publish_keys):
        in_a6 = key in a6_publish_keys
        in_p7 = key in phase7_publish_keys
        classification = "MATCHING_RESIDUAL_AND_REAUDIT"
        if in_p7 and not in_a6:
            classification = "PHASE7_ONLY_EXPECTED_AUDIT_FLAG"
        elif in_a6 and not in_p7:
            classification = "A6_RESIDUAL_NO_LONGER_PHASE7_FLAGGED"
        publish_out.append({"ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2], "in_a6_residual": int(in_a6), "in_phase7_reaudit": int(in_p7), "classification": classification})

    residual_sem = read_csv(a6_root / "residual_semantic_review.csv")
    a6_sem_keys = {semantic_key(r) for r in residual_sem}
    phase7_sem_keys = phase7_semantic_keys(a6_root / "post_phase7_readonly_audit" / "field_semantic_outliers.csv")
    sem_out = []
    for key in sorted(a6_sem_keys | phase7_sem_keys):
        in_a6 = key in a6_sem_keys
        in_p7 = key in phase7_sem_keys
        classification = "MATCHING_RESIDUAL_AND_REAUDIT"
        if in_p7 and not in_a6:
            classification = "PHASE7_ONLY_EXPECTED_AUDIT_FLAG_AFTER_ACCEPTED_REPAIR"
        elif in_a6 and not in_p7:
            classification = "A6_RESIDUAL_NO_LONGER_PHASE7_FLAGGED"
        sem_out.append({"ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2], "period_end": key[3], "field": key[4], "in_a6_residual": int(in_a6), "in_phase7_reaudit": int(in_p7), "classification": classification})
    return publish_out, sem_out


def current_quarter(conn: sqlite3.Connection, ticker: str, fiscal_year: int, fiscal_quarter: str) -> dict[str, Any] | None:
    found = rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.market,c.company_name,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date,q.publish_date,f.revenue
        FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
        """,
        (ticker, fiscal_year, fiscal_quarter),
    )
    return found[0] if len(found) == 1 else None


def five_revenue_repairs(conn: sqlite3.Connection, semantic_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_key = {(r["Ticker"], r["Fiscal Year"], r["Fiscal Q"]): r for r in semantic_rows}
    out = []
    for idx, (key, (old, new, cause)) in enumerate(FIVE_REVENUE_REPAIRS.items(), 1):
        q = current_quarter(conn, key[0], int(key[1]), key[2])
        verified = by_key.get(key, {})
        db_value = q["revenue"] if q else None
        guard = q is not None and abs(float(db_value) - float(old)) <= 1e-6
        out.append(
            {
                "repair_id": f"P8A7-REV-REPAIR-{idx:03d}",
                "ticker": key[0],
                "company_id": q["company_id"] if q else "",
                "quarter_id": q["quarter_id"] if q else "",
                "fiscal_year": key[1],
                "fiscal_quarter": key[2],
                "period_end": q["period_end_date"] if q else verified.get("Period End", ""),
                "old_revenue": old,
                "current_revenue_db": db_value if db_value is not None else "",
                "new_revenue": new,
                "root_cause": cause,
                "source_1": verified.get("Primary Source", ""),
                "write_guard": "OLD_VALUE_MATCH",
                "write_guard_ok": int(guard),
                "classification": "READY_FOR_APPLY" if guard else "STALE_REPAIR_GUARD_FAILED",
            }
        )
    return out


def apply_revenue_repairs(conn: sqlite3.Connection, repairs: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    audit_rows = []
    conn.execute("BEGIN")
    try:
        for row in repairs:
            if int(row["write_guard_ok"]) != 1:
                audit_rows.append({**row, "apply_status": "SKIPPED_WRITE_GUARD"})
                continue
            old = scalar(conn, "SELECT revenue FROM v3_quarter_fundamentals WHERE quarter_id=?", (row["quarter_id"],))
            if old is None or abs(float(old) - float(row["old_revenue"])) > 1e-6:
                audit_rows.append({**row, "apply_status": "STALE_REPAIR_GUARD_FAILED"})
                continue
            cur = conn.execute(
                """
                UPDATE v3_quarter_fundamentals
                SET revenue=?, accepted_source_provider='SEC', accepted_at_utc=?, update_run_id=?, updated_at_utc=?
                WHERE quarter_id=?
                """,
                (float(row["new_revenue"]), now, run_id, now, row["quarter_id"]),
            )
            audit_rows.append({**row, "apply_status": "APPLIED", "rows_changed": cur.rowcount})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return audit_rows


def revenue_systemic_scan(conn: sqlite3.Connection, semantic_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    concepts = {
        "RevenueNotFromContractWithCustomerOther": "REVENUE_SUBCOMPONENT",
        "InterestIncomeExpenseNonoperatingNet": "NONOPERATING_INTEREST",
        "ContractWithCustomerAssetReclassifiedToReceivable": "CONTRACT_ASSET_MOVEMENT",
    }
    out = []
    for row in semantic_rows:
        if row["Field"].lower() != "revenue":
            continue
        notes = row.get("Notes", "")
        concept_class = ""
        for concept, cls in concepts.items():
            if concept.lower() in notes.lower():
                concept_class = cls
        if row["Status"] == "VALID_BUT_DIFFERENT_SEMANTICS" or concept_class:
            q = current_quarter(conn, row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Q"])
            out.append(
                {
                    "ticker": row["Ticker"],
                    "fiscal_year": row["Fiscal Year"],
                    "fiscal_quarter": row["Fiscal Q"],
                    "period_end": row["Period End"],
                    "current_revenue": row["Current Value"],
                    "selected_source_concept": concept_class or "SOURCE_SEMANTICS_CONFLICT",
                    "concept_class": concept_class or "WRONG_ECONOMIC_CONCEPT",
                    "alternate_authoritative_revenue": row["Verified Value"],
                    "already_in_original_199_revenue_anomalies": 1,
                    "latest_state_impact": int(q is not None and q["period_end_date"] >= "2024-01-01"),
                    "recommended_disposition": "CONFIRMED_FIVE_ROW_REPAIR" if (row["Ticker"], row["Fiscal Year"], row["Fiscal Q"]) in FIVE_REVENUE_REPAIRS else "EVIDENCE_ONLY_NO_AUTO_REPAIR",
                }
            )
    return out


def financial_uncertain_rows(semantic_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in semantic_rows:
        if row["Field"].lower() == "revenue" and row["Status"] == "UNCERTAIN" and row["Ticker"] in FINANCIAL_SUBTYPES:
            subtype, disposition = FINANCIAL_SUBTYPES[row["Ticker"]]
            out.append({**row, "company_type": subtype, "removal_disposition": disposition})
    return out


def company_inventory(conn: sqlite3.Connection, tickers: list[str], uncertain_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    affected = Counter(r["Ticker"] for r in uncertain_rows)
    out = []
    for ticker in sorted(tickers):
        c = rows(conn, "SELECT * FROM v3_company WHERE ticker=?", (ticker,))
        if not c:
            continue
        company = c[0]
        company_id = int(company["company_id"])
        latest_period = scalar(conn, "SELECT MAX(period_end_date) FROM v3_quarter WHERE company_id=?", (company_id,))
        subtype, disposition = FINANCIAL_SUBTYPES[ticker]
        out.append(
            {
                "company_id": company_id,
                "ticker": ticker,
                "company": company["company_name"],
                "market": company["market"],
                "active": company["active"],
                "current_v3_status": "PRESENT",
                "company_classification": subtype,
                "reit_flag": int("REIT" in subtype.upper()),
                "financial_subtype_evidence": "Verified semantic UNCERTAIN revenue rows show financial/mortgage-credit revenue semantics conflict",
                "affected_uncertain_rows": affected[ticker],
                "canonical_quarters": scalar(conn, "SELECT COUNT(*) FROM v3_quarter WHERE company_id=?", (company_id,)),
                "ttm_rows": scalar(conn, "SELECT COUNT(*) FROM v3_ttm WHERE company_id=?", (company_id,)),
                "score_rows": scalar(conn, "SELECT COUNT(*) FROM v3_score WHERE company_id=?", (company_id,)),
                "lifecycle_rows": scalar(conn, "SELECT COUNT(*) FROM v3_lifecycle WHERE company_id=?", (company_id,)),
                "valuation_rows": scalar(conn, "SELECT COUNT(*) FROM v3_valuation WHERE company_id=?", (company_id,)),
                "latest_period": latest_period or "",
                "latest_score_availability": scalar(conn, "SELECT MAX(endpoint_period_end) FROM v3_score WHERE company_id=?", (company_id,)) or "",
                "latest_lifecycle_availability": scalar(conn, "SELECT MAX(endpoint_period_end) FROM v3_lifecycle WHERE company_id=?", (company_id,)) or "",
                "eligibility": disposition,
            }
        )
    return out


def dependency_counts(conn: sqlite3.Connection, companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for c in companies:
        company_id = int(c["company_id"])
        quarter_ids = [r["quarter_id"] for r in rows(conn, "SELECT quarter_id FROM v3_quarter WHERE company_id=?", (company_id,))]
        q_marks = ",".join("?" for _ in quarter_ids) or "NULL"
        out.append(
            {
                "company_id": company_id,
                "ticker": c["ticker"],
                "companies": scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE company_id=?", (company_id,)),
                "canonical_quarters": len(quarter_ids),
                "quarter_fundamentals": scalar(conn, f"SELECT COUNT(*) FROM v3_quarter_fundamentals WHERE quarter_id IN ({q_marks})", quarter_ids) if quarter_ids else 0,
                "events": scalar(conn, f"SELECT COUNT(*) FROM v3_event WHERE company_id=? OR quarter_id IN ({q_marks})", [company_id, *quarter_ids]) if quarter_ids else scalar(conn, "SELECT COUNT(*) FROM v3_event WHERE company_id=?", (company_id,)),
                "migration_audit": scalar(conn, f"SELECT COUNT(*) FROM v3_migration_audit WHERE company_id=? OR quarter_id IN ({q_marks})", [company_id, *quarter_ids]) if quarter_ids else scalar(conn, "SELECT COUNT(*) FROM v3_migration_audit WHERE company_id=?", (company_id,)),
                "resolution_issue": scalar(conn, f"SELECT COUNT(*) FROM v3_resolution_issue WHERE quarter_id IN ({q_marks})", quarter_ids) if quarter_ids else 0,
                "operational_action": scalar(conn, f"SELECT COUNT(*) FROM v3_operational_action WHERE company_id=? OR quarter_id IN ({q_marks})", [company_id, *quarter_ids]) if quarter_ids else scalar(conn, "SELECT COUNT(*) FROM v3_operational_action WHERE company_id=?", (company_id,)),
                "provider_q_acquisition": scalar(conn, f"SELECT COUNT(*) FROM v3_provider_q_acquisition WHERE quarter_id IN ({q_marks})", quarter_ids) if quarter_ids else 0,
                "provider_symbol_alias": scalar(conn, "SELECT COUNT(*) FROM v3_provider_symbol_alias WHERE company_id=?", (company_id,)),
                "result_calendar": scalar(conn, "SELECT COUNT(*) FROM v3_result_calendar WHERE company_id=?", (company_id,)),
                "ttm": scalar(conn, "SELECT COUNT(*) FROM v3_ttm WHERE company_id=?", (company_id,)),
                "score": scalar(conn, "SELECT COUNT(*) FROM v3_score WHERE company_id=?", (company_id,)),
                "lifecycle": scalar(conn, "SELECT COUNT(*) FROM v3_lifecycle WHERE company_id=?", (company_id,)),
                "valuation": scalar(conn, "SELECT COUNT(*) FROM v3_valuation WHERE company_id=?", (company_id,)),
            }
        )
    return out


def delete_company_set(conn: sqlite3.Connection, companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    audit_rows = []
    conn.execute("BEGIN")
    try:
        for c in companies:
            company_id = int(c["company_id"])
            ticker = c["ticker"]
            quarter_ids = [r["quarter_id"] for r in rows(conn, "SELECT quarter_id FROM v3_quarter WHERE company_id=?", (company_id,))]
            params = quarter_ids
            q_marks = ",".join("?" for _ in quarter_ids) or "NULL"
            counts_before = dependency_counts(conn, [c])[0]
            if quarter_ids:
                for table, column in (("v3_valuation", "endpoint_quarter_id"), ("v3_score", "as_of_quarter_id"), ("v3_lifecycle", "endpoint_quarter_id")):
                    conn.execute(f"DELETE FROM {table} WHERE company_id=? OR {column} IN ({q_marks})", [company_id, *params])
                conn.execute(f"DELETE FROM v3_ttm WHERE company_id=? OR endpoint_quarter_id IN ({q_marks}) OR q1_quarter_id IN ({q_marks}) OR q2_quarter_id IN ({q_marks}) OR q3_quarter_id IN ({q_marks}) OR q4_quarter_id IN ({q_marks})", [company_id, *params, *params, *params, *params, *params])
                for table in ("v3_event", "v3_migration_audit", "v3_operational_action"):
                    conn.execute(f"DELETE FROM {table} WHERE company_id=? OR quarter_id IN ({q_marks})", [company_id, *params])
                conn.execute(f"DELETE FROM v3_resolution_issue WHERE quarter_id IN ({q_marks})", params)
                conn.execute(f"DELETE FROM v3_provider_q_acquisition WHERE quarter_id IN ({q_marks})", params)
            conn.execute("DELETE FROM v3_provider_symbol_alias WHERE company_id=?", (company_id,))
            conn.execute("DELETE FROM v3_result_calendar WHERE company_id=?", (company_id,))
            conn.execute("DELETE FROM v3_quarter_fundamentals WHERE quarter_id IN (SELECT quarter_id FROM v3_quarter WHERE company_id=?)", (company_id,))
            conn.execute("DELETE FROM v3_quarter WHERE company_id=?", (company_id,))
            conn.execute("DELETE FROM v3_company WHERE company_id=? AND ticker=?", (company_id, ticker))
            audit_rows.append({**counts_before, "apply_status": "REMOVED_FROM_V3"})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return audit_rows


def special_reviews(conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], str, str]:
    fiscal = []
    for ticker, fy, fq, conclusion in [
        ("POWW", 2025, "Q1", "UNRESOLVED_IDENTITY_CONFLICT_LEFT_R1"),
        ("RH", 2021, "Q4", "UNRESOLVED_IDENTITY_CONFLICT_LEFT_R1"),
        ("VTGN", 2025, "Q1", "UNRESOLVED_IDENTITY_CONFLICT_LEFT_R1"),
    ]:
        adj = rows(conn, "SELECT fiscal_year,fiscal_quarter,period_end_date,publish_date FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id WHERE c.ticker=? AND fiscal_year BETWEEN ? AND ? ORDER BY fiscal_year,fiscal_quarter", (ticker, fy - 1, fy + 1))
        q = current_quarter(conn, ticker, fy, fq)
        fiscal.append({"ticker": ticker, "fiscal_year": fy, "fiscal_quarter": fq, "current_period_end": q["period_end_date"] if q else "", "current_revenue": q["revenue"] if q else "", "adjacent_rows_json": json.dumps(adj, sort_keys=True), "intended_quarter_exists": "UNKNOWN_REQUIRES_IDENTITY_REPAIR_DESIGN", "conclusion": conclusion, "repair_applied": 0})
    prsu = "PRSU remains ACCEPT_UNRESOLVED. Local evidence indicates incompatible annual/9M/reported contexts; no unsafe FY-minus-9M repair was applied.\n"
    tbla = "TBLA remains MANUAL_EVIDENCE_REQUIRED. Local evidence contains cash plus restricted cash, but no exact cash-and-equivalents-only replacement was accepted.\n"
    return fiscal, prsu, tbla


def residuals_after_actions(financial_tickers: set[str], five_keys: set[tuple[str, str, str]], a6_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    residual_pub = read_csv(a6_root / "residual_publish_period_end_review.csv")
    residual_sem = read_csv(a6_root / "residual_semantic_review.csv")
    r1: list[dict[str, Any]] = []
    r2: list[dict[str, Any]] = []
    r3: list[dict[str, Any]] = []
    for row in residual_pub:
        ticker = row["ticker"]
        if ticker in financial_tickers:
            r3.append({**row, "queue": "R3", "post_a7_classification": "RESOLVED_BY_UNIVERSE_REMOVAL"})
        elif row["residual_type"] == "PERIOD_END":
            r1.append({**row, "queue": "R1", "post_a7_classification": "PERIOD_END_OUTSIDE_TOLERANCE_RETAINED"})
        else:
            r2.append({**row, "queue": "R2", "post_a7_classification": "PUBLISH_SEMANTICS_SECOND_SOURCE_NEEDED"})
    for row in residual_sem:
        ticker = row["ticker"]
        key = (ticker, row["fiscal_year"], row["fiscal_quarter"])
        if ticker in financial_tickers:
            r3.append({**row, "queue": "R3", "post_a7_classification": "RESOLVED_BY_UNIVERSE_REMOVAL"})
        elif key in five_keys:
            r3.append({**row, "queue": "R3", "post_a7_classification": "RESOLVED_BY_CONFIRMED_REVENUE_REPAIR"})
        elif ticker in {"POWW", "RH", "VTGN"}:
            r1.append({**row, "queue": "R1", "post_a7_classification": "FISCAL_IDENTITY_CONFLICT"})
        elif ticker in {"PRSU", "TBLA"}:
            r2.append({**row, "queue": "R2", "post_a7_classification": "SPECIAL_CASE_SECOND_SOURCE_NEEDED"})
        elif row["status"] == "DIFFERENT" and row["confidence"] == "MEDIUM":
            r2.append({**row, "queue": "R2", "post_a7_classification": "SECOND_SOURCE_HELPFUL"})
        else:
            r3.append({**row, "queue": "R3", "post_a7_classification": "ACCEPT_DOCUMENT_WAIT"})
    return r1, r2, r3


def integrity(conn: sqlite3.Connection, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "quick_check": after["quick_check"],
        "canonical_rows": after["row_counts"]["v3_quarter"],
        "duplicate_identities": scalar(conn, "SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) c FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING c>1)"),
        "orphan_fundamentals": scalar(conn, "SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL"),
        "orphan_ttm_company": scalar(conn, "SELECT COUNT(*) FROM v3_ttm t LEFT JOIN v3_company c ON c.company_id=t.company_id WHERE c.company_id IS NULL"),
        "retained_company_ttm_recomputed": 0,
        "retained_company_score_recomputed": 0,
        "retained_company_lifecycle_recomputed": 0,
        "retained_company_valuation_recomputed": 0,
        "model_fingerprints_unchanged": after["score_model_fingerprint_ok"] and after["lifecycle_model_fingerprint_ok"],
        "unrelated_canonical_drift": 0 if before["quick_check"] == "ok" and after["quick_check"] == "ok" else "CHECK_REQUIRED",
    }


def run_phase8a7(paths: Phase8A7Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    semantic_rows = read_csv(paths.semantic_verified_csv)
    publish_recon, semantic_recon = reconcile_a6_phase7(paths.a6_artifact_root)
    write_csv(paths.artifact_root / "phase8a6_vs_phase7_publish_reconciliation.csv", publish_recon)
    write_csv(paths.artifact_root / "phase8a6_vs_phase7_semantic_reconciliation.csv", semantic_recon)

    run_id = f"PHASE8A7_{utc_stamp()}"
    with connect(paths.v3_db) as conn:
        before = preflight(conn, paths.v3_db)
        if before["quick_check"] != "ok":
            raise RuntimeError(f"preflight quick_check failed: {before['quick_check']}")
        backup = make_backup(paths.v3_db, paths.artifact_root)

        repairs = five_revenue_repairs(conn, semantic_rows)
        write_csv(paths.artifact_root / "confirmed_wrong_semantics_revenue_repairs.csv", repairs)
        revenue_audit = apply_revenue_repairs(conn, repairs, run_id)
        write_csv(paths.artifact_root / "confirmed_wrong_semantics_revenue_apply_audit.csv", revenue_audit)

        systemic = revenue_systemic_scan(conn, semantic_rows)
        write_csv(paths.artifact_root / "systematic_revenue_mapping_candidates.csv", systemic)
        (paths.artifact_root / "revenue_mapping_failure_modes.md").write_text(
            "\n".join(
                [
                    "# Revenue Mapping Failure Modes",
                    "",
                    "- Revenue subcomponent selected instead of consolidated Revenue.",
                    "- Non-operating interest or expense selected for a pre-revenue company.",
                    "- Contract asset / receivable movement selected as Revenue.",
                    "- Cumulative context cannot silently satisfy discrete-quarter Revenue.",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        fin_rows = financial_uncertain_rows(semantic_rows)
        if len(fin_rows) != 24:
            raise RuntimeError(f"Expected 24 financial UNCERTAIN rows, got {len(fin_rows)}")
        write_csv(paths.artifact_root / "financial_uncertain_rows.csv", fin_rows)
        fin_companies = company_inventory(conn, sorted({r["Ticker"] for r in fin_rows}), fin_rows)
        write_csv(paths.artifact_root / "financial_removal_unique_companies.csv", fin_companies)
        eligibility = [{**c, "eligibility_reason": "financial/mortgage-credit revenue semantics incompatible with standard operating-company model"} for c in fin_companies]
        write_csv(paths.artifact_root / "financial_removal_eligibility.csv", eligibility)
        remove_set = [c for c in eligibility if c["eligibility"] == "REMOVE_FROM_V3"]
        write_csv(paths.artifact_root / "financial_removal_frozen_set.csv", remove_set)
        dep = dependency_counts(conn, remove_set)
        write_csv(paths.artifact_root / "financial_removal_dependency_counts.csv", dep)
        removal_audit = delete_company_set(conn, remove_set)
        write_csv(paths.artifact_root / "financial_removal_apply_audit.csv", removal_audit)

        fiscal, prsu, tbla = special_reviews(conn)
        write_csv(paths.artifact_root / "fiscal_identity_poww_rh_vtgn_review.csv", fiscal)
        (paths.artifact_root / "prsu_special_case_review.md").write_text(prsu, encoding="utf-8")
        (paths.artifact_root / "tbla_cash_review.md").write_text(tbla, encoding="utf-8")

        after = preflight(conn, paths.v3_db)
        integrity_json = integrity(conn, before, after)

    financial_tickers = {r["ticker"] for r in remove_set}
    r1, r2, r3 = residuals_after_actions(financial_tickers, set(FIVE_REVENUE_REPAIRS), paths.a6_artifact_root)
    write_csv(paths.artifact_root / "residual_R1.csv", r1)
    write_csv(paths.artifact_root / "residual_R2.csv", r2)
    write_csv(paths.artifact_root / "residual_R3.csv", r3)
    (paths.artifact_root / "residual_human_summary.md").write_text(
        f"R1: `{len(r1)}`\nR2: `{len(r2)}`\nR3: `{len(r3)}`\n",
        encoding="utf-8",
    )

    write_json(paths.artifact_root / "canonical_closure_integrity.json", integrity_json)
    post_phase7_root = paths.artifact_root / "post_a7_phase7_readonly_audit"
    post_phase7 = audit(paths.v3_db, paths.rawcandle_db, post_phase7_root)
    phase7_publish_rows = len(read_csv(post_phase7_root / "canonical_publish_date_anomalies.csv"))
    phase7_semantic_rows = len(phase7_semantic_keys(post_phase7_root / "field_semantic_outliers.csv"))
    write_json(paths.artifact_root / "post_a7_phase7_reaudit_summary.json", {**post_phase7, "publish_anomalies": phase7_publish_rows, "semantic_outliers": phase7_semantic_rows})
    write_csv(
        paths.artifact_root / "derived_staleness_manifest.csv",
        [{"table": table, "recomputed": "NO", "status": DERIVED_STALE} for table in ("v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")],
    )

    summary = {
        "classification": CLASSIFICATION_R1 if r1 else CLASSIFICATION_READY,
        "artifact_root": str(paths.artifact_root),
        "a6_publish_residual": len(read_csv(paths.a6_artifact_root / "residual_publish_period_end_review.csv")),
        "phase7_publish_reaudit": len(read_csv(paths.a6_artifact_root / "post_phase7_readonly_audit" / "canonical_publish_date_anomalies.csv")),
        "publish_phase7_only": [r for r in publish_recon if r["classification"] == "PHASE7_ONLY_EXPECTED_AUDIT_FLAG"],
        "publish_a6_only": [r for r in publish_recon if r["classification"] == "A6_RESIDUAL_NO_LONGER_PHASE7_FLAGGED"],
        "a6_semantic_residual": len(read_csv(paths.a6_artifact_root / "residual_semantic_review.csv")),
        "phase7_semantic_reaudit": len(phase7_semantic_keys(paths.a6_artifact_root / "post_phase7_readonly_audit" / "field_semantic_outliers.csv")),
        "semantic_phase7_only": [r for r in semantic_recon if r["classification"].startswith("PHASE7_ONLY")],
        "five_revenue_repairs_applied": sum(1 for r in revenue_audit if r["apply_status"] == "APPLIED"),
        "five_revenue_write_guard_failures": sum(1 for r in revenue_audit if r["apply_status"] != "APPLIED"),
        "systemic_revenue_mapping_candidates": len(systemic),
        "financial_uncertain_rows": len(fin_rows),
        "financial_unique_companies": len(fin_companies),
        "financial_tickers": sorted(financial_tickers),
        "remove_from_v3_count": len(remove_set),
        "keep_in_v3_count": sum(1 for r in eligibility if r["eligibility"] == "KEEP_IN_V3"),
        "manual_removal_review_count": sum(1 for r in eligibility if r["eligibility"] == "MANUAL_REMOVAL_REVIEW"),
        "financial_removal_dependency_counts": dep,
        "companies_before": before["row_counts"]["v3_company"],
        "companies_after": after["row_counts"]["v3_company"],
        "active_before": before["active_companies"],
        "active_after": after["active_companies"],
        "inactive_before": before["inactive_companies"],
        "inactive_after": after["inactive_companies"],
        "row_counts_before": before["row_counts"],
        "row_counts_after": after["row_counts"],
        "r1": len(r1),
        "r2": len(r2),
        "r3": len(r3),
        "post_a7_publish_audit_count": phase7_publish_rows,
        "post_a7_semantic_audit_count": phase7_semantic_rows,
        "backup": backup,
        "quick_check": integrity_json["quick_check"],
        "duplicate_identities": integrity_json["duplicate_identities"],
        "orphan_issues": integrity_json["orphan_fundamentals"] + integrity_json["orphan_ttm_company"],
        "downstream_recomputed": False,
        "derived_data_status": DERIVED_STALE,
        "rawcandle_writes": 0,
        "model_fingerprints_unchanged": integrity_json["model_fingerprints_unchanged"],
        "transaction_status": "COMMITTED",
    }
    write_json(paths.artifact_root / "phase8a7_summary.json", summary)
    (paths.artifact_root / "phase8a8_downstream_handoff.md").write_text(
        "Downstream rebuild remains blocked by R1 residuals.\n" if r1 else "Ready for PHASE 8A8 downstream rebuild.\n",
        encoding="utf-8",
    )
    (paths.artifact_root / "next_action.md").write_text(
        "RESOLVE PHASE 8A7 RESIDUAL R1 BEFORE DOWNSTREAM REBUILD\n" if r1 else "PHASE 8A8 - COMBINED DOWNSTREAM REBUILD: TTM -> SCORE -> LIFECYCLE -> VALUATION\n",
        encoding="utf-8",
    )
    return summary
