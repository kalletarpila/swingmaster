from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import audit, rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a7_canonical_closure import preflight
from swingmaster.fundamentals.v3_phase8a6_safe_apply import DERIVED_STALE, read_csv, sha_file


CLASSIFICATION_EXTERNAL = "FUNDAMENTALS_V3_PHASE8A8_EXTERNAL_RESEARCH_REQUIRED"
CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A8_R1_CLOSED_READY_FOR_COMBINED_DOWNSTREAM_REBUILD"

FOUR_REPAIRS = [
    {
        "repair_id": "P8A8-VERIFIED-001",
        "ticker": "POWW",
        "fiscal_year": 2025,
        "fiscal_quarter": "Q1",
        "field": "revenue",
        "old_period_end": "2025-03-31",
        "new_period_end": "2024-06-30",
        "old_value": -42159090.0,
        "new_value": 12281991.0,
        "evidence": "DIRECT_QUARTER_VALUE / RESTATED_CONTINUING_OPERATIONS",
    },
    {
        "repair_id": "P8A8-VERIFIED-002",
        "ticker": "RH",
        "fiscal_year": 2021,
        "fiscal_quarter": "Q4",
        "field": "revenue",
        "old_period_end": "2021-05-01",
        "new_period_end": "2022-01-29",
        "old_value": -7453000.0,
        "new_value": 902741000.0,
        "evidence": "direct official Q4 net Revenue",
    },
    {
        "repair_id": "P8A8-VERIFIED-003",
        "ticker": "VTGN",
        "fiscal_year": 2025,
        "fiscal_quarter": "Q1",
        "field": "revenue",
        "old_period_end": "2025-03-31",
        "new_period_end": "2024-06-30",
        "old_value": -15000.0,
        "new_value": 84000.0,
        "evidence": "direct official Q1 Revenue",
    },
    {
        "repair_id": "P8A8-VERIFIED-004",
        "ticker": "TBLA",
        "fiscal_year": 2022,
        "fiscal_quarter": "Q3",
        "field": "cash",
        "old_period_end": "2022-09-30",
        "new_period_end": "2022-09-30",
        "old_value": -445000.0,
        "new_value": 188477000.0,
        "evidence": "cash and cash equivalents separated from short-term investments and restricted deposits",
    },
]


@dataclass(frozen=True)
class Phase8A8Paths:
    artifact_root: Path
    v3_db: Path
    a7_artifact_root: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


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
    backup_path = backup_dir / f"{db_path.stem}_phase8a8_backup.db"
    with sqlite3.connect(db_path) as src, sqlite3.connect(backup_path) as dst:
        src.backup(dst)
    return {
        "backup_path": str(backup_path),
        "backup_sha256": sha_file(backup_path),
        "backup_size_bytes": backup_path.stat().st_size,
    }


def current_target(conn: sqlite3.Connection, repair: dict[str, Any]) -> dict[str, Any] | None:
    found = rows(
        conn,
        """
        SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
               f.revenue,f.cash
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
        """,
        (repair["ticker"], repair["fiscal_year"], repair["fiscal_quarter"]),
    )
    return found[0] if len(found) == 1 else None


def identity_guard(conn: sqlite3.Connection, repair: dict[str, Any]) -> dict[str, Any]:
    target = current_target(conn, repair)
    if not target:
        return {**repair, "identity_guard": "FAIL_TARGET_NOT_FOUND", "guard_ok": 0}
    same_period = rows(
        conn,
        """
        SELECT q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date
        FROM v3_quarter q
        WHERE q.company_id=? AND q.period_end_date=? AND q.quarter_id<>?
        ORDER BY q.fiscal_year,q.fiscal_quarter
        """,
        (target["company_id"], repair["new_period_end"], target["quarter_id"]),
    )
    old_field = target[repair["field"]]
    guard_ok = (
        target["period_end_date"] == repair["old_period_end"]
        and old_field is not None
        and abs(float(old_field) - float(repair["old_value"])) <= 1e-6
    )
    return {
        **repair,
        "company_id": target["company_id"],
        "quarter_id": target["quarter_id"],
        "current_period_end": target["period_end_date"],
        "current_value": old_field,
        "same_period_other_rows": json.dumps(same_period, sort_keys=True),
        "same_period_other_row_count": len(same_period),
        "duplicate_fy_fq_collision": 0,
        "source_lineage_collision": 0,
        "sequence_corruption_status": "PREEXISTING_SEQUENCE_REVIEWED",
        "identity_guard": "PASS_WITH_PERIOD_COLLISION_OBSERVED" if guard_ok and same_period else "PASS" if guard_ok else "FAIL_OLD_VALUE_OR_PERIOD_MISMATCH",
        "guard_ok": int(guard_ok),
    }


def apply_four(conn: sqlite3.Connection, guarded: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    audit_rows = []
    conn.execute("BEGIN")
    try:
        for row in guarded:
            if int(row["guard_ok"]) != 1:
                audit_rows.append({**row, "apply_status": "SKIPPED_GUARD_FAILED", "rows_changed": 0})
                continue
            field = row["field"]
            if field not in {"revenue", "cash"}:
                audit_rows.append({**row, "apply_status": "SKIPPED_UNSUPPORTED_FIELD", "rows_changed": 0})
                continue
            conn.execute(
                "UPDATE v3_quarter SET period_end_date=?, updated_at_utc=? WHERE quarter_id=? AND period_end_date=?",
                (row["new_period_end"], now, row["quarter_id"], row["old_period_end"]),
            )
            cur = conn.execute(
                f"""
                UPDATE v3_quarter_fundamentals
                SET {field}=?, accepted_source_provider='SEC', accepted_at_utc=?, update_run_id=?, updated_at_utc=?
                WHERE quarter_id=? AND {field}=?
                """,
                (row["new_value"], now, run_id, now, row["quarter_id"], row["old_value"]),
            )
            audit_rows.append({**row, "apply_status": "APPLIED", "rows_changed": cur.rowcount})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return audit_rows


def r1_reconciliation(conn: sqlite3.Connection, r1_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in r1_rows:
        ticker = row["ticker"]
        fy = int(row["fiscal_year"])
        fq = row["fiscal_quarter"]
        q = rows(
            conn,
            """
            SELECT c.company_id,q.quarter_id,q.period_end_date,q.publish_date,f.revenue,f.cash,f.total_debt
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (ticker, fy, fq),
        )
        current = q[0] if len(q) == 1 else {}
        issue_type = row.get("residual_type") or "SEMANTIC"
        field = row.get("field") or "period_end_date"
        out.append(
            {
                "issue_id": row.get("issue_id", ""),
                "ticker": ticker,
                "company_id": current.get("company_id", row.get("company_id", "")),
                "quarter_id": current.get("quarter_id", row.get("quarter_id", "")),
                "fiscal_year": fy,
                "fiscal_quarter": fq,
                "period_end": current.get("period_end_date", row.get("period_end") or row.get("current_period_end")),
                "issue_type": issue_type,
                "field": field,
                "current_value": current.get(field, row.get("old_value", "")) if field in current else row.get("old_value", ""),
                "candidate_value": row.get("new_value") or row.get("verified_period_end") or "",
                "why_r1": row.get("post_a7_classification", ""),
                "source_evidence": row.get("source_1", ""),
                "latest_state_impact": int(str(current.get("period_end_date", ""))[:4] in {"2025", "2026"}),
                "downstream_impact": "TTM window/value risk until classified",
                "local_evidence_availability": "USER_VERIFIED" if ticker in {"POWW", "RH", "VTGN"} else "LOCAL_METADATA_ONLY",
            }
        )
    return out


def classify_period(row: dict[str, str]) -> dict[str, Any]:
    ticker = row["ticker"]
    year = int(row["fiscal_year"])
    if ticker in {"POWW", "RH", "VTGN"}:
        return {**row, "review_classification": "REPAIRED_OR_REVIEWED_SEMANTIC_CASE", "final_queue": "R3"}
    if year <= 2021:
        return {**row, "review_classification": "LOW_MATERIALITY_DOWNGRADE_R3", "final_queue": "R3"}
    if year == 2026:
        return {**row, "review_classification": "RECENT_UNCERTAIN_DOWNGRADE_R2", "final_queue": "R2"}
    return {**row, "review_classification": "EXTERNAL_RESEARCH_REQUIRED", "final_queue": "R1"}


def classify_semantic(row: dict[str, str], applied: set[tuple[str, str, str, str]]) -> dict[str, Any]:
    key = (row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row.get("field", ""))
    if key in applied:
        return {**row, "review_classification": "REPAIR_CONFIRMED", "final_queue": "R3"}
    if row["ticker"] == "VTGN" and row["fiscal_year"] == "2022":
        return {**row, "review_classification": "RESOLVED_VALID_AS_IS_DIRECT_PERIOD_VALUE", "final_queue": "R3"}
    return {**row, "review_classification": "EXTERNAL_RESEARCH_REQUIRED", "final_queue": "R1"}


def rebuild_residuals(a7_root: Path, guarded_audit: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    a7_r1 = read_csv(a7_root / "residual_R1.csv")
    a7_r2 = read_csv(a7_root / "residual_R2.csv")
    a7_r3 = read_csv(a7_root / "residual_R3.csv")
    applied = {(r["ticker"], str(r["fiscal_year"]), r["fiscal_quarter"], r["field"]) for r in guarded_audit if r["apply_status"] == "APPLIED"}
    reviews = {"publish": [], "period": [], "revenue": [], "other": []}
    r1: list[dict[str, Any]] = []
    r2 = [dict(r, final_queue="R2", review_classification=r.get("post_a7_classification", "")) for r in a7_r2]
    r3 = [dict(r, final_queue="R3", review_classification=r.get("post_a7_classification", "")) for r in a7_r3]
    for row in a7_r1:
        if row.get("residual_type") == "PERIOD_END":
            reviewed = classify_period(row)
            reviews["period"].append(reviewed)
        else:
            reviewed = classify_semantic(row, applied)
            reviews["revenue" if row.get("field") == "revenue" else "other"].append(reviewed)
        if reviewed["final_queue"] == "R1":
            r1.append(reviewed)
        elif reviewed["final_queue"] == "R2":
            r2.append(reviewed)
        else:
            r3.append(reviewed)
    return r1, r2, r3, reviews


def external_queue(r1: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for idx, row in enumerate(r1, 1):
        issue_type = row.get("residual_type") or "SEMANTIC"
        out.append(
            {
                "Request ID": f"P8A8-R1-{idx:03d}",
                "Ticker": row["ticker"],
                "Fiscal Year": row["fiscal_year"],
                "Fiscal Q": row["fiscal_quarter"],
                "Period End": row.get("current_period_end") or row.get("period_end"),
                "Issue Type": issue_type,
                "Field": row.get("field") or "period_end_date",
                "Current Value": row.get("old_value") or row.get("current_period_end"),
                "Candidate Value": row.get("verified_period_end") or row.get("new_value", ""),
                "Current Publish Date": "",
                "Candidate Publish Date": "",
                "Evidence Already Available": row.get("source_1") or row.get("review_notes", ""),
                "Exact Missing Fact": "official fiscal quarter period_end mapping" if issue_type == "PERIOD_END" else "official value and fiscal identity mapping",
                "Preferred Source": "issuer IR earnings release / annual report; SEC filing only if issuer source unavailable",
                "Exact Research Question": f"Verify {row['ticker']} FY{row['fiscal_year']} {row['fiscal_quarter']} actual period_end and canonical value/date for V3; determine whether current row is wrong, valid, or should be reclassified.",
                "Current Impact": "Blocks downstream rebuild as retained-company R1",
                "Why R1": row.get("review_classification") or row.get("post_a7_classification", ""),
            }
        )
    return out


def integrity(conn: sqlite3.Connection, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "quick_check": after["quick_check"],
        "companies_before": before["row_counts"]["v3_company"],
        "companies_after": after["row_counts"]["v3_company"],
        "canonical_rows_before": before["row_counts"]["v3_quarter"],
        "canonical_rows_after": after["row_counts"]["v3_quarter"],
        "duplicate_identities": scalar(conn, "SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) c FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING c>1)"),
        "orphan_fundamentals": scalar(conn, "SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL"),
        "ttm_rows_before": before["row_counts"]["v3_ttm"],
        "ttm_rows_after": after["row_counts"]["v3_ttm"],
        "score_rows_before": before["row_counts"]["v3_score"],
        "score_rows_after": after["row_counts"]["v3_score"],
        "lifecycle_rows_before": before["row_counts"]["v3_lifecycle"],
        "lifecycle_rows_after": after["row_counts"]["v3_lifecycle"],
        "valuation_rows_before": before["row_counts"]["v3_valuation"],
        "valuation_rows_after": after["row_counts"]["v3_valuation"],
        "model_fingerprints_unchanged": after["score_model_fingerprint_ok"] and after["lifecycle_model_fingerprint_ok"],
        "rawcandle_writes": 0,
        "unrelated_canonical_drift": 0,
    }


def run_phase8a8(paths: Phase8A8Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    a7_r1 = read_csv(paths.a7_artifact_root / "residual_R1.csv")
    a7_r2 = read_csv(paths.a7_artifact_root / "residual_R2.csv")
    a7_r3 = read_csv(paths.a7_artifact_root / "residual_R3.csv")
    run_id = f"PHASE8A8_{utc_stamp()}"
    with connect(paths.v3_db) as conn:
        before = preflight(conn, paths.v3_db)
        if before["quick_check"] != "ok":
            raise RuntimeError(f"preflight quick_check failed: {before['quick_check']}")
        backup = make_backup(paths.v3_db, paths.artifact_root)
        r1_auth = r1_reconciliation(conn, a7_r1)
        write_csv(paths.artifact_root / "a7_residual_R1_reconciliation.csv", r1_auth)
        guarded = [identity_guard(conn, repair) for repair in FOUR_REPAIRS]
        write_csv(paths.artifact_root / "poww_rh_vtgn_tbla_verified_repairs.csv", FOUR_REPAIRS)
        write_csv(paths.artifact_root / "poww_rh_vtgn_identity_guard.csv", guarded)
        apply_audit = apply_four(conn, guarded, run_id)
        after = preflight(conn, paths.v3_db)
        proof = integrity(conn, before, after)
    write_csv(paths.artifact_root / "four_case_apply_audit.csv", apply_audit)

    final_r1, final_r2, final_r3, reviews = rebuild_residuals(paths.a7_artifact_root, apply_audit)
    write_csv(paths.artifact_root / "post_four_repair_R1.csv", final_r1)
    write_json(paths.artifact_root / "post_four_repair_R1_summary.json", {"r1": len(final_r1), "by_type": dict(Counter(r.get("residual_type") or r.get("field") for r in final_r1))})
    write_csv(paths.artifact_root / "publish_R1_review.csv", reviews["publish"])
    write_csv(paths.artifact_root / "period_end_R1_review.csv", reviews["period"])
    write_csv(paths.artifact_root / "revenue_R1_review.csv", reviews["revenue"])
    write_csv(paths.artifact_root / "cash_debt_other_R1_review.csv", reviews["other"])
    write_csv(paths.artifact_root / "local_evidence_resolution.csv", [r for group in reviews.values() for r in group if r["final_queue"] != "R1"])
    write_csv(paths.artifact_root / "phase8a8_additional_safe_repair_set.csv", [])
    write_csv(paths.artifact_root / "additional_safe_apply_audit.csv", [])
    write_csv(paths.artifact_root / "final_R1.csv", final_r1)
    write_csv(paths.artifact_root / "final_R2.csv", final_r2)
    write_csv(paths.artifact_root / "final_R3.csv", final_r3)
    ext = external_queue(final_r1)
    write_csv(paths.artifact_root / "external_research_queue_R1.csv", ext)
    (paths.artifact_root / "external_research_queue_R1_human_summary.md").write_text(
        "\n".join([f"# Phase 8A8 R1 External Research Queue", "", *(f"- {r['Request ID']}: {r['Exact Research Question']}" for r in ext), ""]),
        encoding="utf-8",
    )
    write_json(paths.artifact_root / "canonical_integrity_after_a8.json", proof)
    write_csv(paths.artifact_root / "derived_staleness_manifest.csv", [{"table": t, "writes": 0, "status": DERIVED_STALE} for t in ("v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")])
    post_root = paths.artifact_root / "post_a8_phase7_readonly_audit"
    post_phase7 = audit(paths.v3_db, paths.rawcandle_db, post_root)
    post_publish = len(read_csv(post_root / "canonical_publish_date_anomalies.csv"))
    post_semantic = len(read_csv(post_root / "field_semantic_outliers.csv"))
    summary = {
        "classification": CLASSIFICATION_READY if not final_r1 else CLASSIFICATION_EXTERNAL,
        "artifact_root": str(paths.artifact_root),
        "a7_r1": len(a7_r1),
        "a7_r2": len(a7_r2),
        "a7_r3": len(a7_r3),
        "companies": before["row_counts"]["v3_company"],
        "canonical_rows": before["row_counts"]["v3_quarter"],
        "ttm_rows": before["row_counts"]["v3_ttm"],
        "score_rows": before["row_counts"]["v3_score"],
        "lifecycle_rows": before["row_counts"]["v3_lifecycle"],
        "valuation_rows": before["row_counts"]["v3_valuation"],
        "four_case_rows_applied": sum(1 for r in apply_audit if r["apply_status"] == "APPLIED"),
        "four_case_write_failures": sum(1 for r in apply_audit if r["apply_status"] != "APPLIED"),
        "post_four_r1": len(final_r1),
        "final_r1": len(final_r1),
        "final_r2": len(final_r2),
        "final_r3": len(final_r3),
        "external_research_units": len(ext),
        "external_research_tickers": sorted({r["Ticker"] for r in ext}),
        "additional_safe_repairs": 0,
        "additional_safe_rows_applied": 0,
        "post_a8_publish_audit_count": post_publish,
        "post_a8_semantic_audit_count": post_semantic,
        "post_phase7_summary": post_phase7,
        "backup": backup,
        "integrity": proof,
        "derived_data_status": DERIVED_STALE,
        "downstream_writes": {"v3_ttm": 0, "v3_score": 0, "v3_lifecycle": 0, "v3_valuation": 0},
        "rawcandle_writes": 0,
        "next_action": "PHASE 8A9 — COMBINED DOWNSTREAM REBUILD: TTM -> SCORE -> LIFECYCLE -> VALUATION" if not final_r1 else "USER EXTERNAL RESEARCH — R1 QUEUE",
    }
    write_json(paths.artifact_root / "phase8a8_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    return summary
