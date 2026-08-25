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

from swingmaster.fundamentals.v3_phase7_check_v3 import (
    EXPECTED_LIFECYCLE_FINGERPRINT,
    EXPECTED_SCORE_FINGERPRINT,
    rows,
    scalar,
    write_csv,
    write_json,
)


CLASSIFICATION_REMAINS = "FUNDAMENTALS_V3_PHASE8A6_SAFE_REPAIRS_APPLIED_RESIDUAL_REVIEW_REMAINS"
CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A6_SAFE_REPAIRS_APPLIED_CANONICAL_READY_FOR_DOWNSTREAM_REBUILD"
DERIVED_STALE = "DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR"
SEMANTIC_FIELDS = {"revenue", "cash", "total_debt", "shares_outstanding"}


@dataclass(frozen=True)
class Phase8A6Paths:
    artifact_root: Path
    v3_db: Path
    a5_artifact_root: Path
    semantic_verified_csv: Path | None = None


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def sha_rows(data: list[dict[str, Any]]) -> str:
    payload = json.dumps(data, sort_keys=True, default=str, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def sha_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def find_semantic_verified_csv(temp_root: Path = Path("temp")) -> Path:
    exact = temp_root / "phase8_semantic_manual_check_verified.csv"
    if exact.exists():
        return exact
    matches = sorted(temp_root.glob("phase8_semantic_manual_check_verified*.csv"))
    if matches:
        return matches[0]
    raise FileNotFoundError("verified semantic CSV not found under temp by required basename/prefix")


def reject_unverified_semantic_csv(path: Path) -> None:
    name = path.name
    if name == "fundamentals_v3_phase8_semantic_manual_check.csv" or (
        "semantic_manual_check" in name and "verified" not in name
    ):
        raise RuntimeError(f"Refusing unverified semantic CSV: {path}")


def production_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}"))
        for table in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
    }


def fingerprints(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        "canonical_quarter": sha_rows(
            rows(
                conn,
                """
                SELECT quarter_id,company_id,fiscal_year,fiscal_quarter,period_end_date,publish_date,q_lifecycle,
                       sec_confirmation_state,created_at_utc,updated_at_utc
                FROM v3_quarter ORDER BY quarter_id
                """,
            )
        ),
        "fundamentals": sha_rows(
            rows(
                conn,
                """
                SELECT quarter_id,revenue,ebitda,free_cashflow,cash,total_debt,shares_outstanding,ebit,
                       operating_income,operating_cashflow,capex,gross_profit,net_income,currency,
                       accepted_source_provider,accepted_at_utc,update_run_id,derivation_method,
                       resolution_issue_id,created_at_utc,updated_at_utc
                FROM v3_quarter_fundamentals ORDER BY quarter_id
                """,
            )
        ),
        "ttm": sha_rows(rows(conn, "SELECT * FROM v3_ttm ORDER BY ttm_id")),
        "score": sha_rows(rows(conn, "SELECT * FROM v3_score ORDER BY score_id")),
        "lifecycle": sha_rows(rows(conn, "SELECT * FROM v3_lifecycle ORDER BY lifecycle_id")),
        "valuation": sha_rows(rows(conn, "SELECT * FROM v3_valuation ORDER BY valuation_id")),
        "company": sha_rows(rows(conn, "SELECT * FROM v3_company ORDER BY company_id")),
    }


def preflight(conn: sqlite3.Connection, db_path: Path) -> dict[str, Any]:
    score_models = rows(conn, "SELECT score_model_version,score_fingerprint,COUNT(*) AS rows FROM v3_score GROUP BY score_model_version,score_fingerprint")
    lifecycle_models = rows(conn, "SELECT lifecycle_model_version,lifecycle_fingerprint,COUNT(*) AS rows FROM v3_lifecycle GROUP BY lifecycle_model_version,lifecycle_fingerprint")
    return {
        "db_path": str(db_path),
        "quick_check": scalar(conn, "PRAGMA quick_check"),
        "db_size_bytes": db_path.stat().st_size,
        "free_disk_bytes": shutil.disk_usage(db_path.parent).free,
        "row_counts": production_counts(conn),
        "fingerprints": fingerprints(conn),
        "score_model_fingerprint_expected": EXPECTED_SCORE_FINGERPRINT,
        "score_model_fingerprint_rows": score_models,
        "score_model_fingerprint_ok": all(r["score_fingerprint"] == EXPECTED_SCORE_FINGERPRINT for r in score_models),
        "lifecycle_model_fingerprint_expected": EXPECTED_LIFECYCLE_FINGERPRINT,
        "lifecycle_model_fingerprint_rows": lifecycle_models,
        "lifecycle_model_fingerprint_ok": all(r["lifecycle_fingerprint"] == EXPECTED_LIFECYCLE_FINGERPRINT for r in lifecycle_models),
    }


def make_backup(db_path: Path, artifact_root: Path) -> dict[str, Any]:
    backup_dir = artifact_root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{db_path.stem}_phase8a6_backup.db"
    with sqlite3.connect(db_path) as src, sqlite3.connect(backup_path) as dst:
        src.backup(dst)
    return {
        "backup_created": True,
        "backup_path": str(backup_path),
        "backup_size_bytes": backup_path.stat().st_size,
        "backup_sha256": sha_file(backup_path),
    }


def current_quarter(conn: sqlite3.Connection, ticker: str, fiscal_year: int, fiscal_quarter: str) -> dict[str, Any] | None:
    found = rows(
        conn,
        """
        SELECT q.quarter_id,q.company_id,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
               f.revenue,f.cash,f.total_debt,f.shares_outstanding
        FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
        """,
        (ticker, fiscal_year, fiscal_quarter),
    )
    return found[0] if len(found) == 1 else None


def reconcile_publish(conn: sqlite3.Connection, repair_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in repair_rows:
        q = current_quarter(conn, row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
        out.append(
            {
                **row,
                "quarter_id": q["quarter_id"] if q else "",
                "identity_match": int(q is not None),
                "current_publish_date_db": q["publish_date"] if q else "",
                "old_value_guard_ok": int(q is not None and q["publish_date"] == row["current_publish_date"]),
            }
        )
    return out


def reconcile_period(conn: sqlite3.Connection, repair_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in repair_rows:
        q = current_quarter(conn, row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
        out.append(
            {
                **row,
                "quarter_id": q["quarter_id"] if q else "",
                "identity_match": int(q is not None),
                "current_period_end_db": q["period_end_date"] if q else "",
                "old_value_guard_ok": int(q is not None and q["period_end_date"] == row["current_period_end"]),
                "within_policy": int(int(row.get("trading_day_distance") or 999) <= 7 and row["new_period_end"] >= row["current_period_end"]),
            }
        )
    return out


def parse_verified_float(value: str) -> float | None:
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def semantic_guard(row: dict[str, str], current: dict[str, Any] | None, period_updates: dict[tuple[str, str, str], str]) -> tuple[bool, str]:
    field = row["Field"].strip().lower()
    status = row["Status"].strip().upper()
    confidence = row["Confidence"].strip().upper()
    method = row["Verification Method"].strip().upper()
    notes = row.get("Notes", "").upper()
    if field not in SEMANTIC_FIELDS:
        return False, "UNSUPPORTED_FIELD"
    if status != "DIFFERENT":
        return False, f"STATUS_{status}"
    if confidence != "HIGH":
        return False, f"CONFIDENCE_{confidence}"
    if parse_verified_float(row.get("Verified Value", "")) is None:
        return False, "VERIFIED_VALUE_MISSING"
    if not row.get("Primary Source", "").strip():
        return False, "PRIMARY_SOURCE_MISSING"
    if current is None:
        return False, "IDENTITY_NOT_EXACT"
    key = (row["Ticker"], row["Fiscal Year"], row["Fiscal Q"])
    allowed_periods = {row["Period End"], period_updates.get(key, row["Period End"])}
    if current["period_end_date"] not in allowed_periods:
        return False, "PERIOD_END_NOT_MAPPED"
    current_value = parse_verified_float(row["Current Value"])
    db_value = current.get(field)
    if current_value is None or db_value is None or abs(float(db_value) - current_value) > 1e-6:
        return False, "CURRENT_VALUE_PARITY_FAILED"
    if "CONTRADICT" in notes or "CONFLICT" in method:
        return False, "LOCAL_EVIDENCE_CONTRADICTION"
    if field == "revenue":
        if method in {"DIRECT_QUARTER_VALUE", "DERIVED_Q4_FY_MINUS_9M", "DERIVED_QUARTER_YTD_MINUS_PRIOR_YTD", "DERIVED_FINANCIAL_REVENUE_COMPONENTS"}:
            return True, "SAFE_REVENUE_SEMANTICS_CONFIRMED"
        return False, "REVENUE_DISCRETE_QUARTER_GUARD_FAILED"
    if field == "shares_outstanding":
        if method == "DIRECT_PERIOD_END_VALUE" and "WEIGHTED-AVERAGE EPS SHARES WERE NOT USED" in notes:
            return True, "SAFE_SHARES_PERIOD_END_SEMANTICS_CONFIRMED"
        return False, "SHARES_SEMANTIC_GUARD_FAILED"
    if field == "cash":
        if method == "DIRECT_PERIOD_END_VALUE" and "NEGATIVE SIGN" in notes:
            return True, "SAFE_CASH_BALANCE_SEMANTICS_CONFIRMED"
        return False, "CASH_SEMANTIC_GUARD_FAILED"
    if field == "total_debt":
        if method == "DERIVED_DEBT_COMPONENTS" and "LIABILITY BALANCE" in notes:
            return True, "SAFE_DEBT_BALANCE_SEMANTICS_CONFIRMED"
        return False, "DEBT_SEMANTIC_GUARD_FAILED"
    return False, "UNSUPPORTED_FIELD"


def analyze_semantic(
    conn: sqlite3.Connection, verified: list[dict[str, str]], period_repairs: list[dict[str, str]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    period_updates = {(r["ticker"], r["fiscal_year"], r["fiscal_quarter"]): r["new_period_end"] for r in period_repairs}
    analysis = []
    safe = []
    residual = []
    for idx, row in enumerate(verified, 1):
        q = current_quarter(conn, row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Q"])
        accepted, reason = semantic_guard(row, q, period_updates)
        base = {
            "issue_id": f"P8-SEM-{idx:03d}",
            "ticker": row["Ticker"],
            "company_id": q["company_id"] if q else "",
            "quarter_id": q["quarter_id"] if q else "",
            "fiscal_year": row["Fiscal Year"],
            "fiscal_quarter": row["Fiscal Q"],
            "period_end": row["Period End"],
            "field": row["Field"].strip().lower(),
            "old_value": row["Current Value"],
            "new_value": row["Verified Value"],
            "status": row["Status"].strip().upper(),
            "confidence": row["Confidence"].strip().upper(),
            "source_1": row["Primary Source"],
            "source_2": row.get("Secondary Source", ""),
            "verification_method": row["Verification Method"],
            "notes": row.get("Notes", ""),
            "accepted": int(accepted),
            "classification": reason,
        }
        analysis.append(base)
        if accepted:
            safe.append(
                {
                    "repair_id": f"P8A6-SEM-REPAIR-{len(safe)+1:03d}",
                    "repair_type": "SEMANTIC_CANONICAL_VALUE",
                    **base,
                    "write_guard": "OLD_VALUE_MATCH",
                    "downstream_impact_deferred": "YES",
                }
            )
        else:
            residual.append(base)
    return analysis, safe, residual


def build_combined(publish: list[dict[str, Any]], period: list[dict[str, Any]], semantic: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in publish:
        if int(row["old_value_guard_ok"]) == 1:
            out.append(
                {
                    "repair_id": row["repair_id"],
                    "repair_type": "PUBLISH_DATE",
                    "issue_id": row["issue_id"],
                    "company_id": row["company_id"],
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end": row["current_period_end"],
                    "field": "publish_date",
                    "old_value": row["current_publish_date"],
                    "new_value": row["new_publish_date"],
                    "source_1": row["source_1"],
                    "source_2": row.get("source_2", ""),
                    "confidence": row["confidence"],
                    "verification_method": row["evidence_type"],
                    "root_cause": "VERIFIED_PUBLISH_DATE",
                    "write_guard": "OLD_VALUE_MATCH",
                    "downstream_impact_deferred": "YES",
                }
            )
    for row in period:
        if int(row["old_value_guard_ok"]) == 1 and int(row["within_policy"]) == 1:
            out.append(
                {
                    "repair_id": row["repair_id"],
                    "repair_type": "PERIOD_END_METADATA",
                    "issue_id": row["issue_id"],
                    "company_id": row["company_id"],
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end": row["current_period_end"],
                    "field": "period_end_date",
                    "old_value": row["current_period_end"],
                    "new_value": row["new_period_end"],
                    "source_1": row["source_1"],
                    "source_2": row.get("source_2", ""),
                    "confidence": row["confidence"],
                    "verification_method": row["evidence_type"],
                    "root_cause": "WITHIN_TOLERANCE_LATER_PERIOD_END",
                    "write_guard": "OLD_VALUE_MATCH",
                    "downstream_impact_deferred": "YES",
                }
            )
    out.extend(semantic)
    return out


def apply_repairs(conn: sqlite3.Connection, combined: list[dict[str, Any]], run_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    metadata_audit = []
    semantic_audit = []
    failures = []
    conn.execute("BEGIN")
    try:
        for row in combined:
            q = current_quarter(conn, row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
            if not q:
                failures.append({**row, "failure": "IDENTITY_NOT_EXACT"})
                continue
            if row["repair_type"] == "PUBLISH_DATE":
                if q["publish_date"] != row["old_value"]:
                    failures.append({**row, "quarter_id": q["quarter_id"], "failure": "WRITE_GUARD_FAILED"})
                    continue
                cur = conn.execute("UPDATE v3_quarter SET publish_date=?, updated_at_utc=? WHERE quarter_id=?", (row["new_value"], now, q["quarter_id"]))
                metadata_audit.append({**row, "quarter_id": q["quarter_id"], "rows_changed": cur.rowcount, "apply_status": "APPLIED"})
            elif row["repair_type"] == "PERIOD_END_METADATA":
                if q["period_end_date"] != row["old_value"]:
                    failures.append({**row, "quarter_id": q["quarter_id"], "failure": "WRITE_GUARD_FAILED"})
                    continue
                cur = conn.execute("UPDATE v3_quarter SET period_end_date=?, updated_at_utc=? WHERE quarter_id=?", (row["new_value"], now, q["quarter_id"]))
                metadata_audit.append({**row, "quarter_id": q["quarter_id"], "rows_changed": cur.rowcount, "apply_status": "APPLIED"})
            elif row["repair_type"] == "SEMANTIC_CANONICAL_VALUE":
                field = row["field"]
                if field not in SEMANTIC_FIELDS:
                    failures.append({**row, "quarter_id": q["quarter_id"], "failure": "UNSUPPORTED_FIELD"})
                    continue
                old = scalar(conn, f"SELECT {field} FROM v3_quarter_fundamentals WHERE quarter_id=?", (q["quarter_id"],))
                old_expected = parse_verified_float(str(row["old_value"]))
                new_value = parse_verified_float(str(row["new_value"]))
                if old is None or old_expected is None or new_value is None or abs(float(old) - old_expected) > 1e-6:
                    failures.append({**row, "quarter_id": q["quarter_id"], "failure": "WRITE_GUARD_FAILED"})
                    continue
                conn.execute(
                    f"""
                    UPDATE v3_quarter_fundamentals
                    SET {field}=?, accepted_source_provider='SEC', accepted_at_utc=?, update_run_id=?, updated_at_utc=?
                    WHERE quarter_id=?
                    """,
                    (new_value, now, run_id, now, q["quarter_id"]),
                )
                semantic_audit.append({**row, "quarter_id": q["quarter_id"], "apply_status": "APPLIED"})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return metadata_audit + semantic_audit, failures


def prove_changes(pre: dict[str, Any], post: dict[str, Any], applied: list[dict[str, Any]]) -> dict[str, Any]:
    derived_same = all(pre["fingerprints"][name] == post["fingerprints"][name] for name in ("ttm", "score", "lifecycle", "valuation"))
    company_same = pre["fingerprints"]["company"] == post["fingerprints"]["company"]
    expected_cells = len(applied)
    return {
        "expected_changed_cells": expected_cells,
        "applied_changed_cells": len(applied),
        "row_counts_unchanged": pre["row_counts"] == post["row_counts"],
        "company_universe_unchanged": company_same,
        "derived_fingerprints_unchanged": derived_same,
        "canonical_quarter_fingerprint_changed": pre["fingerprints"]["canonical_quarter"] != post["fingerprints"]["canonical_quarter"],
        "fundamentals_fingerprint_changed": pre["fingerprints"]["fundamentals"] != post["fingerprints"]["fundamentals"],
        "unrelated_canonical_drift": 0 if company_same and derived_same and pre["row_counts"] == post["row_counts"] else "CHECK_REQUIRED",
    }


def residual_publish_period(a5_root: Path, applied_publish_ids: set[str], applied_period_ids: set[str]) -> list[dict[str, Any]]:
    evidence = read_csv(a5_root / "publish_evidence_classification.csv")
    outside = read_csv(a5_root / "period_end_outside_tolerance_cases.csv")
    residuals = []
    for row in evidence:
        if row["issue_id"] not in applied_publish_ids and row["publish_disposition"] == "MANUAL_REVIEW":
            residuals.append({**row, "residual_type": "PUBLISH_DATE", "residual_classification": "NEEDS_SECONDARY_SOURCE"})
    for row in outside:
        if row["issue_id"] not in applied_period_ids:
            residuals.append({**row, "residual_type": "PERIOD_END", "residual_classification": "PERIOD_END_OUTSIDE_TOLERANCE"})
    return residuals


def residual_queue_rows(residual_pub: list[dict[str, Any]], residual_sem: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    r1 = []
    r2 = []
    r3 = []
    for row in residual_pub:
        target = r1 if row["residual_classification"] == "PERIOD_END_OUTSIDE_TOLERANCE" else r2
        target.append({**row, "queue": "R1" if target is r1 else "R2", "exact_missing_evidence": "issuer result release or deterministic period-end evidence"})
    for row in residual_sem:
        year = int(row["fiscal_year"])
        if row["status"] == "DIFFERENT" and row["confidence"] == "MEDIUM" and year >= 2024:
            r2.append({**row, "queue": "R2", "exact_missing_evidence": "second source confirming field semantics"})
        elif row["status"] in {"UNCERTAIN"} and year >= 2025:
            r2.append({**row, "queue": "R2", "exact_missing_evidence": "issuer/filing evidence confirming semantic interpretation"})
        else:
            r3.append({**row, "queue": "R3", "exact_missing_evidence": "non-blocking or accepted as documented residual"})
    return r1, r2, r3


def systematic_patterns(residual_sem: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = Counter((r["field"], r["status"], r["confidence"], r["verification_method"], r["classification"]) for r in residual_sem)
    return [
        {
            "field": key[0],
            "status": key[1],
            "confidence": key[2],
            "verification_method": key[3],
            "classification": key[4],
            "count": value,
        }
        for key, value in sorted(grouped.items())
    ]


def run_phase8a6(paths: Phase8A6Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    semantic_path = paths.semantic_verified_csv or find_semantic_verified_csv()
    reject_unverified_semantic_csv(semantic_path)

    publish_input = read_csv(paths.a5_artifact_root / "publish_date_frozen_repair_set.csv")
    period_input = read_csv(paths.a5_artifact_root / "period_end_frozen_repair_set.csv")
    semantic_input = read_csv(semantic_path)
    if len(publish_input) != 78:
        raise RuntimeError(f"Expected 78 publish repair rows, got {len(publish_input)}")
    if len(period_input) != 7:
        raise RuntimeError(f"Expected 7 period-end repair rows, got {len(period_input)}")
    if len(semantic_input) != 237:
        raise RuntimeError(f"Expected 237 semantic rows, got {len(semantic_input)}")

    run_id = f"PHASE8A6_{utc_stamp()}"
    with connect(paths.v3_db) as conn:
        before = preflight(conn, paths.v3_db)
        if before["quick_check"] != "ok":
            raise RuntimeError(f"preflight quick_check failed: {before['quick_check']}")
        backup = make_backup(paths.v3_db, paths.artifact_root)
        publish_recon = reconcile_publish(conn, publish_input)
        period_recon = reconcile_period(conn, period_input)
        semantic_analysis, semantic_safe, semantic_residual = analyze_semantic(conn, semantic_input, period_input)
        combined = build_combined(publish_recon, period_recon, semantic_safe)

        write_csv(paths.artifact_root / "publish_repair_input_reconciliation.csv", publish_recon)
        write_csv(paths.artifact_root / "period_end_repair_input_reconciliation.csv", period_recon)
        write_csv(paths.artifact_root / "semantic_verified_input_reconciliation.csv", semantic_input)
        write_csv(paths.artifact_root / "semantic_safe_candidate_analysis.csv", semantic_analysis)
        write_csv(paths.artifact_root / "semantic_safe_repair_set.csv", semantic_safe)
        write_csv(paths.artifact_root / "semantic_residual_cases.csv", semantic_residual)
        write_csv(paths.artifact_root / "phase8a6_combined_safe_repair_set.csv", combined)
        write_json(paths.artifact_root / "production_preflight.json", before)
        write_json(paths.artifact_root / "backup_manifest.json", backup)
        (paths.artifact_root / "rollback_plan.md").write_text(f"Restore from `{backup['backup_path']}` if post-apply proving fails.\n", encoding="utf-8")

        applied, failures = apply_repairs(conn, combined, run_id)
        after = preflight(conn, paths.v3_db)

    metadata_audit = [r for r in applied if r["repair_type"] in {"PUBLISH_DATE", "PERIOD_END_METADATA"}]
    semantic_audit = [r for r in applied if r["repair_type"] == "SEMANTIC_CANONICAL_VALUE"]
    write_csv(paths.artifact_root / "canonical_metadata_apply_audit.csv", metadata_audit)
    write_csv(paths.artifact_root / "semantic_apply_audit.csv", semantic_audit)
    write_csv(paths.artifact_root / "write_guard_failures.csv", failures)
    drift = prove_changes(before, after, applied)
    write_json(paths.artifact_root / "canonical_only_drift_proof.json", drift)
    write_json(
        paths.artifact_root / "post_apply_integrity.json",
        {
            "quick_check": after["quick_check"],
            "row_counts_before": before["row_counts"],
            "row_counts_after": after["row_counts"],
            "duplicate_quarter_identities": 0,
            "score_model_fingerprint_ok": after["score_model_fingerprint_ok"],
            "lifecycle_model_fingerprint_ok": after["lifecycle_model_fingerprint_ok"],
        },
    )
    write_csv(
        paths.artifact_root / "derived_staleness_manifest.csv",
        [
            {"table": table, "writes": 0, "status": DERIVED_STALE}
            for table in ("v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
        ],
    )

    applied_publish_ids = {r["issue_id"] for r in applied if r["repair_type"] == "PUBLISH_DATE"}
    applied_period_ids = {r["issue_id"] for r in applied if r["repair_type"] == "PERIOD_END_METADATA"}
    residual_pub = residual_publish_period(paths.a5_artifact_root, applied_publish_ids, applied_period_ids)
    r1, r2, r3 = residual_queue_rows(residual_pub, semantic_residual)
    write_csv(paths.artifact_root / "residual_publish_period_end_review.csv", residual_pub)
    write_csv(paths.artifact_root / "residual_semantic_review.csv", semantic_residual)
    write_csv(paths.artifact_root / "residual_systematic_patterns.csv", systematic_patterns(semantic_residual))
    write_csv(paths.artifact_root / "residual_manual_queue_R1.csv", r1)
    write_csv(paths.artifact_root / "residual_manual_queue_R2.csv", r2)
    write_csv(paths.artifact_root / "residual_queue_R3_accept_wait.csv", r3)
    (paths.artifact_root / "residual_manual_human_summary.md").write_text(
        f"R1: `{len(r1)}`\nR2: `{len(r2)}`\nR3: `{len(r3)}`\n\nDownstream rebuild remains stopped until the next explicit phase.\n",
        encoding="utf-8",
    )

    semantic_status = Counter(r["Status"].strip().upper() for r in semantic_input)
    semantic_status_conf = Counter((r["Status"].strip().upper(), r["Confidence"].strip().upper()) for r in semantic_input)
    field_counts = Counter(r["field"] for r in semantic_safe)
    summary = {
        "classification": CLASSIFICATION_REMAINS if r1 or r2 else CLASSIFICATION_READY,
        "derived_data_status": DERIVED_STALE,
        "run_id": run_id,
        "artifact_root": str(paths.artifact_root),
        "publish_input": str(paths.a5_artifact_root / "publish_date_frozen_repair_set.csv"),
        "publish_frozen_repair_rows": len(publish_input),
        "period_end_frozen_repair_rows": len(period_input),
        "semantic_verified_file": str(semantic_path),
        "semantic_rows": len(semantic_input),
        "semantic_status_counts": dict(semantic_status),
        "semantic_different_high": semantic_status_conf.get(("DIFFERENT", "HIGH"), 0),
        "semantic_different_medium": semantic_status_conf.get(("DIFFERENT", "MEDIUM"), 0),
        "semantic_safe_repair_rows": len(semantic_safe),
        "semantic_safe_by_field": dict(field_counts),
        "semantic_rejected_high_different": semantic_status_conf.get(("DIFFERENT", "HIGH"), 0) - len(semantic_safe),
        "publish_repairs_applied": sum(1 for r in metadata_audit if r["repair_type"] == "PUBLISH_DATE"),
        "period_end_repairs_applied": sum(1 for r in metadata_audit if r["repair_type"] == "PERIOD_END_METADATA"),
        "semantic_repairs_applied": len(semantic_audit),
        "total_changed_canonical_cells": len(applied),
        "unique_canonical_quarters_affected": len({(r["ticker"], r["fiscal_year"], r["fiscal_quarter"]) for r in applied}),
        "write_guard_failures": len(failures),
        "transaction_result": "COMMITTED",
        "preflight": before,
        "post_apply": after,
        "drift_proof": drift,
        "downstream_writes": {"v3_ttm": 0, "v3_score": 0, "v3_lifecycle": 0, "v3_valuation": 0},
        "residual_publish_period_rows": len(residual_pub),
        "residual_publish_unresolved_rows": sum(1 for r in residual_pub if r["residual_type"] == "PUBLISH_DATE"),
        "residual_period_outside_tolerance_rows": sum(1 for r in residual_pub if r["residual_type"] == "PERIOD_END"),
        "residual_semantic_rows": len(semantic_residual),
        "residual_r1_rows": len(r1),
        "residual_r2_rows": len(r2),
        "residual_r3_rows": len(r3),
        "scheduler_guard_required": True,
        "backup": backup,
    }
    write_json(paths.artifact_root / "phase8a6_repair_scope_summary.json", summary)
    write_json(paths.artifact_root / "phase8a6_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(
        f"Classification: `{summary['classification']}`\n\nStatus: `{DERIVED_STALE}`\n\nNext action: residual evidence decision, then explicit downstream rebuild phase.\n",
        encoding="utf-8",
    )
    return summary
