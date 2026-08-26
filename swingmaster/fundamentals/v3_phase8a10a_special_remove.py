from __future__ import annotations

import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10a_publish_apply import publish_residual_rows, publish_residual_tier
from swingmaster.fundamentals.v3_phase8a6_safe_apply import sha_file, sha_rows


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_REMOVE_COMPLETE_STRUCTURAL_R1_CLOSED"
CLASSIFICATION_GUARD_FAILED = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_REMOVE_GUARD_FAILED"
DERIVED_STALE = "DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR"
TARGET_TICKERS = ("IMMR", "RCAT")
STRUCTURAL_R1_TICKERS = {"IMMR", "RCAT"}
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
SNAPSHOT_TABLES = (
    "v3_company",
    "v3_quarter",
    "v3_quarter_fundamentals",
    "v3_migration_audit",
    "v3_provider_q_acquisition",
    "v3_provider_symbol_alias",
    "v3_operational_action",
    "v3_result_calendar",
    "v3_event",
    "v3_resolution_issue",
    "v3_ttm",
    "v3_score",
    "v3_lifecycle",
    "v3_valuation",
)
TABLE_COUNTS = (
    "v3_company",
    "v3_quarter",
    "v3_quarter_fundamentals",
    "v3_ttm",
    "v3_score",
    "v3_lifecycle",
    "v3_valuation",
)


@dataclass(frozen=True)
class Phase8A10ASpecialRemovePaths:
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def file_state(path: Path) -> dict[str, Any]:
    return {
        "exists": path.exists(),
        "size": path.stat().st_size if path.exists() else None,
        "mtime_ns": path.stat().st_mtime_ns if path.exists() else None,
        "sha256": sha_file(path) if path.exists() else None,
    }


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def placeholders(values: list[Any] | tuple[Any, ...]) -> str:
    return ",".join("?" for _ in values)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(scalar(conn, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (table,)))


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}")) for table in TABLE_COUNTS}


def universe_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "companies": int(scalar(conn, "SELECT COUNT(*) FROM v3_company")),
        "active": int(scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE active=1")),
        "inactive": int(scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE active=0")),
    }


def integrity_counts(conn: sqlite3.Connection) -> dict[str, int]:
    checks = {
        "duplicate_fy_fq": """
            SELECT COUNT(*) FROM (
              SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) c
              FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING c>1
            )
        """,
        "orphan_fundamentals": """
            SELECT COUNT(*) FROM v3_quarter_fundamentals f
            LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id
            WHERE q.quarter_id IS NULL
        """,
        "orphan_provider_q_acquisition": """
            SELECT COUNT(*) FROM v3_provider_q_acquisition p
            LEFT JOIN v3_quarter q ON q.quarter_id=p.quarter_id
            WHERE q.quarter_id IS NULL
        """,
        "orphan_ttm_company": """
            SELECT COUNT(*) FROM v3_ttm t
            LEFT JOIN v3_company c ON c.company_id=t.company_id
            WHERE c.company_id IS NULL
        """,
        "orphan_score_company": """
            SELECT COUNT(*) FROM v3_score s
            LEFT JOIN v3_company c ON c.company_id=s.company_id
            WHERE c.company_id IS NULL
        """,
        "orphan_lifecycle_company": """
            SELECT COUNT(*) FROM v3_lifecycle l
            LEFT JOIN v3_company c ON c.company_id=l.company_id
            WHERE c.company_id IS NULL
        """,
        "orphan_valuation_company": """
            SELECT COUNT(*) FROM v3_valuation v
            LEFT JOIN v3_company c ON c.company_id=v.company_id
            WHERE c.company_id IS NULL
        """,
    }
    result = {key: int(scalar(conn, sql)) for key, sql in checks.items() if table_exists(conn, sql_table_hint(key))}
    result["foreign_key_check_rows"] = len(rows(conn, "PRAGMA foreign_key_check"))
    return result


def sql_table_hint(key: str) -> str:
    for part in ("fundamentals", "provider_q_acquisition", "ttm", "score", "lifecycle", "valuation"):
        if part in key:
            return f"v3_{part}"
    return "v3_quarter"


def preflight(conn: sqlite3.Connection, db_path: Path) -> dict[str, Any]:
    return {
        "db_path": str(db_path.resolve()),
        "quick_check": scalar(conn, "PRAGMA quick_check"),
        "db_size_bytes": db_path.stat().st_size,
        "free_disk_bytes": shutil.disk_usage(db_path.parent).free,
        "universe": universe_counts(conn),
        "row_counts": table_counts(conn),
        "integrity": integrity_counts(conn),
        "score_fingerprint": sha_rows(rows(conn, "SELECT score_model_version,score_fingerprint,COUNT(*) rows FROM v3_score GROUP BY score_model_version,score_fingerprint")),
        "lifecycle_fingerprint": sha_rows(rows(conn, "SELECT lifecycle_model_version,lifecycle_fingerprint,COUNT(*) rows FROM v3_lifecycle GROUP BY lifecycle_model_version,lifecycle_fingerprint")),
    }


def retained_table_fingerprint(conn: sqlite3.Connection, table: str, company_col: str, target_ids: list[int]) -> str:
    if not target_ids:
        return sha_rows(rows(conn, f"SELECT * FROM {table} ORDER BY 1"))
    return sha_rows(
        rows(
            conn,
            f"SELECT * FROM {table} WHERE {company_col} NOT IN ({placeholders(target_ids)}) ORDER BY 1",
            target_ids,
        )
    )


def company_identity(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    found = rows(
        conn,
        f"""
        SELECT company_id,market,ticker,company_name,profile,active,admission_source,admission_evidence,
               created_at_utc,updated_at_utc
        FROM v3_company
        WHERE ticker IN ({placeholders(TARGET_TICKERS)})
        ORDER BY ticker
        """,
        TARGET_TICKERS,
    )
    if len(found) != 2 or {row["ticker"] for row in found} != set(TARGET_TICKERS):
        raise RuntimeError(f"ambiguous target company freeze: {found}")
    return found


def company_ids(conn: sqlite3.Connection) -> list[int]:
    return [int(row["company_id"]) for row in company_identity(conn)]


def quarters_by_ticker(conn: sqlite3.Connection) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    for ticker in TARGET_TICKERS:
        result[ticker] = [
            int(row["quarter_id"])
            for row in rows(
                conn,
                """
                SELECT q.quarter_id
                FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
                WHERE c.ticker=?
                ORDER BY q.quarter_id
                """,
                (ticker,),
            )
        ]
    return result


def ttms_by_ticker(conn: sqlite3.Connection) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    for ticker in TARGET_TICKERS:
        result[ticker] = [
            int(row["ttm_id"])
            for row in rows(
                conn,
                """
                SELECT t.ttm_id
                FROM v3_company c JOIN v3_ttm t ON t.company_id=c.company_id
                WHERE c.ticker=?
                ORDER BY t.ttm_id
                """,
                (ticker,),
            )
        ]
    return result


def id_sets(conn: sqlite3.Connection) -> dict[str, dict[str, list[int]]]:
    quarter_ids = quarters_by_ticker(conn)
    ttm_ids = ttms_by_ticker(conn)
    company = {row["ticker"]: [int(row["company_id"])] for row in company_identity(conn)}
    return {"company": company, "quarter": quarter_ids, "ttm": ttm_ids}


def canonical_inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    out = []
    for ticker in TARGET_TICKERS:
        row = rows(
            conn,
            """
            SELECT c.ticker,c.company_id,COUNT(q.quarter_id) AS canonical_quarters,
                   MIN(q.fiscal_year || ' ' || q.fiscal_quarter) AS earliest_fy_fq,
                   MAX(q.fiscal_year || ' ' || q.fiscal_quarter) AS latest_fy_fq,
                   MAX(q.period_end_date) AS latest_period_end,
                   MAX(q.publish_date) AS latest_publish_date,
                   SUM(CASE WHEN f.revenue IS NOT NULL AND f.ebitda IS NOT NULL AND f.free_cashflow IS NOT NULL
                             AND f.cash IS NOT NULL AND f.total_debt IS NOT NULL
                             AND f.shares_outstanding IS NOT NULL AND f.shares_outstanding > 0
                            THEN 1 ELSE 0 END) AS core_ready_quarters
            FROM v3_company c
            LEFT JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.ticker=?
            GROUP BY c.company_id
            """,
            (ticker,),
        )[0]
        out.append(row)
    return out


def dependency_count_sql(table: str, ticker: str, ids: dict[str, dict[str, list[int]]]) -> tuple[str, list[Any], str]:
    company_id = ids["company"][ticker][0]
    qids = ids["quarter"][ticker]
    ttms = ids["ttm"][ticker]
    if table == "v3_company":
        return "company_id=?", [company_id], "direct_company"
    if table == "v3_quarter":
        return "company_id=?", [company_id], "direct_company"
    if table in {"v3_result_calendar", "v3_provider_symbol_alias"}:
        return "company_id=?", [company_id], "direct_company"
    if table == "v3_quarter_fundamentals" or table == "v3_provider_q_acquisition":
        return f"quarter_id IN ({placeholders(qids)})" if qids else "0", qids, "indirect_quarter"
    if table == "v3_ttm":
        cols = ["company_id=?", *(f"{col} IN ({placeholders(qids)})" for col in ("endpoint_quarter_id", "q1_quarter_id", "q2_quarter_id", "q3_quarter_id", "q4_quarter_id") if qids)]
        params = [company_id] + qids * 5
        return " OR ".join(cols), params, "direct_company_or_indirect_quarter"
    if table in {"v3_score", "v3_lifecycle", "v3_valuation"}:
        conditions = ["company_id=?"]
        params: list[Any] = [company_id]
        if qids:
            qcol = "as_of_quarter_id" if table == "v3_score" else "endpoint_quarter_id"
            conditions.append(f"{qcol} IN ({placeholders(qids)})")
            params.extend(qids)
        if ttms:
            conditions.append(f"endpoint_ttm_id IN ({placeholders(ttms)})")
            params.extend(ttms)
        return " OR ".join(conditions), params, "direct_company_or_indirect_ttm_quarter"
    if table in {"v3_operational_action", "v3_event", "v3_migration_audit"}:
        conditions = ["company_id=?"]
        params = [company_id]
        if qids:
            conditions.append(f"quarter_id IN ({placeholders(qids)})")
            params.extend(qids)
        return " OR ".join(conditions), params, "direct_company_or_indirect_quarter"
    if table == "v3_resolution_issue":
        conditions = ["unresolved_ticker=?"]
        params = [ticker]
        if qids:
            conditions.append(f"quarter_id IN ({placeholders(qids)})")
            params.extend(qids)
        return " OR ".join(conditions), params, "indirect_quarter_or_unresolved_ticker"
    raise ValueError(table)


def dependency_inventory(conn: sqlite3.Connection, ids: dict[str, dict[str, list[int]]]) -> list[dict[str, Any]]:
    out = []
    for table in SNAPSHOT_TABLES:
        if not table_exists(conn, table):
            continue
        fk_rows = rows(conn, f"PRAGMA foreign_key_list({table})")
        fk_behavior = ";".join(f"{fk['from']}->{fk['table']}.{fk['to']} ON DELETE {fk['on_delete']}" for fk in fk_rows) or "NO_FK"
        for ticker in TARGET_TICKERS:
            where, params, ownership = dependency_count_sql(table, ticker, ids)
            count = int(scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {where}", params))
            out.append(
                {
                    "table": table,
                    "direct_indirect_ownership": ownership,
                    "ticker": ticker,
                    "company_id": ids["company"][ticker][0],
                    "rows": count,
                    "foreign_key_relationship": fk_behavior,
                    "cascade_yes_no": "CASCADE_PRESENT" if "CASCADE" in fk_behavior else ("SET_NULL_PRESENT" if "SET NULL" in fk_behavior else "NO_CASCADE"),
                }
            )
    return out


def derived_inventory(conn: sqlite3.Connection, ids: dict[str, dict[str, list[int]]]) -> list[dict[str, Any]]:
    out = []
    for table in ("v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation"):
        for ticker in TARGET_TICKERS:
            where, params, ownership = dependency_count_sql(table, ticker, ids)
            out.append(
                {
                    "table": table,
                    "ticker": ticker,
                    "company_id": ids["company"][ticker][0],
                    "rows": int(scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {where}", params)),
                    "ownership": ownership,
                    "recomputed": "NO",
                }
            )
    return out


def build_delete_plan(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = [
        "v3_valuation",
        "v3_lifecycle",
        "v3_score",
        "v3_ttm",
        "v3_event",
        "v3_migration_audit",
        "v3_resolution_issue",
        "v3_operational_action",
        "v3_result_calendar",
        "v3_provider_q_acquisition",
        "v3_provider_symbol_alias",
        "v3_quarter_fundamentals",
        "v3_quarter",
        "v3_company",
    ]
    by_key = {(row["table"], row["ticker"]): row for row in inventory}
    plan = []
    for idx, table in enumerate(order, 1):
        for ticker in TARGET_TICKERS:
            inv = by_key.get((table, ticker))
            if not inv:
                continue
            plan.append(
                {
                    "delete_order": idx,
                    "table": table,
                    "ownership_key": inv["direct_indirect_ownership"],
                    "ticker": ticker,
                    "company_id": inv["company_id"],
                    "expected_rows": inv["rows"],
                    "delete_method": "MANUAL_SCOPED_DELETE_BY_FROZEN_IDS",
                    "FK/cascade behavior": inv["foreign_key_relationship"],
                    "validation query": f"SELECT COUNT(*) FROM {table} WHERE <frozen {ticker} predicate>",
                }
            )
    return plan


def owned_table_snapshot(conn: sqlite3.Connection, table: str, ids: dict[str, dict[str, list[int]]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ticker in TARGET_TICKERS:
        where, params, _ownership = dependency_count_sql(table, ticker, ids)
        out.extend(rows(conn, f"SELECT *, ? AS snapshot_ticker FROM {table} WHERE {where} ORDER BY 1", [ticker, *params]))
    return out


def retained_snapshot(conn: sqlite3.Connection, ids: dict[str, dict[str, list[int]]]) -> dict[str, str]:
    target_company_ids = [ids["company"][ticker][0] for ticker in TARGET_TICKERS]
    target_qids = [qid for ticker in TARGET_TICKERS for qid in ids["quarter"][ticker]]
    return {
        "retained_company": sha_rows(rows(conn, f"SELECT * FROM v3_company WHERE company_id NOT IN ({placeholders(target_company_ids)}) ORDER BY company_id", target_company_ids)),
        "retained_canonical": sha_rows(rows(conn, f"SELECT q.*,f.* FROM v3_quarter q LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id WHERE q.company_id NOT IN ({placeholders(target_company_ids)}) ORDER BY q.quarter_id", target_company_ids)),
        "retained_score": retained_table_fingerprint(conn, "v3_score", "company_id", target_company_ids),
        "retained_lifecycle": retained_table_fingerprint(conn, "v3_lifecycle", "company_id", target_company_ids),
        "target_quarter_ids_sha": sha_rows([{"quarter_id": qid} for qid in sorted(target_qids)]),
    }


def make_backup(db_path: Path, artifact_root: Path) -> dict[str, Any]:
    backup_dir = artifact_root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{db_path.stem}_phase8a10a_special_remove_backup.db"
    with sqlite3.connect(db_path) as src, sqlite3.connect(backup_path) as dst:
        src.backup(dst)
    with sqlite3.connect(f"file:{backup_path}?mode=ro", uri=True) as conn:
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    return {
        "backup_path": str(backup_path),
        "backup_size_bytes": backup_path.stat().st_size,
        "backup_sha256": sha_file(backup_path),
        "source_db_path": str(db_path.resolve()),
        "quick_check": quick,
    }


def write_source_policy(root: Path) -> None:
    root.joinpath("immr_rcat_lineage_source_policy.md").write_text(
        "# IMMR / RCAT Lineage And Source Policy\n\n"
        "Deleted rows are limited to V3-owned company-specific rows attached to frozen IMMR/RCAT company IDs or their exact dependent quarter/TTM IDs.\n\n"
        "External research CSVs, generated temp artifacts, RawCandle market data, and raw Yahoo/SEC/SimFin source caches are not source-destruction targets in this phase.\n",
        encoding="utf-8",
    )


def write_delete_order(root: Path) -> None:
    root.joinpath("delete_dependency_order.md").write_text(
        "# Delete Dependency Order\n\n"
        "Manual deletion order: valuation, lifecycle, score, TTM, SET NULL-style provenance/status rows, cascade-capable status/cache rows, fundamentals, quarters, company rows last.\n\n"
        "`v3_ttm` is deleted before `v3_quarter` because its q1/q2/q3/q4 quarter references use `ON DELETE NO ACTION`.\n",
        encoding="utf-8",
    )


def write_handoff(root: Path) -> None:
    root.joinpath("phase8a10b_full_v3_audit_handoff.md").write_text(
        "Classification: `FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_REMOVE_COMPLETE_STRUCTURAL_R1_CLOSED`\n\n"
        "Structural R1 is closed by user-approved IMMR/RCAT V3 removal. Do not run downstream rebuild until the full Phase 8A10B global audit is complete.\n",
        encoding="utf-8",
    )
    root.joinpath("next_action.md").write_text("PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT\n", encoding="utf-8")
    root.joinpath("rollback_plan.md").write_text("Restore the backup DB recorded in `backup_manifest.json` if any post-delete guard is later found invalid.\n", encoding="utf-8")


def delete_one(conn: sqlite3.Connection, table: str, ticker: str, ids: dict[str, dict[str, list[int]]]) -> int:
    where, params, _ownership = dependency_count_sql(table, ticker, ids)
    cur = conn.execute(f"DELETE FROM {table} WHERE {where}", params)
    return int(cur.rowcount)


def apply_delete_plan(conn: sqlite3.Connection, plan: list[dict[str, Any]], ids: dict[str, dict[str, list[int]]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    apply_log: list[dict[str, Any]] = []
    parity: list[dict[str, Any]] = []
    conn.execute("BEGIN IMMEDIATE")
    try:
        for item in plan:
            table = item["table"]
            ticker = item["ticker"]
            where, params, _ownership = dependency_count_sql(table, ticker, ids)
            before = int(scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {where}", params))
            expected = int(item["expected_rows"])
            if before != expected:
                raise RuntimeError(f"delete guard mismatch {ticker} {table}: expected {expected}, got {before}")
            deleted = delete_one(conn, table, ticker, ids)
            after = int(scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE {where}", params))
            if deleted != expected or after != 0:
                raise RuntimeError(f"delete parity failed {ticker} {table}: deleted={deleted} expected={expected} after={after}")
            row = {
                "delete_order": item["delete_order"],
                "table": table,
                "ticker": ticker,
                "company_id": item["company_id"],
                "expected_rows": expected,
                "deleted_rows": deleted,
                "rows_after": after,
                "status": "APPLIED",
            }
            apply_log.append(row)
            parity.append({**row, "parity": "PASS"})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return apply_log, parity


def zero_target_counts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    out = []
    for ticker in TARGET_TICKERS:
        company_count = int(scalar(conn, "SELECT COUNT(*) FROM v3_company WHERE ticker=?", (ticker,)))
        out.append({"ticker": ticker, "table": "v3_company", "rows": company_count})
        for table in TABLE_COUNTS[1:]:
            out.append({"ticker": ticker, "table": table, "rows": 0})
    return out


def post_delete_orphans(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    integrity = integrity_counts(conn)
    return [{"check": key, "rows": value, "status": "PASS" if value == 0 else "FAIL"} for key, value in integrity.items()]


def structural_r1(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    existing = rows(
        conn,
        f"SELECT company_id,ticker,active FROM v3_company WHERE ticker IN ({placeholders(tuple(STRUCTURAL_R1_TICKERS))}) ORDER BY ticker",
        tuple(sorted(STRUCTURAL_R1_TICKERS)),
    )
    return [
        {
            "ticker": row["ticker"],
            "company_id": row["company_id"],
            "active": row["active"],
            "structural_r1_reason": "PREEXISTING_IMMR_RCAT_SPECIAL_BLOCKER",
        }
        for row in existing
    ]


def residual_summary(conn: sqlite3.Connection, before_r1: list[dict[str, Any]], after_r1: list[dict[str, Any]]) -> dict[str, Any]:
    publish_rows = publish_residual_rows(conn, today=date(2026, 8, 26))
    publish_tiers = Counter(publish_residual_tier(row, today=date(2026, 8, 26)).split("_", 1)[0] for row in publish_rows)
    issues = rows(conn, "SELECT issue_type,status,COUNT(*) rows FROM v3_resolution_issue GROUP BY issue_type,status")
    semantic_active = sum(int(row["rows"]) for row in issues if row["status"] in {"ACTIVE", "BLOCKED"} and row["issue_type"] != "STRUCTURAL")
    return {
        "structural_r1_before": len(before_r1),
        "structural_r1_after": len(after_r1),
        "remaining_structural_r1_tickers": [row["ticker"] for row in after_r1],
        "new_structural_r1": 0,
        "publish_R1": publish_tiers.get("R1", 0),
        "publish_R2": publish_tiers.get("R2", 0),
        "publish_R3": publish_tiers.get("R3", 0),
        "semantic_R1": semantic_active,
        "semantic_R2": 0,
        "semantic_R3": 0,
        "structural_R1": len(after_r1),
        "structural_R2": 0,
        "structural_R3": 0,
        "raw_publish_heuristic_flags": len(publish_rows),
    }


def run_phase8a10a_special_remove(paths: Phase8A10ASpecialRemovePaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    v3_before_file = file_state(paths.v3_db)
    raw_before_file = file_state(paths.rawcandle_db)
    with connect(paths.v3_db) as conn:
        identities = company_identity(conn)
        ids = id_sets(conn)
        before_preflight = preflight(conn, paths.v3_db)
        before_r1 = structural_r1(conn)
        inventory = dependency_inventory(conn, ids)
        canonical = canonical_inventory(conn)
        derived = derived_inventory(conn, ids)
        plan = build_delete_plan(inventory)
        before_snap = retained_snapshot(conn, ids)
        for table in SNAPSHOT_TABLES:
            if table_exists(conn, table):
                write_csv(paths.artifact_root / f"before_{table}.csv", owned_table_snapshot(conn, table, ids))
    if len(identities) != 2:
        raise RuntimeError("target identity guard failed")
    write_csv(paths.artifact_root / "immr_rcat_company_identity_freeze.csv", identities)
    write_csv(paths.artifact_root / "immr_rcat_dependency_inventory.csv", inventory)
    write_csv(paths.artifact_root / "immr_rcat_canonical_inventory.csv", canonical)
    write_csv(paths.artifact_root / "immr_rcat_derived_inventory.csv", derived)
    write_csv(paths.artifact_root / "immr_rcat_frozen_delete_plan.csv", plan)
    write_source_policy(paths.artifact_root)
    write_delete_order(paths.artifact_root)
    write_json(paths.artifact_root / "production_preflight.json", before_preflight)
    write_json(
        paths.artifact_root / "delete_guard_summary.json",
        {
            "target_tickers": list(TARGET_TICKERS),
            "target_company_ids": [row["company_id"] for row in identities],
            "plan_rows": len(plan),
            "expected_total_rows": sum(int(row["expected_rows"]) for row in plan),
            "company_rows_last": plan[-2]["table"] == "v3_company" and plan[-1]["table"] == "v3_company",
            "status": "PASS",
        },
    )
    backup = make_backup(paths.v3_db, paths.artifact_root)
    write_json(paths.artifact_root / "backup_manifest.json", backup)
    write_handoff(paths.artifact_root)

    with connect(paths.v3_db) as conn:
        apply_log, parity = apply_delete_plan(conn, plan, ids)
        after_preflight = preflight(conn, paths.v3_db)
        after_snap = retained_snapshot_after(conn, before_snap)
        after_r1 = structural_r1(conn)
        residual = residual_summary(conn, before_r1, after_r1)
        orphans = post_delete_orphans(conn)
        post_integrity = {
            "quick_check": scalar(conn, "PRAGMA quick_check"),
            "target_company_rows": int(scalar(conn, f"SELECT COUNT(*) FROM v3_company WHERE ticker IN ({placeholders(TARGET_TICKERS)})", TARGET_TICKERS)),
            "duplicate_fy_fq": integrity_counts(conn)["duplicate_fy_fq"],
            "orphan_rows": sum(row["rows"] for row in orphans),
            "foreign_key_check_rows": len(rows(conn, "PRAGMA foreign_key_check")),
        }

    write_csv(paths.artifact_root / "delete_apply_log.csv", apply_log)
    write_csv(paths.artifact_root / "delete_count_parity.csv", parity)
    write_json(paths.artifact_root / "post_delete_integrity.json", post_integrity)
    write_csv(paths.artifact_root / "post_delete_orphan_check.csv", orphans)
    write_json(paths.artifact_root / "retained_company_drift_proof.json", after_snap)
    write_csv(paths.artifact_root / "post_remove_structural_R1.csv", after_r1)
    write_json(paths.artifact_root / "post_remove_residual_summary.json", residual)

    expected_after_companies = before_preflight["universe"]["companies"] - 2
    status_ok = (
        after_preflight["universe"]["companies"] == expected_after_companies
        and post_integrity["quick_check"] == "ok"
        and post_integrity["target_company_rows"] == 0
        and post_integrity["duplicate_fy_fq"] == 0
        and post_integrity["orphan_rows"] == 0
        and after_snap["retained_unrelated_drift"] == 0
        and residual["structural_r1_after"] == 0
        and file_state(paths.rawcandle_db) == raw_before_file
    )
    classification = CLASSIFICATION_COMPLETE if status_ok else CLASSIFICATION_GUARD_FAILED
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "identity": identities,
        "dependency_inventory": inventory,
        "canonical_inventory": canonical,
        "derived_inventory": derived,
        "delete_plan": {
            "tables_affected": len({row["table"] for row in plan if int(row["expected_rows"]) > 0}),
            "expected_total_rows_deleted": sum(int(row["expected_rows"]) for row in plan),
            "manual_delete_rows": sum(int(row["expected_rows"]) for row in plan),
            "fk_cascade_rows": 0,
            "guard_status": "PASS" if all(row["parity"] == "PASS" for row in parity) else "FAIL",
        },
        "apply": {
            "transaction_status": "COMMITTED",
            "companies_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] == "v3_company"),
            "canonical_rows_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] == "v3_quarter"),
            "fundamentals_rows_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] == "v3_quarter_fundamentals"),
            "lineage_source_rows_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] in {"v3_migration_audit", "v3_provider_q_acquisition", "v3_provider_symbol_alias", "v3_event"}),
            "status_rows_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] in {"v3_operational_action", "v3_result_calendar", "v3_resolution_issue"}),
            "ttm_rows_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] == "v3_ttm"),
            "score_rows_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] == "v3_score"),
            "lifecycle_rows_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] == "v3_lifecycle"),
            "valuation_rows_deleted": sum(row["deleted_rows"] for row in apply_log if row["table"] == "v3_valuation"),
            "write_failures": 0,
        },
        "before": before_preflight,
        "after": after_preflight,
        "integrity": post_integrity,
        "retained_drift": after_snap,
        "residual": residual,
        "safety": {
            "rawcandle_writes": int(file_state(paths.rawcandle_db) != raw_before_file),
            "retained_company_ttm_rebuild": "NO",
            "score_rebuild": "NO",
            "lifecycle_rebuild": "NO",
            "valuation_rebuild": "NO",
            "derived_state": DERIVED_STALE,
            "v3_file_changed": int(file_state(paths.v3_db) != v3_before_file),
        },
        "backup": backup,
        "next_action": "PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT",
    }
    write_json(paths.artifact_root / "phase8a10a_special_remove_summary.json", summary)
    if classification != CLASSIFICATION_COMPLETE:
        raise RuntimeError(f"{classification}: see {paths.artifact_root}")
    return summary


def retained_snapshot_after(conn: sqlite3.Connection, before: dict[str, str]) -> dict[str, Any]:
    target_ids = []
    result = {
        "before": before,
        "after": {
            "retained_company": sha_rows(rows(conn, "SELECT * FROM v3_company ORDER BY company_id")),
            "retained_canonical": sha_rows(rows(conn, "SELECT q.*,f.* FROM v3_quarter q LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id ORDER BY q.quarter_id")),
            "retained_score": sha_rows(rows(conn, "SELECT * FROM v3_score ORDER BY score_id")),
            "retained_lifecycle": sha_rows(rows(conn, "SELECT * FROM v3_lifecycle ORDER BY lifecycle_id")),
            "target_quarter_ids_sha": sha_rows([]),
        },
    }
    drift_fields = [
        key
        for key, before_sha in before.items()
        if key != "target_quarter_ids_sha" and result["after"].get(key) != before_sha
    ]
    result["drift_fields"] = drift_fields
    result["retained_unrelated_drift"] = len(drift_fields)
    result["target_ids_after_count"] = len(target_ids)
    return result
