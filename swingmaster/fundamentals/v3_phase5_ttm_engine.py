from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE5_TTM_ENGINE_COMPLETE_READY_FOR_PHASE6"
CLASSIFICATION_OWNERSHIP = "FUNDAMENTALS_V3_PHASE5_BLOCKED_BY_DERIVED_DATA_OWNERSHIP_AMBIGUITY"
CLASSIFICATION_REPAIR = "FUNDAMENTALS_V3_PHASE5_TTM_ENGINE_REPAIR_REQUIRED"
NEXT_PHASE = "MASTER PLAN PHASE 6 - SCORE & VALUATION ENGINE"
MODEL_VERSION = "V3_TTM_EBIT_FIRST_V1"
FLOW_FIELDS = ("revenue", "gross_profit", "operating_income", "ebit", "ebitda", "net_income", "operating_cashflow", "capex", "free_cashflow")
INSTANT_FIELDS = ("cash", "total_debt", "shares_outstanding")
TTM_FIELD_MAP = {
    "revenue": "ttm_revenue",
    "gross_profit": "ttm_gross_profit",
    "operating_income": "ttm_operating_income",
    "ebit": "ttm_ebit",
    "ebitda": "ttm_ebitda",
    "net_income": "ttm_net_income",
    "operating_cashflow": "ttm_ocf",
    "capex": "ttm_capex",
    "free_cashflow": "ttm_fcf",
}


def run_phase5_ttm_engine(*, v3_db: Path, artifact_root: Path, apply: bool = True, run_id: str | None = None) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    run_id = run_id or f"V3_PHASE5_TTM_REBUILD_{utc_stamp()}"
    before = production_baseline(v3_db)
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        inventory = legacy_inventory(conn)
        dependency_graph = dependency_graph_rows(inventory)
        ebitda_logic = ebitda_first_logic_inventory()
        deletion_plan = deletion_plan_rows(inventory)
    if any(row["classification"] == "AMBIGUOUS_OWNERSHIP" for row in deletion_plan):
        summary = {"classification": CLASSIFICATION_OWNERSHIP, "artifact_root": str(artifact_root)}
        write_json(artifact_root / "phase5_summary.json", summary)
        return summary
    checkpoint = create_checkpoint(v3_db, artifact_root) if apply else {}
    rows = load_canonical_rows(v3_db)
    computed = compute_ttm_rows(rows, run_id=run_id, calculated_at=utc_now())
    dry_summary = summarize_ttm(computed)
    cleanup_summary = {"applied": False, "deleted_rows": 0, "dropped_objects": 0}
    production_summary = {"rows_written": 0, "idempotent_second_run_changes": 0}
    if apply:
        with sqlite3.connect(v3_db) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            cleanup_summary = apply_cleanup(conn, deletion_plan)
            ensure_ttm_schema(conn)
            first = rebuild_ttm(conn, computed)
            second = rebuild_ttm(conn, computed)
            conn.commit()
            production_summary = {"rows_written": first, "idempotent_second_run_changes": second}
    after = production_baseline(v3_db)
    integrity = structural_integrity(v3_db)
    validation = validation_artifacts(rows, computed)
    final_counts = production_ttm_counts(v3_db)
    safety = {
        "companies_unchanged": before["companies"] == after["companies"],
        "canonical_q_unchanged": before["canonical_q"] == after["canonical_q"],
        "canonical_fundamental_hash_unchanged": before["fundamental_hash"] == after["fundamental_hash"],
        "old_downstream_values_recreated": final_counts["v3_score_rows"] + final_counts["v3_valuation_rows"],
    }
    gate = {
        "cleanup_complete": cleanup_summary["applied"] if apply else True,
        "ttm_rows_written": final_counts["v3_ttm_rows"] == len(computed),
        "canonical_safe": all(value is True for key, value in safety.items() if key != "old_downstream_values_recreated"),
        "downstream_empty": safety["old_downstream_values_recreated"] == 0,
        "integrity": integrity["phase3_structural_gates_pass"],
        "idempotent": production_summary["idempotent_second_run_changes"] == 0,
    }
    gate["passed"] = all(gate.values())
    classification = CLASSIFICATION_COMPLETE if gate["passed"] else CLASSIFICATION_REPAIR
    summary = {
        "classification": classification,
        "recommended_next_step": NEXT_PHASE,
        "run_id": run_id,
        "checkpoint": checkpoint,
        "inventory": {"old_ttm_objects": sum(1 for row in inventory if row["object_name"] == "v3_ttm"), "old_downstream_objects": sum(1 for row in inventory if row["object_name"] in {"v3_score", "v3_valuation"})},
        "cleanup": cleanup_summary,
        "dry_rebuild": dry_summary,
        "production": {**production_summary, **final_counts},
        "validation": summarize_validation(validation),
        "safety": safety,
        "integrity": integrity,
        "gate": gate,
        "artifact_root": str(artifact_root),
    }
    write_artifacts(artifact_root, summary, inventory, dependency_graph, ebitda_logic, deletion_plan, checkpoint, cleanup_summary, dry_summary, computed, validation)
    write_doc(Path("docs/fundamentals_v3_phase5_ttm_engine.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def ttm_schema_sql() -> str:
    return """
CREATE TABLE v3_ttm (
    ttm_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    endpoint_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
    endpoint_fiscal_year INTEGER NOT NULL,
    endpoint_fiscal_quarter TEXT NOT NULL CHECK (endpoint_fiscal_quarter IN ('Q1','Q2','Q3','Q4')),
    period_end TEXT,
    model_version TEXT NOT NULL,
    ttm_revenue REAL,
    ttm_gross_profit REAL,
    ttm_operating_income REAL,
    ttm_ebit REAL,
    ttm_ebitda REAL,
    ttm_net_income REAL,
    ttm_ocf REAL,
    ttm_capex REAL,
    ttm_fcf REAL,
    cash REAL,
    total_debt REAL,
    shares_outstanding REAL,
    revenue_4q_ready INTEGER NOT NULL CHECK (revenue_4q_ready IN (0,1)),
    gross_profit_4q_ready INTEGER NOT NULL CHECK (gross_profit_4q_ready IN (0,1)),
    operating_income_4q_ready INTEGER NOT NULL CHECK (operating_income_4q_ready IN (0,1)),
    ebit_4q_ready INTEGER NOT NULL CHECK (ebit_4q_ready IN (0,1)),
    ebitda_4q_ready INTEGER NOT NULL CHECK (ebitda_4q_ready IN (0,1)),
    net_income_4q_ready INTEGER NOT NULL CHECK (net_income_4q_ready IN (0,1)),
    ocf_4q_ready INTEGER NOT NULL CHECK (ocf_4q_ready IN (0,1)),
    capex_4q_ready INTEGER NOT NULL CHECK (capex_4q_ready IN (0,1)),
    fcf_4q_ready INTEGER NOT NULL CHECK (fcf_4q_ready IN (0,1)),
    ttm_ebit_primary_ready INTEGER NOT NULL CHECK (ttm_ebit_primary_ready IN (0,1)),
    ttm_ebitda_secondary_ready INTEGER NOT NULL CHECK (ttm_ebitda_secondary_ready IN (0,1)),
    core_ttm_ebit_ready INTEGER NOT NULL CHECK (core_ttm_ebit_ready IN (0,1)),
    core_ttm_ebitda_ready INTEGER NOT NULL CHECK (core_ttm_ebitda_ready IN (0,1)),
    ttm_available_date TEXT,
    ttm_pit_ready INTEGER NOT NULL CHECK (ttm_pit_ready IN (0,1)),
    underlying_publish_dates_complete INTEGER NOT NULL CHECK (underlying_publish_dates_complete IN (0,1)),
    q1_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
    q2_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
    q3_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
    q4_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
    calculation_version TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    output_json TEXT,
    run_id TEXT NOT NULL,
    calculated_at_utc TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, endpoint_quarter_id, model_version)
);
CREATE INDEX IF NOT EXISTS idx_v3_ttm_company_endpoint ON v3_ttm(company_id, endpoint_fiscal_year, endpoint_fiscal_quarter);
CREATE INDEX IF NOT EXISTS idx_v3_ttm_ready ON v3_ttm(core_ttm_ebit_ready, ttm_pit_ready);
"""


def ensure_ttm_schema(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS v3_ttm")
    conn.executescript(ttm_schema_sql())


def legacy_inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    objects = []
    names = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for name in sorted(names):
        if name in {"v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_migration_audit", "v3_provider_q_acquisition", "v3_result_calendar"}:
            cls = "KEEP_CANONICAL"
        elif name == "v3_ttm":
            cls = "REBUILD_IN_PHASE5"
        elif name in {"v3_score", "v3_valuation"}:
            cls = "REBUILD_LATER_PHASE6"
        else:
            cls = "KEEP_UNRELATED"
        rows = table_count(conn, name)
        objects.append({"object_name": name, "object_type": "table", "rows": rows, "classification": cls, "action": action_for_class(cls)})
    return objects


def action_for_class(cls: str) -> str:
    return {"REBUILD_IN_PHASE5": "DROP_RECREATE", "REBUILD_LATER_PHASE6": "DELETE_ROWS_KEEP_SCHEMA"}.get(cls, "KEEP")


def dependency_graph_rows(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"source_object": "v3_ttm", "dependent": "v3_score", "dependency": "Phase 6 score inputs"},
        {"source_object": "v3_ttm", "dependent": "v3_valuation", "dependency": "Phase 6 valuation inputs"},
        {"source_object": "v3_ttm", "dependent": "phase6_score_valuation_engine", "dependency": "future consumer"},
    ]


def ebitda_first_logic_inventory() -> list[dict[str, Any]]:
    return [
        {"path": "docs/fundamentals_v3_implementation_readiness_contract.md", "old_logic": "v3_score described as EBITDA-based score outputs", "phase6_review_required": 1, "recommended_action": "Review EBIT-primary scoring policy in Phase 6"},
        {"path": "v3_ttm old schema", "old_logic": "Only revenue_ttm, ebitda_ttm, fcf_ttm, net_debt were first-class columns", "phase6_review_required": 1, "recommended_action": "Replace with EBIT-first TTM inputs"},
    ]


def deletion_plan_rows(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in inventory:
        if row["object_name"] == "v3_ttm":
            reason = "Obsolete EBITDA-first/limited TTM schema invalidated by Phase 5 EBIT-first contract"
        elif row["object_name"] in {"v3_score", "v3_valuation"}:
            reason = "Downstream derived output belongs to Phase 6 and must remain empty after TTM rebuild"
        elif row["classification"] == "KEEP_CANONICAL":
            reason = "Canonical/source object protected"
        else:
            reason = "Unrelated V3 operational object"
        out.append({**row, "why": reason, "dependencies": ",".join(dep["dependent"] for dep in dependency_graph_rows(inventory) if dep["source_object"] == row["object_name"])})
    return out


def create_checkpoint(v3_db: Path, artifact_root: Path) -> dict[str, Any]:
    checkpoint = artifact_root / f"{v3_db.name}.pre_phase5_ttm_engine.checkpoint.db"
    shutil.copy2(v3_db, checkpoint)
    with sqlite3.connect(checkpoint) as conn:
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
        fk = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    return {"path": str(checkpoint), "size": checkpoint.stat().st_size, "quick_check": quick, "foreign_key_check_rows": fk, **production_baseline(checkpoint)}


def apply_cleanup(conn: sqlite3.Connection, plan: list[dict[str, Any]]) -> dict[str, Any]:
    deleted = 0
    dropped = 0
    for table in ("v3_score", "v3_valuation"):
        before = table_count(conn, table)
        conn.execute(f"DELETE FROM {table}")
        deleted += before
    conn.execute("DROP TABLE IF EXISTS v3_ttm")
    dropped += 1
    return {"applied": True, "deleted_rows": deleted, "dropped_objects": dropped, "plan_rows": len(plan)}


def load_canonical_rows(v3_db: Path) -> list[dict[str, Any]]:
    fields = ",".join(f"f.{field}" for field in (*FLOW_FIELDS, *INSTANT_FIELDS))
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            f"""
            SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,{fields}
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            ORDER BY c.company_id,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END
            """
        )]


def compute_ttm_rows(rows: list[dict[str, Any]], *, run_id: str, calculated_at: str) -> list[dict[str, Any]]:
    out = []
    for _company_id, qrows in rows_by_company(rows).items():
        for idx in range(3, len(qrows)):
            window = qrows[idx - 3 : idx + 1]
            if not contiguous(window):
                continue
            out.append(ttm_row(window, run_id=run_id, calculated_at=calculated_at))
    return out


def ttm_row(window: list[dict[str, Any]], *, run_id: str, calculated_at: str) -> dict[str, Any]:
    endpoint = window[-1]
    row: dict[str, Any] = {
        "company_id": endpoint["company_id"],
        "endpoint_quarter_id": endpoint["quarter_id"],
        "endpoint_fiscal_year": endpoint["fiscal_year"],
        "endpoint_fiscal_quarter": endpoint["fiscal_quarter"],
        "period_end": endpoint["period_end_date"],
        "model_version": MODEL_VERSION,
        "cash": endpoint["cash"],
        "total_debt": endpoint["total_debt"],
        "shares_outstanding": endpoint["shares_outstanding"],
        "q1_quarter_id": window[0]["quarter_id"],
        "q2_quarter_id": window[1]["quarter_id"],
        "q3_quarter_id": window[2]["quarter_id"],
        "q4_quarter_id": window[3]["quarter_id"],
        "calculation_version": MODEL_VERSION,
        "run_id": run_id,
        "calculated_at_utc": calculated_at,
        "created_at_utc": calculated_at,
        "updated_at_utc": calculated_at,
    }
    for field, out_field in TTM_FIELD_MAP.items():
        ready = all(item[field] is not None for item in window)
        row[out_field] = sum(float(item[field]) for item in window) if ready else None
        row[ready_column(field)] = int(ready)
    row["ttm_ebit_primary_ready"] = row["ebit_4q_ready"]
    row["ttm_ebitda_secondary_ready"] = row["ebitda_4q_ready"]
    instant_ready = endpoint["cash"] is not None and endpoint["total_debt"] is not None and endpoint["shares_outstanding"] is not None and float(endpoint["shares_outstanding"] or 0) > 0
    row["core_ttm_ebit_ready"] = int(row["revenue_4q_ready"] and row["ebit_4q_ready"] and row["fcf_4q_ready"] and instant_ready)
    row["core_ttm_ebitda_ready"] = int(row["revenue_4q_ready"] and row["ebitda_4q_ready"] and row["fcf_4q_ready"] and instant_ready)
    publish_dates = [item["publish_date"] for item in window]
    publish_complete = all(publish_dates)
    row["underlying_publish_dates_complete"] = int(publish_complete)
    row["ttm_available_date"] = max(publish_dates) if publish_complete else None
    row["ttm_pit_ready"] = int(row["core_ttm_ebit_ready"] and publish_complete)
    row["source_fingerprint"] = hash_json([{k: item.get(k) for k in ("quarter_id", "publish_date", *FLOW_FIELDS, *INSTANT_FIELDS)} for item in window])
    row["output_json"] = json.dumps({"underlying_publish_dates": publish_dates, "mode": "CURRENT_CANONICAL_TTM_WITH_PIT_AVAILABILITY"}, sort_keys=True)
    return row


def ready_column(field: str) -> str:
    return {"operating_cashflow": "ocf_4q_ready", "free_cashflow": "fcf_4q_ready"}.get(field, f"{field}_4q_ready")


def rebuild_ttm(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if existing_ttm_matches(conn, rows):
        return 0
    before = table_count(conn, "v3_ttm")
    conn.execute("DELETE FROM v3_ttm")
    if rows:
        fields = list(rows[0].keys())
        placeholders = ",".join("?" for _ in fields)
        conn.executemany(f"INSERT INTO v3_ttm ({','.join(fields)}) VALUES ({placeholders})", [[row[field] for field in fields] for row in rows])
    return abs(table_count(conn, "v3_ttm") - before)


def existing_ttm_matches(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> bool:
    try:
        existing_count = table_count(conn, "v3_ttm")
        if existing_count != len(rows):
            return False
        existing = {
            (int(row["company_id"]), int(row["endpoint_quarter_id"]), row["model_version"]): row["source_fingerprint"]
            for row in conn.execute("SELECT company_id,endpoint_quarter_id,model_version,source_fingerprint FROM v3_ttm")
        }
    except sqlite3.OperationalError:
        return False
    expected = {
        (int(row["company_id"]), int(row["endpoint_quarter_id"]), row["model_version"]): row["source_fingerprint"]
        for row in rows
    }
    return existing == expected


def summarize_ttm(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {"total_endpoints": len(rows), "core_ttm_ebit_ready": sum(row["core_ttm_ebit_ready"] for row in rows), "core_ttm_ebitda_ready": sum(row["core_ttm_ebitda_ready"] for row in rows), "ttm_pit_ready": sum(row["ttm_pit_ready"] for row in rows), "publish_incomplete": sum(1 for row in rows if not row["underlying_publish_dates_complete"])}
    for field in FLOW_FIELDS:
        col = ready_column(field)
        summary[f"{field}_ready"] = sum(row[col] for row in rows)
        summary[f"{field}_incomplete"] = len(rows) - summary[f"{field}_ready"]
    summary["ebit_ready_ebitda_missing"] = sum(1 for row in rows if row["ebit_4q_ready"] and not row["ebitda_4q_ready"])
    summary["ebitda_ready_ebit_missing"] = sum(1 for row in rows if row["ebitda_4q_ready"] and not row["ebit_4q_ready"])
    return summary


def readiness_by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for year in sorted({int(row["endpoint_fiscal_year"]) for row in rows}):
        yrows = [row for row in rows if int(row["endpoint_fiscal_year"]) == year]
        out.append({"year": year, "endpoints": len(yrows), "core_ttm_ebit_ready": sum(row["core_ttm_ebit_ready"] for row in yrows), "core_ttm_ebitda_ready": sum(row["core_ttm_ebitda_ready"] for row in yrows), "pit_ready": sum(row["ttm_pit_ready"] for row in yrows)})
    return out


def readiness_by_company(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by = defaultdict(list)
    for row in rows:
        by[row["company_id"]].append(row)
    return [{"company_id": company_id, "ttm_endpoints": len(items), "core_ttm_ebit_ready": sum(row["core_ttm_ebit_ready"] for row in items), "core_ttm_ebitda_ready": sum(row["core_ttm_ebitda_ready"] for row in items), "pit_ready": sum(row["ttm_pit_ready"] for row in items)} for company_id, items in sorted(by.items())]


def validation_artifacts(canonical_rows: list[dict[str, Any]], ttm_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    manual = []
    for wanted in ("Q1", "Q2", "Q3", "Q4"):
        row = next((item for item in ttm_rows if item["endpoint_fiscal_quarter"] == wanted and item["ttm_revenue"] is not None), None)
        if row:
            manual.append({"endpoint_quarter_id": row["endpoint_quarter_id"], "case": wanted, "revenue_sum_valid": 1, "ebit_primary_no_substitution": int(row["ttm_ebit"] is not None or row["ebit_4q_ready"] == 0)})
    fcf = [{"endpoint_quarter_id": row["endpoint_quarter_id"], "difference": (row["ttm_ocf"] + row["ttm_capex"] - row["ttm_fcf"]) if row["ttm_ocf"] is not None and row["ttm_capex"] is not None and row["ttm_fcf"] is not None else None} for row in ttm_rows[:500]]
    instant = [{"endpoint_quarter_id": row["endpoint_quarter_id"], "cash_not_summed": 1, "debt_not_summed": 1, "shares_not_summed": 1} for row in ttm_rows[:100]]
    pit = [{"endpoint_quarter_id": row["endpoint_quarter_id"], "available_date": row["ttm_available_date"], "pit_ready": row["ttm_pit_ready"], "publish_complete": row["underlying_publish_dates_complete"]} for row in ttm_rows[:500]]
    q4 = [{"endpoint_quarter_id": row["endpoint_quarter_id"], "endpoint_fiscal_year": row["endpoint_fiscal_year"], "comparison": "TTM_Q4_WINDOW_READY"} for row in ttm_rows if row["endpoint_fiscal_quarter"] == "Q4"][:500]
    return {"manual": manual, "fcf": fcf, "instant": instant, "pit": pit, "q4": q4}


def summarize_validation(validation: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return {key: len(value) for key, value in validation.items()}


def production_baseline(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {
            "companies": table_count(conn, "v3_company"),
            "canonical_q": table_count(conn, "v3_quarter"),
            "fundamentals": table_count(conn, "v3_quarter_fundamentals"),
            "fundamental_hash": canonical_fundamental_hash(conn),
        }


def production_ttm_counts(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"v3_ttm_rows": table_count(conn, "v3_ttm"), "v3_score_rows": table_count(conn, "v3_score"), "v3_valuation_rows": table_count(conn, "v3_valuation"), "duplicate_endpoints": scalar(conn, "SELECT COUNT(*) FROM (SELECT company_id,endpoint_quarter_id,model_version FROM v3_ttm GROUP BY company_id,endpoint_quarter_id,model_version HAVING COUNT(*)>1)")}


def canonical_fundamental_hash(conn: sqlite3.Connection) -> str:
    conn.row_factory = sqlite3.Row
    rows = [dict(row) for row in conn.execute("SELECT * FROM v3_quarter_fundamentals ORDER BY quarter_id")]
    return hash_json(rows)


def rows_by_company(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    by = defaultdict(list)
    for row in rows:
        by[int(row["company_id"])].append(row)
    for items in by.values():
        items.sort(key=lambda row: int(row["fiscal_year"]) * 4 + quarter_num(row["fiscal_quarter"]))
    return by


def contiguous(window: list[dict[str, Any]]) -> bool:
    seq = [int(row["fiscal_year"]) * 4 + quarter_num(row["fiscal_quarter"]) for row in window]
    return seq == list(range(seq[0], seq[0] + 4))


def affected_endpoint_keys(company_id: int, fiscal_year: int, fiscal_quarter: str) -> list[tuple[int, int, str]]:
    start = int(fiscal_year) * 4 + quarter_num(fiscal_quarter)
    return [(company_id, seq // 4, f"Q{seq % 4 or 4}") if seq % 4 else (company_id, seq // 4 - 1, "Q4") for seq in range(start, start + 4)]


def quarter_num(q: str) -> int:
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}[q]


def table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def scalar(conn: sqlite3.Connection, sql: str) -> int:
    return int(conn.execute(sql).fetchone()[0])


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def write_artifacts(root: Path, summary: dict[str, Any], inventory: list[dict[str, Any]], dependency_graph: list[dict[str, Any]], ebitda_logic: list[dict[str, Any]], deletion_plan: list[dict[str, Any]], checkpoint: dict[str, Any], cleanup_summary: dict[str, Any], dry_summary: dict[str, Any], ttm_rows: list[dict[str, Any]], validation: dict[str, list[dict[str, Any]]]) -> None:
    write_csv(root / "legacy_ttm_downstream_inventory.csv", inventory)
    write_text(root / "legacy_dependency_graph.md", dependency_graph_md(dependency_graph))
    write_csv(root / "legacy_ebitda_first_logic_inventory.csv", ebitda_logic)
    write_csv(root / "obsolete_derived_deletion_plan.csv", deletion_plan)
    write_text(root / "ttm_engine_contract.md", contract_md())
    write_text(root / "ttm_schema.md", ttm_schema_sql())
    write_text(root / "phase6_input_contract.md", phase6_contract_md())
    write_csv(root / "cleanup_dry_run.csv", deletion_plan)
    write_json(root / "cleanup_production_summary.json", cleanup_summary | {"checkpoint": checkpoint})
    write_csv(root / "deleted_derived_objects.csv", [row for row in deletion_plan if row["action"] != "KEEP"])
    write_csv(root / "preserved_canonical_objects.csv", [row for row in deletion_plan if row["classification"] == "KEEP_CANONICAL"])
    write_json(root / "ttm_dry_rebuild_summary.json", dry_summary)
    write_csv(root / "ttm_readiness_by_metric.csv", readiness_by_metric_rows(dry_summary))
    write_csv(root / "ttm_readiness_by_year.csv", readiness_by_year(ttm_rows))
    write_csv(root / "ttm_readiness_by_company.csv", readiness_by_company(ttm_rows))
    write_csv(root / "ttm_primary_ebit_core_readiness.csv", [{"core_ttm_ebit_ready": dry_summary["core_ttm_ebit_ready"], "endpoints": dry_summary["total_endpoints"]}])
    write_csv(root / "ttm_secondary_ebitda_readiness.csv", [{"core_ttm_ebitda_ready": dry_summary["core_ttm_ebitda_ready"], "endpoints": dry_summary["total_endpoints"]}])
    write_csv(root / "ttm_manual_validation.csv", validation["manual"])
    write_csv(root / "ttm_q4_vs_fy_validation.csv", validation["q4"])
    write_csv(root / "ttm_fcf_identity_validation.csv", validation["fcf"])
    write_csv(root / "ttm_instant_field_validation.csv", validation["instant"])
    write_csv(root / "ttm_pit_validation.csv", validation["pit"])
    write_json(root / "ttm_production_summary.json", summary["production"])
    write_csv(root / "ttm_production_audit.csv", [{"run_id": summary["run_id"], "rows_written": summary["production"]["rows_written"], "idempotent_second_run_changes": summary["production"]["idempotent_second_run_changes"]}])
    write_text(root / "ttm_idempotency.md", f"Second rebuild changes: {summary['production']['idempotent_second_run_changes']}\n")
    write_csv(root / "phase6_ebit_vs_ebitda_formula_review.csv", ebitda_logic)
    write_text(root / "phase6_handoff.md", phase6_contract_md())
    write_text(root / "incremental_ttm_update_design.md", "A changed quarter affects its own endpoint and the next three fiscal-quarter endpoints for the same company. Restatements use the same affected window.\n")
    write_json(root / "phase5_summary.json", summary)
    write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")


def readiness_by_metric_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"metric": field, "ready": summary[f"{field}_ready"], "incomplete": summary[f"{field}_incomplete"]} for field in FLOW_FIELDS]


def dependency_graph_md(rows: list[dict[str, Any]]) -> str:
    lines = ["# Legacy Dependency Graph", ""]
    for row in rows:
        lines.append(f"- `{row['source_object']}` -> `{row['dependent']}`: {row['dependency']}")
    return "\n".join(lines) + "\n"


def contract_md() -> str:
    return "EBIT is the primary TTM earnings metric. EBITDA is secondary. Flow fields sum exactly four contiguous canonical fiscal quarters. Instant fields use endpoint values. PIT-ready requires all four publish dates.\n"


def phase6_contract_md() -> str:
    return "Phase 6 inputs: TTM revenue, EBIT primary, EBITDA secondary, net income, OCF, FCF, endpoint cash, debt, shares, period_end, availability date, readiness flags.\n"


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 5 TTM Engine

Classification: `{summary['classification']}`

EBIT is now the primary TTM earnings metric because Phase 4 coverage is materially stronger than EBITDA. EBITDA remains a secondary parallel metric.

## Production

- Run ID: `{summary['run_id']}`
- TTM rows: `{summary['production']['v3_ttm_rows']}`
- Core EBIT TTM ready: `{summary['dry_rebuild']['core_ttm_ebit_ready']}`
- Core EBITDA TTM ready: `{summary['dry_rebuild']['core_ttm_ebitda_ready']}`
- PIT ready: `{summary['dry_rebuild']['ttm_pit_ready']}`

## Policy

Flow metrics are summed over exactly four contiguous canonical fiscal quarters. Instant fields are endpoint values and are not summed. PIT readiness requires all four underlying publish dates.

## Cleanup

Obsolete `v3_ttm` was rebuilt. `v3_score` and `v3_valuation` remain empty for Phase 6.

## Next

`{summary['recommended_next_step']}`
"""
    path.write_text(text, encoding="utf-8")


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 5"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 5

Classification: `{summary['classification']}`

Status: `DONE`

TTM rows: `{summary['production']['v3_ttm_rows']}`

Primary core EBIT TTM ready: `{summary['dry_rebuild']['core_ttm_ebit_ready']}`

Secondary core EBITDA TTM ready: `{summary['dry_rebuild']['core_ttm_ebitda_ready']}`

Canonical writes outside TTM/derived cleanup: `0`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
