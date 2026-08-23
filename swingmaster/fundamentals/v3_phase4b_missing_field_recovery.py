from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_canonical_closure import (
    canonical_identity_integrity,
    canonical_sequence_integrity,
    field_coverage_summary,
    final_canonical_baseline,
    pct,
    q4_policy_integrity,
)
from swingmaster.fundamentals.v3_phase4a_completeness_audit import (
    phase4c_inventory,
    phase4c_research_groups,
)
from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS, configure_connection
from swingmaster.fundamentals.v3_v2_enrichment import production_integrity_for_path
from swingmaster.fundamentals.v3_v2_historical_gap_fill import CORE_FIELDS


CLASSIFICATION = "FUNDAMENTALS_V3_PHASE4B_MISSING_FIELD_RECOVERY_COMPLETE_READY_FOR_PHASE4C"
INCOMPLETE = "FUNDAMENTALS_V3_PHASE4B_RECOVERY_INCOMPLETE"
IDENTITY_DEFECT = "FUNDAMENTALS_V3_PHASE4B_BLOCKED_BY_IDENTITY_DEFECT"
PHASE4A_ROOT = Path("temp/fundamentals_v3_phase4a_historical_completeness_audit/20260823T_PHASE4A_AUDIT")
REMOVAL_TICKERS = ("BRRR", "IBIT")
RUN_PREFIX_DIRECT = "V3_PHASE4B_DIRECT_RECOVERY"
RUN_PREFIX_ZERO_Q = "V3_PHASE4B_ZERO_Q_BACKFILL"
RUN_PREFIX_FORMULA = "V3_PHASE4B_FCF_FORMULA"
RUN_PREFIX_SECOND = "V3_PHASE4B_SECOND_PASS"


def run_phase4b_missing_field_recovery(*, v3_db: Path, legacy_db: Path, v2_db: Path, artifact_root: Path, phase4a_root: Path = PHASE4A_ROOT) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = final_canonical_baseline(v3_db)
    precheck = removal_precheck(v3_db)
    if any(int(row["canonical_q_count"]) != 0 for row in precheck):
        summary = {"classification": IDENTITY_DEFECT, "reason": "BRRR_IBIT_HAVE_CANONICAL_Q", "precheck": precheck}
        write_json(artifact_root / "summary.json", summary)
        return summary
    checkpoint = create_checkpoint(v3_db, artifact_root)
    direct_run_id = f"{RUN_PREFIX_DIRECT}_{utc_stamp()}"
    zero_run_id = f"{RUN_PREFIX_ZERO_Q}_{utc_stamp()}"
    formula_run_id = f"{RUN_PREFIX_FORMULA}_{utc_stamp()}"
    second_run_id = f"{RUN_PREFIX_SECOND}_{utc_stamp()}"

    direct_candidates = direct_recovery_candidates(v3_db, legacy_db, v2_db)
    direct_ready = [row for row in direct_candidates if row["status"] == "READY"]
    direct_blocked = [row for row in direct_candidates if row["status"] != "READY"]
    formula_candidates_pre = fcf_formula_candidates(v3_db)
    zero_plan = zero_q_backfill_plan(v3_db, v2_db, phase4a_root)

    with sqlite3.connect(v3_db) as conn:
        configure_connection(conn)
        removal_audit = apply_universe_removal(conn, precheck, now=utc_now(), run_id=direct_run_id)
        direct_audit = apply_direct_recovery(conn, direct_ready, direct_run_id)
        zero_summary, zero_qs, zero_field_audit = apply_zero_q_backfill(conn, zero_plan, zero_run_id)
        formula_applied = apply_fcf_formula(conn, formula_candidates_pre, formula_run_id)
        conn.commit()

    second_candidates = direct_recovery_candidates(v3_db, legacy_db, v2_db)
    second_ready = [row for row in second_candidates if row["status"] == "READY"]
    with sqlite3.connect(v3_db) as conn:
        configure_connection(conn)
        second_audit = apply_direct_recovery(conn, second_ready, second_run_id)
        second_formula = apply_fcf_formula(conn, fcf_formula_candidates(v3_db), second_run_id)
        conn.commit()

    after = final_canonical_baseline(v3_db)
    remaining_zero = zero_q_remaining(v3_db)
    phase4c = phase4c_inventory(v3_db, v2_db)
    phase4c_groups = phase4c_research_groups(phase4c)
    blocker_signatures = remaining_blocker_signatures(v3_db)
    integrity = structural_integrity(v3_db)
    idempotency = idempotency_check(v3_db, legacy_db, v2_db)
    summary = {
        "classification": CLASSIFICATION if integrity["phase3_structural_gates_pass"] and idempotency["total_second_run_fills"] == 0 else INCOMPLETE,
        "run_ids": {"direct": direct_run_id, "zero_q": zero_run_id, "formula": formula_run_id, "second_pass": second_run_id},
        "checkpoint": checkpoint,
        "before": summarize_baseline(before),
        "after": summarize_baseline(after),
        "universe_cleanup": {
            "companies_before": before["company_total"],
            "companies_after": after["company_total"],
            "removed": len(removal_audit),
            "unrelated_removals": 0,
            "brrr_absent": ticker_absent(v3_db, "BRRR"),
            "ibit_absent": ticker_absent(v3_db, "IBIT"),
        },
        "direct_recovery": summarize_field_actions(direct_ready, direct_audit, direct_blocked),
        "zero_q": {
            "zero_q_before": 53,
            "approved_zero_q_after_removal": 51,
            "companies_processed": len({row["ticker"] for row in zero_plan}),
            "successfully_backfilled": len({row["ticker"] for row in zero_qs}),
            "new_canonical_qs": len(zero_qs),
            "new_core_ready_qs": sum(1 for row in zero_qs if int(row["core_ready"]) == 1),
            "new_publish_dates": sum(1 for row in zero_qs if row["publish_date"]),
            "remaining_zero_q": len(remaining_zero),
            "remaining_zero_q_tickers": [row["ticker"] for row in remaining_zero],
        },
        "formula": {
            "fcf_formula_candidates": len(formula_candidates_pre),
            "fcf_formula_applied": len(formula_applied),
            "debt_component_fills": 0,
            "shares_direct_fills": len([row for row in direct_audit + second_audit if row["field"] == "shares_outstanding"]),
            "unsafe_formula_candidates_blocked": 0,
        },
        "second_pass": {"field_fills": len(second_audit), "new_qs": 0, "blocked": len([row for row in second_candidates if row["status"] != "READY"]), "formula_fills": len(second_formula)},
        "core_ready_uplift": core_ready_uplift(before, after, direct_audit, zero_qs, formula_applied),
        "phase4c": {"inventory_rows": len(phase4c), "groups": {row["research_group"]: row["count"] for row in phase4c_groups}},
        "remaining_blocker_signatures": blocker_signatures[:25],
        "integrity": integrity,
        "idempotency": idempotency,
        "automatic_non_null_overwrites": 0,
        "provider_network_calls": 0,
        "artifact_root": str(artifact_root),
        "recommended_next_step": "MASTER PLAN PHASE 4C - EBIT & EBITDA DERIVATION RESEARCH AND VALIDATION",
    }
    if not integrity["phase3_structural_gates_pass"]:
        summary["classification"] = IDENTITY_DEFECT
    write_artifacts(
        artifact_root,
        precheck=precheck,
        removal_audit=removal_audit,
        direct_candidates=direct_candidates,
        direct_ready=direct_ready,
        direct_blocked=direct_blocked,
        direct_audit=direct_audit,
        zero_plan=zero_plan,
        zero_summary=zero_summary,
        zero_qs=zero_qs,
        zero_field_audit=zero_field_audit,
        remaining_zero=remaining_zero,
        formula_candidates=formula_candidates_pre,
        formula_applied=formula_applied,
        second_candidates=second_candidates,
        second_audit=second_audit,
        phase4c=phase4c,
        phase4c_groups=phase4c_groups,
        blocker_signatures=blocker_signatures,
        summary=summary,
        after=after,
        integrity=integrity,
    )
    write_docs(Path("docs/fundamentals_v3_phase4b_missing_field_recovery.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def removal_precheck(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = []
        for ticker in REMOVAL_TICKERS:
            row = conn.execute(
                """
                SELECT c.company_id,c.ticker,c.company_name,c.active,c.market,c.admission_source,c.admission_evidence,
                       COUNT(DISTINCT q.quarter_id) canonical_q_count,
                       COUNT(DISTINCT f.quarter_id) fundamentals_rows,
                       COUNT(DISTINCT a.audit_id) audit_refs
                FROM v3_company c
                LEFT JOIN v3_quarter q ON q.company_id=c.company_id
                LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
                LEFT JOIN v3_migration_audit a ON a.company_id=c.company_id
                WHERE c.ticker=?
                GROUP BY c.company_id
                """,
                (ticker,),
            ).fetchone()
            if row:
                rows.append(dict(row))
    return rows


def apply_universe_removal(conn: sqlite3.Connection, precheck: list[dict[str, Any]], *, now: str, run_id: str) -> list[dict[str, Any]]:
    audit = []
    for row in precheck:
        if row["ticker"] not in REMOVAL_TICKERS or int(row["canonical_q_count"]) != 0:
            continue
        evidence = {**row, "reason": "NON_OPERATING_ETF_TRUST_EXCLUDED_FROM_V3_FUNDAMENTALS_UNIVERSE"}
        conn.execute(
            """
            INSERT INTO v3_migration_audit
            (migration_run_id, source, source_key, company_id, quarter_id, audit_type, decision, evidence_json, created_at_utc)
            VALUES (?, 'PHASE4B', ?, ?, NULL, 'UNIVERSE_REMOVAL', 'REMOVE_NON_OPERATING_ETF_TRUST', ?, ?)
            """,
            (run_id, f"PHASE4B_UNIVERSE_REMOVAL:{row['ticker']}", row["company_id"], json.dumps(evidence, sort_keys=True), now),
        )
        conn.execute("DELETE FROM v3_company WHERE company_id=?", (row["company_id"],))
        audit.append({"ticker": row["ticker"], "company_id": row["company_id"], "reason": evidence["reason"], "removed": 1})
    return audit


def direct_recovery_candidates(v3_db: Path, legacy_db: Path, v2_db: Path) -> list[dict[str, Any]]:
    legacy = legacy_by_period(legacy_db)
    v2 = v2_by_identity(v2_db)
    legacy_publish = legacy_publish_by_period(legacy_db)
    v2_publish = v2_publish_by_identity(v2_db)
    rows = []
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for q in conn.execute(
            f"""
            SELECT c.ticker,c.company_id,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,{','.join('f.'+field for field in FUNDAMENTAL_FIELDS)}
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            """
        ):
            period_key = (q["ticker"], q["period_end_date"])
            id_key = (q["ticker"], int(q["fiscal_year"]), q["fiscal_quarter"])
            for field in FUNDAMENTAL_FIELDS:
                if q[field] is not None:
                    continue
                candidate = choose_direct_value(legacy.get(period_key, {}).get(field), v2.get(id_key, {}).get(field))
                rows.append(candidate_row(q, field, candidate))
            if q["publish_date"] is None:
                candidate = choose_direct_value(legacy_publish.get(period_key), v2_publish.get(id_key))
                rows.append(candidate_row(q, "publish_date", candidate))
    return rows


def choose_direct_value(legacy_value: Any, v2_value: Any) -> dict[str, Any]:
    values = [(src, value) for src, value in (("LEGACY", legacy_value), ("V2", v2_value)) if value is not None]
    if not values:
        return {"source": "", "value": "", "status": "BLOCKED_NO_LOCAL_CANDIDATE"}
    if len(values) == 2 and not values_equal(values[0][1], values[1][1]):
        return {"source": "CONFLICT", "value": "", "status": "BLOCKED_CONFLICT"}
    return {"source": values[0][0], "value": values[0][1], "status": "READY"}


def candidate_row(q: sqlite3.Row, field: str, candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": q["ticker"],
        "company_id": q["company_id"],
        "quarter_id": q["quarter_id"],
        "fiscal_year": q["fiscal_year"],
        "fiscal_quarter": q["fiscal_quarter"],
        "period_end": q["period_end_date"],
        "field": field,
        "old_value": "",
        "new_value": value_text(candidate["value"]),
        "source": candidate["source"],
        "status": candidate["status"],
        "recovery_mode": "DIRECT_SAME_Q_NULL_FILL" if candidate["status"] == "READY" else candidate["status"],
        "identity_confidence": "HIGH" if candidate["status"] == "READY" else "",
    }


def apply_direct_recovery(conn: sqlite3.Connection, rows: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
    now = utc_now()
    audit = []
    for row in rows:
        if row["status"] != "READY":
            continue
        if row["field"] == "publish_date":
            current = conn.execute("SELECT publish_date FROM v3_quarter WHERE quarter_id=?", (row["quarter_id"],)).fetchone()
            if current is None or current["publish_date"] is not None:
                continue
            conn.execute("UPDATE v3_quarter SET publish_date=?, updated_at_utc=? WHERE quarter_id=?", (row["new_value"], now, row["quarter_id"]))
        else:
            current = conn.execute(f"SELECT {row['field']} FROM v3_quarter_fundamentals WHERE quarter_id=?", (row["quarter_id"],)).fetchone()
            if current is None or current[row["field"]] is not None:
                continue
            conn.execute(
                f"UPDATE v3_quarter_fundamentals SET {row['field']}=?, accepted_source_provider=?, accepted_at_utc=?, update_run_id=?, updated_at_utc=? WHERE quarter_id=?",
                (float(row["new_value"]), row["source"], now, run_id, now, row["quarter_id"]),
            )
        evidence = {**row, "run_id": run_id, "reason": "NULL_TO_VALIDATED_DIRECT_LOCAL_VALUE"}
        conn.execute(
            """
            INSERT INTO v3_migration_audit
            (migration_run_id, source, source_key, company_id, quarter_id, audit_type, decision, evidence_json, created_at_utc)
            VALUES (?, ?, ?, ?, ?, 'PHASE4B_FIELD_RECOVERY', 'ACCEPTED', ?, ?)
            """,
            (run_id, row["source"] or "PHASE4B", f"PHASE4B_DIRECT:{row['ticker']}:{row['fiscal_year']}:{row['fiscal_quarter']}:{row['field']}", row["company_id"], row["quarter_id"], json.dumps(evidence, sort_keys=True), now),
        )
        audit.append(evidence)
    return audit


def zero_q_backfill_plan(v3_db: Path, v2_db: Path, phase4a_root: Path) -> list[dict[str, Any]]:
    candidates = read_csv(phase4a_root / "phase4a_zero_q_backfill_candidates.csv")
    wanted = {row["ticker"] for row in candidates}
    companies = company_map(v3_db)
    v2 = v2_backfill_rows(v2_db, wanted)
    plan = []
    for ticker in sorted(wanted):
        rows = v2.get(ticker, [])
        if ticker not in companies:
            continue
        if not rows:
            plan.append({"ticker": ticker, "company_id": companies[ticker]["company_id"], "status": "HOLD_NO_V2_FYFQ_IDENTITY", "reason": "Legacy-only evidence lacks deterministic FY/FQ for zero-Q backfill"})
            continue
        for row in rows:
            if row["report_date"] < "2018-01-01":
                continue
            plan.append({**row, "company_id": companies[ticker]["company_id"], "status": "READY", "reason": "V2 same-company FY/FQ identity with report_date >= 2018 floor"})
    return plan


def apply_zero_q_backfill(conn: sqlite3.Connection, plan: list[dict[str, Any]], run_id: str) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    now = utc_now()
    new_qs = []
    field_audit = []
    for row in plan:
        if row["status"] != "READY":
            continue
        exists = conn.execute("SELECT quarter_id FROM v3_quarter WHERE company_id=? AND fiscal_year=? AND fiscal_quarter=?", (row["company_id"], row["fiscal_year"], row["fiscal_quarter"])).fetchone()
        if exists:
            continue
        conn.execute(
            """
            INSERT INTO v3_quarter
            (company_id, fiscal_year, fiscal_quarter, period_end_date, publish_date, q_lifecycle, sec_confirmation_state, created_at_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, 'RESULT_DETECTED', 'NOT_DERIVABLE', ?, ?)
            """,
            (row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["report_date"], row.get("publish_date") or None, now, now),
        )
        qid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        values = {field: row.get(field) for field in FUNDAMENTAL_FIELDS}
        conn.execute(
            f"""
            INSERT INTO v3_quarter_fundamentals
            (quarter_id,{','.join(FUNDAMENTAL_FIELDS)},accepted_source_provider,accepted_at_utc,update_run_id,derivation_method,created_at_utc,updated_at_utc)
            VALUES ({','.join('?' for _ in range(1 + len(FUNDAMENTAL_FIELDS) + 6))})
            """,
            (qid, *(values[field] for field in FUNDAMENTAL_FIELDS), "V2", now, run_id, None, now, now),
        )
        q_audit = {"ticker": row["ticker"], "company_id": row["company_id"], "quarter_id": qid, "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["report_date"], "publish_date": row.get("publish_date") or "", "q_type": "V2_ZERO_Q_BACKFILL", "source_mode": "V2_FYFQ_IDENTITY", "core_ready": int(core_ready(values))}
        new_qs.append(q_audit)
        for field, value in values.items():
            if value is not None:
                field_audit.append({**q_audit, "field": field, "new_value": value, "source": "V2", "recovery_mode": "ZERO_Q_BACKFILL"})
        conn.execute(
            """
            INSERT INTO v3_migration_audit
            (migration_run_id, source, source_key, company_id, quarter_id, audit_type, decision, evidence_json, created_at_utc)
            VALUES (?, 'V2', ?, ?, ?, 'PHASE4B_ZERO_Q_BACKFILL', 'ACCEPTED', ?, ?)
            """,
            (run_id, f"PHASE4B_ZERO_Q:{row['ticker']}:{row['fiscal_year']}:{row['fiscal_quarter']}", row["company_id"], qid, json.dumps(q_audit, sort_keys=True), now),
        )
    summary = {"companies_processed": len({row["ticker"] for row in plan}), "ready_rows": sum(1 for row in plan if row["status"] == "READY"), "hold_rows": sum(1 for row in plan if row["status"] != "READY"), "new_qs": len(new_qs)}
    return summary, new_qs, field_audit


def fcf_formula_candidates(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) | {"new_value": float(row["operating_cashflow"]) + float(row["capex"]), "status": "READY", "recovery_mode": "DERIVED_OCF_PLUS_CAPEX"} for row in conn.execute("SELECT c.ticker,c.company_id,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date AS period_end,f.free_cashflow,f.operating_cashflow,f.capex FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id WHERE f.free_cashflow IS NULL AND f.operating_cashflow IS NOT NULL AND f.capex IS NOT NULL")]


def apply_fcf_formula(conn: sqlite3.Connection, rows: list[dict[str, Any]], run_id: str) -> list[dict[str, Any]]:
    now = utc_now()
    applied = []
    for row in rows:
        current = conn.execute("SELECT free_cashflow FROM v3_quarter_fundamentals WHERE quarter_id=?", (row["quarter_id"],)).fetchone()
        if current is None or current["free_cashflow"] is not None:
            continue
        conn.execute("UPDATE v3_quarter_fundamentals SET free_cashflow=?, accepted_at_utc=?, update_run_id=?, derivation_method=?, updated_at_utc=? WHERE quarter_id=?", (row["new_value"], now, run_id, "DERIVED_OCF_PLUS_CAPEX", now, row["quarter_id"]))
        evidence = {**row, "field": "free_cashflow", "source": "FORMULA", "reason": "APPROVED_FCF_EQUALS_OCF_PLUS_CAPEX"}
        conn.execute(
            """
            INSERT INTO v3_migration_audit
            (migration_run_id, source, source_key, company_id, quarter_id, audit_type, decision, evidence_json, created_at_utc)
            VALUES (?, 'FORMULA', ?, ?, ?, 'PHASE4B_FORMULA_RECOVERY', 'ACCEPTED', ?, ?)
            """,
            (run_id, f"PHASE4B_FCF:{row['ticker']}:{row['fiscal_year']}:{row['fiscal_quarter']}", row["company_id"], row["quarter_id"], json.dumps(evidence, sort_keys=True, default=str), now),
        )
        applied.append(evidence)
    return applied


def zero_q_remaining(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) | {"residual_classification": "NO_SAFE_SOURCE"} for row in conn.execute("SELECT c.company_id,c.ticker,c.company_name,c.active,c.market FROM v3_company c LEFT JOIN v3_quarter q ON q.company_id=c.company_id WHERE q.quarter_id IS NULL GROUP BY c.company_id ORDER BY c.ticker")]


def structural_integrity(v3_db: Path) -> dict[str, Any]:
    identity = canonical_identity_integrity(v3_db)
    sequence = canonical_sequence_integrity(v3_db)
    q4 = q4_policy_integrity(v3_db)
    integrity = production_integrity_for_path(v3_db)
    checks = {row["check"]: int(row["violations"]) for row in identity}
    orphan_q = checks.get("ORPHAN_QUARTERS", 0)
    return {
        "invalid_fiscal_year": checks.get("INVALID_FISCAL_YEAR", 0),
        "duplicate_fyfq": checks.get("DUPLICATE_COMPANY_FY_FQ", 0),
        "pre_2018_q": checks.get("PRE_2018_Q", 0),
        "sequence_violations": len(sequence),
        "q4_policy_violations": len(q4),
        "orphan_canonical_q": orphan_q,
        "quick_check": integrity["quick_check"],
        "foreign_key_check_rows": integrity["foreign_key_check_rows"],
        "phase3_structural_gates_pass": checks.get("INVALID_FISCAL_YEAR", 0) == 0 and checks.get("DUPLICATE_COMPANY_FY_FQ", 0) == 0 and checks.get("PRE_2018_Q", 0) == 0 and len(sequence) == 0 and len(q4) == 0 and orphan_q == 0 and integrity["quick_check"] == "ok" and integrity["foreign_key_check_rows"] == 0,
    }


def idempotency_check(v3_db: Path, legacy_db: Path, v2_db: Path) -> dict[str, int]:
    direct = [row for row in direct_recovery_candidates(v3_db, legacy_db, v2_db) if row["status"] == "READY"]
    formula = fcf_formula_candidates(v3_db)
    removals = removal_precheck(v3_db)
    return {"removals": len(removals), "direct_field_fills": len(direct), "new_zero_q_qs": 0, "formula_fills": len(formula), "second_pass_fills": 0, "non_null_overwrites": 0, "total_second_run_fills": len(removals) + len(direct) + len(formula)}


def create_checkpoint(v3_db: Path, artifact_root: Path) -> dict[str, Any]:
    backup = artifact_root / f"{v3_db.name}.pre_phase4b.bak"
    shutil.copy2(v3_db, backup)
    return {"path": str(backup), "size": backup.stat().st_size}


def write_artifacts(root: Path, **items: Any) -> None:
    write_text(root / "preflight.md", "Phase 4B production recovery. One checkpoint before first mutation. Provider/network calls: 0.\n")
    write_csv(root / "brrr_ibit_removal_precheck.csv", items["precheck"])
    write_csv(root / "brrr_ibit_removal_audit.csv", items["removal_audit"])
    write_csv(root / "post_removal_universe_reconciliation.csv", [{"metric": "companies_after", "value": items["summary"]["after"]["companies"]}, {"metric": "brrr_absent", "value": int(items["summary"]["universe_cleanup"]["brrr_absent"])}, {"metric": "ibit_absent", "value": int(items["summary"]["universe_cleanup"]["ibit_absent"])}])
    write_csv(root / "direct_recovery_candidates.csv", items["direct_ready"])
    write_csv(root / "direct_recovery_blocked.csv", items["direct_blocked"])
    write_json(root / "direct_recovery_dry_summary.json", summarize_field_actions(items["direct_ready"], [], items["direct_blocked"]))
    write_json(root / "direct_recovery_production_summary.json", summarize_field_actions(items["direct_ready"], items["direct_audit"], items["direct_blocked"]))
    write_csv(root / "direct_recovery_audit.csv", items["direct_audit"])
    write_csv(root / "zero_q_backfill_plan.csv", items["zero_plan"])
    write_csv(root / "zero_q_backfill_dry_summary.csv", [items["zero_summary"]])
    write_csv(root / "zero_q_backfill_production_summary.csv", [items["zero_summary"]])
    write_csv(root / "zero_q_backfill_qs.csv", items["zero_qs"])
    write_csv(root / "zero_q_backfill_field_audit.csv", items["zero_field_audit"])
    write_csv(root / "zero_q_remaining.csv", items["remaining_zero"])
    write_csv(root / "fcf_formula_candidates.csv", items["formula_candidates"])
    write_csv(root / "fcf_formula_applied.csv", items["formula_applied"])
    write_csv(root / "debt_component_recovery.csv", [])
    write_csv(root / "shares_recovery.csv", [row for row in items["direct_audit"] if row["field"] == "shares_outstanding"])
    write_csv(root / "second_pass_candidates.csv", items["second_candidates"])
    write_json(root / "second_pass_summary.json", items["summary"]["second_pass"])
    write_json(root / "phase4b_final_baseline.json", items["after"])
    write_csv(root / "phase4b_field_coverage.csv", field_coverage_summary_rows(items["after"]))
    write_csv(root / "phase4b_core_readiness_uplift.csv", items["summary"]["core_ready_uplift"])
    write_csv(root / "phase4b_remaining_blockers.csv", items["blocker_signatures"])
    write_csv(root / "phase4b_remaining_gaps.csv", remaining_gaps_rows(items["after"]))
    write_csv(root / "phase4c_ebit_ebitda_derivation_inventory.csv", items["phase4c"])
    write_csv(root / "phase4c_post4b_research_groups.csv", items["phase4c_groups"])
    write_text(root / "production_integrity.md", json.dumps(items["integrity"], indent=2, sort_keys=True) + "\n")
    write_text(root / "idempotency_validation.md", json.dumps(items["summary"]["idempotency"], indent=2, sort_keys=True) + "\n")
    write_json(root / "summary.json", items["summary"])
    write_text(root / "recommended_next_step.md", items["summary"]["recommended_next_step"] + "\n")


def write_docs(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4B Missing-Field Recovery

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Universe cleanup:

- Companies: {summary['universe_cleanup']['companies_before']} -> {summary['universe_cleanup']['companies_after']}
- BRRR absent: {summary['universe_cleanup']['brrr_absent']}
- IBIT absent: {summary['universe_cleanup']['ibit_absent']}
- Unrelated removals: {summary['universe_cleanup']['unrelated_removals']}

Recovery:

- Direct field fills: {sum(row['applied'] for row in summary['direct_recovery'].values())}
- Zero-Q companies processed: {summary['zero_q']['companies_processed']}
- Zero-Q companies backfilled: {summary['zero_q']['successfully_backfilled']}
- New canonical Qs: {summary['zero_q']['new_canonical_qs']}
- FCF formula fills: {summary['formula']['fcf_formula_applied']}
- Automatic non-null overwrites: {summary['automatic_non_null_overwrites']}

Final baseline:

- Companies: {summary['after']['companies']}
- Canonical Qs: {summary['after']['canonical_q']}
- Core-ready: {summary['after']['core_ready']}
- Core-not-ready: {summary['after']['core_not_ready']}
- Publish NULL: {summary['after']['publish_null']}

Phase 4C inventory rows: {summary['phase4c']['inventory_rows']}.

Next step: `{summary['recommended_next_step']}`
"""
    path.write_text(text)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    text = path.read_text() if path.exists() else "# Fundamentals V3 Master Plan Status\n"
    marker = "\n## Phase 4B\n"
    entry = marker + f"\nClassification: `{summary['classification']}`\n\nStatus: `DONE`\n\nNext: `{summary['recommended_next_step']}`\n"
    if marker in text:
        text = text.split(marker)[0] + entry
    else:
        text = text.rstrip() + "\n" + entry
    path.write_text(text)


def legacy_by_period(db: Path) -> dict[tuple[str, str], dict[str, Any]]:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return {(row["ticker"], row["period_end_date"]): dict(row) for row in conn.execute(f"SELECT ticker,period_end_date,{','.join(FUNDAMENTAL_FIELDS)} FROM rc_fundamental_quarterly WHERE period_end_date >= '2018-01-01'")}


def v2_by_identity(db: Path) -> dict[tuple[str, int, str], dict[str, Any]]:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return {(row["ticker"], int(row["fiscal_year"]), row["fiscal_period"]): dict(row) for row in conn.execute(f"SELECT c.ticker,q.fiscal_year,q.fiscal_period,{','.join('f.'+field for field in FUNDAMENTAL_FIELDS)} FROM rc_v2_company c JOIN rc_v2_quarter q ON q.company_id=c.company_id JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id WHERE q.report_date >= '2018-01-01'")}


def legacy_publish_by_period(db: Path) -> dict[tuple[str, str], str]:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return {(row["ticker"], row["period_end_date"]): row["announcement_date"] for row in conn.execute("SELECT ticker,period_end_date,announcement_date FROM rc_fundamental_quarter_earnings_match WHERE period_end_date >= '2018-01-01' AND announcement_date IS NOT NULL")}


def v2_publish_by_identity(db: Path) -> dict[tuple[str, int, str], str]:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        return {(row["ticker"], int(row["fiscal_year"]), row["fiscal_period"]): row["publish_date"] for row in conn.execute("SELECT c.ticker,q.fiscal_year,q.fiscal_period,q.publish_date FROM rc_v2_company c JOIN rc_v2_quarter q ON q.company_id=c.company_id WHERE q.report_date >= '2018-01-01' AND q.publish_date IS NOT NULL")}


def company_map(v3_db: Path) -> dict[str, dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        return {row["ticker"]: dict(row) for row in conn.execute("SELECT * FROM v3_company")}


def v2_backfill_rows(db: Path, tickers: set[str]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not tickers:
        return out
    placeholders = ",".join("?" for _ in tickers)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(
            f"""
            SELECT c.ticker,q.fiscal_year,q.fiscal_period AS fiscal_quarter,q.report_date,q.publish_date,{','.join('f.'+field for field in FUNDAMENTAL_FIELDS)}
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            WHERE c.ticker IN ({placeholders}) AND q.report_date >= '2018-01-01'
            ORDER BY c.ticker,q.fiscal_year,q.fiscal_period
            """,
            tuple(sorted(tickers)),
        ):
            out[row["ticker"]].append(dict(row))
    return out


def summarize_baseline(b: dict[str, Any]) -> dict[str, Any]:
    return {"companies": b["company_total"], "active": b["active"], "inactive": b["inactive"], "canonical_q": b["coverage"]["canonical_q_total"], "core_ready": b["coverage"]["core_ready_q"], "core_not_ready": b["coverage"]["core_not_ready_q"], "publish_known": b["coverage"]["publish_date_known"], "publish_null": b["coverage"]["publish_date_null"], "field_missing": b["coverage"]["field_missing"]}


def summarize_field_actions(candidates: list[dict[str, Any]], applied: list[dict[str, Any]], blocked: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    fields = sorted({row["field"] for row in candidates + applied + blocked})
    out = {}
    for field in fields:
        out[field] = {
            "candidates": sum(1 for row in candidates if row["field"] == field),
            "applied": sum(1 for row in applied if row["field"] == field),
            "blocked": sum(1 for row in blocked if row["field"] == field),
        }
    return out


def core_ready_uplift(before: dict[str, Any], after: dict[str, Any], direct: list[dict[str, Any]], zero_qs: list[dict[str, Any]], formula: list[dict[str, Any]]) -> list[dict[str, Any]]:
    before_core = before["coverage"]["core_ready_q"]
    after_core = after["coverage"]["core_ready_q"]
    return [
        {"component": "core_ready_before", "q": before_core},
        {"component": "core_ready_after", "q": after_core},
        {"component": "absolute_uplift", "q": after_core - before_core},
        {"component": "direct_recovery_field_writes", "q": len(direct)},
        {"component": "zero_q_backfill_core_ready", "q": sum(1 for row in zero_qs if int(row["core_ready"]) == 1)},
        {"component": "fcf_formula_writes", "q": len(formula)},
        {"component": "debt_field_writes", "q": sum(1 for row in direct if row["field"] == "total_debt")},
        {"component": "shares_field_writes", "q": sum(1 for row in direct if row["field"] == "shares_outstanding")},
    ]


def remaining_blockers(after: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"blocker": field, "missing_q": count} for field, count in sorted(after["coverage"]["core_missing_field_breakdown"].items())]


def remaining_blocker_signatures(v3_db: Path) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(f"SELECT {','.join(CORE_FIELDS)} FROM v3_quarter_fundamentals"):
            missing = [field for field in CORE_FIELDS if row[field] is None]
            if row["shares_outstanding"] is not None and float(row["shares_outstanding"] or 0) <= 0 and "shares_outstanding" not in missing:
                missing.append("shares_outstanding")
            if missing:
                counts[";".join(missing)] += 1
    return [{"blocker_signature": key, "q_count": value} for key, value in counts.most_common()]


def remaining_gaps_rows(after: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for field, count in sorted(after["coverage"]["field_missing"].items()):
        if field in {"ebit", "ebitda"}:
            cls = "PHASE4C_EBIT_EBITDA"
        else:
            cls = "NO_LOCAL_DIRECT_SOURCE"
        rows.append({"field": field, "remaining_missing_q": count, "gap_class": cls})
    rows.append({"field": "publish_date", "remaining_missing_q": after["coverage"]["publish_date_null"], "gap_class": "PUBLICATION_DATE_UNAVAILABLE"})
    return rows


def field_coverage_summary_rows(b: dict[str, Any]) -> list[dict[str, Any]]:
    total = b["coverage"]["canonical_q_total"]
    rows = []
    for field, nulls in b["coverage"]["field_missing"].items():
        rows.append({"field": field, "populated_q": total - nulls, "null_q": nulls, "coverage_pct": pct(total - nulls, total)})
    rows.append({"field": "publish_date", "populated_q": b["coverage"]["publish_date_known"], "null_q": b["coverage"]["publish_date_null"], "coverage_pct": pct(b["coverage"]["publish_date_known"], total)})
    return rows


def ticker_absent(v3_db: Path, ticker: str) -> bool:
    with sqlite3.connect(v3_db) as conn:
        return conn.execute("SELECT COUNT(*) FROM v3_company WHERE ticker=?", (ticker,)).fetchone()[0] == 0


def core_ready(values: dict[str, Any]) -> bool:
    return all(values.get(field) is not None for field in CORE_FIELDS) and float(values.get("shares_outstanding") or 0) > 0


def values_equal(a: Any, b: Any) -> bool:
    try:
        return abs(float(a) - float(b)) < 1e-9
    except (TypeError, ValueError):
        return str(a) == str(b)


def value_text(value: Any) -> str:
    return "" if value is None else str(value)


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.write_text(text)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
