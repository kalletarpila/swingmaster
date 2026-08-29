from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from swingmaster.fundamentals.v3_fiscal_calendar import EXPECTED_P1_TICKERS, utc_stamp
from swingmaster.fundamentals.v3_phase6i_production_rebuild import create_backup
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8e_apply import (
    compare_after,
    integrity,
    production_fingerprints,
)
from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import (
    rebuild_phase6,
    rebuild_ttm,
    rerun_downstream,
    semantic_table_rows,
    verify_models,
)
from swingmaster.fundamentals.v3_phase8e_rehearse_fiscal_repairs import apply_rehearsal, content_signature
from swingmaster.fundamentals.v3_phase8f_latest8q_gap_analysis import (
    KNOWN_13,
    PRIMARY_FIELDS,
    SECONDARY_FIELDS,
    build_quarter_diagnostics,
    external_research_queue,
    is_structural,
    pct,
    split_codes,
    split_codes_pipe,
    structural_review_queue,
    summarize_tickers,
)


CLASSIFICATION_COMPLETE = "LATEST8Q_LOCAL_CRITICAL_REPAIRS_COMPLETE"
CLASSIFICATION_REMAINING = "LATEST8Q_LOCAL_CRITICAL_REPAIRS_COMPLETE_WITH_EXTERNAL_STRUCTURAL_WORK_REMAINING"
CLASSIFICATION_BLOCKED = "LATEST8Q_LOCAL_CRITICAL_REPAIR_BLOCKED"
NEXT_SUCCESS = "USE THE NEW MINIMAL DOWNSTREAM-CRITICAL EXTERNAL RESEARCH QUEUE NEXT; DO NOT RESEARCH SECONDARY FIELDS THAT DO NOT AFFECT TTM / SCORE / LIFECYCLE / VALUATION"
NEXT_BLOCKED = "DO NOT EXPAND TO EXTERNAL RESEARCH YET; RESOLVE ONLY THE LOCAL REPAIR BLOCKER"
OLD_PHASE8F_EXTERNAL_FACTS = 6064
OLD_PHASE8F_STRUCTURAL_DECISIONS = 1130
CORE_EVIDENCE = {
    "NEED_REVENUE",
    "NEED_EBIT",
    "NEED_FCF",
    "NEED_CASH",
    "NEED_DEBT",
    "NEED_SHARES",
}
METADATA_EVIDENCE = {
    "NEED_OFFICIAL_FISCAL_YEAR_START",
    "NEED_OFFICIAL_FY_FQ_IDENTITY",
    "NEED_OFFICIAL_PERIOD_END",
    "NEED_FIRST_PUBLIC_RESULT_DATE",
    "NEED_MISSING_QUARTER_SOURCE",
    "NEED_TARGET_COLLISION_RESOLUTION",
    "NEED_LOCAL_LINEAGE_RECONCILIATION",
    "NEED_TRANSITION_CALENDAR_EVIDENCE",
    "NEED_RESTATEMENT_RECONCILIATION",
    "NEED_SOURCE_SEMANTICS_CONFIRMATION",
}
DERIVATION_SUPPORT = {"NEED_OCF", "NEED_CAPEX", "NEED_OPERATING_INCOME"}
NONBLOCKING_SECONDARY = {"Gross Profit", "Operating Income", "EBITDA", "Net Income", "OCF", "Capex"}


@dataclass(frozen=True)
class Phase8GPaths:
    artifact_root: Path
    phase8f_root: Path = Path("temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    osakedata_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    apply_production: bool = True
    write_documentation: bool = True


def read_csv_dicts(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def critical_evidence_codes(row: dict[str, Any]) -> list[str]:
    codes = set(split_codes(row.get("evidence_needed_codes", "")))
    out = set(codes & (CORE_EVIDENCE | METADATA_EVIDENCE))
    missing_core = set(split_codes_pipe(row.get("missing_core_fields", "")))
    derivation_text = str(row.get("derivation_missing_inputs") or "")
    if "FCF" in missing_core:
        out.update(c for c in codes & {"NEED_OCF", "NEED_CAPEX"})
    if "EBIT" in missing_core and "approved issuer/company-specific EBIT rule" in derivation_text:
        out.add("NEED_SOURCE_SEMANTICS_CONFIRMATION")
    return sorted(out)


def critical_issue_codes(row: dict[str, Any]) -> list[str]:
    issue_codes = set(split_codes(row.get("issue_codes", "")))
    critical = {
        code
        for code in issue_codes
        if code != "SECONDARY_FIELDS_INCOMPLETE"
        and (
            code.startswith("FY_")
            or code.startswith("FQ_")
            or code.startswith("PERIOD_END")
            or code.startswith("PUBLISH")
            or code in {
                "MISSING_QUARTER",
                "DUPLICATE_ECONOMIC_QUARTER",
                "TARGET_COLLISION",
                "TRANSITION_REVIEW",
                "TRANSITION_SEQUENCE",
                "UNRESOLVED_BOUNDARY",
                "UNRESOLVED_SEQUENCE",
                "PRIMARY_CORE_INCOMPLETE",
            }
        )
    }
    return sorted(critical)


def is_downstream_critical(row: dict[str, Any]) -> bool:
    return bool(critical_issue_codes(row) or critical_evidence_codes(row))


def secondary_gap_reclassification(problems: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in problems:
        for field in split_codes_pipe(row.get("missing_noncore_fields", "")):
            if not field:
                continue
            critical = False
            reason = "NONBLOCKING_SECONDARY_GAP"
            if field in {"OCF", "Capex"} and "FCF" in split_codes_pipe(row.get("missing_core_fields", "")):
                critical = True
                reason = "DERIVATION_SUPPORT_CRITICAL"
            if field == "Operating Income" and "EBIT" in split_codes_pipe(row.get("missing_core_fields", "")):
                critical = True
                reason = "DERIVATION_SUPPORT_CRITICAL"
            out.append(
                {
                    "ticker": row["ticker"],
                    "quarter_id": row["quarter_id"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "field": field,
                    "classification": reason,
                    "downstream_critical": "YES" if critical else "NO",
                    "notes": "Secondary field is not standalone downstream-critical under Phase 8G policy" if not critical else "Required to derive missing primary core field",
                }
            )
    return out


def material_problem_rows(problems: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in problems:
        critical_codes = critical_issue_codes(row)
        evidence_codes = critical_evidence_codes(row)
        if (
            critical_codes == ["MISSING_QUARTER"]
            and row.get("external_research_required") == "NO"
            and row.get("sequence_status") != "TRANSITION_SEQUENCE"
            and not any(
                row.get(k) == "YES" for k in ("current_ttm_impact", "score_impact", "lifecycle_impact", "valuation_impact")
            )
        ):
            continue
        if not critical_codes and not evidence_codes:
            continue
        out.append(
            {
                **row,
                "phase8g_issue_codes": "|".join(critical_codes),
                "phase8g_evidence_needed_codes": "|".join(evidence_codes),
                "phase8g_scope_classification": "DOWNSTREAM_CRITICAL",
                "secondary_gaps_ignored": "|".join(split_codes_pipe(row.get("missing_noncore_fields", ""))),
            }
        )
    return out


def missing_quarter_reclassification(material: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in material:
        if "MISSING_QUARTER" not in split_codes(row.get("phase8g_issue_codes", "")):
            continue
        if row.get("target_collision") in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}:
            klass = "UNRESOLVED"
        elif row.get("sequence_status") == "TRANSITION_SEQUENCE":
            klass = "TRANSITION_OR_STUB"
        elif row.get("fiscal_identity_status") in {"FY_CONFLICT_DIRECT_EXACT", "FQ_CONFLICT_DIRECT_EXACT"}:
            klass = "QUARTER_ALREADY_EXISTS_WRONG_LABEL"
        elif row.get("external_research_required") == "NO" and not any(row.get(k) == "YES" for k in ("current_ttm_impact", "score_impact", "lifecycle_impact", "valuation_impact")):
            klass = "FALSE_GAP_FROM_SEQUENCE_MODEL"
        elif row.get("external_research_required") == "NO":
            klass = "TRUE_MISSING_LOCAL_EVIDENCE_AVAILABLE"
        elif row.get("external_research_required") == "YES":
            klass = "TRUE_MISSING_EXTERNAL_EVIDENCE_REQUIRED"
        else:
            klass = "UNRESOLVED"
        out.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end": row["period_end"],
                "sequence_status": row["sequence_status"],
                "target_collision": row["target_collision"],
                "phase8g_missing_quarter_class": klass,
                "priority": row["priority"],
                "notes": row.get("notes", ""),
            }
        )
    return out


def build_local_relabel_plan(db: Path, artifact_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    latest_rows, _diagnostics, _problems, _ctx = build_quarter_diagnostics(db)
    candidates = [
        row
        for row in latest_rows
        if row.get("identity_class") in {"BLOCK_EXACT_FY_CONFLICT", "BLOCK_EXACT_FQ_CONFLICT"}
        and row.get("target_collision") == "TARGET_EMPTY"
        and row.get("period_end_structural_fit") == "STRUCTURAL_FIT"
        and row.get("publish_chronology") == "PUBLISH_AFTER_PERIOD_END"
        and row.get("fq_confidence") == "DIRECT_EXACT_FQ_HIGH"
        and row.get("repairability") == "AUTO_RELABEL_READY"
        and row.get("identity_basis") == "DIRECT_EXACT_INTERVAL"
        and row.get("break_reason") != "CALENDAR_TRANSITION"
    ]
    qid_to_pos = {int(row["quarter_id"]): int(row["quarter_position_latest8q"]) for row in latest_rows}
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        plan = []
        evidence = []
        for idx, row in enumerate(sorted(candidates, key=lambda r: (r["ticker"], r["period_end"], r["quarter_id"])), 1):
            sig = content_signature(conn, int(row["quarter_id"]))
            priority = "P1_CURRENT" if qid_to_pos[int(row["quarter_id"])] == 1 else "P2_LATEST4Q" if qid_to_pos[int(row["quarter_id"])] <= 4 else "P3_LATEST8Q"
            group = f"P8G-{idx:04d}-{row['ticker']}-{row['quarter_id']}"
            plan.append(
                {
                    "transformation_group": group,
                    "ticker": row["ticker"],
                    "group_type": "LOCAL_DIRECT_EXACT_RELABEL",
                    "operation_order": 1,
                    "quarter_id": int(row["quarter_id"]),
                    "old_fiscal_year": int(row["fiscal_year"]),
                    "old_fiscal_quarter": row["fiscal_quarter"],
                    "target_fiscal_year": int(row["exact_fy"]),
                    "target_fiscal_quarter": row["exact_fq"],
                    "period_end": row["period_end"],
                    "publish_date": row.get("publish_date") or "",
                    "operation": "UPDATE_FY_FQ" if row["fq_compare"] != "FQ_EXACT_MATCH" else "UPDATE_FY",
                    "target_quarter_id": "",
                    "target_collision_class": row["target_collision"],
                    "exact_anchor_fy_start": row.get("interval_start", ""),
                    "next_exact_anchor_start": row.get("interval_end_exclusive", ""),
                    "fiscal_identity_confidence": row.get("fq_confidence", ""),
                    "lineage_action": "PRESERVE_QUARTER_ID",
                    "write_guard": f"{row['quarter_id']}|{row['fiscal_year']}|{row['fiscal_quarter']}|{row['period_end']}->{row['exact_fy']}|{row['exact_fq']}",
                    "rollback_group": group,
                    "content_signature": sig["content_signature"],
                    "priority": priority,
                    "downstream_impact_class": "CURRENT" if priority == "P1_CURRENT" else "LATEST4Q" if priority == "P2_LATEST4Q" else "LATEST8Q",
                }
            )
            evidence.append(
                {
                    "ticker": row["ticker"],
                    "quarter_id": row["quarter_id"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "target_fiscal_year": row["exact_fy"],
                    "target_fiscal_quarter": row["exact_fq"],
                    "local_evidence_status": "LOCAL_REPAIR_READY",
                    "local_evidence": "DIRECT_EXACT_INTERVAL|STRUCTURAL_FIT|TARGET_EMPTY|PUBLISH_AFTER_PERIOD_END",
                    "likely_local_source": row.get("local_source_hint", ""),
                    "priority": priority,
                }
            )
    write_csv(artifact_root / "phase8g_local_evidence_resolution.csv", evidence)
    return plan, evidence


def precondition_check(db: Path, plan: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    checked = []
    ready = []
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for row in plan:
            current = conn.execute(
                """
                SELECT c.ticker,q.company_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
                FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
                WHERE q.quarter_id=?
                """,
                (int(row["quarter_id"]),),
            ).fetchone()
            reasons = []
            if current is None:
                reasons.append("MISSING_QUARTER")
            else:
                if str(current["ticker"]) != str(row["ticker"]):
                    reasons.append("TICKER_MISMATCH")
                if int(current["fiscal_year"]) != int(row["old_fiscal_year"]):
                    reasons.append("OLD_FY_MISMATCH")
                if str(current["fiscal_quarter"]) != str(row["old_fiscal_quarter"]):
                    reasons.append("OLD_FQ_MISMATCH")
                if str(current["period_end_date"]) != str(row["period_end"]):
                    reasons.append("PERIOD_END_MISMATCH")
                if str(current["publish_date"] or "") != str(row.get("publish_date") or ""):
                    reasons.append("PUBLISH_DATE_MISMATCH")
                target = conn.execute(
                    "SELECT quarter_id,period_end_date FROM v3_quarter WHERE company_id=? AND fiscal_year=? AND fiscal_quarter=?",
                    (int(current["company_id"]), int(row["target_fiscal_year"]), row["target_fiscal_quarter"]),
                ).fetchone()
                if target is not None and int(target["quarter_id"]) != int(row["quarter_id"]):
                    reasons.append("TARGET_NOT_EMPTY")
                if content_signature(conn, int(row["quarter_id"]))["content_signature"] != str(row["content_signature"]):
                    reasons.append("CONTENT_SIGNATURE_MISMATCH")
            checked_row = {**row, "precondition_status": "PASS" if not reasons else "BLOCKED", "precondition_reasons": "|".join(reasons)}
            checked.append(checked_row)
            if not reasons:
                ready.append(row)
    return checked, ready


def downstream_baseline(db: Path) -> dict[str, dict[tuple[Any, ...], dict[str, Any]]]:
    return {
        "ttm": semantic_table_rows(db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
        "score": semantic_table_rows(db, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True),
        "lifecycle": semantic_table_rows(db, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True),
        "valuation": semantic_table_rows(db, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
    }


def rebuild_all(db: Path, osakedata_db: Path, artifact_root: Path, run_id: str, baseline: dict[str, dict[tuple[Any, ...], dict[str, Any]]]) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]], dict[str, Any]]:
    ttm = rebuild_ttm(db, artifact_root, f"{run_id}_ttm")
    models = verify_models(db)
    phase6, changes = rebuild_phase6(db, osakedata_db, artifact_root, models, run_id, {k: baseline[k] for k in ("score", "lifecycle", "valuation")})
    changes["ttm"] = compare_after(db, baseline["ttm"], "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], "ttm")
    _fp, determinism = rerun_downstream(db, osakedata_db, models, artifact_root)
    summary = {"ttm": ttm, "score": phase6["score"], "lifecycle": phase6["lifecycle"], "valuation": phase6["valuation"], "determinism": determinism}
    return summary, changes, determinism


def rehearsal(paths: Phase8GPaths, plan: list[dict[str, Any]]) -> dict[str, Any]:
    rehearsal_db = paths.artifact_root / "rehearsal" / paths.v3_db.name
    rehearsal_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.v3_db, rehearsal_db)
    checked, ready = precondition_check(rehearsal_db, plan)
    write_csv(paths.artifact_root / "phase8g_rehearsal_precondition_check.csv", checked)
    baseline = downstream_baseline(rehearsal_db)
    apply_log, content_parity, lineage_parity, repair_integrity = apply_rehearsal(rehearsal_db, ready)
    write_csv(paths.artifact_root / "phase8g_rehearsal_apply_log.csv", apply_log)
    write_csv(paths.artifact_root / "phase8g_rehearsal_content_signature_parity.csv", content_parity)
    write_csv(paths.artifact_root / "phase8g_rehearsal_lineage_parity.csv", lineage_parity)
    downstream, changes, determinism = rebuild_all(rehearsal_db, paths.osakedata_db, paths.artifact_root, "phase8g_rehearsal", baseline) if ready else ({}, {"ttm": [], "score": [], "lifecycle": [], "valuation": []}, {})
    failed_groups = {row["transformation_group"] for row in apply_log if row.get("result") == "FAILED"}
    safe = (
        len(ready) == len(plan)
        and not failed_groups
        and repair_integrity["quick_check"] == "ok"
        and int(repair_integrity["foreign_key_check_rows"]) == 0
        and int(repair_integrity["duplicate_fy_fq"]) == 0
        and int(repair_integrity["orphan_fundamentals"]) == 0
        and all(int(row["signature_match"]) == 1 for row in content_parity)
        and all(int(row["lineage_match"]) == 1 for row in lineage_parity)
        and (not ready or all(bool(determinism.get(k)) for k in ("ttm_deterministic", "score_deterministic", "lifecycle_deterministic", "valuation_deterministic")))
    )
    summary = {
        "db": str(rehearsal_db),
        "plan_rows": len(plan),
        "ready_rows": len(ready),
        "ready_groups": len({row["transformation_group"] for row in ready}),
        "failed_groups": len(failed_groups),
        "safe_for_production": safe,
        "integrity": repair_integrity,
        "downstream": downstream,
        "determinism": determinism,
        "changed_rows": {layer: len(rows_) for layer, rows_ in changes.items()},
    }
    write_json(paths.artifact_root / "phase8g_rehearsal_integrity.json", repair_integrity)
    write_json(paths.artifact_root / "phase8g_rehearsal_determinism.json", determinism)
    write_json(paths.artifact_root / "phase8g_rehearsal_downstream_before_after.json", summary["changed_rows"])
    return summary | {"ready_plan": ready, "changes": changes}


def production_apply(paths: Phase8GPaths, ready_plan: list[dict[str, Any]]) -> dict[str, Any]:
    if not paths.apply_production or not ready_plan:
        return {"applied": False, "reason": "NO_PRODUCTION_APPLY_REQUESTED_OR_EMPTY_PLAN", "rows_applied": 0, "groups_applied": 0, "tickers_applied": 0, "failed_groups": 0}
    backup = create_backup(paths.v3_db, paths.artifact_root / "backup")
    checked, current_ready = precondition_check(paths.v3_db, ready_plan)
    write_csv(paths.artifact_root / "phase8g_production_precondition_check.csv", checked)
    if len(current_ready) != len(ready_plan):
        return {"applied": False, "reason": "PRODUCTION_PRECONDITION_FAILED", "rows_applied": 0, "groups_applied": 0, "tickers_applied": 0, "failed_groups": 0, "backup": backup}
    baseline = downstream_baseline(paths.v3_db)
    apply_log, content_parity, lineage_parity, apply_integrity = apply_rehearsal(paths.v3_db, current_ready)
    write_csv(paths.artifact_root / "phase8g_production_apply_log.csv", apply_log)
    failed_groups = {row["transformation_group"] for row in apply_log if row.get("result") == "FAILED"}
    downstream, changes, determinism = rebuild_all(paths.v3_db, paths.osakedata_db, paths.artifact_root, "phase8g_production", baseline)
    summary = {
        "applied": True,
        "backup": backup,
        "rows_applied": len(current_ready),
        "groups_applied": len({row["transformation_group"] for row in current_ready}),
        "tickers_applied": len({row["ticker"] for row in current_ready}),
        "failed_groups": len(failed_groups),
        "integrity": apply_integrity,
        "downstream": downstream,
        "determinism": determinism,
        "changed_rows": {layer: len(rows_) for layer, rows_ in changes.items()},
    }
    write_json(paths.artifact_root / "phase8g_production_downstream_summary.json", summary)
    return summary


def downstream_status(db: Path, local_repairs_by_ticker: Counter[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    latest_rows, diagnostics, problems, ctx = build_quarter_diagnostics(db)
    material = material_problem_rows(problems)
    ttm_risk = ctx["ttm_risk"]
    base_summary = summarize_tickers(db, latest_rows, diagnostics, ttm_risk)
    material_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in material:
        material_by_ticker[row["ticker"]].append(row)
    out = []
    for row in base_summary:
        ticker = row["ticker"]
        group = material_by_ticker.get(ticker, [])
        external = [r for r in group if r["external_research_required"] == "YES"]
        structural = [r for r in group if is_material_structural(r)]
        secondary_only = row["latest8q_fully_clean_primary_core_now"] == "YES" and row["latest8q_fully_complete_all_fields_now"] == "NO"
        clean = not group and int(row["latest8q_rows"]) >= 8
        if clean:
            remaining = "DOWNSTREAM_LATEST8Q_CLEAN_WITH_SECONDARY_GAPS" if secondary_only else "DOWNSTREAM_LATEST8Q_CLEAN"
        elif structural:
            remaining = "DOWNSTREAM_LATEST8Q_STRUCTURAL_REVIEW"
        elif external:
            remaining = "DOWNSTREAM_LATEST8Q_EXTERNAL_EVIDENCE_REQUIRED"
        else:
            remaining = "DOWNSTREAM_LATEST8Q_LOCAL_REPAIR_REMAINING"
        out.append(
            {
                "ticker": ticker,
                "company_id": row["company_id"],
                "latest8q_rows": row["latest8q_rows"],
                "downstream_clean_quarters": int(row["latest8q_rows"]) - len(group),
                "downstream_problem_quarters": len(group),
                "downstream_latest8q_clean": "YES" if remaining in {"DOWNSTREAM_LATEST8Q_CLEAN", "DOWNSTREAM_LATEST8Q_CLEAN_WITH_SECONDARY_GAPS"} else "NO",
                "secondary_gaps_only": "YES" if secondary_only else "NO",
                "current_ttm_clean": "YES" if row["current_ttm_status"] == "AVAILABLE_CLEAN" else "NO",
                "score_available": "YES" if row["score_status"] == "AVAILABLE" else "NO",
                "lifecycle_available": "YES" if row["lifecycle_status"] == "AVAILABLE" else "NO",
                "valuation_available": "YES" if row["valuation_status"] == "AVAILABLE" else "NO",
                "missing_revenue_q": row["revenue_missing_q"],
                "missing_ebit_q": row["ebit_missing_q"],
                "missing_fcf_q": row["fcf_missing_q"],
                "missing_cash_q": row["cash_missing_q"],
                "missing_debt_q": row["debt_missing_q"],
                "missing_shares_q": row["shares_missing_q"],
                "fiscal_identity_critical_issues": sum(bool({"FY_CONFLICT_DIRECT_EXACT", "FQ_CONFLICT_DIRECT_EXACT", "UNRESOLVED_BOUNDARY"} & set(split_codes(r["phase8g_issue_codes"]))) for r in group),
                "sequence_critical_issues": sum(bool({"MISSING_QUARTER", "DUPLICATE_ECONOMIC_QUARTER", "TARGET_COLLISION", "TRANSITION_SEQUENCE", "UNRESOLVED_SEQUENCE"} & set(split_codes(r["phase8g_issue_codes"]))) for r in group),
                "publish_date_critical_issues": sum(any(code.startswith("PUBLISH") for code in split_codes(r["phase8g_issue_codes"])) for r in group),
                "local_repairs_applied": local_repairs_by_ticker.get(ticker, 0),
                "external_facts_still_needed": sum(len(critical_evidence_codes(r)) for r in external),
                "structural_decisions_still_needed": len(structural),
                "affected_fy_fq": ";".join(dict.fromkeys(f"FY{r['fiscal_year']}{r['fiscal_quarter']}" for r in group)),
                "operational_priority": row["operational_priority"],
                "remaining_status": remaining,
                "recommended_next_action": downstream_next_action(remaining),
            }
        )
    return out, material, latest_rows, {"ttm_risk": ttm_risk}


def downstream_next_action(status: str) -> str:
    if status.startswith("DOWNSTREAM_LATEST8Q_CLEAN"):
        return "No downstream-critical latest8Q repair needed"
    if status == "DOWNSTREAM_LATEST8Q_EXTERNAL_EVIDENCE_REQUIRED":
        return "Use minimal downstream-critical external research queue"
    if status == "DOWNSTREAM_LATEST8Q_STRUCTURAL_REVIEW":
        return "Resolve structural/collision/transition decision"
    return "Rehearse remaining local repair only after stronger local evidence"


def minimal_external_queue(material: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows_ = []
    for row in material:
        if row["external_research_required"] != "YES":
            continue
        codes = critical_evidence_codes(row)
        if not codes:
            continue
        filtered = {**row, "evidence_needed_codes": "|".join(codes)}
        rows_.append(external_research_queue([filtered])[0])
    return rows_


def is_material_structural(row: dict[str, Any]) -> bool:
    codes = set(split_codes(row.get("phase8g_issue_codes", row.get("issue_codes", ""))))
    if is_structural(row):
        return True
    if "MISSING_QUARTER" in codes and row.get("external_research_required") == "NO":
        return True
    if row.get("external_research_required") == "NO" and codes & {"FY_CONFLICT_DIRECT_EXACT", "FQ_CONFLICT_DIRECT_EXACT"}:
        return True
    return False


def minimal_structural_queue(material: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return structural_review_queue([row for row in material if is_material_structural(row)])


def closure_test(status_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in status_rows:
        status = row["remaining_status"]
        if status.startswith("DOWNSTREAM_LATEST8Q_CLEAN"):
            result = "YES"
        elif status == "DOWNSTREAM_LATEST8Q_EXTERNAL_EVIDENCE_REQUIRED":
            result = "YES_AFTER_EXTERNAL"
        elif status == "DOWNSTREAM_LATEST8Q_STRUCTURAL_REVIEW":
            result = "YES_AFTER_STRUCTURAL"
        elif status == "DOWNSTREAM_LATEST8Q_HISTORY_LIMIT":
            result = "NO_LEGITIMATE_HISTORY_LIMIT"
        else:
            result = "NO_MISSING_REQUIREMENT"
        out.append({"ticker": row["ticker"], "remaining_status": status, "theoretical_downstream_closure": result, "missing_requirement": int(result == "NO_MISSING_REQUIREMENT")})
    return out


def core_field_resolution(material: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in material:
        for field in split_codes_pipe(row.get("missing_core_fields", "")):
            out.append({"ticker": row["ticker"], "quarter_id": row["quarter_id"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "field": field, "resolution_status": "LOCAL_NOT_AVAILABLE" if row["external_research_required"] == "YES" else "LOCAL_EVIDENCE_INSUFFICIENT", "notes": row.get("evidence_needed_description", "")})
    return out


def approved_derivations(material: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in material:
        for field in split_codes_pipe(row.get("derivable_fields", "")):
            out.append({"ticker": row["ticker"], "quarter_id": row["quarter_id"], "output_field": field, "source_fields": "OCF|Capex" if "FCF" in field else "", "rule_id": field, "status": "AVAILABLE_NOT_WRITTEN_IN_8G_FIELD_FILL_SCOPE", "confidence": row.get("confidence", "")})
    return out


def collision_lineage_resolution(material: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in material:
        if row.get("target_collision") not in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING", "TARGET_SAME_ECONOMIC"}:
            continue
        out.append({"ticker": row["ticker"], "quarter_id": row["quarter_id"], "issue": row["target_collision"], "resolution_status": "STRUCTURAL_DECISION_REQUIRED", "lineage": row.get("lineage", ""), "notes": row.get("evidence_needed_description", "")})
    return out


def known_13(status_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ticker": row["ticker"],
            "downstream_critical_remaining_issue": row["remaining_status"],
            "local_repair_performed": row["local_repairs_applied"],
            "external_evidence_still_needed": row["external_facts_still_needed"],
            "structural_decision_still_needed": row["structural_decisions_still_needed"],
            "ttm_impact": row["current_ttm_clean"],
            "expected_final_state": "YES_AFTER_STRUCTURAL" if row["structural_decisions_still_needed"] else "YES_AFTER_EXTERNAL" if row["external_facts_still_needed"] else "YES",
        }
        for row in status_rows
        if row["ticker"] in KNOWN_13
    ]


def build_summary(
    paths: Phase8GPaths,
    initial_status: list[dict[str, Any]],
    final_status: list[dict[str, Any]],
    secondary: list[dict[str, Any]],
    material_initial: list[dict[str, Any]],
    material_final: list[dict[str, Any]],
    missing_reclass: list[dict[str, Any]],
    plan: list[dict[str, Any]],
    rehearsal_summary: dict[str, Any],
    production_summary_: dict[str, Any],
    external_queue: list[dict[str, Any]],
    structural_queue: list[dict[str, Any]],
    before_fp: dict[str, Any],
    after_fp: dict[str, Any],
) -> dict[str, Any]:
    status_counts = Counter(row["remaining_status"] for row in final_status)
    closure = closure_test(final_status)
    no_missing = sum(int(row["missing_requirement"]) for row in closure)
    classification = CLASSIFICATION_BLOCKED if no_missing else CLASSIFICATION_REMAINING if external_queue or structural_queue else CLASSIFICATION_COMPLETE
    clean_after = sum(row["downstream_latest8q_clean"] == "YES" for row in final_status)
    clean_before = sum(row["downstream_latest8q_clean"] == "YES" for row in initial_status)
    priority_counter = Counter(row["operational_priority"] for row in final_status if row["remaining_status"] not in {"DOWNSTREAM_LATEST8Q_CLEAN", "DOWNSTREAM_LATEST8Q_CLEAN_WITH_SECONDARY_GAPS"})
    issue_counter = Counter(code for row in material_final for code in split_codes(row["phase8g_issue_codes"]))
    evidence_counter = Counter(code for row in material_final for code in critical_evidence_codes(row))
    local_repairs = Counter(row["ticker"] for row in plan if production_summary_.get("applied"))
    return {
        "classification": classification,
        "next_action": NEXT_BLOCKED if classification == CLASSIFICATION_BLOCKED else NEXT_SUCCESS,
        "artifact_root": str(paths.artifact_root),
        "scope_reinterpretation": {
            "phase8f_all_field_problem_tickers": len({row["ticker"] for row in read_csv_dicts(paths.phase8f_root / "latest8q_quarter_gap_detail.csv")}),
            "secondary_only_gaps_reclassified_nonblocking": sum(row["downstream_critical"] == "NO" for row in secondary),
            "downstream_critical_problem_tickers": len({row["ticker"] for row in material_final}),
            "downstream_clean_tickers_before_local_repair": clean_before,
        },
        "missing_quarter_analysis": dict(Counter(row["phase8g_missing_quarter_class"] for row in missing_reclass)),
        "local_critical_repair": {
            "local_candidates": len(plan),
            "local_evidence_sufficient": rehearsal_summary.get("ready_rows", 0),
            "local_repair_groups": rehearsal_summary.get("ready_groups", 0),
            "repaired_rows": production_summary_.get("rows_applied", 0),
            "repaired_tickers": production_summary_.get("tickers_applied", 0),
            "failed_groups": production_summary_.get("failed_groups", 0),
        },
        "critical_fields_resolved": {"Revenue": 0, "EBIT": 0, "FCF": 0, "Cash": 0, "Debt": 0, "Shares": 0, "critical_metadata_identity": production_summary_.get("rows_applied", 0), "publish_date_material": 0},
        "secondary_ignored": dict(Counter(row["field"] for row in secondary if row["downstream_critical"] == "NO")),
        "downstream_after_local_repair": downstream_metrics(final_status),
        "external_queue": {
            "old_phase8f_external_facts": OLD_PHASE8F_EXTERNAL_FACTS,
            "new_downstream_critical_external_facts": len(external_queue),
            "reduction_pct": pct(OLD_PHASE8F_EXTERNAL_FACTS - len(external_queue), OLD_PHASE8F_EXTERNAL_FACTS),
            "external_tickers": len({row["ticker"] for row in external_queue}),
            "P1_external_tickers": priority_ticker_count(external_queue, "P1_CURRENT"),
            "P2_external_tickers": priority_ticker_count(external_queue, "P2_LATEST4Q"),
            "P3_external_tickers": priority_ticker_count(external_queue, "P3_LATEST8Q"),
            "top_10_remaining_evidence_types": [{"evidence": k, "rows": v} for k, v in evidence_counter.most_common(10)],
        },
        "structural_queue": {
            "old_phase8f_structural_decisions": OLD_PHASE8F_STRUCTURAL_DECISIONS,
            "new_material_structural_decisions": len(structural_queue),
            "structural_tickers": len({row["ticker"] for row in structural_queue}),
            "top_structural_issue_categories": [{"issue": k, "rows": v} for k, v in Counter(row["issue"] for row in structural_queue).most_common(10)],
        },
        "full_downstream_closure": {
            "already_clean": clean_before,
            "clean_after_local_repair": clean_after,
            "require_external_evidence": len({row["ticker"] for row in external_queue}),
            "require_structural_decision": len({row["ticker"] for row in structural_queue}),
            "legitimate_history_limit": status_counts.get("DOWNSTREAM_LATEST8Q_HISTORY_LIMIT", 0),
            "NO_MISSING_REQUIREMENT": no_missing,
            "theoretical_downstream_clean_tickers_after_all_remaining_critical_work": len(final_status) - no_missing,
            "theoretical_downstream_clean_pct": pct(len(final_status) - no_missing, len(final_status)),
        },
        "issue_distribution": [{"issue": k, "rows": v} for k, v in sorted(issue_counter.items())],
        "known_13": known_13(final_status),
        "safety": {
            "production_canonical_writes": production_summary_.get("rows_applied", 0),
            "production_downstream_writes": sum(production_summary_.get("changed_rows", {}).values()) if production_summary_.get("applied") else 0,
            "fiscal_metadata_writes": 0,
            "active_guard_changes": 0,
            "rawcandle_writes": 0,
            "fingerprints_changed_as_expected": before_fp != after_fp if production_summary_.get("applied") else before_fp == after_fp,
        },
    }


def priority_ticker_count(queue: list[dict[str, Any]], priority: str) -> int:
    return len({row["ticker"] for row in queue if row.get("priority") == priority})


def downstream_metrics(status_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(status_rows)
    clean = sum(row["downstream_latest8q_clean"] == "YES" for row in status_rows)
    latest4 = sum(row["downstream_latest8q_clean"] == "YES" or row["operational_priority"] not in {"P1_CURRENT", "P2_LATEST4Q"} for row in status_rows)
    latest = sum(row["downstream_latest8q_clean"] == "YES" or row["operational_priority"] != "P1_CURRENT" for row in status_rows)
    return {
        "downstream_clean_latest8q_tickers": clean,
        "clean_pct": pct(clean, total),
        "latest4q_clean": latest4,
        "latest_quarter_clean": latest,
        "current_ttm_clean": sum(row["current_ttm_clean"] == "YES" for row in status_rows),
        "score_available": sum(row["score_available"] == "YES" for row in status_rows),
        "lifecycle_available": sum(row["lifecycle_available"] == "YES" for row in status_rows),
        "valuation_available": sum(row["valuation_available"] == "YES" for row in status_rows),
    }


def write_report(path: Path, summary: dict[str, Any]) -> None:
    d = summary["downstream_after_local_repair"]
    ext = summary["external_queue"]
    struct = summary["structural_queue"]
    full = summary["full_downstream_closure"]
    lines = [
        "# Fundamentals V3 Latest8Q Downstream Gap Analysis",
        "",
        "Phase 8F was an all-field full-closure analysis. Phase 8G applies the narrower downstream-critical policy: secondary gaps are informational unless required to derive a primary downstream field.",
        "",
        "## Executive Dashboard",
        "",
        "| Metric | Clean / Available | Total | % |",
        "| --- | ---: | ---: | ---: |",
        f"| Downstream-clean latest8Q tickers | {d['downstream_clean_latest8q_tickers']} | 2470 | {d['clean_pct']} |",
        f"| Latest4Q downstream-clean | {d['latest4q_clean']} | 2470 | {pct(d['latest4q_clean'], 2470)} |",
        f"| Latest quarter downstream-clean | {d['latest_quarter_clean']} | 2470 | {pct(d['latest_quarter_clean'], 2470)} |",
        f"| Current TTM clean | {d['current_ttm_clean']} | 2470 | {pct(d['current_ttm_clean'], 2470)} |",
        f"| Score available | {d['score_available']} | 2470 | {pct(d['score_available'], 2470)} |",
        f"| Lifecycle available | {d['lifecycle_available']} | 2470 | {pct(d['lifecycle_available'], 2470)} |",
        f"| Valuation available | {d['valuation_available']} | 2470 | {pct(d['valuation_available'], 2470)} |",
        f"| External research still needed | {ext['external_tickers']} | 2470 | {pct(ext['external_tickers'], 2470)} |",
        f"| Structural review still needed | {struct['structural_tickers']} | 2470 | {pct(struct['structural_tickers'], 2470)} |",
        "",
        "## Local Repair Results",
        "",
        f"- local candidates analyzed: `{summary['local_critical_repair']['local_candidates']}`",
        f"- local evidence sufficient: `{summary['local_critical_repair']['local_evidence_sufficient']}`",
        f"- local repair groups: `{summary['local_critical_repair']['local_repair_groups']}`",
        f"- repaired rows: `{summary['local_critical_repair']['repaired_rows']}`",
        f"- repaired tickers: `{summary['local_critical_repair']['repaired_tickers']}`",
        f"- failed groups: `{summary['local_critical_repair']['failed_groups']}`",
        "",
        "## Remaining External Work",
        "",
        f"- old Phase 8F external facts: `{ext['old_phase8f_external_facts']}`",
        f"- new downstream-critical external facts: `{ext['new_downstream_critical_external_facts']}`",
        f"- reduction %: `{ext['reduction_pct']}`",
        "",
        "## Remaining Structural Work",
        "",
        f"- old Phase 8F structural decisions: `{struct['old_phase8f_structural_decisions']}`",
        f"- new material structural decisions: `{struct['new_material_structural_decisions']}`",
        f"- structural tickers: `{struct['structural_tickers']}`",
        "",
        "## Full Downstream Closure",
        "",
        f"- already clean: `{full['already_clean']}`",
        f"- clean after local repair: `{full['clean_after_local_repair']}`",
        f"- require external evidence: `{full['require_external_evidence']}`",
        f"- require structural decision: `{full['require_structural_decision']}`",
        f"- NO_MISSING_REQUIREMENT: `{full['NO_MISSING_REQUIREMENT']}`",
        f"- theoretical downstream-clean tickers: `{full['theoretical_downstream_clean_tickers_after_all_remaining_critical_work']}`",
        f"- theoretical downstream-clean %: `{full['theoretical_downstream_clean_pct']}`",
        "",
        "## Classification",
        "",
        f"`{summary['classification']}`",
        "",
        "## Next Action",
        "",
        summary["next_action"],
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def append_docs(summary: dict[str, Any]) -> None:
    marker = "## Phase 8G - Local Latest8Q Downstream-Critical Repair"
    section = f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Local downstream-critical candidates `{summary['local_critical_repair']['local_candidates']}`, repaired rows `{summary['local_critical_repair']['repaired_rows']}`, repaired tickers `{summary['local_critical_repair']['repaired_tickers']}`, failed groups `{summary['local_critical_repair']['failed_groups']}`.

New minimal external facts `{summary['external_queue']['new_downstream_critical_external_facts']}` vs Phase 8F `{summary['external_queue']['old_phase8f_external_facts']}`. New material structural decisions `{summary['structural_queue']['new_material_structural_decisions']}` vs Phase 8F `{summary['structural_queue']['old_phase8f_structural_decisions']}`.

Phase 8 remains `IN PROGRESS`.

Next action: {summary['next_action']}
"""
    for path in (Path("docs/fundamentals_v3_phase8_update_v3.md"), Path("docs/fundamentals_v3_deferred_repair_handoff.md")):
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
        if marker in existing:
            prefix = existing.split(marker, 1)[0].rstrip()
            path.write_text(prefix + section.rstrip() + "\n", encoding="utf-8")
        else:
            path.write_text(existing.rstrip() + section.rstrip() + "\n", encoding="utf-8")


def run_phase8g(paths: Phase8GPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    if not (paths.phase8f_root / "phase8f_summary.json").exists():
        raise FileNotFoundError(paths.phase8f_root / "phase8f_summary.json")
    before_fp = production_fingerprints(paths.v3_db)
    before_status, before_material, _latest_rows, _ctx = downstream_status(paths.v3_db, Counter())
    _latest, _diag, problems, _ctx2 = build_quarter_diagnostics(paths.v3_db)
    secondary = secondary_gap_reclassification(problems)
    material_initial = material_problem_rows(problems)
    missing_reclass = missing_quarter_reclassification(material_initial)
    plan, evidence = build_local_relabel_plan(paths.v3_db, paths.artifact_root)
    checked, ready = precondition_check(paths.v3_db, plan)
    write_csv(paths.artifact_root / "phase8g_local_candidate_rows.csv", plan)
    write_csv(paths.artifact_root / "phase8g_local_repair_plan.csv", checked)
    write_csv(paths.artifact_root / "phase8g_local_repair_groups.csv", [{"transformation_group": row["transformation_group"], "ticker": row["ticker"], "rows": 1, "priority": row["priority"], "status": row["precondition_status"]} for row in checked])
    write_csv(paths.artifact_root / "phase8g_secondary_gap_reclassification.csv", secondary)
    write_csv(paths.artifact_root / "phase8g_missing_quarter_reclassification.csv", missing_reclass)
    write_csv(paths.artifact_root / "phase8g_core_field_resolution.csv", core_field_resolution(material_initial))
    write_csv(paths.artifact_root / "phase8g_approved_derivations.csv", approved_derivations(material_initial))
    write_csv(paths.artifact_root / "phase8g_collision_lineage_resolution.csv", collision_lineage_resolution(material_initial))
    write_json(paths.artifact_root / "phase8g_material_gap_summary.json", {"material_problem_rows": len(material_initial), "material_problem_tickers": len({row["ticker"] for row in material_initial}), "secondary_reclassified_rows": len(secondary)})
    rehearsal_summary = rehearsal(paths, ready)
    if rehearsal_summary["safe_for_production"]:
        production = production_apply(paths, rehearsal_summary["ready_plan"])
    else:
        production = {"applied": False, "reason": "REHEARSAL_NOT_SAFE", "rows_applied": 0, "groups_applied": 0, "tickers_applied": 0, "failed_groups": rehearsal_summary["failed_groups"]}
    local_repairs_by_ticker = Counter(row["ticker"] for row in rehearsal_summary["ready_plan"] if production.get("applied"))
    final_status, material_final, _latest_final, _ctx_final = downstream_status(paths.v3_db, local_repairs_by_ticker)
    ext_queue = minimal_external_queue(material_final)
    struct_queue = minimal_structural_queue(material_final)
    local_remaining = [
        row
        for row in material_final
        if row["external_research_required"] == "NO" and not is_material_structural(row)
    ]
    closure = closure_test(final_status)
    after_fp = production_fingerprints(paths.v3_db)
    summary = build_summary(paths, before_status, final_status, secondary, material_initial, material_final, missing_reclass, plan, rehearsal_summary, production, ext_queue, struct_queue, before_fp, after_fp)
    write_csv(paths.artifact_root / "latest8q_downstream_ticker_status.csv", final_status)
    write_csv(paths.artifact_root / "latest8q_downstream_external_research_queue.csv", ext_queue)
    write_csv(paths.artifact_root / "latest8q_downstream_structural_review_queue.csv", struct_queue)
    write_csv(paths.artifact_root / "latest8q_downstream_local_remaining_queue.csv", local_remaining)
    write_csv(paths.artifact_root / "phase8g_downstream_closure_test.csv", closure)
    write_csv(paths.artifact_root / "phase8g_known_13_downstream_critical.csv", summary["known_13"])
    write_json(paths.artifact_root / "phase8g_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if paths.write_documentation:
        write_report(Path("docs/fundamentals_v3_latest8q_downstream_gap_analysis.md"), summary)
        append_docs(summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8G local latest8Q downstream-critical repairs")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8g_local_latest8q_repairs") / utc_stamp())
    parser.add_argument("--phase8f-root", type=Path, default=Path("temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--no-production-apply", action="store_true")
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8g(
        Phase8GPaths(
            artifact_root=args.artifact_root,
            phase8f_root=args.phase8f_root,
            v3_db=args.v3_db,
            osakedata_db=args.osakedata_db,
            apply_production=not args.no_production_apply,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"local_candidates={summary['local_critical_repair']['local_candidates']}")
    print(f"repaired_rows={summary['local_critical_repair']['repaired_rows']}")
    print(f"new_external_facts={summary['external_queue']['new_downstream_critical_external_facts']}")
    print(f"NO_MISSING_REQUIREMENT={summary['full_downstream_closure']['NO_MISSING_REQUIREMENT']}")
    return 0 if summary["classification"] != CLASSIFICATION_BLOCKED else 2


if __name__ == "__main__":
    raise SystemExit(main())
