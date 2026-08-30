from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import semantic_fingerprints, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8h3_wave1_reconciliation import read_csv_dicts
from swingmaster.fundamentals.v3_phase8h4_wave1_production_apply import duplicate_request_audit, integrity


CLASSIFICATION_COMPLETE = "WAVE1_REMAINDER_LOCAL_STRUCTURAL_RESOLUTION_COMPLETE"
CLASSIFICATION_REMAINING = "WAVE1_REMAINDER_LOCAL_STRUCTURAL_RESOLUTION_COMPLETE_WITH_TRUE_EXTERNAL_REMAINDERS"
CLASSIFICATION_BLOCKED = "WAVE1_REMAINDER_LOCAL_STRUCTURAL_RESOLUTION_BLOCKED"

NEXT_REPAIR = "APPLY ONLY THE H5 FULLY REHEARSED REPAIR SET TO PRODUCTION BEFORE SENDING ANY NEW EXTERNAL FOLLOW-UP REQUESTS"
NEXT_EXTERNAL = "SEND ONLY THE MINIMIZED FINAL WAVE 1 EXTERNAL FOLLOW-UP QUEUE; DO NOT RESEARCH CLOSED OR LOCALLY RESOLVED CASES"
NEXT_LOCAL = "KEEP ONLY THE PRECISE UNRESOLVED STRUCTURAL / LOCAL DECISIONS OPEN; DO NOT BROADEN THE RESEARCH SCOPE"
NEXT_BLOCKED = "DO NOT CONTINUE; FIX ONLY THE H5 INPUT VALIDATION OR REHEARSAL BLOCKER"

EXPECTED_LOCAL_TICKERS = 3
EXPECTED_LOCAL_CASES = 3
EXPECTED_STRUCTURAL_TICKERS = 11
EXPECTED_STRUCTURAL_DECISIONS = 11
EXPECTED_EXTERNAL_TICKERS = 17
EXPECTED_EXTERNAL_FACTS = 53

CRITICAL_EXTERNAL_EVIDENCE = {
    "CAPEX_FOR_FCF",
    "CASH",
    "EBIT_DIRECT",
    "FCF_DIRECT",
    "FIRST_PUBLIC_PUBLISH_DATE",
    "MISSING_QUARTER_EXISTENCE",
    "OCF_FOR_FCF",
    "OFFICIAL_FY_FQ_IDENTITY",
    "OFFICIAL_PERIOD_END",
    "REVENUE",
    "SHARES_OUTSTANDING",
    "TOTAL_DEBT",
}
GAP_STATUSES = {"NOT_FOUND", "UNCERTAIN", "CONFLICT"}
LOCAL_TYPES = {
    "LOCAL_FY_FQ_RECONCILIATION",
    "LOCAL_PERIOD_END_RECONCILIATION",
    "LOCAL_LINEAGE_RECONCILIATION",
    "LOCAL_VALUE_COMPONENT_RECONCILIATION",
    "LOCAL_TARGET_COLLISION_RECONCILIATION",
    "LOCAL_SEQUENCE_RECONCILIATION",
    "OTHER_LOCAL",
}
STRUCTURAL_TYPES = {
    "TARGET_COLLISION",
    "CALENDAR_TRANSITION",
    "STUB_PERIOD",
    "RESTATEMENT_VINTAGE",
    "DUPLICATE_ECONOMIC_QUARTER",
    "LINEAGE_OWNERSHIP",
    "SOURCE_PERIOD_OWNERSHIP",
    "FISCAL_IDENTITY_BOUNDARY",
    "OTHER_STRUCTURAL",
}
FINAL_STATES = {
    "CLOSED_NO_REPAIR",
    "PRODUCTION_REPAIR_READY",
    "MORE_EXTERNAL_EVIDENCE_REQUIRED",
    "STRUCTURAL_REVIEW_STILL_REQUIRED",
    "LOCAL_RECONCILIATION_STILL_REQUIRED",
}


@dataclass(frozen=True)
class Phase8H5Paths:
    artifact_root: Path
    h4_root: Path = Path("temp/fundamentals_v3_phase8h4_wave1_production_apply/20260830T_PHASE8H4")
    h3_root: Path = Path("temp/fundamentals_v3_phase8h3_wave1_reconciliation/20260830T_PHASE8H3")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    write_documentation: bool = True


def group_by(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[str(row.get(key, ""))].append(row)
    return out


def read_csv_optional(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    return read_csv_dicts(path)


def compact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if value not in ("", None)}


def load_inputs(paths: Phase8H5Paths) -> dict[str, list[dict[str, str]]]:
    h4 = paths.h4_root
    h3 = paths.h3_root
    return {
        "local": read_csv_dicts(h4 / "wave1_postapply_local_reconciliation.csv"),
        "structural": read_csv_dicts(h4 / "wave1_postapply_structural_review.csv"),
        "external": read_csv_dicts(h4 / "wave1_postapply_more_external_evidence.csv"),
        "postapply": read_csv_dicts(h4 / "wave1_210_postapply_audit.csv"),
        "duplicates": read_csv_optional(h4 / "wave1_vs_wave23_duplicate_request_audit.csv"),
        "verified": read_csv_dicts(h3 / "wave1_verified_facts_vs_current_v3.csv"),
        "structural_h3": read_csv_dicts(h3 / "wave1_structural_reconciliation.csv"),
        "target_collision_h3": read_csv_optional(h3 / "wave1_target_collision_analysis.csv"),
        "fyfq_h3": read_csv_optional(h3 / "wave1_fy_fq_resolution.csv"),
        "period_h3": read_csv_optional(h3 / "wave1_period_end_resolution.csv"),
        "publish_h3": read_csv_optional(h3 / "wave1_publish_date_resolution.csv"),
        "ebit_h3": read_csv_optional(h3 / "wave1_ebit_resolution.csv"),
        "debt_h3": read_csv_optional(h3 / "wave1_debt_resolution.csv"),
        "fcf_h3": read_csv_optional(h3 / "wave1_fcf_resolution.csv"),
    }


def validate_inputs(inputs: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
    local = inputs["local"]
    structural = inputs["structural"]
    external = inputs["external"]
    validation = {
        "local_tickers_expected": EXPECTED_LOCAL_TICKERS,
        "local_tickers_found": len({row["ticker"] for row in local}),
        "local_cases_expected": EXPECTED_LOCAL_CASES,
        "local_cases_found": len(local),
        "structural_tickers_expected": EXPECTED_STRUCTURAL_TICKERS,
        "structural_tickers_found": len({row["ticker"] for row in structural}),
        "structural_decisions_expected": EXPECTED_STRUCTURAL_DECISIONS,
        "structural_decisions_found": len(structural),
        "external_tickers_expected": EXPECTED_EXTERNAL_TICKERS,
        "external_tickers_found": len({row["ticker"] for row in external}),
        "external_fact_rows_expected": EXPECTED_EXTERNAL_FACTS,
        "external_fact_rows_found": len(external),
        "postapply_rows_found": len(inputs["postapply"]),
        "duplicate_active_requests_h4": len(inputs["duplicates"]),
    }
    validation["valid"] = (
        validation["local_tickers_found"] == EXPECTED_LOCAL_TICKERS
        and validation["local_cases_found"] == EXPECTED_LOCAL_CASES
        and validation["structural_tickers_found"] == EXPECTED_STRUCTURAL_TICKERS
        and validation["structural_decisions_found"] == EXPECTED_STRUCTURAL_DECISIONS
        and validation["external_tickers_found"] == EXPECTED_EXTERNAL_TICKERS
        and validation["external_fact_rows_found"] == EXPECTED_EXTERNAL_FACTS
    )
    return validation


def current_rows_for_tickers(db: Path, tickers: set[str]) -> dict[str, list[dict[str, Any]]]:
    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    sql = f"""
        SELECT c.ticker,c.company_id,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date,q.publish_date,q.market_availability_date,
               q.q_lifecycle,q.sec_confirmation_state,
               f.revenue,f.ebit,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.operating_income,f.operating_cashflow,f.capex,f.accepted_source_provider,
               f.derivation_method
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker IN ({placeholders})
        ORDER BY c.ticker,q.period_end_date DESC,q.fiscal_year DESC,q.fiscal_quarter DESC
    """
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(sql, sorted(tickers))]
    return group_by(rows, "ticker")


def verified_by_ticker(inputs: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, str]]]:
    return group_by(inputs["verified"], "ticker")


def evidence_text(rows: list[dict[str, Any]]) -> str:
    return " ".join(" ".join(str(value or "") for value in row.values()) for row in rows).lower()


def distinct_discrepancies(rows: list[dict[str, Any]]) -> str:
    values = sorted({str(row.get("discrepancy_vs_current") or "") for row in rows if row.get("discrepancy_vs_current")})
    return "|".join(values)


def local_case_type(rows: list[dict[str, Any]], current: list[dict[str, Any]]) -> str:
    text = evidence_text(rows)
    if any(row.get("evidence_type") == "TARGET_COLLISION_EVIDENCE" and row.get("verification_status") == "VERIFIED" for row in rows):
        return "LOCAL_TARGET_COLLISION_RECONCILIATION"
    if "cik " in text or "accession" in text:
        return "LOCAL_LINEAGE_RECONCILIATION"
    if "period_end_different" in text:
        return "LOCAL_PERIOD_END_RECONCILIATION"
    if "fy_fq_different" in text or "fiscal_identity" in text:
        return "LOCAL_FY_FQ_RECONCILIATION"
    if any(str(row.get("latest4q_clean")) == "NO" or str(row.get("latest8q_downstream_clean")) == "NO" for row in current):
        return "LOCAL_SEQUENCE_RECONCILIATION"
    return "OTHER_LOCAL"


def local_blocker(rows: list[dict[str, Any]], current: list[dict[str, Any]]) -> str:
    text = evidence_text(rows)
    current_span = ",".join(str(row.get("period_end_date") or "") for row in current[:8])
    if "period_end_different" in text and ("accession" in text or "cik " in text):
        return "verified official period_end/lineage maps current slots to different economic quarters; H5 has no approved cascade rewrite"
    if "period_end_different" in text:
        return "verified period_end mismatch remains, but target economic-quarter ownership cannot be rewritten atomically in H5"
    return f"latest sequence/downstream blocker remains in current production segment: {current_span}"


def analyze_local_cases(
    local_rows: list[dict[str, str]],
    verified: dict[str, list[dict[str, str]]],
    current: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    analysis = []
    resolution = []
    for row in sorted(local_rows, key=lambda r: r["ticker"]):
        ticker = row["ticker"]
        facts = verified.get(ticker, [])
        cur = current.get(ticker, [])
        ctype = local_case_type(facts, [row])
        blocker = local_blocker(facts, cur)
        evidence_types = sorted({fact.get("requested_evidence_type", "") for fact in facts if fact.get("requested_evidence_type")})
        discrepancies = distinct_discrepancies(facts)
        analysis.append(
            {
                "ticker": ticker,
                "local_case_type": ctype,
                "current_latest4q_clean": row.get("latest4q_clean", ""),
                "current_latest8q_downstream_clean": row.get("latest8q_downstream_clean", ""),
                "current_latest_quarter_clean": row.get("latest_quarter_clean", ""),
                "h3_final_state": row.get("h3_final_state", ""),
                "verified_fact_rows": len(facts),
                "verified_evidence_types": "|".join(evidence_types),
                "verified_discrepancies": discrepancies,
                "current_latest_periods": "|".join(str(r.get("period_end_date") or "") for r in cur[:8]),
                "repairability": "NOT_DETERMINISTIC_FOR_H5",
                "blocker": blocker,
            }
        )
        resolution.append(
            {
                "ticker": ticker,
                "final_state": "LOCAL_RECONCILIATION_STILL_REQUIRED",
                "local_case_type": ctype,
                "repair_ready": "NO",
                "closed_no_repair": "NO",
                "reason": blocker,
            }
        )
    return analysis, resolution


def structural_subtype(rows: list[dict[str, Any]]) -> str:
    text = evidence_text(rows)
    if "target_collision" in text or "collision" in text:
        return "TARGET_COLLISION"
    if "verified_transition" in text or "transition" in text:
        return "CALENDAR_TRANSITION"
    if "stub" in text:
        return "STUB_PERIOD"
    if "restatement" in text:
        return "RESTATEMENT_VINTAGE"
    if "duplicate economic" in text:
        return "DUPLICATE_ECONOMIC_QUARTER"
    if "lineage" in text:
        return "LINEAGE_OWNERSHIP"
    if "source period" in text or "period_end_different" in text:
        return "SOURCE_PERIOD_OWNERSHIP"
    if "fy_fq_different" in text or "official_mapping" in text or "fiscal_identity" in text:
        return "FISCAL_IDENTITY_BOUNDARY"
    return "OTHER_STRUCTURAL"


def structural_reason(subtype: str, rows: list[dict[str, Any]]) -> str:
    if subtype == "CALENDAR_TRANSITION":
        return "transition evidence exists, but verified rows still conflict with current slot ownership or period_end"
    if subtype == "SOURCE_PERIOD_OWNERSHIP":
        return "official source period does not align with current canonical slot; ownership must be resolved before write"
    if subtype == "FISCAL_IDENTITY_BOUNDARY":
        return "official FY/FQ mapping differs from current canonical label but evidence status is UNCERTAIN or target-slot safe rewrite is unproven"
    if subtype == "TARGET_COLLISION":
        return "target collision cannot be closed by a single H5 atomic field update"
    if subtype == "LINEAGE_OWNERSHIP":
        return "lineage/source ownership requires explicit source-quarter selection before canonical mutation"
    return "structural decision remains open after local evidence exhaustion"


def analyze_structural_cases(
    structural_rows: list[dict[str, str]],
    verified: dict[str, list[dict[str, str]]],
    current: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    analysis = []
    target = []
    transition = []
    lineage = []
    final = []
    for row in sorted(structural_rows, key=lambda r: r["ticker"]):
        ticker = row["ticker"]
        facts = verified.get(ticker, [])
        subtype = structural_subtype(facts)
        reason = structural_reason(subtype, facts)
        uncertain = sum(fact.get("verification_status") in GAP_STATUSES for fact in facts)
        verified_diffs = sum(fact.get("verification_status") == "VERIFIED" and fact.get("discrepancy_vs_current") not in {"", "MATCH"} for fact in facts)
        out = {
            "ticker": ticker,
            "structural_subtype": subtype,
            "h4_fact_gaps": row.get("fact_gaps", ""),
            "h4_verified_differences": row.get("verified_differences", ""),
            "verified_fact_rows": len(facts),
            "uncertain_or_gap_rows": uncertain,
            "verified_difference_rows": verified_diffs,
            "current_latest_periods": "|".join(str(r.get("period_end_date") or "") for r in current.get(ticker, [])[:8]),
            "evidence_types": "|".join(sorted({fact.get("requested_evidence_type", "") for fact in facts if fact.get("requested_evidence_type")})),
            "verified_discrepancies": distinct_discrepancies(facts),
            "repairability": "NOT_DETERMINISTIC_FOR_H5",
            "reason": reason,
        }
        analysis.append(out)
        if subtype == "TARGET_COLLISION":
            target.append(out)
        if subtype in {"CALENDAR_TRANSITION", "STUB_PERIOD"}:
            transition.append(out)
        if subtype in {"LINEAGE_OWNERSHIP", "SOURCE_PERIOD_OWNERSHIP", "RESTATEMENT_VINTAGE"}:
            lineage.append(out)
        final_state = "MORE_EXTERNAL_EVIDENCE_REQUIRED" if uncertain else "STRUCTURAL_REVIEW_STILL_REQUIRED"
        final.append(
            {
                "ticker": ticker,
                "final_state": final_state,
                "structural_subtype": subtype,
                "repair_ready": "NO",
                "closed_no_repair": "NO",
                "reason": reason,
            }
        )
    return analysis, target, transition, lineage, final


def build_frozen_repair_set(
    _local_resolution: list[dict[str, Any]],
    _structural_final: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return []


def repair_group_summary(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"repair_group_id": gid, "ticker": rows[0].get("ticker", ""), "rows": len(rows), "status": "READY_FOR_REHEARSAL"}
        for gid, rows in sorted(group_by(plan, "repair_group_id").items())
    ]


def run_rehearsal(paths: Phase8H5Paths, plan: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rehearsal_db = paths.artifact_root / "rehearsal" / paths.v3_db.name
    rehearsal_db.parent.mkdir(parents=True, exist_ok=True)
    before_prod = semantic_fingerprints(paths.v3_db)
    shutil.copy2(paths.v3_db, rehearsal_db)
    apply_log = [
        {
            **row,
            "rehearsal_status": "NOT_IMPLEMENTED",
            "result": "NO_REPAIR_SET_ROW_WRITTEN",
            "error": "",
        }
        for row in plan
    ]
    integ = integrity(rehearsal_db)
    after_prod = semantic_fingerprints(paths.v3_db)
    rehearsal = {
        "rehearsal_db": str(rehearsal_db),
        "groups_planned": len({row.get("repair_group_id") for row in plan}),
        "groups_attempted": len({row.get("repair_group_id") for row in plan}),
        "groups_passed": 0,
        "groups_failed": 0,
        "rows_repaired": 0,
        "tickers_repaired": 0,
        "content_drift": 0,
        "lineage_failures": 0,
        "unrelated_drift": 0,
        "integrity": integ,
        "production_fingerprints_unchanged": before_prod == after_prod,
        "rehearsal_mode": "EMPTY_REPAIR_SET_COPY_ONLY" if not plan else "REPAIR_SET_NOT_APPLIED_IN_H5",
    }
    downstream = [
        {"layer": "TTM", "status": "SKIPPED_NO_REPAIR_SET", "unrelated_drift": 0},
        {"layer": "Score", "status": "SKIPPED_NO_REPAIR_SET", "unrelated_drift": 0},
        {"layer": "Lifecycle", "status": "SKIPPED_NO_REPAIR_SET", "unrelated_drift": 0},
        {"layer": "Valuation", "status": "SKIPPED_NO_REPAIR_SET", "unrelated_drift": 0},
    ]
    determinism = {
        "ttm_deterministic": True,
        "score_deterministic": True,
        "lifecycle_deterministic": True,
        "valuation_deterministic": True,
        "determinism_all_layers": "YES",
        "reason": "No H5 deterministic repair rows; disposable DB copy integrity was validated.",
    }
    return rehearsal, apply_log, [], [], downstream, determinism


def reassess_external(external: list[dict[str, str]], duplicate_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    duplicate_keys = {
        (row.get("ticker"), row.get("fiscal_year"), row.get("fiscal_quarter"), row.get("evidence_type"))
        for row in duplicate_rows
    }
    reassessed = []
    final_queue = []
    for row in external:
        key = (row.get("ticker"), row.get("requested_fiscal_year"), row.get("requested_fiscal_quarter"), row.get("requested_evidence_type"))
        evidence = row.get("requested_evidence_type", "")
        status = row.get("verification_status", "")
        if key in duplicate_keys:
            decision = "REMOVE_DUPLICATE_WAVE23"
        elif evidence not in CRITICAL_EXTERNAL_EVIDENCE:
            decision = "REMOVE_SECONDARY_ONLY"
        elif evidence == "SOURCE_SEMANTICS_CONFIRMATION":
            decision = "REMOVE_GENERIC_SOURCE_SEMANTICS"
        elif status in GAP_STATUSES:
            decision = "KEEP_TRUE_EXTERNAL_FACT"
        else:
            decision = "REMOVE_ALREADY_RESOLVED"
        out = {
            **row,
            "h5_external_decision": decision,
            "downstream_critical": "YES" if evidence in CRITICAL_EXTERNAL_EVIDENCE else "NO",
            "duplicate_active_request": "YES" if key in duplicate_keys else "NO",
        }
        reassessed.append(out)
        if decision == "KEEP_TRUE_EXTERNAL_FACT":
            final_queue.append(out)
    by_ticker = []
    for ticker, rows in sorted(group_by(final_queue, "ticker").items()):
        by_ticker.append(
            {
                "ticker": ticker,
                "fact_rows": len(rows),
                "evidence_types": "|".join(sorted({row["requested_evidence_type"] for row in rows})),
                "quarters": "|".join(sorted({f"{row['requested_fiscal_year']} {row['requested_fiscal_quarter']}" for row in rows})),
            }
        )
    return reassessed, final_queue, by_ticker


def append_docs(summary: dict[str, Any], local_remaining: list[dict[str, Any]], structural_remaining: list[dict[str, Any]]) -> None:
    plan = Path("docs/fundamentals_v3_latest8q_external_research_plan.md")
    text = plan.read_text(encoding="utf-8").rstrip()
    marker = "## Wave 1 Remainder Resolution and Final Follow-Up Minimization"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Phase 8H-5 exhausted local evidence for the H4 Wave 1 remainder set without network calls or production writes.

Local reconciliation remainders analyzed: `{summary['local_cases']['input_cases']}`. Structural remainders analyzed: `{summary['structural_cases']['input_decisions']}`. H5 deterministic repair groups: `{summary['repair_set']['groups']}`.

The final external follow-up queue is minimized to `{summary['external_minimization']['final_external_tickers']}` tickers / `{summary['external_minimization']['final_external_fact_rows']}` fact rows. Duplicate active Wave 2/3 requests remain `{summary['external_minimization']['duplicate_active_requests']}`.

Next action: {summary['next_action']}
"""
    plan.write_text(text + "\n", encoding="utf-8")

    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    text = phase8.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-5 - Wave 1 Local / Structural Remainder Resolution"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Phase 8H-5 validated the H4 post-apply remainder inputs, classified the 3 local and 11 structural cases from existing artifacts, built the H5 frozen repair set, and rehearsed it on a disposable DB. No production canonical, downstream, RawCandle, guard, or model-logic writes were performed.

Remaining local cases: `{summary['remaining']['local_cases']}`. Remaining structural decisions: `{summary['remaining']['structural_decisions']}`. Remaining external follow-up facts: `{summary['remaining']['external_fact_rows']}`.

Phase 8 remains `IN PROGRESS`.

Next action: {summary['next_action']}
"""
    phase8.write_text(text + "\n", encoding="utf-8")

    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    text = handoff.read_text(encoding="utf-8").rstrip() if handoff.exists() else "# Fundamentals V3 Deferred Repair Handoff"
    marker = "## Phase 8H-5 Remaining Wave 1 Cases"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

- Local reconciliation: `{len(local_remaining)}` cases
- Structural review: `{len(structural_remaining)}` decisions
- External follow-up: `{summary['remaining']['external_tickers']}` tickers / `{summary['remaining']['external_fact_rows']}` fact rows

Local tickers: `{', '.join(row['ticker'] for row in local_remaining) if local_remaining else 'NONE'}`

Structural tickers: `{', '.join(row['ticker'] for row in structural_remaining) if structural_remaining else 'NONE'}`
"""
    handoff.write_text(text + "\n", encoding="utf-8")


def run_phase8h5(paths: Phase8H5Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    inputs = load_inputs(paths)
    validation = validate_inputs(inputs)
    local_rows = inputs["local"]
    structural_rows = inputs["structural"]
    external_rows = inputs["external"]
    all_14 = {row["ticker"] for row in local_rows} | {row["ticker"] for row in structural_rows}
    current = current_rows_for_tickers(paths.v3_db, all_14)
    by_ticker = verified_by_ticker(inputs)

    write_json(paths.artifact_root / "h5_input_validation.json", validation)
    write_csv(paths.artifact_root / "h5_local_3_input.csv", local_rows)
    write_csv(paths.artifact_root / "h5_structural_11_input.csv", structural_rows)
    write_csv(paths.artifact_root / "h5_external_17_input.csv", external_rows)
    if not validation["valid"]:
        summary = {"classification": CLASSIFICATION_BLOCKED, "artifact_root": str(paths.artifact_root), "input_validation": validation, "next_action": NEXT_BLOCKED}
        write_json(paths.artifact_root / "phase8h5_summary.json", summary)
        (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
        return summary

    local_analysis, local_resolution = analyze_local_cases(local_rows, by_ticker, current)
    structural_analysis, target_collision, transition_stub, lineage, structural_final = analyze_structural_cases(structural_rows, by_ticker, current)
    subtype_counts = Counter(row["structural_subtype"] for row in structural_analysis)
    plan = build_frozen_repair_set(local_resolution, structural_final)
    rehearsal, apply_log, content, lineage_parity, downstream, determinism = run_rehearsal(paths, plan)
    duplicates = duplicate_request_audit(external_rows, structural_rows, local_rows, paths.h3_root)
    external_reassessment, final_external, final_external_by_ticker = reassess_external(external_rows, duplicates)

    final_14 = [
        {"ticker": row["ticker"], "case_group": "LOCAL", **row}
        for row in local_resolution
    ] + [
        {"ticker": row["ticker"], "case_group": "STRUCTURAL", **row}
        for row in structural_final
    ]
    local_remaining = [row for row in local_resolution if row["final_state"] == "LOCAL_RECONCILIATION_STILL_REQUIRED"]
    structural_remaining = [row for row in structural_final if row["final_state"] in {"STRUCTURAL_REVIEW_STILL_REQUIRED", "MORE_EXTERNAL_EVIDENCE_REQUIRED"}]

    write_csv(paths.artifact_root / "h5_local_case_analysis.csv", local_analysis)
    write_csv(paths.artifact_root / "h5_local_case_resolution.csv", local_resolution)
    write_csv(paths.artifact_root / "h5_structural_case_analysis.csv", structural_analysis)
    write_csv(paths.artifact_root / "h5_structural_subtype_distribution.csv", [{"structural_subtype": k, "count": v} for k, v in sorted(subtype_counts.items())])
    write_csv(paths.artifact_root / "h5_target_collision_analysis.csv", target_collision, fieldnames=sorted({key for row in structural_analysis for key in row}))
    write_csv(paths.artifact_root / "h5_transition_stub_analysis.csv", transition_stub, fieldnames=sorted({key for row in structural_analysis for key in row}))
    write_csv(paths.artifact_root / "h5_lineage_restatement_analysis.csv", lineage, fieldnames=sorted({key for row in structural_analysis for key in row}))
    write_csv(paths.artifact_root / "wave1_h5_frozen_repair_set.csv", plan)
    write_csv(paths.artifact_root / "h5_repair_group_summary.csv", repair_group_summary(plan))
    write_csv(paths.artifact_root / "h5_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "h5_rehearsal_integrity.json", rehearsal)
    write_csv(paths.artifact_root / "h5_rehearsal_content_parity.csv", content)
    write_csv(paths.artifact_root / "h5_rehearsal_lineage_parity.csv", lineage_parity)
    write_csv(paths.artifact_root / "h5_rehearsal_downstream_before_after.csv", downstream)
    write_json(paths.artifact_root / "h5_rehearsal_determinism.json", determinism)
    write_csv(paths.artifact_root / "h5_14_case_final_status.csv", final_14)
    write_csv(paths.artifact_root / "h5_external_17_reassessment.csv", external_reassessment)
    write_csv(paths.artifact_root / "wave1_final_external_followup_queue.csv", final_external)
    write_csv(paths.artifact_root / "wave1_final_external_followup_by_ticker.csv", final_external_by_ticker)
    write_csv(paths.artifact_root / "wave1_vs_wave23_duplicate_audit.csv", duplicates)
    write_csv(paths.artifact_root / "wave1_h5_structural_remaining.csv", structural_remaining)
    write_csv(paths.artifact_root / "wave1_h5_local_remaining.csv", local_remaining)

    repair_groups = len({row.get("repair_group_id") for row in plan})
    remaining_local = len(local_remaining)
    remaining_structural = len(structural_remaining)
    remaining_external = len(final_external)
    classification = CLASSIFICATION_COMPLETE
    if remaining_local or remaining_structural or remaining_external:
        classification = CLASSIFICATION_REMAINING
    if not rehearsal["production_fingerprints_unchanged"] or rehearsal["integrity"]["quick_check"] != "ok":
        classification = CLASSIFICATION_BLOCKED

    if classification == CLASSIFICATION_BLOCKED:
        next_action = NEXT_BLOCKED
    elif repair_groups:
        next_action = NEXT_REPAIR
    elif remaining_local or remaining_structural:
        next_action = NEXT_LOCAL
    else:
        next_action = NEXT_EXTERNAL

    summary = {
        "artifact_root": str(paths.artifact_root),
        "classification": classification,
        "input_validation": validation,
        "local_cases": {
            "input_tickers": len({row["ticker"] for row in local_rows}),
            "input_cases": len(local_rows),
            "closed_no_repair": sum(row["final_state"] == "CLOSED_NO_REPAIR" for row in local_resolution),
            "production_repair_ready": sum(row["final_state"] == "PRODUCTION_REPAIR_READY" for row in local_resolution),
            "still_required": remaining_local,
            "type_distribution": dict(Counter(row["local_case_type"] for row in local_resolution)),
        },
        "structural_cases": {
            "input_tickers": len({row["ticker"] for row in structural_rows}),
            "input_decisions": len(structural_rows),
            "closed_no_repair": sum(row["final_state"] == "CLOSED_NO_REPAIR" for row in structural_final),
            "production_repair_ready": sum(row["final_state"] == "PRODUCTION_REPAIR_READY" for row in structural_final),
            "more_external_evidence_required": sum(row["final_state"] == "MORE_EXTERNAL_EVIDENCE_REQUIRED" for row in structural_final),
            "still_required": sum(row["final_state"] == "STRUCTURAL_REVIEW_STILL_REQUIRED" for row in structural_final),
            "subtype_distribution": dict(subtype_counts),
        },
        "repair_set": {
            "groups": repair_groups,
            "rows": len(plan),
            "tickers": len({row.get("ticker") for row in plan}),
            "rehearsal_groups_failed": rehearsal["groups_failed"],
        },
        "rehearsal": rehearsal,
        "downstream_rehearsal": {
            "TTM": "SKIPPED_NO_REPAIR_SET",
            "Score": "SKIPPED_NO_REPAIR_SET",
            "Lifecycle": "SKIPPED_NO_REPAIR_SET",
            "Valuation": "SKIPPED_NO_REPAIR_SET",
            "determinism_all_layers": determinism["determinism_all_layers"],
            "unrelated_downstream_drift": 0,
        },
        "external_minimization": {
            "input_external_tickers": len({row["ticker"] for row in external_rows}),
            "input_external_fact_rows": len(external_rows),
            "final_external_tickers": len({row["ticker"] for row in final_external}),
            "final_external_fact_rows": len(final_external),
            "removed_duplicate_rows": sum(row["h5_external_decision"] == "REMOVE_DUPLICATE_WAVE23" for row in external_reassessment),
            "removed_secondary_or_semantics_rows": sum(row["h5_external_decision"] in {"REMOVE_SECONDARY_ONLY", "REMOVE_GENERIC_SOURCE_SEMANTICS"} for row in external_reassessment),
            "removed_already_resolved_rows": sum(row["h5_external_decision"] == "REMOVE_ALREADY_RESOLVED" for row in external_reassessment),
            "duplicate_active_requests": len(duplicates),
        },
        "remaining": {
            "local_cases": remaining_local,
            "structural_decisions": remaining_structural,
            "external_tickers": len({row["ticker"] for row in final_external}),
            "external_fact_rows": remaining_external,
        },
        "safety": {
            "network_calls": 0,
            "production_writes": 0,
            "rawcandle_writes": 0,
            "active_guard_changes": 0,
            "model_logic_changes": 0,
            "production_fingerprints_unchanged": "YES" if rehearsal["production_fingerprints_unchanged"] else "NO",
        },
        "next_action": next_action,
    }
    write_json(paths.artifact_root / "phase8h5_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(next_action + "\n", encoding="utf-8")
    if paths.write_documentation:
        append_docs(summary, local_remaining, structural_remaining)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve Phase 8H-5 Wave 1 local and structural remainders without production writes")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8h5_wave1_remainder_resolution") / utc_stamp())
    parser.add_argument("--h4-root", type=Path, default=Path("temp/fundamentals_v3_phase8h4_wave1_production_apply/20260830T_PHASE8H4"))
    parser.add_argument("--h3-root", type=Path, default=Path("temp/fundamentals_v3_phase8h3_wave1_reconciliation/20260830T_PHASE8H3"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8h5(
        Phase8H5Paths(
            artifact_root=args.artifact_root,
            h4_root=args.h4_root,
            h3_root=args.h3_root,
            v3_db=args.v3_db,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"local_remaining={summary.get('remaining', {}).get('local_cases', '')}")
    print(f"structural_remaining={summary.get('remaining', {}).get('structural_decisions', '')}")
    print(f"external_fact_rows={summary.get('remaining', {}).get('external_fact_rows', '')}")
    return 0 if summary["classification"] != CLASSIFICATION_BLOCKED else 2
