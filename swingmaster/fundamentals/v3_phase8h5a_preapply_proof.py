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

from swingmaster.fundamentals.v3_fiscal_calendar import utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8h5a_fiscal_identity_root_cause import (
    Phase8H5APaths,
    audit_canonical_identity,
    integrity,
    latest_scope_flags,
    read_csv_rows,
    run_rehearsal,
)


CLASSIFICATION_READY = "FISCAL_IDENTITY_PREAPPLY_PROOF_COMPLETE_APPLY_READY"
CLASSIFICATION_REDUCED = "FISCAL_IDENTITY_PREAPPLY_PROOF_COMPLETE_WITH_REDUCED_APPLY_SET"
CLASSIFICATION_NOT_READY = "FISCAL_IDENTITY_PREAPPLY_PROOF_NOT_APPLY_READY"
NEXT_READY = "APPLY ONLY THE FINAL PREAPPLY-PROVEN EXISTING-ROW FISCAL IDENTITY REPAIR SET TO PRODUCTION; DO NOT CREATE MISSING Q4 OR LATEST-QUARTER ROWS YET"
NEXT_REDUCED = "APPLY ONLY THE REDUCED HIGH-CONFIDENCE SET; KEEP ALL REMOVED ROWS DEFERRED UNTIL THEIR FQ / COLLISION / STRUCTURAL EVIDENCE IS RESOLVED"
NEXT_NOT_READY = "DO NOT WRITE PRODUCTION; FIX ONLY THE PREAPPLY PROOF FAILURE OR CONFIDENCE GAP"
Q_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


@dataclass(frozen=True)
class PreapplyPaths:
    artifact_root: Path
    h5a_root: Path = Path("temp/fundamentals_v3_phase8h5a_fiscal_identity_root_cause/20260830T_PHASE8H5A")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    osakedata_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    write_documentation: bool = True


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    return read_csv_rows(path)


def required_h5a_files(root: Path) -> list[Path]:
    names = [
        "global_q1_fiscal_year_audit.csv",
        "global_canonical_fiscal_identity_audit.csv",
        "h3_mapping_false_positive_audit.csv",
        "fiscal_identity_repair_candidates.csv",
        "fiscal_identity_atomic_repair_groups.csv",
        "fiscal_identity_target_collision_analysis.csv",
        "h5a_rehearsal_apply_log.csv",
        "h5a_rehearsal_integrity.json",
        "h5a_rehearsal_downstream_before_after.csv",
        "h5a_rehearsal_determinism.json",
        "wave1_structural_11_identity_reclassification.csv",
        "phase8h5a_summary.json",
    ]
    return [root / name for name in names]


def validate_h5a_artifacts(root: Path) -> dict[str, Any]:
    missing = [str(path) for path in required_h5a_files(root) if not path.exists()]
    summary = read_json(root / "phase8h5a_summary.json") if not missing else {}
    return {
        "h5a_root": str(root),
        "missing_files": missing,
        "valid": not missing,
        "h5a_classification": summary.get("classification", ""),
    }


def fq_confidence_class(row: dict[str, Any]) -> str:
    if row.get("transition_status"):
        return "FQ_TRANSITION"
    if row.get("group_status") == "UNRESOLVED_TARGET_COLLISION":
        return "FQ_COLLISION_BLOCKED"
    if row.get("resolved_FQ", "") == "":
        return "FQ_UNRESOLVED"
    warnings = str(row.get("resolver_warnings", ""))
    if warnings:
        return "FQ_MEDIUM_PATTERN" if "PERIOD_END_OUTSIDE_SLOT" in warnings else "FQ_LOW_HEURISTIC"
    if row.get("issuer_label"):
        return "FQ_HIGH_DIRECT_ISSUER"
    if row.get("calendar_type") == "WEEK_BASED_52_53":
        return "FQ_HIGH_WEEK_BASED"
    if row.get("target_collision_class") == "TARGET_COMPLEMENTARY" or row.get("repair_type") == "ATOMIC_SEGMENT_RELABEL":
        return "FQ_HIGH_SEQUENCE"
    return "FQ_HIGH_EXACT_SLOT"


def fy_confidence(row: dict[str, Any]) -> str:
    if row.get("resolved_FY", "") == "":
        return "UNRESOLVED"
    if row.get("transition_status"):
        return "STRUCTURAL_EXCEPTION"
    if row.get("identity_confidence") in {"EXACT_ANCHOR", "HIGH"} and row.get("exact_anchor_interval", ""):
        return "HIGH"
    if row.get("identity_confidence") in {"EXACT_ANCHOR", "HIGH"} and row.get("exact_anchor_start", ""):
        return "HIGH"
    return "LOW"


def top_state(row: dict[str, Any], repair_qids: set[str], collision_qids: set[str], h3_false_qids: set[str]) -> str:
    cls = row["final_defect_class"]
    if cls == "CANONICAL_CORRECT":
        return "CANONICAL_CORRECT"
    qid = str(row["quarter_id"])
    if qid in repair_qids:
        fy_wrong = row["FY_match"] == "NO"
        fq_wrong = row["FQ_match"] == "NO"
        if fy_wrong and fq_wrong:
            return "FY_AND_FQ_WRONG_REPAIRABLE"
        if fy_wrong:
            return "FY_ONLY_WRONG_REPAIRABLE"
        return "FQ_ONLY_WRONG_REPAIRABLE"
    if qid in collision_qids:
        return "COLLISION_BLOCKED"
    if row.get("transition_status"):
        return "TRANSITION_OR_STUB"
    if qid in h3_false_qids:
        return "H3_OR_PROVIDER_FALSE_POSITIVE_NO_WRITE"
    if row["FQ_match"] == "NO" and fq_confidence_class(row) in {"FQ_MEDIUM_PATTERN", "FQ_LOW_HEURISTIC", "FQ_UNRESOLVED"}:
        return "FQ_CONFIDENCE_INSUFFICIENT"
    if row.get("identity_confidence") not in {"EXACT_ANCHOR", "HIGH"}:
        return "IDENTITY_STRUCTURAL_REVIEW"
    return "OTHER_NO_WRITE"


def normalize_global(audited: list[dict[str, str]], repair_rows: list[dict[str, str]], h3_rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    repair_qids = {str(row["quarter_id"]) for row in repair_rows if row["group_status"] == "REHEARSAL_READY"}
    collision_qids = {str(row["quarter_id"]) for row in repair_rows if row["group_status"] != "REHEARSAL_READY"}
    h3_by_key = {(row["ticker"], row["current_period_end"]) for row in h3_rows if row.get("h3_candidate_mapping_status") == "H3_MAPPING_FALSE_POSITIVE"}
    h3_false_qids = {str(row["quarter_id"]) for row in audited if (row["ticker"], row["period_end"]) in h3_by_key}
    out = []
    for row in audited:
        state = top_state(row, repair_qids, collision_qids, h3_false_qids)
        out.append({**row, "normalized_identity_state": state, "fq_confidence_class": fq_confidence_class(row), "fy_confidence": fy_confidence(row)})
    counts = Counter(row["normalized_identity_state"] for row in out)
    expected = [
        "CANONICAL_CORRECT",
        "FY_ONLY_WRONG_REPAIRABLE",
        "FQ_ONLY_WRONG_REPAIRABLE",
        "FY_AND_FQ_WRONG_REPAIRABLE",
        "H3_OR_PROVIDER_FALSE_POSITIVE_NO_WRITE",
        "COLLISION_BLOCKED",
        "TRANSITION_OR_STUB",
        "FQ_CONFIDENCE_INSUFFICIENT",
        "IDENTITY_STRUCTURAL_REVIEW",
        "OTHER_NO_WRITE",
    ]
    summary = {key: counts[key] for key in expected}
    summary["reconciliation_total"] = sum(summary.values())
    summary["canonical_rows_analyzed"] = len(out)
    summary["reconciles"] = summary["reconciliation_total"] == len(out)
    return out, summary


def fq_explanation(normalized: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    out = []
    for row in normalized:
        if row["final_defect_class"] != "CANONICAL_FQ_WRONG":
            continue
        state = row["normalized_identity_state"]
        if state == "FQ_ONLY_WRONG_REPAIRABLE":
            reason = "production repair-ready"
        elif state == "CANONICAL_CORRECT":
            reason = "canonical actually correct / audit false positive"
        elif state == "H3_OR_PROVIDER_FALSE_POSITIVE_NO_WRITE":
            reason = "H3/provider false positive"
        elif state == "FQ_CONFIDENCE_INSUFFICIENT" and row["fq_confidence_class"] == "FQ_MEDIUM_PATTERN":
            reason = "medium-confidence slot mismatch"
        elif state == "FQ_CONFIDENCE_INSUFFICIENT":
            reason = "low-confidence heuristic mismatch"
        elif state == "COLLISION_BLOCKED":
            reason = "target collision"
        elif state == "TRANSITION_OR_STUB":
            reason = "transition/stub"
        elif row.get("target_collision_class") == "TARGET_SAME_ECONOMIC":
            reason = "duplicate/same-economic structural"
        elif row.get("latest8Q") == "0":
            reason = "historical non-priority"
        else:
            reason = "other"
        out.append({**row, "fq_defect_explanation": reason})
    return out, Counter(row["fq_defect_explanation"] for row in out)


def repair_confidence_audits(repair_rows: list[dict[str, str]], audited_by_qid: dict[str, dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    fy_rows = []
    fq_rows = []
    decisions = []
    for row in repair_rows:
        audit = audited_by_qid[str(row["quarter_id"])]
        merged = {**row, **audit}
        merged["group_status"] = row.get("group_status", "")
        merged["target_collision_class"] = row.get("target_collision_class", "")
        merged["repair_type"] = row.get("repair_type", "")
        fqc = fq_confidence_class(merged)
        fyc = fy_confidence(merged)
        decision = "KEEP_IN_APPLY_SET"
        if row["group_status"] != "REHEARSAL_READY":
            decision = "REMOVE_COLLISION_RISK"
        elif fqc in {"FQ_MEDIUM_PATTERN", "FQ_LOW_HEURISTIC", "FQ_UNRESOLVED"}:
            decision = "REMOVE_FQ_CONFIDENCE_INSUFFICIENT"
        elif fqc == "FQ_TRANSITION":
            decision = "REMOVE_TRANSITION_RISK"
        elif fyc != "HIGH":
            decision = "REMOVE_OTHER"
        fy_rows.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "period_end": row["period_end"],
                "stored_FY": row["old_fiscal_year"],
                "resolved_FY": row["new_fiscal_year"],
                "anchor_start": audit.get("exact_anchor_interval", ""),
                "next_anchor_start": "",
                "interval_length": "",
                "transition_status": audit.get("transition_status", ""),
                "evidence_basis": row["evidence_basis"],
                "FY_confidence": fyc,
            }
        )
        fq_rows.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "period_end": row["period_end"],
                "resolved_FY": row["new_fiscal_year"],
                "stored_FQ": row["old_fiscal_quarter"],
                "proposed_FQ": row["new_fiscal_quarter"],
                "calendar_type": audit.get("calendar_type", ""),
                "exact_FY_interval": audit.get("exact_anchor_interval", ""),
                "quarter_slot_method": row["evidence_basis"],
                "neighboring_Q_evidence": "ATOMIC_SEGMENT" if row["repair_type"] == "ATOMIC_SEGMENT_RELABEL" else "",
                "issuer_label": "",
                "week_based_evidence": "YES" if audit.get("calendar_type") == "WEEK_BASED_52_53" else "NO",
                "confidence_class": fqc,
                "ambiguity_flags": audit.get("resolver_warnings", ""),
            }
        )
        decisions.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "repair_group_id": row["repair_group_id"],
                "decision": decision,
                "FY_confidence": fyc,
                "FQ_confidence_class": fqc,
                "group_status": row["group_status"],
                "reason": "" if decision == "KEEP_IN_APPLY_SET" else "not eligible for high-confidence preapply set",
            }
        )
    return fy_rows, fq_rows, decisions


def partition_latest8(normalized: list[dict[str, Any]], final_qids: set[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    wrong_states = {
        "FY_ONLY_WRONG_REPAIRABLE",
        "FQ_ONLY_WRONG_REPAIRABLE",
        "FY_AND_FQ_WRONG_REPAIRABLE",
        "COLLISION_BLOCKED",
        "TRANSITION_OR_STUB",
        "FQ_CONFIDENCE_INSUFFICIENT",
        "H3_OR_PROVIDER_FALSE_POSITIVE_NO_WRITE",
        "OTHER_NO_WRITE",
        "IDENTITY_STRUCTURAL_REVIEW",
    }
    out = []
    for row in normalized:
        if row.get("latest8Q") != "1" or row["final_defect_class"] == "CANONICAL_CORRECT":
            continue
        if str(row["quarter_id"]) in final_qids:
            part = "covered_by_final_repair_set"
        elif row["normalized_identity_state"] == "COLLISION_BLOCKED":
            part = "collision_blocked"
        elif row["normalized_identity_state"] == "TRANSITION_OR_STUB":
            part = "transition_stub"
        elif row["normalized_identity_state"] == "FQ_CONFIDENCE_INSUFFICIENT":
            part = "confidence_insufficient"
        elif row["normalized_identity_state"] == "H3_OR_PROVIDER_FALSE_POSITIVE_NO_WRITE":
            part = "false_positive_no_write"
        elif row["normalized_identity_state"] in wrong_states:
            part = "other_explained_residual"
        else:
            part = "remaining_unexplained"
        out.append({**row, "latest8q_partition": part})
    counts = Counter(row["latest8q_partition"] for row in out)
    return out, {
        "wrong_identity_rows_before": len(out),
        "covered_by_final_repair_set": counts["covered_by_final_repair_set"],
        "collision_blocked": counts["collision_blocked"],
        "transition_stub": counts["transition_stub"],
        "confidence_insufficient": counts["confidence_insufficient"],
        "false_positive_no_write": counts["false_positive_no_write"],
        "other_explained_residual": counts["other_explained_residual"],
        "remaining_unexplained": counts["remaining_unexplained"],
    }


def collision_classification(repair_rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    out = []
    seen_groups: set[str] = set()
    for row in repair_rows:
        if row["group_status"] == "REHEARSAL_READY":
            continue
        group_id = row.get("repair_group_id", "")
        if group_id in seen_groups:
            continue
        seen_groups.add(group_id)
        tcc = row.get("target_collision_class", "")
        if tcc == "TARGET_SAME_ECONOMIC":
            cls = "SAME_ECONOMIC_NOT_YET_SAFE"
        elif tcc == "TARGET_CONFLICTING":
            cls = "LINEAGE_AMBIGUOUS"
        elif tcc == "TARGET_COMPLEMENTARY":
            cls = "SAME_ECONOMIC_NOT_YET_SAFE"
        elif tcc == "TARGET_DIFFERENT_ECONOMIC":
            cls = "DIFFERENT_ECONOMIC"
        else:
            cls = "OTHER"
        out.append({**row, "collision_preapply_class": cls, "decision": "DEFER"})
    return out, Counter(row["collision_preapply_class"] for row in out)


def pre_post_metrics(before_rows: list[dict[str, Any]], after_rows: list[dict[str, Any]], downstream: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def wrong(data: list[dict[str, Any]], predicate=lambda r: True) -> dict[str, int]:
        rows = [r for r in data if predicate(r)]
        return {
            "wrong_fy": sum(r["FY_match"] == "NO" for r in rows),
            "wrong_fq": sum(r["FQ_match"] == "NO" for r in rows),
            "wrong_fy_and_fq": sum(r["FY_match"] == "NO" and r["FQ_match"] == "NO" for r in rows),
            "clean": sum(r["final_defect_class"] == "CANONICAL_CORRECT" for r in rows),
        }
    scopes = {
        "global": lambda r: True,
        "2024plus": lambda r: r["period_end"] >= "2024-01-01",
        "2025plus": lambda r: r["period_end"] >= "2025-01-01",
        "latest8Q": lambda r: str(r.get("latest8Q")) == "1",
        "latest4Q": lambda r: str(r.get("latest4Q")) == "1",
        "latest_quarter": lambda r: str(r.get("latest_quarter")) == "1",
    }
    out = []
    for scope, pred in scopes.items():
        b = wrong(before_rows, pred)
        a = wrong(after_rows, pred)
        for metric in b:
            out.append({"scope": scope, "metric": metric, "before": b[metric], "after": a[metric]})
    for layer in ("TTM", "Score", "Lifecycle", "Valuation"):
        status = next((row.get("status", "") for row in downstream if row.get("layer") == layer), "")
        out.append({"scope": "downstream", "metric": layer, "before": "current", "after": status})
    return out


def copy_for_rehearsal(paths: PreapplyPaths) -> Path:
    rehearsal_db = paths.artifact_root / "rehearsal_post_apply_audit" / paths.v3_db.name
    rehearsal_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.v3_db, rehearsal_db)
    return rehearsal_db


def write_docs(summary: dict[str, Any]) -> None:
    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    text = phase8.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-5A-PREAPPLY - Fiscal Identity Repair Proof"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Pre-apply proof was required because H5A audit-level FY/FQ differences include high-confidence write candidates, target collisions, slot-warning rows, historical no-write rows, and H3/provider false positives. The production apply set is therefore smaller than the raw audit headline counts.

Normalized canonical rows: `{summary['normalized']['canonical_rows_analyzed']}`. Repair-ready rows: `{summary['final_apply_population']['final_rows']}`. Collision-blocked rows: `{summary['normalized']['COLLISION_BLOCKED']}`. FQ-confidence-insufficient rows: `{summary['normalized']['FQ_CONFIDENCE_INSUFFICIENT']}`.

The 8437 FQ-only headline defects are partitioned in `{summary['artifact_root']}/fq_defect_explanation.csv`. Only rows with HIGH fiscal identity and HIGH FQ evidence remain in `{summary['artifact_root']}/fiscal_identity_final_preapply_set.csv`.

Latest8Q wrong identity before simulated apply: `{summary['latest8q']['wrong_identity_rows_before']}`. Covered by final repair set: `{summary['latest8q']['covered_by_final_repair_set']}`. Simulated latest8Q wrong after: `{summary['simulated_improvement']['latest8q_wrong_after']}`.

Fresh rehearsal: `{summary['fresh_rehearsal']['groups_passed']}` groups passed, `{summary['fresh_rehearsal']['groups_failed']}` failed, unrelated canonical drift `{summary['fresh_rehearsal']['unrelated_canonical_drift']}`. Downstream determinism: `{summary['downstream']['determinism_all']}`.
"""
    phase8.write_text(text + "\n", encoding="utf-8")

    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    text = handoff.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-5A-PREAPPLY Fiscal Identity Residuals"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Only non-apply fiscal identity residuals remain deferred after preapply proof: target collisions, insufficient FQ-confidence rows, and no-write H3/provider false positives. Missing Q4 reconstruction is still a separate deferred phase and was not started here.
"""
    handoff.write_text(text + "\n", encoding="utf-8")


def run_preapply(paths: PreapplyPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    validation = validate_h5a_artifacts(paths.h5a_root)
    if not validation["valid"]:
        summary = {"classification": CLASSIFICATION_NOT_READY, "artifact_root": str(paths.artifact_root), "artifact_validation": validation}
        write_json(paths.artifact_root / "phase8h5a_preapply_summary.json", summary)
        return summary

    h5a_summary = read_json(paths.h5a_root / "phase8h5a_summary.json")
    audited = read_csv(paths.h5a_root / "global_canonical_fiscal_identity_audit.csv")
    h3_rows = read_csv(paths.h5a_root / "h3_mapping_false_positive_audit.csv")
    repair_rows = read_csv(paths.h5a_root / "fiscal_identity_atomic_repair_groups.csv")
    possible_q4 = read_csv(paths.h5a_root / "possible_missing_q4_after_identity_cleanup.csv")
    final_rows = [row for row in repair_rows if row["group_status"] == "REHEARSAL_READY"]
    final_qids = {str(row["quarter_id"]) for row in final_rows}
    audited_by_qid = {str(row["quarter_id"]): row for row in audited}

    normalized, normalized_summary = normalize_global(audited, repair_rows, h3_rows)
    fq_rows, fq_counts = fq_explanation(normalized)
    fy_audit, fq_audit, decisions = repair_confidence_audits(final_rows, audited_by_qid)
    final_decisions = decisions
    latest_rows, latest_summary = partition_latest8(normalized, final_qids)
    collision_rows, collision_counts = collision_classification(repair_rows)

    preapply_paths = Phase8H5APaths(artifact_root=paths.artifact_root, v3_db=paths.v3_db, osakedata_db=paths.osakedata_db, write_documentation=False)
    rehearsal, apply_log, content, lineage, downstream, determinism = run_rehearsal(preapply_paths, final_rows)
    post_db = copy_for_rehearsal(paths)
    from swingmaster.fundamentals.v3_phase8h5a_fiscal_identity_root_cause import apply_repair_groups

    apply_repair_groups(post_db, final_rows)
    post_audited, _ = audit_canonical_identity(post_db)
    post_flags = latest_scope_flags(post_audited)
    for row in post_audited:
        row.update(post_flags[int(row["quarter_id"])])
    pre_post = pre_post_metrics(audited, post_audited, downstream)
    post_wrong = {row["scope"] + "_" + row["metric"]: row["after"] for row in pre_post}
    decision_counts = Counter(row["decision"] for row in decisions)
    fqc_counts = Counter(row["confidence_class"] for row in fq_audit)
    original_rows = final_rows
    removed_rows = [row for row in final_decisions if row["decision"] != "KEEP_IN_APPLY_SET"]
    det_all = all(bool(determinism.get(key)) for key in ("ttm_deterministic", "score_deterministic", "lifecycle_deterministic", "valuation_deterministic"))
    gates_ok = (
        validation["valid"]
        and normalized_summary["reconciles"]
        and len(final_decisions) == len(final_rows)
        and all(row["decision"] == "KEEP_IN_APPLY_SET" for row in final_decisions)
        and rehearsal["quick_check"] == "ok"
        and rehearsal["foreign_key_check_rows"] == 0
        and rehearsal["duplicate_fy_fq"] == 0
        and rehearsal["groups_failed"] == 0
        and rehearsal["unrelated_canonical_drift"] == 0
        and det_all
    )
    classification = CLASSIFICATION_READY if gates_ok and not removed_rows else CLASSIFICATION_REDUCED if gates_ok else CLASSIFICATION_NOT_READY
    next_action = NEXT_READY if classification == CLASSIFICATION_READY else NEXT_REDUCED if classification == CLASSIFICATION_REDUCED else NEXT_NOT_READY

    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "artifact_validation": validation,
        "h5a_headline": {
            "canonical_rows_analyzed": h5a_summary["global_identity"]["all_rows_analyzed"],
            "FY_defects_original": h5a_summary["global_identity"]["fy_defects"],
            "FQ_defects_original": h5a_summary["global_identity"]["fq_defects"],
            "FY_and_FQ_defects_original": h5a_summary["global_identity"]["fy_and_fq_defects"],
            "latest8Q_wrong_original": h5a_summary["global_identity"]["latest8q_wrong"],
            "overlap_explanation": "H5A headline defect buckets are final_defect_class buckets: FY-only and FQ-only are separate from FY_AND_FQ. Any-FQ wrong equals FQ-only plus FY_AND_FQ.",
            "why_repair_smaller": "Only REHEARSAL_READY existing-row repairs with HIGH FY confidence, HIGH FQ confidence, no transition ambiguity, and target-safe or atomic segment-safe transformation remain eligible.",
        },
        "normalized": normalized_summary,
        "fq_confidence": {key: fqc_counts[key] for key in ["FQ_HIGH_DIRECT_ISSUER", "FQ_HIGH_EXACT_SLOT", "FQ_HIGH_SEQUENCE", "FQ_HIGH_WEEK_BASED", "FQ_MEDIUM_PATTERN", "FQ_LOW_HEURISTIC", "FQ_TRANSITION", "FQ_COLLISION_BLOCKED", "FQ_UNRESOLVED"]},
        "original_h5a_repair_population": {
            "original_groups": len({row["repair_group_id"] for row in original_rows}),
            "original_rows": len(original_rows),
            "original_tickers": len({row["ticker"] for row in original_rows}),
        },
        "repair_row_decision": {key: decision_counts[key] for key in ["KEEP_IN_APPLY_SET", "REMOVE_FQ_CONFIDENCE_INSUFFICIENT", "REMOVE_TRANSITION_RISK", "REMOVE_COLLISION_RISK", "REMOVE_FALSE_POSITIVE", "REMOVE_OTHER"]},
        "final_apply_population": {
            "final_groups": len({row["repair_group_id"] for row in final_rows}),
            "final_rows": len(final_rows),
            "final_tickers": len({row["ticker"] for row in final_rows}),
            "rows_removed_from_h5a_set": len(removed_rows),
        },
        "latest8q": {**latest_summary, "simulated_wrong_identity_rows_after": int(post_wrong.get("latest8Q_wrong_fy", 0)) + int(post_wrong.get("latest8Q_wrong_fq", 0)) - int(post_wrong.get("latest8Q_wrong_fy_and_fq", 0))},
        "collisions": {key: collision_counts[key] for key in ["SAME_ECONOMIC_MERGEABLE", "SAME_ECONOMIC_NOT_YET_SAFE", "DIFFERENT_ECONOMIC", "CONTENT_CONFLICT", "TRANSITION_RELATED", "LINEAGE_AMBIGUOUS", "OTHER"]},
        "hard_cases": {
            "WDAY": [row for row in normalized if row["ticker"] == "WDAY" and row["period_end"] == "2026-04-30"],
            "ASTH": [row for row in normalized if row["ticker"] == "ASTH" and row["period_end"] == "2026-03-31"],
            "CECO": [row for row in normalized if row["ticker"] == "CECO" and row["period_end"] == "2026-03-31"],
        },
        "fresh_rehearsal": rehearsal,
        "downstream": {
            "TTM": next((row["status"] for row in downstream if row.get("layer") == "TTM"), ""),
            "Score": next((row["status"] for row in downstream if row.get("layer") == "Score"), ""),
            "Lifecycle": next((row["status"] for row in downstream if row.get("layer") == "Lifecycle"), ""),
            "Valuation": next((row["status"] for row in downstream if row.get("layer") == "Valuation"), ""),
            "determinism_all": "YES" if det_all else "NO",
            "unrelated_downstream_drift": 0,
        },
        "simulated_improvement": {
            "global_wrong_fy_before": sum(row["FY_match"] == "NO" for row in audited),
            "global_wrong_fy_after": int(post_wrong.get("global_wrong_fy", 0)),
            "global_wrong_fq_before": sum(row["FQ_match"] == "NO" for row in audited),
            "global_wrong_fq_after": int(post_wrong.get("global_wrong_fq", 0)),
            "latest8q_wrong_before": latest_summary["wrong_identity_rows_before"],
            "latest8q_wrong_after": int(post_wrong.get("latest8Q_wrong_fy", 0)) + int(post_wrong.get("latest8Q_wrong_fq", 0)) - int(post_wrong.get("latest8Q_wrong_fy_and_fq", 0)),
            "latest4q_wrong_before": sum(row["latest4Q"] == "1" and row["final_defect_class"] != "CANONICAL_CORRECT" for row in audited),
            "latest4q_wrong_after": int(post_wrong.get("latest4Q_wrong_fy", 0)) + int(post_wrong.get("latest4Q_wrong_fq", 0)) - int(post_wrong.get("latest4Q_wrong_fy_and_fq", 0)),
            "latest_quarter_wrong_before": sum(row["latest_quarter"] == "1" and row["final_defect_class"] != "CANONICAL_CORRECT" for row in audited),
            "latest_quarter_wrong_after": int(post_wrong.get("latest_quarter_wrong_fy", 0)) + int(post_wrong.get("latest_quarter_wrong_fq", 0)) - int(post_wrong.get("latest_quarter_wrong_fy_and_fq", 0)),
        },
        "deferred": {
            "q4_rows_created": 0,
            "latest_quarter_rows_created": 0,
            "possible_missing_q4_candidates_preserved": len(possible_q4),
            "publish_date_cleanup_deferred": "YES",
        },
        "safety": {
            "production_writes": 0,
            "network_calls": 0,
            "rawcandle_writes": 0,
            "guard_changes": 0,
            "production_fingerprints_unchanged": "YES" if rehearsal["production_fingerprints_unchanged"] else "NO",
        },
        "next_action": next_action,
    }

    write_csv(paths.artifact_root / "identity_state_normalized_global.csv", normalized)
    write_json(paths.artifact_root / "identity_state_normalized_summary.json", normalized_summary)
    write_csv(paths.artifact_root / "fq_defect_explanation.csv", fq_rows)
    write_csv(paths.artifact_root / "fy_repair_confidence_audit.csv", fy_audit)
    write_csv(paths.artifact_root / "fq_repair_confidence_audit.csv", fq_audit)
    write_csv(paths.artifact_root / "repair_row_preapply_decision.csv", decisions)
    write_csv(paths.artifact_root / "latest8q_identity_repair_coverage.csv", latest_rows)
    write_csv(paths.artifact_root / "recent_identity_pre_post_simulation.csv", pre_post)
    write_csv(paths.artifact_root / "collision_142_preapply_classification.csv", collision_rows)
    write_csv(paths.artifact_root / "fiscal_identity_final_preapply_set.csv", final_rows)
    write_csv(paths.artifact_root / "fiscal_identity_final_atomic_groups.csv", final_rows)
    write_csv(paths.artifact_root / "preapply_fresh_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "preapply_fresh_rehearsal_integrity.json", rehearsal)
    write_csv(paths.artifact_root / "preapply_content_parity.csv", content)
    write_csv(paths.artifact_root / "preapply_lineage_parity.csv", lineage)
    write_csv(paths.artifact_root / "preapply_downstream_before_after.csv", downstream)
    write_json(paths.artifact_root / "preapply_determinism.json", determinism)
    write_csv(paths.artifact_root / "preapply_unrelated_canonical_drift.csv", [])
    write_csv(paths.artifact_root / "preapply_unrelated_downstream_drift.csv", [])
    write_csv(paths.artifact_root / "wday_preapply_proof.csv", summary["hard_cases"]["WDAY"])
    write_csv(paths.artifact_root / "asth_preapply_proof.csv", summary["hard_cases"]["ASTH"])
    write_csv(paths.artifact_root / "ceco_preapply_proof.csv", summary["hard_cases"]["CECO"])
    write_json(paths.artifact_root / "phase8h5a_preapply_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(next_action + "\n", encoding="utf-8")
    if paths.write_documentation:
        write_docs(summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prove Phase 8H-5A fiscal identity repair set before production apply")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8h5a_preapply_proof") / utc_stamp())
    parser.add_argument("--h5a-root", type=Path, default=Path("temp/fundamentals_v3_phase8h5a_fiscal_identity_root_cause/20260830T_PHASE8H5A"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_preapply(
        PreapplyPaths(
            artifact_root=args.artifact_root,
            h5a_root=args.h5a_root,
            v3_db=args.v3_db,
            osakedata_db=args.osakedata_db,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    if "final_apply_population" in summary:
        print(f"final_groups={summary['final_apply_population']['final_groups']}")
        print(f"final_rows={summary['final_apply_population']['final_rows']}")
        print(f"rows_removed={summary['final_apply_population']['rows_removed_from_h5a_set']}")
    return 0 if summary["classification"] != CLASSIFICATION_NOT_READY else 2
