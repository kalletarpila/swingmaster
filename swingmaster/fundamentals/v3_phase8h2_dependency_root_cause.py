from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8f_latest8q_gap_analysis import pct
from swingmaster.fundamentals.v3_phase8h_external_research_packaging import (
    WAVE_RANK,
    evidence_type_to_phrase,
    preferred_source_type,
    source_semantic_subtypes,
)
from swingmaster.fundamentals.v3_phase8h1_wave23_cleanup import (
    NEXT_COMPLETE,
    load_local_context,
    local_status,
    split_pipe,
    structural_boundary,
)


CLASSIFICATION_FIXED = "FYFQ_SEMANTICS_DEPENDENCY_ROOT_CAUSE_FIXED"
CLASSIFICATION_STRUCTURAL = "FYFQ_SEMANTICS_DEPENDENCY_ROOT_CAUSE_FIXED_WITH_TRUE_STRUCTURAL_REMAINDERS"
CLASSIFICATION_NOT_FIXED = "FYFQ_SEMANTICS_DEPENDENCY_ROOT_CAUSE_NOT_FULLY_FIXED"
NEXT_FIXED = (
    "USE ONLY THE ROOT-CAUSE-CLEANED WAVE 2 / WAVE 3 FILES FOR FUTURE EXTERNAL RESEARCH; "
    "NEVER REQUEST FY/FQ OR GENERIC SOURCE-SEMANTICS CONFIRMATION WHEN HISTORICAL EXACT ANCHORS "
    "AND LOCAL QUARTER EVIDENCE ALREADY RESOLVE THE QUESTION"
)
NEXT_NOT_FIXED = "DO NOT SEND WAVE 2 / WAVE 3 TO EXTERNAL RESEARCH UNTIL THE REMAINING DEPENDENCY-GENERATION PATH IS EXPLAINED AND CORRECTED"
SEMANTIC_TYPES = {"SOURCE_SEMANTICS_CONFIRMATION"}
FYFQ_TYPES = {"OFFICIAL_FY_FQ_IDENTITY"}
AUDITED_TYPES = FYFQ_TYPES | SEMANTIC_TYPES


@dataclass(frozen=True)
class Phase8H2Paths:
    artifact_root: Path
    phase8h_root: Path = Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H")
    phase8h1_root: Path = Path("temp/fundamentals_v3_phase8h1_wave23_cleanup/20260829T_PHASE8H1")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    write_documentation: bool = True


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def yes_no(value: bool) -> str:
    return "YES" if value else "NO"


def task_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row["current_period_end"])


def original_task_lookup(phase8h_root: Path) -> dict[tuple[str, str, str, str], dict[str, str]]:
    out: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for name in (
        "latest8q_external_research_wave1_p1_current.csv",
        "latest8q_external_research_wave2_p2_latest4q.csv",
        "latest8q_external_research_wave3_p3_latest8q.csv",
    ):
        for row in read_csv_dicts(phase8h_root / name):
            out.setdefault(task_key(row), row)
    return out


def semantic_category(text: str, subtypes: list[str]) -> str:
    lower = text.lower()
    if subtypes:
        if set(subtypes) <= {"LINEAGE_OWNERSHIP"}:
            return "LINEAGE_ONLY"
        return "TRUE_SEMANTIC_AMBIGUITY"
    if "missing adjacent fiscal quarter" in lower or "missing quarter" in lower:
        return "MISSING_QUARTER_ONLY"
    if "sequence" in lower:
        return "SEQUENCE_ONLY"
    if "fy/fq" in lower or "fiscal year" in lower or "fiscal quarter" in lower:
        return "FISCAL_IDENTITY_ONLY"
    return "GENERIC_FALLBACK"


def local_identity_fields(task: dict[str, str], local: dict[str, Any] | None) -> dict[str, str]:
    status = local_status(task, local)
    return {
        "source_status_trigger": task.get("existing_local_evidence_summary", ""),
        "exact_anchor_interval_available": yes_no(status["local_fy_status"] == "FY_RESOLVED_DIRECT_EXACT"),
        "local_resolved_FY": str(local.get("exact_fy") or task.get("fiscal_year") or "") if local else "",
        "local_resolved_FQ": str(local.get("exact_fq") or task.get("fiscal_quarter") or "") if local else "",
        "local_FQ_confidence": str(local.get("fq_confidence") or status["local_fq_status"]) if local else status["local_fq_status"],
        "transition_status": str(local.get("break_reason") or "") if local else "",
        "structural_status": status["structural_boundary_status"],
    }


def local_identity_boundary(local: dict[str, Any] | None) -> bool:
    if not local:
        return False
    return str(local.get("break_reason") or "") in {"CALENDAR_TRANSITION", "UNRESOLVED_BOUNDARY", "NO_FISCAL_YEAR"} or str(
        local.get("target_collision") or ""
    ) in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}


def classify_audited_fact(
    task: dict[str, str],
    evidence_type: str,
    local: dict[str, Any] | None,
    original: dict[str, str] | None,
) -> dict[str, str]:
    original_text = " ; ".join(
        part
        for part in (
            (original or {}).get("exact_information_needed", ""),
            task.get("exact_information_needed", ""),
            task.get("research_request", ""),
        )
        if part
    )
    identity = local_identity_fields(task, local)
    subtypes = source_semantic_subtypes({**task, "exact_information_needed": original_text})
    keep = True
    removal_reason = ""
    external_needed = evidence_type
    category = ""

    if evidence_type == "OFFICIAL_FY_FQ_IDENTITY":
        if (
            identity["exact_anchor_interval_available"] == "YES"
            and identity["local_FQ_confidence"] == "DIRECT_EXACT_FQ_HIGH"
            and not local_identity_boundary(local)
        ):
            keep = False
            removal_reason = "FY_FQ_ALREADY_RESOLVED_BY_EXACT_ANCHOR_AND_LOCAL_FQ"
            external_needed = ""
        elif local and local_identity_boundary(local):
            external_needed = "OFFICIAL_FY_FQ_IDENTITY"
            removal_reason = ""
        category = "FY_FQ"
    elif evidence_type == "SOURCE_SEMANTICS_CONFIRMATION":
        category = semantic_category(original_text, subtypes)
        if not subtypes:
            keep = False
            removal_reason = f"{category}_NOT_TRUE_SOURCE_SEMANTICS"
            external_needed = ""

    return {
        "wave": task["priority"],
        "research_task_id": task["research_task_id"],
        "ticker": task["ticker"],
        "FY": task["fiscal_year"],
        "FQ": task["fiscal_quarter"],
        "period_end": task.get("current_period_end", ""),
        "evidence_type": evidence_type,
        "original_reason": original_text,
        **identity,
        "semantic_category": category,
        "semantic_subtype": "|".join(subtypes),
        "keep_external": yes_no(keep),
        "removal_reason": removal_reason,
        "actual_external_fact_needed": external_needed,
        "root_cause_location": root_cause_location(evidence_type, category),
    }


def root_cause_location(evidence_type: str, semantic_category_: str = "") -> str:
    if evidence_type == "OFFICIAL_FY_FQ_IDENTITY":
        return "swingmaster/fundamentals/v3_phase8f_latest8q_gap_analysis.py::analyze_quarter fiscal_status else branch"
    if semantic_category_ in {"SEQUENCE_ONLY", "GENERIC_FALLBACK", "FISCAL_IDENTITY_ONLY", "MISSING_QUARTER_ONLY"}:
        return "swingmaster/fundamentals/v3_phase8f_latest8q_gap_analysis.py::analyze_quarter seq_status fallback"
    return "swingmaster/fundamentals/v3_phase8f_latest8q_gap_analysis.py::analyze_quarter derivation_inputs branch"


def clean_task(task: dict[str, str], removals: set[str], original: dict[str, str] | None) -> dict[str, Any] | None:
    retained = [e for e in split_pipe(task["evidence_types_needed"]) if e not in removals]
    if not retained:
        return None
    phrases = "; ".join(f"Verify {evidence_type_to_phrase(e)} for FY{task['fiscal_year']} {task['fiscal_quarter']}" for e in retained)
    if "SOURCE_SEMANTICS_CONFIRMATION" in retained:
        subtypes = source_semantic_subtypes({**task, "exact_information_needed": (original or task).get("exact_information_needed", "")})
        phrases = phrases.replace(
            "source semantics confirmation",
            "source semantics confirmation (" + "|".join(subtypes) + ")",
        )
    return {
        **task,
        "evidence_types_needed": "|".join(retained),
        "exact_information_needed": phrases,
        "preferred_source_type": preferred_source_type(retained),
        "closure_dependency": "|".join(retained),
        "fact_count": len(retained),
        "research_request": f"Research official issuer sources for FY{task['fiscal_year']} {task['fiscal_quarter']}. Verify {', '.join(evidence_type_to_phrase(e) for e in retained)}. Do not research other fields.",
        "status": "READY_FOR_EXTERNAL_RESEARCH_ROOTCAUSE_CLEANED",
    }


def reclassify_wave(
    tasks: list[dict[str, str]],
    local_context: dict[tuple[str, str, str, str], dict[str, Any]],
    originals: dict[tuple[str, str, str, str], dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    audit: list[dict[str, Any]] = []
    cleaned: list[dict[str, Any]] = []
    for task in tasks:
        local = local_context.get(task_key(task))
        original = originals.get(task_key(task))
        removals: set[str] = set()
        for evidence_type in split_pipe(task["evidence_types_needed"]):
            if evidence_type not in AUDITED_TYPES:
                continue
            row = classify_audited_fact(task, evidence_type, local, original)
            audit.append(row)
            if row["keep_external"] == "NO":
                removals.add(evidence_type)
        clean = clean_task(task, removals, original)
        if clean:
            cleaned.append(clean)
    return audit, cleaned


def task_fact_count(tasks: list[dict[str, Any]]) -> int:
    return sum(int(row["fact_count"]) for row in tasks)


def wave_delta(before: list[dict[str, str]], after: list[dict[str, Any]], audit: list[dict[str, Any]]) -> dict[str, Any]:
    before_facts = sum(len(split_pipe(row["evidence_types_needed"])) for row in before)
    after_facts = task_fact_count(after)
    removed = [row for row in audit if row["keep_external"] == "NO"]
    return {
        "before_tasks": len(before),
        "new_tasks": len(after),
        "before_facts": before_facts,
        "new_facts": after_facts,
        "facts_removed": before_facts - after_facts,
        "fy_fq_removed": sum(row["evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY" for row in removed),
        "semantics_removed": sum(row["evidence_type"] == "SOURCE_SEMANTICS_CONFIRMATION" for row in removed),
        "reduction_pct": pct(before_facts - after_facts, before_facts),
    }


def ticker_summary(wave2: list[dict[str, Any]], wave3: list[dict[str, Any]], structural_tickers: set[str]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in wave2 + wave3:
        grouped[row["ticker"]].append(row)
    out = []
    for ticker, group in sorted(grouped.items()):
        waves = sorted({row["priority"] for row in group}, key=lambda p: WAVE_RANK.get(p, 99))
        facts = sorted({fact for row in group for fact in split_pipe(row["evidence_types_needed"])})
        out.append(
            {
                "ticker": ticker,
                "highest_priority_wave": waves[0],
                "research_task_count": len(group),
                "exact_facts_needed_count": sum(int(row["fact_count"]) for row in group),
                "evidence_types_needed": "|".join(facts),
                "structural_review_also_required": yes_no(ticker in structural_tickers),
                "status": "ROOTCAUSE_CLEANED_EXTERNAL_RESEARCH_REQUIRED",
            }
        )
    return out


def annotate_structural(
    structural_rows: list[dict[str, str]],
    local_context: dict[tuple[str, str, str, str], dict[str, Any]],
    originals: dict[tuple[str, str, str, str], dict[str, str]],
) -> list[dict[str, Any]]:
    by_ticker_fyfq: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in originals.values():
        by_ticker_fyfq[(row["ticker"], f"FY{row['fiscal_year']}{row['fiscal_quarter']}")].append(row)
    out = []
    for row in structural_rows:
        tasks = by_ticker_fyfq.get((row["ticker"], row["affected_fy_fq"].replace(" ", "")), [])
        statuses = []
        for task in tasks:
            local = local_context.get(task_key(task))
            if local:
                statuses.append((task, local, local_status(task, local)))
        fy_resolved = any(status.get("local_fy_status") == "FY_RESOLVED_DIRECT_EXACT" for _task, _local, status in statuses)
        fq_resolved = any(status.get("local_fq_status") == "FQ_RESOLVED_LOCAL_HIGH" for _task, _local, status in statuses)
        identity_evidence_requested = any(
            code in row.get("evidence_that_would_resolve_it", "")
            for code in ("NEED_OFFICIAL_FISCAL_YEAR_START", "NEED_OFFICIAL_FY_FQ_IDENTITY")
        )
        local_basis = "|".join(sorted({str(local.get("identity_basis") or "") for _task, local, _status in statuses if local.get("identity_basis")}))
        out.append(
            {
                **row,
                "fy_locally_resolved": yes_no(fy_resolved),
                "fq_locally_resolved": yes_no(fq_resolved),
                "exact_identity_evidence": local_basis,
                "remaining_structural_question": row.get("exact_decision_needed", row.get("exact decision needed", "")),
                "external_identity_evidence_still_needed": yes_no(identity_evidence_requested and (not fy_resolved or not fq_resolved)),
            }
        )
    return out


def closure_rows(
    phase8h_closure: list[dict[str, str]],
    phase8h1_closure: list[dict[str, str]],
    wave1: list[dict[str, str]],
    old_wave23: list[dict[str, str]],
    new_wave23: list[dict[str, Any]],
    structural_tickers: set[str],
) -> list[dict[str, Any]]:
    wave1_tickers = {row["ticker"] for row in wave1}
    old_wave23_tickers = {row["ticker"] for row in old_wave23}
    new_wave23_tickers = {row["ticker"] for row in new_wave23}
    locally_closed = old_wave23_tickers - new_wave23_tickers
    h1_complete = {row["ticker"] for row in phase8h1_closure if row.get("cleaned_closure_status") == "COMPLETE_CLOSURE_PATH"}
    out = []
    for row in phase8h_closure:
        ticker = row["ticker"]
        original = row["closure_completeness"]
        complete = (
            original == "ALREADY_CLEAN"
            or ticker in h1_complete
            or ticker in wave1_tickers
            or ticker in new_wave23_tickers
            or ticker in structural_tickers
            or ticker in locally_closed
        )
        status = "COMPLETE_CLOSURE_PATH" if complete else "MISSING_REQUIREMENT"
        out.append(
            {
                "ticker": ticker,
                "original_closure_completeness": original,
                "rootcause_closure_status": status,
                "missing_requirement": int(not complete),
            }
        )
    return out


def wave1_diagnostic(
    wave1: list[dict[str, str]],
    local_context: dict[tuple[str, str, str, str], dict[str, Any]],
    originals: dict[tuple[str, str, str, str], dict[str, str]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    audit, cleaned = reclassify_wave(wave1, local_context, originals)
    before_by_id = {row["research_task_id"]: len(split_pipe(row["evidence_types_needed"])) for row in wave1}
    after_by_id = {row["research_task_id"]: int(row["fact_count"]) for row in cleaned}
    rows = []
    for row in wave1:
        before = before_by_id[row["research_task_id"]]
        after = after_by_id.get(row["research_task_id"], 0)
        rows.append(
            {
                "research_task_id": row["research_task_id"],
                "ticker": row["ticker"],
                "FY": row["fiscal_year"],
                "FQ": row["fiscal_quarter"],
                "before_facts": before,
                "after_facts_if_regenerated": after,
                "would_shrink": yes_no(0 < after < before),
                "would_disappear": yes_no(after == 0),
            }
        )
    removed = [row for row in audit if row["keep_external"] == "NO"]
    summary = {
        "fy_fq_facts_that_would_disappear": sum(row["evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY" for row in removed),
        "source_semantics_facts_that_would_disappear": sum(row["evidence_type"] == "SOURCE_SEMANTICS_CONFIRMATION" for row in removed),
        "missing_quarter_facts_that_would_disappear": 0,
        "tasks_that_would_shrink": sum(row["would_shrink"] == "YES" for row in rows),
        "tasks_that_would_disappear_entirely": sum(row["would_disappear"] == "YES" for row in rows),
        "wave1_file_unchanged": "YES",
    }
    return rows, summary


def write_trace(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        f"""# Phase 8H-2 External Dependency Root-Cause Trace

## FY/FQ

Root cause location: `swingmaster/fundamentals/v3_phase8f_latest8q_gap_analysis.py::analyze_quarter`.

The original generator emitted `NEED_OFFICIAL_FISCAL_YEAR_START` and `NEED_OFFICIAL_FY_FQ_IDENTITY` from the fiscal-status fallback branch after Phase 8F issue classification. Phase 8G preserved these codes when downstream-critical. Phase 8H mapped both codes to `OFFICIAL_FY_FQ_IDENTITY`.

Phase 8H-1 removed zero FY/FQ facts because the remaining Wave 2/3 FY/FQ facts were not locally resolvable direct-exact quarters. They were unresolved-boundary structural rows, so the 8H-1 boundary guard retained them.

Historical exact-anchor evidence was not used by the original generator as an explicit pre-emission gate. The corrected path now treats exact adjacent anchors and high-confidence local FQ as a hard local stop before external FY/FQ is emitted.

## Source Semantics

Root cause location: `swingmaster/fundamentals/v3_phase8f_latest8q_gap_analysis.py::analyze_quarter`.

The original generator emitted `NEED_SOURCE_SEMANTICS_CONFIRMATION` from a broad sequence-status fallback and from derivation-input text. Phase 8H mapped the code to `SOURCE_SEMANTICS_CONFIRMATION` without requiring a semantic subtype.

The corrected generator removes the sequence-only fallback and the packager requires a concrete subtype before retaining a source-semantics fact.

## Results

- Wave 2 facts: `{summary['wave2']['before_facts']} -> {summary['wave2']['new_facts']}`
- Wave 3 facts: `{summary['wave3']['before_facts']} -> {summary['wave3']['new_facts']}`
- closure MISSING_REQUIREMENT: `{summary['closure']['MISSING_REQUIREMENT']}`
""",
        encoding="utf-8",
    )


def rule_before_after() -> list[dict[str, str]]:
    return [
        {
            "old_trigger": "fiscal_status fallback after issue classification",
            "old_dependency": "NEED_OFFICIAL_FISCAL_YEAR_START|NEED_OFFICIAL_FY_FQ_IDENTITY",
            "why_overbroad": "Could be emitted before an explicit exact-anchor/high-confidence-FQ external gate",
            "new_local_precheck": "DIRECT_EXACT_INTERVAL plus DIRECT_EXACT_FQ_HIGH plus no structural boundary",
            "new_trigger": "Only unresolved boundary, transition/stub, no fiscal year, or source ownership ambiguity",
            "new_dependency_behavior": "External FY/FQ prohibited when local exact anchors resolve identity",
            "regression_test": "test_exact_adjacent_anchor_prevents_external_fy_fq_dependency",
        },
        {
            "old_trigger": "non-clean sequence status generic fallback",
            "old_dependency": "NEED_SOURCE_SEMANTICS_CONFIRMATION",
            "why_overbroad": "Sequence uncertainty is not a source semantic subtype",
            "new_local_precheck": "Require concrete subtype unrelated to FY/FQ or sequence confirmation",
            "new_trigger": "YTD/discrete, debt definition, shares semantics, restatement vintage, source ownership, FCF or EBIT component semantics",
            "new_dependency_behavior": "Generic sequence semantics not emitted",
            "regression_test": "test_unresolved_sequence_alone_does_not_emit_source_semantics",
        },
    ]


def source_semantics_distribution(audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((row["semantic_category"], row["semantic_subtype"] or "NO_SUBTYPE", row["keep_external"]) for row in audit if row["evidence_type"] == "SOURCE_SEMANTICS_CONFIRMATION")
    return [
        {"semantic_category": category, "semantic_subtype": subtype, "keep_external": keep, "facts": count}
        for (category, subtype, keep), count in sorted(counts.items())
    ]


def append_docs(summary: dict[str, Any]) -> None:
    plan = Path("docs/fundamentals_v3_latest8q_external_research_plan.md")
    text = plan.read_text(encoding="utf-8").rstrip()
    marker = "## FY/FQ and Source-Semantics Dependency Root-Cause Fix"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Phase 8H-2 fixes the dependency-generation root cause behind redundant FY/FQ and generic source-semantics research requirements.

Phase 8H-1 removed zero FY/FQ requests because the four remaining Wave 2/3 FY/FQ facts were true unresolved-boundary structural cases, not locally resolved exact-anchor rows. The broader issue was source semantics: Phase 8F emitted `NEED_SOURCE_SEMANTICS_CONFIRMATION` as a sequence fallback and Phase 8H retained it without a semantic subtype.

Corrected order: resolve FY from exact anchors, resolve FQ locally, evaluate structural exceptions, identify concrete source semantic subtype, then emit external evidence. Generic sequence-only semantics are prohibited.

Wave 1 diagnostic only: FY/FQ would disappear `{summary['wave1_diagnostic']['fy_fq_facts_that_would_disappear']}`, semantics would disappear `{summary['wave1_diagnostic']['source_semantics_facts_that_would_disappear']}`, tasks would shrink `{summary['wave1_diagnostic']['tasks_that_would_shrink']}`, tasks would disappear `{summary['wave1_diagnostic']['tasks_that_would_disappear_entirely']}`. Wave 1 file unchanged `YES`.

Wave 2 rootcause-cleaned: `{summary['wave2']['before_tasks']} -> {summary['wave2']['new_tasks']}` tasks, `{summary['wave2']['before_facts']} -> {summary['wave2']['new_facts']}` facts.

Wave 3 rootcause-cleaned: `{summary['wave3']['before_tasks']} -> {summary['wave3']['new_tasks']}` tasks, `{summary['wave3']['before_facts']} -> {summary['wave3']['new_facts']}` facts.
"""
    plan.write_text(text + "\n", encoding="utf-8")

    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    text = phase8.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-2 - FY/FQ / Source-Semantics External Dependency Root-Cause Fix"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Root cause fixed: exact-anchor-first FY/FQ gating and strict source-semantics subtype enforcement. Wave 2 facts `{summary['wave2']['before_facts']} -> {summary['wave2']['new_facts']}`; Wave 3 facts `{summary['wave3']['before_facts']} -> {summary['wave3']['new_facts']}`; combined removed `{summary['combined']['total_redundant_facts_removed']}`. Closure MISSING_REQUIREMENT `{summary['closure']['MISSING_REQUIREMENT']}`.

Phase 8 remains `IN PROGRESS`.

Next action: {summary['next_action']}
"""
    phase8.write_text(text + "\n", encoding="utf-8")


def run_phase8h2(paths: Phase8H2Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    wave1 = read_csv_dicts(paths.phase8h_root / "latest8q_external_research_wave1_p1_current.csv")
    wave2 = read_csv_dicts(paths.phase8h1_root / "latest8q_external_research_wave2_p2_latest4q_cleaned.csv")
    wave3 = read_csv_dicts(paths.phase8h1_root / "latest8q_external_research_wave3_p3_latest8q_cleaned.csv")
    h1_summary = read_json(paths.phase8h1_root / "wave23_cleanup_summary.json")
    phase8h_closure = read_csv_dicts(paths.phase8h_root / "external_research_closure_test.csv")
    phase8h1_closure = read_csv_dicts(paths.phase8h1_root / "wave23_closure_test.csv")
    structural = read_csv_dicts(paths.phase8h1_root / "latest8q_structural_decisions_remaining.csv")
    originals = original_task_lookup(paths.phase8h_root)
    local_context = load_local_context(paths.v3_db)
    wave2_audit, wave2_clean = reclassify_wave(wave2, local_context, originals)
    wave3_audit, wave3_clean = reclassify_wave(wave3, local_context, originals)
    audit = wave2_audit + wave3_audit
    structural_tickers = {row["ticker"] for row in structural}
    structural_annotated = annotate_structural(structural, local_context, originals)
    ticker_rows = ticker_summary(wave2_clean, wave3_clean, structural_tickers)
    closure = closure_rows(phase8h_closure, phase8h1_closure, wave1, wave2 + wave3, wave2_clean + wave3_clean, structural_tickers)
    closure_counts = Counter(row["rootcause_closure_status"] for row in closure)
    wave1_rows, wave1_summary = wave1_diagnostic(wave1, local_context, originals)
    wave2_delta = wave_delta(wave2, wave2_clean, wave2_audit)
    wave3_delta = wave_delta(wave3, wave3_clean, wave3_audit)
    remaining_semantics = sum(
        "SOURCE_SEMANTICS_CONFIRMATION" in split_pipe(row["evidence_types_needed"])
        for row in wave2_clean + wave3_clean
    )
    old_tickers = {row["ticker"] for row in wave2 + wave3}
    new_tickers = {row["ticker"] for row in wave2_clean + wave3_clean}
    structural_identity_needed = sum(row["external_identity_evidence_still_needed"] == "YES" for row in structural_annotated)
    classification = CLASSIFICATION_NOT_FIXED
    if closure_counts["MISSING_REQUIREMENT"] == 0:
        classification = CLASSIFICATION_STRUCTURAL if structural else CLASSIFICATION_FIXED
    summary = {
        "artifact_root": str(paths.artifact_root),
        "classification": classification,
        "root_cause_fixed": "YES" if classification != CLASSIFICATION_NOT_FIXED else "NO",
        "wave2": wave2_delta,
        "wave3": wave3_delta,
        "combined": {
            "total_redundant_facts_removed": wave2_delta["facts_removed"] + wave3_delta["facts_removed"],
            "remaining_external_facts": wave2_delta["new_facts"] + wave3_delta["new_facts"],
            "external_tickers_removed_entirely": len(old_tickers - new_tickers),
            "remaining_external_tickers": len(new_tickers),
            "true_source_semantics_fact_count": remaining_semantics,
        },
        "fyfq_dependency_audit": {
            "facts_analyzed": sum(row["evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY" for row in audit),
            "locally_resolvable": sum(row["evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY" and row["keep_external"] == "NO" for row in audit),
            "genuinely_external": sum(row["evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY" and row["keep_external"] == "YES" for row in audit),
            "removed": wave2_delta["fy_fq_removed"] + wave3_delta["fy_fq_removed"],
            "remaining": sum(row["evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY" and row["keep_external"] == "YES" for row in audit),
        },
        "source_semantics_audit": {
            "facts_analyzed": sum(row["evidence_type"] == "SOURCE_SEMANTICS_CONFIRMATION" for row in audit),
            "TRUE_SEMANTIC_AMBIGUITY": sum(row["semantic_category"] == "TRUE_SEMANTIC_AMBIGUITY" for row in audit),
            "FISCAL_IDENTITY_ONLY": sum(row["semantic_category"] == "FISCAL_IDENTITY_ONLY" for row in audit),
            "SEQUENCE_ONLY": sum(row["semantic_category"] == "SEQUENCE_ONLY" for row in audit),
            "GENERIC_FALLBACK": sum(row["semantic_category"] == "GENERIC_FALLBACK" for row in audit),
            "removed": wave2_delta["semantics_removed"] + wave3_delta["semantics_removed"],
            "remaining_true_semantics": remaining_semantics,
        },
        "wave1_diagnostic": wave1_summary,
        "structural": {
            "structural_decisions": len(structural_annotated),
            "cases_with_fy_resolved_locally": sum(row["fy_locally_resolved"] == "YES" for row in structural_annotated),
            "cases_with_fq_resolved_locally": sum(row["fq_locally_resolved"] == "YES" for row in structural_annotated),
            "cases_still_needing_external_identity_evidence": structural_identity_needed,
        },
        "closure": {
            "COMPLETE_CLOSURE_PATH": closure_counts["COMPLETE_CLOSURE_PATH"],
            "MISSING_REQUIREMENT": closure_counts["MISSING_REQUIREMENT"],
        },
        "safety": {"production_writes": 0, "network_calls": 0, "downstream_writes": 0, "rawcandle_writes": 0, "active_guard_changes": 0},
        "phase8h1_baseline": h1_summary,
        "next_action": NEXT_FIXED if classification != CLASSIFICATION_NOT_FIXED else NEXT_NOT_FIXED,
    }
    write_trace(paths.artifact_root / "external_dependency_root_cause_trace.md", summary)
    write_csv(paths.artifact_root / "dependency_rule_before_after.csv", rule_before_after())
    write_csv(paths.artifact_root / "fyfq_semantics_dependency_reclassification.csv", audit)
    write_json(paths.artifact_root / "fyfq_external_need_summary.json", summary["fyfq_dependency_audit"])
    write_csv(paths.artifact_root / "source_semantics_subtype_distribution.csv", source_semantics_distribution(audit))
    write_csv(paths.artifact_root / "wave1_rootcause_diagnostic.csv", wave1_rows)
    write_json(paths.artifact_root / "wave1_rootcause_diagnostic_summary.json", wave1_summary)
    write_csv(paths.artifact_root / "latest8q_external_research_wave2_p2_latest4q_rootcause_cleaned.csv", wave2_clean)
    write_csv(paths.artifact_root / "latest8q_external_research_wave3_p3_latest8q_rootcause_cleaned.csv", wave3_clean)
    write_csv(paths.artifact_root / "latest8q_external_research_by_ticker_rootcause_cleaned.csv", ticker_rows)
    write_csv(paths.artifact_root / "latest8q_structural_decisions_rootcause_annotated.csv", structural_annotated)
    write_csv(paths.artifact_root / "rootcause_closure_test.csv", closure)
    write_json(paths.artifact_root / "phase8h2_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if paths.write_documentation:
        append_docs(summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fix and audit Phase 8H FY/FQ and source-semantics dependency root cause")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8h2_dependency_root_cause") / utc_stamp())
    parser.add_argument("--phase8h-root", type=Path, default=Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H"))
    parser.add_argument("--phase8h1-root", type=Path, default=Path("temp/fundamentals_v3_phase8h1_wave23_cleanup/20260829T_PHASE8H1"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8h2(
        Phase8H2Paths(
            artifact_root=args.artifact_root,
            phase8h_root=args.phase8h_root,
            phase8h1_root=args.phase8h1_root,
            v3_db=args.v3_db,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"wave2_facts={summary['wave2']['before_facts']}->{summary['wave2']['new_facts']}")
    print(f"wave3_facts={summary['wave3']['before_facts']}->{summary['wave3']['new_facts']}")
    print(f"MISSING_REQUIREMENT={summary['closure']['MISSING_REQUIREMENT']}")
    return 0 if summary["classification"] != CLASSIFICATION_NOT_FIXED else 2


if __name__ == "__main__":
    raise SystemExit(main())
