from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import EXPECTED_P1_TICKERS, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8f_latest8q_gap_analysis import build_quarter_diagnostics, pct
from swingmaster.fundamentals.v3_phase8h_external_research_packaging import (
    WAVE_RANK,
    evidence_type_to_phrase,
    preferred_source_type,
)


CLASSIFICATION_COMPLETE = "WAVE23_EXTERNAL_QUEUE_CLEANUP_COMPLETE"
CLASSIFICATION_STRUCTURAL = "WAVE23_EXTERNAL_QUEUE_CLEANUP_COMPLETE_WITH_STRUCTURAL_DEPENDENCIES"
CLASSIFICATION_INCOMPLETE = "WAVE23_EXTERNAL_QUEUE_CLEANUP_INCOMPLETE"
NEXT_COMPLETE = "USE THE CLEANED WAVE 2 / WAVE 3 FILES FOR FUTURE EXTERNAL RESEARCH; DO NOT REQUEST FY/FQ CONFIRMATION WHERE HISTORICAL EXACT ANCHORS ALREADY RESOLVE THE IDENTITY"


@dataclass(frozen=True)
class Phase8H1Paths:
    artifact_root: Path
    phase8h_root: Path = Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    write_documentation: bool = True


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def split_pipe(value: str | None) -> list[str]:
    return [part for part in str(value or "").split("|") if part]


def yes_no(value: bool) -> str:
    return "YES" if value else "NO"


def load_local_context(v3_db: Path) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    latest_rows, _diagnostics, _problems, _ctx = build_quarter_diagnostics(v3_db)
    out = {}
    for row in latest_rows:
        out[(str(row["ticker"]), str(row["fiscal_year"]), str(row["fiscal_quarter"]), str(row["period_end"]))] = row
    return out


def structural_boundary(task: dict[str, str], local: dict[str, Any] | None) -> bool:
    issue = task.get("existing_local_evidence_summary", "") + "|" + task.get("structural_warning", "")
    if task.get("structural_warning"):
        return True
    if any(token in issue for token in ("TRANSITION", "UNRESOLVED", "TARGET_COLLISION", "DUPLICATE_ECONOMIC_QUARTER")):
        return True
    if local and str(local.get("target_collision") or "") in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}:
        return True
    if local and str(local.get("break_reason") or "") in {"CALENDAR_TRANSITION", "UNRESOLVED_BOUNDARY", "NO_FISCAL_YEAR"}:
        return True
    return False


def local_status(task: dict[str, str], local: dict[str, Any] | None) -> dict[str, str]:
    if not local:
        return {
            "exact_anchor_coverage": "NO_LOCAL_ROW_MATCH",
            "local_fy_status": "FY_UNRESOLVED",
            "local_fq_status": "FQ_UNRESOLVED",
            "local_sequence_status": "",
            "structural_boundary_status": "UNRESOLVED",
        }
    exact = local.get("identity_basis") == "DIRECT_EXACT_INTERVAL"
    fq_high = local.get("fq_confidence") == "DIRECT_EXACT_FQ_HIGH" and local.get("period_end_structural_fit") == "STRUCTURAL_FIT"
    boundary = structural_boundary(task, local)
    return {
        "exact_anchor_coverage": "ADJACENT_EXACT_ANCHORS" if exact else str(local.get("identity_basis") or "NO_DIRECT_EXACT_ANCHOR"),
        "local_fy_status": "FY_RESOLVED_DIRECT_EXACT" if exact else "FY_UNRESOLVED",
        "local_fq_status": "FQ_RESOLVED_LOCAL_HIGH" if fq_high and not boundary else "FQ_REQUIRES_REVIEW",
        "local_sequence_status": str(local.get("sequence_status") or ""),
        "structural_boundary_status": "STRUCTURAL_OR_BOUNDARY_REVIEW" if boundary else "NO_STRUCTURAL_BOUNDARY",
    }


def source_semantics_is_only_identity(task: dict[str, str]) -> bool:
    text = str(task.get("exact_information_needed", "") or "")
    lower = text.lower()
    hard_keep = (
        "approved issuer/company-specific ebit rule",
        "ytd",
        "discrete",
        "debt definition",
        "shares period-end",
        "weighted-average",
        "fcf direct",
        "restated",
        "different economic",
        "target collision",
        "source ownership",
        "lineage",
    )
    if any(marker in lower for marker in hard_keep):
        return False
    semantic_parts = [part.strip().lower() for part in text.split(";") if "semantics" in part.lower() or "fy/fq" in part.lower()]
    if not semantic_parts:
        return False
    return all("sequence semantics" in part or "stored fy/fq" in part or "official fiscal year start and fy/fq identity" in part for part in semantic_parts)


def existing_mislabeled_quarter_identified(task: dict[str, str], local: dict[str, Any] | None, status: dict[str, str]) -> bool:
    if not local:
        return False
    if status["local_fy_status"] != "FY_RESOLVED_DIRECT_EXACT" or status["local_fq_status"] != "FQ_RESOLVED_LOCAL_HIGH":
        return False
    exact_fy = str(local.get("exact_fy") or "")
    exact_fq = str(local.get("exact_fq") or "")
    return bool(exact_fy and exact_fq and (exact_fy != str(task["fiscal_year"]) or exact_fq != str(task["fiscal_quarter"])))


def classify_fact(task: dict[str, str], evidence_type: str, local: dict[str, Any] | None) -> dict[str, str]:
    status = local_status(task, local)
    keep = True
    reason = ""
    remaining = evidence_type
    if evidence_type == "OFFICIAL_FY_FQ_IDENTITY":
        if (
            status["structural_boundary_status"] == "NO_STRUCTURAL_BOUNDARY"
            and status["local_fy_status"] == "FY_RESOLVED_DIRECT_EXACT"
            and status["local_fq_status"] == "FQ_RESOLVED_LOCAL_HIGH"
        ):
            keep = False
            reason = "FY_FQ_ALREADY_RESOLVED"
            remaining = ""
        elif status["structural_boundary_status"] == "NO_STRUCTURAL_BOUNDARY" and status["local_fy_status"] == "FY_RESOLVED_DIRECT_EXACT":
            keep = False
            reason = "FY_ALREADY_RESOLVED_DIRECT_EXACT"
            remaining = ""
    elif evidence_type == "SOURCE_SEMANTICS_CONFIRMATION":
        if (
            status["local_fy_status"] == "FY_RESOLVED_DIRECT_EXACT"
            and status["local_fq_status"] == "FQ_RESOLVED_LOCAL_HIGH"
            and status["structural_boundary_status"] == "NO_STRUCTURAL_BOUNDARY"
            and source_semantics_is_only_identity(task)
        ):
            keep = False
            reason = "SOURCE_SEMANTICS_ALREADY_RESOLVED"
            remaining = ""
    elif evidence_type == "MISSING_QUARTER_EXISTENCE":
        if (
            existing_mislabeled_quarter_identified(task, local, status)
            or (
                status["local_fy_status"] == "FY_RESOLVED_DIRECT_EXACT"
                and status["local_fq_status"] == "FQ_RESOLVED_LOCAL_HIGH"
                and status["structural_boundary_status"] == "NO_STRUCTURAL_BOUNDARY"
            )
        ):
            keep = False
            reason = "MISSING_QUARTER_ALREADY_IDENTIFIED_LOCALLY"
            remaining = ""
    return {
        "ticker": task["ticker"],
        "fiscal_year": task["fiscal_year"],
        "fiscal_quarter": task["fiscal_quarter"],
        "evidence_type": evidence_type,
        "original_request": task.get("research_request", ""),
        **status,
        "keep_external": yes_no(keep),
        "removal_reason": reason,
        "remaining_external_requirement": remaining,
        "priority": task["priority"],
        "research_task_id": task["research_task_id"],
    }


def reclassify_wave_tasks(tasks: list[dict[str, str]], local_context: dict[tuple[str, str, str, str], dict[str, Any]]) -> list[dict[str, str]]:
    out = []
    seen: set[tuple[str, str, str, str]] = set()
    for task in tasks:
        local = local_context.get((task["ticker"], task["fiscal_year"], task["fiscal_quarter"], task["current_period_end"]))
        for evidence_type in split_pipe(task["evidence_types_needed"]):
            fact = classify_fact(task, evidence_type, local)
            key = (fact["ticker"], fact["fiscal_year"], fact["fiscal_quarter"], fact["evidence_type"])
            if key in seen and fact["keep_external"] == "YES":
                fact["keep_external"] = "NO"
                fact["removal_reason"] = "DUPLICATE_REQUEST"
                fact["remaining_external_requirement"] = ""
            seen.add(key)
            out.append(fact)
    return out


def rewrite_exact_information(task: dict[str, str], retained: list[str]) -> str:
    pieces = [f"Verify {evidence_type_to_phrase(e)} for FY{task['fiscal_year']} {task['fiscal_quarter']}" for e in retained]
    return "; ".join(pieces)


def clean_tasks(tasks: list[dict[str, str]], reclass: list[dict[str, str]]) -> list[dict[str, Any]]:
    retained_by_task: dict[str, list[str]] = defaultdict(list)
    for row in reclass:
        if row["keep_external"] == "YES":
            retained_by_task[row["research_task_id"]].append(row["evidence_type"])
    out = []
    for task in tasks:
        retained = sorted(dict.fromkeys(retained_by_task.get(task["research_task_id"], [])))
        if not retained:
            continue
        out.append(
            {
                **task,
                "evidence_types_needed": "|".join(retained),
                "exact_information_needed": rewrite_exact_information(task, retained),
                "preferred_source_type": preferred_source_type(retained),
                "closure_dependency": "|".join(retained),
                "fact_count": len(retained),
                "research_request": f"Research official issuer sources for FY{task['fiscal_year']} {task['fiscal_quarter']}. Verify {', '.join(evidence_type_to_phrase(e) for e in retained)}. Do not research other fields.",
                "status": "READY_FOR_EXTERNAL_RESEARCH_CLEANED",
            }
        )
    return out


def wave_summary(old_tasks: list[dict[str, str]], new_tasks: list[dict[str, Any]], reclass: list[dict[str, str]]) -> dict[str, Any]:
    old_facts = sum(len(split_pipe(row["evidence_types_needed"])) for row in old_tasks)
    new_facts = sum(int(row["fact_count"]) for row in new_tasks)
    removed = [row for row in reclass if row["keep_external"] == "NO"]
    fyfq_removed = sum(row["removal_reason"] in {"FY_ALREADY_RESOLVED_DIRECT_EXACT", "FQ_ALREADY_RESOLVED_LOCAL_HIGH", "FY_FQ_ALREADY_RESOLVED"} for row in removed)
    semantics_removed = sum(row["removal_reason"] == "SOURCE_SEMANTICS_ALREADY_RESOLVED" for row in removed)
    missing_removed = sum(row["removal_reason"] == "MISSING_QUARTER_ALREADY_IDENTIFIED_LOCALLY" for row in removed)
    return {
        "old_tasks": len(old_tasks),
        "new_tasks": len(new_tasks),
        "old_facts": old_facts,
        "new_facts": new_facts,
        "facts_removed": old_facts - new_facts,
        "fy_fq_requests_removed": fyfq_removed,
        "source_semantics_requests_removed": semantics_removed,
        "missing_quarter_requests_removed": missing_removed,
        "reduction_pct": pct(old_facts - new_facts, old_facts),
        "tickers": len({row["ticker"] for row in new_tasks}),
    }


def build_ticker_summary(old_tasks: list[dict[str, str]], new_tasks: list[dict[str, Any]], reclass: list[dict[str, str]], structural_tickers: set[str]) -> list[dict[str, Any]]:
    old_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    new_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    reclass_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in old_tasks:
        old_by_ticker[row["ticker"]].append(row)
    for row in new_tasks:
        new_by_ticker[row["ticker"]].append(row)
    for row in reclass:
        reclass_by_ticker[row["ticker"]].append(row)
    out = []
    for ticker in sorted(set(old_by_ticker) | set(new_by_ticker)):
        old_group = old_by_ticker.get(ticker, [])
        new_group = new_by_ticker.get(ticker, [])
        removed = [row for row in reclass_by_ticker.get(ticker, []) if row["keep_external"] == "NO"]
        remaining = sorted({fact for row in new_group for fact in split_pipe(row["evidence_types_needed"])})
        requests = "; ".join(row["research_request"] for row in sorted(new_group, key=lambda r: (WAVE_RANK.get(r["priority"], 99), int(r["fiscal_year"]), r["fiscal_quarter"])))
        out.append(
            {
                "ticker": ticker,
                "old_wave": min((row["priority"] for row in old_group), key=lambda p: WAVE_RANK.get(p, 99)),
                "new_wave": min((row["priority"] for row in new_group), key=lambda p: WAVE_RANK.get(p, 99)) if new_group else "NO_EXTERNAL_RESEARCH_NEEDED",
                "old_fact_count": sum(len(split_pipe(row["evidence_types_needed"])) for row in old_group),
                "new_fact_count": sum(int(row["fact_count"]) for row in new_group),
                "fy_fq_requests_removed": sum(row["removal_reason"] in {"FY_ALREADY_RESOLVED_DIRECT_EXACT", "FQ_ALREADY_RESOLVED_LOCAL_HIGH", "FY_FQ_ALREADY_RESOLVED"} for row in removed),
                "source_semantics_requests_removed": sum(row["removal_reason"] == "SOURCE_SEMANTICS_ALREADY_RESOLVED" for row in removed),
                "missing_quarter_requests_removed": sum(row["removal_reason"] == "MISSING_QUARTER_ALREADY_IDENTIFIED_LOCALLY" for row in removed),
                "remaining_facts": "|".join(remaining),
                "consolidated_cleaned_research_request": requests,
                "structural_review_still_required": yes_no(ticker in structural_tickers),
            }
        )
    return out


def annotate_structural(structural_rows: list[dict[str, str]], local_context: dict[tuple[str, str, str, str], dict[str, Any]], wave_rows_: list[dict[str, str]]) -> list[dict[str, Any]]:
    task_by_ticker_fyfq = {(row["ticker"], f"FY{row['fiscal_year']}{row['fiscal_quarter']}"): row for row in wave_rows_}
    out = []
    for row in structural_rows:
        task = task_by_ticker_fyfq.get((row["ticker"], row["affected_fy_fq"].replace(" ", "")))
        simplified = "NO"
        if task:
            local = local_context.get((task["ticker"], task["fiscal_year"], task["fiscal_quarter"], task["current_period_end"]))
            status = local_status(task, local)
            simplified = yes_no(status["local_fy_status"] == "FY_RESOLVED_DIRECT_EXACT" and status["local_fq_status"] == "FQ_RESOLVED_LOCAL_HIGH")
        out.append({**row, "fy_fq_locally_resolved": simplified, "structural_decision_remaining": "YES"})
    return out


def closure_test(phase8h_closure: list[dict[str, str]], cleaned_tickers: set[str], structural_tickers: set[str], locally_closed_tickers: set[str]) -> list[dict[str, Any]]:
    out = []
    for row in phase8h_closure:
        ticker = row["ticker"]
        original = row["closure_completeness"]
        if original == "ALREADY_CLEAN":
            status = "COMPLETE_CLOSURE_PATH"
        elif ticker in cleaned_tickers or ticker in structural_tickers or ticker in locally_closed_tickers:
            status = "COMPLETE_CLOSURE_PATH"
        else:
            status = "MISSING_REQUIREMENT"
        out.append({"ticker": ticker, "original_closure_completeness": original, "cleaned_closure_status": status, "missing_requirement": int(status == "MISSING_REQUIREMENT")})
    return out


def known_13_rows(ticker_summary: list[dict[str, Any]], structural_tickers: set[str]) -> list[dict[str, Any]]:
    by_ticker = {row["ticker"]: row for row in ticker_summary}
    out = []
    for ticker in EXPECTED_P1_TICKERS:
        row = by_ticker.get(ticker, {})
        removed = int(row.get("fy_fq_requests_removed") or 0) if row else 0
        out.append(
            {
                "ticker": ticker,
                "current_wave": row.get("new_wave", "NOT_IN_WAVE23"),
                "fy_fq_locally_resolved": yes_no(removed > 0),
                "fy_fq_request_removed": yes_no(removed > 0),
                "remaining_external_facts": row.get("remaining_facts", ""),
                "remaining_structural_decision": yes_no(ticker in structural_tickers),
            }
        )
    return out


def write_report(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        f"""# Fundamentals V3 Latest8Q External Research Plan

Phase 8H packages the post-8G external queue for official-source research. It does not browse, edit canonical data, rebuild downstream tables, or write RawCandle.

## Downstream-Critical Policy

Research requests are limited to fiscal identity, genuine missing quarters, official period_end, first-public publish_date, Revenue, EBIT, FCF, Cash, Total Debt, Shares Outstanding, and approved inputs needed to derive those fields.

Gross Profit, EBITDA, Net Income, Operating Income, OCF, and Capex are excluded when they are only secondary completeness gaps. OCF/Capex are retained only when needed to derive missing FCF; Operating Income is retained only as part of an approved EBIT derivation requirement.

## Package Counts

- starting Phase 8G external queue rows: `4413`
- raw normalized dependency facts before deduplication: `9820`
- normalized deduplicated critical facts: `9491`
- duplicate facts removed: `329`
- research tasks: `4413`
- external tickers: `1689`
- average facts/task: `2.1507`

## Waves

- Wave 1 P1_CURRENT: `1066` tasks / `810` tickers / `1857` facts
- Wave 2 P2_LATEST4Q: `370` tasks / `181` tickers / `548` facts
- Wave 3 P3_LATEST8Q: `2977` tasks / `1336` tickers / `7086` facts

## Top Evidence Needs

- `TOTAL_DEBT`: `1817`
- `SOURCE_SEMANTICS_CONFIRMATION`: `1549`
- `MISSING_QUARTER_EXISTENCE`: `1259`
- `FIRST_PUBLIC_PUBLISH_DATE`: `1063`
- `EBIT_DIRECT`: `886`
- `REVENUE`: `466`
- `FCF_DIRECT`: `463`
- `CAPEX_FOR_FCF`: `424`
- `OFFICIAL_FY_FQ_IDENTITY`: `329`
- `CASH`: `252`
- `LINEAGE_OWNERSHIP_EVIDENCE`: `226`
- `TARGET_COLLISION_EVIDENCE`: `225`
- `OFFICIAL_PERIOD_END`: `214`
- `SHARES_OUTSTANDING`: `121`
- `FISCAL_TRANSITION_EVIDENCE`: `118`

## First Batch

First batch uses deterministic impact score `>=175`, emphasizing current TTM, latest-quarter impact, number of downstream layers, and number of required facts. It contains `296` tasks / `210` tickers / `671` facts.

## Structural Separation

Structural decisions remain separate in `latest8q_structural_decisions_remaining.csv`. Mixed external+structural tickers are flagged in the ticker-level package and must not be treated as simple external-only repairs.

## Closure

- ALREADY_CLEAN: `701`
- YES_EXTERNAL_ONLY: `1287`
- YES_EXTERNAL_PLUS_STRUCTURAL: `402`
- YES_STRUCTURAL_ONLY: `80`
- NO_MISSING_REQUIREMENT: `0`

## Classification

`LATEST8Q_EXTERNAL_RESEARCH_PACKAGE_READY_WITH_STRUCTURAL_DEPENDENCIES`

## Next Action

RUN WAVE 1 EXTERNAL RESEARCH FIRST WHILE KEEPING STRUCTURAL DECISIONS SEPARATE; USE NEW EVIDENCE TO REDUCE THE STRUCTURAL QUEUE BEFORE MANUAL REVIEW

## Phase 8H-1 - Wave 2 / Wave 3 Exact-Anchor Cleanup

Phase 8H-1 cleans only Wave 2 and Wave 3. Wave 1 is not modified.

Historical exact FY anchors and local calendar-type FQ resolution must be exhausted before requesting external FY/FQ verification. OFFICIAL_FY_FQ_IDENTITY and identity-only SOURCE_SEMANTICS_CONFIRMATION requests are removed when exact anchors, period_end, fiscal slot logic, and sequence context resolve the quarter locally.

### Results

- Wave 2: `{summary['wave2']['old_tasks']}` tasks -> `{summary['wave2']['new_tasks']}` tasks, `{summary['wave2']['old_facts']}` facts -> `{summary['wave2']['new_facts']}` facts
- Wave 3: `{summary['wave3']['old_tasks']}` tasks -> `{summary['wave3']['new_tasks']}` tasks, `{summary['wave3']['old_facts']}` facts -> `{summary['wave3']['new_facts']}` facts
- combined facts removed: `{summary['combined']['total_facts_removed']}`
- remaining facts: `{summary['combined']['remaining_facts']}`
- tickers no longer needing external research: `{summary['combined']['tickers_no_longer_needing_external_research']}`
- remaining external tickers: `{summary['combined']['remaining_external_tickers']}`

Structural queue remains separate. Structural cases unchanged `{summary['structural']['structural_cases_unchanged']}`, simplified by locally resolved FY/FQ `{summary['structural']['structural_cases_simplified_by_locally_resolved_fy_fq']}`.

Closure test: COMPLETE_CLOSURE_PATH `{summary['closure']['COMPLETE_CLOSURE_PATH']}`, MISSING_REQUIREMENT `{summary['closure']['MISSING_REQUIREMENT']}`.

Safety: production writes `0`, network calls `0`, RawCandle writes `0`.

Classification: `{summary['classification']}`

Next action: {summary['next_action']}
""",
        encoding="utf-8",
    )


def append_phase8_doc(summary: dict[str, Any]) -> None:
    doc_path = Path("docs/fundamentals_v3_phase8_update_v3.md")
    marker = "## Phase 8H-1 - Wave 2 / Wave 3 External Queue Cleanup"
    existing = doc_path.read_text(encoding="utf-8")
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    section = f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Wave 2 facts `{summary['wave2']['old_facts']} -> {summary['wave2']['new_facts']}` and Wave 3 facts `{summary['wave3']['old_facts']} -> {summary['wave3']['new_facts']}` after exhausting historical exact FY anchors and local FQ resolution before external fiscal-identity requests. Total facts removed `{summary['combined']['total_facts_removed']}`; remaining Wave 2/3 facts `{summary['combined']['remaining_facts']}`; remaining external tickers `{summary['combined']['remaining_external_tickers']}`.

Structural cases remain separate: unchanged `{summary['structural']['structural_cases_unchanged']}`, simplified by locally resolved FY/FQ `{summary['structural']['structural_cases_simplified_by_locally_resolved_fy_fq']}`.

Phase 8 remains `IN PROGRESS`.

Next action: {summary['next_action']}
"""
    doc_path.write_text(existing + section, encoding="utf-8")


def run_phase8h1(paths: Phase8H1Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    wave2_old = read_csv_dicts(paths.phase8h_root / "latest8q_external_research_wave2_p2_latest4q.csv")
    wave3_old = read_csv_dicts(paths.phase8h_root / "latest8q_external_research_wave3_p3_latest8q.csv")
    phase8h_ticker = read_csv_dicts(paths.phase8h_root / "latest8q_external_research_by_ticker.csv")
    structural = read_csv_dicts(paths.phase8h_root / "latest8q_structural_decisions_remaining.csv")
    phase8h_closure = read_csv_dicts(paths.phase8h_root / "external_research_closure_test.csv")
    local_context = load_local_context(paths.v3_db)
    structural_tickers = {row["ticker"] for row in structural}
    wave23_old = wave2_old + wave3_old
    reclass = reclassify_wave_tasks(wave23_old, local_context)
    wave2_reclass = [row for row in reclass if row["priority"] == "P2_LATEST4Q"]
    wave3_reclass = [row for row in reclass if row["priority"] == "P3_LATEST8Q"]
    wave2_clean = clean_tasks(wave2_old, wave2_reclass)
    wave3_clean = clean_tasks(wave3_old, wave3_reclass)
    retained = [row for row in reclass if row["keep_external"] == "YES"]
    removed = [row for row in reclass if row["keep_external"] == "NO"]
    ticker_summary = build_ticker_summary(wave23_old, wave2_clean + wave3_clean, reclass, structural_tickers)
    cleaned_tickers = {row["ticker"] for row in wave2_clean + wave3_clean}
    structural_annotated = annotate_structural(structural, local_context, wave23_old)
    old_tickers = {row["ticker"] for row in wave23_old}
    new_tickers = cleaned_tickers
    locally_closed_tickers = old_tickers - new_tickers
    closure = closure_test(
        phase8h_closure,
        cleaned_tickers | {row["ticker"] for row in phase8h_ticker if row["highest_priority_wave"] == "P1_CURRENT"},
        structural_tickers,
        locally_closed_tickers,
    )
    closure_counts = Counter(row["cleaned_closure_status"] for row in closure)
    known_13 = known_13_rows(ticker_summary, structural_tickers)
    wave2 = wave_summary(wave2_old, wave2_clean, wave2_reclass)
    wave3 = wave_summary(wave3_old, wave3_clean, wave3_reclass)
    classification = CLASSIFICATION_INCOMPLETE
    if closure_counts["MISSING_REQUIREMENT"] == 0:
        classification = CLASSIFICATION_STRUCTURAL if structural else CLASSIFICATION_COMPLETE
    summary = {
        "artifact_root": str(paths.artifact_root),
        "classification": classification,
        "wave2": wave2,
        "wave3": wave3,
        "combined": {
            "total_facts_removed": wave2["facts_removed"] + wave3["facts_removed"],
            "remaining_facts": wave2["new_facts"] + wave3["new_facts"],
            "tickers_no_longer_needing_external_research": len(old_tickers - new_tickers),
            "remaining_external_tickers": len(new_tickers),
            "combined_reduction_pct": pct(wave2["facts_removed"] + wave3["facts_removed"], wave2["old_facts"] + wave3["old_facts"]),
        },
        "structural": {
            "structural_cases_unchanged": len(structural),
            "structural_cases_simplified_by_locally_resolved_fy_fq": sum(row["fy_fq_locally_resolved"] == "YES" for row in structural_annotated),
        },
        "closure": {
            "COMPLETE_CLOSURE_PATH": closure_counts["COMPLETE_CLOSURE_PATH"],
            "MISSING_REQUIREMENT": closure_counts["MISSING_REQUIREMENT"],
        },
        "known_13": known_13,
        "safety": {"production_writes": 0, "network_calls": 0, "rawcandle_writes": 0, "guard_changes": 0},
        "next_action": NEXT_COMPLETE,
    }
    write_csv(paths.artifact_root / "wave23_fact_reclassification.csv", reclass)
    write_csv(paths.artifact_root / "wave23_removed_external_requests.csv", removed)
    write_csv(paths.artifact_root / "wave23_retained_external_requests.csv", retained)
    write_csv(paths.artifact_root / "latest8q_external_research_wave2_p2_latest4q_cleaned.csv", wave2_clean)
    write_csv(paths.artifact_root / "latest8q_external_research_wave3_p3_latest8q_cleaned.csv", wave3_clean)
    write_csv(paths.artifact_root / "latest8q_external_research_by_ticker_cleaned.csv", ticker_summary)
    write_csv(paths.artifact_root / "latest8q_structural_decisions_remaining.csv", structural_annotated)
    write_csv(paths.artifact_root / "known_13_wave23_cleanup.csv", known_13)
    write_csv(paths.artifact_root / "wave23_closure_test.csv", closure)
    write_json(paths.artifact_root / "wave23_cleanup_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(NEXT_COMPLETE + "\n", encoding="utf-8")
    if paths.write_documentation:
        write_report(Path("docs/fundamentals_v3_latest8q_external_research_plan.md"), summary)
        append_phase8_doc(summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean Phase 8H Wave 2/3 external research queues with local exact anchors")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8h1_wave23_cleanup") / utc_stamp())
    parser.add_argument("--phase8h-root", type=Path, default=Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8h1(
        Phase8H1Paths(
            artifact_root=args.artifact_root,
            phase8h_root=args.phase8h_root,
            v3_db=args.v3_db,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"wave2_facts={summary['wave2']['old_facts']}->{summary['wave2']['new_facts']}")
    print(f"wave3_facts={summary['wave3']['old_facts']}->{summary['wave3']['new_facts']}")
    print(f"MISSING_REQUIREMENT={summary['closure']['MISSING_REQUIREMENT']}")
    return 0 if summary["classification"] != CLASSIFICATION_INCOMPLETE else 2


if __name__ == "__main__":
    raise SystemExit(main())
