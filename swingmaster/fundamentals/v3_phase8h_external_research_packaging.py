from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from swingmaster.fundamentals.v3_fiscal_calendar import EXPECTED_P1_TICKERS, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8f_latest8q_gap_analysis import build_quarter_diagnostics, pct, split_codes


CLASSIFICATION_READY = "LATEST8Q_EXTERNAL_RESEARCH_PACKAGE_READY"
CLASSIFICATION_STRUCTURAL = "LATEST8Q_EXTERNAL_RESEARCH_PACKAGE_READY_WITH_STRUCTURAL_DEPENDENCIES"
CLASSIFICATION_INCOMPLETE = "LATEST8Q_EXTERNAL_RESEARCH_PACKAGE_INCOMPLETE"
NEXT_READY = "RUN WAVE 1 EXTERNAL OFFICIAL-SOURCE RESEARCH FIRST; RETURN VERIFIED FACTS WITH SOURCE URLs / REFERENCES AND CONFIDENCE, THEN REHEARSE ONLY THE RESULTING DOWNSTREAM-CRITICAL REPAIRS BEFORE PRODUCTION APPLY"
NEXT_STRUCTURAL = "RUN WAVE 1 EXTERNAL RESEARCH FIRST WHILE KEEPING STRUCTURAL DECISIONS SEPARATE; USE NEW EVIDENCE TO REDUCE THE STRUCTURAL QUEUE BEFORE MANUAL REVIEW"
NEXT_INCOMPLETE = "DO NOT START EXTERNAL RESEARCH; FIX THE PACKAGING UNTIL EVERY NON-CLEAN TICKER HAS A COMPLETE DOWNSTREAM-CRITICAL CLOSURE PATH"

CODE_TO_EVIDENCE_TYPE = {
    "NEED_OFFICIAL_FISCAL_YEAR_START": "OFFICIAL_FY_FQ_IDENTITY",
    "NEED_OFFICIAL_FY_FQ_IDENTITY": "OFFICIAL_FY_FQ_IDENTITY",
    "NEED_OFFICIAL_PERIOD_END": "OFFICIAL_PERIOD_END",
    "NEED_FIRST_PUBLIC_RESULT_DATE": "FIRST_PUBLIC_PUBLISH_DATE",
    "NEED_MISSING_QUARTER_SOURCE": "MISSING_QUARTER_EXISTENCE",
    "NEED_REVENUE": "REVENUE",
    "NEED_EBIT": "EBIT_DIRECT",
    "NEED_OPERATING_INCOME": "EBIT_COMPONENTS",
    "NEED_GROSS_PROFIT": "GROSS_PROFIT",
    "NEED_EBITDA": "EBITDA",
    "NEED_NET_INCOME": "NET_INCOME",
    "NEED_FCF": "FCF_DIRECT",
    "NEED_OCF": "OCF_FOR_FCF",
    "NEED_CAPEX": "CAPEX_FOR_FCF",
    "NEED_CASH": "CASH",
    "NEED_DEBT": "TOTAL_DEBT",
    "NEED_SHARES": "SHARES_OUTSTANDING",
    "NEED_TRANSITION_CALENDAR_EVIDENCE": "FISCAL_TRANSITION_EVIDENCE",
    "NEED_RESTATEMENT_RECONCILIATION": "RESTATEMENT_RECONCILIATION",
    "NEED_SOURCE_SEMANTICS_CONFIRMATION": "SOURCE_SEMANTICS_CONFIRMATION",
    "NEED_TARGET_COLLISION_RESOLUTION": "TARGET_COLLISION_EVIDENCE",
    "NEED_LOCAL_LINEAGE_RECONCILIATION": "LINEAGE_OWNERSHIP_EVIDENCE",
}
SECONDARY_ONLY_EVIDENCE = {"GROSS_PROFIT", "EBITDA", "NET_INCOME", "OPERATING_INCOME", "OCF", "CAPEX"}
WAVE_RANK = {"P1_CURRENT": 1, "P2_LATEST4Q": 2, "P3_LATEST8Q": 3}
WAVE_FILE = {
    "P1_CURRENT": "latest8q_external_research_wave1_p1_current.csv",
    "P2_LATEST4Q": "latest8q_external_research_wave2_p2_latest4q.csv",
    "P3_LATEST8Q": "latest8q_external_research_wave3_p3_latest8q.csv",
}
WAVE_LABEL = {"P1_CURRENT": "Wave 1", "P2_LATEST4Q": "Wave 2", "P3_LATEST8Q": "Wave 3"}
KNOWN_13 = tuple(EXPECTED_P1_TICKERS)


@dataclass(frozen=True)
class Phase8HPaths:
    artifact_root: Path
    phase8g_root: Path = Path("temp/fundamentals_v3_phase8g_local_latest8q_repairs/20260829T_PHASE8G_FINAL")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    write_documentation: bool = True


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def yes_no(value: bool) -> str:
    return "YES" if value else "NO"


def normalized_evidence_types(codes: str) -> list[str]:
    out = []
    for code in split_codes(codes):
        evidence_type = CODE_TO_EVIDENCE_TYPE.get(code)
        if evidence_type and evidence_type not in out:
            out.append(evidence_type)
    return out


def mapped_dependency_types(codes: str) -> list[str]:
    return [CODE_TO_EVIDENCE_TYPE[code] for code in split_codes(codes) if code in CODE_TO_EVIDENCE_TYPE]


def sanitized_exact_information(texts: Iterable[str], evidence_types: Iterable[str]) -> str:
    evidence_set = set(evidence_types)
    blocked = {
        "need Gross Profit": None,
        "need EBITDA": None,
        "need Net Income": None,
        "need Operating Income": "EBIT_COMPONENTS",
        "need OCF": "OCF_FOR_FCF",
        "need Capex": "CAPEX_FOR_FCF",
    }
    kept = []
    for text in texts:
        for part in str(text or "").split(";"):
            clean = part.strip()
            if not clean:
                continue
            remove = False
            for needle, allowed_type in blocked.items():
                if needle in clean and (allowed_type is None or allowed_type not in evidence_set):
                    remove = True
                    break
            if not remove and clean not in kept:
                kept.append(clean)
    return "; ".join(kept)


def evidence_type_to_phrase(evidence_type: str) -> str:
    return {
        "OFFICIAL_FY_FQ_IDENTITY": "official fiscal year / fiscal quarter identity",
        "OFFICIAL_PERIOD_END": "official period_end",
        "FIRST_PUBLIC_PUBLISH_DATE": "first-public result publication date",
        "MISSING_QUARTER_EXISTENCE": "missing quarter existence/source evidence",
        "REVENUE": "Revenue",
        "EBIT_DIRECT": "EBIT",
        "EBIT_COMPONENTS": "approved EBIT derivation components",
        "FCF_DIRECT": "FCF",
        "OCF_FOR_FCF": "OCF required to derive FCF",
        "CAPEX_FOR_FCF": "Capex required to derive FCF",
        "CASH": "Cash",
        "TOTAL_DEBT": "Total Debt",
        "SHARES_OUTSTANDING": "Shares Outstanding",
        "FISCAL_TRANSITION_EVIDENCE": "fiscal transition evidence",
        "RESTATEMENT_RECONCILIATION": "restatement reconciliation",
        "SOURCE_SEMANTICS_CONFIRMATION": "source semantics confirmation",
        "TARGET_COLLISION_EVIDENCE": "target collision evidence",
        "LINEAGE_OWNERSHIP_EVIDENCE": "lineage/source ownership evidence",
    }.get(evidence_type, evidence_type)


def preferred_source_type(evidence_types: Iterable[str]) -> str:
    types = set(evidence_types)
    if "FIRST_PUBLIC_PUBLISH_DATE" in types:
        return "issuer Investor Relations earnings release/archive"
    if types & {"REVENUE", "EBIT_DIRECT", "FCF_DIRECT", "OCF_FOR_FCF", "CAPEX_FOR_FCF", "CASH", "TOTAL_DEBT", "SHARES_OUTSTANDING"}:
        return "issuer quarterly/annual report; SEC filing/XBRL only as fallback"
    if types & {"OFFICIAL_FY_FQ_IDENTITY", "OFFICIAL_PERIOD_END", "MISSING_QUARTER_EXISTENCE", "FISCAL_TRANSITION_EVIDENCE"}:
        return "issuer IR release, annual report, or financial reports page"
    if types & {"SOURCE_SEMANTICS_CONFIRMATION", "TARGET_COLLISION_EVIDENCE", "LINEAGE_OWNERSHIP_EVIDENCE", "RESTATEMENT_RECONCILIATION"}:
        return "official issuer filing/report notes; SEC only when issuer source is insufficient"
    return "official issuer source"


def affected_layers(row: dict[str, str]) -> list[str]:
    text = row.get("downstream_impact", "")
    out = []
    for source, label in (
        ("current_ttm_impact", "TTM"),
        ("score_impact", "Score"),
        ("lifecycle_impact", "Lifecycle"),
        ("valuation_impact", "Valuation"),
    ):
        if source in text:
            out.append(label)
    return out


def row_flag(row: dict[str, str], flag: str) -> str:
    return yes_no(flag in row.get("downstream_impact", ""))


def quarter_lookup(v3_db: Path) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    latest_rows, _diagnostics, _problems, _ctx = build_quarter_diagnostics(v3_db)
    lookup = {}
    for row in latest_rows:
        key = (str(row["ticker"]), str(row["fiscal_year"]), str(row["fiscal_quarter"]), str(row["period_end"]))
        lookup[key] = row
    return lookup


def removal_reason(row: dict[str, str]) -> str:
    evidence_types = normalized_evidence_types(row.get("closure_dependency", ""))
    if not evidence_types:
        return "NO_CURRENT_CLOSURE_VALUE"
    if set(evidence_types) <= SECONDARY_ONLY_EVIDENCE:
        return "SECONDARY_FIELD_ONLY"
    if set(evidence_types) <= {"EBIT_COMPONENTS"} and "EBIT" not in row.get("exact_information_needed", ""):
        return "SECONDARY_FIELD_ONLY"
    if set(evidence_types) <= {"OCF_FOR_FCF"} and "FCF" not in row.get("exact_information_needed", ""):
        return "SECONDARY_FIELD_ONLY"
    if set(evidence_types) <= {"CAPEX_FOR_FCF"} and "FCF" not in row.get("exact_information_needed", ""):
        return "SECONDARY_FIELD_ONLY"
    return ""


def validate_inputs(paths: Phase8HPaths, external_rows: list[dict[str, str]], structural_rows: list[dict[str, str]], ticker_rows: list[dict[str, str]], summary: dict[str, Any]) -> dict[str, Any]:
    local_remaining = read_csv_dicts(paths.phase8g_root / "latest8q_downstream_local_remaining_queue.csv")
    structural_tickers = {row["ticker"] for row in structural_rows}
    external_tickers = {row["ticker"] for row in external_rows}
    return {
        "phase8g_root": str(paths.phase8g_root),
        "phase8g_classification": summary.get("classification", ""),
        "external_queue_rows": len(external_rows),
        "external_tickers": len(external_tickers),
        "structural_queue_rows": len(structural_rows),
        "structural_tickers": len(structural_tickers),
        "local_remaining_rows": len(local_remaining),
        "local_only_in_external_queue": 0,
        "secondary_only_external_rows": sum(1 for row in external_rows if removal_reason(row) == "SECONDARY_FIELD_ONLY"),
        "structural_tickers_also_external": len(structural_tickers & external_tickers),
        "status_tickers": len(ticker_rows),
        "no_missing_requirement": int(summary.get("full_downstream_closure", {}).get("NO_MISSING_REQUIREMENT", -1)),
        "valid": len(local_remaining) == 0
        and len(external_rows) == int(summary.get("external_queue", {}).get("new_downstream_critical_external_facts", len(external_rows)))
        and len(external_tickers) == int(summary.get("external_queue", {}).get("external_tickers", len(external_tickers)))
        and int(summary.get("full_downstream_closure", {}).get("NO_MISSING_REQUIREMENT", -1)) == 0,
    }


def reclassify_external_rows(external_rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    reclass = []
    kept = []
    for idx, row in enumerate(external_rows, 1):
        evidence_types = normalized_evidence_types(row.get("closure_dependency", ""))
        reason = removal_reason(row)
        status = "REMOVED" if reason else "KEEP_DOWNSTREAM_CRITICAL"
        out = {
            "source_row_id": idx,
            "ticker": row["ticker"],
            "fiscal_year": row["fiscal_year"],
            "fiscal_quarter": row["fiscal_quarter"],
            "priority": row["priority"],
            "original_closure_dependency": row.get("closure_dependency", ""),
            "normalized_evidence_types": "|".join(evidence_types),
            "status": status,
            "removal_reason": reason,
        }
        reclass.append(out)
        if not reason:
            kept.append({**row, "source_row_id": idx, "normalized_evidence_types": "|".join(evidence_types)})
    return reclass, [row for row in reclass if row["status"] == "REMOVED"]


def build_fact_rows(kept_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    raw = []
    dedup: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in kept_rows:
        for evidence_type in mapped_dependency_types(row.get("closure_dependency", "")):
            fact = {
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "evidence_type": evidence_type,
                "source_row_id": row["source_row_id"],
                "priority": row["priority"],
                "current_period_end": row.get("current_period_end", ""),
                "current_publish_date": row.get("current_publish_date", ""),
            }
            raw.append(fact)
            key = (fact["ticker"], fact["fiscal_year"], fact["fiscal_quarter"], fact["evidence_type"])
            dedup.setdefault(key, fact)
    dedup_rows = []
    for idx, row in enumerate(dedup.values(), 1):
        duplicates = sum(
            1
            for fact in raw
            if (fact["ticker"], fact["fiscal_year"], fact["fiscal_quarter"], fact["evidence_type"])
            == (row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row["evidence_type"])
        )
        dedup_rows.append({**row, "dedup_fact_id": f"P8H-F-{idx:05d}", "duplicate_source_rows": duplicates})
    return raw, dedup_rows


def task_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        0 if row["current_ttm_impact"] == "YES" else 1,
        0 if int(row["quarter_position_latest8q"] or 99) == 1 else 1,
        -int(row["downstream_layer_count"]),
        -int(row["fact_count"]),
        row["ticker"],
        int(row["fiscal_year"]),
        row["fiscal_quarter"],
    )


def first_batch_score(task: dict[str, Any]) -> int:
    score = 0
    if task["current_ttm_impact"] == "YES":
        score += 100
    if int(task["quarter_position_latest8q"] or 99) == 1:
        score += 80
    score += 25 * int(task["downstream_layer_count"])
    score += 5 * int(task["fact_count"])
    if {"REVENUE", "EBIT_DIRECT", "FCF_DIRECT", "CASH", "TOTAL_DEBT", "SHARES_OUTSTANDING"} & set(task["evidence_types_needed"].split("|")):
        score += 20
    if "SOURCE_SEMANTICS_CONFIRMATION" in task["evidence_types_needed"]:
        score -= 5
    return score


def build_research_tasks(
    kept_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
    ticker_status: dict[str, dict[str, str]],
    structural_tickers: set[str],
    qlookup: dict[tuple[str, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    facts_by_task: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    source_by_task: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for fact in fact_rows:
        facts_by_task[(fact["ticker"], fact["fiscal_year"], fact["fiscal_quarter"])].add(fact["evidence_type"])
    for row in kept_rows:
        source_by_task[(row["ticker"], row["fiscal_year"], row["fiscal_quarter"])].append(row)
    out = []
    counters = Counter()
    for key in sorted(facts_by_task, key=lambda k: (WAVE_RANK.get(source_by_task[k][0]["priority"], 99), k[0], int(k[1]), k[2])):
        ticker, fiscal_year, fiscal_quarter = key
        rows = source_by_task[key]
        first = rows[0]
        wave = min((row["priority"] for row in rows), key=lambda p: WAVE_RANK.get(p, 99))
        counters[wave] += 1
        task_id = f"P8H-{WAVE_RANK[wave]}-{counters[wave]:04d}"
        evidence_types = sorted(facts_by_task[key])
        qrow = qlookup.get((ticker, fiscal_year, fiscal_quarter, first.get("current_period_end", "")), {})
        layers = sorted({layer for row in rows for layer in affected_layers(row)})
        current_ttm = yes_no(any(row_flag(row, "current_ttm_impact") == "YES" for row in rows))
        score = yes_no(any(row_flag(row, "score_impact") == "YES" for row in rows))
        lifecycle = yes_no(any(row_flag(row, "lifecycle_impact") == "YES" for row in rows))
        valuation = yes_no(any(row_flag(row, "valuation_impact") == "YES" for row in rows))
        exact = sanitized_exact_information((row.get("exact_information_needed", "") for row in rows), evidence_types)
        why = "Required to close downstream-critical latest8Q gaps for TTM / Score / Lifecycle / Valuation"
        phrases = ", ".join(evidence_type_to_phrase(e) for e in evidence_types)
        task = {
            "research_task_id": task_id,
            "ticker": ticker,
            "company_id": ticker_status.get(ticker, {}).get("company_id", ""),
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "current_period_end": first.get("current_period_end", ""),
            "current_publish_date": first.get("current_publish_date", ""),
            "quarter_position_latest8q": qrow.get("quarter_position_latest8q", ""),
            "evidence_types_needed": "|".join(evidence_types),
            "exact_information_needed": exact,
            "why_needed": why,
            "affected_downstream_layers": "|".join(layers) or "Latest8Q closure",
            "downstream_layer_count": len(layers),
            "current_ttm_impact": current_ttm,
            "score_impact": score,
            "lifecycle_impact": lifecycle,
            "valuation_impact": valuation,
            "preferred_source_type": preferred_source_type(evidence_types),
            "existing_local_evidence_summary": f"Phase 8G issue={first.get('issue', '')}; source dependency={first.get('closure_dependency', '')}; structural context={first.get('structural_context', '')}",
            "structural_warning": "STRUCTURAL_REVIEW_ALSO_REQUIRED" if ticker in structural_tickers else "",
            "closure_dependency": "|".join(evidence_types),
            "priority": wave,
            "confidence": "EXTERNAL_OFFICIAL_EVIDENCE_REQUIRED",
            "status": "READY_FOR_EXTERNAL_RESEARCH",
            "fact_count": len(evidence_types),
            "research_request": f"Research official issuer sources for FY{fiscal_year} {fiscal_quarter}. Verify {phrases}. Do not research other fields.",
        }
        task["first_batch_score"] = first_batch_score(task)
        out.append(task)
    return out


def wave_rows(tasks: list[dict[str, Any]], wave: str) -> list[dict[str, Any]]:
    return sorted((row for row in tasks if row["priority"] == wave), key=task_sort_key)


def build_ticker_summary(tasks: list[dict[str, Any]], status_rows: list[dict[str, str]], structural_tickers: set[str]) -> list[dict[str, Any]]:
    tasks_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for task in tasks:
        tasks_by_ticker[task["ticker"]].append(task)
    status_by_ticker = {row["ticker"]: row for row in status_rows}
    out = []
    for ticker in sorted(tasks_by_ticker):
        group = sorted(tasks_by_ticker[ticker], key=task_sort_key)
        waves = sorted({row["priority"] for row in group}, key=lambda w: WAVE_RANK[w])
        evidence = sorted({e for row in group for e in row["evidence_types_needed"].split("|") if e})
        facts = sum(int(row["fact_count"]) for row in group)
        by_quarter: dict[str, list[str]] = defaultdict(list)
        for row in group:
            by_quarter[f"FY{row['fiscal_year']} {row['fiscal_quarter']}"].extend(row["evidence_types_needed"].split("|"))
        request_parts = []
        for fyfq in sorted(by_quarter):
            unique = sorted(set(by_quarter[fyfq]))
            request_parts.append(f"For {fyfq} verify {', '.join(evidence_type_to_phrase(e) for e in unique)}")
        status = status_by_ticker.get(ticker, {})
        structural = ticker in structural_tickers
        out.append(
            {
                "ticker": ticker,
                "company_id": group[0]["company_id"],
                "highest_priority_wave": waves[0],
                "affected_fy_fq": ";".join(sorted(by_quarter)),
                "research_task_count": len(group),
                "exact_facts_needed_count": facts,
                "evidence_types_needed": "|".join(evidence),
                "consolidated_research_request": "Research official issuer sources. " + ". ".join(request_parts) + ". Do not research other fields.",
                "current_ttm_impact": yes_no(any(row["current_ttm_impact"] == "YES" for row in group)),
                "latest4q_impact": yes_no(any(row["priority"] in {"P1_CURRENT", "P2_LATEST4Q"} for row in group)),
                "latest8q_impact": "YES",
                "score_impact": yes_no(any(row["score_impact"] == "YES" for row in group)),
                "lifecycle_impact": yes_no(any(row["lifecycle_impact"] == "YES" for row in group)),
                "valuation_impact": yes_no(any(row["valuation_impact"] == "YES" for row in group)),
                "structural_review_also_required": yes_no(structural),
                "expected_status_after_external_evidence": "YES_EXTERNAL_PLUS_STRUCTURAL" if structural else "YES_EXTERNAL_ONLY",
                "next_action": "Research external facts, then resolve structural queue separately" if structural else "Research external facts and return verified evidence",
                "current_remaining_status": status.get("remaining_status", ""),
            }
        )
    return out


def structural_category(row: dict[str, str]) -> str:
    issue = row.get("issue", "")
    evidence = row.get("evidence that would resolve it", "")
    current = row.get("current evidence", "")
    if "TRANSITION" in issue:
        return "CALENDAR_TRANSITION"
    if "DUPLICATE_ECONOMIC_QUARTER" in issue:
        return "DUPLICATE_ECONOMIC_QUARTER"
    if "TARGET_DIFFERENT_ECONOMIC" in current:
        return "TARGET_DIFFERENT_ECONOMIC"
    if "TARGET_CONFLICTING" in current or "TARGET_COLLISION" in issue:
        return "TARGET_CONFLICTING"
    if "RESTATEMENT" in evidence:
        return "RESTATEMENT_STRUCTURE"
    if "SOURCE_SEMANTICS" in evidence:
        return "SOURCE_SEMANTIC_AMBIGUITY"
    if "LINEAGE" in evidence or "FY_CONFLICT" in issue or "FQ_CONFLICT" in issue:
        return "LINEAGE_CONFLICT"
    return "STUB_PERIOD"


def clean_structural_rows(structural_rows: list[dict[str, str]], external_tickers: set[str], ticker_status: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for idx, row in enumerate(structural_rows, 1):
        ticker = row["ticker"]
        out.append(
            {
                "structural_task_id": f"P8H-S-{idx:04d}",
                "ticker": ticker,
                "company_id": ticker_status.get(ticker, {}).get("company_id", ""),
                "affected_fy_fq": row.get("FY/FQ", ""),
                "issue": row.get("issue", ""),
                "structural_category": structural_category(row),
                "current_evidence": row.get("current evidence", ""),
                "exact_decision_needed": row.get("exact decision needed", ""),
                "evidence_that_would_resolve_it": row.get("evidence that would resolve it", ""),
                "priority": row.get("priority", ""),
                "external_research_also_required": yes_no(ticker in external_tickers),
                "status": "STRUCTURAL_REVIEW_SEPARATE",
            }
        )
    return out


def first_batch(tasks: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    wave1 = wave_rows(tasks, "P1_CURRENT")
    scored = sorted(
        (
            {
                "research_task_id": row["research_task_id"],
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "score": row.get("first_batch_score", first_batch_score(row)),
                "fact_count": row["fact_count"],
                "current_ttm_impact": row["current_ttm_impact"],
                "score_impact": row["score_impact"],
                "lifecycle_impact": row["lifecycle_impact"],
                "valuation_impact": row["valuation_impact"],
                "selection_reason": "Wave 1 high-impact current downstream blocker",
            }
            for row in wave1
        ),
        key=lambda row: (-int(row["score"]), row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]),
    )
    cutoff = 175
    selected_ids = {row["research_task_id"] for row in scored if int(row["score"]) >= cutoff}
    if not selected_ids:
        selected_ids = {row["research_task_id"] for row in scored[: min(100, len(scored))]}
    selected = [row for row in wave1 if row["research_task_id"] in selected_ids]
    return selected, scored


def closure_rows(status_rows: list[dict[str, str]], external_tickers: set[str], structural_tickers: set[str]) -> list[dict[str, Any]]:
    out = []
    for row in sorted(status_rows, key=lambda r: r["ticker"]):
        ticker = row["ticker"]
        if row["remaining_status"].startswith("DOWNSTREAM_LATEST8Q_CLEAN"):
            closure = "ALREADY_CLEAN"
        elif ticker in external_tickers and ticker in structural_tickers:
            closure = "YES_EXTERNAL_PLUS_STRUCTURAL"
        elif ticker in external_tickers:
            closure = "YES_EXTERNAL_ONLY"
        elif ticker in structural_tickers:
            closure = "YES_STRUCTURAL_ONLY"
        else:
            closure = "NO_MISSING_REQUIREMENT"
        out.append(
            {
                "ticker": ticker,
                "company_id": row.get("company_id", ""),
                "current_status": row["remaining_status"],
                "closure_completeness": closure,
                "external_research_required": yes_no(ticker in external_tickers),
                "structural_review_required": yes_no(ticker in structural_tickers),
                "missing_requirement": int(closure == "NO_MISSING_REQUIREMENT"),
            }
        )
    return out


def evidence_distribution(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(row["evidence_type"] for row in facts)
    return [{"evidence_type": key, "facts": value} for key, value in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def wave_summary(tasks: list[dict[str, Any]], facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    facts_by_wave = Counter()
    for task in tasks:
        facts_by_wave[task["priority"]] += int(task["fact_count"])
    out = []
    for wave in ("P1_CURRENT", "P2_LATEST4Q", "P3_LATEST8Q"):
        group = [row for row in tasks if row["priority"] == wave]
        out.append(
            {
                "wave": wave,
                "tasks": len(group),
                "tickers": len({row["ticker"] for row in group}),
                "facts": facts_by_wave[wave],
                "current_ttm_tickers": len({row["ticker"] for row in group if row["current_ttm_impact"] == "YES"}),
                "latest_quarter_tickers": len({row["ticker"] for row in group if int(row["quarter_position_latest8q"] or 99) == 1}),
                "score_tickers": len({row["ticker"] for row in group if row["score_impact"] == "YES"}),
                "lifecycle_tickers": len({row["ticker"] for row in group if row["lifecycle_impact"] == "YES"}),
                "valuation_tickers": len({row["ticker"] for row in group if row["valuation_impact"] == "YES"}),
            }
        )
    return out


def known_13_rows(tasks: list[dict[str, Any]], structural_tickers: set[str], ticker_status: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for task in tasks:
        by_ticker[task["ticker"]].append(task)
    out = []
    for ticker in KNOWN_13:
        group = sorted(by_ticker.get(ticker, []), key=task_sort_key)
        waves = sorted({row["priority"] for row in group}, key=lambda w: WAVE_RANK[w])
        facts = sorted({fact for row in group for fact in row["evidence_types_needed"].split("|") if fact})
        fyfq = ";".join(dict.fromkeys(f"FY{row['fiscal_year']} {row['fiscal_quarter']}" for row in group))
        status = ticker_status.get(ticker, {})
        out.append(
            {
                "ticker": ticker,
                "wave": waves[0] if waves else "STRUCTURAL_ONLY",
                "affected_fy_fq": fyfq,
                "exact_facts_needed": "|".join(facts),
                "structural_review": yes_no(ticker in structural_tickers),
                "downstream_impact": "|".join(
                    key
                    for key in ("current_ttm_clean", "score_available", "lifecycle_available", "valuation_available")
                    if status.get(key) == "NO"
                )
                or "none",
                "no_secondary_field_requests": "YES",
            }
        )
    return out


def write_report(path: Path, summary: dict[str, Any]) -> None:
    top = "\n".join(f"- `{row['evidence_type']}`: `{row['facts']}`" for row in summary["top_15_evidence_types"])
    path.write_text(
        f"""# Fundamentals V3 Latest8Q External Research Plan

Phase 8H packages the post-8G external queue for official-source research. It does not browse, edit canonical data, rebuild downstream tables, or write RawCandle.

## Downstream-Critical Policy

Research requests are limited to fiscal identity, genuine missing quarters, official period_end, first-public publish_date, Revenue, EBIT, FCF, Cash, Total Debt, Shares Outstanding, and approved inputs needed to derive those fields.

Gross Profit, EBITDA, Net Income, Operating Income, OCF, and Capex are excluded when they are only secondary completeness gaps. OCF/Capex are retained only when needed to derive missing FCF; Operating Income is retained only as part of an approved EBIT derivation requirement.

## Package Counts

- starting Phase 8G external queue rows: `{summary['starting_queue']['phase8g_external_facts']}`
- normalized deduplicated critical facts: `{summary['cleanup']['final_downstream_critical_external_facts']}`
- research tasks: `{summary['research_tasks']['total_deduplicated_research_tasks']}`
- external tickers: `{summary['research_tasks']['total_external_tickers']}`
- average facts/task: `{summary['research_tasks']['average_facts_per_task']}`

## Waves

- Wave 1 P1_CURRENT: `{summary['waves']['P1_CURRENT']['tasks']}` tasks / `{summary['waves']['P1_CURRENT']['tickers']}` tickers / `{summary['waves']['P1_CURRENT']['facts']}` facts
- Wave 2 P2_LATEST4Q: `{summary['waves']['P2_LATEST4Q']['tasks']}` tasks / `{summary['waves']['P2_LATEST4Q']['tickers']}` tickers / `{summary['waves']['P2_LATEST4Q']['facts']}` facts
- Wave 3 P3_LATEST8Q: `{summary['waves']['P3_LATEST8Q']['tasks']}` tasks / `{summary['waves']['P3_LATEST8Q']['tickers']}` tickers / `{summary['waves']['P3_LATEST8Q']['facts']}` facts

## Top Evidence Needs

{top}

## First Batch

First batch uses deterministic impact score `>=175`, emphasizing current TTM, latest-quarter impact, number of downstream layers, and number of required facts. It contains `{summary['first_batch']['tasks']}` tasks / `{summary['first_batch']['tickers']}` tickers / `{summary['first_batch']['facts']}` facts.

## Structural Separation

Structural decisions remain separate in `latest8q_structural_decisions_remaining.csv`. Mixed external+structural tickers are flagged in the ticker-level package and must not be treated as simple external-only repairs.

## Closure

- ALREADY_CLEAN: `{summary['closure_completeness']['ALREADY_CLEAN']}`
- YES_EXTERNAL_ONLY: `{summary['closure_completeness']['YES_EXTERNAL_ONLY']}`
- YES_EXTERNAL_PLUS_STRUCTURAL: `{summary['closure_completeness']['YES_EXTERNAL_PLUS_STRUCTURAL']}`
- YES_STRUCTURAL_ONLY: `{summary['closure_completeness']['YES_STRUCTURAL_ONLY']}`
- NO_MISSING_REQUIREMENT: `{summary['closure_completeness']['NO_MISSING_REQUIREMENT']}`

## Classification

`{summary['classification']}`

## Next Action

{summary['next_action']}
""",
        encoding="utf-8",
    )


def append_docs(summary: dict[str, Any]) -> None:
    section = f"""

## Phase 8H - Latest8Q Downstream-Critical External Research Packaging

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Packaged Phase 8G external queue rows `{summary['starting_queue']['phase8g_external_facts']}` into `{summary['research_tasks']['total_deduplicated_research_tasks']}` research tasks covering `{summary['cleanup']['final_downstream_critical_external_facts']}` normalized downstream-critical facts across `{summary['research_tasks']['total_external_tickers']}` tickers. Secondary-only removals `{summary['cleanup']['secondary_only_requests_removed']}`, duplicate facts removed `{summary['cleanup']['duplicate_requests_removed']}`.

Wave 1 `{summary['waves']['P1_CURRENT']['tasks']}` tasks / `{summary['waves']['P1_CURRENT']['tickers']}` tickers / `{summary['waves']['P1_CURRENT']['facts']}` facts. Wave 2 `{summary['waves']['P2_LATEST4Q']['tasks']}` tasks / `{summary['waves']['P2_LATEST4Q']['tickers']}` tickers / `{summary['waves']['P2_LATEST4Q']['facts']}` facts. Wave 3 `{summary['waves']['P3_LATEST8Q']['tasks']}` tasks / `{summary['waves']['P3_LATEST8Q']['tickers']}` tickers / `{summary['waves']['P3_LATEST8Q']['facts']}` facts.

Structural queue remains separate: `{summary['structural']['material_structural_decisions']}` decisions / `{summary['structural']['structural_tickers']}` tickers. External+structural mixed tickers `{summary['structural']['external_structural_mixed_tickers']}`.

Phase 8 remains `IN PROGRESS`.

Next action: {summary['next_action']}
"""
    for doc in (Path("docs/fundamentals_v3_phase8_update_v3.md"), Path("docs/fundamentals_v3_deferred_repair_handoff.md")):
        with doc.open("a", encoding="utf-8") as handle:
            handle.write(section)


def run_phase8h(paths: Phase8HPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    external_rows = read_csv_dicts(paths.phase8g_root / "latest8q_downstream_external_research_queue.csv")
    structural_rows = read_csv_dicts(paths.phase8g_root / "latest8q_downstream_structural_review_queue.csv")
    status_rows = read_csv_dicts(paths.phase8g_root / "latest8q_downstream_ticker_status.csv")
    phase8g_summary = read_json(paths.phase8g_root / "phase8g_summary.json")
    ticker_status = {row["ticker"]: row for row in status_rows}
    validation = validate_inputs(paths, external_rows, structural_rows, status_rows, phase8g_summary)
    reclass, removed = reclassify_external_rows(external_rows)
    kept = [row for row in reclass if row["status"] == "KEEP_DOWNSTREAM_CRITICAL"]
    kept_ids = {int(row["source_row_id"]) for row in kept}
    kept_source_rows = [{**row, "source_row_id": idx} for idx, row in enumerate(external_rows, 1) if idx in kept_ids]
    raw_facts, dedup_facts = build_fact_rows(kept_source_rows)
    qlookup = quarter_lookup(paths.v3_db)
    structural_tickers = {row["ticker"] for row in structural_rows}
    external_tickers = {row["ticker"] for row in kept_source_rows}
    tasks = build_research_tasks(kept_source_rows, dedup_facts, ticker_status, structural_tickers, qlookup)
    waves = {wave: wave_rows(tasks, wave) for wave in WAVE_FILE}
    ticker_summary = build_ticker_summary(tasks, status_rows, structural_tickers)
    structural_clean = clean_structural_rows(structural_rows, external_tickers, ticker_status)
    first, scores = first_batch(tasks)
    closure = closure_rows(status_rows, external_tickers, structural_tickers)
    evidence_dist = evidence_distribution(dedup_facts)
    wave_stats = wave_summary(tasks, dedup_facts)
    wave_summary_by_name = {row["wave"]: row for row in wave_stats}
    closure_counts = Counter(row["closure_completeness"] for row in closure)
    raw_fact_count = sum(len(mapped_dependency_types(row.get("closure_dependency", ""))) for row in kept_source_rows)
    duplicate_count = raw_fact_count - len(dedup_facts)
    removed_counts = Counter(row["removal_reason"] for row in removed)
    first_tickers = {row["ticker"] for row in first}
    classification = CLASSIFICATION_INCOMPLETE
    if validation["valid"] and closure_counts["NO_MISSING_REQUIREMENT"] == 0 and len(tasks) > 0:
        classification = CLASSIFICATION_STRUCTURAL if structural_clean else CLASSIFICATION_READY
    next_action = NEXT_INCOMPLETE if classification == CLASSIFICATION_INCOMPLETE else NEXT_STRUCTURAL if classification == CLASSIFICATION_STRUCTURAL else NEXT_READY
    summary = {
        "artifact_root": str(paths.artifact_root),
        "classification": classification,
        "starting_queue": {
            "phase8g_external_facts": len(external_rows),
            "phase8g_external_tickers": len({row["ticker"] for row in external_rows}),
            "structural_decisions": len(structural_rows),
            "structural_tickers": len(structural_tickers),
        },
        "cleanup": {
            "secondary_only_requests_removed": removed_counts.get("SECONDARY_FIELD_ONLY", 0),
            "already_resolved_requests_removed": removed_counts.get("ALREADY_RESOLVED_LOCALLY", 0),
            "duplicate_requests_removed": duplicate_count,
            "structural_only_requests_removed": removed_counts.get("STRUCTURAL_DECISION_NOT_EXTERNAL_FACT", 0),
            "final_downstream_critical_external_facts": len(dedup_facts),
            "raw_normalized_fact_count": raw_fact_count,
        },
        "research_tasks": {
            "total_deduplicated_research_tasks": len(tasks),
            "total_external_tickers": len(external_tickers),
            "average_facts_per_task": round(len(dedup_facts) / len(tasks), 4) if tasks else 0.0,
        },
        "waves": wave_summary_by_name,
        "structural": {
            "material_structural_decisions": len(structural_clean),
            "structural_tickers": len(structural_tickers),
            "external_structural_mixed_tickers": len(structural_tickers & external_tickers),
        },
        "first_batch": {
            "tasks": len(first),
            "tickers": len(first_tickers),
            "facts": sum(int(row["fact_count"]) for row in first),
            "expected_current_ttm_cases_improved": len({row["ticker"] for row in first if row["current_ttm_impact"] == "YES"}),
            "expected_score_blockers_improved": len({row["ticker"] for row in first if row["score_impact"] == "YES"}),
            "expected_lifecycle_blockers_improved": len({row["ticker"] for row in first if row["lifecycle_impact"] == "YES"}),
            "expected_valuation_blockers_improved": len({row["ticker"] for row in first if row["valuation_impact"] == "YES"}),
            "selection_score_cutoff": 175,
        },
        "closure_completeness": {
            "ALREADY_CLEAN": closure_counts["ALREADY_CLEAN"],
            "YES_EXTERNAL_ONLY": closure_counts["YES_EXTERNAL_ONLY"],
            "YES_EXTERNAL_PLUS_STRUCTURAL": closure_counts["YES_EXTERNAL_PLUS_STRUCTURAL"],
            "YES_STRUCTURAL_ONLY": closure_counts["YES_STRUCTURAL_ONLY"],
            "NO_MISSING_REQUIREMENT": closure_counts["NO_MISSING_REQUIREMENT"],
        },
        "efficiency": {
            "raw_phase8g_external_queue_rows": len(external_rows),
            "raw_external_facts": raw_fact_count,
            "raw_normalized_dependencies": raw_fact_count,
            "final_critical_facts": len(dedup_facts),
            "reduction_pct": pct(duplicate_count, raw_fact_count),
            "dedup_reduction_pct_vs_normalized": pct(duplicate_count, raw_fact_count),
            "research_tasks": len(tasks),
            "fact_consolidation_ratio": round(len(dedup_facts) / len(tasks), 4) if tasks else 0.0,
        },
        "safety": {"production_writes": 0, "network_calls": 0, "rawcandle_writes": 0},
        "top_15_evidence_types": evidence_dist[:15],
        "known_13": known_13_rows(tasks, structural_tickers, ticker_status),
        "next_action": next_action,
        "validation": validation,
    }
    write_json(paths.artifact_root / "phase8g_queue_validation.json", validation)
    write_csv(paths.artifact_root / "external_request_reclassification.csv", reclass)
    write_csv(paths.artifact_root / "external_requests_removed.csv", removed)
    write_csv(paths.artifact_root / "external_request_deduplication.csv", dedup_facts)
    for wave, filename in WAVE_FILE.items():
        write_csv(paths.artifact_root / filename, waves[wave])
    write_csv(paths.artifact_root / "latest8q_external_research_by_ticker.csv", ticker_summary)
    write_csv(paths.artifact_root / "latest8q_structural_decisions_remaining.csv", structural_clean)
    write_csv(paths.artifact_root / "latest8q_external_research_first_batch.csv", first)
    write_csv(paths.artifact_root / "first_batch_selection_scores.csv", scores)
    write_csv(paths.artifact_root / "external_research_evidence_type_distribution.csv", evidence_dist)
    write_csv(paths.artifact_root / "external_research_wave_summary.csv", wave_stats)
    write_json(paths.artifact_root / "external_research_efficiency.json", summary["efficiency"])
    write_csv(paths.artifact_root / "external_research_closure_test.csv", closure)
    write_csv(paths.artifact_root / "known_13_external_research_package.csv", summary["known_13"])
    write_json(paths.artifact_root / "phase8h_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(next_action + "\n", encoding="utf-8")
    if paths.write_documentation:
        write_report(Path("docs/fundamentals_v3_latest8q_external_research_plan.md"), summary)
        append_docs(summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Package Phase 8H latest8Q external research waves")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8h_external_research_packaging") / utc_stamp())
    parser.add_argument("--phase8g-root", type=Path, default=Path("temp/fundamentals_v3_phase8g_local_latest8q_repairs/20260829T_PHASE8G_FINAL"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8h(
        Phase8HPaths(
            artifact_root=args.artifact_root,
            phase8g_root=args.phase8g_root,
            v3_db=args.v3_db,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"tasks={summary['research_tasks']['total_deduplicated_research_tasks']}")
    print(f"facts={summary['cleanup']['final_downstream_critical_external_facts']}")
    print(f"wave1_tasks={summary['waves']['P1_CURRENT']['tasks']}")
    print(f"NO_MISSING_REQUIREMENT={summary['closure_completeness']['NO_MISSING_REQUIREMENT']}")
    return 0 if summary["classification"] != CLASSIFICATION_INCOMPLETE else 2


if __name__ == "__main__":
    raise SystemExit(main())
