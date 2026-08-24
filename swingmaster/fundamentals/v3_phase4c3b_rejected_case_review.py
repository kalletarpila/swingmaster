from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import v3_phase4c2d_sec_formula_rerun as phase4c2d
from swingmaster.fundamentals.v3_canonical_closure import field_coverage_summary, final_canonical_baseline
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity


OUTCOME_NO_ISSUE = "REJECTED_CASE_REVIEW_NO_SYSTEMATIC_ISSUE_FOUND"
OUTCOME_REFINEMENT = "REJECTED_CASE_REVIEW_BOUNDED_REFINEMENT_JUSTIFIED"
OUTCOME_MAJOR = "REJECTED_CASE_REVIEW_MAJOR_ARCHITECTURAL_ISSUE_FOUND"
NEXT_4D = "MASTER PLAN PHASE 4D - HISTORICAL COMPLETENESS CLOSURE"
NEXT_4C3C = "MASTER PLAN PHASE 4C-3C - BOUNDED REJECTION-LOGIC REFINEMENT"


def run_phase4c3b_rejected_case_review(*, v3_db: Path, component_db: Path, artifact_root: Path, sample_size: int = 15) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline = final_canonical_baseline(v3_db)
    missing = {row["field"]: int(row["null_q"]) for row in field_coverage_summary(v3_db)}
    v3_rows = phase4c2d.load_v3_rows(v3_db)
    qcomponents, _, qfailures = phase4c2d.quarterize_components(phase4c2d.load_component_rows(component_db))
    mapped = phase4c2d.map_targets_to_components(v3_rows, qcomponents)
    component_facts = load_component_fact_rows(component_db)
    pool = build_rejected_case_pool(mapped, qfailures)
    selected = select_cases(pool, sample_size=sample_size)
    ebit_candidates = phase4c2d.build_ebit_candidates(mapped)
    da_candidates = phase4c2d.build_da_candidates(mapped)
    reviews = [review_case(case, mapped, component_facts, ebit_candidates, da_candidates) for case in selected]
    pattern_counts = pattern_frequency_scan(pool, reviews)
    outcome = decide_outcome(reviews, pattern_counts)
    next_step = NEXT_4C3C if outcome == OUTCOME_REFINEMENT else NEXT_4D
    integrity = structural_integrity(v3_db)
    summary = {
        "outcome": outcome,
        "recommended_next_step": next_step,
        "baseline": {
            "companies": baseline["company_total"],
            "canonical_q": baseline["coverage"]["canonical_q_total"],
            "core_ready": baseline["coverage"]["core_ready_q"],
            "core_not_ready": baseline["coverage"]["core_not_ready_q"],
            "ebit_missing": missing.get("ebit", 0),
            "ebitda_missing": missing.get("ebitda", 0),
        },
        "selection": {
            "selected_cases": len(reviews),
            "ebit_cases": sum(1 for row in reviews if row["metric"] == "ebit"),
            "ebitda_cases": sum(1 for row in reviews if row["metric"] == "ebitda"),
            "q4_cases": sum(1 for row in reviews if row["fiscal_quarter"] == "Q4"),
        },
        "dispositions": dict(Counter(row["disposition"] for row in reviews)),
        "patterns": summarize_patterns(pattern_counts),
        "full_population_impact": full_population_impact(pattern_counts),
        "safety": {"canonical_financial_writes": 0, "metadata_writes": 0},
        "integrity": integrity,
        "artifact_root": str(artifact_root),
    }
    write_artifacts(artifact_root, summary, pool, selected, reviews, component_facts, pattern_counts)
    write_doc(Path("docs/fundamentals_v3_phase4c_3b_rejected_case_review.md"), summary, reviews, pattern_counts)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def load_component_fact_rows(component_db: Path) -> list[dict[str, Any]]:
    rows = phase4c2d.load_component_rows(component_db)
    return sorted(rows, key=lambda row: (row["ticker"], int(row["fiscal_year"] or 0), row["fiscal_period"] or "", row["semantic_role"], row["concept_name"]))


def build_rejected_case_pool(mapped: list[dict[str, Any]], qfailures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    qfailure_keys = {(int(row["company_id"]), int(row["fiscal_year"]), row["fiscal_quarter"], row["semantic_role"]) for row in qfailures if row.get("company_id") and row.get("fiscal_year")}
    out = []
    for row in mapped:
        for metric in ("ebit", "ebitda"):
            if row.get(metric) is not None:
                continue
            category = categorize_rejection(row, metric, qfailure_keys)
            candidate = candidate_value(row, metric)
            out.append({
                "company_id": row["company_id"],
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["period_end_date"],
                "metric": metric,
                "rejection_category": category,
                "diagnostic_priority": diagnostic_priority(row, metric, category),
                "formula_component_candidate": candidate["formula"],
                "calculated_value": candidate["value"],
                "component_roles": "|".join(sorted(row["components"])),
            })
    return sorted(out, key=lambda row: (-int(row["diagnostic_priority"]), row["ticker"], row["metric"], int(row["fiscal_year"]), row["fiscal_quarter"]))


def categorize_rejection(row: dict[str, Any], metric: str, qfailure_keys: set[tuple[int, int, str, str]]) -> str:
    comps = row["components"]
    if row["fiscal_quarter"] == "Q4":
        return "Q4_REJECTION" if metric == "ebit" else "Q4_DA_REJECTION"
    if metric == "ebit":
        interest_count = len(set(comps) & (phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES))
        if "PRETAX" not in comps or any((row["company_id"], row["fiscal_year"], row["fiscal_quarter"], "PRETAX") == key for key in qfailure_keys):
            return "COMPONENT_QUARTERIZATION_REJECTION"
        if interest_count > 1:
            return "MULTIPLE_INTEREST_CANDIDATES"
        if "ISSUER_SPECIFIC_INTEREST" in comps:
            return "ISSUER_SPECIFIC_UNRESOLVED"
        if interest_count == 1:
            return "LOW_SAMPLE"
        return "INTEREST_SEMANTIC_AMBIGUITY"
    has_da = bool(set(comps) & phase4c2d.DA_ROLES)
    has_dep_amort = bool(set(comps) & phase4c2d.DEP_ROLES) and bool(set(comps) & phase4c2d.AMORT_ROLES)
    if row.get("ebit") is not None and (has_da or has_dep_amort):
        return "CANONICAL_EBIT_AVAILABLE_BUT_DA_NOT_APPROVED"
    if has_da and has_dep_amort:
        return "COMBINED_DA_VS_DEP_AMORT_CONFLICT"
    if has_dep_amort:
        return "LOW_SAMPLE_DA"
    return "D_AND_A_SEMANTIC_REJECTION"


def diagnostic_priority(row: dict[str, Any], metric: str, category: str) -> int:
    score = 0
    comps = row["components"]
    if metric == "ebit" and "PRETAX" in comps:
        score += 3
    if metric == "ebit" and set(comps) & (phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES):
        score += 3
    if metric == "ebitda" and row.get("ebit") is not None:
        score += 3
    if metric == "ebitda" and (set(comps) & phase4c2d.DA_ROLES or (set(comps) & phase4c2d.DEP_ROLES and set(comps) & phase4c2d.AMORT_ROLES)):
        score += 3
    if row["fiscal_quarter"] == "Q4":
        score += 2
    if category in {"MULTIPLE_INTEREST_CANDIDATES", "CANONICAL_EBIT_AVAILABLE_BUT_DA_NOT_APPROVED", "LOW_SAMPLE", "LOW_SAMPLE_DA"}:
        score += 2
    return score


def select_cases(pool: list[dict[str, Any]], *, sample_size: int = 15) -> list[dict[str, Any]]:
    wanted = [
        ("MULTIPLE_INTEREST_CANDIDATES", "ebit"),
        ("MULTIPLE_INTEREST_CANDIDATES", "ebit"),
        ("INTEREST_SEMANTIC_AMBIGUITY", "ebit"),
        ("ISSUER_SPECIFIC_UNRESOLVED", "ebit"),
        ("LOW_SAMPLE", "ebit"),
        ("COMPONENT_QUARTERIZATION_REJECTION", "ebit"),
        ("Q4_REJECTION", "ebit"),
        ("CANONICAL_EBIT_AVAILABLE_BUT_DA_NOT_APPROVED", "ebitda"),
        ("CANONICAL_EBIT_AVAILABLE_BUT_DA_NOT_APPROVED", "ebitda"),
        ("COMBINED_DA_VS_DEP_AMORT_CONFLICT", "ebitda"),
        ("LOW_SAMPLE_DA", "ebitda"),
        ("D_AND_A_SEMANTIC_REJECTION", "ebitda"),
        ("Q4_DA_REJECTION", "ebitda"),
        ("Q4_DA_REJECTION", "ebitda"),
        ("NUMERICALLY_CLOSE_BUT_NOT_APPROVED", ""),
    ]
    selected = []
    seen_companies: set[tuple[Any, str]] = set()
    seen_quarters: set[tuple[Any, str, Any, str]] = set()
    for category, metric in wanted:
        for row in pool:
            company_key = (row["company_id"], row["metric"])
            quarter_key = (row["company_id"], row["metric"], row["fiscal_year"], row["fiscal_quarter"])
            metric_ok = not metric or row["metric"] == metric
            if row["rejection_category"] == category and metric_ok and company_key not in seen_companies and quarter_key not in seen_quarters:
                selected.append(row)
                seen_companies.add(company_key)
                seen_quarters.add(quarter_key)
                break
        if len(selected) >= sample_size:
            return selected
    for row in pool:
        if sum(1 for item in selected if item["metric"] == "ebit") >= 7:
            break
        company_key = (row["company_id"], row["metric"])
        quarter_key = (row["company_id"], row["metric"], row["fiscal_year"], row["fiscal_quarter"])
        if row["metric"] == "ebit" and company_key not in seen_companies and quarter_key not in seen_quarters:
            selected.append(row)
            seen_companies.add(company_key)
            seen_quarters.add(quarter_key)
    for row in pool:
        company_key = (row["company_id"], row["metric"])
        quarter_key = (row["company_id"], row["metric"], row["fiscal_year"], row["fiscal_quarter"])
        if company_key not in seen_companies and quarter_key not in seen_quarters:
            selected.append(row)
            seen_companies.add(company_key)
            seen_quarters.add(quarter_key)
        if len(selected) >= sample_size:
            break
    return selected


def candidate_value(row: dict[str, Any], metric: str) -> dict[str, Any]:
    comps = row["components"]
    if metric == "ebit" and "PRETAX" in comps:
        interest = phase4c2d.first_component(comps, phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES)
        if interest:
            return {"formula": "PRETAX_PLUS_INTEREST", "value": comps["PRETAX"]["value"] + interest["value"]}
    if metric == "ebitda" and row.get("ebit") is not None:
        da = phase4c2d.first_component(comps, phase4c2d.DA_ROLES)
        if da:
            return {"formula": "CANONICAL_EBIT_PLUS_DA", "value": float(row["ebit"]) + da["value"]}
        dep = phase4c2d.first_component(comps, phase4c2d.DEP_ROLES)
        amort = phase4c2d.first_component(comps, phase4c2d.AMORT_ROLES)
        if dep and amort:
            return {"formula": "CANONICAL_EBIT_PLUS_DEP_AMORT", "value": float(row["ebit"]) + dep["value"] + amort["value"]}
    return {"formula": "", "value": ""}


def review_case(case: dict[str, Any], mapped: list[dict[str, Any]], component_facts: list[dict[str, Any]], ebit_candidates: list[dict[str, Any]], da_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    history = historical_fit(case, ebit_candidates if case["metric"] == "ebit" else da_candidates)
    facts = facts_for_case(case, component_facts)
    gate = exact_gate(case, history, facts)
    disposition = disposition_for(case, history, facts, gate)
    return {
        **case,
        "selection_reason": selection_reason(case),
        "potentially_recoverable_before_review": int(case["calculated_value"] != ""),
        "learning_value": learning_value(case),
        "historical_fit_observations": len(history),
        "historical_fit_within_1pct": sum(int(row.get("within_1_pct", 0)) for row in history),
        "historical_fit_summary": fit_summary(history),
        "component_fact_count": len(facts),
        "exact_rejection_gate": gate,
        "disposition": disposition,
        "review_reasoning": review_reasoning(case, history, facts, disposition),
    }


def historical_fit(case: dict[str, Any], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for row in candidates if row["company_id"] == case["company_id"]]
    if case["metric"] == "ebitda":
        rows = [row for row in rows if row["formula_id"] in {"DA_COMBINED", "DEP_PLUS_AMORT"}]
    return sorted(rows, key=lambda row: (int(row["fiscal_year"]), row["fiscal_quarter"]))[-8:]


def facts_for_case(case: dict[str, Any], facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    roles = {"PRETAX"} | phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES if case["metric"] == "ebit" else phase4c2d.DA_ROLES | phase4c2d.DEP_ROLES | phase4c2d.AMORT_ROLES
    return [
        row for row in facts
        if row["company_id"] == case["company_id"]
        and int(row["fiscal_year"] or 0) == int(case["fiscal_year"])
        and row["fiscal_period"] in {case["fiscal_quarter"], "FY"}
        and row["semantic_role"] in roles
    ][:25]


def exact_gate(case: dict[str, Any], history: list[dict[str, Any]], facts: list[dict[str, Any]]) -> str:
    if not facts:
        return "COMPONENT_FACTS_MISSING_OR_NOT_QUARTERIZABLE"
    if case["fiscal_quarter"] == "Q4":
        return "Q4_SEPARATE_APPROVAL_NOT_MET"
    if len(history) < 4:
        return "TOO_FEW_TARGETS"
    if history and sum(int(row.get("within_1_pct", 0)) for row in history) == len(history):
        return "NUMERICALLY_CLOSE_BUT_NOT_APPROVED"
    return case["rejection_category"]


def disposition_for(case: dict[str, Any], history: list[dict[str, Any]], facts: list[dict[str, Any]], gate: str) -> str:
    if not facts:
        return "REJECTION_CORRECT"
    if gate == "NUMERICALLY_CLOSE_BUT_NOT_APPROVED":
        return "REJECTION_TOO_STRICT"
    if gate == "TOO_FEW_TARGETS" and case["calculated_value"] != "":
        return "NEEDS_MORE_EVIDENCE"
    if "Q4" in gate:
        return "REJECTION_CORRECT"
    if case["rejection_category"] in {"MULTIPLE_INTEREST_CANDIDATES", "COMBINED_DA_VS_DEP_AMORT_CONFLICT"}:
        return "NEEDS_MORE_EVIDENCE"
    return "REJECTION_CORRECT"


def selection_reason(case: dict[str, Any]) -> str:
    return f"Selected as {case['rejection_category']} with diagnostic priority {case['diagnostic_priority']}."


def learning_value(case: dict[str, Any]) -> str:
    return "Tests whether a bounded final refinement is warranted for this rejection pattern."


def fit_summary(history: list[dict[str, Any]]) -> str:
    if not history:
        return "no local historical target fit available"
    within = sum(int(row.get("within_1_pct", 0)) for row in history)
    return f"{within}/{len(history)} historical candidates within 1%"


def review_reasoning(case: dict[str, Any], history: list[dict[str, Any]], facts: list[dict[str, Any]], disposition: str) -> str:
    if disposition == "REJECTION_TOO_STRICT":
        return "Historical same-company candidate fit is perfect locally, but the current gate did not approve the missing target."
    if disposition == "NEEDS_MORE_EVIDENCE":
        return "SEC components exist and candidate is calculable, but local historical evidence is too thin or ambiguous for automatic promotion."
    if not facts:
        return "The selected quarter lacks usable local SEC component facts for the target derivation."
    return "The rejection is consistent with current safety policy for this evidence chain."


def pattern_frequency_scan(pool: list[dict[str, Any]], reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dispositions_by_category = defaultdict(Counter)
    for row in reviews:
        dispositions_by_category[row["rejection_category"]][row["disposition"]] += 1
    out = []
    for category, rows in group_by(pool, "rejection_category").items():
        review_dispositions = dispositions_by_category.get(category, Counter())
        finding = classify_pattern(category, review_dispositions)
        out.append({
            "pattern": category,
            "finding_class": finding,
            "affected_rows": len(rows),
            "affected_companies": len({row["company_id"] for row in rows}),
            "ebit_rows": sum(1 for row in rows if row["metric"] == "ebit"),
            "ebitda_rows": sum(1 for row in rows if row["metric"] == "ebitda"),
            "q4_rows": sum(1 for row in rows if row["fiscal_quarter"] == "Q4"),
            "sample_dispositions": json.dumps(dict(review_dispositions), sort_keys=True),
        })
    return sorted(out, key=lambda row: row["affected_rows"], reverse=True)


def classify_pattern(category: str, dispositions: Counter[str]) -> str:
    if dispositions["REJECTION_TOO_STRICT"]:
        return "SYSTEMATIC_OVERSTRICT_GATE"
    if dispositions["NEEDS_MORE_EVIDENCE"]:
        if category in {"MULTIPLE_INTEREST_CANDIDATES", "COMBINED_DA_VS_DEP_AMORT_CONFLICT"}:
            return "CONCEPT_REGISTRY_GAP"
        return "JUSTIFIED_REJECTION"
    return "JUSTIFIED_REJECTION"


def decide_outcome(reviews: list[dict[str, Any]], patterns: list[dict[str, Any]]) -> str:
    material = [row for row in patterns if row["finding_class"] in {"SYSTEMATIC_OVERSTRICT_GATE", "CONCEPT_REGISTRY_GAP"} and int(row["affected_rows"]) >= 25]
    if material:
        return OUTCOME_REFINEMENT
    if any(row["disposition"] == "IMPLEMENTATION_ERROR" for row in reviews):
        return OUTCOME_MAJOR
    return OUTCOME_NO_ISSUE


def summarize_patterns(patterns: list[dict[str, Any]]) -> dict[str, Any]:
    return {row["pattern"]: {"finding_class": row["finding_class"], "affected_rows": row["affected_rows"], "affected_companies": row["affected_companies"]} for row in patterns}


def full_population_impact(patterns: list[dict[str, Any]]) -> dict[str, Any]:
    actionable = [row for row in patterns if row["finding_class"] in {"SYSTEMATIC_OVERSTRICT_GATE", "CONCEPT_REGISTRY_GAP"}]
    return {
        "rows_affected_by_confirmed_systematic_issues": sum(int(row["affected_rows"]) for row in actionable),
        "companies_affected": sum(int(row["affected_companies"]) for row in actionable),
        "ebit_rows": sum(int(row["ebit_rows"]) for row in actionable),
        "ebitda_rows": sum(int(row["ebitda_rows"]) for row in actionable),
        "q4_rows": sum(int(row["q4_rows"]) for row in actionable),
        "estimated_additional_safe_recovery_if_fixed": 0,
    }


def group_by(rows: list[dict[str, Any]], key: str) -> dict[Any, list[dict[str, Any]]]:
    out = defaultdict(list)
    for row in rows:
        out[row[key]].append(row)
    return dict(out)


def write_artifacts(root: Path, summary: dict[str, Any], pool: list[dict[str, Any]], selected: list[dict[str, Any]], reviews: list[dict[str, Any]], component_facts: list[dict[str, Any]], pattern_counts: list[dict[str, Any]]) -> None:
    write_csv(root / "rejected_case_pool.csv", pool)
    write_csv(root / "selected_15_cases.csv", selected)
    write_text(root / "selection_rationale.md", selection_rationale_md(reviews))
    for i, review in enumerate(reviews, start=1):
        write_text(root / f"case_review_{i:02d}.md", case_review_md(review))
    write_csv(root / "selected_case_summary.csv", reviews)
    write_csv(root / "case_component_facts.csv", component_fact_artifact_rows(reviews, component_facts))
    write_csv(root / "case_formula_comparisons.csv", formula_comparison_rows(reviews))
    write_csv(root / "case_historical_fit.csv", historical_fit_artifact_rows(reviews))
    write_csv(root / "case_rejection_gate_trace.csv", gate_trace_rows(reviews))
    write_text(root / "cross_case_pattern_analysis.md", pattern_analysis_md(pattern_counts))
    write_csv(root / "systematic_pattern_counts.csv", pattern_counts)
    write_csv(root / "full_residual_pattern_scan.csv", pattern_counts)
    write_csv(root / "bounded_refinement_candidates.csv", [row for row in pattern_counts if row["finding_class"] in {"SYSTEMATIC_OVERSTRICT_GATE", "CONCEPT_REGISTRY_GAP"}])
    write_csv(root / "justified_rejection_examples.csv", [row for row in reviews if row["disposition"] == "REJECTION_CORRECT"])
    write_json(root / "phase4c3b_summary.json", summary)
    write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")


def selection_rationale_md(reviews: list[dict[str, Any]]) -> str:
    lines = ["# Selection Rationale", ""]
    for i, row in enumerate(reviews, start=1):
        lines.append(f"{i}. {row['ticker']} FY{row['fiscal_year']} {row['fiscal_quarter']} {row['metric']}: {row['selection_reason']}")
    return "\n".join(lines) + "\n"


def case_review_md(row: dict[str, Any]) -> str:
    return f"""# {row['ticker']} FY{row['fiscal_year']} {row['fiscal_quarter']} {row['metric'].upper()}

Period end: `{row['period_end_date']}`

Rejection category: `{row['rejection_category']}`

Candidate: `{row['formula_component_candidate']}` = `{row['calculated_value']}`

Historical fit: {row['historical_fit_summary']}

Exact rejection gate: `{row['exact_rejection_gate']}`

Human-review disposition: `{row['disposition']}`

Reasoning: {row['review_reasoning']}
"""


def component_fact_artifact_rows(reviews: list[dict[str, Any]], facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    keys = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["metric"]) for row in reviews}
    for review in reviews:
        for fact in facts_for_case(review, facts):
            out.append({**{k: review[k] for k in ("ticker", "fiscal_year", "fiscal_quarter", "metric")}, **{k: fact.get(k, "") for k in ("semantic_role", "concept_name", "concept_label", "value", "unit", "start_date", "end_date", "fiscal_period", "form", "accession", "filed_date", "dimensions_json")}})
    return out


def formula_comparison_rows(reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: row[k] for k in ("ticker", "fiscal_year", "fiscal_quarter", "metric", "formula_component_candidate", "calculated_value", "historical_fit_summary")} for row in reviews]


def historical_fit_artifact_rows(reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: row[k] for k in ("ticker", "fiscal_year", "fiscal_quarter", "metric", "historical_fit_observations", "historical_fit_within_1pct", "historical_fit_summary")} for row in reviews]


def gate_trace_rows(reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: row[k] for k in ("ticker", "fiscal_year", "fiscal_quarter", "metric", "rejection_category", "exact_rejection_gate", "disposition", "review_reasoning")} for row in reviews]


def pattern_analysis_md(patterns: list[dict[str, Any]]) -> str:
    lines = ["# Cross-Case Pattern Analysis", ""]
    for row in patterns[:20]:
        lines.append(f"- {row['pattern']}: {row['finding_class']} ({row['affected_rows']} rows, {row['affected_companies']} companies)")
    return "\n".join(lines) + "\n"


def write_doc(path: Path, summary: dict[str, Any], reviews: list[dict[str, Any]], patterns: list[dict[str, Any]]) -> None:
    lines = [
        "# Fundamentals V3 Phase 4C-3B Rejected Case Review",
        "",
        f"Outcome: `{summary['outcome']}`",
        "",
        "The review opened 15 representative rejected EBIT/EBITDA cases after Phase 4C-3 production apply. No canonical financial or metadata writes were performed.",
        "",
        "| Ticker | FY/FQ | Metric | Rejection | Candidate | Hist fit | Disposition |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in reviews:
        lines.append(f"| {row['ticker']} | FY{row['fiscal_year']} {row['fiscal_quarter']} | {row['metric']} | {row['rejection_category']} | {row['formula_component_candidate']} `{row['calculated_value']}` | {row['historical_fit_summary']} | {row['disposition']} |")
    lines.extend(["", "## Repeated Patterns", ""])
    for row in patterns[:10]:
        lines.append(f"- `{row['pattern']}`: `{row['finding_class']}`, rows `{row['affected_rows']}`, companies `{row['affected_companies']}`")
    lines.extend(["", f"Next: `{summary['recommended_next_step']}`", ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 4C-3B"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 4C-3B

Outcome: `{summary['outcome']}`

Status: `REJECTED_CASE_REVIEW_COMPLETE`

Canonical financial writes: `0`

Metadata writes: `0`

Selected cases: `{summary['selection']['selected_cases']}`

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
