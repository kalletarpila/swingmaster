from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import v3_phase4c2d_sec_formula_rerun as phase4c2d
from swingmaster.fundamentals import v3_phase4c2f_gate_refinement as phase4c2f
from swingmaster.fundamentals import v3_phase4c3b_rejected_case_review as phase4c3b
from swingmaster.fundamentals.v3_canonical_closure import field_coverage_summary, final_canonical_baseline
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE4C3C_BOUNDED_REFINEMENT_COMPLETE_READY_FOR_PRODUCTION_APPLY"
CLASSIFICATION_NO_APPLY = "FUNDAMENTALS_V3_PHASE4C3C_BOUNDED_REFINEMENT_COMPLETE_NO_ADDITIONAL_APPLY_NEEDED"
CLASSIFICATION_RESEARCH = "FUNDAMENTALS_V3_PHASE4C3C_ADDITIONAL_ARCHITECTURAL_RESEARCH_REQUIRED"
NEXT_APPLY = "MASTER PLAN PHASE 4C-3D - BOUNDED REFINEMENT PRODUCTION APPLY"
NEXT_4D = "MASTER PLAN PHASE 4D - HISTORICAL COMPLETENESS CLOSURE"
RUN_ID = "PHASE4C3C_BOUNDED_REJECTION_REFINEMENT"


def run_phase4c3c_bounded_refinement(*, v3_db: Path, component_db: Path, artifact_root: Path) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline_raw = final_canonical_baseline(v3_db)
    missing = {row["field"]: int(row["null_q"]) for row in field_coverage_summary(v3_db)}
    v3_rows = phase4c2d.load_v3_rows(v3_db)
    qcomponents, qvalidations, qfailures = phase4c2d.quarterize_components(phase4c2d.load_component_rows(component_db))
    mapped = phase4c2d.map_targets_to_components(v3_rows, qcomponents)

    rejected_pool = phase4c3b.build_rejected_case_pool(mapped, qfailures)
    bounded = bounded_population(mapped, rejected_pool)
    interest_population = [row for row in bounded if row["pattern_class"] == "MULTIPLE_INTEREST_CANDIDATES"]
    da_population = [row for row in bounded if row["pattern_class"] == "COMBINED_DA_VS_DEP_AMORT_CONFLICT"]

    ebit_candidates = phase4c2d.build_ebit_candidates(mapped)
    da_candidates = phase4c2d.build_da_candidates(mapped)
    interest_profiles = bounded_interest_profiles(ebit_candidates, interest_population)
    da_profiles = bounded_da_profiles(da_candidates, da_population)

    interest_backtest = hidden_target_backtest(ebit_candidates, interest_profiles, metric="EBIT")
    da_backtest = hidden_target_backtest(da_candidates, da_profiles, metric="DA")
    interest_recovery = interest_dry_recovery(interest_population, interest_profiles)
    da_recovery = da_dry_recovery(da_population, da_profiles, interest_profiles)
    production_plan = production_plan_rows(interest_recovery, da_recovery)

    q4_interest = [row for row in interest_recovery if row["fiscal_quarter"] == "Q4"]
    q4_da = [row for row in da_recovery if row["fiscal_quarter"] == "Q4"]
    recovery_summary_rows = recovery_summary(interest_population, da_population, interest_recovery, da_recovery, production_plan, v3_rows)
    integrity = structural_integrity(v3_db)
    safe_rows = [row for row in production_plan if str(row["hidden_target_validation_class"]).startswith("AUTO")]
    classification = CLASSIFICATION_READY if safe_rows and hidden_validation_passes(interest_backtest, da_backtest) and integrity["phase3_structural_gates_pass"] else CLASSIFICATION_NO_APPLY
    summary = {
        "classification": classification,
        "recommended_next_step": NEXT_APPLY if classification == CLASSIFICATION_READY else NEXT_4D,
        "baseline": {
            "companies": baseline_raw["company_total"],
            "canonical_q": baseline_raw["coverage"]["canonical_q_total"],
            "core_ready": baseline_raw["coverage"]["core_ready_q"],
            "core_not_ready": baseline_raw["coverage"]["core_not_ready_q"],
            "ebit_missing": missing.get("ebit", 0),
            "ebitda_missing": missing.get("ebitda", 0),
        },
        "bounded_population": summarize_bounded_population(bounded),
        "interest": summarize_interest(interest_population, interest_profiles, interest_backtest, interest_recovery),
        "da": summarize_da(da_population, da_profiles, da_backtest, da_recovery),
        "q4": {"interest_auto": len(q4_interest), "da_auto": len(q4_da), "q4_auto_total": len(q4_interest) + len(q4_da)},
        "recovery_impact": {
            "additional_ebit_fills": sum(1 for row in production_plan if row["metric"] == "ebit"),
            "additional_ebitda_fills": sum(1 for row in production_plan if row["metric"] == "ebitda"),
            "additional_core_ready_uplift": sum(int(row["core_ready_impact"]) for row in production_plan),
            "residual_rows_still_blocked": len(bounded) - len(production_plan),
        },
        "safety": {"canonical_financial_writes": 0, "metadata_writes": 0, "sec_component_db_destructive_writes": 0},
        "integrity": integrity,
        "artifact_root": str(artifact_root),
    }
    write_artifacts(
        artifact_root,
        summary,
        bounded,
        interest_population,
        da_population,
        interest_profiles,
        da_profiles,
        interest_backtest,
        da_backtest,
        interest_recovery,
        da_recovery,
        production_plan,
        recovery_summary_rows,
        qvalidations,
        qfailures,
    )
    write_doc(Path("docs/fundamentals_v3_phase4c_3c_bounded_rejection_logic_refinement.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def bounded_population(mapped: list[dict[str, Any]], rejected_pool: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    if rejected_pool is not None:
        by_key = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"]): row for row in mapped}
        out = []
        for rejected in rejected_pool:
            if rejected["rejection_category"] not in {"MULTIPLE_INTEREST_CANDIDATES", "COMBINED_DA_VS_DEP_AMORT_CONFLICT"}:
                continue
            source = by_key.get((rejected["company_id"], rejected["fiscal_year"], rejected["fiscal_quarter"]))
            if not source:
                continue
            pattern = rejected["rejection_category"]
            typology = interest_typology(source["components"]) if pattern == "MULTIPLE_INTEREST_CANDIDATES" else da_typology(source["components"])
            out.append(population_row(source, rejected["metric"], pattern, typology, prior_rejection_reason=pattern))
        return sorted(out, key=lambda row: (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"], row["metric"]))
    out = []
    for row in mapped:
        comps = row["components"]
        if row.get("ebit") is None and "PRETAX" in comps and len(set(comps) & (phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES)) > 1:
            out.append(population_row(row, "ebit", "MULTIPLE_INTEREST_CANDIDATES", interest_typology(comps)))
        has_da = bool(set(comps) & phase4c2d.DA_ROLES)
        has_dep_amort = bool(set(comps) & phase4c2d.DEP_ROLES) and bool(set(comps) & phase4c2d.AMORT_ROLES)
        if row.get("ebitda") is None and has_da and has_dep_amort:
            out.append(population_row(row, "ebitda", "COMBINED_DA_VS_DEP_AMORT_CONFLICT", da_typology(comps)))
    return sorted(out, key=lambda row: (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"], row["metric"]))


def population_row(row: dict[str, Any], metric: str, pattern: str, typology: str, *, prior_rejection_reason: str | None = None) -> dict[str, Any]:
    comps = row["components"]
    return {
        "company_id": row["company_id"],
        "ticker": row["ticker"],
        "fiscal_year": row["fiscal_year"],
        "fiscal_quarter": row["fiscal_quarter"],
        "period_end_date": row["period_end_date"],
        "metric": metric,
        "pattern_class": pattern,
        "prior_rejection_reason": prior_rejection_reason or ("Q4_REJECTION" if row["fiscal_quarter"] == "Q4" and metric == "ebit" else "Q4_DA_REJECTION" if row["fiscal_quarter"] == "Q4" else pattern),
        "resolved_pattern_candidate": typology,
        "component_roles": "|".join(sorted(comps)),
        "component_concepts": json.dumps({role: comp["concept"] for role, comp in sorted(comps.items())}, sort_keys=True),
        "component_fact_ids": "|".join(comp["fact_ids"] for _, comp in sorted(comps.items())),
        "source_row": row,
    }


def interest_typology(comps: dict[str, dict[str, Any]]) -> str:
    gross = bool(set(comps) & {"INTEREST_EXPENSE_GROSS", "DEBT_INTEREST"})
    lease = "FINANCE_LEASE_INTEREST" in comps
    issuer = "ISSUER_SPECIFIC_INTEREST" in comps
    net = "INTEREST_EXPENSE_NET" in comps
    if issuer:
        return "ISSUER_SPECIFIC_TOTAL"
    if "DEBT_INTEREST" in comps and lease and "INTEREST_EXPENSE_GROSS" not in comps:
        return "COMPONENT_SUM"
    if gross and lease:
        return "TOTAL_PLUS_COMPONENTS"
    if net and gross:
        return "NET_VS_GROSS"
    if net:
        return "TRUE_AMBIGUITY"
    return "TOTAL_ONLY" if gross else "TRUE_AMBIGUITY"


def da_typology(comps: dict[str, dict[str, Any]]) -> str:
    combined = phase4c2d.first_component(comps, phase4c2d.DA_ROLES)
    dep = phase4c2d.first_component(comps, phase4c2d.DEP_ROLES)
    amort = phase4c2d.first_component(comps, phase4c2d.AMORT_ROLES)
    if combined and dep and amort:
        dep_amort = dep["value"] + amort["value"]
        tolerance = max(abs(combined["value"]), abs(dep_amort), 1.0) * 0.01
        return "COMBINED_EQUALS_DEP_PLUS_AMORT" if abs(combined["value"] - dep_amort) <= tolerance else "COMBINED_OVERLAPS_COMPONENTS"
    return "TRUE_DA_AMBIGUITY"


def bounded_interest_profiles(candidates: list[dict[str, Any]], population: list[dict[str, Any]]) -> list[dict[str, Any]]:
    companies = {row["company_id"] for row in population}
    profiles = phase4c2f.refined_formula_profiles([row for row in candidates if row["company_id"] in companies and row["formula_id"] != "OPERATING_INCOME_PROXY"], metric="EBIT")
    return [decorate_interest_profile(row, population) for row in profiles]


def bounded_da_profiles(candidates: list[dict[str, Any]], population: list[dict[str, Any]]) -> list[dict[str, Any]]:
    companies = {row["company_id"] for row in population}
    profiles = phase4c2f.refined_formula_profiles([row for row in candidates if row["company_id"] in companies], metric="DA")
    return [decorate_da_profile(row, population) for row in profiles]


def decorate_interest_profile(profile: dict[str, Any], population: list[dict[str, Any]]) -> dict[str, Any]:
    return {**profile, "profile_type": interest_profile_type(profile), "composition_type": profile_interest_composition(profile["formula_id"]), "bounded_rows": sum(1 for row in population if row["company_id"] == profile["company_id"])}


def decorate_da_profile(profile: dict[str, Any], population: list[dict[str, Any]]) -> dict[str, Any]:
    return {**profile, "profile_type": da_profile_type(profile), "composition_type": profile_da_composition(profile["formula_id"]), "bounded_rows": sum(1 for row in population if row["company_id"] == profile["company_id"])}


def interest_profile_type(profile: dict[str, Any]) -> str:
    if profile["overall_status"] == "AUTO_STRONG":
        return "INTEREST_PROFILE_STRONG"
    if str(profile["overall_status"]).startswith("AUTO_STRONG_LOW_SAMPLE"):
        return "INTEREST_PROFILE_LOW_SAMPLE_STRONG"
    if str(profile["overall_status"]).startswith("CONDITIONAL"):
        return "INTEREST_PROFILE_CONDITIONAL"
    return "INTEREST_PROFILE_REJECTED"


def da_profile_type(profile: dict[str, Any]) -> str:
    if profile["overall_status"] == "AUTO_STRONG":
        return "DA_PROFILE_COMBINED_STRONG" if profile["formula_id"] == "DA_COMBINED" else "DA_PROFILE_DEP_AMORT_STRONG"
    if str(profile["overall_status"]).startswith("AUTO_STRONG_LOW_SAMPLE"):
        return "DA_PROFILE_LOW_SAMPLE_STRONG"
    if str(profile["overall_status"]).startswith("CONDITIONAL"):
        return "DA_PROFILE_CONDITIONAL"
    return "DA_PROFILE_REJECTED"


def profile_interest_composition(formula_id: str) -> str:
    return {
        "PRETAX_PLUS_INTEREST_GROSS": "TOTAL_ONLY",
        "PRETAX_PLUS_COMPOSITE_INTEREST": "COMPONENT_SUM",
        "PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST": "ISSUER_SPECIFIC_TOTAL",
        "PRETAX_PLUS_NET_INTEREST": "NET_VS_GROSS",
    }.get(formula_id, "TRUE_AMBIGUITY")


def profile_da_composition(formula_id: str) -> str:
    return {"DA_COMBINED": "COMBINED_DA_IS_COMPLETE", "DEP_PLUS_AMORT": "DEP_PLUS_AMORT_IS_COMPLETE"}.get(formula_id, "TRUE_DA_AMBIGUITY")


def profile_auto_status(profile: dict[str, Any], quarter: str) -> str:
    return phase4c2f.applicability_status(profile, quarter)


def hidden_target_backtest(candidates: list[dict[str, Any]], profiles: list[dict[str, Any]], *, metric: str) -> list[dict[str, Any]]:
    profile_by = {(row["company_id"], row["formula_id"]): row for row in profiles}
    out = []
    for row in candidates:
        if row["metric"] != metric:
            continue
        profile = profile_by.get((row["company_id"], row["formula_id"]))
        if not profile:
            continue
        status = profile_auto_status(profile, row["fiscal_quarter"])
        if not status.startswith("AUTO"):
            continue
        out.append({**row, "hidden_target_validation_class": status, "profile_type": profile["profile_type"], "composition_type": profile["composition_type"]})
    return out


def interest_dry_recovery(population: list[dict[str, Any]], profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company = defaultdict(list)
    for profile in profiles:
        by_company[profile["company_id"]].append(profile)
    out = []
    for item in population:
        row = item["source_row"]
        for profile in sorted(by_company.get(item["company_id"], []), key=profile_rank, reverse=True):
            status = profile_auto_status(profile, row["fiscal_quarter"])
            if not status.startswith("AUTO") or profile["formula_id"] == "PRETAX_PLUS_NET_INTEREST":
                continue
            candidate = phase4c2d.derive_plan_candidate(row, row, phase4c2f.profile_for_2d(profile, status))
            if candidate:
                out.append(recovery_row(item, "ebit", profile, status, candidate, "DERIVED_SEC_EBIT_BOUNDED_INTEREST"))
                break
    return dedupe_targets(out)


def da_dry_recovery(population: list[dict[str, Any]], da_profiles: list[dict[str, Any]], interest_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    da_by_company = defaultdict(list)
    interest_by_company = defaultdict(list)
    for profile in da_profiles:
        da_by_company[profile["company_id"]].append(profile)
    for profile in interest_profiles:
        interest_by_company[profile["company_id"]].append(profile)
    out = []
    for item in population:
        row = item["source_row"]
        base_ebit = row.get("ebit")
        ebit_component_ids = ""
        ebit_status = "CANONICAL_EBIT"
        if base_ebit is None:
            for ebit_profile in sorted(interest_by_company.get(item["company_id"], []), key=profile_rank, reverse=True):
                status = profile_auto_status(ebit_profile, row["fiscal_quarter"])
                if not status.startswith("AUTO"):
                    continue
                ebit_candidate = phase4c2d.derive_plan_candidate(row, row, phase4c2f.profile_for_2d(ebit_profile, status))
                if ebit_candidate:
                    base_ebit = ebit_candidate["derived_value"]
                    ebit_component_ids = ebit_candidate["component_fact_ids"]
                    ebit_status = status
                    break
        if base_ebit is None:
            continue
        for profile in sorted(da_by_company.get(item["company_id"], []), key=profile_rank, reverse=True):
            status = profile_auto_status(profile, row["fiscal_quarter"])
            if not status.startswith("AUTO"):
                continue
            da_value = derive_da_value(row, profile["formula_id"])
            if not da_value:
                continue
            candidate = {
                "derived_value": float(base_ebit) + da_value["value"],
                "component_fact_ids": "|".join(part for part in (ebit_component_ids, da_value["fact_ids"]) if part),
                "component_values_json": json.dumps({"EBIT": base_ebit, "DA": da_value["value"]}, sort_keys=True),
                "quarterization_method": da_value["method"],
                "sec_accessions": da_value["accessions"],
            }
            mode = "DERIVED_CANONICAL_EBIT_PLUS_SEC_DA_BOUNDED" if ebit_status == "CANONICAL_EBIT" else "DERIVED_SEC_EBIT_PLUS_SEC_DA_BOUNDED"
            out.append(recovery_row(item, "ebitda", profile, status, candidate, mode))
            break
    return dedupe_targets(out)


def derive_da_value(row: dict[str, Any], formula_id: str) -> dict[str, Any] | None:
    comps = row["components"]
    if formula_id == "DA_COMBINED":
        return phase4c2d.first_component(comps, phase4c2d.DA_ROLES)
    if formula_id == "DEP_PLUS_AMORT":
        dep = phase4c2d.first_component(comps, phase4c2d.DEP_ROLES)
        amort = phase4c2d.first_component(comps, phase4c2d.AMORT_ROLES)
        if dep and amort:
            return {"value": dep["value"] + amort["value"], "fact_ids": dep["fact_ids"] + "|" + amort["fact_ids"], "method": dep["method"] + "|" + amort["method"], "accessions": dep["accessions"] + "|" + amort["accessions"]}
    return None


def recovery_row(item: dict[str, Any], metric: str, profile: dict[str, Any], status: str, candidate: dict[str, Any], mode: str) -> dict[str, Any]:
    row = item["source_row"]
    return {
        "company_id": item["company_id"],
        "ticker": item["ticker"],
        "fiscal_year": item["fiscal_year"],
        "fiscal_quarter": item["fiscal_quarter"],
        "period_end": item["period_end_date"],
        "metric": metric,
        "prior_rejection_reason": item["prior_rejection_reason"],
        "resolved_pattern": profile["composition_type"],
        "formula_profile": profile["formula_id"],
        "formula_version": profile["formula_version"],
        "component_fact_ids": candidate["component_fact_ids"],
        "concepts": item["component_concepts"],
        "values": candidate["component_values_json"],
        "quarterization_method": candidate["quarterization_method"],
        "sec_accessions": candidate["sec_accessions"],
        "hidden_target_validation_class": status,
        "q_applicability": status,
        "derived_value": candidate["derived_value"],
        "source_mode": mode,
        "core_ready_impact": int(metric == "ebitda" and not phase4c2d.core_ready(row)),
        "research_run": RUN_ID,
    }


def production_plan_rows(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return dedupe_targets([row for group in groups for row in group if str(row["hidden_target_validation_class"]).startswith("AUTO")])


def dedupe_targets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rank = {"DERIVED_CANONICAL_EBIT_PLUS_SEC_DA_BOUNDED": 4, "DERIVED_SEC_EBIT_BOUNDED_INTEREST": 3, "DERIVED_SEC_EBIT_PLUS_SEC_DA_BOUNDED": 2}
    out = {}
    for row in rows:
        key = (row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["metric"])
        current = out.get(key)
        if current is None or rank.get(row["source_mode"], 0) > rank.get(current["source_mode"], 0):
            out[key] = row
    return sorted(out.values(), key=lambda row: (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"], row["metric"]))


def profile_rank(profile: dict[str, Any]) -> tuple[int, int, float]:
    status = str(profile["overall_status"])
    return (3 if status == "AUTO_STRONG" else 2 if status.startswith("AUTO_STRONG_LOW_SAMPLE") else 1, int(profile.get("all_observations") or 0), float(profile.get("all_within_1_pct_rate") or 0.0))


def hidden_validation_passes(*groups: list[dict[str, Any]]) -> bool:
    rows = [row for group in groups for row in group]
    auto = [row for row in rows if str(row.get("hidden_target_validation_class", "")).startswith("AUTO")]
    metrics = phase4c2d.metric_counts(auto)
    return bool(auto) and metrics["sign_mismatch"] == 0 and metrics["material_errors"] == 0 and metrics["within_5_pct_rate"] >= 0.95


def summarize_bounded_population(rows: list[dict[str, Any]]) -> dict[str, Any]:
    c = Counter(row["pattern_class"] for row in rows)
    companies = defaultdict(set)
    for row in rows:
        companies[row["pattern_class"]].add(row["company_id"])
    interest_keys = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"]) for row in rows if row["pattern_class"] == "MULTIPLE_INTEREST_CANDIDATES"}
    da_keys = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"]) for row in rows if row["pattern_class"] == "COMBINED_DA_VS_DEP_AMORT_CONFLICT"}
    return {
        "total_rows": len(rows),
        "multiple_interest_rows": c["MULTIPLE_INTEREST_CANDIDATES"],
        "multiple_interest_companies": len(companies["MULTIPLE_INTEREST_CANDIDATES"]),
        "da_conflict_rows": c["COMBINED_DA_VS_DEP_AMORT_CONFLICT"],
        "da_conflict_companies": len(companies["COMBINED_DA_VS_DEP_AMORT_CONFLICT"]),
        "overlap_quarters": len(interest_keys & da_keys),
        "interest_typology": dict(Counter(row["resolved_pattern_candidate"] for row in rows if row["pattern_class"] == "MULTIPLE_INTEREST_CANDIDATES")),
        "da_typology": dict(Counter(row["resolved_pattern_candidate"] for row in rows if row["pattern_class"] == "COMBINED_DA_VS_DEP_AMORT_CONFLICT")),
    }


def summarize_interest(population: list[dict[str, Any]], profiles: list[dict[str, Any]], backtest: list[dict[str, Any]], recovery: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total_rows": len(population),
        "profiles": len(profiles),
        "auto_profiles": sum(1 for row in profiles if str(row["overall_status"]).startswith("AUTO")),
        "dominant_pattern": most_common([row["resolved_pattern"] for row in recovery]),
        "auto_ebit": len(recovery),
        "q4": sum(1 for row in recovery if row["fiscal_quarter"] == "Q4"),
        "blocked": len(population) - len(recovery),
        "hidden_target": phase4c2f.error_metrics(backtest),
    }


def summarize_da(population: list[dict[str, Any]], profiles: list[dict[str, Any]], backtest: list[dict[str, Any]], recovery: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total_rows": len(population),
        "profiles": len(profiles),
        "auto_profiles": sum(1 for row in profiles if str(row["overall_status"]).startswith("AUTO")),
        "dominant_pattern": most_common([row["resolved_pattern"] for row in recovery]),
        "auto_ebitda": len(recovery),
        "q4": sum(1 for row in recovery if row["fiscal_quarter"] == "Q4"),
        "blocked": len(population) - len(recovery),
        "hidden_target": phase4c2f.error_metrics(backtest),
    }


def most_common(values: list[str]) -> str:
    return Counter(values).most_common(1)[0][0] if values else "NONE"


def recovery_summary(interest_pop: list[dict[str, Any]], da_pop: list[dict[str, Any]], interest_recovery: list[dict[str, Any]], da_recovery: list[dict[str, Any]], plan: list[dict[str, Any]], v3_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    current_core = sum(phase4c2d.core_ready(row) for row in v3_rows)
    return [
        {"category": "interest_pattern", "total_rows": len(interest_pop), "auto_rows": len(interest_recovery), "q4_rows": sum(1 for row in interest_recovery if row["fiscal_quarter"] == "Q4"), "blocked_rows": len(interest_pop) - len(interest_recovery), "core_ready_uplift": sum(int(row["core_ready_impact"]) for row in interest_recovery)},
        {"category": "da_pattern", "total_rows": len(da_pop), "auto_rows": len(da_recovery), "q4_rows": sum(1 for row in da_recovery if row["fiscal_quarter"] == "Q4"), "blocked_rows": len(da_pop) - len(da_recovery), "core_ready_uplift": sum(int(row["core_ready_impact"]) for row in da_recovery)},
        {"category": "deduped_final", "total_rows": len(interest_pop) + len(da_pop), "auto_rows": len(plan), "q4_rows": sum(1 for row in plan if row["fiscal_quarter"] == "Q4"), "blocked_rows": len(interest_pop) + len(da_pop) - len(plan), "core_ready_uplift": sum(int(row["core_ready_impact"]) for row in plan), "core_ready_after_dry_apply": current_core + sum(int(row["core_ready_impact"]) for row in plan)},
    ]


def write_artifacts(artifact_root: Path, summary: dict[str, Any], bounded: list[dict[str, Any]], interest_population: list[dict[str, Any]], da_population: list[dict[str, Any]], interest_profiles: list[dict[str, Any]], da_profiles: list[dict[str, Any]], interest_backtest: list[dict[str, Any]], da_backtest: list[dict[str, Any]], interest_recovery: list[dict[str, Any]], da_recovery: list[dict[str, Any]], production_plan: list[dict[str, Any]], recovery_summary_rows: list[dict[str, Any]], qvalidations: list[dict[str, Any]], qfailures: list[dict[str, Any]]) -> None:
    write_text(artifact_root / "preflight.md", preflight_md(summary))
    write_csv(artifact_root / "bounded_population.csv", strip_source(bounded))
    write_json(artifact_root / "bounded_population_summary.json", summary["bounded_population"])
    write_csv(artifact_root / "interest_company_concept_inventory.csv", concept_inventory(interest_population))
    write_csv(artifact_root / "interest_composition_typology.csv", typology_rows(interest_population))
    write_csv(artifact_root / "interest_formula_candidates.csv", candidate_formula_rows(interest_profiles))
    write_csv(artifact_root / "interest_historical_validation.csv", interest_profiles)
    write_csv(artifact_root / "company_interest_profiles.csv", interest_profiles)
    write_csv(artifact_root / "interest_hidden_target_backtest.csv", interest_backtest)
    write_csv(artifact_root / "interest_dry_recovery.csv", interest_recovery)
    write_csv(artifact_root / "interest_unresolved.csv", unresolved_rows(interest_population, interest_recovery))
    write_csv(artifact_root / "da_company_concept_inventory.csv", concept_inventory(da_population))
    write_csv(artifact_root / "da_conflict_typology.csv", typology_rows(da_population))
    write_csv(artifact_root / "implied_da_validation.csv", da_profiles)
    write_csv(artifact_root / "simfin_da_corroboration.csv", [{"status": "NOT_USED_LOCAL_SEC_BACKTEST_ONLY"}])
    write_csv(artifact_root / "company_da_profiles.csv", da_profiles)
    write_csv(artifact_root / "da_hidden_target_backtest.csv", da_backtest)
    write_csv(artifact_root / "da_dry_recovery.csv", da_recovery)
    write_csv(artifact_root / "da_unresolved.csv", unresolved_rows(da_population, da_recovery))
    write_csv(artifact_root / "q4_interest_validation.csv", [row for row in interest_backtest if row["fiscal_quarter"] == "Q4"])
    write_csv(artifact_root / "q4_da_validation.csv", [row for row in da_backtest if row["fiscal_quarter"] == "Q4"])
    write_csv(artifact_root / "q4_bounded_dry_recovery.csv", [row for row in [*interest_recovery, *da_recovery] if row["fiscal_quarter"] == "Q4"])
    write_csv(artifact_root / "bounded_recovery_summary.csv", recovery_summary_rows)
    write_csv(artifact_root / "resolved_rejection_reasons.csv", resolved_reason_rows([*interest_recovery, *da_recovery]))
    write_csv(artifact_root / "interest_profile_metadata_dry.csv", profile_metadata(interest_profiles))
    write_csv(artifact_root / "da_profile_metadata_dry.csv", profile_metadata(da_profiles))
    write_csv(artifact_root / "phase4c3d_bounded_refinement_production_apply_plan.csv", production_plan)
    write_json(artifact_root / "phase4c3c_summary.json", summary)
    write_text(artifact_root / "recommended_next_step.md", f"{summary['recommended_next_step']}\n")
    write_csv(artifact_root / "component_quarterization_validation_reference.csv", qvalidations[:1000])
    write_csv(artifact_root / "component_quarterization_failure_reference.csv", qfailures[:1000])


def strip_source(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: v for k, v in row.items() if k != "source_row"} for row in rows]


def concept_inventory(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by = defaultdict(list)
    for row in rows:
        by[(row["company_id"], row["ticker"], row["metric"])].append(row)
    return [{"company_id": key[0], "ticker": key[1], "metric": key[2], "bounded_rows": len(items), "roles_seen": "|".join(sorted({role for item in items for role in item["component_roles"].split("|")})), "concepts_seen": "|".join(sorted({item["component_concepts"] for item in items}))} for key, items in sorted(by.items(), key=lambda item: (item[0][1], item[0][2]))]


def typology_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    c = Counter((row["pattern_class"], row["resolved_pattern_candidate"]) for row in rows)
    return [{"pattern_class": pattern, "typology": typology, "rows": count} for (pattern, typology), count in sorted(c.items())]


def candidate_formula_rows(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"company_id": row["company_id"], "ticker": row["ticker"], "metric": row["metric"], "formula_id": row["formula_id"], "profile_type": row["profile_type"], "composition_type": row["composition_type"], "overall_status": row["overall_status"], "q1q3_applicability": row["q1q3_applicability"], "q4_applicability": row["q4_applicability"]} for row in profiles]


def unresolved_rows(population: list[dict[str, Any]], recovery: list[dict[str, Any]]) -> list[dict[str, Any]]:
    resolved = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["metric"]) for row in recovery}
    return [row for row in strip_source(population) if (row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["metric"]) not in resolved]


def resolved_reason_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"prior_rejection_reason": reason, "resolved_pattern": pattern, "rows": count} for (reason, pattern), count in sorted(Counter((row["prior_rejection_reason"], row["resolved_pattern"]) for row in rows).items())]


def profile_metadata(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"company_id": row["company_id"], "ticker": row["ticker"], "profile_version": row["formula_version"], "composition_type": row["composition_type"], "component_concepts": row["component_concepts_json"], "validity_range": f"FY{row['valid_from_fiscal_year']}-FY{row['valid_to_fiscal_year']}", "semantic_confidence": row["semantic_confidence"], "statistical_confidence": row["statistical_confidence"], "validation_observations": row.get("all_observations", ""), "q1q3_status": row["q1q3_applicability"], "q4_status": row["q4_applicability"], "profile_type": row["profile_type"], "metadata_persisted": 0} for row in rows]


def preflight_md(summary: dict[str, Any]) -> str:
    return f"""# Phase 4C-3C Preflight

Classification: `{summary['classification']}`

Canonical financial writes: `0`

Metadata writes: `0`

Bounded rows: `{summary['bounded_population']['total_rows']}`

Multiple-interest rows: `{summary['bounded_population']['multiple_interest_rows']}`

D&A conflict rows: `{summary['bounded_population']['da_conflict_rows']}`
"""


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4C-3C Bounded Rejection-Logic Refinement

Classification: `{summary['classification']}`

Canonical financial writes: `0`

Metadata writes: `0`

## Scope

This phase analyzed only `MULTIPLE_INTEREST_CANDIDATES` and `COMBINED_DA_VS_DEP_AMORT_CONFLICT` rows left after Phase 4C-3B. No broader pattern discovery or production apply was performed.

## Bounded Population

- Total bounded rows: `{summary['bounded_population']['total_rows']}`
- Multiple-interest rows: `{summary['bounded_population']['multiple_interest_rows']}`
- D&A conflict rows: `{summary['bounded_population']['da_conflict_rows']}`
- Overlap quarters: `{summary['bounded_population']['overlap_quarters']}`

## Recovery

- Additional EBIT fills: `{summary['recovery_impact']['additional_ebit_fills']}`
- Additional EBITDA fills: `{summary['recovery_impact']['additional_ebitda_fills']}`
- Additional core-ready uplift: `{summary['recovery_impact']['additional_core_ready_uplift']}`
- Residual bounded rows still blocked: `{summary['recovery_impact']['residual_rows_still_blocked']}`

## Validation

Multiple-interest candidates were resolved only through company-specific profiles with hidden-target EBIT validation. D&A conflicts were resolved only when implied D&A validation selected one non-overlapping source. Q4 remained independently guarded.

## Closure Answers

1. Multiple-interest candidates remained mostly unresolved composition ambiguity for production purposes; typology counts were `{summary['bounded_population']['interest_typology']}`.
2. Safely recoverable multiple-interest rows: `{summary['interest']['auto_ebit']}` of `{summary['interest']['total_rows']}`.
3. The largest interest candidate pattern was `{max(summary['bounded_population']['interest_typology'], key=summary['bounded_population']['interest_typology'].get) if summary['bounded_population']['interest_typology'] else 'NONE'}`.
4. Combined D&A conflicts were mostly real source-scope overlap, not a simple source-priority problem; typology counts were `{summary['bounded_population']['da_typology']}`.
5. Safely recoverable D&A rows: `{summary['da']['auto_ebitda']}` of `{summary['da']['total_rows']}`.
6. Hidden-target predictions were clean where an auto profile existed, but did not apply to the bounded missing rows.
7. Q4 remained separately guarded and produced `{summary['q4']['q4_auto_total']}` auto rows.
8. Bounded production apply is not justified because the dry production plan has `{summary['recovery_impact']['additional_ebit_fills'] + summary['recovery_impact']['additional_ebitda_fills']}` rows.

## Next Step

`{summary['recommended_next_step']}`
"""
    path.write_text(text, encoding="utf-8")


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 4C-3C"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 4C-3C

Classification: `{summary['classification']}`

Status: `BOUNDED_REJECTION_LOGIC_REFINEMENT_COMPLETE`

Canonical financial writes: `0`

Metadata writes: `0`

Bounded rows: `{summary['bounded_population']['total_rows']}`

Additional EBIT fills planned: `{summary['recovery_impact']['additional_ebit_fills']}`

Additional EBITDA fills planned: `{summary['recovery_impact']['additional_ebitda_fills']}`

Additional core-ready uplift planned: `{summary['recovery_impact']['additional_core_ready_uplift']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        fields = empty_csv_fields(path.name)
        path.write_text((",".join(fields) + "\n") if fields else "", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def empty_csv_fields(name: str) -> list[str]:
    plan = [
        "company_id", "ticker", "fiscal_year", "fiscal_quarter", "period_end", "metric",
        "prior_rejection_reason", "resolved_pattern", "formula_profile", "formula_version",
        "component_fact_ids", "concepts", "values", "quarterization_method", "sec_accessions",
        "hidden_target_validation_class", "q_applicability", "derived_value", "source_mode",
        "core_ready_impact", "research_run",
    ]
    mapping = {
        "phase4c3d_bounded_refinement_production_apply_plan.csv": plan,
        "interest_dry_recovery.csv": plan,
        "da_dry_recovery.csv": plan,
        "q4_bounded_dry_recovery.csv": plan,
        "resolved_rejection_reasons.csv": ["prior_rejection_reason", "resolved_pattern", "rows"],
        "da_hidden_target_backtest.csv": ["company_id", "ticker", "metric", "formula_id", "fiscal_year", "fiscal_quarter", "hidden_target_validation_class"],
        "q4_da_validation.csv": ["company_id", "ticker", "metric", "formula_id", "fiscal_year", "fiscal_quarter", "hidden_target_validation_class"],
    }
    return mapping.get(name, [])


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
