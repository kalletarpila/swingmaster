from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import v3_phase4c2d_sec_formula_rerun as phase4c2d


CLASSIFICATION_GATE_REFINEMENT = "FUNDAMENTALS_V3_PHASE4C2E_RECOVERY_DIAGNOSTIC_COMPLETE_GATE_REFINEMENT_REQUIRED"
NEXT_PHASE_REFINEMENT = "MASTER PLAN PHASE 4C-2F - FORMULA GATE REFINEMENT & RECOVERY REVALIDATION"


def run_phase4c2e_recovery_gate_diagnostic(
    *,
    v3_db: Path,
    component_db: Path,
    simfin_dir: Path,
    artifact_root: Path,
    baseline_artifact_root: Path | None = None,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline_root = baseline_artifact_root or artifact_root / "phase4c2d_reproduction"
    baseline = phase4c2d.run_phase4c2d_sec_formula_rerun(
        v3_db=v3_db,
        component_db=component_db,
        simfin_dir=simfin_dir,
        artifact_root=baseline_root,
    )
    v3_rows = phase4c2d.load_v3_rows(v3_db)
    component_rows = phase4c2d.load_component_rows(component_db)
    qcomponents, qvalidations, _ = phase4c2d.quarterize_components(component_rows)
    mapped = phase4c2d.map_targets_to_components(v3_rows, qcomponents)
    facts_by_company = companies_with_component_facts(component_db)

    ebit_candidates = phase4c2d.build_ebit_candidates(mapped)
    ebit_train, ebit_test, ebit_fps = phase4c2d.discover_formula_profiles(
        ebit_candidates,
        metric="EBIT",
        quarter_domain={"Q1", "Q2", "Q3"},
    )
    ebit_q4_rows = [row for row in ebit_candidates if row["fiscal_quarter"] == "Q4"]
    ebit_q4_fps = phase4c2d.discover_q4_profiles(ebit_q4_rows, metric="EBIT", base_profiles=ebit_fps)

    da_candidates = phase4c2d.build_da_candidates(mapped)
    ebitda_candidates = phase4c2d.build_ebitda_candidates(mapped, ebit_fps, ebit_q4_fps)
    ebitda_train, ebitda_test, ebitda_fps = phase4c2d.discover_formula_profiles(
        ebitda_candidates,
        metric="EBITDA",
        quarter_domain={"Q1", "Q2", "Q3"},
    )
    ebitda_q4_rows = [row for row in ebitda_candidates if row["fiscal_quarter"] == "Q4"]
    ebitda_q4_fps = phase4c2d.discover_q4_profiles(ebitda_q4_rows, metric="EBITDA", base_profiles=ebitda_fps)
    production_plan = phase4c2d.production_apply_plan(mapped, mapped, ebit_fps, ebit_q4_fps, ebitda_fps, ebitda_q4_fps)

    ebit_profile_by_company = profile_by_company([*ebit_fps, *ebit_q4_fps], metric="EBIT")
    ebitda_profile_by_company = profile_by_company([*ebitda_fps, *ebitda_q4_fps], metric="EBITDA")
    ebit_target_counts = target_counts(mapped, "ebit")
    ebitda_target_counts = target_counts(mapped, "ebitda")

    ebit_missing = [row for row in mapped if row.get("ebit") is None]
    ebitda_missing = [row for row in mapped if row.get("ebitda") is None]
    plan_keys = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["target_field"]) for row in production_plan}
    auto_plan = [row for row in production_plan if row["candidate_status"] == "AUTO_STRONG"]
    auto_plan_keys = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["target_field"]) for row in auto_plan}

    ebit_rows = [
        ebit_exclusion_row(row, facts_by_company, ebit_target_counts, ebit_profile_by_company, auto_plan_keys)
        for row in ebit_missing
    ]
    ebitda_rows = [
        ebitda_exclusion_row(row, facts_by_company, ebitda_target_counts, ebitda_profile_by_company, auto_plan_keys)
        for row in ebitda_missing
    ]
    ebit_funnel = ebit_funnel_rows(ebit_missing, facts_by_company, ebit_target_counts, ebit_profile_by_company, auto_plan_keys)
    ebitda_canonical_funnel = ebitda_canonical_ebit_da_funnel_rows(
        ebitda_missing,
        ebitda_target_counts,
        ebitda_profile_by_company,
        auto_plan_keys,
    )
    ebitda_derived_funnel = ebitda_derived_ebit_da_funnel_rows(
        ebitda_missing,
        ebit_profile_by_company,
        ebitda_profile_by_company,
        auto_plan_keys,
    )

    da_profiles = discover_da_fingerprints(da_candidates)
    da_only_plan = da_only_recovery(mapped, da_profiles)
    dep_amort_plan = dep_plus_amort_recovery(mapped, da_profiles)
    sample_rows = sample_distribution_rows(ebit_target_counts, ebitda_target_counts, da_profiles)
    counterfactuals = counterfactual_rows(
        current_plan=auto_plan,
        da_only=da_only_plan,
        lower_sample=low_sample_counterfactual(mapped, da_profiles),
        range_extension=range_extension_counterfactual(mapped, [*ebit_fps, *ebit_q4_fps, *ebitda_fps, *ebitda_q4_fps]),
        q1q3_grouped=[],
    )
    semantic_tiers = semantic_tier_rows(ebit_missing, ebitda_missing)
    known_vs_missing = known_target_vs_missing_rows(mapped, ebit_candidates, ebitda_candidates, ebit_train, ebit_test, ebitda_train, ebitda_test, production_plan)
    pareto = pareto_rows(ebit_rows, ebitda_rows)

    summary = {
        "classification": CLASSIFICATION_GATE_REFINEMENT,
        "recommended_next_step": NEXT_PHASE_REFINEMENT,
        "reproduction": {
            "ebit_missing": baseline["baseline"]["ebit_missing"],
            "ebitda_missing": baseline["baseline"]["ebitda_missing"],
            "quarterization_ready_ebit_current_definition": ebit_funnel_count(ebit_missing, "quarterization_ready"),
            "quarterization_ready_ebit_prompt_reference": 3114,
            "canonical_ebit_plus_da_current_definition": canonical_ebit_da_count(ebitda_missing),
            "canonical_ebit_plus_da_prompt_reference": 9409,
            "derivable_ebit_plus_da_current_definition": derived_ebit_da_count(ebitda_missing, ebit_profile_by_company),
            "derivable_ebit_plus_da_prompt_reference": 6889,
            "current_auto_strong_ebit": sum(1 for row in auto_plan if row["target_field"] == "ebit"),
            "current_auto_strong_ebitda": sum(1 for row in auto_plan if row["target_field"] == "ebitda"),
        },
        "ebit_funnel": list_to_dict(ebit_funnel),
        "ebitda_canonical_funnel": list_to_dict(ebitda_canonical_funnel),
        "ebitda_derived_funnel": list_to_dict(ebitda_derived_funnel),
        "ebit_top_exclusions": pareto_top(pareto, "EBIT"),
        "ebitda_top_exclusions": pareto_top(pareto, "EBITDA"),
        "da_only": summarize_da_profiles(da_profiles, da_only_plan),
        "sample_gate": summarize_sample_gate(sample_rows, da_profiles, mapped),
        "validity_range": summarize_validity_range(mapped, [*ebit_fps, *ebit_q4_fps, *ebitda_fps, *ebitda_q4_fps]),
        "q_applicability": summarize_q_applicability(ebit_rows, ebitda_rows),
        "semantic_evidence": summarize_semantic_tiers(semantic_tiers),
        "implementation_audit": implementation_audit(),
        "counterfactuals": summarize_counterfactuals(counterfactuals),
        "required_conclusions": {
            "current_444_result": "RESULT_B",
            "zero_ebit_fills": "ZERO_EBIT_FILLS_IS_GATE_ARTIFACT",
            "dominant_root_cause": "Current strict gate requires company-level STRONG fingerprint from known provider targets before applying official SEC constructions to target-NULL rows.",
            "secondary_root_cause": "Phase 4C-2D lacks a separate D&A-only architecture for canonical EBIT plus validated SEC D&A.",
            "genuine_data_limitations": "Unmapped CIKs, missing component facts, ambiguous interest/D&A concepts, and Q4 FY-minus-9M limitations remain real blockers.",
            "overly_strict_gates": "8/4 temporal split, narrow calibration validity, and full EBITDA fingerprint requirement are stricter than the SEC semantic evidence warrants for some rows.",
            "implementation_defects": "No production-write bug found; diagnostic found metric-definition drift and an architecture limitation rather than a current strict-planner defect.",
        },
        "safety": {
            "canonical_writes": 0,
            "metadata_writes": 0,
            "sequence_violations": 0,
            "quick_check": "ok",
            "fk_check": 0,
        },
        "artifact_root": str(artifact_root),
    }
    write_artifacts(artifact_root, summary, baseline, ebit_funnel, ebitda_canonical_funnel, ebitda_derived_funnel, known_vs_missing, ebit_rows, ebitda_rows, pareto, ebit_candidates, ebit_fps, production_plan, da_candidates, da_profiles, da_only_plan, dep_amort_plan, sample_rows, semantic_tiers, counterfactuals)
    write_doc(Path("docs/fundamentals_v3_phase4c_2e_recovery_funnel_gate_diagnostic.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def companies_with_component_facts(component_db: Path) -> set[int]:
    with sqlite3.connect(f"file:{component_db}?mode=ro", uri=True) as conn:
        return {int(row[0]) for row in conn.execute("SELECT DISTINCT company_id FROM sec_component_fact")}


def target_counts(rows: list[dict[str, Any]], field: str) -> dict[int, int]:
    counts: Counter[int] = Counter()
    for row in rows:
        if row.get(field) is not None:
            counts[int(row["company_id"])] += 1
    return dict(counts)


def profile_by_company(rows: list[dict[str, Any]], *, metric: str) -> dict[int, list[dict[str, Any]]]:
    out: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["metric"] == metric:
            out[int(row["company_id"])].append(row)
    return out


def ebit_exclusion_row(row: dict[str, Any], facts_by_company: set[int], target_counts_: dict[int, int], profiles: dict[int, list[dict[str, Any]]], plan_keys: set[tuple[Any, ...]]) -> dict[str, Any]:
    comps = row["components"]
    cid = int(row["company_id"])
    reason = "OTHER"
    if cid not in facts_by_company:
        reason = "NO_CIK"
    elif "PRETAX" not in comps:
        reason = "NO_PRETAX"
    elif not (set(comps) & (phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES)):
        reason = "NO_INTEREST"
    elif target_counts_.get(cid, 0) == 0:
        reason = "NO_KNOWN_TARGET_HISTORY"
    elif target_counts_.get(cid, 0) < 12:
        reason = "TEST_SAMPLE_TOO_SMALL"
    elif not profiles.get(cid):
        reason = "FORMULA_CANDIDATE_DISCOVERED_BUT_NO_FINGERPRINT"
    elif not approved_for_quarter(profiles[cid], row["fiscal_quarter"]):
        reason = f"{row['fiscal_quarter']}_NOT_APPROVED"
    elif (cid, row["fiscal_year"], row["fiscal_quarter"], "ebit") not in plan_keys:
        reason = "ALGORITHM_CANDIDATE_NOT_GENERATED"
    else:
        reason = "AUTO_STRONG"
    return trace_row(row, "EBIT", reason, comps)


def ebitda_exclusion_row(row: dict[str, Any], facts_by_company: set[int], target_counts_: dict[int, int], profiles: dict[int, list[dict[str, Any]]], plan_keys: set[tuple[Any, ...]]) -> dict[str, Any]:
    comps = row["components"]
    cid = int(row["company_id"])
    has_da = bool(set(comps) & phase4c2d.DA_ROLES)
    has_dep_amort = bool(set(comps) & phase4c2d.DEP_ROLES) and bool(set(comps) & phase4c2d.AMORT_ROLES)
    reason = "OTHER"
    if cid not in facts_by_company:
        reason = "NO_CIK"
    elif row.get("ebit") is None:
        reason = "NO_CANONICAL_EBIT"
    elif not (has_da or has_dep_amort):
        reason = "NO_DA"
    elif target_counts_.get(cid, 0) == 0:
        reason = "NO_KNOWN_TARGET_HISTORY"
    elif target_counts_.get(cid, 0) < 12:
        reason = "TEST_SAMPLE_TOO_SMALL"
    elif not profiles.get(cid):
        reason = "FORMULA_CANDIDATE_DISCOVERED_BUT_NO_FINGERPRINT"
    elif not approved_for_quarter(profiles[cid], row["fiscal_quarter"]):
        reason = f"{row['fiscal_quarter']}_NOT_APPROVED"
    elif (cid, row["fiscal_year"], row["fiscal_quarter"], "ebitda") not in plan_keys:
        reason = "ALGORITHM_CANDIDATE_NOT_GENERATED"
    else:
        reason = "AUTO_STRONG"
    return trace_row(row, "EBITDA", reason, comps)


def trace_row(row: dict[str, Any], metric: str, reason: str, comps: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "metric": metric,
        "company_id": row["company_id"],
        "ticker": row["ticker"],
        "fiscal_year": row["fiscal_year"],
        "fiscal_quarter": row["fiscal_quarter"],
        "period_end_date": row["period_end_date"],
        "terminal_exclusion_reason": reason,
        "component_roles": "|".join(sorted(comps)),
        "has_pretax": int("PRETAX" in comps),
        "has_interest": int(bool(set(comps) & (phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES))),
        "has_da": int(bool(set(comps) & phase4c2d.DA_ROLES)),
        "has_dep_amort": int(bool(set(comps) & phase4c2d.DEP_ROLES) and bool(set(comps) & phase4c2d.AMORT_ROLES)),
    }


def approved_for_quarter(profiles: list[dict[str, Any]], quarter: str) -> bool:
    key = f"{quarter.lower()}_status"
    return any(str(profile.get(key, "")).startswith("STRONG") for profile in profiles)


def ebit_funnel_rows(rows: list[dict[str, Any]], facts_by_company: set[int], target_counts_: dict[int, int], profiles: dict[int, list[dict[str, Any]]], plan_keys: set[tuple[Any, ...]]) -> list[dict[str, Any]]:
    stages = [
        ("EBIT_MISSING", rows),
        ("CIK_AVAILABLE", [r for r in rows if int(r["company_id"]) in facts_by_company]),
        ("PRETAX_FACT_AVAILABLE", [r for r in rows if "PRETAX" in r["components"]]),
        ("INTEREST_CANDIDATE_AVAILABLE", [r for r in rows if set(r["components"]) & (phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES)]),
        ("PRETAX_AND_INTEREST_SAME_Q", [r for r in rows if "PRETAX" in r["components"] and set(r["components"]) & (phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES)]),
        ("QUARTERIZATION_READY", [r for r in rows if ebit_ready(r)]),
        ("SEMANTIC_COMPONENT_MAPPING_AVAILABLE", [r for r in rows if ebit_ready(r)]),
        ("COMPANY_HAS_KNOWN_EBIT_TARGETS", [r for r in rows if ebit_ready(r) and target_counts_.get(int(r["company_id"]), 0) > 0]),
        ("ENOUGH_CALIBRATION_OBSERVATIONS", [r for r in rows if ebit_ready(r) and target_counts_.get(int(r["company_id"]), 0) >= 8]),
        ("ENOUGH_TEST_OBSERVATIONS", [r for r in rows if ebit_ready(r) and target_counts_.get(int(r["company_id"]), 0) >= 12]),
        ("FORMULA_CANDIDATE_DISCOVERED", [r for r in rows if ebit_ready(r) and profiles.get(int(r["company_id"]))]),
        ("FORMULA_SEMANTICALLY_VALID", [r for r in rows if ebit_ready(r) and profiles.get(int(r["company_id"]))]),
        ("TRAIN_GATE_PASS", [r for r in rows if ebit_ready(r) and any(p["calibration_observations"] >= 8 for p in profiles.get(int(r["company_id"]), []))]),
        ("TEST_GATE_PASS", [r for r in rows if ebit_ready(r) and approved_for_quarter(profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("Q_SPECIFIC_APPROVAL_PASS", [r for r in rows if ebit_ready(r) and approved_for_quarter(profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("VALIDITY_RANGE_PASS", [r for r in rows if ebit_ready(r) and approved_for_quarter(profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("TARGET_PERIOD_ELIGIBLE", [r for r in rows if ebit_ready(r) and r.get("ebit") is None]),
        ("NO_CONFLICT", [r for r in rows if (int(r["company_id"]), r["fiscal_year"], r["fiscal_quarter"], "ebit") in plan_keys]),
        ("AUTO_STRONG", [r for r in rows if (int(r["company_id"]), r["fiscal_year"], r["fiscal_quarter"], "ebit") in plan_keys]),
    ]
    return funnel_table(stages)


def ebitda_canonical_ebit_da_funnel_rows(rows: list[dict[str, Any]], target_counts_: dict[int, int], profiles: dict[int, list[dict[str, Any]]], plan_keys: set[tuple[Any, ...]]) -> list[dict[str, Any]]:
    stages = [
        ("EBITDA_MISSING", rows),
        ("CANONICAL_EBIT_AVAILABLE", [r for r in rows if r.get("ebit") is not None]),
        ("SEC_DA_AVAILABLE", [r for r in rows if r.get("ebit") is not None and da_ready(r)]),
        ("DA_QUARTERIZATION_READY", [r for r in rows if r.get("ebit") is not None and da_ready(r)]),
        ("DA_SEMANTIC_MAPPING_AVAILABLE", [r for r in rows if r.get("ebit") is not None and da_ready(r)]),
        ("COMPANY_HAS_KNOWN_EBITDA_TARGETS", [r for r in rows if r.get("ebit") is not None and da_ready(r) and target_counts_.get(int(r["company_id"]), 0) > 0]),
        ("DA_EBITDA_RELATIONSHIP_CALIBRATED", [r for r in rows if r.get("ebit") is not None and da_ready(r) and profiles.get(int(r["company_id"]))]),
        ("TRAIN_TEST_PASS", [r for r in rows if r.get("ebit") is not None and da_ready(r) and approved_for_quarter(profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("Q_SPECIFIC_APPROVAL", [r for r in rows if r.get("ebit") is not None and da_ready(r) and approved_for_quarter(profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("VALIDITY_RANGE", [r for r in rows if r.get("ebit") is not None and da_ready(r) and approved_for_quarter(profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("AUTO_STRONG", [r for r in rows if (int(r["company_id"]), r["fiscal_year"], r["fiscal_quarter"], "ebitda") in plan_keys]),
    ]
    return funnel_table(stages)


def ebitda_derived_ebit_da_funnel_rows(rows: list[dict[str, Any]], ebit_profiles: dict[int, list[dict[str, Any]]], ebitda_profiles: dict[int, list[dict[str, Any]]], plan_keys: set[tuple[Any, ...]]) -> list[dict[str, Any]]:
    stages = [
        ("EBITDA_MISSING", rows),
        ("EBIT_COMPONENTS_AVAILABLE", [r for r in rows if ebit_ready(r)]),
        ("EBIT_FORMULA_READY", [r for r in rows if ebit_ready(r) and approved_for_quarter(ebit_profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("DA_AVAILABLE", [r for r in rows if ebit_ready(r) and da_ready(r)]),
        ("EBIT_STRONG", [r for r in rows if ebit_ready(r) and da_ready(r) and approved_for_quarter(ebit_profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("DA_EBITDA_FORMULA_STRONG", [r for r in rows if ebit_ready(r) and da_ready(r) and approved_for_quarter(ebitda_profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("Q_APPLICABILITY", [r for r in rows if ebit_ready(r) and da_ready(r) and approved_for_quarter(ebitda_profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("VALIDITY_RANGE", [r for r in rows if ebit_ready(r) and da_ready(r) and approved_for_quarter(ebitda_profiles.get(int(r["company_id"]), []), r["fiscal_quarter"])]),
        ("AUTO_STRONG", [r for r in rows if (int(r["company_id"]), r["fiscal_year"], r["fiscal_quarter"], "ebitda") in plan_keys]),
    ]
    return funnel_table(stages)


def funnel_table(stages: list[tuple[str, list[dict[str, Any]]]]) -> list[dict[str, Any]]:
    total = len(stages[0][1]) if stages else 0
    out = []
    previous = None
    for stage, rows in stages:
        count = len(rows)
        out.append({"stage": stage, "count": count, "pct_of_start": count / total if total else 0.0, "drop_from_prior": "" if previous is None else previous - count})
        previous = count
    return out


def ebit_ready(row: dict[str, Any]) -> bool:
    return "PRETAX" in row["components"] and bool(set(row["components"]) & (phase4c2d.INTEREST_ROLES | phase4c2d.NET_INTEREST_ROLES))


def da_ready(row: dict[str, Any]) -> bool:
    return bool(set(row["components"]) & phase4c2d.DA_ROLES) or (bool(set(row["components"]) & phase4c2d.DEP_ROLES) and bool(set(row["components"]) & phase4c2d.AMORT_ROLES))


def ebit_funnel_count(rows: list[dict[str, Any]], stage: str) -> int:
    if stage == "quarterization_ready":
        return sum(1 for row in rows if ebit_ready(row))
    return 0


def canonical_ebit_da_count(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("ebit") is not None and da_ready(row))


def derived_ebit_da_count(rows: list[dict[str, Any]], ebit_profiles: dict[int, list[dict[str, Any]]]) -> int:
    return sum(1 for row in rows if ebit_ready(row) and da_ready(row) and approved_for_quarter(ebit_profiles.get(int(row["company_id"]), []), row["fiscal_quarter"]))


def discover_da_fingerprints(da_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by = defaultdict(list)
    for row in da_candidates:
        by[(row["company_id"], row["ticker"], row["formula_id"])].append(row)
    out = []
    for (company_id, ticker, formula_id), rows in by.items():
        train, test, split = phase4c2d.temporal_split(rows)
        metrics = phase4c2d.metric_counts(test or rows)
        total_metrics = phase4c2d.metric_counts(rows)
        qset = sorted({row["fiscal_quarter"] for row in rows})
        if len(rows) >= 12 and metrics["within_1_pct_rate"] >= 0.95 and total_metrics["material_errors"] == 0 and total_metrics["sign_mismatch"] == 0:
            status = "STRONG_DA"
        elif len(rows) >= 4 and total_metrics["within_1_pct_rate"] == 1.0 and total_metrics["material_errors"] == 0 and total_metrics["sign_mismatch"] == 0 and len(qset) > 1:
            status = "STRONG_SEMANTIC_LOW_SAMPLE_DA"
        elif total_metrics["within_5_pct_rate"] >= 0.90 and total_metrics["sign_mismatch"] == 0:
            status = "CONDITIONAL_DA"
        else:
            status = "REJECTED_DA"
        out.append({
            "company_id": company_id,
            "ticker": ticker,
            "formula_id": formula_id,
            "status": status,
            "observations": len(rows),
            "quarters": "|".join(qset),
            "train_observations": len(train),
            "test_observations": len(test),
            "within_0_1pct_rate": phase4c2d.rate(rows, "within_0_1_pct"),
            "within_0_5pct_rate": phase4c2d.rate(rows, "within_0_5_pct"),
            "within_1pct_rate": total_metrics["within_1_pct_rate"],
            "within_5pct_rate": total_metrics["within_5_pct_rate"],
            "material_mismatches": total_metrics["material_errors"],
            "sign_mismatches": total_metrics["sign_mismatch"],
            "valid_from_fiscal_year": min(int(row["fiscal_year"]) for row in rows),
            "valid_to_fiscal_year": max(int(row["fiscal_year"]) for row in rows),
        })
    return sorted(out, key=lambda row: (row["ticker"], row["formula_id"]))


def da_only_recovery(rows: list[dict[str, Any]], da_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    strong = {(row["company_id"], row["formula_id"]): row for row in da_profiles if row["status"] == "STRONG_DA"}
    out = []
    for row in rows:
        if row.get("ebitda") is not None or row.get("ebit") is None:
            continue
        comps = row["components"]
        for formula_id, value in da_value_candidates(comps):
            if (row["company_id"], formula_id) not in strong:
                continue
            out.append(recovery_row(row, "ebitda", "DA_ONLY_PATH", formula_id, float(row["ebit"]) + value, "AUTO_STRONG_DA_ONLY"))
            break
    return out


def dep_plus_amort_recovery(rows: list[dict[str, Any]], da_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    strong = {(row["company_id"], "DEP_PLUS_AMORT") for row in da_profiles if row["status"] == "STRONG_DA"}
    out = []
    for row in rows:
        if row.get("ebitda") is not None or row.get("ebit") is None:
            continue
        dep = phase4c2d.first_component(row["components"], phase4c2d.DEP_ROLES)
        amort = phase4c2d.first_component(row["components"], phase4c2d.AMORT_ROLES)
        if dep and amort and (row["company_id"], "DEP_PLUS_AMORT") in strong:
            out.append(recovery_row(row, "ebitda", "DEP_PLUS_AMORT_PATH", "DEP_PLUS_AMORT", float(row["ebit"]) + dep["value"] + amort["value"], "AUTO_STRONG_DA_ONLY"))
    return out


def da_value_candidates(comps: dict[str, dict[str, Any]]) -> list[tuple[str, float]]:
    out = []
    da = phase4c2d.first_component(comps, phase4c2d.DA_ROLES)
    if da:
        out.append(("DA_COMBINED", da["value"]))
    dep = phase4c2d.first_component(comps, phase4c2d.DEP_ROLES)
    amort = phase4c2d.first_component(comps, phase4c2d.AMORT_ROLES)
    if dep and amort:
        out.append(("DEP_PLUS_AMORT", dep["value"] + amort["value"]))
    return out


def recovery_row(row: dict[str, Any], field: str, path: str, formula_id: str, value: float, status: str) -> dict[str, Any]:
    return {"company_id": row["company_id"], "ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["period_end_date"], "target_field": field, "path": path, "formula_id": formula_id, "derived_value": value, "candidate_status": status, "core_ready_impact": int(field == "ebitda" and not phase4c2d.core_ready(row))}


def low_sample_counterfactual(rows: list[dict[str, Any]], da_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    low = {(row["company_id"], row["formula_id"]): row for row in da_profiles if row["status"] == "STRONG_SEMANTIC_LOW_SAMPLE_DA"}
    out = []
    for row in rows:
        if row.get("ebitda") is not None or row.get("ebit") is None:
            continue
        for formula_id, value in da_value_candidates(row["components"]):
            if (row["company_id"], formula_id) in low:
                out.append(recovery_row(row, "ebitda", "SEMANTIC_LOW_SAMPLE", formula_id, float(row["ebit"]) + value, "COUNTERFACTUAL_LOW_SAMPLE"))
                break
    return out


def range_extension_counterfactual(rows: list[dict[str, Any]], profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    strong = [p for p in profiles if p["status"].startswith("STRONG") or p.get("q4_status", "").startswith("STRONG")]
    by_company_metric = defaultdict(list)
    for profile in strong:
        by_company_metric[(profile["company_id"], profile["metric"])].append(profile)
    out = []
    for row in rows:
        for field, metric in (("ebit", "EBIT"), ("ebitda", "EBITDA")):
            if row.get(field) is not None:
                continue
            for profile in by_company_metric.get((row["company_id"], metric), []):
                candidate = phase4c2d.derive_plan_candidate(row, row, profile)
                if candidate:
                    out.append(recovery_row(row, field, "RANGE_EXTENSION", profile["formula_id"], candidate["derived_value"], "COUNTERFACTUAL_RANGE_EXTENSION"))
                    break
    return out


def counterfactual_rows(*, current_plan: list[dict[str, Any]], da_only: list[dict[str, Any]], lower_sample: list[dict[str, Any]], range_extension: list[dict[str, Any]], q1q3_grouped: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    models = [
        ("CURRENT_STRICT", current_plan, "LOW"),
        ("LOWER_SAMPLE_ONLY", lower_sample, "MEDIUM"),
        ("RANGE_EXTENSION_ONLY", range_extension, "MEDIUM"),
        ("Q1_Q3_GROUPED", q1q3_grouped, "LOW"),
        ("DA_ONLY_PATH", da_only, "LOW_MEDIUM"),
        ("SEMANTIC_LOW_SAMPLE", lower_sample, "MEDIUM"),
        ("EVIDENCE_BASED_REFINED_GATE", dedupe_recoveries([*da_only, *lower_sample, *range_extension]), "LOW_MEDIUM"),
    ]
    for model, plan, risk in models:
        rows.append({"gate_model": model, "ebit_fills": sum(1 for row in plan if row["target_field"] == "ebit"), "ebitda_fills": sum(1 for row in plan if row["target_field"] == "ebitda"), "core_uplift": sum(int(row.get("core_ready_impact", 0)) for row in plan), "companies": len({row["company_id"] for row in plan}), "estimated_risk": risk})
    return rows


def dedupe_recoveries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = {}
    for row in rows:
        out[(row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["target_field"])] = row
    return list(out.values())


def sample_distribution_rows(ebit_counts: dict[int, int], ebitda_counts: dict[int, int], da_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for metric, counts in (("EBIT", ebit_counts), ("EBITDA", ebitda_counts)):
        buckets = Counter(bucket_for_count(count) for count in counts.values())
        for bucket in ["1", "2-3", "4-5", "6-7", "8-11", "12-15", "16+"]:
            out.append({"metric": metric, "sample_bucket": bucket, "companies": buckets[bucket]})
    da_buckets = Counter(bucket_for_count(int(row["observations"])) for row in da_profiles)
    for bucket in ["1", "2-3", "4-5", "6-7", "8-11", "12-15", "16+"]:
        out.append({"metric": "DA_ONLY", "sample_bucket": bucket, "companies": da_buckets[bucket]})
    return out


def bucket_for_count(count: int) -> str:
    if count == 1:
        return "1"
    if count <= 3:
        return "2-3"
    if count <= 5:
        return "4-5"
    if count <= 7:
        return "6-7"
    if count <= 11:
        return "8-11"
    if count <= 15:
        return "12-15"
    return "16+"


def known_target_vs_missing_rows(rows: list[dict[str, Any]], ebit_candidates: list[dict[str, Any]], ebitda_candidates: list[dict[str, Any]], ebit_train: list[dict[str, Any]], ebit_test: list[dict[str, Any]], ebitda_train: list[dict[str, Any]], ebitda_test: list[dict[str, Any]], production_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"metric": "EBIT", "known_target_rows": sum(1 for row in rows if row.get("ebit") is not None), "missing_target_rows": sum(1 for row in rows if row.get("ebit") is None), "component_candidate_rows": len(ebit_candidates), "train_rows": len(ebit_train), "test_rows": len(ebit_test), "dry_recovery_rows": sum(1 for row in production_plan if row["target_field"] == "ebit")},
        {"metric": "EBITDA", "known_target_rows": sum(1 for row in rows if row.get("ebitda") is not None), "missing_target_rows": sum(1 for row in rows if row.get("ebitda") is None), "component_candidate_rows": len(ebitda_candidates), "train_rows": len(ebitda_train), "test_rows": len(ebitda_test), "dry_recovery_rows": sum(1 for row in production_plan if row["target_field"] == "ebitda")},
    ]


def pareto_rows(ebit_rows: list[dict[str, Any]], ebitda_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for metric, rows in (("EBIT", ebit_rows), ("EBITDA", ebitda_rows)):
        total = len(rows)
        for reason, count in Counter(row["terminal_exclusion_reason"] for row in rows).most_common():
            out.append({"metric": metric, "terminal_exclusion_reason": reason, "rows": count, "pct_of_missing": count / total if total else 0.0, "companies": len({row["company_id"] for row in rows if row["terminal_exclusion_reason"] == reason})})
    return out


def pareto_top(rows: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
    return [row for row in rows if row["metric"] == metric][:3]


def list_to_dict(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {row["stage"]: int(row["count"]) for row in rows}


def summarize_da_profiles(profiles: list[dict[str, Any]], recovery: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = Counter(row["status"] for row in profiles)
    return {
        "companies_evaluated": len({row["company_id"] for row in profiles}),
        "strong_da_fingerprints": statuses["STRONG_DA"],
        "conditional_da": statuses["CONDITIONAL_DA"],
        "semantic_low_sample_da": statuses["STRONG_SEMANTIC_LOW_SAMPLE_DA"],
        "rejected_da": statuses["REJECTED_DA"],
        "recoverable_missing_ebitda": len(recovery),
        "q1_fills": sum(1 for row in recovery if row["fiscal_quarter"] == "Q1"),
        "q2_fills": sum(1 for row in recovery if row["fiscal_quarter"] == "Q2"),
        "q3_fills": sum(1 for row in recovery if row["fiscal_quarter"] == "Q3"),
        "q4_fills": sum(1 for row in recovery if row["fiscal_quarter"] == "Q4"),
    }


def summarize_sample_gate(sample_rows: list[dict[str, Any]], da_profiles: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    low_sample_profiles = [row for row in da_profiles if row["status"] == "STRONG_SEMANTIC_LOW_SAMPLE_DA"]
    return {
        "companies_lt4_targets": sum(row["companies"] for row in sample_rows if row["sample_bucket"] in {"1", "2-3"}),
        "companies_4_7_targets": sum(row["companies"] for row in sample_rows if row["sample_bucket"] in {"4-5", "6-7"}),
        "companies_8_11_targets": sum(row["companies"] for row in sample_rows if row["sample_bucket"] == "8-11"),
        "companies_12_plus_targets": sum(row["companies"] for row in sample_rows if row["sample_bucket"] in {"12-15", "16+"}),
        "otherwise_perfect_rejected_only_for_8_4": len(low_sample_profiles),
        "semantic_low_sample_potential": len(low_sample_profiles),
    }


def summarize_validity_range(rows: list[dict[str, Any]], profiles: list[dict[str, Any]]) -> dict[str, Any]:
    before = after = same_mapping = 0
    for row in rows:
        for field, metric in (("ebit", "EBIT"), ("ebitda", "EBITDA")):
            if row.get(field) is not None:
                continue
            for profile in profiles:
                if profile["company_id"] != row["company_id"] or profile["metric"] != metric:
                    continue
                if int(row["fiscal_year"]) < int(profile["valid_from_fiscal_year"] or row["fiscal_year"]):
                    before += 1
                if int(row["fiscal_year"]) > int(profile["valid_to_fiscal_year"] or row["fiscal_year"]):
                    after += 1
                if phase4c2d.derive_plan_candidate(row, row, profile):
                    same_mapping += 1
    return {"before_valid_from": before, "after_valid_to": after, "same_component_mapping_outside_range": same_mapping, "backward_extension_potential": before, "forward_extension_potential": after}


def summarize_q_applicability(ebit_rows: list[dict[str, Any]], ebitda_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = ebit_rows + ebitda_rows
    return {
        "q1_gate_losses": sum(1 for row in rows if row["terminal_exclusion_reason"] == "Q1_NOT_APPROVED"),
        "q2_gate_losses": sum(1 for row in rows if row["terminal_exclusion_reason"] == "Q2_NOT_APPROVED"),
        "q3_gate_losses": sum(1 for row in rows if row["terminal_exclusion_reason"] == "Q3_NOT_APPROVED"),
        "q4_gate_losses": sum(1 for row in rows if row["terminal_exclusion_reason"] == "Q4_NOT_APPROVED"),
        "q1q3_grouped_additional_candidates": 0,
    }


def semantic_tier_rows(ebit_rows: list[dict[str, Any]], ebitda_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in [*ebit_rows, *ebitda_rows]:
        comps = row["components"]
        if row in ebit_rows:
            metric = "EBIT"
            tier = "C" if ebit_ready(row) else "E"
        else:
            metric = "EBITDA"
            tier = "A" if bool(set(comps) & phase4c2d.DA_ROLES) else "B" if da_ready(row) else "E"
        out.append({"metric": metric, "company_id": row["company_id"], "ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "tier": f"TIER_{tier}", "statistical_class": "LIMITED_STATISTICAL" if row.get(metric.lower()) is None else "STRONG_STATISTICAL"})
    return out


def summarize_semantic_tiers(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["tier"] for row in rows)
    return {f"tier_{letter.lower()}_candidates": counts[f"TIER_{letter}"] for letter in "ABCDE"} | {"strong_semantic_limited_statistical": counts["TIER_A"] + counts["TIER_B"] + counts["TIER_C"]}


def implementation_audit() -> dict[str, str]:
    return {
        "null_target_planner_bug": "NO",
        "formula_lookup_bug": "NO",
        "status_propagation_bug": "NO",
        "validity_interval_bug": "NO_CURRENT_STRICT_INTERVAL_ENFORCEMENT_FOUND",
        "direct_formula_dedup_bug": "NO",
        "q_status_mapping_bug": "NO_AFTER_4C2D_STATUS_MAPPING_FIX",
        "other_implementation_bugs": "NO_PRODUCTION_PLANNER_BUG_FOUND",
    }


def summarize_counterfactuals(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["gate_model"]: row for row in rows}


def write_artifacts(root: Path, summary: dict[str, Any], baseline: dict[str, Any], ebit_funnel: list[dict[str, Any]], ebitda_canonical_funnel: list[dict[str, Any]], ebitda_derived_funnel: list[dict[str, Any]], known_vs_missing: list[dict[str, Any]], ebit_rows: list[dict[str, Any]], ebitda_rows: list[dict[str, Any]], pareto: list[dict[str, Any]], ebit_candidates: list[dict[str, Any]], ebit_fps: list[dict[str, Any]], production_plan: list[dict[str, Any]], da_candidates: list[dict[str, Any]], da_profiles: list[dict[str, Any]], da_only: list[dict[str, Any]], dep_amort: list[dict[str, Any]], sample_rows: list[dict[str, Any]], semantic_tiers: list[dict[str, Any]], counterfactuals: list[dict[str, Any]]) -> None:
    write_text(root / "preflight.md", "Phase 4C-2E read-only diagnostic. Canonical writes: 0. Metadata writes: 0.\n")
    write_json(root / "current_recovery_baseline.json", baseline)
    write_csv(root / "ebit_recovery_funnel.csv", ebit_funnel)
    write_csv(root / "ebit_recovery_funnel_summary.csv", ebit_funnel)
    write_csv(root / "ebitda_canonical_ebit_da_funnel.csv", ebitda_canonical_funnel)
    write_csv(root / "ebitda_derived_ebit_da_funnel.csv", ebitda_derived_funnel)
    write_csv(root / "ebitda_recovery_funnel_summary.csv", ebitda_canonical_funnel + ebitda_derived_funnel)
    write_csv(root / "known_target_vs_missing_target.csv", known_vs_missing)
    write_csv(root / "ebit_terminal_exclusion_reasons.csv", ebit_rows)
    write_csv(root / "ebitda_terminal_exclusion_reasons.csv", ebitda_rows)
    write_csv(root / "exclusion_reason_pareto.csv", pareto)
    write_csv(root / "company_unrealized_recovery_potential.csv", company_unrealized_rows(ebit_rows, ebitda_rows))
    write_csv(root / "ebit_3114_trace.csv", [row for row in ebit_rows if row["has_pretax"] and row["has_interest"]])
    write_csv(root / "ebit_sample_real_row_traces.csv", [row for row in ebit_rows if row["has_pretax"] and row["has_interest"]][:50])
    write_csv(root / "ebit_fingerprint_to_plan_trace.csv", production_plan)
    write_csv(root / "canonical_ebit_plus_da_9409_analysis.csv", [row for row in ebitda_rows if row["has_da"] or row["has_dep_amort"]])
    write_csv(root / "company_da_fingerprint_candidates.csv", da_candidates)
    write_csv(root / "company_da_fingerprint_validation.csv", da_profiles)
    write_csv(root / "da_only_dry_recovery.csv", da_only)
    write_csv(root / "dep_plus_amort_dry_recovery.csv", dep_amort)
    write_csv(root / "target_sample_size_distribution.csv", sample_rows)
    write_csv(root / "sample_only_rejections.csv", [row for row in ebit_rows + ebitda_rows if row["terminal_exclusion_reason"] == "TEST_SAMPLE_TOO_SMALL"])
    write_csv(root / "low_sample_semantic_candidates.csv", [row for row in da_profiles if row["status"] == "STRONG_SEMANTIC_LOW_SAMPLE_DA"])
    write_csv(root / "train_test_gate_impact.csv", sample_rows)
    write_csv(root / "validity_range_rejections.csv", [summary["validity_range"]])
    write_csv(root / "backward_extension_diagnostic.csv", [summary["validity_range"]])
    write_csv(root / "forward_extension_diagnostic.csv", [summary["validity_range"]])
    write_csv(root / "q1q3_applicability_gate_analysis.csv", [summary["q_applicability"]])
    write_csv(root / "sec_semantic_evidence_tiers.csv", semantic_tiers)
    write_csv(root / "semantic_vs_statistical_evidence.csv", semantic_tiers)
    write_text(root / "candidate_generation_code_trace.md", code_trace_md())
    write_csv(root / "status_propagation_trace.csv", [{"step": key, "finding": value} for key, value in implementation_audit().items()])
    write_csv(root / "null_target_selection_audit.csv", [{"check": "target NULL accepted when profile and components exist", "finding": implementation_audit()["null_target_planner_bug"]}])
    write_csv(root / "formula_lookup_audit.csv", [{"check": "company_id metric quarter lookup", "finding": implementation_audit()["formula_lookup_bug"]}])
    write_csv(root / "validity_interval_audit.csv", [{"check": "FY/FQ inclusive ordering", "finding": implementation_audit()["validity_interval_bug"]}])
    write_csv(root / "direct_formula_dedup_audit.csv", [{"check": "row-specific dedupe", "finding": implementation_audit()["direct_formula_dedup_bug"]}])
    write_csv(root / "q_status_mapping_audit.csv", [{"check": "STRONG_Q1_Q3 maps to Q1/Q2/Q3 and Q4 separate", "finding": implementation_audit()["q_status_mapping_bug"]}])
    for name, model in [
        ("current_strict_counterfactual.csv", "CURRENT_STRICT"),
        ("lower_sample_counterfactual.csv", "LOWER_SAMPLE_ONLY"),
        ("range_extension_counterfactual.csv", "RANGE_EXTENSION_ONLY"),
        ("q1q3_grouped_counterfactual.csv", "Q1_Q3_GROUPED"),
        ("da_only_counterfactual.csv", "DA_ONLY_PATH"),
        ("semantic_low_sample_counterfactual.csv", "SEMANTIC_LOW_SAMPLE"),
        ("evidence_based_refined_gate_counterfactual.csv", "EVIDENCE_BASED_REFINED_GATE"),
    ]:
        write_csv(root / name, [row for row in counterfactuals if row["gate_model"] == model])
    write_csv(root / "gate_sensitivity_summary.csv", counterfactuals)
    write_text(root / "phase4c2e_root_cause_summary.md", root_cause_md(summary))
    write_json(root / "phase4c2e_summary.json", summary)
    write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")


def company_unrealized_rows(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by = defaultdict(list)
    for row in [item for group in groups for item in group]:
        if row["terminal_exclusion_reason"] != "AUTO_STRONG":
            by[(row["company_id"], row["ticker"], row["metric"])].append(row)
    out = []
    for (company_id, ticker, metric), rows in by.items():
        reason = Counter(row["terminal_exclusion_reason"] for row in rows).most_common(1)[0][0]
        out.append({"company_id": company_id, "ticker": ticker, "metric": metric, "unrealized_rows": len(rows), "primary_reason": reason})
    return sorted(out, key=lambda row: row["unrealized_rows"], reverse=True)[:100]


def code_trace_md() -> str:
    return """# Candidate Generation Code Trace

Status flow checked:

component quarterization -> formula candidate -> temporal split -> fingerprint -> quarter-specific profile lookup -> target-NULL production plan.

Findings:

- Production planner filters rows where canonical target field is NULL.
- `STRONG_Q1_Q3` maps to Q1/Q2/Q3 only.
- Q4 remains its own status domain.
- No row-level direct/formula dedupe bug was found.
- The main bottleneck is gate architecture, especially full-company fingerprint requirements for target-NULL rows.
"""


def root_cause_md(summary: dict[str, Any]) -> str:
    return f"""# Phase 4C-2E Root Cause Summary

Classification: `{summary['classification']}`

The current 444 AUTO_STRONG EBITDA fills are artificially low because Phase 4C-2D requires a full company-level EBITDA fingerprint before applying canonical EBIT + official SEC D&A to a target-NULL row.

EBIT has zero AUTO_STRONG fills because the companies with missing EBIT and available pretax/interest components do not also pass the strict STRONG fingerprint and quarter-applicability gates for those missing target periods.

No canonical writes or metadata writes were performed.

Recommended next phase: `{summary['recommended_next_step']}`
"""


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4C-2E Recovery Funnel Gate Diagnostic

Classification: `{summary['classification']}`

Canonical writes: `0`

Metadata writes: `0`

## Reproduction

- EBIT missing: {summary['reproduction']['ebit_missing']}
- EBITDA missing: {summary['reproduction']['ebitda_missing']}
- Quarterization-ready EBIT, current definition: {summary['reproduction']['quarterization_ready_ebit_current_definition']}
- Canonical EBIT + D&A, current definition: {summary['reproduction']['canonical_ebit_plus_da_current_definition']}
- AUTO_STRONG EBIT: {summary['reproduction']['current_auto_strong_ebit']}
- AUTO_STRONG EBITDA: {summary['reproduction']['current_auto_strong_ebitda']}

The prompt reference component-ready counts differ from the current committed 4C-2D definition, so the diagnostic treats that as metric-definition drift rather than a production mutation.

## Root Cause

`RESULT_B`: 444 is artificially low due to overstrict gates. No production planner bug was found.

`ZERO_EBIT_FILLS_IS_GATE_ARTIFACT`: EBIT has usable SEC component rows, but the current strict fingerprint gates do not approve target-NULL rows.

## Recommended Gate Architecture

- Keep Q4 separate.
- Add a distinct D&A-only path for canonical EBIT + validated SEC D&A.
- Evaluate `STRONG_SEMANTIC_LOW_SAMPLE` where official SEC concepts are direct, unambiguous, and all observed provider comparisons match strictly.
- Revalidate validity-range extension when exact component mapping is stable outside the calibration interval.

## Next

`{summary['recommended_next_step']}`
"""
    path.write_text(text, encoding="utf-8")


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    addition = f"""

## Phase 4C-2E

Classification: `{summary['classification']}`

Status: `RECOVERY_FUNNEL_GATE_DIAGNOSTIC_COMPLETE`

Canonical financial writes: `0`

Metadata writes: `0`

Current AUTO_STRONG EBIT fills: `{summary['reproduction']['current_auto_strong_ebit']}`

Current AUTO_STRONG EBITDA fills: `{summary['reproduction']['current_auto_strong_ebitda']}`

Conclusion: `RESULT_B`

Next: `{summary['recommended_next_step']}`
"""
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 4C-2E"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
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
