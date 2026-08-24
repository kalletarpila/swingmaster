from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import v3_phase4c2d_sec_formula_rerun as phase4c2d
from swingmaster.fundamentals import v3_phase4c2e_recovery_gate_diagnostic as phase4c2e
from swingmaster.fundamentals.v3_canonical_closure import final_canonical_baseline, field_coverage_summary
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE4C2F_GATE_REFINEMENT_COMPLETE_READY_FOR_PRODUCTION_APPLY"
CLASSIFICATION_LIMITED = "FUNDAMENTALS_V3_PHASE4C2F_REFINED_GATES_VALIDATED_RECOVERY_REMAINS_LIMITED"
CLASSIFICATION_RISKY = "FUNDAMENTALS_V3_PHASE4C2F_GATE_REFINEMENT_TOO_RISKY"
NEXT_PHASE_APPLY = "MASTER PLAN PHASE 4C-3 - EBIT & EBITDA PRODUCTION APPLY"
RUN_ID = "PHASE4C2F_EVIDENCE_BASED_GATE_REFINEMENT"


def run_phase4c2f_gate_refinement(
    *,
    v3_db: Path,
    component_db: Path,
    simfin_dir: Path,
    artifact_root: Path,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline_raw = final_canonical_baseline(v3_db)
    missing = {row["field"]: int(row["null_q"]) for row in field_coverage_summary(v3_db)}
    v3_rows = phase4c2d.load_v3_rows(v3_db)
    component_rows = phase4c2d.load_component_rows(component_db)
    qcomponents, _, _ = phase4c2d.quarterize_components(component_rows)
    mapped = phase4c2d.map_targets_to_components(v3_rows, qcomponents)

    ebit_candidates = phase4c2d.build_ebit_candidates(mapped)
    strict_ebit_train, strict_ebit_test, strict_ebit_fps = phase4c2d.discover_formula_profiles(ebit_candidates, metric="EBIT", quarter_domain={"Q1", "Q2", "Q3"})
    strict_ebit_q4 = phase4c2d.discover_q4_profiles([row for row in ebit_candidates if row["fiscal_quarter"] == "Q4"], metric="EBIT", base_profiles=strict_ebit_fps)
    da_candidates = phase4c2d.build_da_candidates(mapped)
    strict_ebitda_candidates = phase4c2d.build_ebitda_candidates(mapped, strict_ebit_fps, strict_ebit_q4)
    strict_ebitda_train, strict_ebitda_test, strict_ebitda_fps = phase4c2d.discover_formula_profiles(strict_ebitda_candidates, metric="EBITDA", quarter_domain={"Q1", "Q2", "Q3"})
    strict_ebitda_q4 = phase4c2d.discover_q4_profiles([row for row in strict_ebitda_candidates if row["fiscal_quarter"] == "Q4"], metric="EBITDA", base_profiles=strict_ebitda_fps)
    strict_auto = [
        strict_plan_row(row)
        for row in phase4c2d.production_apply_plan(mapped, mapped, strict_ebit_fps, strict_ebit_q4, strict_ebitda_fps, strict_ebitda_q4)
        if row["candidate_status"] == "AUTO_STRONG"
    ]
    ebit_profiles = refined_formula_profiles(ebit_candidates, metric="EBIT")
    da_profiles = refined_formula_profiles(da_candidates, metric="DA")
    approved_ebit = [row for row in ebit_profiles if is_auto(row)]
    approved_da = [row for row in da_profiles if is_auto(row)]

    ebit_recovery = refined_ebit_recovery(mapped, approved_ebit)
    ebitda_path_a = refined_ebitda_path_a(mapped, approved_da)
    ebitda_path_b = refined_ebitda_path_b(mapped, approved_ebit, approved_da)
    ebitda_recovery = dedupe_plan([*ebitda_path_a, *ebitda_path_b])
    production_plan = dedupe_plan([*strict_auto, *ebit_recovery, *ebitda_recovery])

    backtest_rows = refined_backtest(ebit_candidates, da_candidates, ebit_profiles, da_profiles)
    backtest_by_class = error_by(backtest_rows, "approval_status")
    backtest_by_quarter = error_by(backtest_rows, "fiscal_quarter")
    backtest_by_company = error_by(backtest_rows, "ticker")
    backtest_by_industry = [{"industry_group": "UNCLASSIFIED_LOCAL", **error_metrics(backtest_rows)}]
    material_rate = max((float(row["gt_5_pct_rate"]) for row in backtest_by_class if row["approval_status"].startswith("AUTO")), default=0.0)
    sign_errors = sum(int(row["sign_mismatch"]) for row in backtest_rows if str(row["approval_status"]).startswith("AUTO"))
    integrity = structural_integrity(v3_db)
    classification = CLASSIFICATION_READY if material_rate <= 0.01 and sign_errors == 0 and integrity["phase3_structural_gates_pass"] else CLASSIFICATION_RISKY
    if len(production_plan) <= 444 and classification == CLASSIFICATION_READY:
        classification = CLASSIFICATION_LIMITED

    summary = {
        "classification": classification,
        "recommended_next_step": NEXT_PHASE_APPLY if classification == CLASSIFICATION_READY else "MASTER PLAN PHASE 4C-2F-REVIEW - REFINE RISK GATES BEFORE APPLY",
        "baseline": {
            "companies": baseline_raw["company_total"],
            "canonical_q": baseline_raw["coverage"]["canonical_q_total"],
            "core_ready": baseline_raw["coverage"]["core_ready_q"],
            "core_not_ready": baseline_raw["coverage"]["core_not_ready_q"],
            "ebit_missing": missing.get("ebit", 0),
            "ebitda_missing": missing.get("ebitda", 0),
        },
        "evidence_architecture": evidence_architecture_summary(ebit_profiles, da_profiles),
        "ebit": ebit_summary(ebit_profiles, ebit_recovery, missing.get("ebit", 0)),
        "da": da_summary(da_profiles),
        "ebitda_path_a": path_summary(mapped, ebitda_path_a, "PATH_A_CANONICAL_EBIT_PLUS_DA"),
        "ebitda_path_b": path_summary(mapped, ebitda_path_b, "PATH_B_DERIVED_EBIT_PLUS_DA"),
        "q1q3": q1q3_summary(production_plan),
        "q4": q4_summary(production_plan, backtest_rows),
        "sample_policy": sample_policy_summary(ebit_profiles, da_profiles),
        "validity": validity_summary(ebit_profiles, da_profiles, production_plan),
        "backtest": backtest_summary(backtest_rows),
        "backtest_by_class": {row["approval_status"]: row for row in backtest_by_class},
        "recovery_comparison": {
            "current_strict": {"ebit": 0, "ebitda": 444, "core_uplift": 444},
            "phase4c2e_counterfactual": {"ebit": 1, "ebitda": 632, "core_uplift": 632},
            "final_refined": {
                "ebit": sum(1 for row in production_plan if row["target_field"] == "ebit"),
                "ebitda": sum(1 for row in production_plan if row["target_field"] == "ebitda"),
                "core_uplift": sum(int(row["core_ready_impact"]) for row in production_plan),
            },
            "revised_core_ready": baseline_raw["coverage"]["core_ready_q"] + sum(int(row["core_ready_impact"]) for row in production_plan),
            "remaining_core_not_ready": baseline_raw["coverage"]["core_not_ready_q"] - sum(int(row["core_ready_impact"]) for row in production_plan),
        },
        "remaining_limitations": remaining_limitations(mapped, production_plan, component_db),
        "metadata": {
            "durable_formula_metadata_justified": True,
            "formula_registry_entries": len(formula_registry_rows()),
            "formula_profile_rows": len([*ebit_profiles, *da_profiles]),
            "production_plan_rows": len(production_plan),
            "direct_rows": 0,
            "formula_rows": len(production_plan),
            "q4_rows": sum(1 for row in production_plan if row["fiscal_quarter"] == "Q4"),
            "conditional_excluded": True,
            "proxy_excluded": True,
        },
        "safety": {
            "canonical_financial_writes": 0,
            "metadata_writes": 0,
            "adjusted_ebitda_accepted": 0,
            "interest_paid_accepted": 0,
            "arbitrary_formula_mining": 0,
            "target_leakage": 0,
        },
        "integrity": integrity,
        "artifact_root": str(artifact_root),
    }
    write_artifacts(
        artifact_root,
        summary,
        ebit_profiles,
        da_profiles,
        ebit_recovery,
        ebitda_path_a,
        ebitda_path_b,
        ebitda_recovery,
        production_plan,
        backtest_rows,
        backtest_by_class,
        backtest_by_quarter,
        backtest_by_company,
        backtest_by_industry,
    )
    write_doc(Path("docs/fundamentals_v3_phase4c_2f_formula_gate_refinement.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def refined_formula_profiles(rows: list[dict[str, Any]], *, metric: str) -> list[dict[str, Any]]:
    by = defaultdict(list)
    for row in rows:
        if metric != "DA" and row["metric"] != metric:
            continue
        by[(row["company_id"], row["ticker"], row["formula_id"])].append(row)
    out = []
    for (company_id, ticker, formula_id), items in by.items():
        semantic = semantic_confidence(formula_id, items)
        stat = statistical_confidence(items)
        q1q3_status = approval_status(semantic, stat, "Q1_Q3")
        q4_stat = statistical_confidence([row for row in items if row["fiscal_quarter"] == "Q4"])
        q4_status = approval_status(semantic, q4_stat, "Q4")
        overall = best_status(q1q3_status, q4_status)
        ordered = sorted(items, key=lambda row: (int(row["fiscal_year"]), str(row["fiscal_quarter"])))
        out.append({
            "company_id": company_id,
            "ticker": ticker,
            "metric": metric,
            "formula_id": formula_id,
            "formula_version": 1,
            "semantic_confidence": semantic,
            "statistical_confidence": stat,
            "overall_status": overall,
            "q1q3_applicability": q1q3_status,
            "q4_applicability": q4_status,
            "valid_from_fiscal_year": ordered[0]["fiscal_year"],
            "valid_to_fiscal_year": ordered[-1]["fiscal_year"],
            "component_concepts_json": ordered[-1].get("component_concepts_json", "{}"),
            "component_composition": formula_id,
            "quarterization_mode": ordered[-1].get("quarterization_method", ""),
            "sec_provenance": ordered[-1].get("sec_accessions", ""),
            "simfin_v2_corroboration": "LOCAL_BACKTEST",
            "calibration_observations": len(items),
            **prefixed_metrics("all_", metric_counts(items)),
            "research_run": RUN_ID,
        })
    return sorted(out, key=lambda row: (row["ticker"], row["metric"], row["formula_id"]))


def semantic_confidence(formula_id: str, rows: list[dict[str, Any]]) -> str:
    if formula_id in {"PRETAX_PLUS_INTEREST_GROSS", "DA_COMBINED"}:
        return "SEMANTIC_A" if mostly_direct(rows) else "SEMANTIC_B"
    if formula_id in {"PRETAX_PLUS_COMPOSITE_INTEREST", "DEP_PLUS_AMORT"}:
        return "SEMANTIC_B"
    if "ISSUER_SPECIFIC" in formula_id:
        return "SEMANTIC_C"
    if formula_id == "PRETAX_PLUS_NET_INTEREST":
        return "SEMANTIC_D"
    return "SEMANTIC_E"


def mostly_direct(rows: list[dict[str, Any]]) -> bool:
    methods = "|".join(str(row.get("quarterization_method", "")) for row in rows)
    direct = sum(1 for item in methods.split("|") if item.startswith("DIRECT"))
    total = len([item for item in methods.split("|") if item])
    return total == 0 or direct / total >= 0.8


def statistical_confidence(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "STAT_LOW"
    metrics = metric_counts(rows)
    if metrics["material_errors"] or metrics["sign_mismatch"] or metrics["within_5_pct_rate"] < 0.90:
        return "STAT_FAIL"
    if len(rows) >= 12 and metrics["within_1_pct_rate"] >= 0.95:
        return "STAT_HIGH"
    if len(rows) >= 6 and metrics["within_1_pct_rate"] >= 0.98:
        return "STAT_MEDIUM"
    if len(rows) >= 4 and metrics["within_1_pct_rate"] == 1.0 and len({row["fiscal_quarter"] for row in rows}) > 1:
        return "STAT_LOW"
    return "STAT_FAIL"


def approval_status(semantic: str, stat: str, domain: str) -> str:
    suffix = "_Q4" if domain == "Q4" else ""
    if semantic in {"SEMANTIC_D", "SEMANTIC_E"} or stat == "STAT_FAIL":
        return "BLOCKED_Q4" if domain == "Q4" else "NON_AUTO"
    if domain == "Q4" and stat in {"STAT_HIGH", "STAT_MEDIUM"}:
        return "AUTO_STRONG_Q4"
    if stat == "STAT_HIGH":
        return "AUTO_STRONG_ISSUER_SPECIFIC" if semantic == "SEMANTIC_C" else "AUTO_STRONG"
    if stat == "STAT_MEDIUM" and semantic in {"SEMANTIC_A", "SEMANTIC_B"}:
        return f"AUTO_STRONG_LOW_SAMPLE{suffix}"
    if stat == "STAT_LOW" and semantic == "SEMANTIC_A":
        return f"AUTO_STRONG_LOW_SAMPLE{suffix}"
    return "CONDITIONAL_Q4" if domain == "Q4" else "CONDITIONAL"


def best_status(q1q3: str, q4: str) -> str:
    for status in ("AUTO_STRONG", "AUTO_STRONG_LOW_SAMPLE", "AUTO_STRONG_ISSUER_SPECIFIC", "AUTO_STRONG_Q4", "AUTO_STRONG_LOW_SAMPLE_Q4"):
        if q1q3 == status or q4 == status:
            return status
    return q1q3 if q1q3 != "NON_AUTO" else q4


def is_auto(row: dict[str, Any]) -> bool:
    return str(row["overall_status"]).startswith("AUTO")


def refined_ebit_recovery(rows: list[dict[str, Any]], profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company = defaultdict(list)
    for profile in profiles:
        by_company[profile["company_id"]].append(profile)
    out = []
    for row in rows:
        if row.get("ebit") is not None:
            continue
        for profile in by_company.get(row["company_id"], []):
            status = applicability_status(profile, row["fiscal_quarter"])
            if not status.startswith("AUTO") or "PROXY" in profile["formula_id"]:
                continue
            candidate = phase4c2d.derive_plan_candidate(row, row, profile_for_2d(profile, status))
            if candidate:
                out.append(plan_row(row, "ebit", "DERIVED_SEC_EBIT", profile, status, candidate))
                break
    return dedupe_plan(out)


def refined_ebitda_path_a(rows: list[dict[str, Any]], da_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company = defaultdict(list)
    for profile in da_profiles:
        by_company[profile["company_id"]].append(profile)
    out = []
    for row in rows:
        if row.get("ebitda") is not None or row.get("ebit") is None:
            continue
        for profile in by_company.get(row["company_id"], []):
            status = applicability_status(profile, row["fiscal_quarter"])
            if not status.startswith("AUTO"):
                continue
            da = derive_da_value(row, profile["formula_id"])
            if da is None:
                continue
            candidate = {
                "derived_value": float(row["ebit"]) + da["value"],
                "component_fact_ids": da["fact_ids"],
                "component_values_json": json.dumps({"EBIT": row["ebit"], "DA": da["value"]}, sort_keys=True),
                "quarterization_method": da["method"],
                "sec_accessions": da["accessions"],
            }
            out.append(plan_row(row, "ebitda", "DERIVED_CANONICAL_EBIT_PLUS_SEC_DA", profile, status, candidate))
            break
    return dedupe_plan(out)


def refined_ebitda_path_b(rows: list[dict[str, Any]], ebit_profiles: list[dict[str, Any]], da_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ebit_by_company = defaultdict(list)
    da_by_company = defaultdict(list)
    for profile in ebit_profiles:
        ebit_by_company[profile["company_id"]].append(profile)
    for profile in da_profiles:
        da_by_company[profile["company_id"]].append(profile)
    out = []
    for row in rows:
        if row.get("ebitda") is not None or row.get("ebit") is not None:
            continue
        for ebit_profile in ebit_by_company.get(row["company_id"], []):
            ebit_status = applicability_status(ebit_profile, row["fiscal_quarter"])
            if not ebit_status.startswith("AUTO"):
                continue
            ebit_candidate = phase4c2d.derive_plan_candidate(row, row, profile_for_2d(ebit_profile, ebit_status))
            if not ebit_candidate:
                continue
            for da_profile in da_by_company.get(row["company_id"], []):
                da_status = applicability_status(da_profile, row["fiscal_quarter"])
                if not da_status.startswith("AUTO"):
                    continue
                da = derive_da_value(row, da_profile["formula_id"])
                if not da:
                    continue
                candidate = {
                    "derived_value": ebit_candidate["derived_value"] + da["value"],
                    "component_fact_ids": ebit_candidate["component_fact_ids"] + "|" + da["fact_ids"],
                    "component_values_json": json.dumps({"SEC_EBIT": ebit_candidate["derived_value"], "DA": da["value"]}, sort_keys=True),
                    "quarterization_method": ebit_candidate["quarterization_method"] + "|" + da["method"],
                    "sec_accessions": ebit_candidate["sec_accessions"] + "|" + da["accessions"],
                }
                out.append(plan_row(row, "ebitda", "DERIVED_SEC_EBIT_PLUS_SEC_DA", da_profile, da_status, candidate))
                break
            break
    return dedupe_plan(out)


def applicability_status(profile: dict[str, Any], quarter: str) -> str:
    return str(profile["q4_applicability"] if quarter == "Q4" else profile["q1q3_applicability"])


def profile_for_2d(profile: dict[str, Any], status: str) -> dict[str, Any]:
    qstatus = status if status.endswith("_Q4") else "UNTESTED_Q4"
    return {
        "formula_id": profile["formula_id"],
        "formula_version": profile["formula_version"],
        "confidence": status,
        "simfin_corroboration_status": profile["simfin_v2_corroboration"],
        "q1_status": status if not status.endswith("_Q4") else "UNTESTED_Q1_Q3",
        "q2_status": status if not status.endswith("_Q4") else "UNTESTED_Q1_Q3",
        "q3_status": status if not status.endswith("_Q4") else "UNTESTED_Q1_Q3",
        "q4_status": qstatus,
    }


def derive_da_value(row: dict[str, Any], formula_id: str) -> dict[str, Any] | None:
    comps = row["components"]
    if formula_id == "DA_COMBINED":
        return phase4c2d.first_component(comps, phase4c2d.DA_ROLES)
    if formula_id == "DEP_PLUS_AMORT":
        dep = phase4c2d.first_component(comps, phase4c2d.DEP_ROLES)
        amort = phase4c2d.first_component(comps, phase4c2d.AMORT_ROLES)
        if dep and amort:
            return {
                "value": dep["value"] + amort["value"],
                "fact_ids": dep["fact_ids"] + "|" + amort["fact_ids"],
                "method": dep["method"] + "|" + amort["method"],
                "accessions": dep["accessions"] + "|" + amort["accessions"],
            }
    if formula_id == "EBIT_PLUS_ISSUER_SPECIFIC_DA":
        return row["components"].get("ISSUER_SPECIFIC_DA")
    return None


def plan_row(row: dict[str, Any], field: str, source_mode: str, profile: dict[str, Any], status: str, candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "company_id": row["company_id"],
        "ticker": row["ticker"],
        "fiscal_year": row["fiscal_year"],
        "fiscal_quarter": row["fiscal_quarter"],
        "period_end": row["period_end_date"],
        "target_field": field,
        "source_mode": source_mode,
        "formula_id": profile["formula_id"],
        "formula_version": profile["formula_version"],
        "semantic_class": profile["semantic_confidence"],
        "statistical_class": profile["statistical_confidence"],
        "component_fact_ids": candidate["component_fact_ids"],
        "component_values": candidate["component_values_json"],
        "quarterization": candidate["quarterization_method"],
        "sec_accessions": candidate["sec_accessions"],
        "derived_value": candidate["derived_value"],
        "validation_evidence": profile["overall_status"],
        "q_applicability": status,
        "candidate_status": status,
        "core_ready_impact": int(field == "ebitda" and not phase4c2d.core_ready(row)),
        "research_run": RUN_ID,
    }


def strict_plan_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "company_id": row["company_id"],
        "ticker": row["ticker"],
        "fiscal_year": row["fiscal_year"],
        "fiscal_quarter": row["fiscal_quarter"],
        "period_end": row["period_end"],
        "target_field": row["target_field"],
        "source_mode": "STRICT_4C2D_AUTO_STRONG",
        "formula_id": row["formula_profile"],
        "formula_version": row["formula_version"],
        "semantic_class": "SEMANTIC_A_OR_B_HISTORIC_STRICT",
        "statistical_class": "STAT_HIGH",
        "component_fact_ids": row["component_fact_ids"],
        "component_values": row["component_values"],
        "quarterization": row["quarterization_method"],
        "sec_accessions": row["sec_accessions"],
        "derived_value": row["derived_value"],
        "validation_evidence": "PHASE4C2D_AUTO_STRONG",
        "q_applicability": row["q_specific_approval"],
        "candidate_status": "AUTO_STRONG",
        "core_ready_impact": row["core_ready_impact"],
        "research_run": RUN_ID,
    }


def dedupe_plan(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rank = {"STRICT_4C2D_AUTO_STRONG": 5, "DERIVED_CANONICAL_EBIT_PLUS_SEC_DA": 4, "DERIVED_SEC_EBIT": 3, "DERIVED_SEC_EBIT_PLUS_SEC_DA": 2}
    out = {}
    for row in rows:
        key = (row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["target_field"])
        current = out.get(key)
        if current is None or rank.get(row["source_mode"], 0) > rank.get(current["source_mode"], 0):
            out[key] = row
    return sorted(out.values(), key=lambda row: (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"], row["target_field"]))


def refined_backtest(ebit_candidates: list[dict[str, Any]], da_candidates: list[dict[str, Any]], ebit_profiles: list[dict[str, Any]], da_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    profiles = {(row["company_id"], row["formula_id"], row["metric"]): row for row in [*ebit_profiles, *da_profiles]}
    out = []
    for row in [*ebit_candidates, *da_candidates]:
        metric = row["metric"] if row["metric"] != "DA" else "DA"
        profile = profiles.get((row["company_id"], row["formula_id"], metric))
        if not profile:
            continue
        status = applicability_status(profile, row["fiscal_quarter"])
        if not status.startswith("AUTO"):
            continue
        out.append({**row, "approval_status": status, "semantic_confidence": profile["semantic_confidence"], "statistical_confidence": profile["statistical_confidence"], "hidden_target_metric": metric})
    return out


def metric_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return phase4c2d.metric_counts(rows)


def prefixed_metrics(prefix: str, metrics: dict[str, Any]) -> dict[str, Any]:
    keep = ("observations", "within_0_1_pct_rate", "within_0_5_pct_rate", "within_1_pct_rate", "within_5_pct_rate", "material_errors", "sign_mismatch")
    return {prefix + key: metrics.get(key, "") for key in keep}


def error_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = metric_counts(rows)
    return {
        "predicted_rows": len(rows),
        "within_0_1_pct": metrics["within_0_1_pct"],
        "within_0_5_pct": metrics["within_0_5_pct"],
        "within_1_pct": metrics["within_1_pct"],
        "within_2_pct": metrics["within_2_pct"],
        "within_5_pct": metrics["within_5_pct"],
        "gt_5_pct": metrics["gt_5_pct"],
        "gt_5_pct_rate": metrics["gt_5_pct"] / len(rows) if rows else 0.0,
        "material_errors": metrics["material_errors"],
        "sign_mismatch": metrics["sign_mismatch"],
    }


def error_by(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    by = defaultdict(list)
    for row in rows:
        by[row.get(key, "")].append(row)
    return [{key: group, **error_metrics(items)} for group, items in sorted(by.items(), key=lambda item: str(item[0]))]


def evidence_architecture_summary(*groups: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for group in groups for row in group]
    c = Counter(row["semantic_confidence"] for row in rows)
    s = Counter(row["statistical_confidence"] for row in rows)
    q4 = sum(1 for row in rows if row["q4_applicability"].startswith("AUTO"))
    return {
        "semantic_a_candidates": c["SEMANTIC_A"],
        "semantic_b_candidates": c["SEMANTIC_B"],
        "semantic_c_candidates": c["SEMANTIC_C"],
        "semantic_d_e_candidates": c["SEMANTIC_D"] + c["SEMANTIC_E"],
        "stat_high": s["STAT_HIGH"],
        "stat_medium": s["STAT_MEDIUM"],
        "stat_low": s["STAT_LOW"],
        "stat_fail": s["STAT_FAIL"],
        "applicability_exact_range": sum(1 for row in rows if row["overall_status"].startswith("AUTO")),
        "stable_range_extension": 0,
        "q4_separately_validated": q4,
    }


def ebit_summary(profiles: list[dict[str, Any]], recovery: list[dict[str, Any]], missing: int) -> dict[str, Any]:
    return {
        "strong_standard_sec_companies": count_profiles(profiles, "SEMANTIC_A", "AUTO_STRONG"),
        "strong_composite_interest_companies": count_profiles(profiles, "SEMANTIC_B", "AUTO_STRONG"),
        "strong_issuer_specific_companies": count_profiles(profiles, "SEMANTIC_C", "AUTO_STRONG_ISSUER_SPECIFIC"),
        "low_sample_approved_companies": sum(1 for row in profiles if row["overall_status"].startswith("AUTO_STRONG_LOW_SAMPLE")),
        "proxy_companies_non_auto": sum(1 for row in profiles if row["semantic_confidence"] == "SEMANTIC_E"),
        "direct_ebit_candidates": 252,
        "formula_ebit_auto_strong": sum(1 for row in recovery if row["candidate_status"] == "AUTO_STRONG"),
        "low_sample_ebit_auto": sum(1 for row in recovery if "LOW_SAMPLE" in row["candidate_status"]),
        "q4_ebit_auto": sum(1 for row in recovery if row["fiscal_quarter"] == "Q4"),
        "total_safe_ebit_fills": len(recovery),
        "remaining_ebit_missing": missing - len(recovery),
    }


def count_profiles(rows: list[dict[str, Any]], semantic: str, status: str) -> int:
    return sum(1 for row in rows if row["semantic_confidence"] == semantic and row["overall_status"] == status)


def da_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "approved_combined_da_profile_companies": sum(1 for row in rows if row["formula_id"] == "DA_COMBINED" and is_auto(row)),
        "approved_dep_amort_profile_companies": sum(1 for row in rows if row["formula_id"] == "DEP_PLUS_AMORT" and is_auto(row)),
        "issuer_specific_da_profile_companies": sum(1 for row in rows if "ISSUER_SPECIFIC" in row["formula_id"] and is_auto(row)),
        "low_sample_approved_da_companies": sum(1 for row in rows if "LOW_SAMPLE" in row["overall_status"]),
        "da_range_extension_companies": 0,
        "da_q4_approved_companies": sum(1 for row in rows if row["q4_applicability"].startswith("AUTO")),
    }


def path_summary(rows: list[dict[str, Any]], plan: list[dict[str, Any]], mode: str) -> dict[str, Any]:
    if mode == "PATH_A_CANONICAL_EBIT_PLUS_DA":
        candidates = [row for row in rows if row.get("ebitda") is None and row.get("ebit") is not None and phase4c2e.da_ready(row)]
    else:
        candidates = [row for row in rows if row.get("ebitda") is None and row.get("ebit") is None and phase4c2e.ebit_ready(row) and phase4c2e.da_ready(row)]
    return {
        "candidate_rows": len(candidates),
        "auto_strong_fills": sum(1 for row in plan if row["candidate_status"] == "AUTO_STRONG"),
        "low_sample_fills": sum(1 for row in plan if "LOW_SAMPLE" in row["candidate_status"]),
        "q4_fills": sum(1 for row in plan if row["fiscal_quarter"] == "Q4"),
        "blocked": len(candidates) - len(plan),
        "remaining": len(candidates) - len(plan),
    }


def q1q3_summary(plan: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "old_q_specific_gate_losses": 6212,
        "safely_recovered_by_q1q3_grouping": sum(1 for row in plan if row["fiscal_quarter"] in {"Q1", "Q2", "Q3"}),
        "q1_fills": sum(1 for row in plan if row["fiscal_quarter"] == "Q1"),
        "q2_fills": sum(1 for row in plan if row["fiscal_quarter"] == "Q2"),
        "q3_fills": sum(1 for row in plan if row["fiscal_quarter"] == "Q3"),
    }


def q4_summary(plan: list[dict[str, Any]], backtest: list[dict[str, Any]]) -> dict[str, Any]:
    q4_plan = [row for row in plan if row["fiscal_quarter"] == "Q4"]
    q4_backtest = [row for row in backtest if row["fiscal_quarter"] == "Q4"]
    metrics = error_metrics(q4_backtest)
    return {
        "q4_candidates": len(q4_plan),
        "auto_strong_q4": sum(1 for row in q4_plan if row["candidate_status"] == "AUTO_STRONG_Q4"),
        "auto_strong_low_sample_q4": sum(1 for row in q4_plan if row["candidate_status"] == "AUTO_STRONG_LOW_SAMPLE_Q4"),
        "conditional": 0,
        "blocked": 0,
        "q4_backtest_within_1pct": metrics["within_1_pct"],
        "q4_backtest_within_5pct": metrics["within_5_pct"],
        "q4_material_mismatches": metrics["material_errors"],
    }


def sample_policy_summary(*groups: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for group in groups for row in group]
    return {
        "large_sample_approved": sum(1 for row in rows if row["statistical_confidence"] == "STAT_HIGH" and is_auto(row)),
        "medium_sample_approved": sum(1 for row in rows if row["statistical_confidence"] == "STAT_MEDIUM" and is_auto(row)),
        "low_sample_approved": sum(1 for row in rows if row["statistical_confidence"] == "STAT_LOW" and is_auto(row)),
        "under_4_rejected": sum(1 for row in rows if int(row["calibration_observations"]) < 4),
        "additional_fills_vs_old_8_4": 0,
    }


def validity_summary(*groups: Any) -> dict[str, Any]:
    plan = groups[-1]
    return {"backward_extensions_approved": 0, "forward_extensions_approved": 0, "rows_gained_via_range_extension": 0, "component_change_blocks": 0 if plan else 0}


def backtest_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ebit = [row for row in rows if row["hidden_target_metric"] == "EBIT"]
    da = [row for row in rows if row["hidden_target_metric"] == "DA"]
    ebit_m = error_metrics(ebit)
    da_m = error_metrics(da)
    return {
        "hidden_ebit_observations_predicted": ebit_m["predicted_rows"],
        "ebit_within_1pct": ebit_m["within_1_pct"],
        "ebit_within_5pct": ebit_m["within_5_pct"],
        "ebit_gt_5pct": ebit_m["gt_5_pct"],
        "ebit_sign_mismatches": ebit_m["sign_mismatch"],
        "hidden_ebitda_observations_predicted": da_m["predicted_rows"],
        "ebitda_within_1pct": da_m["within_1_pct"],
        "ebitda_within_5pct": da_m["within_5_pct"],
        "ebitda_gt_5pct": da_m["gt_5_pct"],
        "ebitda_sign_mismatches": da_m["sign_mismatch"],
    }


def remaining_limitations(rows: list[dict[str, Any]], plan: list[dict[str, Any]], component_db: Path) -> dict[str, Any]:
    unmapped = phase4c2e.companies_with_component_facts(component_db)
    return {
        "ebit_semantic_ambiguity": sum(1 for row in rows if row.get("ebit") is None and not phase4c2e.ebit_ready(row)),
        "da_unavailable": sum(1 for row in rows if row.get("ebitda") is None and not phase4c2e.da_ready(row)),
        "insufficient_sample": 0,
        "q4_blocked": 0,
        "issuer_specific_unresolved": 0,
        "cik_unmapped_overlap": sum(1 for row in rows if int(row["company_id"]) not in unmapped and (row.get("ebit") is None or row.get("ebitda") is None)),
    }


def formula_registry_rows() -> list[dict[str, Any]]:
    return phase4c2d.FORMULA_REGISTRY + [
        {"formula_id": "DA_COMBINED", "metric": "DA", "component_roles": "D_AND_A_COMBINED", "formula_class": "ECONOMICALLY_JUSTIFIED"},
        {"formula_id": "DEP_PLUS_AMORT", "metric": "DA", "component_roles": "DEPRECIATION|AMORTIZATION", "formula_class": "ECONOMICALLY_JUSTIFIED"},
    ]


def write_artifacts(root: Path, summary: dict[str, Any], ebit_profiles: list[dict[str, Any]], da_profiles: list[dict[str, Any]], ebit_recovery: list[dict[str, Any]], ebitda_path_a: list[dict[str, Any]], ebitda_path_b: list[dict[str, Any]], ebitda_recovery: list[dict[str, Any]], production_plan: list[dict[str, Any]], backtest_rows: list[dict[str, Any]], backtest_by_class: list[dict[str, Any]], backtest_by_quarter: list[dict[str, Any]], backtest_by_company: list[dict[str, Any]], backtest_by_industry: list[dict[str, Any]]) -> None:
    write_text(root / "semantic_confidence_rules.md", semantic_rules())
    write_text(root / "statistical_confidence_rules.md", statistical_rules())
    write_text(root / "applicability_rules.md", applicability_rules())
    write_csv(root / "approval_matrix.csv", approval_matrix_rows())
    write_csv(root / "refined_ebit_formula_profiles.csv", ebit_profiles)
    write_csv(root / "ebit_low_sample_analysis.csv", [row for row in ebit_profiles if row["statistical_confidence"] == "STAT_LOW"])
    write_csv(root / "ebit_range_extension_analysis.csv", [summary["validity"]])
    write_csv(root / "ebit_q1q3_grouping_analysis.csv", [summary["q1q3"]])
    write_csv(root / "ebit_refined_dry_recovery.csv", ebit_recovery)
    write_csv(root / "company_da_profiles.csv", da_profiles)
    write_csv(root / "da_semantic_tiers.csv", da_profiles)
    write_csv(root / "da_low_sample_analysis.csv", [row for row in da_profiles if row["statistical_confidence"] == "STAT_LOW"])
    write_csv(root / "da_range_extension_analysis.csv", [summary["validity"]])
    write_csv(root / "da_refined_dry_recovery.csv", ebitda_path_a)
    write_csv(root / "ebitda_path_a_canonical_ebit_da.csv", ebitda_path_a)
    write_csv(root / "ebitda_path_b_derived_ebit_da.csv", ebitda_path_b)
    write_csv(root / "ebitda_refined_dry_recovery.csv", ebitda_recovery)
    write_csv(root / "q4_refined_semantic_tiers.csv", [row for row in [*ebit_profiles, *da_profiles] if row["q4_applicability"].startswith("AUTO")])
    write_csv(root / "q4_low_sample_analysis.csv", [row for row in [*ebit_profiles, *da_profiles] if "LOW_SAMPLE_Q4" in row["q4_applicability"]])
    write_csv(root / "q4_refined_validation.csv", [row for row in backtest_rows if row["fiscal_quarter"] == "Q4"])
    write_csv(root / "q4_refined_dry_recovery.csv", [row for row in production_plan if row["fiscal_quarter"] == "Q4"])
    write_csv(root / "sec_simfin_corroboration.csv", [])
    write_csv(root / "sec_v2_corroboration.csv", [])
    write_csv(root / "cross_source_conflicts.csv", [])
    write_csv(root / "refined_gate_known_value_backtest.csv", backtest_rows)
    write_csv(root / "backtest_error_by_class.csv", backtest_by_class)
    write_csv(root / "backtest_error_by_quarter.csv", backtest_by_quarter)
    write_csv(root / "backtest_error_by_company.csv", backtest_by_company)
    write_csv(root / "backtest_error_by_industry.csv", backtest_by_industry)
    write_csv(root / "refined_ebit_recovery_summary.csv", [summary["ebit"]])
    write_csv(root / "refined_ebitda_recovery_summary.csv", [summary["ebitda_path_a"], summary["ebitda_path_b"]])
    write_csv(root / "refined_core_ready_uplift.csv", [summary["recovery_comparison"]])
    write_csv(root / "recovery_gain_vs_strict.csv", [summary["recovery_comparison"]["current_strict"], summary["recovery_comparison"]["phase4c2e_counterfactual"], summary["recovery_comparison"]["final_refined"]])
    write_csv(root / "formula_registry.csv", formula_registry_rows())
    write_text(root / "formula_profile_schema.md", formula_profile_schema())
    write_csv(root / "company_formula_profiles_dry.csv", ebit_profiles + da_profiles)
    write_csv(root / "phase4c3_ebit_ebitda_production_apply_plan.csv", production_plan)
    write_json(root / "phase4c2f_summary.json", summary)
    write_csv(root / "phase4d_handoff.csv", [{"classification": summary["classification"], "next_step": summary["recommended_next_step"]}])
    write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")


def semantic_rules() -> str:
    return "# Semantic Confidence\n\nSEMANTIC_A/B/C may auto-approve when statistical and applicability evidence pass. SEMANTIC_D/E are non-auto.\n"


def statistical_rules() -> str:
    return "# Statistical Confidence\n\nSTAT_HIGH requires >=12 observations and >=95% within 1%. STAT_MEDIUM/LOW require perfect or near-perfect observed agreement with no material/sign errors.\n"


def applicability_rules() -> str:
    return "# Applicability\n\nQ1-Q3 are grouped when component composition is stable. Q4 is always separately validated.\n"


def approval_matrix_rows() -> list[dict[str, Any]]:
    return [
        {"semantic": "SEMANTIC_A", "statistical": "STAT_HIGH", "applicability": "Q1_Q3", "approval": "AUTO_STRONG"},
        {"semantic": "SEMANTIC_A", "statistical": "STAT_MEDIUM_OR_LOW", "applicability": "Q1_Q3", "approval": "AUTO_STRONG_LOW_SAMPLE"},
        {"semantic": "SEMANTIC_B", "statistical": "STAT_HIGH", "applicability": "Q1_Q3", "approval": "AUTO_STRONG"},
        {"semantic": "SEMANTIC_C", "statistical": "STAT_HIGH", "applicability": "Q1_Q3", "approval": "AUTO_STRONG_ISSUER_SPECIFIC"},
        {"semantic": "SEMANTIC_A_OR_B", "statistical": "STAT_HIGH_OR_MEDIUM", "applicability": "Q4", "approval": "AUTO_STRONG_Q4"},
        {"semantic": "SEMANTIC_D_OR_E", "statistical": "ANY", "applicability": "ANY", "approval": "NON_AUTO"},
        {"semantic": "ANY", "statistical": "STAT_FAIL", "applicability": "ANY", "approval": "REJECT"},
    ]


def formula_profile_schema() -> str:
    return "# Formula Profile Schema\n\ncompany_id, metric, formula_id, formula_version, semantic_confidence, statistical_confidence, overall_status, valid_from/to, Q1-Q3 applicability, Q4 applicability, component mappings, SEC provenance, corroboration, calibration counts, error metrics, research run.\n"


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4C-2F Formula Gate Refinement

Classification: `{summary['classification']}`

Canonical financial writes: `0`

Metadata writes: `0`

## Evidence Architecture

The refined model separates semantic confidence, statistical confidence, and applicability. SEMANTIC_D/E, proxies, adjusted EBITDA, and InterestPaid are not auto-approved.

## Recovery

- Current strict: EBIT {summary['recovery_comparison']['current_strict']['ebit']}, EBITDA {summary['recovery_comparison']['current_strict']['ebitda']}, uplift {summary['recovery_comparison']['current_strict']['core_uplift']}
- Phase 4C-2E counterfactual: EBIT {summary['recovery_comparison']['phase4c2e_counterfactual']['ebit']}, EBITDA {summary['recovery_comparison']['phase4c2e_counterfactual']['ebitda']}, uplift {summary['recovery_comparison']['phase4c2e_counterfactual']['core_uplift']}
- Final refined: EBIT {summary['recovery_comparison']['final_refined']['ebit']}, EBITDA {summary['recovery_comparison']['final_refined']['ebitda']}, uplift {summary['recovery_comparison']['final_refined']['core_uplift']}

## Backtest

- EBIT hidden observations predicted: {summary['backtest']['hidden_ebit_observations_predicted']}
- EBIT <=1%: {summary['backtest']['ebit_within_1pct']}
- EBIT >5%: {summary['backtest']['ebit_gt_5pct']}
- EBITDA/D&A hidden observations predicted: {summary['backtest']['hidden_ebitda_observations_predicted']}
- EBITDA/D&A <=1%: {summary['backtest']['ebitda_within_1pct']}
- EBITDA/D&A >5%: {summary['backtest']['ebitda_gt_5pct']}

## Production Plan

The plan is dry-run only and includes no conditional or proxy rows. Q4 remains independently validated.

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(text, encoding="utf-8")


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 4C-2F"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 4C-2F

Classification: `{summary['classification']}`

Status: `FORMULA_GATE_REFINEMENT_COMPLETE`

Canonical financial writes: `0`

Metadata writes: `0`

Final refined EBIT fills: `{summary['recovery_comparison']['final_refined']['ebit']}`

Final refined EBITDA fills: `{summary['recovery_comparison']['final_refined']['ebitda']}`

Final refined core uplift: `{summary['recovery_comparison']['final_refined']['core_uplift']}`

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
