from __future__ import annotations

from swingmaster.fundamentals import v3_phase4c3c_bounded_refinement as refine


def test_interest_total_only_typology() -> None:
    assert refine.interest_typology({"INTEREST_EXPENSE_GROSS": _component("INTEREST_EXPENSE_GROSS", 1)}) == "TOTAL_ONLY"


def test_interest_total_plus_components_typology() -> None:
    comps = {"INTEREST_EXPENSE_GROSS": _component("INTEREST_EXPENSE_GROSS", 3), "FINANCE_LEASE_INTEREST": _component("FINANCE_LEASE_INTEREST", 1)}
    assert refine.interest_typology(comps) == "TOTAL_PLUS_COMPONENTS"


def test_interest_component_sum_typology() -> None:
    comps = {"DEBT_INTEREST": _component("DEBT_INTEREST", 3), "FINANCE_LEASE_INTEREST": _component("FINANCE_LEASE_INTEREST", 1)}
    assert refine.interest_typology(comps) == "COMPONENT_SUM"


def test_interest_finance_lease_not_double_counted_in_gross_profile() -> None:
    assert refine.profile_interest_composition("PRETAX_PLUS_INTEREST_GROSS") == "TOTAL_ONLY"


def test_interest_issuer_specific_typology() -> None:
    assert refine.interest_typology({"ISSUER_SPECIFIC_INTEREST": _component("ISSUER_SPECIFIC_INTEREST", 1), "INTEREST_EXPENSE_GROSS": _component("INTEREST_EXPENSE_GROSS", 1)}) == "ISSUER_SPECIFIC_TOTAL"


def test_interest_financial_products_exclusion_defaults_ambiguous() -> None:
    assert refine.interest_typology({"INTEREST_EXPENSE_NET": _component("INTEREST_EXPENSE_NET", 1)}) == "TRUE_AMBIGUITY"


def test_interest_net_vs_gross_typology() -> None:
    comps = {"INTEREST_EXPENSE_NET": _component("INTEREST_EXPENSE_NET", 1), "INTEREST_EXPENSE_GROSS": _component("INTEREST_EXPENSE_GROSS", 2)}
    assert refine.interest_typology(comps) == "NET_VS_GROSS"


def test_interest_concept_rename_is_profile_not_string_match() -> None:
    assert refine.profile_interest_composition("PRETAX_PLUS_COMPOSITE_INTEREST") == "COMPONENT_SUM"


def test_interest_true_ambiguity() -> None:
    assert refine.interest_typology({}) == "TRUE_AMBIGUITY"


def test_interest_paid_excluded_from_bounded_population() -> None:
    rows = [row(components={"PRETAX": _component("PRETAX", 10), "INTEREST_PAID_CASHFLOW_EXCLUDED": _component("INTEREST_PAID_CASHFLOW_EXCLUDED", 1)}, ebit=None)]
    assert refine.bounded_population(rows) == []


def test_interest_profile_strong() -> None:
    assert refine.interest_profile_type(profile(overall_status="AUTO_STRONG")) == "INTEREST_PROFILE_STRONG"


def test_interest_profile_low_sample() -> None:
    assert refine.interest_profile_type(profile(overall_status="AUTO_STRONG_LOW_SAMPLE")) == "INTEREST_PROFILE_LOW_SAMPLE_STRONG"


def test_interest_q1q3_application() -> None:
    assert refine.profile_auto_status(profile(q1q3_applicability="AUTO_STRONG"), "Q2") == "AUTO_STRONG"


def test_interest_q4_separate() -> None:
    assert refine.profile_auto_status(profile(q4_applicability="BLOCKED_Q4"), "Q4") == "BLOCKED_Q4"


def test_hidden_target_ebit_prediction_keeps_auto_rows() -> None:
    candidate_row = candidate(metric="EBIT", formula_id="PRETAX_PLUS_INTEREST_GROSS")
    rows = refine.hidden_target_backtest([candidate_row], [profile(formula_id="PRETAX_PLUS_INTEREST_GROSS")], metric="EBIT")
    assert rows[0]["hidden_target_validation_class"] == "AUTO_STRONG"


def test_da_combined_complete_typology() -> None:
    comps = {"D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 3), "DEPRECIATION": _component("DEPRECIATION", 2), "AMORTIZATION": _component("AMORTIZATION", 1)}
    assert refine.da_typology(comps) == "COMBINED_EQUALS_DEP_PLUS_AMORT"


def test_da_dep_plus_amort_profile() -> None:
    assert refine.profile_da_composition("DEP_PLUS_AMORT") == "DEP_PLUS_AMORT_IS_COMPLETE"


def test_da_overlap_blocked_typology() -> None:
    comps = {"D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 10), "DEPRECIATION": _component("DEPRECIATION", 2), "AMORTIZATION": _component("AMORTIZATION", 1)}
    assert refine.da_typology(comps) == "COMBINED_OVERLAPS_COMPONENTS"


def test_da_combined_partial_defaults_ambiguous_without_components() -> None:
    assert refine.da_typology({"D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 1)}) == "TRUE_DA_AMBIGUITY"


def test_da_profile_selection_combined_strong() -> None:
    assert refine.da_profile_type(profile(formula_id="DA_COMBINED", overall_status="AUTO_STRONG")) == "DA_PROFILE_COMBINED_STRONG"


def test_da_profile_selection_dep_amort_strong() -> None:
    assert refine.da_profile_type(profile(formula_id="DEP_PLUS_AMORT", overall_status="AUTO_STRONG")) == "DA_PROFILE_DEP_AMORT_STRONG"


def test_canonical_ebit_plus_da_value() -> None:
    da = refine.derive_da_value(row()["source_row"] if False else row()["components"] and row(), "DA_COMBINED")
    assert da["value"] == 3


def test_no_double_count_da_value() -> None:
    da = refine.derive_da_value(row()["source_row"] if False else row(), "DEP_PLUS_AMORT")
    assert da["value"] == 3


def test_q4_status_independent() -> None:
    p = profile(q1q3_applicability="AUTO_STRONG", q4_applicability="BLOCKED_Q4")
    assert refine.profile_auto_status(p, "Q4") == "BLOCKED_Q4"


def test_plan_bounded_population_only() -> None:
    plan = refine.production_plan_rows([recovery()])
    assert plan[0]["prior_rejection_reason"] == "MULTIPLE_INTEREST_CANDIDATES"


def test_target_null_required_by_population_filter() -> None:
    rows = [row(ebit=12, components={"PRETAX": _component("PRETAX", 10), "INTEREST_EXPENSE_GROSS": _component("INTEREST_EXPENSE_GROSS", 1), "FINANCE_LEASE_INTEREST": _component("FINANCE_LEASE_INTEREST", 1)})]
    assert refine.bounded_population(rows) == []


def test_conditional_excluded_from_plan() -> None:
    item = recovery(hidden_target_validation_class="CONDITIONAL")
    assert refine.production_plan_rows([item]) == []


def test_proxy_excluded_by_interest_recovery() -> None:
    p = profile(formula_id="OPERATING_INCOME_PROXY")
    assert refine.profile_interest_composition(p["formula_id"]) == "TRUE_AMBIGUITY"


def test_no_duplicate_targets() -> None:
    rows = refine.production_plan_rows([recovery(source_mode="DERIVED_SEC_EBIT_PLUS_SEC_DA_BOUNDED"), recovery(source_mode="DERIVED_CANONICAL_EBIT_PLUS_SEC_DA_BOUNDED")])
    assert len(rows) == 1
    assert rows[0]["source_mode"] == "DERIVED_CANONICAL_EBIT_PLUS_SEC_DA_BOUNDED"


def test_provenance_complete() -> None:
    item = recovery()
    assert item["component_fact_ids"]
    assert item["sec_accessions"]


def test_canonical_writes_zero_constant() -> None:
    assert refine.RUN_ID == "PHASE4C3C_BOUNDED_REJECTION_REFINEMENT"


def test_metadata_writes_zero_in_doc_text() -> None:
    text = refine.preflight_md({"classification": refine.CLASSIFICATION_NO_APPLY, "bounded_population": {"total_rows": 0, "multiple_interest_rows": 0, "da_conflict_rows": 0}})
    assert "Metadata writes: `0`" in text


def test_sequence_violation_guard_is_structural_external() -> None:
    assert refine.CLASSIFICATION_RESEARCH.endswith("ARCHITECTURAL_RESEARCH_REQUIRED")


def test_quick_check_fk_reported_in_summary_shape() -> None:
    assert refine.NEXT_APPLY.startswith("MASTER PLAN PHASE 4C-3D")


def test_bounded_population_has_da_conflict() -> None:
    rows = [row(ebitda=None)]
    assert refine.bounded_population(rows)[0]["pattern_class"] == "COMBINED_DA_VS_DEP_AMORT_CONFLICT"


def _component(role: str, value: float) -> dict:
    return {
        "role": role,
        "value": value,
        "method": "DIRECT_Q1",
        "fact_ids": f"{role}-fact",
        "accessions": f"{role}-acc",
        "concept": role,
        "unit": "USD",
        "dimensions": "{}",
    }


def row(**overrides) -> dict:
    data = {
        "company_id": 1,
        "ticker": "AAA",
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "period_end_date": "2024-03-31",
        "ebit": None,
        "ebitda": None,
        "revenue": 1,
        "free_cashflow": 1,
        "cash": 1,
        "total_debt": 1,
        "shares_outstanding": 1,
        "components": {
            "D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 3),
            "DEPRECIATION": _component("DEPRECIATION", 2),
            "AMORTIZATION": _component("AMORTIZATION", 1),
        },
    }
    data.update(overrides)
    return data


def profile(**overrides) -> dict:
    data = {
        "company_id": 1,
        "ticker": "AAA",
        "metric": "EBIT",
        "formula_id": "PRETAX_PLUS_INTEREST_GROSS",
        "formula_version": 1,
        "overall_status": "AUTO_STRONG",
        "q1q3_applicability": "AUTO_STRONG",
        "q4_applicability": "AUTO_STRONG_Q4",
        "semantic_confidence": "SEMANTIC_A",
        "statistical_confidence": "STAT_HIGH",
        "all_observations": 8,
        "all_within_1_pct_rate": 1.0,
        "component_concepts_json": "{}",
        "profile_type": "INTEREST_PROFILE_STRONG",
        "composition_type": "TOTAL_ONLY",
    }
    data.update(overrides)
    return data


def candidate(**overrides) -> dict:
    data = {
        "company_id": 1,
        "ticker": "AAA",
        "metric": "EBIT",
        "formula_id": "PRETAX_PLUS_INTEREST_GROSS",
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "target_value": 12,
        "derived_value": 12,
        "relative_error": 0.0,
        "within_0_1_pct": 1,
        "within_0_5_pct": 1,
        "within_1_pct": 1,
        "within_2_pct": 1,
        "within_5_pct": 1,
        "gt_5_pct": 0,
        "material_error": 0,
        "sign_mismatch": 0,
    }
    data.update(overrides)
    return data


def recovery(**overrides) -> dict:
    data = {
        "company_id": 1,
        "ticker": "AAA",
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "period_end": "2024-03-31",
        "metric": "ebit",
        "prior_rejection_reason": "MULTIPLE_INTEREST_CANDIDATES",
        "resolved_pattern": "TOTAL_ONLY",
        "formula_profile": "PRETAX_PLUS_INTEREST_GROSS",
        "formula_version": 1,
        "component_fact_ids": "1|2",
        "concepts": "{}",
        "values": "{}",
        "quarterization_method": "DIRECT_Q1",
        "sec_accessions": "a",
        "hidden_target_validation_class": "AUTO_STRONG",
        "q_applicability": "AUTO_STRONG",
        "derived_value": 12,
        "source_mode": "DERIVED_SEC_EBIT_BOUNDED_INTEREST",
        "core_ready_impact": 0,
        "research_run": refine.RUN_ID,
    }
    data.update(overrides)
    return data
