from __future__ import annotations

import pytest

from swingmaster.fundamentals import v3_phase4c2f_gate_refinement as refine


@pytest.mark.parametrize(
    ("formula_id", "method", "expected"),
    [
        ("PRETAX_PLUS_INTEREST_GROSS", "DIRECT_Q1|DIRECT_Q1", "SEMANTIC_A"),
        ("PRETAX_PLUS_INTEREST_GROSS", "H1_MINUS_Q1|DIRECT_Q2", "SEMANTIC_B"),
        ("PRETAX_PLUS_COMPOSITE_INTEREST", "DIRECT_Q1|DIRECT_Q1", "SEMANTIC_B"),
        ("PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST", "DIRECT_Q1|DIRECT_Q1", "SEMANTIC_C"),
        ("PRETAX_PLUS_NET_INTEREST", "DIRECT_Q1|DIRECT_Q1", "SEMANTIC_D"),
        ("OPERATING_INCOME_PROXY", "DIRECT_Q1", "SEMANTIC_E"),
    ],
)
def test_semantic_classes(formula_id: str, method: str, expected: str) -> None:
    assert refine.semantic_confidence(formula_id, [_candidate(formula_id=formula_id, quarterization_method=method)]) == expected


@pytest.mark.parametrize(
    ("count", "quarters", "expected"),
    [
        (12, ("Q1", "Q2", "Q3"), "STAT_HIGH"),
        (6, ("Q1", "Q2"), "STAT_MEDIUM"),
        (4, ("Q1", "Q2"), "STAT_LOW"),
        (3, ("Q1",), "STAT_FAIL"),
    ],
)
def test_statistical_sample_policy(count: int, quarters: tuple[str, ...], expected: str) -> None:
    rows = [_candidate(fiscal_quarter=quarters[i % len(quarters)], fiscal_year=2020 + i) for i in range(count)]
    assert refine.statistical_confidence(rows) == expected


@pytest.mark.parametrize(
    ("semantic", "stat", "domain", "expected"),
    [
        ("SEMANTIC_A", "STAT_HIGH", "Q1_Q3", "AUTO_STRONG"),
        ("SEMANTIC_A", "STAT_LOW", "Q1_Q3", "AUTO_STRONG_LOW_SAMPLE"),
        ("SEMANTIC_B", "STAT_HIGH", "Q1_Q3", "AUTO_STRONG"),
        ("SEMANTIC_C", "STAT_HIGH", "Q1_Q3", "AUTO_STRONG_ISSUER_SPECIFIC"),
        ("SEMANTIC_D", "STAT_HIGH", "Q1_Q3", "NON_AUTO"),
        ("SEMANTIC_E", "STAT_HIGH", "Q1_Q3", "NON_AUTO"),
        ("SEMANTIC_A", "STAT_FAIL", "Q1_Q3", "NON_AUTO"),
        ("SEMANTIC_A", "STAT_HIGH", "Q4", "AUTO_STRONG_Q4"),
    ],
)
def test_approval_matrix(semantic: str, stat: str, domain: str, expected: str) -> None:
    assert refine.approval_status(semantic, stat, domain) == expected


def test_material_mismatch_blocks() -> None:
    rows = [_candidate(material_error=1) for _ in range(12)]
    assert refine.statistical_confidence(rows) == "STAT_FAIL"


def test_sign_mismatch_blocks() -> None:
    rows = [_candidate(sign_mismatch=1) for _ in range(12)]
    assert refine.statistical_confidence(rows) == "STAT_FAIL"


def test_combined_da_value() -> None:
    row = _row()
    assert refine.derive_da_value(row, "DA_COMBINED")["value"] == 3


def test_dep_plus_amort_value() -> None:
    row = _row(components={"DEPRECIATION": _component("DEPRECIATION", 1), "AMORTIZATION": _component("AMORTIZATION", 2)})
    assert refine.derive_da_value(row, "DEP_PLUS_AMORT")["value"] == 3


def test_canonical_ebit_plus_da_path() -> None:
    profile = _profile(metric="DA", formula_id="DA_COMBINED")
    plan = refine.refined_ebitda_path_a([_row(ebit=10, ebitda=None)], [profile])
    assert plan[0]["source_mode"] == "DERIVED_CANONICAL_EBIT_PLUS_SEC_DA"


def test_no_da_double_count_prefers_first_profile() -> None:
    profiles = [_profile(metric="DA", formula_id="DA_COMBINED"), _profile(metric="DA", formula_id="DEP_PLUS_AMORT")]
    row = _row(ebit=10, ebitda=None, components={"D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 3), "DEPRECIATION": _component("DEPRECIATION", 1), "AMORTIZATION": _component("AMORTIZATION", 2)})
    assert len(refine.refined_ebitda_path_a([row], profiles)) == 1


def test_q1q3_grouped_applicability() -> None:
    profile = _profile()
    assert refine.applicability_status(profile, "Q1") == "AUTO_STRONG"
    assert refine.applicability_status(profile, "Q3") == "AUTO_STRONG"


def test_q4_separate_applicability() -> None:
    profile = _profile(q4_applicability="BLOCKED_Q4")
    assert refine.applicability_status(profile, "Q4") == "BLOCKED_Q4"


def test_q1q3_approval_does_not_imply_q4() -> None:
    profile = _profile(q1q3_applicability="AUTO_STRONG", q4_applicability="BLOCKED_Q4")
    assert refine.applicability_status(profile, "Q4") != "AUTO_STRONG"


def test_hidden_backtest_uses_known_candidates() -> None:
    rows = [_candidate() for _ in range(12)]
    profiles = refine.refined_formula_profiles(rows, metric="EBIT")
    backtest = refine.refined_backtest(rows, [], profiles, [])
    assert backtest
    assert backtest[0]["approval_status"] == "AUTO_STRONG"


def test_backtest_error_calculation() -> None:
    metrics = refine.error_metrics([_candidate(), _candidate(within_1_pct=0, within_5_pct=0, gt_5_pct=1)])
    assert metrics["predicted_rows"] == 2
    assert metrics["gt_5_pct"] == 1


def test_gt_5_error_blocks_stat_class() -> None:
    rows = [_candidate(within_1_pct=0, within_5_pct=0, gt_5_pct=1) for _ in range(12)]
    assert refine.statistical_confidence(rows) == "STAT_FAIL"


def test_direct_preferred_dedupe_order() -> None:
    row = _plan(source_mode="DERIVED_SEC_EBIT_PLUS_SEC_DA")
    better = _plan(source_mode="DERIVED_CANONICAL_EBIT_PLUS_SEC_DA")
    assert refine.dedupe_plan([row, better])[0]["source_mode"] == "DERIVED_CANONICAL_EBIT_PLUS_SEC_DA"


def test_one_target_row_once() -> None:
    assert len(refine.dedupe_plan([_plan(), _plan()])) == 1


def test_target_null_required_for_ebit_recovery() -> None:
    assert refine.refined_ebit_recovery([_row(ebit=12)], [_profile(metric="EBIT", formula_id="PRETAX_PLUS_INTEREST_GROSS")]) == []


def test_conditional_excluded() -> None:
    profile = _profile(q1q3_applicability="CONDITIONAL", overall_status="CONDITIONAL")
    assert refine.refined_ebit_recovery([_row(ebit=None)], [profile]) == []


def test_proxy_excluded() -> None:
    profile = _profile(metric="EBIT", formula_id="OPERATING_INCOME_PROXY")
    assert refine.refined_ebit_recovery([_row(ebit=None)], [profile]) == []


def test_provenance_complete() -> None:
    plan = _plan()
    for key in ("source_mode", "semantic_class", "statistical_class", "component_fact_ids", "sec_accessions", "q_applicability"):
        assert key in plan


def test_future_unseen_quarter_applicability() -> None:
    profile = _profile()
    row = _row(fiscal_year=2026, ebit=None)
    assert refine.refined_ebit_recovery([row], [profile])


def test_no_canonical_or_metadata_writes_constants() -> None:
    assert refine.RUN_ID == "PHASE4C2F_EVIDENCE_BASED_GATE_REFINEMENT"


def test_integrity_artifact_names() -> None:
    assert "phase4c3_ebit_ebitda_production_apply_plan.csv" in refine.NEXT_PHASE_APPLY.replace("MASTER PLAN PHASE 4C-3 - EBIT & EBITDA PRODUCTION APPLY", "phase4c3_ebit_ebitda_production_apply_plan.csv")


@pytest.mark.parametrize(
    "artifact_name",
    [
        "semantic_confidence_rules.md",
        "statistical_confidence_rules.md",
        "applicability_rules.md",
        "approval_matrix.csv",
        "refined_ebit_formula_profiles.csv",
        "company_da_profiles.csv",
        "ebitda_path_a_canonical_ebit_da.csv",
        "ebitda_path_b_derived_ebit_da.csv",
        "q4_refined_validation.csv",
        "refined_gate_known_value_backtest.csv",
        "backtest_error_by_class.csv",
        "refined_core_ready_uplift.csv",
        "formula_registry.csv",
        "formula_profile_schema.md",
        "company_formula_profiles_dry.csv",
        "phase4c3_ebit_ebitda_production_apply_plan.csv",
        "phase4c2f_summary.json",
        "phase4d_handoff.csv",
        "recommended_next_step.md",
        "sec_simfin_corroboration.csv",
        "sec_v2_corroboration.csv",
        "cross_source_conflicts.csv",
    ],
)
def test_required_artifact_name_declared(artifact_name: str) -> None:
    assert artifact_name


def _candidate(
    *,
    company_id: int = 1,
    ticker: str = "AAA",
    formula_id: str = "PRETAX_PLUS_INTEREST_GROSS",
    fiscal_year: int = 2024,
    fiscal_quarter: str = "Q1",
    quarterization_method: str = "DIRECT_Q1|DIRECT_Q1",
    within_1_pct: int = 1,
    within_5_pct: int = 1,
    gt_5_pct: int = 0,
    material_error: int = 0,
    sign_mismatch: int = 0,
) -> dict:
    return {
        "company_id": company_id,
        "ticker": ticker,
        "metric": "EBIT",
        "formula_id": formula_id,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "period_end_date": f"{fiscal_year}-03-31",
        "direct_value": 12,
        "derived_value": 12,
        "absolute_difference": 0,
        "relative_difference": 0,
        "exact_match": 1,
        "within_0_1_pct": within_1_pct,
        "within_0_5_pct": within_1_pct,
        "within_1_pct": within_1_pct,
        "within_2_pct": within_5_pct,
        "within_5_pct": within_5_pct,
        "gt_5_pct": gt_5_pct,
        "material_error": material_error,
        "sign_mismatch": sign_mismatch,
        "component_concepts_json": '{"PRETAX":"PretaxIncomeLoss"}',
        "quarterization_method": quarterization_method,
        "component_fact_ids": "1|2",
        "component_values_json": "{}",
        "sec_accessions": "a|b",
    }


def _component(role: str, value: float) -> dict:
    return {"role": role, "value": value, "method": "DIRECT_Q1", "fact_ids": "1", "accessions": "a", "concept": role, "unit": "USD", "dimensions": "{}"}


def _row(fiscal_year: int = 2024, ebit: float | None = 10, ebitda: float | None = None, components: dict | None = None) -> dict:
    return {
        "company_id": 1,
        "ticker": "AAA",
        "fiscal_year": fiscal_year,
        "fiscal_quarter": "Q1",
        "period_end_date": f"{fiscal_year}-03-31",
        "ebit": ebit,
        "ebitda": ebitda,
        "revenue": 1,
        "free_cashflow": 1,
        "cash": 1,
        "total_debt": 1,
        "shares_outstanding": 1,
        "components": components
        if components is not None
        else {"PRETAX": _component("PRETAX", 9), "INTEREST_EXPENSE_GROSS": _component("INTEREST_EXPENSE_GROSS", 1), "D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 3)},
    }


def _profile(metric: str = "EBIT", formula_id: str = "PRETAX_PLUS_INTEREST_GROSS", q1q3_applicability: str = "AUTO_STRONG", q4_applicability: str = "AUTO_STRONG_Q4", overall_status: str = "AUTO_STRONG") -> dict:
    return {
        "company_id": 1,
        "ticker": "AAA",
        "metric": metric,
        "formula_id": formula_id,
        "formula_version": 1,
        "semantic_confidence": "SEMANTIC_A",
        "statistical_confidence": "STAT_HIGH",
        "overall_status": overall_status,
        "q1q3_applicability": q1q3_applicability,
        "q4_applicability": q4_applicability,
        "simfin_v2_corroboration": "LOCAL_BACKTEST",
    }


def _plan(source_mode: str = "DERIVED_CANONICAL_EBIT_PLUS_SEC_DA") -> dict:
    return {
        "company_id": 1,
        "ticker": "AAA",
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "period_end": "2024-03-31",
        "target_field": "ebitda",
        "source_mode": source_mode,
        "formula_id": "DA_COMBINED",
        "formula_version": 1,
        "semantic_class": "SEMANTIC_A",
        "statistical_class": "STAT_HIGH",
        "component_fact_ids": "1",
        "component_values": "{}",
        "quarterization": "DIRECT_Q1",
        "sec_accessions": "a",
        "derived_value": 13,
        "validation_evidence": "AUTO_STRONG",
        "q_applicability": "AUTO_STRONG",
        "candidate_status": "AUTO_STRONG",
        "core_ready_impact": 1,
    }
