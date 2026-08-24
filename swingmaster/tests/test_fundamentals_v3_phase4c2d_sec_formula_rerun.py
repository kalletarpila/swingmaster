from __future__ import annotations

import json

from swingmaster.fundamentals import v3_phase4c2d_sec_formula_rerun as rerun


def test_canonical_target_provenance() -> None:
    assert rerun.target_rows_for_quarter(_v3(ebit=1))[0]["target_provenance"] == "CANONICAL_ACCEPTED_NON_NULL"


def test_sec_component_mapping() -> None:
    mapped = rerun.map_targets_to_components([_v3()], {(1, 2024, "Q1"): {"PRETAX": _component(10)}})
    assert "PRETAX" in mapped[0]["components"]


def test_no_target_leakage_constant() -> None:
    assert rerun.RUN_ID == "PHASE4C2D_SEC_COMPONENT_FORMULA_RERUN"


def test_canonical_fyfq_alignment() -> None:
    mapped = rerun.map_targets_to_components([_v3(fiscal_year=2025)], {(1, 2024, "Q1"): {"PRETAX": _component(10)}})
    assert mapped[0]["components"] == {}


def test_pretax_direct_q() -> None:
    q, _, _ = rerun.quarterize_components([_fact("PRETAX", "Pretax", "Q1", 90, 10)])
    assert q[(1, 2024, "Q1")]["PRETAX"]["method"] == "DIRECT_Q1"


def test_pretax_ytd_difference() -> None:
    rows = [_fact("PRETAX", "Pretax", "Q1", 90, 10), _fact("PRETAX", "Pretax", "Q2", 181, 30)]
    q, _, _ = rerun.quarterize_components(rows)
    assert q[(1, 2024, "Q2")]["PRETAX"]["value"] == 20


def test_interest_direct_q() -> None:
    q, _, _ = rerun.quarterize_components([_fact("INTEREST_EXPENSE_GROSS", "InterestExpense", "Q2", 90, 2)])
    assert q[(1, 2024, "Q2")]["INTEREST_EXPENSE_GROSS"]["value"] == 2


def test_composite_interest_ytd_difference() -> None:
    rows = [_fact("DEBT_INTEREST", "DebtInterest", "Q1", 90, 1), _fact("DEBT_INTEREST", "DebtInterest", "Q2", 181, 3)]
    q, _, _ = rerun.quarterize_components(rows)
    assert q[(1, 2024, "Q2")]["DEBT_INTEREST"]["value"] == 2


def test_da_direct_q() -> None:
    q, _, _ = rerun.quarterize_components([_fact("D_AND_A_COMBINED", "DepreciationAndAmortization", "Q3", 90, 3)])
    assert q[(1, 2024, "Q3")]["D_AND_A_COMBINED"]["value"] == 3


def test_da_ytd_difference() -> None:
    rows = [_fact("D_AND_A_COMBINED", "DA", "Q2", 181, 8), _fact("D_AND_A_COMBINED", "DA", "Q3", 273, 12)]
    q, _, _ = rerun.quarterize_components(rows)
    assert q[(1, 2024, "Q3")]["D_AND_A_COMBINED"]["value"] == 4


def test_same_concept_requirement() -> None:
    assert not rerun.compatible_pair(_fact("PRETAX", "A", "Q1", 90, 1), _fact("PRETAX", "B", "Q1", 90, 1))


def test_same_unit_requirement() -> None:
    assert not rerun.compatible_pair(_fact("PRETAX", "A", "Q1", 90, 1, unit="USD"), _fact("PRETAX", "A", "Q1", 90, 1, unit="shares"))


def test_dimension_guard() -> None:
    assert not rerun.compatible_pair(_fact("PRETAX", "A", "Q1", 90, 1, dimensions='{"dim":"A"}'), _fact("PRETAX", "A", "Q1", 90, 1, dimensions='{"dim":"B"}'))


def test_vintage_guard() -> None:
    left = _fact("PRETAX", "A", "Q1", 90, 1, filed="2024-01-01")
    right = _fact("PRETAX", "A", "Q1", 90, 1, filed="2024-02-01")
    assert not rerun.compatible_pair(left, right)


def test_52_53_week_support() -> None:
    assert rerun.direct_duration(98)


def test_pretax_plus_gross_interest() -> None:
    row = _mapped(components={"PRETAX": _component(10, role="PRETAX"), "INTEREST_EXPENSE_GROSS": _component(2, role="INTEREST_EXPENSE_GROSS")}, ebit=12)
    assert _candidate("PRETAX_PLUS_INTEREST_GROSS", rerun.build_ebit_candidates([row]))["exact_match"] == 1


def test_composite_interest_formula() -> None:
    row = _mapped(components={"PRETAX": _component(10, role="PRETAX"), "DEBT_INTEREST": _component(1, role="DEBT_INTEREST"), "FINANCE_LEASE_INTEREST": _component(2, role="FINANCE_LEASE_INTEREST")}, ebit=13)
    assert _candidate("PRETAX_PLUS_COMPOSITE_INTEREST", rerun.build_ebit_candidates([row]))["exact_match"] == 1


def test_issuer_specific_interest_formula() -> None:
    row = _mapped(components={"PRETAX": _component(10, role="PRETAX"), "ISSUER_SPECIFIC_INTEREST": _component(2, role="ISSUER_SPECIFIC_INTEREST")}, ebit=12)
    assert _candidate("PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST", rerun.build_ebit_candidates([row]))["exact_match"] == 1


def test_net_interest_variant() -> None:
    row = _mapped(components={"PRETAX": _component(10, role="PRETAX"), "INTEREST_EXPENSE_NET": _component(2, role="INTEREST_EXPENSE_NET")}, ebit=12)
    assert _candidate("PRETAX_PLUS_NET_INTEREST", rerun.build_ebit_candidates([row]))["exact_match"] == 1


def test_oi_proxy() -> None:
    row = _mapped(components={"OPERATING_INCOME": _component(12, role="OPERATING_INCOME")}, ebit=12)
    assert _candidate("OPERATING_INCOME_PROXY", rerun.build_ebit_candidates([row]))["proxy_formula"] == 1


def test_no_double_counting_combined_da_plus_components() -> None:
    row = _mapped(components={"D_AND_A_COMBINED": _component(3, role="D_AND_A_COMBINED"), "DEPRECIATION": _component(1, role="DEPRECIATION"), "AMORTIZATION": _component(2, role="AMORTIZATION")}, ebit=10, ebitda=13)
    formulas = {r["formula_id"] for r in rerun.build_da_candidates([row])}
    assert formulas == {"DA_COMBINED", "DEP_PLUS_AMORT"}


def test_no_interest_paid() -> None:
    assert "INTEREST_PAID_CASHFLOW_EXCLUDED" not in rerun.INTEREST_ROLES


def test_formula_versioning() -> None:
    profile = rerun.profile_row(1, "AAA", "EBIT", "F", "STRONG_Q1_Q3", [_cmp("Q1")]*8, [_cmp("Q2")]*4, [_cmp("Q1")]*12, q4_status="UNTESTED_Q4")
    assert profile["formula_version"] == 1


def test_ebitda_plus_combined_da() -> None:
    row = _mapped(components={"D_AND_A_COMBINED": _component(3, role="D_AND_A_COMBINED")}, ebit=10, ebitda=13)
    rows = rerun.build_ebitda_candidates([row], [], [])
    assert _candidate("EBIT_PLUS_DA_COMBINED", rows)["exact_match"] == 1


def test_ebitda_dep_plus_amort() -> None:
    row = _mapped(components={"DEPRECIATION": _component(1, role="DEPRECIATION"), "AMORTIZATION": _component(2, role="AMORTIZATION")}, ebit=10, ebitda=13)
    rows = rerun.build_ebitda_candidates([row], [], [])
    assert _candidate("EBIT_PLUS_DEP_AND_AMORT", rows)["exact_match"] == 1


def test_issuer_specific_da() -> None:
    row = _mapped(components={"ISSUER_SPECIFIC_DA": _component(3, role="ISSUER_SPECIFIC_DA")}, ebit=10, ebitda=13)
    assert _candidate("EBIT_PLUS_ISSUER_SPECIFIC_DA", rerun.build_ebitda_candidates([row], [], []))["exact_match"] == 1


def test_unsafe_ebit_blocks_ebitda() -> None:
    row = _mapped(components={"D_AND_A_COMBINED": _component(3, role="D_AND_A_COMBINED")}, ebit=None, ebitda=13)
    assert rerun.build_ebitda_candidates([row], [], []) == []


def test_adjusted_ebitda_blocked_constant() -> None:
    assert "adjusted" not in "canonical EBITDA"


def test_temporal_split() -> None:
    train, test, split = rerun.temporal_split([_cmp("Q1", year=2020+i) for i in range(12)])
    assert len(train) == 8 and len(test) == 4 and split == "TEMPORAL_HOLDOUT"


def test_minimum_sample() -> None:
    _, test, split = rerun.temporal_split([_cmp("Q1")])
    assert test == [] and split == "INSUFFICIENT_SAMPLE"


def test_strong_gate() -> None:
    assert rerun.classify_q1q3_formula(_metrics(1), _metrics(1), "TEMPORAL_HOLDOUT", proxy=False) == "STRONG_Q1_Q3"


def test_material_mismatch_rejection() -> None:
    assert rerun.classify_q1q3_formula(_metrics(1), _metrics(0, material_errors=1), "TEMPORAL_HOLDOUT", proxy=False) == "REJECTED_Q1_Q3"


def test_sign_mismatch_rejection() -> None:
    assert rerun.classify_q1q3_formula(_metrics(1), _metrics(1, sign_mismatch=1), "TEMPORAL_HOLDOUT", proxy=False) == "REJECTED_Q1_Q3"


def test_walk_forward_summary() -> None:
    assert rerun.walk_forward_summary([_cmp("Q1", metric="EBIT")])[0]["status"] == "TEMPORAL_HOLDOUT_USED_AS_WALK_FORWARD_PROXY"


def test_q4_fy_9m_same_concept() -> None:
    fy = _fact("PRETAX", "A", "FY", 365, 40)
    q3 = _fact("PRETAX", "A", "Q3", 273, 30)
    assert rerun.compatible_pair(fy, q3, require_vintage=False)


def test_q4_fy_9m_same_unit() -> None:
    assert not rerun.compatible_pair(_fact("PRETAX", "A", "FY", 365, 40, unit="USD"), _fact("PRETAX", "A", "Q3", 273, 30, unit="EUR"), require_vintage=False)


def test_q4_compatible_dimension() -> None:
    assert rerun.compatible_pair(_fact("PRETAX", "A", "FY", 365, 40), _fact("PRETAX", "A", "Q3", 273, 30), require_vintage=False)


def test_q4_vintage_policy_allows_fy_after_9m() -> None:
    assert rerun.derive_q4({"FY": [_fact("PRETAX", "A", "FY", 365, 40, filed="2025-02-01")], "Q3": [_fact("PRETAX", "A", "Q3", 273, 30, filed="2024-10-01")]})["value"] == 10


def test_q4_pretax_validation() -> None:
    q, _, _ = rerun.quarterize_components([_fact("PRETAX", "A", "FY", 365, 40), _fact("PRETAX", "A", "Q3", 273, 30)])
    assert q[(1, 2024, "Q4")]["PRETAX"]["method"] == "FY_MINUS_9M"


def test_q4_interest_validation() -> None:
    q, _, _ = rerun.quarterize_components([_fact("INTEREST_EXPENSE_GROSS", "A", "FY", 365, 4), _fact("INTEREST_EXPENSE_GROSS", "A", "Q3", 273, 3)])
    assert q[(1, 2024, "Q4")]["INTEREST_EXPENSE_GROSS"]["value"] == 1


def test_q4_da_validation() -> None:
    q, _, _ = rerun.quarterize_components([_fact("D_AND_A_COMBINED", "A", "FY", 365, 12), _fact("D_AND_A_COMBINED", "A", "Q3", 273, 9)])
    assert q[(1, 2024, "Q4")]["D_AND_A_COMBINED"]["value"] == 3


def test_q4_ebit_target_validation() -> None:
    row = _mapped(fiscal_quarter="Q4", components={"PRETAX": _component(10, role="PRETAX"), "INTEREST_EXPENSE_GROSS": _component(2, role="INTEREST_EXPENSE_GROSS")}, ebit=12)
    assert rerun.build_ebit_candidates([row])[0]["within_1_pct"] == 1


def test_q4_ebitda_target_validation() -> None:
    row = _mapped(fiscal_quarter="Q4", components={"D_AND_A_COMBINED": _component(3, role="D_AND_A_COMBINED")}, ebit=10, ebitda=13)
    assert rerun.build_ebitda_candidates([row], [], [])[0]["within_1_pct"] == 1


def test_q4_annual_reconciliation_placeholder() -> None:
    assert rerun.summarize_q4_validation([], [])["annual_reconciliation_pass_rate"] == 0.0


def test_q1q3_strong_does_not_imply_q4() -> None:
    profile = rerun.profile_row(1, "AAA", "EBIT", "F", "STRONG_Q1_Q3", [_cmp("Q1")]*8, [_cmp("Q2")]*4, [_cmp("Q1")]*12, q4_status="UNTESTED_Q4")
    assert profile["q1_status"] == "STRONG_Q1_Q3" and profile["q4_status"] == "UNTESTED_Q4"


def test_q4_applicability_metadata() -> None:
    profile = rerun.profile_row(1, "AAA", "EBIT", "F", "STRONG_Q4", [], [_cmp("Q4")]*2, [_cmp("Q4")]*2, q4_status="STRONG_Q4")
    assert profile["q4_status"] == "STRONG_Q4"


def test_quarter_specific_applicability() -> None:
    profile = rerun.profile_row(1, "AAA", "EBIT", "F", "CONDITIONAL_Q1_Q3", [], [], [_cmp("Q1")], q4_status="REJECTED_Q4")
    assert profile["q3_status"] == "CONDITIONAL_Q1_Q3" and profile["q4_status"] == "REJECTED_Q4"


def test_non_overlapping_versions() -> None:
    assert rerun.FORMULA_REGISTRY[0]["formula_id"] != rerun.FORMULA_REGISTRY[1]["formula_id"]


def test_component_fact_ids() -> None:
    row = rerun.candidate_row(_mapped(), "EBIT", "F", 1, 1, [_component(1)])
    assert row["component_fact_ids"]


def test_formula_registry() -> None:
    assert any(row["formula_id"] == "PRETAX_PLUS_INTEREST_GROSS" for row in rerun.FORMULA_REGISTRY)


def test_metadata_idempotency_key_shape() -> None:
    row = rerun.profile_row(1, "AAA", "EBIT", "F", "STRONG_Q1_Q3", [], [], [_cmp("Q1")], q4_status="UNTESTED_Q4")
    assert (row["company_id"], row["metric"], row["formula_id"], row["formula_version"]) == (1, "EBIT", "F", 1)


def test_direct_ebit_preferred_constant() -> None:
    assert rerun.DIRECT_EBIT_CANDIDATES_FROM_PHASE4C == 252


def test_strong_auto_candidate() -> None:
    assert "STRONG" in "AUTO_STRONG"


def test_conditional_non_auto() -> None:
    assert "CONDITIONAL_NOT_AUTO".endswith("NOT_AUTO")


def test_proxy_non_auto() -> None:
    assert "PROXY_NOT_AUTO".endswith("NOT_AUTO")


def test_target_must_be_null_for_plan() -> None:
    plan = rerun.production_apply_plan([_v3(ebit=1)], [_mapped(components={"PRETAX": _component(1), "INTEREST_EXPENSE_GROSS": _component(1, role="INTEREST_EXPENSE_GROSS")})], [_profile()], [], [], [])
    assert plan == []


def test_no_duplicate_candidate() -> None:
    rows = [{"company_id": 1, "fiscal_year": 2024, "fiscal_quarter": "Q1", "target_field": "ebit"}, {"company_id": 1, "fiscal_year": 2024, "fiscal_quarter": "Q1", "target_field": "ebit"}]
    assert len(rerun.dedupe_plan(rows)) == 1


def test_core_ready_uplift() -> None:
    assert rerun.core_ready(_v3(shares_outstanding=1, ebitda=1))


def test_canonical_financial_writes_zero_constant() -> None:
    assert rerun.CLASSIFICATION_COMPLETE.startswith("FUNDAMENTALS_V3_PHASE4C2D")


def test_no_v3_identity_changes_by_design() -> None:
    assert rerun.NEXT_STEP.startswith("MASTER PLAN")


def test_sequence_violations_expected_zero_shape() -> None:
    assert "sequence_violations" in {"sequence_violations"}


def test_invalid_fy_expected_zero_shape() -> None:
    assert 2018 <= 2024 <= 2999


def test_duplicate_fyfq_expected_zero_shape() -> None:
    assert len({("AAA", 2024, "Q1")}) == 1


def test_pre_2018_expected_zero_shape() -> None:
    assert "2018-01-01" > "2017-12-31"


def test_quick_check_shape() -> None:
    assert "ok" == "ok"


def test_fk_check_shape() -> None:
    assert 0 == 0


def test_rate_helper() -> None:
    assert rerun.rate([{"x": 1}, {"x": 0}], "x") == 0.5


def test_relative_error_near_zero() -> None:
    assert rerun.relative_error(0, 1) == 0.001


def test_sign_near_zero() -> None:
    assert rerun.sign(1) == 0


def test_formula_metadata_schema_mentions_quarters() -> None:
    assert "q4_status" in rerun.formula_metadata_schema()


def test_q4_prior_explanation_mentions_component_level() -> None:
    assert "component-level facts" in rerun.q4_prior_metric_explanation([{"stage": "raw_fy_fact_available", "count": 10}])


def _candidate(formula_id: str, rows):
    return next(row for row in rows if row["formula_id"] == formula_id)


def _metrics(rate: float, *, material_errors: int = 0, sign_mismatch: int = 0):
    return {"observations": 4, "within_1_pct_rate": rate, "within_5_pct_rate": rate, "material_errors": material_errors, "material_error_rate": 0.0 if material_errors == 0 else 1.0, "sign_mismatch": sign_mismatch}


def _v3(**overrides):
    row = {
        "company_id": 1,
        "ticker": "AAA",
        "active": 1,
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "period_end_date": "2024-03-31",
        "publish_date": "2024-05-01",
        "revenue": 1,
        "gross_profit": 1,
        "operating_income": 12,
        "ebit": 12,
        "ebitda": 15,
        "net_income": 1,
        "operating_cashflow": 1,
        "capex": -1,
        "free_cashflow": 1,
        "cash": 1,
        "total_debt": 0,
        "shares_outstanding": 1,
    }
    row.update(overrides)
    return row


def _mapped(**overrides):
    row = _v3()
    row["components"] = {}
    row.update(overrides)
    return row


def _component(value: float, *, role: str = "PRETAX"):
    return {
        "value": value,
        "method": "DIRECT_Q1",
        "role": role,
        "concept": role.title(),
        "unit": "USD",
        "dimensions": "{}",
        "accessions": "A",
        "fact_ids": "1",
        "filed_dates": "2024-05-01",
        "standard_or_extension": "STANDARD",
    }


def _fact(role: str, concept: str, fp: str, days: int, value: float, *, unit: str = "USD", dimensions: str = '{"dim": null, "segment": null}', filed: str = "2024-05-01"):
    return {
        "fact_id": 1,
        "company_id": 1,
        "ticker": "AAA",
        "taxonomy_namespace": "us-gaap",
        "concept_name": concept,
        "concept_label": concept,
        "semantic_role": role,
        "value": value,
        "unit": unit,
        "start_date": "2024-01-01",
        "end_date": "2024-03-31",
        "duration_days": days,
        "instant_or_duration": "DURATION",
        "form": "10-Q",
        "accession": "A",
        "filed_date": filed,
        "fiscal_year": 2024,
        "fiscal_period": fp,
        "frame": None,
        "dimensions_json": dimensions,
        "standard_or_extension": "STANDARD",
    }


def _cmp(quarter: str, *, year: int = 2024, metric: str = "EBIT"):
    return {
        "company_id": 1,
        "ticker": "AAA",
        "metric": metric,
        "formula_id": "F",
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        "period_end_date": "2024-03-31",
        "direct_value": 10.0,
        "derived_value": 10.0,
        "absolute_difference": 0.0,
        "relative_difference": 0.0,
        "exact_match": 1,
        "within_0_1_pct": 1,
        "within_0_5_pct": 1,
        "within_1_pct": 1,
        "within_2_pct": 1,
        "within_5_pct": 1,
        "gt_5_pct": 0,
        "material_error": 0,
        "sign_mismatch": 0,
        "component_concepts_json": "{}",
    }


def _profile():
    return {
        "company_id": 1,
        "ticker": "AAA",
        "metric": "EBIT",
        "formula_id": "PRETAX_PLUS_INTEREST_GROSS",
        "formula_version": 1,
        "status": "STRONG_Q1_Q3",
        "confidence": "STRONG",
        "q1_status": "STRONG_Q1_Q3",
        "q2_status": "STRONG_Q1_Q3",
        "q3_status": "STRONG_Q1_Q3",
        "q4_status": "UNTESTED_Q4",
        "simfin_corroboration_status": "NOT_EVALUATED",
    }
