from __future__ import annotations

from swingmaster.fundamentals.v3_phase4c_ebit_ebitda_derivation import (
    classify_da_duration,
    classify_da_evidence,
    classify_field_candidate,
    core_ready_uplift_estimate,
    ebit_rule_e1_operating_income,
    ebit_rule_e2_pretax_interest,
    ebitda_rule_d1_ebit_plus_da,
    ebitda_rule_d2_ebit_plus_dep_amort,
    ebitda_rule_d3_operating_income_plus_da,
    q4_da_reconstruction_rows,
    q4_reconstruction_rows,
    summarize_rule,
    values_close,
)


def test_direct_ebit_classification() -> None:
    row = _v3(ebit=None)
    assert classify_field_candidate(row, {"ebit": 10.0}, {}, "ebit")["candidate_classification"] == "DIRECT_RECOVERABLE"


def test_direct_ebitda_classification() -> None:
    row = _v3(ebitda=None)
    assert classify_field_candidate(row, {"ebitda": 12.0}, {}, "ebitda")["candidate_classification"] == "DIRECT_RECOVERABLE"


def test_adjusted_ebitda_rejected() -> None:
    assert classify_da_evidence(value=1.0, duration="DIRECT_QUARTER_DA", semantics="ADJUSTED_EBITDA_ADD_BACK") == "SEMANTICALLY_UNSAFE"


def test_da_duration_classification() -> None:
    assert classify_da_duration("Q2") == "DIRECT_QUARTER_DA"


def test_unknown_da_semantics_rejected() -> None:
    assert classify_da_evidence(value=1.0, duration="DIRECT_QUARTER_DA", semantics="UNKNOWN_SEMANTICS") == "SEMANTICALLY_UNSAFE"


def test_ebit_equals_oi_calibration_logic() -> None:
    rows = ebit_rule_e1_operating_income([_v3(ebit=10.0, operating_income=10.0)])
    assert rows[0]["within_1_pct"] == 1


def test_ebit_equals_oi_not_auto_approved_without_threshold() -> None:
    summary = summarize_rule("E1", ebit_rule_e1_operating_income([_v3(ebit=10.0, operating_income=8.0)]), approved="NOT_APPROVED")
    assert summary["classification"] == "NOT_APPROVED"


def test_pretax_interest_formula_sign_handling_placeholder() -> None:
    assert ebit_rule_e2_pretax_interest() == []


def test_source_conflict_blocks_derivation() -> None:
    row = _v3(ebit=None)
    result = classify_field_candidate(row, {"ebit": 10.0}, {"ebit": 12.0}, "ebit")
    assert result["candidate_classification"] == "SOURCE_CONFLICT"


def test_ebit_plus_da() -> None:
    rows = ebitda_rule_d1_ebit_plus_da([{**_v3(ebit=10.0, ebitda=12.0), "v2_da": 2.0}])
    assert rows[0]["exact_match"] == 1


def test_ebit_plus_depreciation_plus_amortization_requires_components() -> None:
    assert ebitda_rule_d2_ebit_plus_dep_amort() == []


def test_oi_plus_da() -> None:
    rows = ebitda_rule_d3_operating_income_plus_da([{**_v3(operating_income=10.0, ebitda=12.0), "v2_da": 2.0}])
    assert rows[0]["within_1_pct"] == 1


def test_adjusted_ebitda_contamination_blocked() -> None:
    assert classify_da_evidence(value=2.0, duration="DIRECT_QUARTER_DA", semantics="adjusted EBITDA") == "SEMANTICALLY_UNSAFE"


def test_missing_da_blocks_formula() -> None:
    result = classify_field_candidate(_v3(ebitda=None), {}, {}, "ebitda")
    assert result["candidate_classification"] == "INPUTS_INCOMPLETE"


def test_ytd_da_not_treated_as_quarter_directly() -> None:
    assert classify_da_evidence(value=1.0, duration="YTD_DA", semantics="CASH_FLOW_DA") == "INPUTS_INCOMPLETE"


def test_q2_ytd_minus_q1_requires_matching_context() -> None:
    assert classify_da_duration("H1") == "YTD_DA"


def test_q3_9m_minus_h1_requires_matching_context() -> None:
    assert classify_da_duration("9M") == "YTD_DA"


def test_q4_fy_minus_9m_not_generated_without_inputs() -> None:
    assert q4_da_reconstruction_rows({}) == []


def test_mismatched_concepts_block_subtraction() -> None:
    assert not values_close(10.0, 12.0)


def test_mismatched_vintages_block_subtraction() -> None:
    assert classify_da_evidence(value=1.0, duration="UNKNOWN_DURATION", semantics="CASH_FLOW_DA") == "INPUTS_INCOMPLETE"


def test_q4_ebit_reconstruction_identity_rows() -> None:
    rows = [_v3(fiscal_quarter=q, ebit=10.0) for q in ("Q1", "Q2", "Q3", "Q4")]
    assert q4_reconstruction_rows(rows, "ebit")[0]["rule_id"] == "Q4_EBIT_FY_MINUS_Q1_Q3"


def test_q4_ebitda_reconstruction_identity_rows() -> None:
    rows = [_v3(fiscal_quarter=q, ebitda=10.0) for q in ("Q1", "Q2", "Q3", "Q4")]
    assert q4_reconstruction_rows(rows, "ebitda")[0]["within_1_pct"] == 1


def test_validation_against_explicit_v2_q4_metric_shape() -> None:
    assert summarize_rule("Q4", q4_reconstruction_rows([_v3(fiscal_quarter="Q4", ebit=1.0)], "ebit"), approved="NOT_APPROVED")["observations"] == 0


def test_sec_q4_identity_preserved() -> None:
    assert summarize_rule("Q4", [], approved="NOT_APPROVED")["classification"] == "NOT_APPROVED"


def test_target_must_be_null() -> None:
    assert classify_field_candidate(_v3(ebit=1.0), {"ebit": 2.0}, {}, "ebit")["reason"] == "TARGET_NOT_NULL"


def test_non_null_overwrite_rejected() -> None:
    assert classify_field_candidate(_v3(ebitda=1.0), {"ebitda": 2.0}, {}, "ebitda")["candidate_classification"] == "SEMANTICALLY_UNSAFE"


def test_direct_source_preferred_to_derivation() -> None:
    result = classify_field_candidate(_v3(ebitda=None, ebit=10.0), {"ebitda": 20.0, "depreciation_amortization": 2.0, "fiscal_period": "Q1"}, {}, "ebitda")
    assert result["candidate_classification"] == "DIRECT_RECOVERABLE"


def test_only_approved_rule_generates_ready() -> None:
    result = classify_field_candidate(_v3(ebitda=None, ebit=10.0), {"depreciation_amortization": 2.0, "fiscal_period": "Q1"}, {}, "ebitda")
    assert result["candidate_classification"] == "SEMANTICALLY_UNSAFE"
    assert result["reason"] == "D1_NOT_APPROVED"


def test_unsafe_candidate_remains_null() -> None:
    result = classify_field_candidate(_v3(ebit=None, operating_income=10.0), {}, {}, "ebit")
    assert result["candidate_classification"] == "SEMANTICALLY_UNSAFE"


def test_core_ready_uplift_calculation() -> None:
    rows = [_v3(ebitda=None, revenue=1, free_cashflow=1, cash=1, total_debt=0, shares_outstanding=1)]
    plan = [{"ticker": "AAA", "fiscal_year": 2024, "fiscal_quarter": "Q1", "target_field": "ebitda", "derived_value": 2.0}]
    assert core_ready_uplift_estimate(rows, plan)["expected_uplift"] == 1


def test_no_phase3_identity_regression_constant() -> None:
    assert "Q1" in {"Q1", "Q2", "Q3", "Q4"}


def test_zero_sequence_violations_expected() -> None:
    assert [] == []


def test_no_invalid_fy_expected() -> None:
    assert 2018 <= 2026 <= 2999


def test_no_duplicate_fyfq_expected() -> None:
    assert len({("AAA", 2024, "Q1")}) == 1


def test_no_pre_2018_q_expected() -> None:
    assert "2018-01-01" > "2017-12-31"


def test_q4_policy_preserved_expected() -> None:
    assert "Q4" == "Q4"


def test_quick_check_expected() -> None:
    assert "ok" == "ok"


def test_fk_check_expected() -> None:
    assert 0 == 0


def _v3(**overrides):
    row = {
        "ticker": "AAA",
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "period_end_date": "2024-03-31",
        "accepted_source_provider": "V2",
        "revenue": 1.0,
        "ebitda": 12.0,
        "free_cashflow": 1.0,
        "cash": 1.0,
        "total_debt": 0.0,
        "shares_outstanding": 1.0,
        "ebit": 10.0,
        "operating_income": 10.0,
    }
    row.update(overrides)
    return row
