from __future__ import annotations

import pytest

from swingmaster.fundamentals.v3_sec_q4_field_semantics import (
    Q4_FIELD_POLICY,
    build_q4_field_plan,
    classify_sec_field_semantics,
    derive_q4_flow_value,
    q4_expected_core_readiness,
    q4_expected_field_coverage,
    select_fy_end_instant_value,
    select_q4_source_mode,
    validate_q4_vintage_compatibility,
)


def test_flow_vs_instant_classification() -> None:
    assert classify_sec_field_semantics("revenue")["field_type"] == "FLOW"
    assert classify_sec_field_semantics("cash")["field_type"] == "INSTANT"


def test_revenue_fy_minus_9m() -> None:
    assert derive_q4_flow_value(fiscal_year_value=100.0, nine_month_value=70.0, mode="FY_MINUS_9M") == 30.0


def test_revenue_fy_minus_q1_q2_q3() -> None:
    assert derive_q4_flow_value(fiscal_year_value=100.0, q1=20.0, q2=25.0, q3=30.0, mode="FY_MINUS_Q1_Q2_Q3") == 25.0


def test_incompatible_concept_blocks_subtraction() -> None:
    assert Q4_FIELD_POLICY["ebit"].approval_status == "NOT_APPROVED_LEAVE_NULL"


def test_incompatible_duration_blocks_subtraction() -> None:
    with pytest.raises(ValueError):
        derive_q4_flow_value(fiscal_year_value=100.0, mode="BAD_MODE")


def test_incompatible_vintage_blocks_subtraction() -> None:
    result = validate_q4_vintage_compatibility("2025-02-20", ["2025-03-01"], basis="LATEST_CONSISTENT")
    assert result["status"] == "Q4_DERIVATION_VINTAGE_CONFLICT"


def test_cash_direct_fy_end_instant() -> None:
    assert select_fy_end_instant_value("cash", fy_value=10.0) == 10.0


def test_cash_subtraction_forbidden() -> None:
    with pytest.raises(ValueError):
        derive_q4_flow_value(fiscal_year_value=10.0, q1=1.0, q2=1.0, q3=1.0, mode="FY_MINUS_Q1_Q2_Q3", field="cash")


def test_total_debt_direct_fy_end() -> None:
    assert select_fy_end_instant_value("total_debt", fy_value=22.0) == 22.0


def test_total_debt_st_lt_component_construction() -> None:
    assert select_fy_end_instant_value("total_debt", fy_value=None, components={"short_term_debt": 4.0, "long_term_debt": 18.0}) == 22.0


def test_debt_subtraction_forbidden() -> None:
    with pytest.raises(ValueError):
        derive_q4_flow_value(fiscal_year_value=10.0, q1=1.0, q2=1.0, q3=1.0, mode="FY_MINUS_Q1_Q2_Q3", field="total_debt")


def test_shares_period_end_instant() -> None:
    assert select_fy_end_instant_value("shares_outstanding", fy_value=1000.0) == 1000.0


def test_weighted_average_shares_rejected() -> None:
    result = classify_sec_field_semantics("shares_outstanding", "WeightedAverageNumberOfDilutedSharesOutstanding")
    assert result["approved"] == 0


def test_shares_subtraction_forbidden() -> None:
    with pytest.raises(ValueError):
        derive_q4_flow_value(fiscal_year_value=10.0, q1=1.0, q2=1.0, q3=1.0, mode="FY_MINUS_Q1_Q2_Q3", field="shares_outstanding")


def test_capex_treated_as_flow() -> None:
    assert Q4_FIELD_POLICY["capex"].field_type == "FLOW"


def test_capex_sign_convention() -> None:
    assert "negative" in Q4_FIELD_POLICY["capex"].final_phase3c2_policy


def test_ocf_fy_minus_9m() -> None:
    assert Q4_FIELD_POLICY["operating_cashflow"].preferred_mode == "FY_MINUS_9M"


def test_fcf_from_reconstructed_ocf_plus_capex() -> None:
    assert Q4_FIELD_POLICY["free_cashflow"].preferred_mode == "APPROVED_DERIVATION"


def test_ebit_not_automatically_operating_income() -> None:
    assert "operating_income is not enough" in Q4_FIELD_POLICY["ebit"].required_sec_concept_compatibility


def test_ebitda_unsafe_remains_null() -> None:
    assert Q4_FIELD_POLICY["ebitda"].final_phase3c2_policy == "leave NULL for SEC-reconstructed Q4s"


def test_q4_identity_valid_with_missing_ebitda() -> None:
    plan = build_q4_field_plan([_q4_row()])
    assert any(row["field"] == "ebitda" and row["will_populate"] == 0 for row in plan)
    assert any(row["field"] == "revenue" and row["will_populate"] == 1 for row in plan)


def test_direct_q4_precedence() -> None:
    assert select_q4_source_mode("revenue", {"DIRECT_QUARTER", "FY_MINUS_Q1_Q2_Q3"}) == "DIRECT_QUARTER"


def test_fy_minus_9m_precedence_over_noisier_fallback() -> None:
    assert select_q4_source_mode("operating_cashflow", {"FY_MINUS_9M", "FY_MINUS_Q1_Q2_Q3"}) == "FY_MINUS_9M"


def test_publication_date_from_annual_result() -> None:
    coverage = q4_expected_field_coverage(build_q4_field_plan([_q4_row()]), [_q4_row(publish_date="2025-02-20")])
    assert next(row for row in coverage if row["field"] == "publish_date")["populated"] == 1


def test_publish_null_allowed() -> None:
    coverage = q4_expected_field_coverage(build_q4_field_plan([_q4_row()]), [_q4_row(publish_date="")])
    assert next(row for row in coverage if row["field"] == "publish_date")["left_null"] == 1


def test_as_reported_latest_consistent_vintage() -> None:
    assert validate_q4_vintage_compatibility("2025-02-20", ["2024-05-01"], basis="AS_REPORTED")["compatible"] == 1
    assert validate_q4_vintage_compatibility("2025-02-20", ["2024-05-01"], basis="LATEST_CONSISTENT")["compatible"] == 1


def test_field_policy_deterministic() -> None:
    assert list(Q4_FIELD_POLICY) == list(Q4_FIELD_POLICY)


def test_expected_q4_coverage_deterministic() -> None:
    plan = build_q4_field_plan([_q4_row()])
    assert q4_expected_field_coverage(plan, [_q4_row()]) == q4_expected_field_coverage(plan, [_q4_row()])


def test_no_v3_writes_contract_is_readonly_by_design() -> None:
    assert all(policy.final_phase3c2_policy for policy in Q4_FIELD_POLICY.values())


def test_no_legacy_writes_contract_is_readonly_by_design() -> None:
    assert len(Q4_FIELD_POLICY) == 12


def test_no_v2_writes_contract_is_readonly_by_design() -> None:
    assert "total_debt" in Q4_FIELD_POLICY


def test_core_readiness_missing_ebitda_only() -> None:
    readiness = q4_expected_core_readiness(build_q4_field_plan([_q4_row()]))
    assert next(row for row in readiness if row["core_bucket"] == "MISSING_EBITDA_ONLY")["rows"] == 1


def _q4_row(publish_date: str = "2025-02-20") -> dict[str, str]:
    return {
        "ticker": "AAA",
        "fiscal_year": "2025",
        "period_end_date": "2025-12-31",
        "publish_date": publish_date,
        "balance_sheet_direct_instant_fields": "cash;total_debt;shares_outstanding",
        "field_derivation_methods": '{"revenue":"LEGACY_SEC_FY_MINUS_Q1_Q2_Q3_PENDING_CONCEPT_CHECK","free_cashflow":"LEGACY_SEC_DERIVE_FROM_Q4_OCF_PLUS_CAPEX_IF_INPUTS_SAFE","operating_cashflow":"LEGACY_SEC_FY_MINUS_Q1_Q2_Q3_PENDING_CONCEPT_CHECK","capex":"LEGACY_SEC_FY_MINUS_Q1_Q2_Q3_PENDING_CONCEPT_CHECK"}',
    }
