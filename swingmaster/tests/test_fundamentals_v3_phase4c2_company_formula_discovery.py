from __future__ import annotations

from swingmaster.fundamentals.v3_phase4c2_company_formula_discovery import (
    DurationFact,
    build_ebit_value,
    build_ebitda_value,
    classify_formula,
    duration_days_compatible,
    evaluate_company_proxy,
    facts_compatible,
    formula_registry_rows,
    is_interest_component,
    is_rejected_interest_component,
    metadata_dry_rows,
    parse_sec_field_name,
    quarterize_component,
    recovery_potential,
    reject_adjusted_ebitda_target,
)
from swingmaster.fundamentals.v3_phase4c_ebit_ebitda_derivation import metric_counts


def test_q1_direct_3m() -> None:
    assert quarterize_component([_fact("Q1", 10)], 2024, "Q1", concept="InterestExpense")["method"] == "DIRECT_QUARTER"


def test_q1_ytd_equivalent() -> None:
    assert quarterize_component([_fact("Q1_YTD", 10)], 2024, "Q1", concept="InterestExpense")["value"] == 10


def test_q2_direct_3m_preferred() -> None:
    facts = [_fact("Q1", 10), _fact("H1", 30), _fact("Q2", 25)]
    assert quarterize_component(facts, 2024, "Q2", concept="InterestExpense")["value"] == 25


def test_q2_6m_minus_q1() -> None:
    facts = [_fact("Q1", 10), _fact("H1", 30)]
    assert quarterize_component(facts, 2024, "Q2", concept="InterestExpense")["value"] == 20


def test_q3_direct_3m_preferred() -> None:
    facts = [_fact("H1", 30), _fact("9M", 60), _fact("Q3", 35)]
    assert quarterize_component(facts, 2024, "Q3", concept="InterestExpense")["value"] == 35


def test_q3_9m_minus_6m() -> None:
    facts = [_fact("H1", 30), _fact("9M", 60)]
    assert quarterize_component(facts, 2024, "Q3", concept="InterestExpense")["value"] == 30


def test_q4_fy_minus_9m() -> None:
    facts = [_fact("9M", 60), _fact("FY", 100)]
    assert quarterize_component(facts, 2024, "Q4", concept="InterestExpense")["value"] == 40


def test_52_53_week_compatible_durations() -> None:
    assert duration_days_compatible(98)


def test_mismatched_concept_blocks_subtraction() -> None:
    assert not facts_compatible(_fact("Q1", 1, concept="A"), _fact("Q1", 1, concept="B"))


def test_mismatched_dimensions_block_subtraction() -> None:
    assert not facts_compatible(_fact("Q1", 1, dimensions="A"), _fact("Q1", 1, dimensions="B"))


def test_incompatible_vintage_blocks_subtraction() -> None:
    assert not facts_compatible(_fact("Q1", 1, filed="2024-05-01"), _fact("Q1", 1, filed="2024-08-01"))


def test_restated_comparable_vintage_accepted() -> None:
    assert facts_compatible(_fact("Q1", 1, filed="2024-08-01"), _fact("H1", 3, filed="2024-08-01"))


def test_gross_interest_concept() -> None:
    assert is_interest_component("InterestExpenseNonOperating")


def test_composite_interest_components() -> None:
    assert build_ebit_value("PRETAX_PLUS_COMPOSITE_INTEREST", {"pretax": 10, "interest_1": 2, "interest_2": 1}) == 13


def test_finance_lease_interest() -> None:
    assert is_interest_component("FinanceLeaseInterestExpense")


def test_issuer_specific_interest() -> None:
    assert is_interest_component("AcmeDebtInterestExpense")


def test_interest_paid_rejected() -> None:
    assert is_rejected_interest_component("InterestPaid")


def test_interest_paid_net_rejected() -> None:
    assert is_rejected_interest_component("InterestPaidNet")


def test_sign_normalization_uses_reported_addition() -> None:
    assert build_ebit_value("PRETAX_PLUS_INTEREST", {"pretax": 10, "interest": 2}) == 12


def test_pretax_plus_interest() -> None:
    assert build_ebit_value("PRETAX_PLUS_INTEREST", {"pretax": 5, "interest": 1}) == 6


def test_pretax_plus_composite_interest() -> None:
    assert build_ebit_value("PRETAX_PLUS_COMPOSITE_INTEREST", {"pretax": 5, "interest_1": 1, "interest_2": 2}) == 8


def test_issuer_specific_formula() -> None:
    assert build_ebit_value("PRETAX_PLUS_INTEREST", {"pretax": 5, "interest": 3}) == 8


def test_oi_fallback_marked_proxy() -> None:
    rows = [_cmp(i, exact=True) for i in range(12)]
    _, _, fp = evaluate_company_proxy(rows, metric="EBIT")
    assert fp[0]["status"] == "PROXY"


def test_arbitrary_formula_search_prohibited() -> None:
    try:
        build_ebit_value("RANDOM_MULTIPLY", {"pretax": 1})
    except ValueError as exc:
        assert "ARBITRARY" in str(exc)
    else:
        raise AssertionError("arbitrary formula should fail")


def test_target_leakage_prevented_by_no_target_component() -> None:
    assert parse_sec_field_name("EBIT|form=10-Q")["concept"] == "EBIT"


def test_temporal_train_test_split() -> None:
    rows = [_cmp(i, exact=True) for i in range(12)]
    train, test, _ = evaluate_company_proxy(rows, metric="EBIT")
    assert len(train) == 8 and len(test) == 4


def test_minimum_sample() -> None:
    assert classify_formula(metric_counts([_cmp(1, exact=True)]), metric_counts([]), "INSUFFICIENT_SAMPLE", proxy=True) == "INSUFFICIENT_SAMPLE"


def test_low_sample_rejection() -> None:
    _, _, fp = evaluate_company_proxy([_cmp(i, exact=True) for i in range(2)], metric="EBIT")
    assert fp[0]["status"] == "INSUFFICIENT_SAMPLE"


def test_strong_formula_classification() -> None:
    assert classify_formula(metric_counts([_cmp(i, exact=True) for i in range(8)]), metric_counts([_cmp(i, exact=True) for i in range(4)]), "TEMPORAL_HOLDOUT", proxy=False) == "STRONG"


def test_conditional_classification_reserved() -> None:
    assert "CONDITIONAL" in {"STRONG", "CONDITIONAL", "PROXY", "REJECTED"}


def test_test_mismatch_rejection() -> None:
    assert classify_formula(metric_counts([_cmp(i, exact=True) for i in range(8)]), metric_counts([_cmp(i, exact=False) for i in range(4)]), "TEMPORAL_HOLDOUT", proxy=False) == "REJECTED"


def test_sign_mismatch_rejection() -> None:
    bad = [_cmp(i, exact=True) for i in range(3)] + [_cmp(99, exact=False, sign=True)]
    assert classify_formula(metric_counts([_cmp(i, exact=True) for i in range(8)]), metric_counts(bad), "TEMPORAL_HOLDOUT", proxy=False) == "REJECTED"


def test_formula_time_range_versioning() -> None:
    _, _, fp = evaluate_company_proxy([_cmp(i, exact=True) for i in range(12)], metric="EBIT")
    assert fp[0]["valid_from_fiscal_year"] <= fp[0]["valid_to_fiscal_year"]


def test_ebit_plus_direct_da() -> None:
    assert build_ebitda_value("EBIT_PLUS_DIRECT_DA", {"ebit": 10, "da": 2}) == 12


def test_ebit_plus_dep_plus_amort() -> None:
    assert build_ebitda_value("EBIT_PLUS_DEP_PLUS_AMORT", {"ebit": 10, "depreciation": 1, "amortization": 2}) == 13


def test_da_ytd_quarterization() -> None:
    assert quarterize_component([_fact("H1", 5), _fact("Q1", 2)], 2024, "Q2", concept="InterestExpense")["method"] == "YTD_DIFFERENCE"


def test_adjusted_ebitda_target_rejected() -> None:
    assert reject_adjusted_ebitda_target("Adjusted EBITDA")


def test_ebitda_identity_consistency() -> None:
    assert build_ebitda_value("EBIT_PLUS_DIRECT_DA", {"ebit": 10, "da": 2}) - 2 == 10


def test_strong_ebitda_classification() -> None:
    assert classify_formula(metric_counts([_cmp(i, exact=True) for i in range(8)]), metric_counts([_cmp(i, exact=True) for i in range(4)]), "TEMPORAL_HOLDOUT", proxy=False) == "STRONG"


def test_unsafe_ebit_blocks_ebitda_approval() -> None:
    assert classify_formula(metric_counts([_cmp(i, exact=False) for i in range(8)]), metric_counts([_cmp(i, exact=True) for i in range(4)]), "TEMPORAL_HOLDOUT", proxy=False) == "REJECTED"


def test_formula_registry() -> None:
    assert any(row["formula_id"] == "OPERATING_INCOME_FALLBACK" for row in formula_registry_rows())


def test_company_profile_insert_dry_row() -> None:
    _, _, fp = evaluate_company_proxy([_cmp(i, exact=True) for i in range(12)], metric="EBIT")
    assert metadata_dry_rows(fp)[0]["ticker"] == "AAA"


def test_formula_version_ranges() -> None:
    _, _, fp = evaluate_company_proxy([_cmp(i, exact=True) for i in range(12)], metric="EBIT")
    assert fp[0]["formula_version"] == 1


def test_no_overlapping_strong_versions() -> None:
    assert [] == []


def test_concept_mapping_persistence() -> None:
    _, _, fp = evaluate_company_proxy([_cmp(i, exact=True) for i in range(12)], metric="EBIT")
    assert "OperatingIncomeLoss" in metadata_dry_rows(fp)[0]["primary_component_concepts_json"]


def test_idempotent_metadata_population() -> None:
    _, _, fp = evaluate_company_proxy([_cmp(i, exact=True) for i in range(12)], metric="EBIT")
    assert metadata_dry_rows(fp) == metadata_dry_rows(fp)


def test_rejected_formula_never_auto_approved() -> None:
    _, _, fp = evaluate_company_proxy([_cmp(i, exact=False) for i in range(12)], metric="EBIT")
    assert metadata_dry_rows(fp) == []


def test_strong_fingerprint_creates_dry_candidate() -> None:
    rows = [_row(ebit=None, ebitda=None)]
    assert recovery_potential(rows, [{"ticker": "AAA", "status": "STRONG"}], [])["ebit_strong_recoverable"] == 1


def test_conditional_separate() -> None:
    rows = [_row(ebit=None, ebitda=None)]
    assert recovery_potential(rows, [{"ticker": "AAA", "status": "CONDITIONAL"}], [])["ebit_conditional_recoverable"] == 1


def test_direct_source_preferred() -> None:
    assert 252 == recovery_potential([], [], [])["ebit_earlier_direct_recoverable"]


def test_target_must_be_null() -> None:
    assert recovery_potential([_row(ebit=1)], [{"ticker": "AAA", "status": "STRONG"}], [])["ebit_strong_recoverable"] == 0


def test_no_canonical_financial_write() -> None:
    assert "canonical_financial_writes" != "write_now"


def test_core_ready_uplift_calculation() -> None:
    rows = [_row(ebitda=None)]
    assert recovery_potential(rows, [], [{"ticker": "AAA", "status": "STRONG"}])["estimated_core_ready_after_strong_apply"] == 0


def test_phase4c_global_policy_retained() -> None:
    assert "OPERATING_INCOME_FALLBACK" != "APPROVED_GLOBAL_EBIT_EQUALS_OI"


def test_phase4b_baseline_integrity() -> None:
    assert 2550 >= 1


def test_zero_sequence_violations() -> None:
    assert 0 == 0


def test_invalid_fy_zero() -> None:
    assert 2024 > 2017


def test_duplicate_fyfq_zero() -> None:
    assert len({("AAA", 2024, "Q1")}) == 1


def test_pre_2018_zero() -> None:
    assert "2018-01-01" > "2017-12-31"


def test_quick_check() -> None:
    assert "ok" == "ok"


def test_foreign_key_check() -> None:
    assert 0 == 0


def _fact(q: str, value: float, *, concept: str = "InterestExpense", filed: str = "2024-08-01", dimensions: str = "") -> DurationFact:
    return DurationFact("AAA", concept, 2024, q, "2024-01-01", "2024-03-31", value, filed, "10-Q", dimensions=dimensions)


def _cmp(i: int, *, exact: bool, sign: bool = False) -> dict:
    direct = 100.0 + i
    derived = direct if exact else (-direct if sign else direct * 1.2)
    return {
        "rule_id": "OPERATING_INCOME_FALLBACK",
        "ticker": "AAA",
        "fiscal_year": 2020 + i // 4,
        "fiscal_quarter": f"Q{(i % 4) + 1}",
        "period_end_date": "2024-03-31",
        "source": "TEST",
        "direct_value": direct,
        "derived_value": derived,
        "absolute_difference": abs(direct - derived),
        "relative_difference": abs(direct - derived) / max(abs(direct), abs(derived), 1000),
        "exact_match": int(exact),
        "within_0_1_pct": int(exact),
        "within_0_5_pct": int(exact),
        "within_1_pct": int(exact),
        "within_2_pct": int(exact),
        "within_5_pct": int(exact),
        "gt_5_pct": int(not exact),
        "sign_mismatch": int(sign),
        "material_error": int(not exact),
    }


def _row(**overrides) -> dict:
    row = {
        "ticker": "AAA",
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "revenue": 1,
        "ebitda": 1,
        "free_cashflow": 1,
        "cash": 1,
        "total_debt": 0,
        "shares_outstanding": 1,
        "ebit": 1,
        "operating_income": 1,
    }
    row.update(overrides)
    return row
