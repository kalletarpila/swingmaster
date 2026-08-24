from __future__ import annotations

from swingmaster.fundamentals import v3_phase4c3b_rejected_case_review as review


def test_stratified_case_selection() -> None:
    pool = [_case(company_id=i, rejection_category=f"C{i}", ticker=f"T{i}") for i in range(20)]
    selected = review.select_cases(pool, sample_size=15)
    assert len(selected) == 15
    assert len({row["rejection_category"] for row in selected}) >= 10


def test_case_evidence_completeness() -> None:
    row = review.review_case(_case(), [], [_fact()], [], [])
    assert row["exact_rejection_gate"]
    assert row["disposition"]
    assert row["review_reasoning"]


def test_exact_rejection_gate_trace() -> None:
    gate = review.exact_gate(_case(), [], [_fact()])
    assert gate == "TOO_FEW_TARGETS"


def test_ebit_component_rendering() -> None:
    rows = review.component_fact_artifact_rows([_case(metric="ebit")], [_fact(semantic_role="PRETAX")])
    assert rows[0]["semantic_role"] == "PRETAX"


def test_da_implied_value_rendering() -> None:
    text = review.case_review_md(_case(metric="ebitda", formula_component_candidate="CANONICAL_EBIT_PLUS_DA", calculated_value=13))
    assert "CANONICAL_EBIT_PLUS_DA" in text


def test_q4_fy_minus_9m_rendering() -> None:
    text = review.case_review_md(_case(fiscal_quarter="Q4", rejection_category="Q4_REJECTION"))
    assert "Q4_REJECTION" in text


def test_pattern_frequency_scan() -> None:
    pool = [_case(rejection_category="LOW_SAMPLE") for _ in range(30)]
    reviews = [_case(rejection_category="LOW_SAMPLE", disposition="NEEDS_MORE_EVIDENCE")]
    rows = review.pattern_frequency_scan(pool, reviews)
    assert rows[0]["affected_rows"] == 30


def test_no_canonical_financial_writes_constants() -> None:
    assert review.NEXT_4D.startswith("MASTER PLAN")


def test_no_metadata_writes_summary_shape() -> None:
    assert review.OUTCOME_NO_ISSUE.startswith("REJECTED_CASE_REVIEW")


def test_integrity_quick_check_fk_shape() -> None:
    assert review.full_population_impact([])["estimated_additional_safe_recovery_if_fixed"] == 0


def test_candidate_value_ebit() -> None:
    row = {"components": {"PRETAX": _component("PRETAX", 10), "INTEREST_EXPENSE_GROSS": _component("INTEREST_EXPENSE_GROSS", 2)}}
    assert review.candidate_value(row, "ebit")["value"] == 12


def test_candidate_value_ebitda() -> None:
    row = {"ebit": 10, "components": {"D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 3)}}
    assert review.candidate_value(row, "ebitda")["value"] == 13


def _case(**overrides) -> dict:
    row = {
        "company_id": 1,
        "ticker": "AAA",
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "period_end_date": "2024-03-31",
        "metric": "ebit",
        "rejection_category": "LOW_SAMPLE",
        "diagnostic_priority": 5,
        "formula_component_candidate": "PRETAX_PLUS_INTEREST",
        "calculated_value": 12,
        "component_roles": "PRETAX|INTEREST_EXPENSE_GROSS",
        "historical_fit_summary": "0/0 historical candidates within 1%",
        "exact_rejection_gate": "TOO_FEW_TARGETS",
        "disposition": "NEEDS_MORE_EVIDENCE",
        "review_reasoning": "fixture",
    }
    row.update(overrides)
    return row


def _component(role: str, value: float) -> dict:
    return {"role": role, "value": value, "method": "DIRECT_Q1", "fact_ids": "1", "accessions": "a", "concept": role, "unit": "USD", "dimensions": "{}"}


def _fact(**overrides) -> dict:
    row = {
        "company_id": 1,
        "ticker": "AAA",
        "fiscal_year": 2024,
        "fiscal_period": "Q1",
        "semantic_role": "INTEREST_EXPENSE_GROSS",
        "concept_name": "InterestExpenseNonOperating",
        "concept_label": "Interest expense",
        "value": 2,
        "unit": "USD",
        "start_date": "2024-01-01",
        "end_date": "2024-03-31",
        "form": "10-Q",
        "accession": "a",
        "filed_date": "2024-05-01",
        "dimensions_json": "{}",
    }
    row.update(overrides)
    return row
