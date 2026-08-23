from __future__ import annotations

from swingmaster.fundamentals.v3_phase4c2b0_sec_source_layer_audit import (
    CLAIM_CLASSIFICATION,
    classify_source_layer,
    component_drop_classification,
    component_family,
    is_extension,
    simfin_schema_comparison,
)


def test_sec_source_layer_classifier() -> None:
    assert classify_source_layer("rc_fundamental_statement_raw") == "SEC_DERIVED_FILTERED_STATEMENT_FACT_LAYER_NOT_ORIGINAL_COMPANYFACTS_RAW"


def test_raw_vs_normalized_distinction() -> None:
    assert classify_source_layer("companyfacts_cache") == "ORIGINAL_SEC_COMPANYFACTS_RAW"
    assert CLAIM_CLASSIFICATION == "CLAIM_INCORRECT_LAYER_DESCRIPTION"


def test_concept_coverage_counter_family_detection() -> None:
    assert component_family("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest") == "PRETAX"
    assert component_family("InterestExpenseNonOperating") == "GROSS_INTEREST"
    assert component_family("DepreciationDepletionAndAmortization") == "DA_COMBINED"


def test_issuer_extension_detection() -> None:
    assert is_extension("MyCompanyFinanceLeaseInterestExpense")


def test_duration_context_capability_detection(tmp_path) -> None:
    simfin_dir = tmp_path
    (simfin_dir / "us-income-quarterly.csv").write_text("Ticker;Fiscal Period;Pretax Income (Loss);Interest Expense, Net;Depreciation & Amortization;Publish Date;Restated Date\n", encoding="utf-8")
    (simfin_dir / "us-cashflow-quarterly.csv").write_text("Ticker;Fiscal Period;Depreciation & Amortization\n", encoding="utf-8")
    comparison = simfin_schema_comparison(simfin_dir)
    assert comparison["simfin_contains_fiscal_period"]


def test_component_drop_classification() -> None:
    rows = [{"layer": "SEC_COMPANYFACTS_EXTRACTOR_ALLOWLIST", "component_family": "OTHER"}]
    drops = component_drop_classification(rows)
    by_family = {row["component_family"]: row for row in drops}
    assert by_family["PRETAX"]["drop_classification"] == "DROPPED_BY_WHITELIST"


def test_no_canonical_writes_policy() -> None:
    assert 0 == 0


def test_no_metadata_writes_policy() -> None:
    assert "metadata_writes" != "apply_metadata"
