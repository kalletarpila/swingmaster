from __future__ import annotations

from pathlib import Path

import pytest

from swingmaster.fundamentals import v3_phase4c2b_simfin_validation as simfin


def test_parse_float_blank_is_none() -> None:
    assert simfin.parse_float("") is None


def test_parse_float_number() -> None:
    assert simfin.parse_float("12.5") == 12.5


def test_maybe_sum_requires_both_values() -> None:
    assert simfin.maybe_sum(1, None) is None


def test_maybe_sum_adds_values() -> None:
    assert simfin.maybe_sum(1, 2) == 3.0


def test_core_ready_requires_all_core_fields() -> None:
    assert simfin.core_ready(_v3(shares_outstanding=1.0))


def test_core_ready_rejects_zero_shares() -> None:
    assert not simfin.core_ready(_v3(shares_outstanding=0.0))


def test_identity_base_uses_income_report_date_first() -> None:
    assert simfin.identity_base(_sf(income_report_date="2024-03-31"))["simfin_report_date"] == "2024-03-31"


def test_join_rows_prefixes_simfin_fields() -> None:
    rows = simfin.join_rows({("AAA", 2024, "Q1"): _v3()}, {("AAA", 2024, "Q1"): _sf(revenue=1.0)})
    assert rows[0]["simfin_revenue"] == 1.0


def test_build_identity_candidates_matches_exact_fyfq() -> None:
    rows = simfin.build_identity_candidates({("AAA", 2024, "Q1"): _v3()}, {("AAA", 2024, "Q1"): _sf()})
    assert rows[0]["identity_classification"] == "EXACT_FYFQ"


def test_build_identity_candidates_marks_same_q_period_compatible() -> None:
    v3 = _v3(period_end_date="2024-03-31")
    sf = _sf(income_report_date="2024-03-31")
    rows = simfin.build_identity_candidates({("AAA", 2024, "Q1"): v3}, {("AAA", 2024, "Q1"): sf})
    assert rows[0]["identity_classification"] == "SAME_Q_PERIOD_COMPATIBLE"


def test_build_identity_candidates_marks_no_canonical_match() -> None:
    rows = simfin.build_identity_candidates({}, {("AAA", 2024, "Q1"): _sf()})
    assert rows[0]["identity_classification"] == "NO_CANONICAL_MATCH"


def test_summarize_identity_counts_matches() -> None:
    summary = simfin.summarize_identity([
        {"identity_classification": "EXACT_FYFQ"},
        {"identity_classification": "SAME_Q_PERIOD_COMPATIBLE"},
        {"identity_classification": "NO_CANONICAL_MATCH"},
    ])
    assert summary["identity_precision_assessment"] == pytest.approx(2 / 3)


def test_publish_date_validation_accepts_exact_match() -> None:
    rows = simfin.publish_date_validation([{**_v3(publish_date="2024-05-01"), "simfin_publish_date": "2024-05-01"}])
    assert rows[0]["exact_date"] == 1


def test_publish_date_validation_marks_material_difference() -> None:
    rows = simfin.publish_date_validation([{**_v3(publish_date="2024-05-01"), "simfin_publish_date": "2024-05-10"}])
    assert rows[0]["material_difference"] == 1


def test_classify_field_approves_direct_high_quality() -> None:
    assert simfin.classify_field(_metrics(observations=200, within_1_pct_rate=0.96, material_error_rate=0, sign_mismatch=0)) == "APPROVED_DIRECT"


def test_classify_field_approves_conditionally() -> None:
    assert simfin.classify_field(_metrics(observations=200, within_1_pct_rate=0.50, within_5_pct_rate=0.91, material_error_rate=0.01)) == "APPROVED_CONDITIONALLY"


def test_classify_field_rejects_low_sample() -> None:
    assert simfin.classify_field(_metrics(observations=99)) == "SEMANTICS_UNRESOLVED"


def test_classify_field_validation_only_for_weak_match() -> None:
    assert simfin.classify_field(_metrics(observations=200, within_1_pct_rate=0.1, within_5_pct_rate=0.2, material_error_rate=0.2)) == "VALIDATION_ONLY"


def test_field_policy_forces_shares_not_approved() -> None:
    rows = simfin.field_policy_rows({"shares_outstanding": [_cmp()] * 120})
    assert rows[0]["classification"] == "NOT_APPROVED_FOR_CANONICAL_SHARES"


def test_field_policy_forces_cash_validation_only() -> None:
    rows = simfin.field_policy_rows({"cash": [_cmp()] * 120})
    assert rows[0]["classification"] == "VALIDATION_ONLY"


def test_field_policy_forces_capex_validation_only() -> None:
    rows = simfin.field_policy_rows({"capex": [_cmp()] * 120})
    assert rows[0]["classification"] == "VALIDATION_ONLY"


def test_field_policy_publish_date_conditional_when_dates_close() -> None:
    rows = simfin.field_policy_rows({"publish_date": [{"exact_date": 1, "within_3_days": 1, "material_difference": 0}]})
    assert rows[0]["classification"] == "APPROVED_CONDITIONALLY"


def test_ebit_formula_candidates_include_four_formulas() -> None:
    rows = simfin.ebit_formula_candidates([_joined(ebit=12.0, simfin_pretax=10.0, simfin_interest_expense_net=2.0)])
    assert len({row["formula_id"] for row in rows}) == 4


def test_ebit_reported_formula_adds_interest() -> None:
    row = next(r for r in simfin.ebit_formula_candidates([_joined(ebit=12.0, simfin_pretax=10.0, simfin_interest_expense_net=2.0)]) if r["formula_id"].endswith("REPORTED"))
    assert row["derived_value"] == 12.0


def test_classify_formula_strong_for_clean_train_test() -> None:
    assert simfin.classify_formula("FORMULA", _metrics(observations=8, within_1_pct_rate=1), _metrics(observations=4, within_1_pct_rate=1, material_errors=0, sign_mismatch=0)) == "STRONG"


def test_classify_formula_proxy_for_proxy_formula() -> None:
    assert simfin.classify_formula("FORMULA_PROXY", _metrics(observations=8, within_1_pct_rate=1), _metrics(observations=4, within_1_pct_rate=1, material_errors=0, sign_mismatch=0)) == "PROXY"


def test_classify_formula_rejects_short_test() -> None:
    assert simfin.classify_formula("FORMULA", _metrics(observations=8, within_1_pct_rate=1), _metrics(observations=3)) == "INSUFFICIENT_SAMPLE"


def test_company_formula_discovery_uses_temporal_holdout() -> None:
    candidates = [_candidate("AAA", y, q, "F1") for y in range(2020, 2023) for q in ("Q1", "Q2", "Q3", "Q4")]
    train, test, fingerprints = simfin.company_formula_discovery(candidates, metric="EBIT")
    assert len(train) == 8 and len(test) == 4 and fingerprints[0]["status"] == "STRONG"


def test_income_vs_cashflow_da_compares_sources() -> None:
    rows = simfin.income_vs_cashflow_da([_joined(simfin_income_da=2.0, simfin_cashflow_da=2.0)])
    assert rows[0]["exact_match"] == 1


def test_implied_da_validation_uses_ebitda_minus_ebit() -> None:
    rows = simfin.implied_da_validation([_joined(ebit=10.0, ebitda=12.0, simfin_income_da=2.0)])
    assert rows[0]["direct_value"] == 2.0


def test_ebitda_formula_requires_strong_ebit_fingerprint() -> None:
    rows = simfin.ebitda_formula_candidates([_joined(ebit=10.0, ebitda=12.0, simfin_income_da=2.0)], [])
    assert all(row["formula_id"] == "SIMFIN_EBITDA_F4_OPERATING_INCOME_PROXY_PLUS_CASHFLOW_DA" for row in rows)


def test_ebitda_formula_allows_strong_ebit_fingerprint() -> None:
    rows = simfin.ebitda_formula_candidates([_joined(ebit=10.0, ebitda=12.0, simfin_income_da=2.0)], [{"ticker": "AAA", "formula_id": "F1", "status": "STRONG"}])
    assert rows


def test_dry_multifield_recovery_fills_null_only() -> None:
    policy = [{"field": "revenue", "classification": "APPROVED_DIRECT"}]
    rows = simfin.dry_multifield_recovery({("AAA", 2024, "Q1"): _v3(revenue=None)}, {("AAA", 2024, "Q1"): _sf(revenue=5.0)}, policy)
    assert rows[0]["target_field"] == "revenue"


def test_dry_multifield_recovery_does_not_overwrite() -> None:
    policy = [{"field": "revenue", "classification": "APPROVED_DIRECT"}]
    assert simfin.dry_multifield_recovery({("AAA", 2024, "Q1"): _v3(revenue=1.0)}, {("AAA", 2024, "Q1"): _sf(revenue=5.0)}, policy) == []


def test_dry_formula_recovery_requires_strong_fingerprint() -> None:
    rows = simfin.dry_formula_recovery({("AAA", 2024, "Q1"): _v3(ebit=None)}, {("AAA", 2024, "Q1"): _sf()}, [{"ticker": "AAA", "status": "CONDITIONAL"}], "ebit")
    assert rows == []


def test_core_ready_uplift_estimates_direct_fill() -> None:
    rows = {("AAA", 2024, "Q1"): _v3(ebitda=None)}
    uplift = simfin.core_ready_uplift(rows, [], [{"ticker": "AAA", "fiscal_year": 2024, "fiscal_quarter": "Q1", "target_field": "ebitda"}])
    assert uplift["combined_strong_uplift"] == 1


def test_period_semantics_declares_standalone_quarter() -> None:
    summary = simfin.period_semantics({("AAA", 2024, "Q1"): _sf()})
    assert summary["flow_rows_are"] == "STANDALONE_QUARTER"


def test_summarize_metric_recovery_keeps_prior_direct_candidates() -> None:
    summary = simfin.summarize_metric_recovery([{"x": 1}], missing=10, direct=252)
    assert summary["earlier_direct_candidates"] == 252


def test_recovery_by_quarter_groups_targets() -> None:
    rows = simfin.recovery_by_quarter([{"target_field": "revenue", "fiscal_quarter": "Q1"}])
    assert rows == [{"target_field": "revenue", "fiscal_quarter": "Q1", "rows": 1}]


@pytest.mark.parametrize(
    ("status", "rank"),
    [
        ("STRONG", 5),
        ("CONDITIONAL", 4),
        ("PROXY", 3),
        ("REJECTED", 2),
        ("INSUFFICIENT_SAMPLE", 1),
        ("UNKNOWN", 0),
    ],
)
def test_status_rank_order(status: str, rank: int) -> None:
    assert simfin.status_rank(status) == rank


@pytest.mark.parametrize(
    "field",
    ["revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow", "capex", "free_cashflow", "cash", "total_debt", "shares_outstanding"],
)
def test_field_map_contains_required_fields(field: str) -> None:
    assert field in simfin.FIELD_MAP


@pytest.mark.parametrize("ticker", ["CAVA", "NEUP", "LFCR", "BNC", "SJM", "LYTS", "BCTX", "FERG", "JKHY", "OLLI", "RH", "SGLY"])
def test_special_ticker_set_contains_required_cases(ticker: str) -> None:
    assert ticker in simfin.SPECIAL_TICKERS


def test_special_case_regression_does_not_reopen_identity() -> None:
    rows = simfin.special_case_regression({("CAVA", 2024, "Q1"): _v3()}, {("CAVA", 2024, "Q1"): _sf()})
    assert rows[0]["status"] == "NO_IDENTITY_REOPENED"


def test_summarize_q4_counts_q4_rows() -> None:
    summary = simfin.summarize_q4({("AAA", 2024, "Q4"): _sf(fiscal_quarter="Q4")}, [], [])
    assert summary["simfin_q4_rows"] == 1


def test_summarize_policy_returns_field_lookup() -> None:
    assert simfin.summarize_policy([{"field": "revenue", "classification": "APPROVED_DIRECT"}])["revenue"]["classification"] == "APPROVED_DIRECT"


def test_summarize_da_reports_preferred_method() -> None:
    summary = simfin.summarize_da([], [])
    assert summary["preferred_da_method_companies"] == "CASHFLOW_DA_FOR_V2_COMPATIBILITY_VALIDATION_ONLY"


def test_write_csv_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    simfin.write_csv(path, [])
    assert path.read_text(encoding="utf-8") == ""


def test_write_csv_with_header(tmp_path: Path) -> None:
    path = tmp_path / "rows.csv"
    simfin.write_csv(path, [{"b": 2, "a": 1}])
    assert path.read_text(encoding="utf-8").splitlines()[0] == "a,b"


def test_write_text(tmp_path: Path) -> None:
    path = tmp_path / "note.md"
    simfin.write_text(path, "ok\n")
    assert path.read_text(encoding="utf-8") == "ok\n"


def test_write_artifacts_uses_required_validation_names(tmp_path: Path) -> None:
    simfin.write_artifacts(
        tmp_path,
        summary={
            "period_semantics": {},
            "q4": {},
            "source_architecture": {"simfin_role": "role", "durable_formula_metadata_justified": False, "sec_future_role": "sec"},
            "classification": simfin.CLASSIFICATION,
            "recommended_next_step": simfin.NEXT_STEP,
            "field_policy": {},
        },
        files=[],
        simfin_rows={},
        identity=[],
        special=[],
        field_validations={"operating_cashflow": [], "free_cashflow": [], "total_debt": [], "shares_outstanding": []},
        field_policy=[],
        ebit_candidates=[],
        ebit_train=[],
        ebit_test=[],
        ebit_fingerprints=[],
        da_rows=[],
        implied_da=[],
        ebitda_candidates=[],
        ebitda_train=[],
        ebitda_test=[],
        ebitda_fingerprints=[],
        multifield_recovery=[],
        ebit_dry=[],
        ebitda_dry=[],
        core_uplift={},
    )
    assert (tmp_path / "ocf_validation.csv").exists()
    assert (tmp_path / "fcf_validation.csv").exists()
    assert (tmp_path / "debt_validation.csv").exists()
    assert (tmp_path / "shares_validation.csv").exists()


def _v3(**overrides):
    row = {
        "ticker": "AAA",
        "active": 1,
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "period_end_date": "2024-03-31",
        "publish_date": "2024-05-01",
        "revenue": 1.0,
        "gross_profit": 1.0,
        "operating_income": 1.0,
        "ebit": 10.0,
        "ebitda": 12.0,
        "net_income": 1.0,
        "operating_cashflow": 1.0,
        "capex": -1.0,
        "free_cashflow": 1.0,
        "cash": 1.0,
        "total_debt": 0.0,
        "shares_outstanding": 1.0,
    }
    row.update(overrides)
    return row


def _sf(**overrides):
    row = {
        "ticker": "AAA",
        "fiscal_year": 2024,
        "fiscal_quarter": "Q1",
        "income_report_date": "2024-03-30",
        "cashflow_report_date": "2024-03-30",
        "balance_report_date": "2024-03-30",
        "publish_date": "2024-05-01",
        "revenue": 1.0,
        "gross_profit": 1.0,
        "operating_income": 1.0,
        "net_income": 1.0,
        "operating_cashflow": 1.0,
        "capex": -1.0,
        "free_cashflow": 0.0,
        "cash": 1.0,
        "total_debt": 0.0,
        "shares_basic": 1.0,
    }
    row.update(overrides)
    return row


def _joined(**overrides):
    row = {**_v3(), **{f"simfin_{k}": v for k, v in _sf().items()}}
    row.update({
        "simfin_pretax": 10.0,
        "simfin_interest_expense_net": 2.0,
        "simfin_pretax_adj": 10.0,
        "simfin_operating_income": 12.0,
        "simfin_income_da": 2.0,
        "simfin_cashflow_da": 2.0,
    })
    row.update(overrides)
    return row


def _cmp():
    return {
        "exact_match": 1,
        "within_0_1_pct": 1,
        "within_0_5_pct": 1,
        "within_1_pct": 1,
        "within_2_pct": 1,
        "within_5_pct": 1,
        "gt_5_pct": 0,
        "relative_difference": 0.0,
        "sign_mismatch": 0,
        "material_error": 0,
    }


def _metrics(**overrides):
    row = {
        "observations": 100,
        "within_1_pct_rate": 0.0,
        "within_5_pct_rate": 0.0,
        "material_error_rate": 0.0,
        "material_errors": 0,
        "sign_mismatch": 0,
    }
    row.update(overrides)
    return row


def _candidate(ticker: str, fiscal_year: int, fiscal_quarter: str, formula: str):
    return {
        **_cmp(),
        "ticker": ticker,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "formula_id": formula,
    }
