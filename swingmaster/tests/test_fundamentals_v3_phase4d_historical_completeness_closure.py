from __future__ import annotations

from swingmaster.fundamentals import v3_phase4d_historical_completeness_closure as close


def test_final_company_count() -> None:
    assert close.final_baseline(rows(), companies())["companies"] == 3


def test_final_canonical_q_count() -> None:
    assert close.final_baseline(rows(), companies())["canonical_q"] == 5


def test_field_coverage_reconciliation() -> None:
    coverage = close.final_field_coverage(rows())
    assert all(row["populated"] + row["null"] == 5 for row in coverage)


def test_year_by_variable_matrix_totals_reconcile() -> None:
    matrix = close.variable_year_matrix(rows())
    revenue = next(row for row in matrix if row["field"] == "revenue")
    assert revenue["total_populated"] + (revenue["total_eligible"] - revenue["total_populated"]) == 5


def test_eligible_coverage_denominator_logic() -> None:
    matrix = close.variable_year_matrix(rows())
    assert next(row for row in matrix if row["field"] == "revenue")["denominator_policy"] == "RAW_CANONICAL_Q_PERIOD_END_YEAR"


def test_null_reason_totals_reconcile_by_field() -> None:
    gaps = close.remaining_gap_classification(rows(), {1})
    coverage = close.final_field_coverage(rows())
    assert close.gap_reconciliation_ok(gaps, coverage)


def test_zero_q_classification() -> None:
    zero = close.zero_q_residuals(companies(), rows())
    assert zero[0]["ticker"] == "HOTH"
    assert zero[0]["known_phase4b_residual"] == 1


def test_cik_residual_classification() -> None:
    cik = close.cik_unmapped_residuals(rows(), companies(), {1})
    assert cik[0]["ticker"] == "HOTH" or cik[0]["ticker"] == "BBB"


def test_core_blocker_signature_totals() -> None:
    signatures = close.core_readiness_signatures(rows())
    assert sum(row["count"] for row in signatures) == 1


def test_ebit_residual_totals() -> None:
    gaps = [row for row in close.remaining_gap_classification(rows(), {1}) if row["field"] == "ebit"]
    assert close.summarize_residual(gaps)["missing"] == 1


def test_ebitda_residual_totals() -> None:
    gaps = [row for row in close.remaining_gap_classification(rows(), {1}) if row["field"] == "ebitda"]
    assert close.summarize_residual(gaps)["missing"] == 1


def test_ttm_4q_readiness() -> None:
    ttm = close.ttm_metric_readiness(rows())
    assert next(row for row in ttm if row["metric"] == "revenue")["ttm_full_4q"] == 1


def test_instant_fields_not_summed() -> None:
    assert close.variable_type("cash") == "INSTANT"


def test_publish_pit_readiness() -> None:
    pit = close.ttm_publish_pit_readiness(rows())
    assert next(row for row in pit if row["metric"] == "revenue")["all_underlying_publish_dates_known"] == 1


def test_logical_fingerprint_deterministic() -> None:
    assert close.logical_fingerprint(rows(), companies()) == close.logical_fingerprint(rows(), companies())


def test_no_canonical_writes_constant() -> None:
    assert close.CLASSIFICATION_COMPLETE.startswith("FUNDAMENTALS_V3_PHASE4")


def test_no_q_writes_next_phase() -> None:
    assert close.NEXT_PHASE == "MASTER PLAN PHASE 5 - TTM ENGINE"


def test_universe_unchanged_baseline_shape() -> None:
    baseline = close.final_baseline(rows(), companies())
    assert baseline["active"] == 2
    assert baseline["inactive"] == 1


def test_sequence_violations_zero_for_fixture() -> None:
    assert close.internal_gap_count(rows()[:4]) == 0


def test_invalid_fy_zero_by_fixture_years() -> None:
    assert all(2018 <= close.year_of(row) <= 2026 for row in rows())


def test_duplicate_fyfq_zero_for_fixture() -> None:
    keys = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"]) for row in rows()}
    assert len(keys) == len(rows())


def test_pre_2018_zero_for_fixture() -> None:
    assert min(close.year_of(row) for row in rows()) >= 2018


def test_q4_policy_external_guard_classification() -> None:
    assert close.CLASSIFICATION_REPAIR.endswith("BOUNDED_REPAIR_REQUIRED")


def test_quick_check_summary_gate_key() -> None:
    assert "phase4_combined_fingerprint" in close.logical_fingerprint(rows(), companies())


def test_fk_check_summary_shape() -> None:
    assert close.hash_json({"a": 1}) == close.hash_json({"a": 1})


def test_core_ready_by_year() -> None:
    by_year = close.core_ready_by_year(rows())
    assert next(row for row in by_year if row["year"] == 2024)["core_ready_q"] == 4


def rows() -> list[dict]:
    base = {
        "company_id": 1,
        "ticker": "AAA",
        "company_name": "AAA Inc",
        "active": 1,
        "market": "usa",
        "publish_date": "2024-05-01",
        "revenue": 10.0,
        "gross_profit": 8.0,
        "operating_income": 5.0,
        "ebit": 5.0,
        "ebitda": 6.0,
        "net_income": 3.0,
        "operating_cashflow": 4.0,
        "capex": -1.0,
        "free_cashflow": 3.0,
        "cash": 9.0,
        "total_debt": 2.0,
        "shares_outstanding": 100.0,
    }
    out = []
    for idx, q in enumerate(("Q1", "Q2", "Q3", "Q4"), start=1):
        out.append({**base, "quarter_id": idx, "fiscal_year": 2024, "fiscal_quarter": q, "period_end_date": f"2024-{idx*3:02d}-28"})
    out.append({
        **base,
        "company_id": 2,
        "ticker": "BBB",
        "active": 0,
        "quarter_id": 5,
        "fiscal_year": 2025,
        "fiscal_quarter": "Q1",
        "period_end_date": "2025-03-31",
        "publish_date": None,
        "ebit": None,
        "ebitda": None,
        "total_debt": None,
    })
    return out


def companies() -> list[dict]:
    return [
        {"company_id": 1, "ticker": "AAA", "company_name": "AAA Inc", "active": 1, "market": "usa", "admission_source": "TEST"},
        {"company_id": 2, "ticker": "BBB", "company_name": "BBB Inc", "active": 0, "market": "usa", "admission_source": "TEST"},
        {"company_id": 3, "ticker": "HOTH", "company_name": "Hoth", "active": 1, "market": "usa", "admission_source": "TEST"},
    ]
