from __future__ import annotations

from swingmaster.fundamentals.v3_legacy_hold_recovery import (
    READY_STATES,
    SecPeriodEvidence,
    build_dry_import_plan,
    build_q4_construction_plan,
    classify_final_rows,
    expected_contribution,
    final_classification,
    historical_gap_inventory,
    legacy_fiscal_year_structures,
    parse_sec_field_name,
    q4_field_eligibility_rows,
    q4_presence_by_year,
    resolve_duplicate_ready_fyfqs,
    sequence_validation,
    transition_population_reanalysis,
    validate_q4_against_known_canonical,
    v2_historical_anchor_calibration,
)


def test_q1_q2_q3_fy_is_normal_sec_annual_structure() -> None:
    structures = legacy_fiscal_year_structures(_sec_year("AAA", 2025, ("Q1", "Q2", "Q3", "FY")))

    assert structures[0]["structure"] == "Q1_Q2_Q3_FY"


def test_missing_explicit_q4_does_not_create_fiscal_transition() -> None:
    presence = q4_presence_by_year(legacy_fiscal_year_structures(_sec_year("AAA", 2025, ("Q1", "Q2", "Q3", "FY"))))

    assert presence[0]["q4_state"] == "EXPECTED_SEC_Q4_NOT_SEPARATELY_FILED"


def test_q4_fy_minus_9m_revenue_derivation_policy_exists() -> None:
    row = next(row for row in q4_field_eligibility_rows() if row["field"] == "operating_cashflow")

    assert row["eligibility"] == "SAFE_DIRECT_FY_MINUS_9M"


def test_q4_fy_minus_q1_q2_q3_derivation_policy_exists() -> None:
    row = next(row for row in q4_field_eligibility_rows() if row["field"] == "revenue")

    assert row["eligibility"] == "SAFE_FY_MINUS_Q1_Q2_Q3"


def test_instant_cash_uses_fy_end_direct_value() -> None:
    row = next(row for row in q4_field_eligibility_rows() if row["field"] == "cash")

    assert row["eligibility"] == "DIRECT_FY_END_INSTANT"


def test_instant_debt_uses_fy_end_value() -> None:
    row = next(row for row in q4_field_eligibility_rows() if row["field"] == "total_debt")

    assert row["eligibility"] == "DIRECT_FY_END_INSTANT"


def test_instant_shares_are_not_differenced() -> None:
    row = next(row for row in q4_field_eligibility_rows() if row["field"] == "shares_outstanding")

    assert "do not difference" in row["notes"].lower()


def test_incompatible_duration_blocks_derivation_policy() -> None:
    concept, attrs = parse_sec_field_name("Revenue|form=10-K|unit=USD|fy=2025|fp=FY|start=NULL|filed=2026-02-01")

    assert concept == "Revenue"
    assert attrs["start"] == "NULL"


def test_incompatible_concept_blocks_derivation_by_policy() -> None:
    row = next(row for row in q4_field_eligibility_rows() if row["field"] == "ebit")

    assert row["eligibility"] == "SEMANTICALLY_UNSAFE"


def test_missing_inputs_leave_field_null_in_q4_plan() -> None:
    ready = [_ready("AAA", 2025, "Q4", "2025-12-31", "READY_SEC_Q4_STRUCTURE")]
    plan = build_q4_construction_plan(ready, _sec_year("AAA", 2025, ("FY",)), [], {("AAA", "2025-12-31"): {"ticker": "AAA", "period_end_date": "2025-12-31"}})

    assert "UNSUPPORTED_NULL" in plan[0]["field_derivation_methods"]


def test_q4_identity_may_be_valid_with_partial_fields() -> None:
    ready = [_ready("AAA", 2025, "Q4", "2025-12-31", "READY_SEC_Q4_STRUCTURE")]

    assert build_dry_import_plan(ready, [dict(ready[0])])[0]["phase3c2_recommendation"] == "READY_FOR_PHASE3C2_IMPORT"


def test_fcf_derived_from_reconstructed_ocf_plus_capex_policy() -> None:
    ready = [_ready("AAA", 2025, "Q4", "2025-12-31", "READY_SEC_Q4_STRUCTURE")]
    plan = build_q4_construction_plan(ready, _sec_year("AAA", 2025, ("FY",)), [], {("AAA", "2025-12-31"): {"ticker": "AAA", "period_end_date": "2025-12-31", "free_cashflow": 1.0}})

    assert "LEGACY_SEC_DERIVE_FROM_Q4_OCF_PLUS_CAPEX_IF_INPUTS_SAFE" in plan[0]["field_derivation_methods"]


def test_ebitda_unsafe_derivation_remains_null() -> None:
    row = next(row for row in q4_field_eligibility_rows() if row["field"] == "ebitda")

    assert row["eligibility"] == "SEMANTICALLY_UNSAFE"


def test_q4_known_v3_calibration() -> None:
    v3 = [_v3("AAA", 2025, "Q4", "2025-12-31", revenue=100.0)]
    legacy = {("AAA", "2025-12-31"): {"ticker": "AAA", "period_end_date": "2025-12-31", "revenue": 100.0}}

    rows = validate_q4_against_known_canonical(v3, legacy, _sec_year("AAA", 2025, ("FY",)))

    assert any(row["field"] == "revenue" and row["within_1pct"] for row in rows)


def test_restatement_vintage_conflict_blocks_field_derivation() -> None:
    sec = _evidence("AAA", "2025-12-31", 2025, "FY", filed="2026-02-01")
    sec.filed_dates["2026-02-02"] += 1

    assert len(sec.filed_dates) == 2


def test_annual_publication_date_maps_to_q4() -> None:
    row = classify_final_rows([], [_hold("AAA", "2025-12-31")], {}, {("AAA", "2025-12-31"): _evidence("AAA", "2025-12-31", 2025, "FY", filed="2026-02-01")}, {})[0]

    assert row["publish_date"] == "2026-02-01"


def test_sec_fy_q4_identity_uses_period_end_not_sec_fy_label() -> None:
    row = classify_final_rows([], [_hold("AAA", "2018-10-31")], {}, {("AAA", "2018-10-31"): _evidence("AAA", "2018-10-31", 2019, "FY", filed="2018-11-20")}, {})[0]

    assert row["final_disposition"] == "READY_SEC_Q4_STRUCTURE"
    assert row["fiscal_year"] == 2018
    assert row["identity_evidence"] == "SEC_FY_ROW_REPRESENTS_Q4_SLOT_PERIOD_END_ANCHORED"


def test_january_sec_fy_q4_identity_maps_to_prior_canonical_year() -> None:
    row = classify_final_rows([], [_hold("AAA", "2026-01-31")], {}, {("AAA", "2026-01-31"): _evidence("AAA", "2026-01-31", 2026, "FY", filed="2026-03-03")}, {})[0]

    assert row["fiscal_year"] == 2025


def test_invalid_v2_fiscal_year_does_not_become_ready_identity() -> None:
    row = classify_final_rows(
        [],
        [_hold("AAA", "2019-06-30")],
        {},
        {("AAA", "2019-06-30"): _evidence("AAA", "2019-06-30", 2019, "Q2")},
        {("AAA", "2019-06-30"): {"ticker": "AAA", "period_end_date": "2019-06-30", "fiscal_year": 43646, "fiscal_quarter": "Q2"}},
    )[0]

    assert row["final_disposition"] == "HOLD_INSUFFICIENT_EVIDENCE"
    assert row["identity_evidence"] == "V2_EXACT_PERIOD_INVALID_FISCAL_YEAR"


def test_publish_date_may_remain_null() -> None:
    row = classify_final_rows([], [_hold("AAA", "2025-12-31")], {}, {("AAA", "2025-12-31"): _evidence("AAA", "2025-12-31", 2025, "FY", filed=None)}, {})[0]

    assert row["publish_date"] == ""


def test_false_fiscal_transition_becomes_normal_sec_boundary() -> None:
    final = classify_final_rows([], [_hold("AAA", "2025-12-31", disposition="HOLD_TRUE_FISCAL_TRANSITION")], {}, {("AAA", "2025-12-31"): _evidence("AAA", "2025-12-31", 2025, "FY")}, {})
    rows = transition_population_reanalysis([_hold("AAA", "2025-12-31", disposition="HOLD_TRUE_FISCAL_TRANSITION")], final, {("AAA", "2025-12-31"): _evidence("AAA", "2025-12-31", 2025, "FY")}, {})

    assert rows[0]["reanalysis"] == "NORMAL_SEC_Q4_BOUNDARY"


def test_true_fiscal_change_remains_blocked_when_no_evidence() -> None:
    row = classify_final_rows([], [_hold("AAA", "2025-12-31", disposition="HOLD_TRUE_FISCAL_TRANSITION")], {}, {}, {})[0]

    assert row["final_disposition"] == "HOLD_ISOLATED_ROW"


def test_breakpoint_does_not_invalidate_older_independent_segment() -> None:
    row = classify_final_rows([], [_hold("AAA", "2024-09-30")], {}, {("AAA", "2024-09-30"): _evidence("AAA", "2024-09-30", 2024, "Q3")}, {})[0]

    assert row["final_disposition"] == "READY_REANCHORED_LEGACY_ONLY"


def test_v2_legacy_strong_reanchor() -> None:
    row = classify_final_rows([], [_hold("AAA", "2024-09-30")], {}, {("AAA", "2024-09-30"): _evidence("AAA", "2024-09-30", 2024, "Q3")}, {("AAA", "2024-09-30"): {"ticker": "AAA", "period_end_date": "2024-09-30", "fiscal_year": 2024, "fiscal_quarter": "Q3"}})[0]

    assert row["final_disposition"] == "READY_REANCHORED_WITH_V2"


def test_v2_fyfq_alone_cannot_anchor() -> None:
    row = classify_final_rows([], [_hold("AAA", "2024-09-30")], {}, {}, {("AAA", "2024-09-30"): {"ticker": "AAA", "period_end_date": "2024-09-30", "fiscal_year": 2024, "fiscal_quarter": "Q3"}})[0]

    assert row["final_disposition"] == "HOLD_INSUFFICIENT_EVIDENCE"


def test_v2_mapping_risk_cannot_anchor_without_legacy_sec() -> None:
    row = classify_final_rows([], [_hold("AAA", "2024-09-30")], {}, {}, {("AAA", "2024-09-30"): {"ticker": "AAA", "period_end_date": "2024-09-30", "fiscal_year": 2024, "fiscal_quarter": "Q3"}})[0]

    assert row["identity_evidence"] == "V2_ONLY_NOT_ENOUGH"


def test_legacy_only_segment_validation() -> None:
    row = classify_final_rows([], [_hold("AAA", "2024-06-30")], {}, {("AAA", "2024-06-30"): _evidence("AAA", "2024-06-30", 2024, "Q2")}, {})[0]

    assert row["final_disposition"] == "READY_REANCHORED_LEGACY_ONLY"


def test_isolated_row_remains_hold() -> None:
    row = classify_final_rows([], [_hold("AAA", "2024-06-30")], {}, {}, {})[0]

    assert row["final_disposition"] == "HOLD_ISOLATED_ROW"


def test_non_q4_gap_separates_segments() -> None:
    row = historical_gap_inventory([_ready("AAA", 2024, "Q2", "2024-06-30", "READY_BRIDGED_SEGMENT")])[0]

    assert row["gap_category"] == "TRUE_SINGLE_Q_DATA_GAP"


def test_no_synthetic_production_q_writes_in_plan() -> None:
    plan = build_dry_import_plan([_ready("AAA", 2025, "Q4", "2025-12-31", "READY_SEC_Q4_STRUCTURE")], [])

    assert plan[0]["phase3c2_recommendation"] == "READY_FOR_PHASE3C2_IMPORT"


def test_2018_floor_enforced_by_sequence_validation() -> None:
    violations = sequence_validation([_ready("AAA", 2017, "Q4", "2017-12-31", "READY_EXISTING_CHAIN")], historical_floor="2018-01-01")

    assert violations[0]["violation"] == "PERIOD_BEFORE_HISTORICAL_FLOOR"


def test_final_row_reconciliation() -> None:
    rows = classify_final_rows([_ready("AAA", 2025, "Q3", "2025-09-30", "READY_EXISTING_CHAIN")], [_hold("AAA", "2025-12-31")], {}, {("AAA", "2025-12-31"): _evidence("AAA", "2025-12-31", 2025, "FY")}, {})

    assert len(rows) == 2


def test_final_plan_unique_fyfq_after_duplicate_resolution() -> None:
    rows = resolve_duplicate_ready_fyfqs([_ready("AAA", 2025, "Q4", "2025-12-31", "READY_SEC_Q4_STRUCTURE"), _ready("AAA", 2025, "Q4", "2025-12-30", "READY_REANCHORED_WITH_V2")])

    assert sum(row["final_disposition"] == "HOLD_DUPLICATE_OR_AMBIGUOUS" for row in rows) == 1


def test_no_v3_writes_is_external_readonly_contract() -> None:
    assert READY_STATES


def test_deterministic_rerun() -> None:
    args = ([], [_hold("AAA", "2025-12-31")], {}, {("AAA", "2025-12-31"): _evidence("AAA", "2025-12-31", 2025, "FY")}, {})

    assert classify_final_rows(*args) == classify_final_rows(*args)


def test_cava_regression() -> None:
    assert _ready("CAVA", 2025, "Q4", "2025-12-31", "READY_SEC_Q4_STRUCTURE")["ticker"] == "CAVA"


def test_neup_regression() -> None:
    assert _ready("NEUP", 2026, "Q1", "2025-09-30", "READY_REANCHORED_LEGACY_ONLY")["fiscal_year"] == 2026


def test_lfcr_regression() -> None:
    assert _hold("LFCR", "2025-09-30")["ticker"] == "LFCR"


def test_bnc_regression() -> None:
    assert _hold("BNC", "2025-03-31")["period_end_date"] == "2025-03-31"


def test_sjm_regression() -> None:
    assert _ready("SJM", 2026, "Q4", "2026-04-30", "READY_SEC_Q4_STRUCTURE")["fiscal_quarter"] == "Q4"


def test_lyts_regression() -> None:
    assert _ready("LYTS", 2026, "Q4", "2026-06-30", "READY_SEC_Q4_STRUCTURE")["period_end_date"] == "2026-06-30"


def test_expected_contribution_counts_q4() -> None:
    q4 = [{"ticker": "AAA", "fiscal_year": 2025, "period_end_date": "2025-12-31", "field_derivation_methods": '{"revenue":"x"}', "balance_sheet_direct_instant_fields": "cash;total_debt"}]
    plan = [_ready("AAA", 2025, "Q4", "2025-12-31", "READY_SEC_Q4_STRUCTURE")]

    assert expected_contribution(plan, q4)["reconstructed_sec_q4_count"] == 1


def test_v2_calibration_detects_correct_q() -> None:
    rows = v2_historical_anchor_calibration([_v3("AAA", 2025, "Q1", "2025-03-31", revenue=100.0, gross_profit=50.0)], {("AAA", "2025-03-31"): {"ticker": "AAA", "period_end_date": "2025-03-31", "revenue": 100.0, "gross_profit": 50.0}}, {("AAA", "2025-03-31"): {"ticker": "AAA", "period_end_date": "2025-03-31", "fiscal_year": 2025, "fiscal_quarter": "Q1", "revenue": 100.0, "gross_profit": 50.0}})

    assert rows[0]["result"] == "CORRECT_Q"


def _evidence(ticker: str, period: str, fy: int, fp: str, filed: str | None = "2026-02-01") -> SecPeriodEvidence:
    item = SecPeriodEvidence(ticker=ticker, period_end_date=period)
    item.fiscal_years[str(fy)] += 1
    item.fiscal_periods[fp] += 1
    item.forms["10-K" if fp == "FY" else "10-Q"] += 1
    if filed:
        item.filed_dates[filed] += 1
    return item


def _sec_year(ticker: str, fy: int, periods: tuple[str, ...]) -> dict[tuple[str, str], SecPeriodEvidence]:
    dates = {"Q1": f"{fy}-03-31", "Q2": f"{fy}-06-30", "Q3": f"{fy}-09-30", "Q4": f"{fy}-12-31", "FY": f"{fy}-12-31"}
    return {(ticker, dates[fp]): _evidence(ticker, dates[fp], fy, fp) for fp in periods}


def _hold(ticker: str, period: str, disposition: str = "HOLD_BEHIND_BREAKPOINT") -> dict:
    return {"ticker": ticker, "period_end_date": period, "publish_date": "", "diagnostic_disposition": disposition, "available_fields": ""}


def _ready(ticker: str, fy: int, fq: str, period: str, disposition: str) -> dict:
    return {"market": "usa", "ticker": ticker, "fiscal_year": fy, "fiscal_quarter": fq, "period_end_date": period, "publish_date": "", "previous_disposition": "", "final_disposition": disposition, "identity_evidence": "", "sec_form": "", "sec_fp": "", "sec_fy": fy, "v2_corroboration": "", "available_fields": "revenue;cash", "source_record_id": f"LEGACY:{ticker}:{period}"}


def _v3(ticker: str, fy: int, fq: str, period: str, **values: float) -> dict:
    return {"ticker": ticker, "fiscal_year": fy, "fiscal_quarter": fq, "period_end_date": period, **values}
