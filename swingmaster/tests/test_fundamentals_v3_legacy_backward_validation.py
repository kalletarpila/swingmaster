from __future__ import annotations

from pathlib import Path

from swingmaster.fundamentals.v3_legacy_backward_validation import (
    BackwardValidationPolicy,
    V3_HISTORICAL_PERIOD_END_FLOOR,
    adjacent_q_results,
    anchor_summary,
    anchor_accounting_rows,
    company_status_accounting_rows,
    conflict_typology,
    corrected_breakpoint_reason,
    depth_years,
    final_classification,
    final_classification_3c1c,
    missing_quarter_gap,
    legacy_income_fingerprint,
    period_continuity,
    predecessor,
    read_only_proof,
    select_anchors,
    sequence_validation,
    special_case_validation,
    validate_legacy_backward_chain,
    validate_without_anchor,
)


def test_recent_anchor_selection_prefers_2026_confirmed() -> None:
    anchors = select_anchors([_v3(2025, "Q4", "2025-12-31"), _v3(2026, "Q1", "2026-03-31")], [_overlap(2026, "Q1", "2026-03-31")])

    assert anchors["AAA"]["fiscal_year"] == 2026
    assert anchors["AAA"]["reliable_anchor"] == 1


def test_anchor_selection_falls_back_to_2025_then_older() -> None:
    anchors = select_anchors([_v3(2024, "Q4", "2024-12-31"), _v3(2025, "Q4", "2025-12-31")], [_overlap(2025, "Q4", "2025-12-31")])

    assert anchors["AAA"]["fiscal_year"] == 2025
    assert anchors["AAA"]["anchor_source"] == "RECENT_LEGACY_SAME_QUARTER_CONFIRMED"


def test_anchor_selection_marks_unconfirmed_fallback_unreliable() -> None:
    anchors = select_anchors([_v3(2026, "Q1", "2026-03-31")], [])

    assert anchors["AAA"]["reliable_anchor"] == 0


def test_q4_to_q3_backward_transition() -> None:
    assert predecessor(2026, "Q4") == (2026, "Q3")


def test_q1_to_previous_fy_q4_transition() -> None:
    assert predecessor(2026, "Q1") == (2025, "Q4")


def test_normal_period_end_continuity() -> None:
    assert period_continuity("2026-03-31", "2025-12-31") == "EXPECTED_QUARTER_INTERVAL"


def test_52_53_week_continuity() -> None:
    assert period_continuity("2026-03-31", "2025-12-15") == "SAFE_52_53_WEEK_VARIANT"


def test_provider_date_small_variant() -> None:
    assert period_continuity("2026-03-31", "2025-11-30") == "SMALL_PROVIDER_DATE_VARIANT"


def test_duplicate_predecessor_period_is_detected() -> None:
    assert period_continuity("2026-03-31", "2026-03-31") == "DUPLICATE_PERIOD"


def test_missing_predecessor_creates_breakpoint() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2026, "Q1", "2026-03-31"), legacy_rows=[_legacy("2024-12-31")], v2_rows={})

    assert result.breakpoints[0]["breakpoint_reason"] == "PERIOD_END_CONTINUITY_BREAK"


def test_fiscal_sequence_break_stops_chain() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2026, "Q1", "2026-03-31"), legacy_rows=[_legacy("2024-01-01")], v2_rows={})

    assert result.breakpoints[0]["breakpoint_reason"] == "PERIOD_END_CONTINUITY_BREAK"


def test_newer_validated_chain_remains_valid_after_old_break() -> None:
    result = validate_legacy_backward_chain(
        ticker="AAA",
        anchor=_v3(2026, "Q1", "2026-03-31"),
        legacy_rows=[_legacy("2025-12-31"), _legacy("2024-01-01")],
        v2_rows={},
    )

    assert result.ready_rows[0]["fiscal_year"] == 2025
    assert result.hold_rows[0]["diagnostic_disposition"] == "BEHIND_BREAKPOINT_UNCONFIRMED"


def test_fiscal_year_transition_requires_explicit_handling() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2026, "Q1", "2026-03-31"), legacy_rows=[_legacy("2025-07-01")], v2_rows={})

    assert result.hold_rows[0]["diagnostic_disposition"] == "TRANSITION_REQUIRES_RESOLUTION"


def test_v2_corroboration_supports_chain() -> None:
    result = validate_legacy_backward_chain(
        ticker="AAA",
        anchor=_v3(2026, "Q1", "2026-03-31"),
        legacy_rows=[_legacy("2025-12-31")],
        v2_rows={("AAA", 2025, "Q4"): {"period_end_date": "2025-12-31"}},
    )

    assert result.ready_rows[0]["diagnostic_disposition"] == "V2_CORROBORATED_CHAIN_CONFIRMED"


def test_v2_disagreement_does_not_control_legacy_sequence() -> None:
    result = validate_legacy_backward_chain(
        ticker="AAA",
        anchor=_v3(2026, "Q1", "2026-03-31"),
        legacy_rows=[_legacy("2025-12-31")],
        v2_rows={("AAA", 2025, "Q4"): {"period_end_date": "2025-12-30"}},
    )

    assert result.ready_rows[0]["diagnostic_disposition"] == "BACKWARD_CHAIN_CONFIRMED"
    assert result.ready_rows[0]["v2_corroboration"] == "V2_FYFQ_COUNTERPART"


def test_revenue_basic_income_calibration_agrees() -> None:
    rows = [_overlap(2026, "Q1", "2026-03-31", v3_revenue=100.0, legacy_revenue=101.0, v3_gross_profit=40.0, legacy_gross_profit=40.1)]

    assert legacy_income_fingerprint(rows)[0]["income_fingerprint"] == "ALL_MOST_AGREE"


def test_semantic_risk_fields_do_not_veto_identity_alone() -> None:
    rows = [_overlap(2026, "Q1", "2026-03-31", v3_revenue=100.0, legacy_revenue=100.0, v3_ebitda=1.0, legacy_ebitda=-100.0)]

    assert conflict_typology(rows, adjacent_q_results([_v3(2026, "Q1", "2026-03-31", revenue=100.0)], {("AAA", "2026-03-31"): _legacy("2026-03-31", revenue=100.0)}, rows)) == []


def test_same_q_adjacent_discrimination_prefers_same_q() -> None:
    v3_rows = [_v3(2025, "Q4", "2025-12-31", revenue=50_000_000.0), _v3(2026, "Q1", "2026-03-31", revenue=100_000_000.0)]
    overlap = [_overlap(2026, "Q1", "2026-03-31")]

    rows = adjacent_q_results(v3_rows, {("AAA", "2026-03-31"): _legacy("2026-03-31", revenue=100_000_000.0)}, overlap)

    assert rows[0]["best_match"] == "SAME_Q_BEST"


def test_wrong_fyfq_label_creates_sequence_violation() -> None:
    rows = [_ready(2025, "Q4", "2025-12-31"), _ready(2025, "Q4", "2025-09-30")]

    assert sequence_validation(rows)[0]["violation"] == "DUPLICATE_FYFQ"


def test_pre_2018_rows_are_excluded_from_chain() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2018, "Q1", "2018-03-31"), legacy_rows=[_legacy("2017-12-31")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.row_classifications == []


def test_no_duplicate_fyfq_in_ready_rows() -> None:
    assert sequence_validation([_ready(2025, "Q4", "2025-12-31"), _ready(2025, "Q3", "2025-09-30")]) == []


def test_read_only_proof_detects_no_writes(tmp_path: Path) -> None:
    db = _proof_db(tmp_path)
    counts = {"v3": {"v3_company": 1, "v3_quarter": 1, "v3_quarter_fundamentals": 1, "v3_migration_audit": 0, "v3_resolution_issue": 0}, "legacy": {"rc_fundamental_quarterly": 1}, "v2": {"rc_v2_quarter": 1, "rc_v2_fundamental_quarterly": 1}}

    proof = read_only_proof(counts, counts, db)

    assert proof["v3_writes"] == 0
    assert proof["quick_check"] == "ok"


def test_deterministic_rerun() -> None:
    args = {"ticker": "AAA", "anchor": _v3(2026, "Q1", "2026-03-31"), "legacy_rows": [_legacy("2025-12-31")], "v2_rows": {}}

    assert validate_legacy_backward_chain(**args).dry_plan == validate_legacy_backward_chain(**args).dry_plan


def test_no_reliable_anchor_holds_rows() -> None:
    result = validate_without_anchor(ticker="AAA", legacy_rows=[_legacy("2026-03-31")])

    assert result.hold_rows[0]["diagnostic_disposition"] == "INSUFFICIENT_EVIDENCE"


def test_cava_regression_is_reported_without_special_logic() -> None:
    rows = special_case_validation([{"ticker": "CAVA"}], [])

    assert rows[0]["ticker"] == "CAVA"
    assert rows[0]["has_anchor"] == 1


def test_neup_regression_preserves_ready_count() -> None:
    rows = special_case_validation([{"ticker": "NEUP"}], [{"ticker": "NEUP", "phase3c2_recommendation": "READY_FOR_PHASE3C2_IMPORT"}])

    assert next(row for row in rows if row["ticker"] == "NEUP")["ready_rows"] == 1


def test_lfcr_regression_preserves_hold_count() -> None:
    rows = special_case_validation([{"ticker": "LFCR"}], [{"ticker": "LFCR", "phase3c2_recommendation": "HOLD_FOR_PHASE3C2B_REVIEW"}])

    assert next(row for row in rows if row["ticker"] == "LFCR")["hold_rows"] == 1


def test_bnc_regression_preserves_transition_hold() -> None:
    rows = special_case_validation([{"ticker": "BNC"}], [{"ticker": "BNC", "phase3c2_recommendation": "HOLD_FOR_PHASE3C2B_REVIEW"}])

    assert next(row for row in rows if row["ticker"] == "BNC")["status"] == "PRESERVED_OR_NOT_IN_REFINED_BASELINE"


def test_recent_anchor_gate_uses_observed_strength_not_fixed_percentage() -> None:
    recent = [{"overlap_rows": 100, "same_quarter_confirmed": 1, "period_end_exact_or_compatible": 100}]

    assert final_classification(recent, [_ready(2025, "Q4", "2025-12-31")], []).endswith("READY_FOR_3C2")


def test_depth_years_uses_oldest_ready_period() -> None:
    assert round(depth_years(_v3(2026, "Q1", "2026-03-31"), [_ready(2025, "Q4", "2025-12-31")]), 2) == 0.25


def test_anchor_summary_counts_no_reliable_anchor() -> None:
    summary = anchor_summary([{"anchor_bucket": "2026", "reliable_anchor": 1}, {"anchor_bucket": "NONE", "reliable_anchor": 0}], [])

    assert summary["companies_with_2026_anchor"] == 1
    assert summary["companies_with_no_reliable_anchor"] == 1


def test_2017_12_31_excluded_by_2018_floor() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2018, "Q1", "2018-03-31"), legacy_rows=[_legacy("2017-12-31")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.row_classifications == []


def test_2018_01_01_included_by_2018_floor() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2018, "Q2", "2018-04-01"), legacy_rows=[_legacy("2018-01-01")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.ready_rows[0]["period_end_date"] == "2018-01-01"


def test_newer_than_2018_floor_included() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2019, "Q1", "2019-03-31"), legacy_rows=[_legacy("2018-12-31")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.ready_rows[0]["diagnostic_disposition"] == "READY_DIRECT_CHAIN"


def test_pre_2018_does_not_create_breakpoint() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2018, "Q1", "2018-03-31"), legacy_rows=[_legacy("2017-12-31")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.breakpoints == []


def test_chain_stops_successfully_at_2018_floor() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2018, "Q2", "2018-04-01"), legacy_rows=[_legacy("2018-01-01"), _legacy("2017-10-01")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.summary["validated_to_historical_floor"] == 1


def test_ordinary_q1_to_previous_fy_q4_is_not_transition_anomaly() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2026, "Q1", "2026-03-31"), legacy_rows=[_legacy("2025-12-31")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.ready_rows[0]["fiscal_quarter"] == "Q4"
    assert result.breakpoints == []


def test_explicit_q4_sequence_is_direct_ready() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2026, "Q1", "2026-03-31"), legacy_rows=[_legacy("2025-12-31"), _legacy("2025-09-30")], v2_rows={}, policy=BackwardValidationPolicy())

    assert [row["diagnostic_disposition"] for row in result.ready_rows] == ["READY_DIRECT_CHAIN", "READY_DIRECT_CHAIN"]


def test_missing_q4_with_coherent_q3_bridge() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2026, "Q1", "2026-03-31"), legacy_rows=[_legacy("2025-09-30")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.ready_rows[0]["diagnostic_disposition"] == "READY_BRIDGED_CHAIN"


def test_missing_q4_without_enough_evidence_blocks() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2026, "Q1", "2026-03-31"), legacy_rows=[_legacy("2025-06-30")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.breakpoints[0]["breakpoint_reason"] == "MULTI_QUARTER_DATA_GAP"


def test_annual_row_q4_representation_diagnostic_reason() -> None:
    assert corrected_breakpoint_reason("TRANSITION_PERIOD", 1, "Q4") == "MISSING_EXPLICIT_Q4"


def test_one_quarter_gap_bridge() -> None:
    assert missing_quarter_gap("2026-03-31", "2025-09-30") == 1


def test_two_quarter_gap_conservative_handling() -> None:
    assert corrected_breakpoint_reason("TRANSITION_PERIOD", 2, "Q2") == "MULTI_QUARTER_DATA_GAP"


def test_more_than_two_quarter_gap_blocks() -> None:
    assert missing_quarter_gap("2026-03-31", "2025-01-01") == 3


def test_13_week_cadence_is_expected_interval() -> None:
    assert period_continuity("2026-03-29", "2025-12-28") == "EXPECTED_QUARTER_INTERVAL"


def test_53_week_cadence_variant_is_supported() -> None:
    assert period_continuity("2026-03-31", "2025-12-15") == "SAFE_52_53_WEEK_VARIANT"


def test_small_provider_date_variant_still_supported() -> None:
    assert period_continuity("2026-03-31", "2025-11-30") == "SMALL_PROVIDER_DATE_VARIANT"


def test_true_fiscal_year_change_blocks() -> None:
    result = validate_legacy_backward_chain(ticker="AAA", anchor=_v3(2026, "Q1", "2026-03-31"), legacy_rows=[_legacy("2025-03-31")], v2_rows={}, policy=BackwardValidationPolicy())

    assert result.ready_rows == []
    assert result.breakpoints[0]["bridgeability"] == "REQUIRES_3C2B_REVIEW"


def test_fy_naming_mismatch_classification_placeholder() -> None:
    assert corrected_breakpoint_reason("OUT_OF_ORDER", 0, "Q4") == "PERIOD_END_TRUE_BREAK"


def test_duplicate_predecessor_blocks_under_3c1c() -> None:
    assert corrected_breakpoint_reason("DUPLICATE_PERIOD", 0, "Q4") == "DUPLICATE_PERIOD"


def test_mutually_exclusive_anchor_accounting() -> None:
    rows = anchor_accounting_rows([{"anchor_category": "ANCHOR_2026"}, {"anchor_category": "NO_LEGACY_2018_PLUS_HISTORY"}], 2)

    assert rows[-2]["companies"] == 2
    assert rows[-1]["companies"] == 2


def test_company_status_reconciliation_to_universe() -> None:
    rows = company_status_accounting_rows([{"company_status": "FULL_OR_PARTIAL_VALID_CHAIN"}, {"company_status": "NO_RELIABLE_ANCHOR"}], 2)

    assert rows[-2]["companies"] == 2


def test_ready_rows_contain_no_pre_2018_dates() -> None:
    violations = sequence_validation([_ready(2017, "Q4", "2017-12-31")], historical_floor=V3_HISTORICAL_PERIOD_END_FLOOR)

    assert violations[0]["violation"] == "PERIOD_BEFORE_HISTORICAL_FLOOR"


def test_final_classification_3c1c_ready_gate() -> None:
    recent = [{"overlap_rows": 1, "same_quarter_confirmed": 1, "period_end_exact_or_compatible": 1}]

    assert final_classification_3c1c(recent, [_ready(2018, "Q1", "2018-01-01")], []).endswith("READY_FOR_3C2")


def _v3(fy: int, fq: str, period: str, **values: float) -> dict:
    return {
        "market": "usa",
        "ticker": "AAA",
        "active": 1,
        "fiscal_year": fy,
        "fiscal_quarter": fq,
        "period_end_date": period,
        "publish_date": None,
        "anchor_source": "TEST_CONFIRMED_ANCHOR",
        "reliable_anchor": 1,
        **values,
    }


def _legacy(period: str, **values: float) -> dict:
    base = {"ticker": "AAA", "period_end_date": period, "publish_date": None}
    for field in ("revenue", "gross_profit", "operating_income", "ebit", "ebitda", "net_income", "operating_cashflow", "capex", "free_cashflow", "cash", "total_debt", "shares_outstanding"):
        base[field] = values.get(field)
    return base


def _overlap(fy: int, fq: str, period: str, **values: float) -> dict:
    row = {"ticker": "AAA", "fiscal_year": fy, "fiscal_quarter": fq, "v3_period_end_date": period, "legacy_period_end_date": period, "period_relation": "EXACT", "identity_classification": "SAME_QUARTER_CONFIRMED"}
    for field in ("revenue", "gross_profit", "operating_income", "ebit", "ebitda", "net_income", "operating_cashflow", "capex", "free_cashflow", "cash", "total_debt", "shares_outstanding"):
        row[f"v3_{field}"] = values.get(f"v3_{field}")
        row[f"legacy_{field}"] = values.get(f"legacy_{field}")
    return row


def _ready(fy: int, fq: str, period: str) -> dict:
    return {"ticker": "AAA", "fiscal_year": fy, "fiscal_quarter": fq, "period_end_date": period, "diagnostic_disposition": "BACKWARD_CHAIN_CONFIRMED"}


def _proof_db(tmp_path: Path) -> Path:
    db = tmp_path / "proof.db"
    import sqlite3

    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_company(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE v3_quarter(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE v3_quarter_fundamentals(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE v3_migration_audit(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE v3_resolution_issue(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE rc_fundamental_quarterly(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE rc_v2_quarter(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TABLE rc_v2_fundamental_quarterly(id INTEGER PRIMARY KEY)")
    return db
