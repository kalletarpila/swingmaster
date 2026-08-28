from __future__ import annotations

from datetime import date

from swingmaster.fundamentals.v3_phase8d5_fiscal_year_interval_refinement import (
    band,
    d5_decision,
    fy_start_for_type,
    month_quarter_starts,
    resolve_fq_within_fy,
    resolve_issuer_fiscal_year_for_date,
    resolve_row,
)


def test_exact_adjacent_fy_anchors_define_interval() -> None:
    profile = {"calendar_type": "FIXED_DATE_FISCAL_YEAR"}
    anchors = {2026: date(2025, 7, 1), 2027: date(2026, 7, 1)}

    result = resolve_issuer_fiscal_year_for_date(profile, anchors, date(2026, 3, 31))

    assert result["fiscal_year"] == 2026
    assert result["interval_start"] == date(2025, 7, 1)
    assert result["interval_end_exclusive"] == date(2026, 7, 1)
    assert result["evidence_type"] == "EXACT_ADJACENT_ANCHOR_INTERVAL"


def test_date_in_interval_maps_to_issuer_fy_not_calendar_year() -> None:
    profile = {"calendar_type": "FIXED_DATE_FISCAL_YEAR"}
    anchors = {2026: date(2025, 11, 1), 2027: date(2026, 11, 1)}

    result = resolve_issuer_fiscal_year_for_date(profile, anchors, date(2026, 1, 31))

    assert result["fiscal_year"] == 2026


def test_fixed_date_july_start_fy2026_quarters() -> None:
    profile = {"calendar_type": "FIXED_DATE_FISCAL_YEAR"}
    anchors = {2026: date(2025, 7, 1), 2027: date(2026, 7, 1)}
    expected = {
        "2025-09-30": ("Q1", "2025-09-30"),
        "2025-12-31": ("Q2", "2025-12-31"),
        "2026-03-31": ("Q3", "2026-03-31"),
        "2026-06-30": ("Q4", "2026-06-30"),
    }

    for period_end, (fq, expected_end) in expected.items():
        row = {"company_id": 1, "calendar_type": "FIXED_DATE_FISCAL_YEAR", "fiscal_year": 2026, "fiscal_quarter": fq, "period_end": period_end}
        resolved = resolve_row(row, profile, anchors, {})
        assert resolved["d5_inferred_fiscal_year"] == 2026
        assert resolved["d5_inferred_fiscal_quarter"] == fq
        assert resolved["d5_expected_period_end"] == expected_end


def test_fixed_date_november_start_fy2026_quarters() -> None:
    profile = {"calendar_type": "FIXED_DATE_FISCAL_YEAR"}
    anchors = {2026: date(2025, 11, 1), 2027: date(2026, 11, 1)}
    expected = {
        "2026-01-31": "Q1",
        "2026-04-30": "Q2",
        "2026-07-31": "Q3",
        "2026-10-31": "Q4",
    }

    for period_end, fq in expected.items():
        row = {"company_id": 1, "calendar_type": "FIXED_DATE_FISCAL_YEAR", "fiscal_year": 2026, "fiscal_quarter": fq, "period_end": period_end}
        resolved = resolve_row(row, profile, anchors, {})
        assert resolved["d5_inferred_fiscal_year"] == 2026
        assert resolved["d5_inferred_fiscal_quarter"] == fq


def test_fixed_date_backward_fy_start_inference() -> None:
    start, evidence = fy_start_for_type(2024, "FIXED_DATE_FISCAL_YEAR", {2026: date(2025, 7, 1)})

    assert start == date(2023, 7, 1)
    assert evidence == "STABLE_MONTH_BACKWARD_INFERENCE"


def test_leap_and_month_end_handling() -> None:
    starts = month_quarter_starts(date(2024, 2, 29), date(2025, 2, 28))

    assert starts[1] == date(2024, 5, 31)
    assert starts[2] == date(2024, 8, 31)
    assert starts[3] == date(2024, 11, 30)


def test_calendar_year_interval_direct() -> None:
    result = resolve_issuer_fiscal_year_for_date({"calendar_type": "CALENDAR_YEAR"}, {}, date(2026, 9, 30))

    assert result["fiscal_year"] == 2026
    assert result["interval_start"] == date(2026, 1, 1)


def test_week_based_resolves_fy_before_fq_for_364_day_year() -> None:
    profile = {"calendar_type": "WEEK_BASED_52_53"}
    anchors = {2026: date(2025, 2, 2), 2027: date(2026, 2, 1)}
    row = {"company_id": 1, "calendar_type": "WEEK_BASED_52_53", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2025-05-03"}

    resolved = resolve_row(row, profile, anchors, {})

    assert resolved["d5_inferred_fiscal_year"] == 2026
    assert resolved["d5_inferred_fiscal_quarter"] == "Q1"


def test_week_based_handles_371_day_year_with_known_placement() -> None:
    profile = {"calendar_type": "WEEK_BASED_52_53"}
    anchors = {2026: date(2025, 2, 2), 2027: date(2026, 2, 8)}
    row = {"company_id": 1, "calendar_type": "WEEK_BASED_52_53", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2025-05-10"}

    resolved = resolve_row(row, profile, anchors, {(1, 2026): "EXTRA_WEEK_Q1"})

    assert resolved["d5_inferred_fiscal_year"] == 2026
    assert resolved["d5_inferred_fiscal_quarter"] == "Q1"
    assert resolved["d5_expected_period_end"] == "2025-05-10"


def test_uncertain_older_week_based_fy_is_marked_uncertain() -> None:
    result = resolve_issuer_fiscal_year_for_date({"calendar_type": "WEEK_BASED_52_53"}, {2026: date(2025, 2, 2)}, date(2023, 5, 6))

    assert result["confidence"] == "FY_UNCERTAIN"


def test_transition_stops_extrapolation() -> None:
    result = resolve_issuer_fiscal_year_for_date({"calendar_type": "FIXED_DATE_FISCAL_YEAR"}, {2026: date(2025, 7, 1)}, date(2024, 9, 30), "POSSIBLE_TRANSITION")

    assert result["confidence"] == "FY_UNCERTAIN"
    assert result["evidence_type"] == "TRANSITION_REVIEW"


def test_offset_mode_detection() -> None:
    assert band(91) == "+90_DAY"
    assert band(-182) == "-180_DAY"
    assert band(270) == "+270_DAY"
    assert band(-365) == "-365_DAY"
    assert band(371) == "+371_DAY"


def test_d5_decision_clears_resolved_fy_and_fq_reasons() -> None:
    row = {
        "ticker": "AAA",
        "current_guard_decision": "BLOCK",
        "current_guard_reasons": "FY_SHIFT_MINUS_ONE|FQ_SLOT_MISMATCH|PERIOD_END_OUTSIDE_SLOT",
        "fiscal_year": 2026,
        "d5_inferred_fiscal_year": 2026,
        "d5_slot_available": 1,
        "d5_abs_offset_days": 0,
    }

    assert d5_decision(row) == ("PASS", "")


def test_d5_decision_preserves_known_p1_as_review() -> None:
    row = {
        "ticker": "BBY",
        "current_guard_decision": "BLOCK",
        "current_guard_reasons": "FQ_SLOT_MISMATCH",
        "fiscal_year": 2026,
        "d5_inferred_fiscal_year": 2026,
        "d5_slot_available": 1,
        "d5_abs_offset_days": 0,
    }

    assert d5_decision(row) == ("REVIEW", "KNOWN_P1_STRUCTURAL_DEFECT_REVIEW")


def test_resolve_fq_does_not_force_quarter_when_fy_uncertain() -> None:
    row = {"company_id": 1, "calendar_type": "FIXED_DATE_FISCAL_YEAR", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2025-09-30"}

    resolved = resolve_fq_within_fy(row, {"fiscal_year": "", "confidence": "FY_UNCERTAIN"}, {"calendar_type": "FIXED_DATE_FISCAL_YEAR"}, {}, {})

    assert resolved["d5_slot_available"] == 0
