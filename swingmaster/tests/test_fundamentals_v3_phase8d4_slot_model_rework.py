from __future__ import annotations

from datetime import date

from swingmaster.fundamentals.v3_phase8d4_slot_model_rework import (
    add_months,
    month_slots,
    new_decision,
    new_slot,
    possible_week_expected_ends,
    resolve_extra_week,
    week_slots,
)


def test_calendar_year_uses_calendar_month_quarters() -> None:
    row = {"company_id": 1, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2026-03-31", "calendar_type": "CALENDAR_YEAR"}
    slot = new_slot(row, {"calendar_type": "CALENDAR_YEAR"}, {}, {})

    assert slot["new_expected_period_end"] == "2026-03-31"
    assert slot["new_abs_offset_days"] == 0


def test_calendar_year_q2_q3_q4_boundaries() -> None:
    profile = {"calendar_type": "CALENDAR_YEAR"}

    assert new_slot({"company_id": 1, "fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end": "2026-06-30", "calendar_type": "CALENDAR_YEAR"}, profile, {}, {})["new_expected_period_end"] == "2026-06-30"
    assert new_slot({"company_id": 1, "fiscal_year": 2026, "fiscal_quarter": "Q3", "period_end": "2026-09-30", "calendar_type": "CALENDAR_YEAR"}, profile, {}, {})["new_expected_period_end"] == "2026-09-30"
    assert new_slot({"company_id": 1, "fiscal_year": 2026, "fiscal_quarter": "Q4", "period_end": "2026-12-31", "calendar_type": "CALENDAR_YEAR"}, profile, {}, {})["new_expected_period_end"] == "2026-12-31"


def test_fixed_date_uses_calendar_months_for_july_start() -> None:
    profile = {"calendar_type": "FIXED_DATE_FISCAL_YEAR"}
    anchors = {2026: date(2025, 7, 1), 2027: date(2026, 7, 1)}

    slot = new_slot({"company_id": 1, "fiscal_year": 2026, "fiscal_quarter": "Q3", "period_end": "2026-03-31", "calendar_type": "FIXED_DATE_FISCAL_YEAR"}, profile, anchors, {})

    assert slot["new_expected_period_end"] == "2026-03-31"
    assert slot["new_reason"] == "fixed_date_calendar_month_quarters"


def test_fixed_date_november_start_and_non_first_day_month_arithmetic() -> None:
    assert add_months(date(2025, 11, 1), 3) == date(2026, 2, 1)
    assert add_months(date(2025, 2, 15), 3) == date(2025, 5, 15)
    assert add_months(date(2024, 2, 29), 12) == date(2025, 2, 28)
    slots = month_slots(2026, {"calendar_type": "FIXED_DATE_FISCAL_YEAR"}, {2026: date(2025, 11, 1), 2027: date(2026, 11, 1)})

    assert slots["starts"][1] == date(2026, 2, 1)
    assert slots["starts"][2] == date(2026, 5, 1)


def test_week_based_364_and_371_years_preserve_weekday() -> None:
    profile = {"calendar_type": "WEEK_BASED_52_53"}
    start = date(2025, 2, 2)

    slots52 = week_slots(2026, profile, {2026: start, 2027: start + __import__("datetime").timedelta(days=364)})
    slots53 = week_slots(2026, profile, {2026: start, 2027: start + __import__("datetime").timedelta(days=371)}, "EXTRA_WEEK_Q1")

    assert slots52["year_type"] == "VERIFIED_52_WEEK_YEAR"
    assert slots52["starts"][1] == start + __import__("datetime").timedelta(days=91)
    assert slots53["year_type"] == "VERIFIED_53_WEEK_YEAR"
    assert slots53["starts"][1] == start + __import__("datetime").timedelta(days=98)
    assert slots53["starts"][1].weekday() == start.weekday()


def test_week_based_extra_week_placements_and_ambiguity() -> None:
    profile = {"calendar_type": "WEEK_BASED_52_53"}
    anchors = {2026: date(2025, 2, 2), 2027: date(2026, 2, 8)}

    q1_end = possible_week_expected_ends(2026, "Q1", profile, anchors, "EXTRA_WEEK_Q1")[0]
    q4_end = possible_week_expected_ends(2026, "Q4", profile, anchors, "EXTRA_WEEK_Q4")[0]
    ambiguous_q2 = possible_week_expected_ends(2026, "Q2", profile, anchors, "EXTRA_WEEK_AMBIGUOUS")

    assert q1_end == date(2025, 5, 10)
    assert q4_end == date(2026, 2, 7)
    assert len(ambiguous_q2) > 1


def test_resolve_extra_week_from_local_period_end_sequence() -> None:
    profile = {1: {"calendar_type": "WEEK_BASED_52_53"}}
    anchors = {1: {2026: date(2025, 2, 2), 2027: date(2026, 2, 8)}}
    population = [
        {"company_id": 1, "ticker": "W", "calendar_type": "WEEK_BASED_52_53", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2025-05-10"},
        {"company_id": 1, "ticker": "W", "calendar_type": "WEEK_BASED_52_53", "fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end": "2025-08-09"},
    ]

    assert resolve_extra_week(population, profile, anchors)[(1, 2026)] == "EXTRA_WEEK_Q1"


def test_other_verified_is_conservative_without_anchor() -> None:
    row = {"company_id": 1, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2026-03-31", "calendar_type": "OTHER_VERIFIED"}

    assert new_slot(row, {"calendar_type": "OTHER_VERIFIED"}, {}, {})["new_slot_available"] == 0


def test_new_decision_preserves_exact_anchor_authority_and_known_p1_review() -> None:
    p1 = {"ticker": "BBY", "current_guard_decision": "BLOCK", "current_guard_reasons": "FQ_SLOT_MISMATCH", "new_slot_available": 1, "new_abs_offset_days": 0, "new_inferred_fiscal_year": 2026, "fiscal_year": 2026}
    hard = {"ticker": "AAA", "current_guard_decision": "BLOCK", "current_guard_reasons": "FY_SHIFT_MINUS_ONE", "new_slot_available": 1, "new_abs_offset_days": 0, "new_inferred_fiscal_year": 2025, "fiscal_year": 2026}

    assert new_decision(p1) == ("REVIEW", "KNOWN_P1_STRUCTURAL_DEFECT_REVIEW")
    assert new_decision(hard)[0] == "BLOCK"
