from __future__ import annotations

from swingmaster.fundamentals.v3_residual_sequence_repair import DELETE_PLAN, UPDATE_PLAN, disposition_for


def test_bctx_non_calendar_yahoo_q1_becomes_q3() -> None:
    assert UPDATE_PLAN[1550][:3] == (2025, "Q3", "BCTX")


def test_ferg_non_calendar_yahoo_october_starts_next_fy() -> None:
    assert UPDATE_PLAN[4481][:3] == (2026, "Q1", "FERG")


def test_jkhy_june_year_end_march_is_q3() -> None:
    assert UPDATE_PLAN[6297][:3] == (2025, "Q3", "JKHY")


def test_lfcr_transition_rows_are_provider_variants() -> None:
    assert DELETE_PLAN[6798][0] == "LFCR"
    assert disposition_for("LFCR", "DELETE_PROVIDER_VARIANT") == "TRUE_FISCAL_CALENDAR_TRANSITION_CANONICAL_OK"


def test_olli_52_53_week_yahoo_month_end_variant_deleted() -> None:
    assert DELETE_PLAN[8637][0] == "OLLI"
    assert disposition_for("OLLI", "DELETE_PROVIDER_VARIANT") == "PROVIDER_PERIOD_VARIANT_CANONICAL_OK"


def test_rh_52_53_week_next_q1_mapping() -> None:
    assert UPDATE_PLAN[9977][:3] == (2026, "Q1", "RH")


def test_sgly_june_year_end_march_is_q3() -> None:
    assert UPDATE_PLAN[10512][:3] == (2025, "Q3", "SGLY")


def test_no_field_value_repair_is_encoded_in_plan() -> None:
    assert all("field" not in item[3].lower() for item in UPDATE_PLAN.values())
