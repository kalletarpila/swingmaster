from __future__ import annotations

from swingmaster.fundamentals_v2.quarter_identity import (
    AMBIGUOUS,
    EXACT_DATE_INFERRED_FISCAL,
    EXACT_VERIFIED_FISCAL,
    INVALID_IDENTITY,
    NO_MATCH,
    OUTSIDE_TOLERANCE,
    TOLERANCE_DATE_INFERRED_FISCAL,
    TOLERANCE_VERIFIED_FISCAL,
    ProviderQuarterCandidate,
    QuarterIdentity,
    match_cross_provider_quarter,
    match_date_inferred_provider_quarter,
)


def test_verified_fiscal_exact_date() -> None:
    result = match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-31")])
    assert result.outcome == EXACT_VERIFIED_FISCAL
    assert result.candidate and result.candidate.candidate_id == "a"
    assert result.date_offset_days == 0
    assert result.absolute_date_offset_days == 0
    assert result.fiscal_identity_verified is True


def test_verified_fiscal_plus_and_minus_seven_match() -> None:
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-04-07")]).outcome == TOLERANCE_VERIFIED_FISCAL
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-24")]).outcome == TOLERANCE_VERIFIED_FISCAL


def test_verified_fiscal_plus_minus_eight_rejects() -> None:
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-04-08")]).outcome == OUTSIDE_TOLERANCE
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-23")]).outcome == OUTSIDE_TOLERANCE


def test_verified_fiscal_different_fy_or_fq_rejects_even_same_date() -> None:
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-31", fy=2025)]).outcome == NO_MATCH
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-31", fq="Q2")]).outcome == NO_MATCH


def test_date_inferred_requires_explicit_opt_in() -> None:
    result = match_cross_provider_quarter(_canonical(), [_date_only_candidate("a", "2026-03-31")])
    assert result.outcome == INVALID_IDENTITY


def test_date_inferred_exact_unique_date() -> None:
    result = match_cross_provider_quarter(
        _canonical(),
        [_date_only_candidate("a", "2026-03-31")],
        allow_date_inferred_fiscal_match=True,
        provider_fiscal_identity_usable=False,
    )
    assert result.outcome == EXACT_DATE_INFERRED_FISCAL
    assert result.fiscal_identity_verified is False
    assert result.provider_date == "2026-03-31"
    assert result.canonical_report_date == "2026-03-31"


def test_date_inferred_plus_minus_one_and_seven_match() -> None:
    for period_date in ("2026-04-01", "2026-03-30", "2026-04-07", "2026-03-24"):
        result = match_cross_provider_quarter(
            _canonical(),
            [_date_only_candidate("a", period_date)],
            allow_date_inferred_fiscal_match=True,
            provider_fiscal_identity_usable=False,
        )
        assert result.outcome == TOLERANCE_DATE_INFERRED_FISCAL
        assert result.absolute_date_offset_days in {1, 7}
        assert result.fiscal_identity_verified is False


def test_date_inferred_plus_minus_eight_rejects() -> None:
    for period_date in ("2026-04-08", "2026-03-23"):
        result = match_cross_provider_quarter(
            _canonical(),
            [_date_only_candidate("a", period_date)],
            allow_date_inferred_fiscal_match=True,
            provider_fiscal_identity_usable=False,
        )
        assert result.outcome == OUTSIDE_TOLERANCE


def test_two_canonical_quarters_within_inferred_window_rejects() -> None:
    result = match_date_inferred_provider_quarter(
        _date_only_candidate("a", "2026-03-31"),
        [
            QuarterIdentity("AAPL", 2026, "Q1", "2026-03-31", quarter_id=1),
            QuarterIdentity("AAPL", 2026, "Q2", "2026-04-03", quarter_id=2),
        ],
    )
    assert result.outcome == AMBIGUOUS
    assert result.ambiguity_reason == "multiple_canonical_quarters_within_tolerance"


def test_two_provider_rows_equally_close_rejects() -> None:
    result = match_cross_provider_quarter(
        _canonical(),
        [_date_only_candidate("before", "2026-03-30"), _date_only_candidate("after", "2026-04-01")],
        allow_date_inferred_fiscal_match=True,
        provider_fiscal_identity_usable=False,
    )
    assert result.outcome == AMBIGUOUS
    assert result.ambiguity_reason == "equal_distance_provider_rows"


def test_exact_provider_row_wins_over_non_exact_when_unique() -> None:
    result = match_cross_provider_quarter(
        _canonical(),
        [_date_only_candidate("near", "2026-04-01"), _date_only_candidate("exact", "2026-03-31")],
        allow_date_inferred_fiscal_match=True,
        provider_fiscal_identity_usable=False,
    )
    assert result.outcome == EXACT_DATE_INFERRED_FISCAL
    assert result.candidate and result.candidate.candidate_id == "exact"


def test_fiscal_identity_present_prefers_verified_route_not_inferred() -> None:
    result = match_cross_provider_quarter(
        _canonical(),
        [_candidate("verified", "2026-04-01"), _date_only_candidate("date-only", "2026-03-31")],
        allow_date_inferred_fiscal_match=True,
    )
    assert result.outcome == TOLERANCE_VERIFIED_FISCAL
    assert result.candidate and result.candidate.candidate_id == "verified"
    assert result.fiscal_identity_verified is True


def test_fiscal_identity_conflict_is_not_overridden_by_inferred_fallback() -> None:
    result = match_cross_provider_quarter(
        _canonical(),
        [_candidate("wrong-fiscal", "2026-03-31", fy=2025, fq="Q4")],
        allow_date_inferred_fiscal_match=True,
    )
    assert result.outcome == NO_MATCH


def test_conflicting_provider_fiscal_can_use_inference_only_when_marked_unusable() -> None:
    result = match_cross_provider_quarter(
        _canonical(),
        [_candidate("wrong-fiscal-unusable", "2026-03-31", fy=2025, fq="Q4")],
        allow_date_inferred_fiscal_match=True,
        provider_fiscal_identity_usable=False,
    )
    assert result.outcome == EXACT_DATE_INFERRED_FISCAL
    assert result.fiscal_identity_verified is False


def test_non_calendar_fiscal_year_uses_supplied_identity_not_calendar_inference() -> None:
    canonical = QuarterIdentity("AAPL", 2026, "Q1", "2025-12-27")
    result = match_cross_provider_quarter(canonical, [ProviderQuarterCandidate("a", "AAPL", 2026, "Q1", "2025-12-31")])
    assert result.outcome == TOLERANCE_VERIFIED_FISCAL

    wrong_fq = match_cross_provider_quarter(canonical, [ProviderQuarterCandidate("a", "AAPL", 2025, "Q4", "2025-12-31")])
    assert wrong_fq.outcome == NO_MATCH


def test_invalid_identity_and_event_series_reject_safely() -> None:
    assert match_cross_provider_quarter(QuarterIdentity("AAPL", 2026, None, "2026-03-31"), [_candidate("a", "2026-03-31")]).outcome == INVALID_IDENTITY

    event_series = _date_only_candidate("shares-pit", "2026-03-31", is_quarterly_statement_fact=False)
    assert (
        match_cross_provider_quarter(
            _canonical(),
            [event_series],
            allow_date_inferred_fiscal_match=True,
            provider_fiscal_identity_usable=False,
        ).outcome
        == NO_MATCH
    )
    assert match_cross_provider_quarter(_canonical(), [ProviderQuarterCandidate("a", "AAPL", 2026, "Q1", "bad")]).outcome == NO_MATCH


def _canonical() -> QuarterIdentity:
    return QuarterIdentity("AAPL", 2026, "Q1", "2026-03-31")


def _candidate(candidate_id: str, period_date: str, *, fy: int = 2026, fq: str = "Q1") -> ProviderQuarterCandidate:
    return ProviderQuarterCandidate(candidate_id, "AAPL", fy, fq, period_date)


def _date_only_candidate(candidate_id: str, period_date: str, *, is_quarterly_statement_fact: bool = True) -> ProviderQuarterCandidate:
    return ProviderQuarterCandidate(candidate_id, "AAPL", None, None, period_date, is_quarterly_statement_fact=is_quarterly_statement_fact)
