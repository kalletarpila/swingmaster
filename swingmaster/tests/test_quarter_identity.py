from __future__ import annotations

from swingmaster.fundamentals_v2.quarter_identity import (
    ProviderQuarterCandidate,
    QuarterIdentity,
    match_cross_provider_quarter,
)


def test_exact_date_match() -> None:
    result = match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-31")])
    assert result.outcome == "EXACT_DATE_MATCH"
    assert result.candidate and result.candidate.candidate_id == "a"
    assert result.date_diff_days == 0


def test_plus_minus_one_and_seven_match() -> None:
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-04-01")]).outcome == "TOLERANCE_MATCH"
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-30")]).outcome == "TOLERANCE_MATCH"
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-04-07")]).outcome == "TOLERANCE_MATCH"
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-24")]).outcome == "TOLERANCE_MATCH"


def test_plus_minus_eight_rejects() -> None:
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-04-08")]).outcome == "OUTSIDE_DATE_TOLERANCE"
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-23")]).outcome == "OUTSIDE_DATE_TOLERANCE"


def test_different_fy_or_fq_rejects_even_same_date() -> None:
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-31", fy=2025)]).outcome == "NO_MATCH_FISCAL_IDENTITY"
    assert match_cross_provider_quarter(_canonical(), [_candidate("a", "2026-03-31", fq="Q2")]).outcome == "NO_MATCH_FISCAL_IDENTITY"


def test_nearest_unique_wins_and_exact_wins_over_non_exact() -> None:
    result = match_cross_provider_quarter(_canonical(), [_candidate("far", "2026-04-04"), _candidate("near", "2026-04-01")])
    assert result.outcome == "TOLERANCE_MATCH"
    assert result.candidate and result.candidate.candidate_id == "near"

    exact = match_cross_provider_quarter(_canonical(), [_candidate("near", "2026-04-01"), _candidate("exact", "2026-03-31")])
    assert exact.outcome == "EXACT_DATE_MATCH"
    assert exact.candidate and exact.candidate.candidate_id == "exact"


def test_equal_distance_ambiguity_rejects_unless_unique_earlier_policy() -> None:
    result = match_cross_provider_quarter(_canonical(), [_candidate("before", "2026-03-30"), _candidate("after", "2026-04-01")])
    assert result.outcome == "AMBIGUOUS_MULTIPLE_MATCHES"

    earlier = match_cross_provider_quarter(
        _canonical(),
        [_candidate("before", "2026-03-30"), _candidate("after", "2026-04-01")],
        prefer_earlier_on_equal_distance=True,
    )
    assert earlier.outcome == "TOLERANCE_MATCH"
    assert earlier.candidate and earlier.candidate.candidate_id == "before"


def test_non_calendar_fiscal_year_uses_supplied_identity_not_calendar_inference() -> None:
    canonical = QuarterIdentity("AAPL", 2026, "Q1", "2025-12-27")
    result = match_cross_provider_quarter(canonical, [ProviderQuarterCandidate("a", "AAPL", 2026, "Q1", "2025-12-31")])
    assert result.outcome == "TOLERANCE_MATCH"

    wrong_fq = match_cross_provider_quarter(canonical, [ProviderQuarterCandidate("a", "AAPL", 2025, "Q4", "2025-12-31")])
    assert wrong_fq.outcome == "NO_MATCH_FISCAL_IDENTITY"


def test_missing_identity_and_bad_date_reject_safely() -> None:
    assert match_cross_provider_quarter(QuarterIdentity("AAPL", 2026, None, "2026-03-31"), [_candidate("a", "2026-03-31")]).outcome == "MISSING_IDENTITY_COMPONENT"
    assert match_cross_provider_quarter(_canonical(), [ProviderQuarterCandidate("a", "AAPL", 2026, "Q1", "bad")]).outcome == "INVALID_DATE"


def _canonical() -> QuarterIdentity:
    return QuarterIdentity("AAPL", 2026, "Q1", "2026-03-31")


def _candidate(candidate_id: str, period_date: str, *, fy: int = 2026, fq: str = "Q1") -> ProviderQuarterCandidate:
    return ProviderQuarterCandidate(candidate_id, "AAPL", fy, fq, period_date)
