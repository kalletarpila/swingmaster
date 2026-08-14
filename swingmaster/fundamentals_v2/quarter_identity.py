from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable


DEFAULT_QUARTER_DATE_TOLERANCE_DAYS = 7


EXACT_VERIFIED_FISCAL = "EXACT_VERIFIED_FISCAL"
TOLERANCE_VERIFIED_FISCAL = "TOLERANCE_VERIFIED_FISCAL"
EXACT_DATE_INFERRED_FISCAL = "EXACT_DATE_INFERRED_FISCAL"
TOLERANCE_DATE_INFERRED_FISCAL = "TOLERANCE_DATE_INFERRED_FISCAL"
NO_MATCH = "NO_MATCH"
AMBIGUOUS = "AMBIGUOUS"
OUTSIDE_TOLERANCE = "OUTSIDE_TOLERANCE"
INVALID_IDENTITY = "INVALID_IDENTITY"


@dataclass(frozen=True)
class QuarterIdentity:
    company_key: str
    fiscal_year: int | str | None
    fiscal_period: str | None
    report_date: str | None
    quarter_id: str | int | None = None


@dataclass(frozen=True)
class ProviderQuarterCandidate:
    candidate_id: str
    company_key: str
    fiscal_year: int | str | None
    fiscal_period: str | None
    period_date: str | None
    is_quarterly_statement_fact: bool = True


@dataclass(frozen=True)
class QuarterIdentityMatch:
    outcome: str
    candidate: ProviderQuarterCandidate | None = None
    canonical: QuarterIdentity | None = None
    date_diff_days: int | None = None
    reason: str = ""
    provider_date: str | None = None
    canonical_report_date: str | None = None
    date_offset_days: int | None = None
    absolute_date_offset_days: int | None = None
    fiscal_identity_verified: bool = False
    ambiguous: bool = False
    ambiguity_reason: str | None = None

    @property
    def match_mode(self) -> str:
        return self.outcome


def match_cross_provider_quarter(
    canonical: QuarterIdentity,
    candidates: Iterable[ProviderQuarterCandidate],
    *,
    tolerance_days: int = DEFAULT_QUARTER_DATE_TOLERANCE_DAYS,
    allow_date_inferred_fiscal_match: bool = False,
    provider_fiscal_identity_usable: bool = True,
    prefer_earlier_on_equal_distance: bool = False,
) -> QuarterIdentityMatch:
    """Match one canonical quarter to provider rows.

    Verified fiscal matching is the default and requires same company, same FY,
    same FQ, and a provider date within tolerance.

    DATE_TOLERANCE_INFERRED fiscal matching is a deliberate internal-use risk
    tradeoff. When explicitly enabled and provider FY/FQ is unavailable or
    classified unusable, it may associate a date-only provider quarterly fact to
    the unique V2 quarter within +/-7 calendar days. Inferred matches are never
    reported as fiscal-verified, expose date-offset provenance, reject
    ambiguity, and must not be used for event-series observations.
    """
    canonical_date_result = _canonical_date(canonical)
    if isinstance(canonical_date_result, QuarterIdentityMatch):
        return canonical_date_result
    canonical_date = canonical_date_result

    candidate_list = list(candidates)
    has_usable_provider_fiscal_identity = provider_fiscal_identity_usable and any(
        _has_fiscal_identity(candidate) for candidate in candidate_list
    )
    if has_usable_provider_fiscal_identity:
        return _match_verified_fiscal(
            canonical,
            canonical_date,
            candidate_list,
            tolerance_days=tolerance_days,
            prefer_earlier_on_equal_distance=prefer_earlier_on_equal_distance,
        )

    if not allow_date_inferred_fiscal_match:
        return QuarterIdentityMatch(INVALID_IDENTITY, reason="provider_fiscal_identity_missing_or_unusable")

    return _match_date_inferred_for_canonical(
        canonical,
        canonical_date,
        candidate_list,
        tolerance_days=tolerance_days,
        prefer_earlier_on_equal_distance=prefer_earlier_on_equal_distance,
    )


def match_date_inferred_provider_quarter(
    provider: ProviderQuarterCandidate,
    canonical_candidates: Iterable[QuarterIdentity],
    *,
    provider_rows_for_company: Iterable[ProviderQuarterCandidate] | None = None,
    tolerance_days: int = DEFAULT_QUARTER_DATE_TOLERANCE_DAYS,
) -> QuarterIdentityMatch:
    """Match one date-only quarterly provider row to a unique canonical quarter.

    The helper enforces the relaxed policy's forward side: exactly one canonical
    quarter for the same company may be inside tolerance. When peer provider
    rows are supplied, the reverse side is also checked and ambiguous provider
    rows for the selected canonical quarter are rejected.
    """
    if not provider.is_quarterly_statement_fact:
        return QuarterIdentityMatch(INVALID_IDENTITY, candidate=provider, reason="not_quarterly_statement_fact")
    if not str(provider.company_key or "").strip():
        return QuarterIdentityMatch(INVALID_IDENTITY, candidate=provider, reason="provider:company_key")
    try:
        provider_date = _parse_date(provider.period_date)
    except ValueError as exc:
        return QuarterIdentityMatch(INVALID_IDENTITY, candidate=provider, reason=f"provider:{exc}")

    plausible: list[tuple[QuarterIdentity, int]] = []
    invalid_canonical_date = False
    for canonical in canonical_candidates:
        if _company(canonical.company_key) != _company(provider.company_key):
            continue
        try:
            canonical_date = _parse_date(canonical.report_date)
        except ValueError:
            invalid_canonical_date = True
            continue
        diff = (provider_date - canonical_date).days
        if abs(diff) <= tolerance_days:
            plausible.append((canonical, diff))

    if not plausible:
        reason = "canonical_date" if invalid_canonical_date else "no_canonical_quarter_within_tolerance"
        return QuarterIdentityMatch(OUTSIDE_TOLERANCE, candidate=provider, reason=reason)
    if len(plausible) > 1:
        return _ambiguous(provider=provider, reason="multiple_canonical_quarters_within_tolerance")

    selected = plausible[0]
    canonical, diff = selected
    if provider_rows_for_company is not None:
        reverse = _match_date_inferred_for_canonical(
            canonical,
            _parse_date(canonical.report_date),
            list(provider_rows_for_company),
            tolerance_days=tolerance_days,
        )
        if reverse.outcome == AMBIGUOUS:
            return reverse
        if reverse.candidate != provider:
            return _ambiguous(provider=provider, canonical=canonical, reason="provider_row_not_unique_for_canonical")

    return _matched(
        EXACT_DATE_INFERRED_FISCAL if diff == 0 else TOLERANCE_DATE_INFERRED_FISCAL,
        provider,
        canonical,
        diff,
        verified=False,
    )


def _match_verified_fiscal(
    canonical: QuarterIdentity,
    canonical_date: date,
    candidates: list[ProviderQuarterCandidate],
    *,
    tolerance_days: int,
    prefer_earlier_on_equal_distance: bool,
) -> QuarterIdentityMatch:
    if not _has_fiscal_identity(canonical):
        return QuarterIdentityMatch(INVALID_IDENTITY, canonical=canonical, reason="canonical_fiscal_identity")
    fiscal_candidates: list[tuple[ProviderQuarterCandidate, int]] = []
    invalid_candidate_date = False
    for candidate in candidates:
        if not candidate.is_quarterly_statement_fact:
            continue
        if not _has_fiscal_identity(candidate):
            continue
        if _company(candidate.company_key) != _company(canonical.company_key):
            continue
        if _fy(candidate.fiscal_year) != _fy(canonical.fiscal_year) or _fq(candidate.fiscal_period) != _fq(canonical.fiscal_period):
            continue
        try:
            provider_date = _parse_date(candidate.period_date)
        except ValueError:
            invalid_candidate_date = True
            continue
        fiscal_candidates.append((candidate, (provider_date - canonical_date).days))

    if not fiscal_candidates:
        reason = "provider_date" if invalid_candidate_date else "no_same_company_fy_fq_candidate"
        return QuarterIdentityMatch(NO_MATCH, canonical=canonical, reason=reason)
    return _rank_provider_candidates(canonical, fiscal_candidates, tolerance_days, True, prefer_earlier_on_equal_distance)


def _match_date_inferred_for_canonical(
    canonical: QuarterIdentity,
    canonical_date: date,
    candidates: list[ProviderQuarterCandidate],
    *,
    tolerance_days: int,
    prefer_earlier_on_equal_distance: bool = False,
) -> QuarterIdentityMatch:
    inferred_candidates: list[tuple[ProviderQuarterCandidate, int]] = []
    invalid_candidate_date = False
    for candidate in candidates:
        if not candidate.is_quarterly_statement_fact:
            continue
        if _company(candidate.company_key) != _company(canonical.company_key):
            continue
        try:
            provider_date = _parse_date(candidate.period_date)
        except ValueError:
            invalid_candidate_date = True
            continue
        inferred_candidates.append((candidate, (provider_date - canonical_date).days))
    if not inferred_candidates:
        reason = "provider_date" if invalid_candidate_date else "no_same_company_provider_candidate"
        return QuarterIdentityMatch(NO_MATCH, canonical=canonical, reason=reason)
    return _rank_provider_candidates(canonical, inferred_candidates, tolerance_days, False, prefer_earlier_on_equal_distance)


def _rank_provider_candidates(
    canonical: QuarterIdentity,
    dated_candidates: list[tuple[ProviderQuarterCandidate, int]],
    tolerance_days: int,
    verified: bool,
    prefer_earlier_on_equal_distance: bool,
) -> QuarterIdentityMatch:
    within = [(candidate, diff) for candidate, diff in dated_candidates if abs(diff) <= tolerance_days]
    if not within:
        return QuarterIdentityMatch(OUTSIDE_TOLERANCE, canonical=canonical, reason=f"tolerance_days={tolerance_days}")

    exact = [(candidate, diff) for candidate, diff in within if diff == 0]
    if len(exact) == 1:
        return _matched(EXACT_VERIFIED_FISCAL if verified else EXACT_DATE_INFERRED_FISCAL, exact[0][0], canonical, 0, verified)
    if len(exact) > 1:
        return _ambiguous(canonical=canonical, reason="multiple_exact_provider_rows")

    min_distance = min(abs(diff) for _candidate, diff in within)
    nearest = [(candidate, diff) for candidate, diff in within if abs(diff) == min_distance]
    if len(nearest) == 1:
        return _matched(
            TOLERANCE_VERIFIED_FISCAL if verified else TOLERANCE_DATE_INFERRED_FISCAL,
            nearest[0][0],
            canonical,
            nearest[0][1],
            verified,
        )

    if prefer_earlier_on_equal_distance:
        earlier = [(candidate, diff) for candidate, diff in nearest if diff < 0]
        if len(earlier) == 1:
            return _matched(
                TOLERANCE_VERIFIED_FISCAL if verified else TOLERANCE_DATE_INFERRED_FISCAL,
                earlier[0][0],
                canonical,
                earlier[0][1],
                verified,
            )
    return _ambiguous(canonical=canonical, reason="equal_distance_provider_rows")


def _matched(
    outcome: str,
    candidate: ProviderQuarterCandidate,
    canonical: QuarterIdentity,
    diff: int,
    verified: bool,
) -> QuarterIdentityMatch:
    return QuarterIdentityMatch(
        outcome=outcome,
        candidate=candidate,
        canonical=canonical,
        date_diff_days=diff,
        provider_date=candidate.period_date,
        canonical_report_date=canonical.report_date,
        date_offset_days=diff,
        absolute_date_offset_days=abs(diff),
        fiscal_identity_verified=verified,
    )


def _ambiguous(
    *,
    provider: ProviderQuarterCandidate | None = None,
    canonical: QuarterIdentity | None = None,
    reason: str,
) -> QuarterIdentityMatch:
    return QuarterIdentityMatch(
        AMBIGUOUS,
        candidate=provider,
        canonical=canonical,
        reason=reason,
        ambiguous=True,
        ambiguity_reason=reason,
    )


def _canonical_date(canonical: QuarterIdentity) -> date | QuarterIdentityMatch:
    if not str(canonical.company_key or "").strip():
        return QuarterIdentityMatch(INVALID_IDENTITY, canonical=canonical, reason="canonical:company_key")
    try:
        return _parse_date(canonical.report_date)
    except ValueError as exc:
        return QuarterIdentityMatch(INVALID_IDENTITY, canonical=canonical, reason=f"canonical:{exc}")


def _has_fiscal_identity(identity: QuarterIdentity | ProviderQuarterCandidate) -> bool:
    return (
        bool(str(identity.company_key or "").strip())
        and identity.fiscal_year not in (None, "")
        and bool(str(identity.fiscal_period or "").strip())
    )


def _parse_date(value: str | None) -> date:
    if value is None:
        raise ValueError("missing_date")
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(str(value)) from exc


def _company(value: str) -> str:
    return str(value).strip().upper()


def _fy(value: int | str | None) -> str:
    return str(value).strip()


def _fq(value: str | None) -> str:
    return str(value).strip().upper()
