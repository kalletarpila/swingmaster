from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable


DEFAULT_QUARTER_DATE_TOLERANCE_DAYS = 7


@dataclass(frozen=True)
class QuarterIdentity:
    company_key: str
    fiscal_year: int | str | None
    fiscal_period: str | None
    report_date: str | None


@dataclass(frozen=True)
class ProviderQuarterCandidate:
    candidate_id: str
    company_key: str
    fiscal_year: int | str | None
    fiscal_period: str | None
    period_date: str | None


@dataclass(frozen=True)
class QuarterIdentityMatch:
    outcome: str
    candidate: ProviderQuarterCandidate | None = None
    date_diff_days: int | None = None
    reason: str = ""


def match_cross_provider_quarter(
    canonical: QuarterIdentity,
    candidates: Iterable[ProviderQuarterCandidate],
    *,
    tolerance_days: int = DEFAULT_QUARTER_DATE_TOLERANCE_DAYS,
    prefer_earlier_on_equal_distance: bool = False,
) -> QuarterIdentityMatch:
    missing = _missing_identity(canonical)
    if missing:
        return QuarterIdentityMatch("MISSING_IDENTITY_COMPONENT", reason="canonical:" + ",".join(missing))
    try:
        canonical_date = _parse_date(canonical.report_date)
    except ValueError as exc:
        return QuarterIdentityMatch("INVALID_DATE", reason=f"canonical:{exc}")

    fiscal_candidates: list[tuple[ProviderQuarterCandidate, int]] = []
    missing_candidate_identity = False
    invalid_candidate_date = False
    for candidate in candidates:
        if _missing_identity(candidate):
            missing_candidate_identity = True
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
        diff = (provider_date - canonical_date).days
        fiscal_candidates.append((candidate, diff))

    if not fiscal_candidates:
        if missing_candidate_identity:
            return QuarterIdentityMatch("MISSING_IDENTITY_COMPONENT", reason="provider")
        if invalid_candidate_date:
            return QuarterIdentityMatch("INVALID_DATE", reason="provider")
        return QuarterIdentityMatch("NO_MATCH_FISCAL_IDENTITY")

    within = [(candidate, diff) for candidate, diff in fiscal_candidates if abs(diff) <= tolerance_days]
    if not within:
        return QuarterIdentityMatch("OUTSIDE_DATE_TOLERANCE")

    exact = [(candidate, diff) for candidate, diff in within if diff == 0]
    if len(exact) == 1:
        return QuarterIdentityMatch("EXACT_DATE_MATCH", exact[0][0], 0)
    if len(exact) > 1:
        return QuarterIdentityMatch("AMBIGUOUS_MULTIPLE_MATCHES", reason="multiple_exact")

    min_distance = min(abs(diff) for _candidate, diff in within)
    nearest = [(candidate, diff) for candidate, diff in within if abs(diff) == min_distance]
    if len(nearest) == 1:
        return QuarterIdentityMatch("TOLERANCE_MATCH", nearest[0][0], nearest[0][1])

    if prefer_earlier_on_equal_distance:
        earlier = [(candidate, diff) for candidate, diff in nearest if diff < 0]
        if len(earlier) == 1:
            return QuarterIdentityMatch("TOLERANCE_MATCH", earlier[0][0], earlier[0][1])

    return QuarterIdentityMatch("AMBIGUOUS_MULTIPLE_MATCHES", reason="equal_distance")


def _missing_identity(identity: QuarterIdentity | ProviderQuarterCandidate) -> list[str]:
    missing: list[str] = []
    if not str(identity.company_key or "").strip():
        missing.append("company_key")
    if identity.fiscal_year in (None, ""):
        missing.append("fiscal_year")
    if not str(identity.fiscal_period or "").strip():
        missing.append("fiscal_period")
    date_value = identity.report_date if isinstance(identity, QuarterIdentity) else identity.period_date
    if not str(date_value or "").strip():
        missing.append("date")
    return missing


def _parse_date(value: str | None) -> date:
    if value is None:
        raise ValueError("missing")
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
