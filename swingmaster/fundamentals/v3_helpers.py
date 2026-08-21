from __future__ import annotations

import calendar
import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Mapping


V3_CHECK_PLAN_VERSION = "fundamental_result_check_plan_v3"
CALENDAR_COMPARISON_METHOD_APPROX_3M = "APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END"
CALENDAR_COMPARISON_METHOD_ACTUAL_RANGE = "ACTUAL_PERIOD_RANGE"
CALENDAR_COMPARISON_QUALITY_APPROX_OVERLAP = "APPROX_OVERLAP"
CALENDAR_COMPARISON_QUALITY_ACTUAL_RANGE_OVERLAP = "ACTUAL_RANGE_OVERLAP"
CALENDAR_COMPARISON_QUALITY_AMBIGUOUS = "AMBIGUOUS"
CALENDAR_COMPARISON_QUALITY_INSUFFICIENT_DATES = "INSUFFICIENT_DATES"

CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
FISCAL_QUARTERS = ("Q1", "Q2", "Q3", "Q4")


@dataclass(frozen=True)
class V3WorkUnitIdentity:
    market: str
    ticker: str
    fiscal_year: int
    fiscal_quarter: str


@dataclass(frozen=True)
class CalendarComparisonPeriod:
    period_end_date: str | None
    derived_period_start_date: str | None
    calendar_comparison_year: int | None
    calendar_comparison_quarter: str | None
    calendar_comparison_method: str
    calendar_comparison_quality: str


def make_v3_work_unit_key(*, market: Any, ticker: Any, fiscal_year: Any, fiscal_quarter: Any) -> str:
    return serialize_v3_work_unit_identity(
        V3WorkUnitIdentity(
            market=normalize_market(market),
            ticker=normalize_ticker(ticker),
            fiscal_year=int(fiscal_year),
            fiscal_quarter=normalize_fiscal_quarter(fiscal_quarter),
        )
    )


def serialize_v3_work_unit_identity(identity: V3WorkUnitIdentity) -> str:
    if not is_valid_fiscal_identity(
        market=identity.market,
        ticker=identity.ticker,
        fiscal_year=identity.fiscal_year,
        fiscal_quarter=identity.fiscal_quarter,
    ):
        raise ValueError("V3_INVALID_WORK_UNIT_IDENTITY")
    return f"{normalize_market(identity.market)}|{normalize_ticker(identity.ticker)}|{int(identity.fiscal_year)}|{normalize_fiscal_quarter(identity.fiscal_quarter)}"


def parse_v3_work_unit_key(value: str) -> V3WorkUnitIdentity:
    parts = str(value).split("|")
    if len(parts) != 4:
        raise ValueError(f"V3_INVALID_WORK_UNIT_KEY:{value}")
    market, ticker, fiscal_year, fiscal_quarter = parts
    if not is_valid_fiscal_identity(market=market, ticker=ticker, fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter):
        raise ValueError(f"V3_INVALID_WORK_UNIT_KEY:{value}")
    return V3WorkUnitIdentity(
        market=normalize_market(market),
        ticker=normalize_ticker(ticker),
        fiscal_year=int(fiscal_year),
        fiscal_quarter=normalize_fiscal_quarter(fiscal_quarter),
    )


def v3_canonical_scope_hash(work_unit_keys: list[str] | tuple[str, ...]) -> str:
    normalized = sorted({serialize_v3_work_unit_identity(parse_v3_work_unit_key(key)) for key in work_unit_keys})
    payload = "\n".join(normalized)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_market(value: Any) -> str:
    market = str(value or "").strip().lower()
    if not market:
        raise ValueError("V3_MARKET_REQUIRED")
    return market


def normalize_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper()
    if not ticker:
        raise ValueError("V3_TICKER_REQUIRED")
    return ticker


def normalize_fiscal_quarter(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"1", "Q1"}:
        return "Q1"
    if text in {"2", "Q2"}:
        return "Q2"
    if text in {"3", "Q3"}:
        return "Q3"
    if text in {"4", "Q4"}:
        return "Q4"
    raise ValueError(f"V3_INVALID_FISCAL_QUARTER:{value}")


def is_valid_fiscal_identity(*, market: Any, ticker: Any, fiscal_year: Any, fiscal_quarter: Any) -> bool:
    try:
        normalize_market(market)
        normalize_ticker(ticker)
        int(fiscal_year)
        normalize_fiscal_quarter(fiscal_quarter)
    except (TypeError, ValueError):
        return False
    return True


def derive_q_core_fields_ready(
    fundamentals: Mapping[str, Any],
    *,
    market: Any = "usa",
    ticker: Any = "UNKNOWN",
    fiscal_year: Any = 2000,
    fiscal_quarter: Any = "Q1",
    profile: str = "ORDINARY",
) -> bool:
    if profile != "ORDINARY":
        return False
    if not is_valid_fiscal_identity(
        market=market,
        ticker=ticker,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
    ):
        return False
    for field_name in CORE_FIELDS:
        if fundamentals.get(field_name) is None:
            return False
    try:
        return float(fundamentals["shares_outstanding"]) > 0
    except (TypeError, ValueError):
        return False


def derive_free_cashflow(operating_cashflow: Any, capex: Any) -> float | None:
    ocf = _to_float_or_none(operating_cashflow)
    capex_value = _to_float_or_none(capex)
    if ocf is None or capex_value is None:
        return None
    return ocf + capex_value


def derive_ordinary_ebitda(operating_income: Any, depreciation_amortization: Any) -> float | None:
    operating_income_value = _to_float_or_none(operating_income)
    da_value = _to_float_or_none(depreciation_amortization)
    if operating_income_value is None or da_value is None:
        return None
    return operating_income_value + da_value


def derive_total_debt(short_term_debt: Any, long_term_debt: Any) -> float | None:
    short_value = _to_float_or_none(short_term_debt)
    long_value = _to_float_or_none(long_term_debt)
    if short_value is None and long_value is None:
        return None
    return (short_value or 0.0) + (long_value or 0.0)


def derive_net_debt(total_debt: Any, cash: Any) -> float | None:
    total_debt_value = _to_float_or_none(total_debt)
    cash_value = _to_float_or_none(cash)
    if total_debt_value is None or cash_value is None:
        return None
    return total_debt_value - cash_value


def derive_calendar_comparison_period(period_end_date: str | date | None) -> CalendarComparisonPeriod:
    parsed_end = _parse_date(period_end_date)
    if parsed_end is None:
        return CalendarComparisonPeriod(
            period_end_date=None if period_end_date is None else str(period_end_date),
            derived_period_start_date=None,
            calendar_comparison_year=None,
            calendar_comparison_quarter=None,
            calendar_comparison_method=CALENDAR_COMPARISON_METHOD_APPROX_3M,
            calendar_comparison_quality=CALENDAR_COMPARISON_QUALITY_INSUFFICIENT_DATES,
        )
    start = _subtract_calendar_months(parsed_end, 3)
    overlaps = _calendar_quarter_overlaps(start, parsed_end)
    if not overlaps:
        quality = CALENDAR_COMPARISON_QUALITY_INSUFFICIENT_DATES
        year = None
        quarter = None
    else:
        max_days = max(days for _period, days in overlaps)
        winners = [period for period, days in overlaps if days == max_days]
        if len(winners) == 1:
            year, quarter = winners[0]
            quality = CALENDAR_COMPARISON_QUALITY_APPROX_OVERLAP
        else:
            midpoint = start + timedelta(days=((parsed_end - start).days // 2))
            midpoint_period = _calendar_quarter_for_date(midpoint)
            if midpoint_period in winners:
                year, quarter = midpoint_period
                quality = CALENDAR_COMPARISON_QUALITY_APPROX_OVERLAP
            else:
                year = None
                quarter = None
                quality = CALENDAR_COMPARISON_QUALITY_AMBIGUOUS
    return CalendarComparisonPeriod(
        period_end_date=parsed_end.isoformat(),
        derived_period_start_date=start.isoformat(),
        calendar_comparison_year=year,
        calendar_comparison_quarter=quarter,
        calendar_comparison_method=CALENDAR_COMPARISON_METHOD_APPROX_3M,
        calendar_comparison_quality=quality,
    )


def _parse_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _subtract_calendar_months(value: date, months: int) -> date:
    month_index = value.month - months
    year = value.year + ((month_index - 1) // 12)
    month = ((month_index - 1) % 12) + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _calendar_quarter_for_date(value: date) -> tuple[int, str]:
    quarter_num = ((value.month - 1) // 3) + 1
    return value.year, f"Q{quarter_num}"


def _quarter_bounds(year: int, quarter: str) -> tuple[date, date]:
    quarter_num = int(quarter[1])
    start_month = ((quarter_num - 1) * 3) + 1
    end_month = start_month + 2
    return date(year, start_month, 1), date(year, end_month, calendar.monthrange(year, end_month)[1])


def _calendar_quarter_overlaps(start: date, end: date) -> list[tuple[tuple[int, str], int]]:
    if start > end:
        return []
    periods: list[tuple[int, str]] = []
    cursor = date(start.year, ((start.month - 1) // 3) * 3 + 1, 1)
    while cursor <= end:
        period = _calendar_quarter_for_date(cursor)
        periods.append(period)
        quarter_num = int(period[1][1])
        next_month = quarter_num * 3 + 1
        next_year = period[0]
        if next_month > 12:
            next_month = 1
            next_year += 1
        cursor = date(next_year, next_month, 1)
    overlaps: list[tuple[tuple[int, str], int]] = []
    for period in periods:
        q_start, q_end = _quarter_bounds(*period)
        overlap_start = max(start, q_start)
        overlap_end = min(end, q_end)
        if overlap_start <= overlap_end:
            overlaps.append((period, (overlap_end - overlap_start).days + 1))
    return overlaps
