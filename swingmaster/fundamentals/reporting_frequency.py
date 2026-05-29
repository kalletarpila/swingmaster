from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date


LOOKBACK_MONTHS_DEFAULT = 30
RECENT_WINDOW_DAYS = 460
QUARTER_MIN_GAP_DAYS = 60
QUARTER_MAX_GAP_DAYS = 120
HALF_YEAR_MIN_GAP_DAYS = 150
HALF_YEAR_MAX_GAP_DAYS = 220


@dataclass(frozen=True)
class ReportingFrequencyClassification:
    reporting_frequency_class: str
    inferred_reporting_frequency: str
    has_valid_ttm_coverage: int
    reason: str


def market_matches_ticker(market: str, ticker: str) -> bool:
    normalized_market = market.strip().lower()
    normalized_ticker = ticker.strip().upper()
    if normalized_market == "usa":
        return not normalized_ticker.endswith(".HE")
    if normalized_market == "omxh":
        return normalized_ticker.endswith(".HE")
    return True


def subtract_months_floor(value: date, months: int) -> date:
    year = value.year
    month = value.month - months
    while month <= 0:
        year -= 1
        month += 12
    return date(year, month, 1)


def parse_period_date(value: str) -> date:
    return date.fromisoformat(value)


def classify_reporting_frequency(period_end_dates: list[str]) -> ReportingFrequencyClassification:
    if not period_end_dates:
        return ReportingFrequencyClassification("OTHER_INSUFFICIENT", "INSUFFICIENT", 0, "NO_PERIOD_ROWS")

    try:
        parsed_dates = sorted({parse_period_date(value) for value in period_end_dates})
    except ValueError:
        return ReportingFrequencyClassification("UNKNOWN", "UNKNOWN", 0, "MALFORMED_PERIOD_DATES")

    if not parsed_dates:
        return ReportingFrequencyClassification("OTHER_INSUFFICIENT", "INSUFFICIENT", 0, "NO_PERIOD_ROWS")

    latest_date = parsed_dates[-1]
    recent_dates = [value for value in parsed_dates if (latest_date - value).days <= RECENT_WINDOW_DAYS]
    recent_count = len(recent_dates)
    if recent_count == 0:
        return ReportingFrequencyClassification("OTHER_INSUFFICIENT", "INSUFFICIENT", 0, "OTHER_INSUFFICIENT_RECENT_PERIODS")
    if recent_count == 1 and len(parsed_dates) == 1:
        return ReportingFrequencyClassification("ANNUAL_ONLY", "ANNUAL_ONLY", 0, "ONLY_ONE_RECENT_PERIOD")
    if recent_count == 1:
        return ReportingFrequencyClassification("OTHER_INSUFFICIENT", "INSUFFICIENT", 0, "OTHER_INSUFFICIENT_RECENT_PERIODS")

    recent_gaps = [(right - left).days for left, right in zip(recent_dates, recent_dates[1:])]
    latest_two_gap = recent_gaps[-1]

    if recent_count >= 4:
        latest_four_dates = recent_dates[-4:]
        latest_four_gaps = [(right - left).days for left, right in zip(latest_four_dates, latest_four_dates[1:])]
        if all(QUARTER_MIN_GAP_DAYS <= gap <= QUARTER_MAX_GAP_DAYS for gap in latest_four_gaps):
            return ReportingFrequencyClassification("QUARTERLY", "QUARTERLY", 1, "ENOUGH_RECENT_QUARTERS")

    if recent_count >= 5:
        latest_five_dates = recent_dates[-5:]
        latest_five_gaps = [(right - left).days for left, right in zip(latest_five_dates, latest_five_dates[1:])]
        quarterly_like_gap_count = sum(1 for gap in latest_five_gaps if QUARTER_MIN_GAP_DAYS <= gap <= QUARTER_MAX_GAP_DAYS)
        missing_period_gap_count = sum(1 for gap in latest_five_gaps if HALF_YEAR_MIN_GAP_DAYS <= gap <= HALF_YEAR_MAX_GAP_DAYS)
        if quarterly_like_gap_count == 3 and missing_period_gap_count == 1:
            return ReportingFrequencyClassification(
                "QUARTERLY_MISSING_SOURCE_PERIOD",
                "INSUFFICIENT",
                0,
                "QUARTERLY_PATTERN_WITH_MISSING_RECENT_PERIOD",
            )

    if recent_count >= 4:
        latest_four_dates = recent_dates[-4:]
        latest_four_gaps = [(right - left).days for left, right in zip(latest_four_dates, latest_four_dates[1:])]
        if (
            QUARTER_MIN_GAP_DAYS <= latest_four_gaps[0] <= QUARTER_MAX_GAP_DAYS
            and QUARTER_MIN_GAP_DAYS <= latest_four_gaps[1] <= QUARTER_MAX_GAP_DAYS
            and HALF_YEAR_MIN_GAP_DAYS <= latest_four_gaps[2] <= HALF_YEAR_MAX_GAP_DAYS
        ):
            return ReportingFrequencyClassification(
                "QUARTERLY_MISSING_SOURCE_PERIOD",
                "INSUFFICIENT",
                0,
                "QUARTERLY_PATTERN_WITH_MISSING_RECENT_PERIOD",
            )

    if (
        recent_count >= 2
        and recent_count <= 3
        and HALF_YEAR_MIN_GAP_DAYS <= latest_two_gap <= HALF_YEAR_MAX_GAP_DAYS
        and all(HALF_YEAR_MIN_GAP_DAYS <= gap <= HALF_YEAR_MAX_GAP_DAYS for gap in recent_gaps)
    ):
        return ReportingFrequencyClassification("TRUE_SEMIANNUAL", "SEMIANNUAL", 1, "CONSISTENT_HALF_YEAR_PERIODS")

    return ReportingFrequencyClassification("OTHER_INSUFFICIENT", "INSUFFICIENT", 0, "OTHER_INSUFFICIENT_RECENT_PERIODS")


def load_ticker_period_end_dates(conn: sqlite3.Connection, ticker: str, lookback_months: int = LOOKBACK_MONTHS_DEFAULT) -> list[str]:
    rows = conn.execute(
        """
        SELECT period_end_date
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
        ORDER BY period_end_date ASC
        """,
        (ticker.upper(),),
    ).fetchall()
    all_period_end_dates = sorted({str(row[0]) for row in rows})
    if not all_period_end_dates:
        return []
    try:
        parsed_dates = sorted(parse_period_date(value) for value in all_period_end_dates)
        latest_date = parsed_dates[-1]
        lookback_start = subtract_months_floor(latest_date, lookback_months)
        return [value.isoformat() for value in parsed_dates if value >= lookback_start]
    except ValueError:
        return all_period_end_dates


def classify_ticker_reporting_frequency(
    conn: sqlite3.Connection,
    ticker: str,
    lookback_months: int = LOOKBACK_MONTHS_DEFAULT,
) -> ReportingFrequencyClassification:
    return classify_reporting_frequency(load_ticker_period_end_dates(conn, ticker, lookback_months=lookback_months))
