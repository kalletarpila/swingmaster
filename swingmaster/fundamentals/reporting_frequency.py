from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from calendar import monthrange


LOOKBACK_MONTHS_DEFAULT = 30
RECENT_WINDOW_DAYS = 460
QUARTER_MIN_GAP_DAYS = 60
QUARTER_MAX_GAP_DAYS = 120
HALF_YEAR_MIN_GAP_DAYS = 150
HALF_YEAR_MAX_GAP_DAYS = 220
CLASSIFIER_VERSION = "reporting_frequency_v1"


@dataclass(frozen=True)
class ReportingFrequencyClassification:
    reporting_frequency_class: str
    inferred_reporting_frequency: str
    has_valid_ttm_coverage: int
    reason: str
    observed_period_end_dates: tuple[str, ...]
    expected_period_end_dates: tuple[str, ...]
    missing_period_end_dates: tuple[str, ...]
    missing_period_count: int
    source_data_max_period_end_date: str
    classifier_version: str


def _build_classification(
    reporting_frequency_class: str,
    inferred_reporting_frequency: str,
    has_valid_ttm_coverage: int,
    reason: str,
    observed_period_end_dates: tuple[str, ...],
    expected_period_end_dates: tuple[str, ...] = (),
    missing_period_end_dates: tuple[str, ...] = (),
    source_data_max_period_end_date: str = "",
) -> ReportingFrequencyClassification:
    return ReportingFrequencyClassification(
        reporting_frequency_class=reporting_frequency_class,
        inferred_reporting_frequency=inferred_reporting_frequency,
        has_valid_ttm_coverage=has_valid_ttm_coverage,
        reason=reason,
        observed_period_end_dates=observed_period_end_dates,
        expected_period_end_dates=expected_period_end_dates,
        missing_period_end_dates=missing_period_end_dates,
        missing_period_count=len(missing_period_end_dates),
        source_data_max_period_end_date=source_data_max_period_end_date,
        classifier_version=CLASSIFIER_VERSION,
    )


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


def add_months_preserving_month_end(value: date, months: int) -> date:
    year = value.year
    month = value.month + months
    while month > 12:
        year += 1
        month -= 12
    while month <= 0:
        year -= 1
        month += 12
    day = monthrange(year, month)[1]
    return date(year, month, day)


def _expected_quarterly_periods(observed_dates: list[date]) -> tuple[str, ...]:
    if not observed_dates:
        return ()
    expected_dates = [observed_dates[0]]
    current = observed_dates[0]
    last = observed_dates[-1]
    while True:
        next_value = add_months_preserving_month_end(current, 3)
        if next_value > last:
            break
        expected_dates.append(next_value)
        current = next_value
    return tuple(value.isoformat() for value in expected_dates)


def classify_reporting_frequency(period_end_dates: list[str]) -> ReportingFrequencyClassification:
    observed_periods = tuple(sorted({str(value) for value in period_end_dates}))
    if not period_end_dates:
        return _build_classification("OTHER_INSUFFICIENT", "INSUFFICIENT", 0, "NO_PERIOD_ROWS", observed_periods)

    try:
        parsed_dates = sorted({parse_period_date(value) for value in period_end_dates})
    except ValueError:
        return _build_classification("UNKNOWN", "UNKNOWN", 0, "MALFORMED_PERIOD_DATES", observed_periods)

    if not parsed_dates:
        return _build_classification("OTHER_INSUFFICIENT", "INSUFFICIENT", 0, "NO_PERIOD_ROWS", observed_periods)

    latest_date = parsed_dates[-1]
    recent_dates = [value for value in parsed_dates if (latest_date - value).days <= RECENT_WINDOW_DAYS]
    recent_count = len(recent_dates)
    observed_recent_periods = tuple(value.isoformat() for value in recent_dates)
    source_data_max_period_end_date = latest_date.isoformat()
    if recent_count == 0:
        return _build_classification(
            "OTHER_INSUFFICIENT",
            "INSUFFICIENT",
            0,
            "OTHER_INSUFFICIENT_RECENT_PERIODS",
            observed_recent_periods,
            source_data_max_period_end_date=source_data_max_period_end_date,
        )
    if recent_count == 1 and len(parsed_dates) == 1:
        return _build_classification(
            "ANNUAL_ONLY",
            "ANNUAL_ONLY",
            0,
            "ONLY_ONE_RECENT_PERIOD",
            observed_recent_periods,
            source_data_max_period_end_date=source_data_max_period_end_date,
        )
    if recent_count == 1:
        return _build_classification(
            "OTHER_INSUFFICIENT",
            "INSUFFICIENT",
            0,
            "OTHER_INSUFFICIENT_RECENT_PERIODS",
            observed_recent_periods,
            source_data_max_period_end_date=source_data_max_period_end_date,
        )

    recent_gaps = [(right - left).days for left, right in zip(recent_dates, recent_dates[1:])]
    latest_two_gap = recent_gaps[-1]

    if recent_count >= 4:
        latest_four_dates = recent_dates[-4:]
        latest_four_gaps = [(right - left).days for left, right in zip(latest_four_dates, latest_four_dates[1:])]
        if all(QUARTER_MIN_GAP_DAYS <= gap <= QUARTER_MAX_GAP_DAYS for gap in latest_four_gaps):
            return _build_classification(
                "QUARTERLY",
                "QUARTERLY",
                1,
                "ENOUGH_RECENT_QUARTERS",
                observed_recent_periods,
                source_data_max_period_end_date=source_data_max_period_end_date,
            )

    if recent_count >= 5:
        latest_five_dates = recent_dates[-5:]
        latest_five_gaps = [(right - left).days for left, right in zip(latest_five_dates, latest_five_dates[1:])]
        quarterly_like_gap_count = sum(1 for gap in latest_five_gaps if QUARTER_MIN_GAP_DAYS <= gap <= QUARTER_MAX_GAP_DAYS)
        missing_period_gap_count = sum(1 for gap in latest_five_gaps if HALF_YEAR_MIN_GAP_DAYS <= gap <= HALF_YEAR_MAX_GAP_DAYS)
        if quarterly_like_gap_count == 3 and missing_period_gap_count == 1:
            expected_period_end_dates = _expected_quarterly_periods(latest_five_dates)
            missing_period_end_dates = tuple(
                value for value in expected_period_end_dates if value not in observed_recent_periods
            )
            return _build_classification(
                "QUARTERLY_MISSING_SOURCE_PERIOD",
                "INSUFFICIENT",
                0,
                "QUARTERLY_PATTERN_WITH_MISSING_RECENT_PERIOD",
                observed_recent_periods,
                expected_period_end_dates=expected_period_end_dates,
                missing_period_end_dates=missing_period_end_dates,
                source_data_max_period_end_date=source_data_max_period_end_date,
            )

    if recent_count >= 4:
        latest_four_dates = recent_dates[-4:]
        latest_four_gaps = [(right - left).days for left, right in zip(latest_four_dates, latest_four_dates[1:])]
        if (
            QUARTER_MIN_GAP_DAYS <= latest_four_gaps[0] <= QUARTER_MAX_GAP_DAYS
            and QUARTER_MIN_GAP_DAYS <= latest_four_gaps[1] <= QUARTER_MAX_GAP_DAYS
            and HALF_YEAR_MIN_GAP_DAYS <= latest_four_gaps[2] <= HALF_YEAR_MAX_GAP_DAYS
        ):
            expected_period_end_dates = _expected_quarterly_periods(latest_four_dates)
            missing_period_end_dates = tuple(
                value for value in expected_period_end_dates if value not in observed_recent_periods
            )
            return _build_classification(
                "QUARTERLY_MISSING_SOURCE_PERIOD",
                "INSUFFICIENT",
                0,
                "QUARTERLY_PATTERN_WITH_MISSING_RECENT_PERIOD",
                observed_recent_periods,
                expected_period_end_dates=expected_period_end_dates,
                missing_period_end_dates=missing_period_end_dates,
                source_data_max_period_end_date=source_data_max_period_end_date,
            )

    if (
        recent_count >= 2
        and recent_count <= 3
        and HALF_YEAR_MIN_GAP_DAYS <= latest_two_gap <= HALF_YEAR_MAX_GAP_DAYS
        and all(HALF_YEAR_MIN_GAP_DAYS <= gap <= HALF_YEAR_MAX_GAP_DAYS for gap in recent_gaps)
    ):
        return _build_classification(
            "TRUE_SEMIANNUAL",
            "SEMIANNUAL",
            1,
            "CONSISTENT_HALF_YEAR_PERIODS",
            observed_recent_periods,
            source_data_max_period_end_date=source_data_max_period_end_date,
        )

    return _build_classification(
        "OTHER_INSUFFICIENT",
        "INSUFFICIENT",
        0,
        "OTHER_INSUFFICIENT_RECENT_PERIODS",
        observed_recent_periods,
        source_data_max_period_end_date=source_data_max_period_end_date,
    )


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
