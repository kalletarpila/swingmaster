from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path


LOOKBACK_MONTHS_DEFAULT = 30
RECENT_WINDOW_DAYS = 460
QUARTER_MIN_GAP_DAYS = 60
QUARTER_MAX_GAP_DAYS = 120
HALF_YEAR_MIN_GAP_DAYS = 150
HALF_YEAR_MAX_GAP_DAYS = 220
OUTPUT_FIELDS = (
    "ticker",
    "market",
    "period_count_in_lookback",
    "period_end_dates",
    "reporting_frequency_class",
    "inferred_reporting_frequency",
    "has_valid_ttm_coverage",
    "reason",
)


@dataclass(frozen=True)
class ReportingFrequencyClassification:
    reporting_frequency_class: str
    inferred_reporting_frequency: str
    has_valid_ttm_coverage: int
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit observed fundamental reporting frequency from quarterly rows")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", required=True, help="Market filter")
    parser.add_argument("--lookback-months", type=int, default=LOOKBACK_MONTHS_DEFAULT, help="Lookback window in months")
    parser.add_argument("--output", default=None, help="Optional CSV output path")
    parser.add_argument("--format", choices=("csv", "text"), default="text", help="Output format")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def resolve_output_path(output_arg: str | None) -> Path | None:
    if output_arg is None:
        return None
    return Path(output_arg).expanduser().resolve()


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


def load_reporting_frequency_rows(db_path: Path, market: str, lookback_months: int) -> list[dict[str, object]]:
    with sqlite3.connect(str(db_path)) as conn:
        raw_rows = conn.execute(
            """
            SELECT ticker, period_end_date
            FROM rc_fundamental_quarterly
            ORDER BY ticker ASC, period_end_date ASC
            """
        ).fetchall()

    by_ticker: dict[str, list[str]] = {}
    for ticker_value, period_end_date_value in raw_rows:
        ticker = str(ticker_value).upper()
        if not market_matches_ticker(market, ticker):
            continue
        by_ticker.setdefault(ticker, []).append(str(period_end_date_value))

    rows: list[dict[str, object]] = []
    normalized_market = market.strip().lower()
    for ticker in sorted(by_ticker):
        all_period_end_dates = sorted({value for value in by_ticker[ticker]})
        filtered_period_end_dates = all_period_end_dates
        try:
            parsed_dates = sorted(parse_period_date(value) for value in all_period_end_dates)
            latest_date = parsed_dates[-1]
            lookback_start = subtract_months_floor(latest_date, lookback_months)
            filtered_period_end_dates = [value.isoformat() for value in parsed_dates if value >= lookback_start]
        except ValueError:
            filtered_period_end_dates = all_period_end_dates

        classification = classify_reporting_frequency(filtered_period_end_dates)
        rows.append(
            {
                "ticker": ticker,
                "market": normalized_market,
                "period_count_in_lookback": len(filtered_period_end_dates),
                "period_end_dates": ",".join(filtered_period_end_dates),
                "reporting_frequency_class": classification.reporting_frequency_class,
                "inferred_reporting_frequency": classification.inferred_reporting_frequency,
                "has_valid_ttm_coverage": classification.has_valid_ttm_coverage,
                "reason": classification.reason,
            }
        )
    return rows


def write_csv_rows(rows: list[dict[str, object]], output_path: Path | None) -> None:
    if output_path is None:
        import sys

        writer = csv.DictWriter(sys.stdout, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def print_text_rows(rows: list[dict[str, object]]) -> None:
    for row in rows:
        print(
            " | ".join(
                [
                    f"ticker={row['ticker']}",
                    f"market={row['market']}",
                    f"period_count_in_lookback={row['period_count_in_lookback']}",
                    f"period_end_dates={row['period_end_dates']}",
                    f"reporting_frequency_class={row['reporting_frequency_class']}",
                    f"inferred_reporting_frequency={row['inferred_reporting_frequency']}",
                    f"has_valid_ttm_coverage={row['has_valid_ttm_coverage']}",
                    f"reason={row['reason']}",
                ]
            )
        )


def build_summary(rows: list[dict[str, object]], market: str, output_path: Path | None) -> dict[str, object]:
    quarterly_count = sum(1 for row in rows if row["reporting_frequency_class"] == "QUARTERLY")
    true_semiannual_count = sum(1 for row in rows if row["reporting_frequency_class"] == "TRUE_SEMIANNUAL")
    quarterly_missing_source_period_count = sum(
        1 for row in rows if row["reporting_frequency_class"] == "QUARTERLY_MISSING_SOURCE_PERIOD"
    )
    annual_only_count = sum(1 for row in rows if row["reporting_frequency_class"] == "ANNUAL_ONLY")
    other_insufficient_count = sum(1 for row in rows if row["reporting_frequency_class"] == "OTHER_INSUFFICIENT")
    unknown_count = sum(1 for row in rows if row["reporting_frequency_class"] == "UNKNOWN")
    semiannual_count = sum(1 for row in rows if row["inferred_reporting_frequency"] == "SEMIANNUAL")
    insufficient_count = sum(1 for row in rows if row["inferred_reporting_frequency"] == "INSUFFICIENT")
    valid_ttm_coverage_count = sum(int(row["has_valid_ttm_coverage"]) for row in rows)
    return {
        "market": market.strip().lower(),
        "tickers_total": len(rows),
        "quarterly_count": quarterly_count,
        "true_semiannual_count": true_semiannual_count,
        "quarterly_missing_source_period_count": quarterly_missing_source_period_count,
        "semiannual_count": semiannual_count,
        "annual_only_count": annual_only_count,
        "other_insufficient_count": other_insufficient_count,
        "insufficient_count": insufficient_count,
        "unknown_count": unknown_count,
        "valid_ttm_coverage_count": valid_ttm_coverage_count,
        "output_path": "" if output_path is None else str(output_path),
    }


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    output_path = resolve_output_path(args.output)
    rows = load_reporting_frequency_rows(
        db_path=db_path,
        market=args.market,
        lookback_months=args.lookback_months,
    )

    if args.format == "csv":
        write_csv_rows(rows, output_path)
    else:
        print_text_rows(rows)
        if output_path is not None:
            write_csv_rows(rows, output_path)

    if args.format == "text":
        summary = build_summary(rows, market=args.market, output_path=output_path)
        _summary(market=summary["market"])
        _summary(tickers_total=summary["tickers_total"])
        _summary(quarterly_count=summary["quarterly_count"])
        _summary(true_semiannual_count=summary["true_semiannual_count"])
        _summary(quarterly_missing_source_period_count=summary["quarterly_missing_source_period_count"])
        _summary(semiannual_count=summary["semiannual_count"])
        _summary(annual_only_count=summary["annual_only_count"])
        _summary(other_insufficient_count=summary["other_insufficient_count"])
        _summary(insufficient_count=summary["insufficient_count"])
        _summary(unknown_count=summary["unknown_count"])
        _summary(valid_ttm_coverage_count=summary["valid_ttm_coverage_count"])
        _summary(output_path=summary["output_path"])


if __name__ == "__main__":
    main()
