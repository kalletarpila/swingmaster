from __future__ import annotations

import argparse
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from swingmaster.fundamentals.reporting_frequency import (
    LOOKBACK_MONTHS_DEFAULT,
    ReportingFrequencyClassification,
    classify_reporting_frequency,
    market_matches_ticker,
    parse_period_date,
    subtract_months_floor,
)

OUTPUT_FIELDS = (
    "ticker",
    "market",
    "period_count_in_lookback",
    "period_end_dates",
    "observed_period_end_dates",
    "expected_period_end_dates",
    "missing_period_end_dates",
    "missing_period_count",
    "source_data_max_period_end_date",
    "classifier_version",
    "reporting_frequency_class",
    "inferred_reporting_frequency",
    "has_valid_ttm_coverage",
    "reason",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit observed fundamental reporting frequency from quarterly rows")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", required=True, help="Market filter")
    parser.add_argument("--lookback-months", type=int, default=LOOKBACK_MONTHS_DEFAULT, help="Lookback window in months")
    parser.add_argument("--output", default=None, help="Optional CSV output path")
    parser.add_argument("--format", choices=("csv", "text"), default="text", help="Output format")
    parser.add_argument("--write-db", action="store_true", help="Write classification rows to SQLite snapshot table")
    parser.add_argument("--as-of-date", default=None, help="Snapshot as_of_date in YYYY-MM-DD format")
    parser.add_argument("--run-id", default=None, help="Deterministic write run identifier")
    parser.add_argument("--write-mode", choices=("insert", "replace-run"), default="insert", help="DB write mode")
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


def resolve_created_at_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def validate_write_args(args: argparse.Namespace) -> None:
    if not getattr(args, "write_db", False):
        return
    if args.as_of_date is None:
        raise SystemExit("FUNDAMENTAL_REPORTING_FREQUENCY_AUDIT_AS_OF_DATE_REQUIRED")
    if args.run_id is None:
        raise SystemExit("FUNDAMENTAL_REPORTING_FREQUENCY_AUDIT_RUN_ID_REQUIRED")
    parse_period_date(args.as_of_date)


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
                "observed_period_end_dates": ",".join(classification.observed_period_end_dates),
                "expected_period_end_dates": ",".join(classification.expected_period_end_dates),
                "missing_period_end_dates": ",".join(classification.missing_period_end_dates),
                "missing_period_count": classification.missing_period_count,
                "source_data_max_period_end_date": classification.source_data_max_period_end_date,
                "classifier_version": classification.classifier_version,
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
                    f"missing_period_end_dates={row['missing_period_end_dates']}",
                    f"missing_period_count={row['missing_period_count']}",
                    f"reason={row['reason']}",
                ]
            )
        )


def delete_rows_for_run_id(conn: sqlite3.Connection, run_id: str) -> int:
    cursor = conn.execute(
        """
        DELETE FROM rc_fundamental_reporting_frequency_classification
        WHERE run_id = ?
        """,
        (run_id,),
    )
    return int(cursor.rowcount)


def insert_classification_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, object]],
    as_of_date: str,
    lookback_months: int,
    run_id: str,
    created_at_utc: str,
) -> int:
    conn.executemany(
        """
        INSERT INTO rc_fundamental_reporting_frequency_classification (
            ticker,
            market,
            as_of_date,
            lookback_months,
            reporting_frequency_class,
            inferred_reporting_frequency,
            has_valid_ttm_coverage,
            reason,
            period_count_in_lookback,
            observed_period_end_dates,
            expected_period_end_dates,
            missing_period_end_dates,
            missing_period_count,
            source_data_max_period_end_date,
            classifier_version,
            run_id,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["market"],
                as_of_date,
                lookback_months,
                row["reporting_frequency_class"],
                row["inferred_reporting_frequency"],
                row["has_valid_ttm_coverage"],
                row["reason"],
                row["period_count_in_lookback"],
                row["observed_period_end_dates"],
                row["expected_period_end_dates"],
                row["missing_period_end_dates"],
                row["missing_period_count"],
                row["source_data_max_period_end_date"],
                row["classifier_version"],
                run_id,
                created_at_utc,
            )
            for row in rows
        ],
    )
    return len(rows)


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
    validate_write_args(args)
    db_path = resolve_db_path(args.db)
    output_path = resolve_output_path(args.output)
    rows = load_reporting_frequency_rows(
        db_path=db_path,
        market=args.market,
        lookback_months=args.lookback_months,
    )
    rows_written = 0

    if args.format == "csv":
        write_csv_rows(rows, output_path)
    else:
        print_text_rows(rows)
        if output_path is not None:
            write_csv_rows(rows, output_path)

    if args.write_db:
        created_at_utc = resolve_created_at_utc()
        with sqlite3.connect(str(db_path)) as conn:
            if args.write_mode == "replace-run":
                delete_rows_for_run_id(conn, str(args.run_id))
            rows_written = insert_classification_rows(
                conn=conn,
                rows=rows,
                as_of_date=str(args.as_of_date),
                lookback_months=int(args.lookback_months),
                run_id=str(args.run_id),
                created_at_utc=created_at_utc,
            )
            conn.commit()

    if args.format == "text" or args.write_db:
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
        _summary(write_db=1 if args.write_db else 0)
        if args.write_db:
            _summary(write_mode=args.write_mode)
            _summary(rows_written=rows_written)
            _summary(as_of_date=str(args.as_of_date))
            _summary(run_id=str(args.run_id))


if __name__ == "__main__":
    main()
