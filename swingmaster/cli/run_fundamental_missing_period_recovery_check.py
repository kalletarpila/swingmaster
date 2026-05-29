from __future__ import annotations

import argparse
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_yahoo_quarterly_prototype import (
    FIELD_CANDIDATES,
    collect_period_end_dates,
    resolve_series_value,
)
from swingmaster.fundamentals.providers.yahoo import YahooFinanceClient
from swingmaster.fundamentals.reporting_frequency import parse_period_date

OUTPUT_FIELDS = (
    "ticker",
    "market",
    "classification_run_id",
    "classification_as_of_date",
    "missing_period_end_date",
    "recovery_status",
    "has_core_fields",
    "found_period_end_dates",
    "reason",
    "checked_at_utc",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check whether missing quarterly periods now appear in Yahoo data")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", required=True, help="Market filter")
    parser.add_argument("--classification-run-id", required=True, help="Persisted reporting-frequency snapshot run id")
    parser.add_argument("--output", default=None, help="Optional CSV output path")
    parser.add_argument("--format", choices=("csv", "text"), default="text", help="Output format")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit after deterministic sorting")
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


def resolve_checked_at_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_classification_rows(
    db_path: Path,
    market: str,
    classification_run_id: str,
    limit: int | None,
) -> list[sqlite3.Row]:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        query = """
            SELECT ticker, market, as_of_date, missing_period_end_dates
            FROM rc_fundamental_reporting_frequency_classification
            WHERE market = ?
              AND run_id = ?
              AND reporting_frequency_class = 'QUARTERLY_MISSING_SOURCE_PERIOD'
              AND missing_period_count > 0
            ORDER BY ticker ASC
        """
        params: list[Any] = [market.strip().lower(), classification_run_id]
        if limit is not None:
            query += "\nLIMIT ?"
            params.append(limit)
        rows = conn.execute(query, tuple(params)).fetchall()
    return rows


def parse_missing_period_end_dates(missing_period_end_dates: str) -> list[str]:
    parsed: set[str] = set()
    for raw_value in str(missing_period_end_dates).split(","):
        value = raw_value.strip()
        if not value:
            continue
        try:
            parsed.add(parse_period_date(value).isoformat())
        except ValueError:
            continue
    return sorted(parsed)


def build_yahoo_period_core_field_map(payload: dict[str, Any]) -> dict[str, int]:
    income_payload = payload.get("quarterly_income_stmt", {})
    balance_payload = payload.get("quarterly_balance_sheet", {})
    cashflow_payload = payload.get("quarterly_cashflow", {})
    period_end_dates = collect_period_end_dates([income_payload, balance_payload, cashflow_payload])
    has_core_fields_by_period: dict[str, int] = {}
    for period_end_date in period_end_dates:
        has_core_fields = any(
            (
                resolve_series_value(income_payload, period_end_date, FIELD_CANDIDATES["revenue"]) is not None,
                resolve_series_value(income_payload, period_end_date, FIELD_CANDIDATES["gross_profit"]) is not None,
                resolve_series_value(income_payload, period_end_date, FIELD_CANDIDATES["operating_income"]) is not None,
                resolve_series_value(income_payload, period_end_date, FIELD_CANDIDATES["net_income"]) is not None,
                resolve_series_value(cashflow_payload, period_end_date, FIELD_CANDIDATES["operating_cashflow"]) is not None,
                resolve_series_value(cashflow_payload, period_end_date, FIELD_CANDIDATES["capex"]) is not None,
                resolve_series_value(cashflow_payload, period_end_date, FIELD_CANDIDATES["free_cashflow"]) is not None,
                resolve_series_value(balance_payload, period_end_date, FIELD_CANDIDATES["cash"]) is not None,
                resolve_series_value(balance_payload, period_end_date, FIELD_CANDIDATES["total_debt"]) is not None,
            )
        )
        has_core_fields_by_period[period_end_date] = 1 if has_core_fields else 0
    return has_core_fields_by_period


def build_recovery_rows(
    db_path: Path,
    market: str,
    classification_run_id: str,
    limit: int | None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    classification_rows = load_classification_rows(
        db_path=db_path,
        market=market,
        classification_run_id=classification_run_id,
        limit=limit,
    )
    client = YahooFinanceClient()
    checked_at_utc = resolve_checked_at_utc()
    recovery_rows: list[dict[str, object]] = []
    missing_periods_checked = 0

    for classification_row in classification_rows:
        ticker = str(classification_row["ticker"]).upper()
        normalized_market = str(classification_row["market"]).strip().lower()
        as_of_date = str(classification_row["as_of_date"])
        missing_period_end_dates = parse_missing_period_end_dates(str(classification_row["missing_period_end_dates"]))
        if not missing_period_end_dates:
            recovery_rows.append(
                {
                    "ticker": ticker,
                    "market": normalized_market,
                    "classification_run_id": classification_run_id,
                    "classification_as_of_date": as_of_date,
                    "missing_period_end_date": "",
                    "recovery_status": "NO_MISSING_PERIODS",
                    "has_core_fields": 0,
                    "found_period_end_dates": "",
                    "reason": "NO_PARSEABLE_MISSING_PERIODS",
                    "checked_at_utc": checked_at_utc,
                }
            )
            continue

        missing_periods_checked += len(missing_period_end_dates)
        try:
            payload = client.get_raw_payload(ticker)
        except Exception:
            for missing_period_end_date in missing_period_end_dates:
                recovery_rows.append(
                    {
                        "ticker": ticker,
                        "market": normalized_market,
                        "classification_run_id": classification_run_id,
                        "classification_as_of_date": as_of_date,
                        "missing_period_end_date": missing_period_end_date,
                        "recovery_status": "FETCH_FAILED",
                        "has_core_fields": 0,
                        "found_period_end_dates": "",
                        "reason": "YAHOO_FETCH_FAILED",
                        "checked_at_utc": checked_at_utc,
                    }
                )
            continue

        try:
            has_core_fields_by_period = build_yahoo_period_core_field_map(payload)
            found_period_end_dates = sorted(has_core_fields_by_period)
        except Exception:
            for missing_period_end_date in missing_period_end_dates:
                recovery_rows.append(
                    {
                        "ticker": ticker,
                        "market": normalized_market,
                        "classification_run_id": classification_run_id,
                        "classification_as_of_date": as_of_date,
                        "missing_period_end_date": missing_period_end_date,
                        "recovery_status": "PARSE_FAILED",
                        "has_core_fields": 0,
                        "found_period_end_dates": "",
                        "reason": "YAHOO_PARSE_FAILED",
                        "checked_at_utc": checked_at_utc,
                    }
                )
            continue

        for missing_period_end_date in missing_period_end_dates:
            has_core_fields = int(has_core_fields_by_period.get(missing_period_end_date, 0))
            if missing_period_end_date in has_core_fields_by_period:
                if has_core_fields == 1:
                    recovery_status = "FOUND_IN_YAHOO"
                    reason = "MISSING_PERIOD_FOUND_IN_YAHOO"
                else:
                    recovery_status = "FOUND_IN_YAHOO_INCOMPLETE"
                    reason = "MISSING_PERIOD_FOUND_BUT_INCOMPLETE"
            else:
                recovery_status = "STILL_MISSING"
                reason = "MISSING_PERIOD_STILL_MISSING"
            recovery_rows.append(
                {
                    "ticker": ticker,
                    "market": normalized_market,
                    "classification_run_id": classification_run_id,
                    "classification_as_of_date": as_of_date,
                    "missing_period_end_date": missing_period_end_date,
                    "recovery_status": recovery_status,
                    "has_core_fields": has_core_fields,
                    "found_period_end_dates": ",".join(found_period_end_dates),
                    "reason": reason,
                    "checked_at_utc": checked_at_utc,
                }
            )

    summary = {
        "market": market.strip().lower(),
        "classification_run_id": classification_run_id,
        "classification_rows_checked": len(classification_rows),
        "missing_periods_checked": missing_periods_checked,
        "found_in_yahoo_count": sum(1 for row in recovery_rows if row["recovery_status"] == "FOUND_IN_YAHOO"),
        "found_in_yahoo_incomplete_count": sum(
            1 for row in recovery_rows if row["recovery_status"] == "FOUND_IN_YAHOO_INCOMPLETE"
        ),
        "still_missing_count": sum(1 for row in recovery_rows if row["recovery_status"] == "STILL_MISSING"),
        "fetch_failed_count": sum(1 for row in recovery_rows if row["recovery_status"] == "FETCH_FAILED"),
        "parse_failed_count": sum(1 for row in recovery_rows if row["recovery_status"] == "PARSE_FAILED"),
        "no_missing_periods_count": sum(1 for row in recovery_rows if row["recovery_status"] == "NO_MISSING_PERIODS"),
    }
    return recovery_rows, summary


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
                    f"classification_run_id={row['classification_run_id']}",
                    f"classification_as_of_date={row['classification_as_of_date']}",
                    f"missing_period_end_date={row['missing_period_end_date']}",
                    f"recovery_status={row['recovery_status']}",
                    f"has_core_fields={row['has_core_fields']}",
                    f"found_period_end_dates={row['found_period_end_dates']}",
                    f"reason={row['reason']}",
                    f"checked_at_utc={row['checked_at_utc']}",
                ]
            )
        )


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    output_path = resolve_output_path(args.output)
    rows, summary = build_recovery_rows(
        db_path=db_path,
        market=args.market,
        classification_run_id=args.classification_run_id,
        limit=args.limit,
    )

    if args.format == "csv":
        write_csv_rows(rows, output_path)
    else:
        print_text_rows(rows)
        if output_path is not None:
            write_csv_rows(rows, output_path)

    _summary(market=summary["market"])
    _summary(classification_run_id=summary["classification_run_id"])
    _summary(classification_rows_checked=summary["classification_rows_checked"])
    _summary(missing_periods_checked=summary["missing_periods_checked"])
    _summary(found_in_yahoo_count=summary["found_in_yahoo_count"])
    _summary(found_in_yahoo_incomplete_count=summary["found_in_yahoo_incomplete_count"])
    _summary(still_missing_count=summary["still_missing_count"])
    _summary(fetch_failed_count=summary["fetch_failed_count"])
    _summary(parse_failed_count=summary["parse_failed_count"])
    _summary(no_missing_periods_count=summary["no_missing_periods_count"])
    _summary(output_path="" if output_path is None else str(output_path))
    _summary(limit="" if args.limit is None else args.limit)


if __name__ == "__main__":
    main()
