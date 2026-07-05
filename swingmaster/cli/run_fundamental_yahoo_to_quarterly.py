from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS
from swingmaster.fundamentals.reported_yahoo_dual_write_adapter import (
    write_yahoo_quarterly_rows_with_optional_vintage,
)


DEFAULT_MARKET = "omxh"
DEFAULT_SYMBOL = "NOKIA.HE"
CORE_FIELDS = (
    "revenue",
    "operating_income",
    "net_income",
    "operating_cashflow",
    "cash",
    "total_debt",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge Yahoo quarterly rows into generic rc_fundamental_quarterly")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", default=DEFAULT_MARKET, help="Market code")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Validate and map only without writing rows")
    parser.add_argument(
        "--replace-symbol",
        action="store_true",
        help="Delete existing rc_fundamental_quarterly rows for the selected ticker before insert",
    )
    parser.add_argument("--write-vintage", action="store_true", help="Opt in to latest/vintage/provenance writes")
    parser.add_argument("--vintage-market", help="Vintage market; required with --write-vintage")
    parser.add_argument("--vintage-available-at-utc", help="PIT availability timestamp; required with --write-vintage")
    parser.add_argument("--vintage-ingested-at-utc", help="Ingestion timestamp; required with --write-vintage")
    parser.add_argument("--vintage-run-id", help="Vintage write run id; required with --write-vintage")
    parser.add_argument(
        "--vintage-normalization-run-id",
        help="Optional normalization run id for vintage metadata",
    )
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def load_yahoo_quarterly_rows(conn: sqlite3.Connection, market: str, symbol: str) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                market,
                symbol,
                period_end_date,
                revenue,
                gross_profit,
                operating_income,
                net_income,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                shares_outstanding,
                shares_source,
                shares_quality,
                source_run_id,
                run_id,
                created_at_utc
            FROM rc_fundamental_yahoo_quarterly
            WHERE market = ?
              AND symbol = ?
            ORDER BY period_end_date ASC
            """,
            (market, symbol.upper()),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return rows


def should_write_row(row: sqlite3.Row) -> bool:
    return any(row[field_name] is not None for field_name in CORE_FIELDS)


def map_to_generic_quarterly_rows(rows: list[sqlite3.Row], run_id: str) -> list[dict[str, Any]]:
    mapped_rows: list[dict[str, Any]] = []
    for row in rows:
        if not should_write_row(row):
            continue
        mapped_rows.append(
            {
                "ticker": str(row["symbol"]).upper(),
                "period_end_date": str(row["period_end_date"]),
                "revenue": row["revenue"],
                "gross_profit": row["gross_profit"],
                "operating_income": row["operating_income"],
                "ebit": row["operating_income"],
                "ebitda": None,
                "net_income": row["net_income"],
                "operating_cashflow": row["operating_cashflow"],
                "capex": row["capex"],
                "free_cashflow": row["free_cashflow"],
                "cash": row["cash"],
                "total_debt": row["total_debt"],
                "shares_outstanding": row["shares_outstanding"],
                "currency": None,
                "run_id": run_id,
            }
        )
    return mapped_rows


def replace_symbol_rows(conn: sqlite3.Connection, symbol: str) -> int:
    cursor = conn.execute(
        """
        DELETE FROM rc_fundamental_quarterly
        WHERE ticker = ?
        """,
        (symbol.upper(),),
    )
    return int(cursor.rowcount)


def insert_quarterly_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    conn.executemany(
        """
        INSERT OR REPLACE INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            ebit,
            ebitda,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            currency,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["period_end_date"],
                row["revenue"],
                row["gross_profit"],
                row["operating_income"],
                row["ebit"],
                row["ebitda"],
                row["net_income"],
                row["operating_cashflow"],
                row["capex"],
                row["free_cashflow"],
                row["cash"],
                row["total_debt"],
                row["shares_outstanding"],
                row["currency"],
                row["run_id"],
            )
            for row in rows
        ],
    )
    return len(rows)


def yahoo_quarterly_rows_by_generic_key(rows: list[sqlite3.Row]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (str(row["symbol"]).upper(), str(row["period_end_date"])): dict(row)
        for row in rows
        if should_write_row(row)
    }


def run_yahoo_to_quarterly(
    db_path: Path,
    market: str,
    symbol: str,
    run_id: str,
    dry_run: bool,
    replace_symbol: bool,
    *,
    write_vintage: bool = False,
    vintage_market: str | None = None,
    vintage_available_at_utc: str | None = None,
    vintage_ingested_at_utc: str | None = None,
    vintage_run_id: str | None = None,
    vintage_normalization_run_id: str | None = None,
) -> dict[str, Any]:
    normalized_symbol = symbol.upper()
    if write_vintage:
        _validate_vintage_args(
            vintage_market=vintage_market,
            vintage_available_at_utc=vintage_available_at_utc,
            vintage_ingested_at_utc=vintage_ingested_at_utc,
            vintage_run_id=vintage_run_id,
        )

    with sqlite3.connect(str(db_path)) as conn:
        input_rows = load_yahoo_quarterly_rows(conn, market, normalized_symbol)
    mapped_rows = map_to_generic_quarterly_rows(input_rows, run_id)
    rows_skipped = len(input_rows) - len(mapped_rows)
    rows_written = 0
    if not dry_run:
        with sqlite3.connect(str(db_path)) as conn:
            if replace_symbol:
                replace_symbol_rows(conn, normalized_symbol)
            if write_vintage:
                yahoo_rows_by_key = yahoo_quarterly_rows_by_generic_key(input_rows)
                _require_yahoo_source_rows_for_non_null_fields(
                    normalized_rows=mapped_rows,
                    yahoo_quarterly_rows_by_key=yahoo_rows_by_key,
                )
                result = write_yahoo_quarterly_rows_with_optional_vintage(
                    conn,
                    normalized_rows=mapped_rows,
                    yahoo_quarterly_rows_by_key=yahoo_rows_by_key,
                    write_vintage=True,
                    market=str(vintage_market),
                    available_at_utc=str(vintage_available_at_utc),
                    ingested_at_utc=str(vintage_ingested_at_utc),
                    run_id=str(vintage_run_id),
                    mode="yahoo_to_generic_bridge",
                    normalization_run_id=vintage_normalization_run_id,
                )
                rows_written = result["latest_rows_written"]
            else:
                rows_written = insert_quarterly_rows(conn, mapped_rows)
            conn.commit()
    return {
        "market": market,
        "symbol": normalized_symbol,
        "source": "yahoo",
        "input_rows": len(input_rows),
        "rows_written": rows_written,
        "rows_skipped": rows_skipped,
        "dry_run": "true" if dry_run else "false",
        "replace_symbol": "true" if replace_symbol else "false",
        "run_id": run_id,
    }


def _validate_vintage_args(
    *,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
) -> None:
    required_values = {
        "vintage_market": vintage_market,
        "vintage_available_at_utc": vintage_available_at_utc,
        "vintage_ingested_at_utc": vintage_ingested_at_utc,
        "vintage_run_id": vintage_run_id,
    }
    missing = [name for name, value in required_values.items() if value is None or not str(value).strip()]
    if missing:
        raise ValueError("YAHOO_TO_QUARTERLY_CLI_VINTAGE_REQUIRED_FIELDS_MISSING:" + ",".join(missing))


def _require_yahoo_source_rows_for_non_null_fields(
    *,
    normalized_rows: list[dict[str, Any]],
    yahoo_quarterly_rows_by_key: dict[tuple[str, str], dict[str, Any]],
) -> None:
    for row in normalized_rows:
        ticker = str(row["ticker"]).upper()
        period_end_date = str(row["period_end_date"])
        yahoo_row = yahoo_quarterly_rows_by_key.get((ticker, period_end_date))
        if yahoo_row is None:
            raise ValueError(f"YAHOO_TO_QUARTERLY_CLI_VINTAGE_SOURCE_ROW_MISSING:{ticker},{period_end_date}")
        for field_name in REPORTED_FINANCIAL_FIELDS:
            source_field_name = "operating_income" if field_name == "ebit" else field_name
            if row.get(field_name) is not None and source_field_name not in yahoo_row:
                raise ValueError(
                    "YAHOO_TO_QUARTERLY_CLI_VINTAGE_SOURCE_FIELD_MISSING:"
                    f"{ticker},{period_end_date},{field_name}"
                )


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    write_vintage = bool(getattr(args, "write_vintage", False))
    summary = run_yahoo_to_quarterly(
        db_path=db_path,
        market=args.market,
        symbol=args.symbol,
        run_id=args.run_id,
        dry_run=args.dry_run,
        replace_symbol=args.replace_symbol,
        write_vintage=write_vintage,
        vintage_market=getattr(args, "vintage_market", None),
        vintage_available_at_utc=getattr(args, "vintage_available_at_utc", None),
        vintage_ingested_at_utc=getattr(args, "vintage_ingested_at_utc", None),
        vintage_run_id=getattr(args, "vintage_run_id", None),
        vintage_normalization_run_id=getattr(args, "vintage_normalization_run_id", None),
    )
    _summary(market=summary["market"])
    _summary(symbol=summary["symbol"])
    _summary(source=summary["source"])
    _summary(input_rows=summary["input_rows"])
    _summary(rows_written=summary["rows_written"])
    _summary(rows_skipped=summary["rows_skipped"])
    _summary(dry_run=summary["dry_run"])
    _summary(replace_symbol=summary["replace_symbol"])
    _summary(run_id=summary["run_id"])
    if write_vintage:
        _summary(vintage_write="enabled")


if __name__ == "__main__":
    main()
