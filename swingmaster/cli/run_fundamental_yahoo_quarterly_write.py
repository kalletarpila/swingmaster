from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_yahoo_quarterly_prototype import (
    DEFAULT_SYMBOL,
    build_normalized_rows,
    load_latest_yahoo_raw_row,
    should_persist_row,
)


DEFAULT_MARKET = "omxh"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write normalized Yahoo quarterly rows for NOKIA.HE")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", default=DEFAULT_MARKET, help="Market code")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Normalize and validate only without writing rows")
    parser.add_argument(
        "--replace-symbol",
        action="store_true",
        help="Delete existing rows for market+symbol before inserting new rows",
    )
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def build_persist_rows(
    market: str,
    symbol: str,
    source_run_id: str,
    run_id: str,
    created_at_utc: str,
    normalized_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in normalized_rows:
        if not should_persist_row(row):
            continue
        rows.append(
            {
                "market": market,
                "symbol": symbol,
                "period_end_date": row["period_end_date"],
                "revenue": row["revenue"],
                "gross_profit": row["gross_profit"],
                "operating_income": row["operating_income"],
                "net_income": row["net_income"],
                "operating_cashflow": row["operating_cashflow"],
                "capex": row["capex"],
                "free_cashflow": row["free_cashflow"],
                "cash": row["cash"],
                "total_debt": row["total_debt"],
                "shares_outstanding": row["shares_outstanding"],
                "shares_source": row["shares_source"],
                "shares_quality": row["shares_quality"],
                "source_run_id": source_run_id,
                "run_id": run_id,
                "created_at_utc": created_at_utc,
            }
        )
    return rows


def replace_symbol_rows(conn: sqlite3.Connection, market: str, symbol: str) -> int:
    cursor = conn.execute(
        """
        DELETE FROM rc_fundamental_yahoo_quarterly
        WHERE market = ? AND symbol = ?
        """,
        (market, symbol),
    )
    return int(cursor.rowcount)


def insert_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    conn.executemany(
        """
        INSERT INTO rc_fundamental_yahoo_quarterly (
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["market"],
                row["symbol"],
                row["period_end_date"],
                row["revenue"],
                row["gross_profit"],
                row["operating_income"],
                row["net_income"],
                row["operating_cashflow"],
                row["capex"],
                row["free_cashflow"],
                row["cash"],
                row["total_debt"],
                row["shares_outstanding"],
                row["shares_source"],
                row["shares_quality"],
                row["source_run_id"],
                row["run_id"],
                row["created_at_utc"],
            )
            for row in rows
        ],
    )
    return len(rows)


def run_yahoo_quarterly_write(
    db_path: Path,
    market: str,
    symbol: str,
    run_id: str,
    dry_run: bool,
    replace_symbol: bool,
) -> dict[str, Any]:
    normalized_symbol = symbol.upper()
    created_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with sqlite3.connect(str(db_path)) as conn:
        raw_row = load_latest_yahoo_raw_row(conn, normalized_symbol)
    normalized_rows = build_normalized_rows(raw_row)
    persist_rows = build_persist_rows(
        market=market,
        symbol=normalized_symbol,
        source_run_id=str(raw_row["run_id"]),
        run_id=run_id,
        created_at_utc=created_at_utc,
        normalized_rows=normalized_rows,
    )

    rows_skipped = len(normalized_rows) - len(persist_rows)
    rows_deleted = 0
    rows_written = 0
    if not dry_run:
        with sqlite3.connect(str(db_path)) as conn:
            if replace_symbol:
                rows_deleted = replace_symbol_rows(conn, market, normalized_symbol)
            rows_written = insert_rows(conn, persist_rows)
            conn.commit()

    return {
        "market": market,
        "symbol": normalized_symbol,
        "source_run_id": str(raw_row["run_id"]),
        "periods_total": len(normalized_rows),
        "rows_normalized": len(normalized_rows),
        "rows_skipped": rows_skipped,
        "rows_deleted": rows_deleted,
        "rows_written": rows_written,
        "dry_run": "true" if dry_run else "false",
        "replace_symbol": "true" if replace_symbol else "false",
        "run_id": run_id,
        "rows": persist_rows,
    }


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    result = run_yahoo_quarterly_write(
        db_path=db_path,
        market=args.market,
        symbol=args.symbol,
        run_id=args.run_id,
        dry_run=args.dry_run,
        replace_symbol=args.replace_symbol,
    )
    _summary(market=result["market"])
    _summary(symbol=result["symbol"])
    _summary(source_run_id=result["source_run_id"])
    _summary(periods_total=result["periods_total"])
    _summary(rows_normalized=result["rows_normalized"])
    _summary(rows_skipped=result["rows_skipped"])
    _summary(rows_deleted=result["rows_deleted"])
    _summary(rows_written=result["rows_written"])
    _summary(dry_run=result["dry_run"])
    _summary(replace_symbol=result["replace_symbol"])
    _summary(run_id=result["run_id"])


if __name__ == "__main__":
    main()
