from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.fetch_raw_statements import (
    SUPPORTED_STATEMENT_TYPES,
    count_statement_rows,
    fetch_quarterly_statements_raw,
    insert_raw_statement_rows,
    validate_non_empty_statements,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap raw quarterly fundamentals into SQLite")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate only without writing rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def run_bootstrap_raw(db_path: Path, ticker: str, run_id: str, dry_run: bool) -> tuple[int, int]:
    statements = fetch_quarterly_statements_raw(ticker)
    validate_non_empty_statements(statements)
    statements_loaded = len(SUPPORTED_STATEMENT_TYPES)
    rows_written = sum(count_statement_rows(statements[statement_type]) for statement_type in SUPPORTED_STATEMENT_TYPES)

    if dry_run:
        return statements_loaded, rows_written

    with sqlite3.connect(str(db_path)) as conn:
        total_rows_written = 0
        for statement_type in SUPPORTED_STATEMENT_TYPES:
            total_rows_written += insert_raw_statement_rows(
                conn=conn,
                ticker=ticker,
                statement_type=statement_type,
                dataframe=statements[statement_type],
                run_id=run_id,
            )
        conn.commit()
    return statements_loaded, total_rows_written


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    statements_loaded, rows_written = run_bootstrap_raw(
        db_path=db_path,
        ticker=args.ticker,
        run_id=args.run_id,
        dry_run=args.dry_run,
    )
    _summary(ticker=args.ticker)
    _summary(statements_loaded=statements_loaded)
    _summary(rows_written=rows_written)
    _summary(db_path=str(db_path))
    _summary(run_id=args.run_id)
    _summary(status="dry-run" if args.dry_run else "ok")


if __name__ == "__main__":
    main()
