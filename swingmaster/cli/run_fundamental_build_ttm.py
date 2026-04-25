from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.build_ttm import build_and_insert_ttm_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TTM fundamentals rows from normalized quarterly rows")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Build and validate only without writing rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    with sqlite3.connect(str(db_path)) as conn:
        quarterly_rows, ttm_rows_written, first_as_of_date, last_as_of_date = build_and_insert_ttm_rows(
            conn=conn,
            ticker=args.ticker,
            run_id=args.run_id,
            dry_run=args.dry_run,
        )
    _summary(ticker=args.ticker)
    _summary(quarterly_rows=quarterly_rows)
    _summary(ttm_rows_written=ttm_rows_written)
    _summary(first_as_of_date=first_as_of_date if first_as_of_date is not None else "NULL")
    _summary(last_as_of_date=last_as_of_date if last_as_of_date is not None else "NULL")
    _summary(db_path=str(db_path))
    _summary(run_id=args.run_id)
    _summary(status="dry-run" if args.dry_run else "ok")


if __name__ == "__main__":
    main()
