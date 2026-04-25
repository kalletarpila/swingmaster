from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.sec_reconstruct_quarterly import (
    insert_reconstructed_quarterly_rows,
    load_sec_fact_rows,
    reconstruct_quarterly_rows,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct SEC quarterly raw rows from stored sec_fact rows")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--retrieved-at-utc", required=True, help="Deterministic retrieved timestamp")
    parser.add_argument("--dry-run", action="store_true", help="Reconstruct without writing rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def run_sec_reconstruct_quarterly(
    db_path: Path,
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
    dry_run: bool,
) -> tuple[int, list[dict]]:
    normalized_ticker = ticker.upper()
    with sqlite3.connect(str(db_path)) as conn:
        sec_fact_rows = load_sec_fact_rows(conn, normalized_ticker)
        reconstructed_rows = reconstruct_quarterly_rows(sec_fact_rows, normalized_ticker, run_id, retrieved_at_utc)
        if not dry_run:
            insert_reconstructed_quarterly_rows(conn, reconstructed_rows)
            conn.commit()
    return len(sec_fact_rows), reconstructed_rows


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    sec_fact_rows_read, reconstructed_rows = run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker=args.ticker,
        run_id=args.run_id,
        retrieved_at_utc=args.retrieved_at_utc,
        dry_run=args.dry_run,
    )
    periods = sorted({row["period_end_date"] for row in reconstructed_rows})
    _summary(ticker=args.ticker.upper())
    _summary(sec_fact_rows_read=sec_fact_rows_read)
    _summary(quarterly_rows_reconstructed=len(reconstructed_rows))
    _summary(output_periods=len(periods))
    _summary(income_rows=sum(1 for row in reconstructed_rows if row["statement_type"] == "income"))
    _summary(cashflow_rows=sum(1 for row in reconstructed_rows if row["statement_type"] == "cashflow"))
    _summary(balance_rows=sum(1 for row in reconstructed_rows if row["statement_type"] == "balance"))
    _summary(first_period=periods[0] if periods else "NULL")
    _summary(last_period=periods[-1] if periods else "NULL")
    _summary(db_path=str(db_path))
    _summary(run_id=args.run_id)
    _summary(source="sec_edgar")
    _summary(period_type="quarterly")
    _summary(status="dry-run" if args.dry_run else "ok")


if __name__ == "__main__":
    main()
