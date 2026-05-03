from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.build_ttm import build_ttm_rows, insert_ttm_rows, load_quarterly_rows


DEFAULT_TICKER = "NOKIA.HE"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TTM rows from existing rc_fundamental_quarterly rows")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", default=DEFAULT_TICKER, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Build and validate only without writing rows")
    parser.add_argument(
        "--replace-ticker",
        action="store_true",
        help="Delete existing rc_fundamental_ttm rows for the selected ticker before insert",
    )
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def delete_ttm_rows_for_ticker(conn: sqlite3.Connection, ticker: str) -> int:
    cursor = conn.execute(
        """
        DELETE FROM rc_fundamental_ttm
        WHERE ticker = ?
        """,
        (ticker.upper(),),
    )
    return int(cursor.rowcount)


def run_quarterly_to_ttm(
    db_path: Path,
    ticker: str,
    run_id: str,
    dry_run: bool,
    replace_ticker: bool,
) -> dict[str, object]:
    normalized_ticker = ticker.upper()
    with sqlite3.connect(str(db_path)) as conn:
        quarterly_rows = load_quarterly_rows(conn, normalized_ticker)
        ttm_rows = build_ttm_rows(quarterly_rows, run_id)
        rows_written = 0
        if not dry_run:
            if replace_ticker:
                delete_ttm_rows_for_ticker(conn, normalized_ticker)
            rows_written = insert_ttm_rows(conn, ttm_rows)
            conn.commit()
    return {
        "ticker": normalized_ticker,
        "input_quarterly_rows": len(quarterly_rows),
        "ttm_rows_built": len(ttm_rows),
        "rows_written": rows_written,
        "dry_run": "true" if dry_run else "false",
        "run_id": run_id,
    }


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    summary = run_quarterly_to_ttm(
        db_path=db_path,
        ticker=args.ticker,
        run_id=args.run_id,
        dry_run=args.dry_run,
        replace_ticker=args.replace_ticker,
    )
    _summary(ticker=summary["ticker"])
    _summary(input_quarterly_rows=summary["input_quarterly_rows"])
    _summary(ttm_rows_built=summary["ttm_rows_built"])
    _summary(rows_written=summary["rows_written"])
    _summary(dry_run=summary["dry_run"])
    _summary(run_id=summary["run_id"])


if __name__ == "__main__":
    main()
