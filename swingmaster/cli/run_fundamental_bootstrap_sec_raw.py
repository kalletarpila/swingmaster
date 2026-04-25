from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.sec_edgar import (
    SEC_STATEMENT_TYPE_BY_TAG,
    SEC_USER_AGENT,
    extract_companyfacts_raw_rows,
    fetch_companyfacts,
    resolve_cik,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap SEC EDGAR raw facts into fundamentals raw table")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--retrieved-at-utc", required=True, help="Deterministic retrieved timestamp")
    parser.add_argument("--user-agent", default=SEC_USER_AGENT, help="Optional SEC User-Agent header")
    parser.add_argument("--dry-run", action="store_true", help="Extract and validate only without writing rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def insert_sec_raw_rows(conn: sqlite3.Connection, rows: list[dict]) -> int:
    conn.executemany(
        """
        INSERT OR REPLACE INTO rc_fundamental_statement_raw (
            ticker,
            statement_type,
            period_end_date,
            period_type,
            field_name,
            field_value,
            currency,
            source,
            retrieved_at_utc,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["statement_type"],
                row["period_end_date"],
                row["period_type"],
                row["field_name"],
                row["field_value"],
                row["currency"],
                row["source"],
                row["retrieved_at_utc"],
                row["run_id"],
            )
            for row in rows
        ],
    )
    return len(rows)


def run_sec_raw_bootstrap(
    db_path: Path,
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
    user_agent: str,
    dry_run: bool,
) -> tuple[str, list[dict]]:
    normalized_ticker = ticker.upper()
    cik = resolve_cik(normalized_ticker, user_agent)
    companyfacts = fetch_companyfacts(cik, user_agent)
    rows = extract_companyfacts_raw_rows(normalized_ticker, companyfacts, run_id, retrieved_at_utc)
    if not rows:
        raise RuntimeError(f"SEC_FACTS_NOT_FOUND:{normalized_ticker}")

    if not dry_run:
        with sqlite3.connect(str(db_path)) as conn:
            insert_sec_raw_rows(conn, rows)
            conn.commit()

    return cik, rows


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    cik, rows = run_sec_raw_bootstrap(
        db_path=db_path,
        ticker=args.ticker,
        run_id=args.run_id,
        retrieved_at_utc=args.retrieved_at_utc,
        user_agent=args.user_agent,
        dry_run=args.dry_run,
    )
    statement_counts = {
        "income": sum(1 for row in rows if row["statement_type"] == "income"),
        "cashflow": sum(1 for row in rows if row["statement_type"] == "cashflow"),
        "balance": sum(1 for row in rows if row["statement_type"] == "balance"),
    }
    end_dates = sorted(row["period_end_date"] for row in rows)
    _summary(ticker=args.ticker.upper())
    _summary(cik=cik)
    _summary(facts_selected=len(rows))
    _summary(rows_written=len(rows))
    _summary(income_facts=statement_counts["income"])
    _summary(cashflow_facts=statement_counts["cashflow"])
    _summary(balance_facts=statement_counts["balance"])
    _summary(first_end_date=end_dates[0] if end_dates else "NULL")
    _summary(last_end_date=end_dates[-1] if end_dates else "NULL")
    _summary(db_path=str(db_path))
    _summary(run_id=args.run_id)
    _summary(source="sec_edgar")
    _summary(period_type="sec_fact")
    _summary(status="dry-run" if args.dry_run else "ok")


if __name__ == "__main__":
    main()
