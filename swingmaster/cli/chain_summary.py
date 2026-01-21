from __future__ import annotations

import argparse
import sqlite3
from collections import Counter
from typing import Dict, List

from swingmaster.infra.sqlite.db import get_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize state chains over a date range")
    parser.add_argument("--rc-db", default="swingmaster_rc.db", help="RC database path")
    parser.add_argument("--date-from", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--date-to", required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--limit", type=int, default=50, help="Top list limit")
    parser.add_argument("--run-id", help="Specific run_id to anchor universe")
    parser.add_argument("--anchor-run-id", help="Alias for run-id when anchoring universe")
    parser.add_argument("--latest-run-on-date", help="Resolve latest run_id for given date (anchor only)")
    parser.add_argument("--anchor-date", help="Date used to anchor ticker universe when run-id is provided")
    return parser.parse_args()


def _chunked(iterable, size: int):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def load_state_days(
    conn: sqlite3.Connection, date_from: str, date_to: str, tickers: List[str] | None
):
    if tickers:
        rows: List[sqlite3.Row] = []
        for chunk in _chunked(tickers, 500):
            placeholders = ",".join("?" * len(chunk))
            params = [date_from, date_to, *chunk]
            chunk_rows = conn.execute(
                f"""
                SELECT ticker, date, state, age
                FROM rc_state_daily
                WHERE date >= ? AND date <= ? AND ticker IN ({placeholders})
                ORDER BY ticker, date
                """,
                params,
            ).fetchall()
            rows.extend(chunk_rows)
        return rows
    rows = conn.execute(
        """
        SELECT ticker, date, state, age
        FROM rc_state_daily
        WHERE date >= ? AND date <= ?
        ORDER BY ticker, date
        """,
        (date_from, date_to),
    ).fetchall()
    return rows


def load_transition_counts(
    conn: sqlite3.Connection, date_from: str, date_to: str, tickers: List[str] | None
) -> Dict[str, int]:
    if tickers:
        counts: Dict[str, int] = {}
        for chunk in _chunked(tickers, 500):
            placeholders = ",".join("?" * len(chunk))
            params = [date_from, date_to, *chunk]
            rows = conn.execute(
                f"""
                SELECT ticker, COUNT(*) as c
                FROM rc_transition
                WHERE date >= ? AND date <= ? AND ticker IN ({placeholders})
                GROUP BY ticker
                """,
                params,
            ).fetchall()
            counts.update({row[0]: row[1] for row in rows})
        return counts
    rows = conn.execute(
        """
        SELECT ticker, COUNT(*) as c
        FROM rc_transition
        WHERE date >= ? AND date <= ?
        GROUP BY ticker
        """,
        (date_from, date_to),
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def load_transitions_total(
    conn: sqlite3.Connection, date_from: str, date_to: str, tickers: List[str] | None
) -> int:
    if tickers:
        total = 0
        for chunk in _chunked(tickers, 500):
            placeholders = ",".join("?" * len(chunk))
            params = [date_from, date_to, *chunk]
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM rc_transition
                WHERE date >= ? AND date <= ? AND ticker IN ({placeholders})
                """,
                params,
            ).fetchone()
            total += int(row[0]) if row else 0
        return total
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_transition
        WHERE date >= ? AND date <= ?
        """,
        (date_from, date_to),
    ).fetchone()
    return int(row[0]) if row else 0


def load_expected_days(conn: sqlite3.Connection, date_from: str, date_to: str, tickers: List[str] | None) -> int:
    if tickers:
        total_dates = set()
        for chunk in _chunked(tickers, 500):
            placeholders = ",".join("?" * len(chunk))
            params = [date_from, date_to, *chunk]
            rows = conn.execute(
                f"""
                SELECT DISTINCT date
                FROM rc_state_daily
                WHERE date >= ? AND date <= ? AND ticker IN ({placeholders})
                """,
                params,
            ).fetchall()
            total_dates.update(row[0] for row in rows)
        return len(total_dates)
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT date)
        FROM rc_state_daily
        WHERE date >= ? AND date <= ?
        """,
        (date_from, date_to),
    ).fetchone()
    return int(row[0]) if row else 0


def summarize(rows, transition_counts: Dict[str, int]):
    per_ticker: Dict[str, Dict[str, object]] = {}
    overall_state_days = Counter()
    total_ticker_days = 0

    current_ticker = None
    ticker_rows: List[sqlite3.Row] = []

    def flush_ticker(trs: List[sqlite3.Row]):
        nonlocal total_ticker_days
        if not trs:
            return
        tkr = trs[0]["ticker"]
        days = len(trs)
        total_ticker_days += days
        first_state = trs[0]["state"]
        last_state = trs[-1]["state"]
        max_age = max(r["age"] for r in trs)
        last_age = trs[-1]["age"]
        state_days = Counter(r["state"] for r in trs)
        overall_state_days.update(state_days)
        per_ticker[tkr] = {
            "days": days,
            "first_state": first_state,
            "last_state": last_state,
            "max_age": max_age,
            "last_age": last_age,
            "state_days": state_days,
            "transitions": transition_counts.get(tkr, 0),
        }

    for row in rows:
        tkr = row["ticker"]
        if current_ticker is None:
            current_ticker = tkr
        if tkr != current_ticker:
            flush_ticker(ticker_rows)
            ticker_rows = []
            current_ticker = tkr
        ticker_rows.append(row)
    flush_ticker(ticker_rows)

    return per_ticker, overall_state_days, total_ticker_days


def format_pct(count: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{(count / total) * 100:.1f}%"


def print_report(
    per_ticker,
    overall_state_days,
    total_ticker_days: int,
    transitions_total: int,
    expected_days: int,
    limit: int,
):
    tickers_count = len(per_ticker)

    print("OVERALL STATE DAY DISTRIBUTION:")
    for state, count in sorted(overall_state_days.items(), key=lambda x: (-x[1], x[0])):
        print(f"  STATE {state}: {count} ({format_pct(count, total_ticker_days)})")

    print("TOP ACTIVE (by transitions):")
    top_active = [
        item for item in per_ticker.items() if item[1]["transitions"] > 0
    ]
    top_active = sorted(top_active, key=lambda x: (-x[1]["transitions"], x[0]))[:limit]
    if top_active:
        for tkr, data in top_active:
            print(
                f"  {tkr} transitions={data['transitions']} days={data['days']} "
                f"first={data['first_state']} last={data['last_state']} max_age={data['max_age']} last_age={data['last_age']}"
            )
    else:
        print("  (none)")

    print("TOP STAGNANT (by max_age):")
    top_stagnant = sorted(
        per_ticker.items(), key=lambda x: (-x[1]["max_age"], x[0])
    )[:limit]
    for tkr, data in top_stagnant:
        print(
            f"  {tkr} max_age={data['max_age']} last_age={data['last_age']} days={data['days']} "
            f"last={data['last_state']} transitions={data['transitions']}"
        )


def main() -> None:
    args = parse_args()
    conn = get_connection(args.rc_db)
    try:
        anchor_run_id = args.run_id or args.anchor_run_id
        anchor_date = args.latest_run_on_date or args.anchor_date
        if anchor_run_id and args.latest_run_on_date:
            raise ValueError("Use either --run-id/--anchor-run-id or --latest-run-on-date, not both")
        if anchor_run_id and anchor_date is None:
            raise ValueError("--anchor-date is required when providing --run-id/--anchor-run-id")
        if anchor_run_id is None and args.latest_run_on_date:
            row = conn.execute(
                """
                SELECT r.run_id
                FROM rc_run r
                JOIN rc_state_daily s ON s.run_id = r.run_id
                WHERE s.date=?
                ORDER BY r.created_at DESC, r.run_id DESC
                LIMIT 1
                """,
                (args.latest_run_on_date,),
            ).fetchone()
            anchor_run_id = row[0] if row else None
            anchor_date = args.latest_run_on_date

        anchor_tickers: List[str] | None = None
        if anchor_run_id:
            if anchor_date is None:
                raise ValueError("--anchor-date or --latest-run-on-date must be provided when anchoring run")
            rows = conn.execute(
                """
                SELECT DISTINCT ticker
                FROM rc_state_daily
                WHERE date=? AND run_id=?
                ORDER BY ticker
                """,
                (anchor_date, anchor_run_id),
            ).fetchall()
            anchor_tickers = [row[0] for row in rows]
            if not anchor_tickers:
                print(f"RUN_FILTER anchor_date={anchor_date} run_id={anchor_run_id} tickers=0 (no data)")
                return

        rows = load_state_days(conn, args.date_from, args.date_to, anchor_tickers)
        transition_counts = load_transition_counts(conn, args.date_from, args.date_to, anchor_tickers)
        transitions_total = load_transitions_total(conn, args.date_from, args.date_to, anchor_tickers)
        expected_days = load_expected_days(conn, args.date_from, args.date_to, anchor_tickers)
        per_ticker, overall_state_days, total_ticker_days = summarize(rows, transition_counts)

        print(
            f"RC_DB={args.rc_db} RANGE={args.date_from}..{args.date_to} "
            f"TICKERS={len(per_ticker)} TICKER_DAYS={total_ticker_days} TRANSITIONS_TOTAL={transitions_total} "
            f"RUN_FILTER anchor_date={anchor_date if anchor_date else 'none'} run_id={anchor_run_id if anchor_run_id else 'none'} tickers={len(anchor_tickers) if anchor_tickers else 'all'}"
        )
        full_coverage = sum(1 for data in per_ticker.values() if expected_days > 0 and data["days"] == expected_days)
        pct_full = format_pct(full_coverage, len(per_ticker)) if expected_days > 0 else "0.0%"
        print(
            f"EXPECTED_TRADING_DAYS={expected_days} FULL_COVERAGE_TICKERS={full_coverage} ({pct_full})"
        )
        print_report(
            per_ticker,
            overall_state_days,
            total_ticker_days,
            transitions_total,
            expected_days,
            args.limit,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
