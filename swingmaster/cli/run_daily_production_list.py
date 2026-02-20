from __future__ import annotations

import argparse
import sqlite3

from swingmaster.ew_score.daily_list import fetch_daily_production_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print daily production list with EW score columns")
    parser.add_argument("--date", required=True, help="As-of date (YYYY-MM-DD)")
    parser.add_argument("--db", required=True, help="RC SQLite database path")
    parser.add_argument("--state", default=None, help="Optional state filter (e.g. ENTRY_WINDOW)")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit")
    return parser.parse_args()


def _fmt(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def main() -> None:
    args = parse_args()

    conn = sqlite3.connect(args.db)
    try:
        rows = fetch_daily_production_rows(
            conn=conn,
            date=args.date,
            state=args.state,
            limit=args.limit,
        )
    finally:
        conn.close()

    print("DAILY_PRODUCTION_LIST")
    print("ticker | state | ew_level_day3 | ew_score_day3 | ew_rule")
    for row in rows:
        print(
            f"{_fmt(row.get('ticker'))} | "
            f"{_fmt(row.get('state'))} | "
            f"{_fmt(row.get('ew_level_day3'))} | "
            f"{_fmt(row.get('ew_score_day3'))} | "
            f"{_fmt(row.get('ew_rule'))}"
        )


if __name__ == "__main__":
    main()
