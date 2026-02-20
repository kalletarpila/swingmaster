from __future__ import annotations

import argparse
import sqlite3

from swingmaster.ew_score.compute import compute_and_store_ew_scores


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute EW day3 score and store rows to rc_ew_score_daily")
    parser.add_argument("--date", required=True, help="As-of date (YYYY-MM-DD)")
    parser.add_argument("--db", required=True, help="RC SQLite database path")
    parser.add_argument("--osakedata", required=True, help="Osakedata SQLite database path")
    parser.add_argument("--rule", required=True, help="EW score model rule_id")
    parser.add_argument("--print", action="store_true", dest="print_rows", help="Print scored rows")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    rc_conn = sqlite3.connect(args.db)
    os_conn = sqlite3.connect(args.osakedata)
    try:
        n = compute_and_store_ew_scores(
            rc_conn=rc_conn,
            osakedata_conn=os_conn,
            as_of_date=args.date,
            rule_id=args.rule,
            repo=None,
            print_rows=args.print_rows,
        )
    finally:
        os_conn.close()
        rc_conn.close()

    if not args.print_rows:
        print(f"stored_rows={n}")


if __name__ == "__main__":
    main()
