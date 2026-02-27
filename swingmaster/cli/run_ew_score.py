from __future__ import annotations

import argparse
import sqlite3
from datetime import date

from swingmaster.ew_score.compute import compute_and_store_ew_scores, compute_and_store_ew_scores_range
from swingmaster.ew_score.model_config import load_model_config
from swingmaster.ew_score.models.resolve_model import EwScoreRuleResolutionError, resolve_ew_score_rule


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute EW day3 score and store rows to rc_ew_score_daily")
    parser.add_argument("--date", default=None, help="As-of date (YYYY-MM-DD)")
    parser.add_argument("--date-from", default=None, help="Range start date (YYYY-MM-DD)")
    parser.add_argument("--date-to", default=None, help="Range end date (YYYY-MM-DD)")
    parser.add_argument("--db", required=True, help="RC SQLite database path")
    parser.add_argument("--osakedata", required=True, help="Osakedata SQLite database path")
    parser.add_argument("--rule", required=True, help="EW score model rule_id")
    parser.add_argument("--print", action="store_true", dest="print_rows", help="Print scored rows")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    single_mode = args.date is not None
    range_mode = args.date_from is not None or args.date_to is not None
    if single_mode and range_mode:
        raise SystemExit("ERROR: use either --date OR --date-from/--date-to")
    if not single_mode and not range_mode:
        raise SystemExit("ERROR: provide --date OR --date-from and --date-to")
    if range_mode and (args.date_from is None or args.date_to is None):
        raise SystemExit("ERROR: range mode requires both --date-from and --date-to")

    try:
        resolved_rule_id, resolved_rule_path = resolve_ew_score_rule(args.rule)
        load_model_config(resolved_rule_id)
    except (EwScoreRuleResolutionError, ValueError):
        print("SUMMARY status=ERROR message=EW_SCORE_RULE_NOT_FOUND")
        raise SystemExit(2)

    print(f"SUMMARY resolved_rule_id={resolved_rule_id}")
    print(f"SUMMARY resolved_rule_path={resolved_rule_path.resolve()}")

    rc_conn = sqlite3.connect(args.db)
    os_conn = sqlite3.connect(args.osakedata)
    try:
        if single_mode:
            n = compute_and_store_ew_scores(
                rc_conn=rc_conn,
                osakedata_conn=os_conn,
                as_of_date=args.date,
                rule_id=resolved_rule_id,
                repo=None,
                print_rows=args.print_rows,
            )
            dates_processed = 1
        else:
            n = compute_and_store_ew_scores_range(
                rc_conn=rc_conn,
                osakedata_conn=os_conn,
                date_from=args.date_from,
                date_to=args.date_to,
                rule_id=resolved_rule_id,
                print_rows=args.print_rows,
            )
            d0 = date.fromisoformat(args.date_from)
            d1 = date.fromisoformat(args.date_to)
            dates_processed = (d1 - d0).days + 1
    finally:
        os_conn.close()
        rc_conn.close()

    if single_mode and not args.print_rows:
        print(f"stored_rows={n}")
    if range_mode:
        print(f"total_rows_written={n}")
        print(f"dates_processed={dates_processed}")


if __name__ == "__main__":
    main()
