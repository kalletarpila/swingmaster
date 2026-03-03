from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from swingmaster.cli.daily_report import (
    MARKET_CONFIGS,
    WEEKLY_REPORTS_DIR,
    build_daily_report_rows,
    fetch_recent_trading_dates,
    write_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate weekly BUYS report for all markets")
    parser.add_argument("--date", required=True, help="Last included date (YYYY-MM-DD)")
    parser.add_argument("--out-dir", default=None, help="Output directory (default: weekly_reports)")
    return parser.parse_args()


def _report_markets() -> List[str]:
    return ["fin", "se", "usa"]


def main() -> None:
    args = parse_args()
    as_of_date = args.date
    out_dir = Path(args.out_dir or WEEKLY_REPORTS_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    combined_rows = []
    warned_fastpass_missing = False

    for market in _report_markets():
        config = MARKET_CONFIGS[market]
        trading_dates = fetch_recent_trading_dates(config.db_path, as_of_date, limit=7)
        for trading_date in trading_dates:
            try:
                final_rows, missing_fastpass_table = build_daily_report_rows(config.db_path, config, trading_date)
            except FileNotFoundError as exc:
                print(str(exc), file=sys.stderr)
                raise SystemExit(2)

            if missing_fastpass_table and not warned_fastpass_missing:
                print("WARNING FASTPASS_TABLE_MISSING")
                warned_fastpass_missing = True

            buy_rows = [
                row
                for row in final_rows
                if row.get("section") == "BUYS" and row.get("ticker") not in {None, "", "(none)"}
            ]
            combined_rows.extend(buy_rows)

    combined_rows.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("as_of_date") or ""),
            str(row.get("ticker") or ""),
            str(row.get("rule_hit") or ""),
        )
    )

    if not combined_rows:
        combined_rows = [
            {
                "section": "BUYS",
                "as_of_date": as_of_date,
                "market": None,
                "ticker": "(none)",
                "state_prev": None,
                "state_today": None,
                "from_state": None,
                "to_state": None,
                "event_date": None,
                "entry_window_date": None,
                "first_time_in_ew_ever": None,
                "days_in_stabilizing_before_ew": None,
                "days_in_current_episode": None,
                "days_in_ew_trading": None,
                "ew_score_fastpass": None,
                "ew_level_fastpass": None,
                "ew_score_rolling": None,
                "ew_level_rolling": None,
                "rule_hit": "EMPTY_SECTION",
                "buy_badges": None,
            }
        ]

    txt_out = out_dir / f"weekly_report_{as_of_date}.txt"
    csv_out = out_dir / f"weekly_report_{as_of_date}.csv"
    write_outputs(combined_rows, txt_out, csv_out)
    print(f"WEEKLY TXT: {txt_out}")
    print(f"WEEKLY CSV: {csv_out}")


if __name__ == "__main__":
    main()
