from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.score_percentile import (
    FUND_SCORE_PERCENTILE_V2_PRE,
    resolve_created_at_utc,
    run_fundamental_score_percentile,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute cross-sectional percentile fundamental scores")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--osakedata-db", required=True, help="Osakedata SQLite database path")
    parser.add_argument("--as-of-date", required=True, help="Target date in YYYY-MM-DD format")
    parser.add_argument("--rule-id", default=FUND_SCORE_PERCENTILE_V2_PRE, help="Percentile rule identifier")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--market", default="usa", help="Ticker metadata market filter")
    parser.add_argument("--created-at-utc", default=None, help="Explicit created_at_utc timestamp")
    parser.add_argument("--dry-run", action="store_true", help="Compute without writing rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    osakedata_db_path = resolve_db_path(args.osakedata_db)
    created_at_utc = resolve_created_at_utc(args.created_at_utc)

    with sqlite3.connect(str(db_path)) as fundamentals_conn, sqlite3.connect(str(osakedata_db_path)) as osakedata_conn:
        summary = run_fundamental_score_percentile(
            fundamentals_conn=fundamentals_conn,
            osakedata_conn=osakedata_conn,
            target_date=args.as_of_date,
            rule_id=args.rule_id,
            run_id=args.run_id,
            market=args.market,
            created_at_utc=created_at_utc,
            dry_run=args.dry_run,
        )

    _summary(db_path=str(db_path))
    _summary(osakedata_db_path=str(osakedata_db_path))
    _summary(target_date=args.as_of_date)
    _summary(rule_id=args.rule_id)
    _summary(run_id=args.run_id)
    _summary(market=args.market)
    _summary(universe_size=summary["universe_size"])
    _summary(rows_computed=summary["rows_computed"])
    _summary(rows_written=summary["rows_written"])
    _summary(sector_count=summary["sector_count"])
    _summary(industry_count=summary["industry_count"])
    _summary(dry_run=str(args.dry_run).lower())
    _summary(status="ok")


if __name__ == "__main__":
    main()
