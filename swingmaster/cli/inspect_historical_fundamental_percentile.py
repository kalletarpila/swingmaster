from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.historical_percentile import (
    DEFAULT_METRIC,
    DEFAULT_OSAKEDATA_DB,
    asdict_result,
    calculate_ticker_historical_percentile_as_of,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect read-only effective-date-safe historical fundamental percentiles")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--osakedata-db", default=str(DEFAULT_OSAKEDATA_DB))
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--market", default="usa")
    parser.add_argument("--sector")
    parser.add_argument("--industry")
    parser.add_argument("--metric", default=DEFAULT_METRIC)
    parser.add_argument("--include-peers", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with sqlite3.connect(str(Path(args.fundamentals_db).expanduser().resolve())) as fundamentals_conn, sqlite3.connect(
        str(Path(args.osakedata_db).expanduser().resolve())
    ) as osakedata_conn:
        result = calculate_ticker_historical_percentile_as_of(
            fundamentals_conn,
            osakedata_conn,
            ticker=args.ticker,
            target_date=args.as_of_date,
            market=args.market,
            sector=args.sector,
            industry=args.industry,
            metric=args.metric,
            include_peers=args.include_peers,
        )
    payload = asdict_result(result)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"status: {result.status}")
        print(f"ticker: {result.ticker}")
        print(f"target_date: {result.target_date}")
        print(f"population_date_policy: {result.population_date_policy}")
        print(f"peer_population_size: {result.peer_population_size}")
        print(f"excluded_null_effective_score_count: {result.excluded_null_effective_score_count}")
        print(f"excluded_no_available_score_count: {result.excluded_no_available_score_count}")
        print(f"ticker_score_period: {result.ticker_score_period}")
        print(f"ticker_score_effective_trading_date: {result.ticker_score_effective_trading_date}")
        if result.percentile_row is not None:
            print(f"{result.metric}: {result.percentile_row.get(result.metric)}")
        if result.current_percentile_row is not None:
            print(f"current_{result.metric}: {result.current_percentile_row.get(result.metric)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
