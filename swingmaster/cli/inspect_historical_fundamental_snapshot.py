from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.historical_snapshot import (
    DEFAULT_PRICE_DB,
    asdict_snapshot,
    build_historical_fundamental_snapshot,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect read-only effective-date-safe historical fundamental snapshot")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--price-db", default=str(DEFAULT_PRICE_DB))
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--market", default="usa")
    parser.add_argument("--include-percentiles", action="store_true")
    parser.add_argument("--include-valuation", action="store_true")
    parser.add_argument("--include-current-comparison", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    fundamentals_path = Path(args.fundamentals_db).expanduser().resolve()
    price_path = Path(args.price_db).expanduser().resolve()
    with sqlite3.connect(str(fundamentals_path)) as fundamentals_conn, sqlite3.connect(str(price_path)) as price_conn:
        snapshot = build_historical_fundamental_snapshot(
            fundamentals_conn,
            price_conn,
            ticker=args.ticker,
            as_of_date=args.as_of_date,
            market=args.market,
            include_percentiles=args.include_percentiles,
            include_valuation=args.include_valuation,
            include_current_comparison=args.include_current_comparison,
        )
    payload = asdict_snapshot(snapshot)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"ticker: {snapshot.ticker}")
        print(f"requested_as_of_date: {snapshot.requested_as_of_date}")
        print(f"snapshot_status: {snapshot.snapshot_status}")
        print(f"source_ttm_as_of_date: {snapshot.source_ttm_as_of_date}")
        print(f"source_ttm_effective_trading_date: {snapshot.source_ttm_effective_trading_date}")
        print(f"source_score_effective_trading_date: {snapshot.source_score_effective_trading_date}")
        print(f"selected_price_date: {snapshot.selected_price_date}")
        print(f"score_percentile_population_size: {snapshot.score_percentile_population_size}")
        print(f"valuation_status: {snapshot.valuation_status}")
        print(f"missing_components: {','.join(snapshot.missing_components)}")
        print(f"warnings: {','.join(snapshot.warnings)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
