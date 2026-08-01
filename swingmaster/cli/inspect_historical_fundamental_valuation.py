from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.historical_valuation import (
    DEFAULT_PRICE_DB,
    asdict_result,
    calculate_historical_valuation_as_of,
    calculate_historical_valuation_with_percentiles_as_of,
)
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect read-only effective-date-safe historical fundamental valuation")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--price-db", default=str(DEFAULT_PRICE_DB))
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--market", default="usa")
    parser.add_argument("--include-current-comparison", action="store_true")
    parser.add_argument("--include-historical-percentiles", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    fundamentals_path = Path(args.fundamentals_db).expanduser().resolve()
    price_path = Path(args.price_db).expanduser().resolve()
    with sqlite3.connect(str(fundamentals_path)) as fundamentals_conn, sqlite3.connect(str(price_path)) as price_conn:
        if args.include_historical_percentiles:
            result = calculate_historical_valuation_with_percentiles_as_of(
                fundamentals_conn,
                price_conn,
                ticker=args.ticker,
                as_of_date=args.as_of_date,
                market=args.market,
                percentile_conn=price_conn,
                include_current_comparison=args.include_current_comparison,
            )
        else:
            result = calculate_historical_valuation_as_of(
                fundamentals_conn,
                price_conn,
                ticker=args.ticker,
                as_of_date=args.as_of_date,
                market=args.market,
                include_current_comparison=args.include_current_comparison,
            )
    payload = asdict_result(result)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"ticker: {result.ticker}")
        print(f"requested_as_of_date: {result.requested_as_of_date}")
        print(f"selected_price_date: {result.selected_price_date}")
        print(f"price_selection_status: {result.price_selection_status}")
        print(f"source_ttm_as_of_date: {result.source_ttm_as_of_date}")
        print(f"source_ttm_effective_trading_date: {result.source_ttm_effective_trading_date}")
        print(f"valuation_status: {result.valuation_status}")
        if result.valuation_row is not None:
            print(f"valuation_ev_ebit: {result.valuation_row.get('valuation_ev_ebit')}")
            print(f"valuation_fcf_yield: {result.valuation_row.get('valuation_fcf_yield')}")
            print(f"valuation_bucket: {result.valuation_row.get('valuation_bucket')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
