"""List tickers for a given market/sector universe.

Purpose:
  - Resolve and print tickers based on universe filters.
Inputs:
  - CLI args (market, sector, industry, tickers list).
Outputs:
  - Printed ticker list and counts to stdout.
Example:
  - PYTHONPATH=. python3 swingmaster/cli/list_universe.py --market OMXH
"""

from __future__ import annotations

import argparse

from swingmaster.app_api.dto import UniverseSpec, UniverseMode, UniverseSample
from swingmaster.infra.sqlite.db import get_connection
from swingmaster.infra.sqlite.repos.ticker_universe_reader import TickerUniverseReader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List resolved universe tickers")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--mode", required=True, choices=[
        "tickers",
        "market",
        "market_sector",
        "market_sector_industry",
    ])
    parser.add_argument("--tickers", help="Comma-separated tickers for mode=tickers")
    parser.add_argument("--market", help="Market code")
    parser.add_argument("--sector", help="Sector name")
    parser.add_argument("--industry", help="Industry name")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--sample", choices=["first_n", "random"], default="first_n")
    parser.add_argument("--seed", type=int, default=1)
    return parser.parse_args()


def build_spec(args: argparse.Namespace) -> UniverseSpec:
    tickers_list = None
    if args.tickers:
        tickers_list = [t.strip() for t in args.tickers.split(",") if t.strip()]

    mode: UniverseMode = args.mode
    sample: UniverseSample = args.sample

    spec = UniverseSpec(
        mode=mode,
        tickers=tickers_list,
        market=args.market,
        sector=args.sector,
        industry=args.industry,
        limit=args.limit,
        sample=sample,
        seed=args.seed,
    )
    spec.validate()
    return spec


def main() -> None:
    args = parse_args()
    conn = get_connection(args.db)
    try:
        reader = TickerUniverseReader(conn)
        spec = build_spec(args)
        tickers = reader.resolve_tickers(spec)
        for t in tickers:
            print(t)
        print(f"COUNT={len(tickers)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
