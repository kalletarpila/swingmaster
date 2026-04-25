from __future__ import annotations

import argparse

from swingmaster.fundamentals.fetch_raw_statements import _get_yfinance_module, inspect_quarterly_statement_candidates


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect yfinance quarterly statement access paths")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def main() -> None:
    args = parse_args()
    yf = _get_yfinance_module()
    yf_ticker = yf.Ticker(args.ticker)
    candidates = inspect_quarterly_statement_candidates(yf_ticker)

    for statement_type in ("income", "balance", "cashflow"):
        for candidate in candidates[statement_type]:
            print(f"path={candidate['path_name']}")
            print(f"shape={candidate['shape']}")
            print(f"empty={str(candidate['empty']).lower()}")
            print(f"period_count={candidate['period_count']}")
            print(f"first_period={candidate['first_period'] if candidate['first_period'] is not None else 'NULL'}")
            print(f"last_period={candidate['last_period'] if candidate['last_period'] is not None else 'NULL'}")
            print(f"index_head_10={candidate['index_head_10']}")

    _summary(ticker=args.ticker)
    _summary(max_income_periods=max(candidate["period_count"] for candidate in candidates["income"]))
    _summary(max_balance_periods=max(candidate["period_count"] for candidate in candidates["balance"]))
    _summary(max_cashflow_periods=max(candidate["period_count"] for candidate in candidates["cashflow"]))
    _summary(status="ok")


if __name__ == "__main__":
    main()
