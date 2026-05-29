from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.cli.run_fundamental_quarterly_to_ttm import run_quarterly_to_ttm
from swingmaster.fundamentals.reporting_frequency import classify_ticker_reporting_frequency, market_matches_ticker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic bulk TTM rebuild from rc_fundamental_quarterly")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--run-id", required=True, help="Deterministic base run identifier")
    parser.add_argument("--market", default=None, help="Optional market filter")
    parser.add_argument("--ticker", default=None, help="Optional single ticker override")
    parser.add_argument("--tickers", default=None, help="Optional comma-separated ticker list")
    parser.add_argument("--limit", type=int, default=None, help="Optional ticker limit after sorting")
    parser.add_argument(
        "--replace-ticker",
        action="store_true",
        help="Delete existing rc_fundamental_ttm rows for each selected ticker before insert",
    )
    parser.add_argument("--dry-run", action="store_true", help="Build and validate only without writing rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def derive_child_run_id(base_run_id: str) -> str:
    return f"{base_run_id}__TTM"


def normalize_tickers(tickers_arg: str | None) -> list[str]:
    if tickers_arg is None or not tickers_arg.strip():
        return []
    return sorted({ticker.strip().upper() for ticker in tickers_arg.split(",") if ticker.strip()})


def apply_market_filter(tickers: list[str], market: str | None) -> list[str]:
    if market is None:
        return tickers
    return [ticker for ticker in tickers if market_matches_ticker(market, ticker)]


def load_ticker_universe(db_path: Path) -> list[str]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ticker
            FROM rc_fundamental_quarterly
            ORDER BY ticker ASC
            """
        ).fetchall()
    return [str(row[0]).upper() for row in rows]


def resolve_ticker_universe(
    db_path: Path,
    market: str | None,
    ticker: str | None,
    tickers_arg: str | None,
    limit: int | None,
) -> list[str]:
    explicit_input = False
    if ticker is not None:
        resolved = [ticker.strip().upper()]
        explicit_input = True
    elif tickers_arg is not None:
        resolved = normalize_tickers(tickers_arg)
        explicit_input = True
    else:
        resolved = load_ticker_universe(db_path)

    if market is not None:
        invalid = [symbol for symbol in resolved if not market_matches_ticker(market, symbol)]
        if explicit_input and invalid:
            raise RuntimeError(f"FUNDAMENTAL_TTM_BATCH_MARKET_TICKER_MISMATCH:{','.join(invalid)}")
        resolved = apply_market_filter(resolved, market)

    resolved = sorted(resolved)
    if limit is not None:
        resolved = resolved[:limit]
    return resolved


def resolve_ttm_skip_reason(reporting_frequency_class: str) -> str | None:
    if reporting_frequency_class == "TRUE_SEMIANNUAL":
        return "SEMIANNUAL_TTM_NOT_IMPLEMENTED"
    if reporting_frequency_class == "QUARTERLY_MISSING_SOURCE_PERIOD":
        return "QUARTERLY_MISSING_SOURCE_PERIOD"
    if reporting_frequency_class in {"ANNUAL_ONLY", "OTHER_INSUFFICIENT", "UNKNOWN"}:
        return reporting_frequency_class
    return None


def run_fundamental_ttm_batch(
    db_path: Path,
    run_id: str,
    market: str | None,
    ticker: str | None,
    tickers_arg: str | None,
    limit: int | None,
    replace_ticker: bool,
    dry_run: bool,
) -> dict[str, object]:
    tickers = resolve_ticker_universe(
        db_path=db_path,
        market=market,
        ticker=ticker,
        tickers_arg=tickers_arg,
        limit=limit,
    )
    child_run_id = derive_child_run_id(run_id)
    tickers_processed = 0
    tickers_succeeded = 0
    tickers_skipped_insufficient_rows = 0
    reporting_frequency_quarterly_count = 0
    reporting_frequency_true_semiannual_skipped_count = 0
    reporting_frequency_quarterly_missing_source_period_skipped_count = 0
    reporting_frequency_other_skipped_count = 0
    rows_written = 0

    with sqlite3.connect(str(db_path)) as conn:
        for symbol in tickers:
            print(f"TICKER {symbol}")
            if market_matches_ticker("omxh", symbol):
                classification = classify_ticker_reporting_frequency(conn=conn, ticker=symbol)
                skip_reason = resolve_ttm_skip_reason(classification.reporting_frequency_class)
                if skip_reason is not None:
                    print(f"STEP ttm=SKIPPED_{skip_reason}")
                    tickers_processed += 1
                    if classification.reporting_frequency_class == "TRUE_SEMIANNUAL":
                        reporting_frequency_true_semiannual_skipped_count += 1
                    elif classification.reporting_frequency_class == "QUARTERLY_MISSING_SOURCE_PERIOD":
                        reporting_frequency_quarterly_missing_source_period_skipped_count += 1
                    else:
                        reporting_frequency_other_skipped_count += 1
                    continue
                reporting_frequency_quarterly_count += 1

            try:
                summary = run_quarterly_to_ttm(
                    db_path=db_path,
                    ticker=symbol,
                    run_id=child_run_id,
                    dry_run=dry_run,
                    replace_ticker=False if dry_run else replace_ticker,
                )
            except RuntimeError as exc:
                if str(exc) == f"FUNDAMENTAL_TTM_INSUFFICIENT_ROWS:{symbol}":
                    print("STEP ttm=SKIPPED_INSUFFICIENT_ROWS")
                    tickers_processed += 1
                    tickers_skipped_insufficient_rows += 1
                    continue
                print(f"TICKER {symbol}=FAILED")
                print(f"ERROR ticker={symbol} step=ttm")
                raise

            print("STEP ttm=OK")
            tickers_processed += 1
            tickers_succeeded += 1
            rows_written += int(summary["rows_written"])

    summary = {
        "tickers_total": len(tickers),
        "tickers_processed": tickers_processed,
        "tickers_succeeded": tickers_succeeded,
        "tickers_skipped_insufficient_rows": tickers_skipped_insufficient_rows,
        "reporting_frequency_quarterly_count": reporting_frequency_quarterly_count,
        "reporting_frequency_true_semiannual_skipped_count": reporting_frequency_true_semiannual_skipped_count,
        "reporting_frequency_quarterly_missing_source_period_skipped_count": (
            reporting_frequency_quarterly_missing_source_period_skipped_count
        ),
        "reporting_frequency_other_skipped_count": reporting_frequency_other_skipped_count,
        "rows_written": rows_written,
        "market": market.strip().lower() if market is not None else "ALL",
        "dry_run": 1 if dry_run else 0,
        "replace_ticker": 1 if replace_ticker else 0,
        "run_id": run_id,
    }
    _summary(**summary)
    return summary


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    run_fundamental_ttm_batch(
        db_path=db_path,
        run_id=args.run_id,
        market=args.market,
        ticker=args.ticker,
        tickers_arg=args.tickers,
        limit=args.limit,
        replace_ticker=args.replace_ticker,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
