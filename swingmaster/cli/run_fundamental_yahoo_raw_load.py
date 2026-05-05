from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_yahoo_audit import run_yahoo_audit


DEFAULT_MARKET = "usa"
DEFAULT_EXCHANGE = "USA"
DEFAULT_BATCH_SIZE = 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic bulk Yahoo raw load for USA tickers")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", default=DEFAULT_MARKET, help="Market code")
    parser.add_argument("--run-id", required=True, help="Deterministic base run identifier")
    parser.add_argument("--ticker", default=None, help="Optional single ticker override")
    parser.add_argument("--tickers", default=None, help="Optional comma-separated ticker list")
    parser.add_argument("--limit-tickers", type=int, default=None, help="Optional limit after sorting")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Deterministic batch size")
    parser.add_argument("--dry-run", action="store_true", help="Print plan only without fetching or writing rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def validate_market(market: str) -> str:
    normalized_market = market.strip().lower()
    if normalized_market != DEFAULT_MARKET:
        raise RuntimeError(f"YAHOO_RAW_LOAD_UNSUPPORTED_MARKET:{market}")
    return normalized_market


def normalize_tickers(tickers_arg: str | None) -> list[str]:
    if tickers_arg is None or not tickers_arg.strip():
        return []
    return sorted({ticker.strip().upper() for ticker in tickers_arg.split(",") if ticker.strip()})


def load_usa_ticker_universe(db_path: Path) -> list[str]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ticker
            FROM rc_fundamental_quarterly
            WHERE ticker NOT LIKE '%.HE'
            ORDER BY ticker
            """
        ).fetchall()
    return [str(row[0]).upper() for row in rows]


def resolve_tickers(
    db_path: Path,
    market: str,
    ticker: str | None,
    tickers_arg: str | None,
    limit_tickers: int | None,
) -> list[str]:
    validate_market(market)
    if ticker is not None and tickers_arg is not None and tickers_arg.strip():
        raise RuntimeError("YAHOO_RAW_LOAD_TICKER_AND_TICKERS_MUTUALLY_EXCLUSIVE")
    if ticker is not None:
        tickers = [ticker.strip().upper()]
    elif tickers_arg is not None and tickers_arg.strip():
        tickers = normalize_tickers(tickers_arg)
    else:
        tickers = load_usa_ticker_universe(db_path)
    if limit_tickers is not None:
        tickers = tickers[:limit_tickers]
    return tickers


def chunk_tickers(tickers: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise RuntimeError(f"YAHOO_RAW_LOAD_INVALID_BATCH_SIZE:{batch_size}")
    return [tickers[idx : idx + batch_size] for idx in range(0, len(tickers), batch_size)]


def derive_batch_run_id(base_run_id: str, batch_index: int) -> str:
    return f"{base_run_id}__RAW__B{batch_index:04d}"


def run_batch(
    db_path: Path,
    market: str,
    tickers: list[str],
    batch_run_id: str,
    dry_run: bool,
) -> dict[str, Any]:
    return run_yahoo_audit(
        db_path=db_path,
        market=market,
        exchange=DEFAULT_EXCHANGE,
        symbols_arg=",".join(tickers),
        limit=None,
        run_id=batch_run_id,
        dry_run=dry_run,
    )


def run_fundamental_yahoo_raw_load(
    db_path: Path,
    market: str,
    run_id: str,
    ticker: str | None,
    tickers_arg: str | None,
    limit_tickers: int | None,
    batch_size: int,
    dry_run: bool,
) -> dict[str, Any]:
    normalized_market = validate_market(market)
    tickers = resolve_tickers(
        db_path=db_path,
        market=normalized_market,
        ticker=ticker,
        tickers_arg=tickers_arg,
        limit_tickers=limit_tickers,
    )
    batches = chunk_tickers(tickers, batch_size)
    if dry_run:
        for batch_number, batch_tickers in enumerate(batches, start=1):
            batch_run_id = derive_batch_run_id(run_id, batch_number)
            print(f"BATCH {batch_number}/{len(batches)} tickers={len(batch_tickers)} run_id={batch_run_id}")
        summary = {
            "market": normalized_market,
            "tickers_total": len(tickers),
            "tickers_processed": 0,
            "batch_size": batch_size,
            "batches_total": len(batches),
            "batches_executed": 0,
            "ok_count": 0,
            "empty_count": 0,
            "error_count": 0,
            "rows_written": 0,
            "dry_run": 1,
            "run_id": run_id,
        }
        _summary(**summary)
        return summary

    ok_count = 0
    empty_count = 0
    error_count = 0
    rows_written = 0
    tickers_processed = 0
    batches_executed = 0

    for batch_number, batch_tickers in enumerate(batches, start=1):
        batch_run_id = derive_batch_run_id(run_id, batch_number)
        print(f"BATCH {batch_number}/{len(batches)} tickers={len(batch_tickers)} run_id={batch_run_id}")
        try:
            batch_summary = run_batch(
                db_path=db_path,
                market=normalized_market,
                tickers=batch_tickers,
                batch_run_id=batch_run_id,
                dry_run=False,
            )
        except Exception:
            print(f"BATCH {batch_number}=FAILED")
            print(f"ERROR batch_run_id={batch_run_id}")
            raise
        batches_executed += 1
        tickers_processed += len(batch_tickers)
        ok_count += int(batch_summary["ok_count"])
        empty_count += int(batch_summary["empty_count"])
        error_count += int(batch_summary["error_count"])
        rows_written += int(batch_summary["rows_written"])

    summary = {
        "market": normalized_market,
        "tickers_total": len(tickers),
        "tickers_processed": tickers_processed,
        "batch_size": batch_size,
        "batches_total": len(batches),
        "batches_executed": batches_executed,
        "ok_count": ok_count,
        "empty_count": empty_count,
        "error_count": error_count,
        "rows_written": rows_written,
        "dry_run": 0,
        "run_id": run_id,
    }
    _summary(**summary)
    return summary


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    try:
        run_fundamental_yahoo_raw_load(
            db_path=db_path,
            market=args.market,
            run_id=args.run_id,
            ticker=args.ticker,
            tickers_arg=args.tickers,
            limit_tickers=args.limit_tickers,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
