from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_quarterly_to_ttm import run_quarterly_to_ttm
from swingmaster.cli.run_fundamental_valuation import run_fundamental_valuation
from swingmaster.cli.run_fundamental_yahoo_audit import run_yahoo_audit
from swingmaster.cli.run_fundamental_yahoo_quarterly_write import run_yahoo_quarterly_write
from swingmaster.cli.run_fundamental_yahoo_to_quarterly import run_yahoo_to_quarterly
from swingmaster.fundamentals.lifecycle import run_lifecycle_classification
from swingmaster.fundamentals.score import run_fundamental_scoring


DEFAULT_MARKET = "omxh"
UNIVERSE_MARKET = "omxh"
DEFAULT_EXCHANGE = "HE"
YAHOO_TICKER_DELAY_SECONDS = 0.5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Yahoo fundamentals batch pipeline for OMXH tickers")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--osakedata-db", required=True, help="Osakedata SQLite database path")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--limit", type=int, default=None, help="Optional symbol limit after deterministic sorting")
    parser.add_argument("--dry-run", action="store_true", help="Execute validation flow without writing rows")
    parser.add_argument(
        "--replace-symbol",
        action="store_true",
        help="Replace existing symbol rows in intermediate and downstream fundamentals tables",
    )
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def resolve_failure_log_path(db_path: Path, run_id: str) -> Path:
    return db_path.parent / f"failed_yahoo_batch_{run_id}.txt"


def load_omxh_ticker_universe(osakedata_db_path: Path) -> list[str]:
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT osake
            FROM osakedata
            WHERE market = ?
            ORDER BY osake
            """,
            (UNIVERSE_MARKET,),
        ).fetchall()
    return [str(row[0]) for row in rows]


def resolve_latest_close_as_of_date(osakedata_db_path: Path, market: str) -> str:
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        row = conn.execute(
            """
            SELECT MAX(pvm)
            FROM osakedata
            WHERE market = ?
              AND close IS NOT NULL
            """,
            (market,),
        ).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"FUNDAMENTAL_YAHOO_BATCH_VALUATION_AS_OF_DATE_NOT_FOUND:{market}")
    return str(row[0])


def run_lifecycle_step(db_path: Path, ticker: str, dry_run: bool) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        rows_classified, _class_counts = run_lifecycle_classification(
            conn=conn,
            ticker=ticker,
            dry_run=dry_run,
        )
    return 0 if dry_run else rows_classified


def run_score_step(db_path: Path, ticker: str, dry_run: bool) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        rows_scored, _min_score, _max_score, _avg_score = run_fundamental_scoring(
            conn=conn,
            ticker=ticker,
            dry_run=dry_run,
        )
    return 0 if dry_run else rows_scored


def process_symbol(
    db_path: Path,
    market: str,
    symbol: str,
    run_id: str,
    dry_run: bool,
    replace_symbol: bool,
) -> dict[str, Any]:
    raw_summary = run_yahoo_audit(
        db_path=db_path,
        market=market,
        exchange=DEFAULT_EXCHANGE,
        symbols_arg=symbol,
        limit=None,
        run_id=run_id,
        dry_run=dry_run,
    )
    if int(raw_summary["error_count"]) > 0:
        raise RuntimeError(f"YAHOO_RAW_ERROR:{symbol}")

    yahoo_quarterly_summary = run_yahoo_quarterly_write(
        db_path=db_path,
        market=market,
        symbol=symbol,
        run_id=run_id,
        dry_run=dry_run,
        replace_symbol=replace_symbol,
    )
    generic_quarterly_summary = run_yahoo_to_quarterly(
        db_path=db_path,
        market=market,
        symbol=symbol,
        run_id=run_id,
        dry_run=dry_run,
        replace_symbol=replace_symbol,
    )
    ttm_summary = run_quarterly_to_ttm(
        db_path=db_path,
        ticker=symbol,
        run_id=run_id,
        dry_run=dry_run,
        replace_ticker=replace_symbol,
    )
    lifecycle_rows_written = run_lifecycle_step(
        db_path=db_path,
        ticker=symbol.upper(),
        dry_run=dry_run,
    )
    score_rows_written = run_score_step(
        db_path=db_path,
        ticker=symbol.upper(),
        dry_run=dry_run,
    )

    return {
        "symbol": symbol.upper(),
        "raw_status": "OK" if int(raw_summary["ok_count"]) > 0 else "EMPTY",
        "yahoo_quarterly_rows_written": int(yahoo_quarterly_summary["rows_written"]),
        "quarterly_rows_written": int(generic_quarterly_summary["rows_written"]),
        "ttm_rows_written": int(ttm_summary["rows_written"]),
        "lifecycle_rows_written": lifecycle_rows_written,
        "score_rows_written": score_rows_written,
    }


def run_yahoo_batch_fin(
    db_path: Path,
    osakedata_db_path: Path,
    run_id: str,
    limit: int | None,
    dry_run: bool,
    replace_symbol: bool,
) -> dict[str, Any]:
    universe = load_omxh_ticker_universe(osakedata_db_path)
    symbols = universe[:limit] if limit is not None else universe
    failure_log_path = resolve_failure_log_path(db_path, run_id)

    symbols_ok = 0
    symbols_error = 0
    quarterly_rows_written_total = 0
    ttm_rows_written_total = 0
    lifecycle_rows_written_total = 0
    score_rows_written_total = 0
    failure_lines: list[str] = []

    for symbol in symbols:
        if symbols_ok + symbols_error > 0:
            time.sleep(YAHOO_TICKER_DELAY_SECONDS)
        try:
            result = process_symbol(
                db_path=db_path,
                market=DEFAULT_MARKET,
                symbol=symbol,
                run_id=run_id,
                dry_run=dry_run,
                replace_symbol=replace_symbol,
            )
            symbols_ok += 1
            quarterly_rows_written_total += int(result["quarterly_rows_written"])
            ttm_rows_written_total += int(result["ttm_rows_written"])
            lifecycle_rows_written_total += int(result["lifecycle_rows_written"])
            score_rows_written_total += int(result["score_rows_written"])
        except Exception as exc:
            symbols_error += 1
            failure_lines.append(f"{symbol}\t{exc}")
            print(f"ERROR symbol={symbol} message={exc}", file=sys.stderr)

    if failure_lines:
        failure_log_path.write_text("\n".join(failure_lines) + "\n", encoding="utf-8")

    valuation_as_of_date = resolve_latest_close_as_of_date(osakedata_db_path, DEFAULT_MARKET)
    valuation_summary = run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market=DEFAULT_MARKET,
        as_of_date=valuation_as_of_date,
        ticker=None,
        run_id=f"{run_id}__VALUATION",
        dry_run=dry_run,
        replace=True,
    )
    valuation_rows_written = int(valuation_summary["rows_written"])

    return {
        "market": DEFAULT_MARKET,
        "universe_size": len(universe),
        "symbols_processed": len(symbols),
        "symbols_ok": symbols_ok,
        "symbols_error": symbols_error,
        "quarterly_rows_written_total": quarterly_rows_written_total,
        "ttm_rows_written_total": ttm_rows_written_total,
        "lifecycle_rows_written_total": lifecycle_rows_written_total,
        "score_rows_written_total": score_rows_written_total,
        "valuation_as_of_date": valuation_as_of_date,
        "valuation_rows_written": valuation_rows_written,
        "dry_run": "true" if dry_run else "false",
        "run_id": run_id,
    }


def main() -> None:
    args = parse_args()
    summary = run_yahoo_batch_fin(
        db_path=resolve_db_path(args.db),
        osakedata_db_path=resolve_db_path(args.osakedata_db),
        run_id=args.run_id,
        limit=args.limit,
        dry_run=args.dry_run,
        replace_symbol=args.replace_symbol,
    )
    _summary(market=summary["market"])
    _summary(universe_size=summary["universe_size"])
    _summary(symbols_processed=summary["symbols_processed"])
    _summary(symbols_ok=summary["symbols_ok"])
    _summary(symbols_error=summary["symbols_error"])
    _summary(quarterly_rows_written_total=summary["quarterly_rows_written_total"])
    _summary(ttm_rows_written_total=summary["ttm_rows_written_total"])
    _summary(lifecycle_rows_written_total=summary["lifecycle_rows_written_total"])
    _summary(score_rows_written_total=summary["score_rows_written_total"])
    _summary(valuation_as_of_date=summary["valuation_as_of_date"])
    _summary(valuation_rows_written=summary["valuation_rows_written"])
    _summary(dry_run=summary["dry_run"])
    _summary(run_id=summary["run_id"])


if __name__ == "__main__":
    main()
