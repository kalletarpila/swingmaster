from __future__ import annotations

import argparse
import json
import random
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_calendar import (
    SOURCE_YAHOO,
    new_york_today_from_utc,
    select_future_yahoo_estimate,
    upsert_earnings_calendar,
)
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker
from swingmaster.fundamentals.quarter_completeness import utc_timestamp, validate_temp_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh current Yahoo future earnings calendar estimates")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ticker", action="append", default=None)
    parser.add_argument("--tickers-file", default=None)
    parser.add_argument("--first-n", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--sleep-min-seconds", type=float, default=0.8)
    parser.add_argument("--sleep-max-seconds", type=float, default=1.4)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--rate-limit-backoff-seconds", default="30,60,120")
    parser.add_argument("--checkpoint-json", default=None)
    parser.add_argument("--summary-json", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--resume-from-json", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--backup", default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.apply == args.dry_run:
        raise ValueError("CHOOSE_EXACTLY_ONE_OF_APPLY_OR_DRY_RUN")
    if args.sleep_min_seconds < 0 or args.sleep_max_seconds < args.sleep_min_seconds:
        raise ValueError("INVALID_SLEEP_RANGE")
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    output_root = validate_temp_path(
        Path(args.output_root) if args.output_root else Path("temp") / "earnings_calendar_and_ingestion_status" / utc_timestamp()
    )
    checkpoint_json = validate_temp_path(Path(args.checkpoint_json)) if args.checkpoint_json else output_root / "calendar_checkpoint.json"
    summary_json = validate_temp_path(Path(args.summary_json)) if args.summary_json else output_root / "calendar_summary.json"
    output_csv = validate_temp_path(Path(args.output_csv)) if args.output_csv else output_root / "calendar_rows.csv"
    backup_path = validate_temp_path(Path(args.backup)) if args.backup else output_root / "backups" / f"{db_path.name}.pre_earnings_calendar.bak"
    if args.resume_from_json:
        validate_temp_path(Path(args.resume_from_json), must_exist=True)

    tickers = _select_tickers(db_path, args)
    if args.apply:
        _backup(db_path, backup_path)
        run_migration(db_path)

    today_new_york = new_york_today_from_utc()
    rows: list[dict[str, Any]] = []
    counts = {
        "selected_tickers": len(tickers),
        "successful_source_tickers": 0,
        "future_estimate_found_count": 0,
        "no_current_estimate_count": 0,
        "completed_event_found_count": 0,
        "upcoming_count": 0,
        "due_today_count": 0,
        "date_passed_event_not_found_count": 0,
        "date_changed_count": 0,
        "source_failed_count": 0,
        "parse_failed_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
        "unchanged_count": 0,
    }
    rng = random.Random(args.random_seed)
    backoffs = [float(item) for item in str(args.rate_limit_backoff_seconds).split(",") if item]
    for index, ticker in enumerate(tickers, start=1):
        observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        try:
            source_rows = _fetch_yahoo_rows(ticker, args.max_retries, backoffs)
            estimate = select_future_yahoo_estimate(source_rows, today_new_york=today_new_york, ticker=ticker)
            counts["successful_source_tickers"] += 1
            counts["future_estimate_found_count" if estimate else "no_current_estimate_count"] += 1
        except Exception as exc:  # pragma: no cover - live source defensive path
            estimate = None
            counts["source_failed_count"] += 1
            rows.append({"ticker": ticker, "status": "SOURCE_FAILED", "error": str(exc)})
            _write_checkpoint(checkpoint_json, counts, rows)
            continue
        if args.apply:
            with sqlite3.connect(str(db_path)) as conn:
                before = _calendar_row(conn, ticker)
                status = upsert_earnings_calendar(
                    conn,
                    market="usa",
                    ticker=ticker,
                    estimate=estimate,
                    observed_at_utc=observed_at,
                    today_new_york=today_new_york,
                )
                after = _calendar_row(conn, ticker)
                conn.commit()
            if before is None:
                counts["inserted_count"] += 1
            elif before == after:
                counts["unchanged_count"] += 1
            else:
                counts["updated_count"] += 1
        else:
            status = "DRY_RUN_ESTIMATE_FOUND" if estimate else "DRY_RUN_NO_CURRENT_ESTIMATE"
        _count_status(counts, status)
        rows.append({"ticker": ticker, "calendar_status": status, "estimated_announcement_date": estimate.estimated_announcement_date if estimate else None, "source": SOURCE_YAHOO})
        _write_checkpoint(checkpoint_json, counts, rows)
        if index < len(tickers):
            time.sleep(rng.uniform(args.sleep_min_seconds, args.sleep_max_seconds))

    _write_json(summary_json, counts)
    _write_csv(output_csv, rows)
    if args.json_output:
        print(json.dumps(counts, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        for key, value in counts.items():
            print(f"SUMMARY {key}={value}")
        print(f"ARTIFACT checkpoint_json={checkpoint_json}")
        print(f"ARTIFACT summary_json={summary_json}")
        print(f"ARTIFACT output_csv={output_csv}")
        if args.apply:
            print(f"ARTIFACT backup={backup_path}")
    return 0


def _select_tickers(db_path: Path, args: argparse.Namespace) -> list[str]:
    tickers = [normalize_ticker(ticker) for ticker in (args.ticker or [])]
    if args.tickers_file:
        path = validate_temp_path(Path(args.tickers_file), must_exist=True)
        tickers.extend(normalize_ticker(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if not tickers:
        with sqlite3.connect(str(db_path)) as conn:
            tickers = [str(row[0]) for row in conn.execute("SELECT DISTINCT ticker FROM rc_fundamental_quarterly ORDER BY ticker")]
    if args.first_n is not None:
        tickers = tickers[: args.first_n]
    if args.sample_size is not None:
        rng = random.Random(args.random_seed)
        tickers = sorted(rng.sample(tickers, min(args.sample_size, len(tickers))))
    return sorted(dict.fromkeys(tickers))


def _fetch_yahoo_rows(ticker: str, max_retries: int, backoffs: list[float]) -> list[dict[str, Any]]:
    import yfinance as yf

    attempt = 0
    while True:
        try:
            data = yf.Ticker(ticker).get_earnings_dates(limit=12)
            return data.reset_index().to_dict("records") if hasattr(data, "reset_index") else []
        except Exception:
            attempt += 1
            if attempt >= max_retries:
                raise
            time.sleep(backoffs[min(attempt - 1, len(backoffs) - 1)] if backoffs else 30.0)


def _backup(db_path: Path, backup_path: Path) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    if backup_path.exists():
        return
    with sqlite3.connect(str(db_path)) as source, sqlite3.connect(str(backup_path)) as target:
        source.backup(target)


def _calendar_row(conn: sqlite3.Connection, ticker: str) -> tuple[Any, ...] | None:
    return conn.execute(
        "SELECT calendar_status, estimated_announcement_at, date_change_count, completed_earnings_event_id FROM rc_earnings_calendar WHERE ticker=?",
        (normalize_ticker(ticker),),
    ).fetchone()


def _count_status(counts: dict[str, int], status: str) -> None:
    mapping = {
        "COMPLETED_EVENT_FOUND": "completed_event_found_count",
        "UPCOMING": "upcoming_count",
        "DUE_TODAY": "due_today_count",
        "DATE_PASSED_EVENT_NOT_FOUND": "date_passed_event_not_found_count",
    }
    key = mapping.get(status)
    if key:
        counts[key] += 1


def _write_checkpoint(path: Path, counts: dict[str, int], rows: list[dict[str, Any]]) -> None:
    _write_json(path, {"summary": counts, "rows": rows})


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not rows:
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
