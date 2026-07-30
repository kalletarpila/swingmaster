from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.earnings_events import (
    DEFAULT_SAFETY_MARGIN_DAYS,
    default_fundamentals_usa_db_path,
    fetch_yahoo_earnings_events,
    open_readonly_db,
    plan_earnings_history_range,
    plan_yahoo_earnings_limit,
    normalize_ticker,
)


ARTIFACT_SCHEMA_VERSION = 1
DEFAULT_SLEEP_SECONDS = 0.5
PER_TICKER_FIELDS = (
    "ticker",
    "fundamentals_row_count",
    "oldest_required_period_end_date",
    "fetch_lower_bound",
    "calculated_limit",
    "requested_limit",
    "limit_was_capped",
    "raw_yahoo_row_count",
    "completed_qualifying_count",
    "oldest_completed_announcement_date",
    "newest_completed_announcement_date",
    "covers_oldest_fundamentals_period",
    "covers_fetch_lower_bound",
    "coverage_status",
    "source_status",
    "error_type",
    "error_message",
    "elapsed_seconds",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Yahoo earnings-date coverage audit")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ticker", action="append", default=None)
    parser.add_argument("--tickers-file", default=None)
    parser.add_argument("--first-n", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--resume-from-json", default=None)
    parser.add_argument("--no-network", action="store_true")
    return parser.parse_args(argv)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def db_identity(db_path: Path) -> dict[str, Any]:
    stat = db_path.stat()
    return {
        "path": str(db_path.resolve()),
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def load_universe(conn: Any) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT UPPER(ticker) AS ticker
        FROM rc_fundamental_quarterly
        WHERE ticker IS NOT NULL
          AND period_end_date IS NOT NULL
          AND date(period_end_date) IS NOT NULL
        ORDER BY UPPER(ticker)
        """
    ).fetchall()
    return [str(row["ticker"]) for row in rows]


def select_tickers(args: argparse.Namespace, conn: Any) -> list[str]:
    if args.ticker:
        tickers = [normalize_ticker(ticker) for ticker in args.ticker]
    elif args.tickers_file:
        text = Path(args.tickers_file).read_text(encoding="utf-8")
        tickers = [normalize_ticker(line) for line in text.splitlines() if line.strip()]
    else:
        tickers = load_universe(conn)
    tickers = sorted(dict.fromkeys(tickers))
    if args.sample_size is not None:
        if args.sample_size < 0:
            raise ValueError("SAMPLE_SIZE_MUST_BE_NON_NEGATIVE")
        rng = random.Random(args.random_seed)
        tickers = sorted(rng.sample(tickers, min(args.sample_size, len(tickers))))
    if args.first_n is not None:
        if args.first_n < 0:
            raise ValueError("FIRST_N_MUST_BE_NON_NEGATIVE")
        tickers = tickers[: args.first_n]
    return tickers


def load_resume_artifact(path: Path, current_db_identity: dict[str, Any]) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("artifact_schema_version") != ARTIFACT_SCHEMA_VERSION:
        raise ValueError("INCOMPATIBLE_ARTIFACT_SCHEMA_VERSION")
    previous_identity = payload.get("database_identity") or {}
    if previous_identity.get("path") != current_db_identity.get("path"):
        raise ValueError("INCOMPATIBLE_ARTIFACT_DATABASE_PATH")
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("INVALID_ARTIFACT_RESULTS")
    return {normalize_ticker(str(row["ticker"])): dict(row) for row in results if "ticker" in row}


def audit_ticker(
    ticker: str,
    *,
    db_path: Path,
    no_network: bool,
    max_retries: int,
    sleep_seconds: float,
) -> dict[str, Any]:
    started = time.perf_counter()
    with open_readonly_db(db_path) as conn:
        range_plan = plan_earnings_history_range(conn, ticker)
    if range_plan.status == "NO_FUNDAMENTALS_HISTORY" or range_plan.fetch_lower_bound is None:
        return _row_from_plans(ticker, range_plan, None, None, "NO_FUNDAMENTALS_HISTORY", None, started)
    limit_plan = plan_yahoo_earnings_limit(range_plan.fetch_lower_bound)
    if no_network:
        return _row_from_plans(ticker, range_plan, limit_plan, None, "NOT_REQUESTED", None, started)

    attempts = max_retries + 1
    last_result: Any = None
    for attempt in range(attempts):
        last_result = fetch_yahoo_earnings_events(
            ticker=ticker,
            range_plan=range_plan,
            limit_plan=limit_plan,
            include_future=False,
        )
        if last_result.status not in {"SOURCE_FAILED", "PARSE_FAILED"}:
            break
        if attempt < attempts - 1:
            backoff = max(sleep_seconds, 0.0) * (attempt + 1)
            if backoff > 0:
                time.sleep(backoff)
    return _row_from_plans(ticker, range_plan, limit_plan, last_result, last_result.status, last_result.error_message, started)


def _row_from_plans(
    ticker: str,
    range_plan: Any,
    limit_plan: Any,
    source_result: Any,
    source_status: str,
    error_message: str | None,
    started: float,
) -> dict[str, Any]:
    coverage = None if source_result is None else source_result.coverage
    diagnostics = None if source_result is None else source_result.diagnostics
    if coverage is None:
        coverage_status = source_status
        oldest_completed = None
        newest_completed = None
        covers_oldest = False
        covers_lower = False
    else:
        coverage_status = coverage.coverage_status
        oldest_completed = coverage.oldest_returned_completed_announcement_date
        newest_completed = coverage.newest_returned_completed_announcement_date
        covers_oldest = coverage.covers_oldest_fundamentals_period
        covers_lower = coverage.covers_fetch_lower_bound
    return {
        "ticker": normalize_ticker(ticker),
        "fundamentals_row_count": range_plan.qualifying_fundamentals_row_count,
        "oldest_required_period_end_date": range_plan.oldest_required_period_end_date,
        "fetch_lower_bound": range_plan.fetch_lower_bound,
        "calculated_limit": None if limit_plan is None else limit_plan.uncapped_requested_limit,
        "requested_limit": None if limit_plan is None else limit_plan.requested_limit,
        "limit_was_capped": False if limit_plan is None else limit_plan.capped,
        "raw_yahoo_row_count": None if diagnostics is None else diagnostics.raw_row_count,
        "completed_qualifying_count": None if diagnostics is None else diagnostics.completed_qualifying_count,
        "oldest_completed_announcement_date": oldest_completed,
        "newest_completed_announcement_date": newest_completed,
        "covers_oldest_fundamentals_period": covers_oldest,
        "covers_fetch_lower_bound": covers_lower,
        "coverage_status": coverage_status,
        "source_status": source_status,
        "error_type": _classify_error(error_message),
        "error_message": error_message,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }


def _classify_error(error_message: str | None) -> str | None:
    if not error_message:
        return None
    lowered = error_message.lower()
    if "rate" in lowered or "429" in lowered or "too many" in lowered:
        return "RATE_LIMIT"
    if "http" in lowered or "curl" in lowered or "resolve host" in lowered:
        return "HTTP_OR_NETWORK"
    return "ERROR"


def aggregate_results(results: list[dict[str, Any]], *, total_tickers: int, started_at_utc: str, completed_at_utc: str) -> dict[str, Any]:
    successful = [row for row in results if row["source_status"] not in {"SOURCE_FAILED", "PARSE_FAILED"}]
    oldest_fundamentals = sorted(
        row["oldest_required_period_end_date"] for row in results if row.get("oldest_required_period_end_date")
    )
    oldest_yahoo = sorted(row["oldest_completed_announcement_date"] for row in successful if row.get("oldest_completed_announcement_date"))
    return {
        "total_tickers": total_tickers,
        "processed_tickers": len(results),
        "successful_tickers": len(successful),
        "coverage_ok_count": _count_status(results, "COVERAGE_OK"),
        "coverage_partial_count": _count_status(results, "COVERAGE_PARTIAL"),
        "covers_oldest_fundamentals_period_count": sum(1 for row in results if row.get("covers_oldest_fundamentals_period")),
        "does_not_cover_oldest_fundamentals_period_count": sum(1 for row in results if not row.get("covers_oldest_fundamentals_period")),
        "no_yahoo_rows_count": _count_status(results, "NO_YAHOO_ROWS"),
        "source_failed_count": _count_status(results, "SOURCE_FAILED"),
        "parse_failed_count": _count_status(results, "PARSE_FAILED"),
        "capped_limit_count": sum(1 for row in results if row.get("limit_was_capped")),
        "oldest_fundamentals_date_across_universe": oldest_fundamentals[0] if oldest_fundamentals else None,
        "oldest_yahoo_date_across_successful_tickers": oldest_yahoo[0] if oldest_yahoo else None,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
    }


def _count_status(results: list[dict[str, Any]], status: str) -> int:
    return sum(1 for row in results if row.get("coverage_status") == status or row.get("source_status") == status)


def run_audit(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if args.sleep_seconds < 0:
        raise ValueError("SLEEP_SECONDS_MUST_BE_NON_NEGATIVE")
    if args.max_retries < 0:
        raise ValueError("MAX_RETRIES_MUST_BE_NON_NEGATIVE")
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    current_identity = db_identity(db_path)
    with open_readonly_db(db_path) as conn:
        tickers = select_tickers(args, conn)
    resume_results: dict[str, dict[str, Any]] = {}
    if args.resume_from_json:
        resume_results = load_resume_artifact(Path(args.resume_from_json), current_identity)
    started_at_utc = utc_now_text()
    results: list[dict[str, Any]] = []
    for idx, ticker in enumerate(tickers):
        previous = resume_results.get(ticker)
        if previous is not None and previous.get("source_status") not in {"SOURCE_FAILED", "PARSE_FAILED"}:
            results.append(previous)
            continue
        if idx > 0 and args.sleep_seconds > 0 and not args.no_network:
            time.sleep(args.sleep_seconds)
        results.append(
            audit_ticker(
                ticker,
                db_path=db_path,
                no_network=args.no_network,
                max_retries=args.max_retries,
                sleep_seconds=args.sleep_seconds,
            )
        )
    completed_at_utc = utc_now_text()
    payload = {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "database_identity": current_identity,
        "arguments": {
            "tickers": tickers,
            "no_network": bool(args.no_network),
            "sleep_seconds": args.sleep_seconds,
            "max_retries": args.max_retries,
            "random_seed": args.random_seed,
        },
        "summary": aggregate_results(results, total_tickers=len(tickers), started_at_utc=started_at_utc, completed_at_utc=completed_at_utc),
        "results": results,
    }
    if args.output_json:
        Path(args.output_json).write_text(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    if args.output_csv:
        with Path(args.output_csv).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=PER_TICKER_FIELDS)
            writer.writeheader()
            writer.writerows([{field: row.get(field) for field in PER_TICKER_FIELDS} for row in results])
    return payload, 0


def print_text(payload: dict[str, Any]) -> None:
    for key, value in payload["summary"].items():
        print(f"SUMMARY {key}={value}")
    for row in payload["results"]:
        print(
            "TICKER "
            + " ".join(
                f"{field}={row.get(field)}"
                for field in (
                    "ticker",
                    "coverage_status",
                    "covers_oldest_fundamentals_period",
                    "covers_fetch_lower_bound",
                    "requested_limit",
                    "oldest_completed_announcement_date",
                    "source_status",
                )
            )
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload, exit_code = run_audit(args)
    except Exception as exc:
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if args.output_json is None:
        print(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        print_text(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
