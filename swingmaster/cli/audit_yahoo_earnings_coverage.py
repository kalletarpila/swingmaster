from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from dataclasses import asdict
from datetime import date
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
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


ARTIFACT_SCHEMA_VERSION = 2
DEFAULT_SLEEP_SECONDS = 0.5
PER_TICKER_FIELDS = (
    "ticker",
    "fundamentals_row_count",
    "oldest_required_period_end_date",
    "newest_fundamentals_period_end_date",
    "fetch_lower_bound",
    "calculated_limit",
    "requested_limit",
    "limit_was_capped",
    "cap_source",
    "raw_yahoo_row_count",
    "completed_qualifying_count",
    "unreported_count",
    "duplicate_count",
    "invalid_count",
    "oldest_completed_announcement_date",
    "newest_completed_announcement_date",
    "covers_oldest_fundamentals_period",
    "covers_fetch_lower_bound",
    "coverage_status",
    "source_status",
    "planning_classification",
    "uncovered_actual_days",
    "uncovered_actual_quarters",
    "anomaly_flags",
    "attempt_count",
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
    parser.add_argument("--sleep-min-seconds", type=float, default=None)
    parser.add_argument("--sleep-max-seconds", type=float, default=None)
    parser.add_argument("--rate-limit-backoff-seconds", default="30,60,120")
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--output-summary-json", default=None)
    parser.add_argument("--resume-from-json", default=None)
    parser.add_argument("--no-network", action="store_true")
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument("--progress-log", default=None)
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


def load_newest_fundamentals_period_end(conn: Any, ticker: str) -> str | None:
    row = conn.execute(
        """
        SELECT MAX(date(period_end_date)) AS newest_period_end
        FROM rc_fundamental_quarterly
        WHERE UPPER(ticker) = ?
          AND period_end_date IS NOT NULL
          AND date(period_end_date) IS NOT NULL
        """,
        (normalize_ticker(ticker),),
    ).fetchone()
    return str(row["newest_period_end"]) if row is not None and row["newest_period_end"] is not None else None


def audit_ticker(
    ticker: str,
    *,
    db_path: Path,
    no_network: bool,
    max_retries: int,
    sleep_seconds: float,
    rate_limit_backoffs: list[float],
) -> dict[str, Any]:
    started = time.perf_counter()
    with open_readonly_db(db_path) as conn:
        range_plan = plan_earnings_history_range(conn, ticker)
        newest_fundamentals_period_end_date = load_newest_fundamentals_period_end(conn, ticker)
    if range_plan.status == "NO_FUNDAMENTALS_HISTORY" or range_plan.fetch_lower_bound is None:
        return _row_from_plans(ticker, range_plan, None, None, "NO_FUNDAMENTALS_HISTORY", None, started, newest_fundamentals_period_end_date, 0)
    limit_plan = plan_yahoo_earnings_limit(range_plan.fetch_lower_bound)
    if no_network:
        return _row_from_plans(ticker, range_plan, limit_plan, None, "NOT_REQUESTED", None, started, newest_fundamentals_period_end_date, 0)

    attempts = max_retries + 1
    last_result: Any = None
    attempt_count = 0
    for attempt in range(attempts):
        attempt_count = attempt + 1
        last_result = fetch_yahoo_earnings_events(
            ticker=ticker,
            range_plan=range_plan,
            limit_plan=limit_plan,
            include_future=False,
        )
        if last_result.status not in {"SOURCE_FAILED", "PARSE_FAILED"}:
            break
        if attempt < attempts - 1:
            error_type = _classify_error(last_result.status, last_result.error_message)
            if error_type == "RATE_LIMIT" and attempt < len(rate_limit_backoffs):
                backoff = rate_limit_backoffs[attempt]
            else:
                backoff = max(sleep_seconds, 0.0) * (attempt + 1)
            if backoff > 0:
                time.sleep(backoff)
    return _row_from_plans(
        ticker,
        range_plan,
        limit_plan,
        last_result,
        last_result.status,
        last_result.error_message,
        started,
        newest_fundamentals_period_end_date,
        attempt_count,
    )


def _row_from_plans(
    ticker: str,
    range_plan: Any,
    limit_plan: Any,
    source_result: Any,
    source_status: str,
    error_message: str | None,
    started: float,
    newest_fundamentals_period_end_date: str | None,
    attempt_count: int,
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
    row = {
        "ticker": normalize_ticker(ticker),
        "fundamentals_row_count": range_plan.qualifying_fundamentals_row_count,
        "oldest_required_period_end_date": range_plan.oldest_required_period_end_date,
        "newest_fundamentals_period_end_date": newest_fundamentals_period_end_date,
        "fetch_lower_bound": range_plan.fetch_lower_bound,
        "calculated_limit": None if limit_plan is None else limit_plan.uncapped_requested_limit,
        "requested_limit": None if limit_plan is None else limit_plan.requested_limit,
        "limit_was_capped": False if limit_plan is None else limit_plan.capped,
        "cap_source": None if limit_plan is None else limit_plan.cap_source,
        "raw_yahoo_row_count": None if diagnostics is None else diagnostics.raw_row_count,
        "completed_qualifying_count": None if diagnostics is None else diagnostics.completed_qualifying_count,
        "unreported_count": None if diagnostics is None else diagnostics.unreported_count,
        "duplicate_count": None if diagnostics is None else diagnostics.duplicate_count,
        "invalid_count": None if diagnostics is None else diagnostics.invalid_count,
        "oldest_completed_announcement_date": oldest_completed,
        "newest_completed_announcement_date": newest_completed,
        "covers_oldest_fundamentals_period": covers_oldest,
        "covers_fetch_lower_bound": covers_lower,
        "coverage_status": coverage_status,
        "source_status": source_status,
        "error_type": _classify_error(source_status, error_message),
        "error_message": error_message,
        "attempt_count": attempt_count,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    row["planning_classification"] = _planning_classification(row)
    row["uncovered_actual_days"] = _uncovered_actual_days(row)
    row["uncovered_actual_quarters"] = _uncovered_actual_quarters(row["uncovered_actual_days"])
    row["anomaly_flags"] = ",".join(_anomaly_flags(row))
    return row


def _classify_error(source_status: str, error_message: str | None) -> str | None:
    if source_status == "PARSE_FAILED":
        return "PARSE_ERROR"
    if not error_message:
        return None
    lowered = error_message.lower()
    if "rate" in lowered or "429" in lowered or "too many" in lowered:
        return "RATE_LIMIT"
    if "timeout" in lowered or "timed out" in lowered:
        return "TIMEOUT"
    if "http" in lowered or "curl" in lowered or "resolve host" in lowered or "dns" in lowered:
        return "HTTP_ERROR"
    return "YAHOO_SOURCE_ERROR"


def aggregate_results(results: list[dict[str, Any]], *, total_tickers: int, started_at_utc: str, completed_at_utc: str) -> dict[str, Any]:
    successful = [row for row in results if row["source_status"] not in {"SOURCE_FAILED", "PARSE_FAILED"}]
    source_successful = [row for row in results if row["source_status"] not in {"SOURCE_FAILED", "PARSE_FAILED", "NOT_REQUESTED"}]
    completed_counts = [
        int(row["completed_qualifying_count"])
        for row in results
        if isinstance(row.get("completed_qualifying_count"), int)
    ]
    oldest_fundamentals = sorted(
        row["oldest_required_period_end_date"] for row in results if row.get("oldest_required_period_end_date")
    )
    newest_fundamentals = sorted(
        row["newest_fundamentals_period_end_date"] for row in results if row.get("newest_fundamentals_period_end_date")
    )
    oldest_yahoo = sorted(row["oldest_completed_announcement_date"] for row in successful if row.get("oldest_completed_announcement_date"))
    newest_yahoo = sorted(row["newest_completed_announcement_date"] for row in successful if row.get("newest_completed_announcement_date"))
    classification_counts = _counts_by(results, "planning_classification")
    capped_rows = [row for row in results if row.get("limit_was_capped")]
    uncovered_actual_days = [int(row["uncovered_actual_days"]) for row in results if isinstance(row.get("uncovered_actual_days"), int)]
    elapsed_seconds = _elapsed_seconds(started_at_utc, completed_at_utc)
    return {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "total_tickers": total_tickers,
        "total_universe_tickers": total_tickers,
        "processed_tickers": len(results),
        "successful_tickers": len(successful),
        "source_successful_tickers": len(source_successful),
        "coverage_ok_count": _count_status(results, "COVERAGE_OK"),
        "coverage_partial_count": _count_status(results, "COVERAGE_PARTIAL"),
        "covers_oldest_fundamentals_period_count": sum(1 for row in results if row.get("covers_oldest_fundamentals_period")),
        "does_not_cover_oldest_fundamentals_period_count": sum(1 for row in results if not row.get("covers_oldest_fundamentals_period")),
        "covers_fetch_lower_bound_count": sum(1 for row in results if row.get("covers_fetch_lower_bound")),
        "does_not_cover_fetch_lower_bound_count": sum(1 for row in results if not row.get("covers_fetch_lower_bound")),
        "no_yahoo_rows_count": _count_status(results, "NO_YAHOO_ROWS"),
        "source_failed_count": _count_status(results, "SOURCE_FAILED"),
        "parse_failed_count": _count_status(results, "PARSE_FAILED"),
        "capped_limit_count": sum(1 for row in results if row.get("limit_was_capped")),
        "not_requested_count": _count_status(results, "NOT_REQUESTED"),
        "actual_history_available_count": sum(1 for row in results if (row.get("completed_qualifying_count") or 0) > 0),
        "actual_history_missing_count": sum(1 for row in results if row.get("completed_qualifying_count") == 0),
        "total_completed_events_available": sum(completed_counts),
        "min_completed_events_available": min(completed_counts) if completed_counts else None,
        "median_completed_events_available": median(completed_counts) if completed_counts else None,
        "max_completed_events_available": max(completed_counts) if completed_counts else None,
        "planning_classification_counts": classification_counts,
        "planning_classification_tickers": _tickers_by(results, "planning_classification"),
        "error_type_counts": _counts_by(results, "error_type"),
        "anomaly_flag_counts": _anomaly_flag_counts(results),
        "capped_limit_analysis": {
            "capped_ticker_count": len(capped_rows),
            "max_uncovered_actual_days": max(uncovered_actual_days) if uncovered_actual_days else 0,
            "median_uncovered_actual_days": median(uncovered_actual_days) if uncovered_actual_days else 0,
            "max_uncovered_actual_quarters": _uncovered_actual_quarters(max(uncovered_actual_days)) if uncovered_actual_days else 0,
            "oldest_capped_fetch_lower_bound": min((row["fetch_lower_bound"] for row in capped_rows if row.get("fetch_lower_bound")), default=None),
        },
        "percentages": _percentages(results),
        "oldest_fundamentals_date_across_universe": oldest_fundamentals[0] if oldest_fundamentals else None,
        "newest_fundamentals_date_across_universe": newest_fundamentals[-1] if newest_fundamentals else None,
        "oldest_yahoo_date_across_successful_tickers": oldest_yahoo[0] if oldest_yahoo else None,
        "newest_yahoo_date_across_successful_tickers": newest_yahoo[-1] if newest_yahoo else None,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "elapsed_seconds": elapsed_seconds,
    }


def _count_status(results: list[dict[str, Any]], status: str) -> int:
    return sum(1 for row in results if row.get("coverage_status") == status or row.get("source_status") == status)


def _counts_by(results: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in results:
        value = row.get(field)
        if value:
            counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _tickers_by(results: list[dict[str, Any]], field: str) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for row in results:
        value = row.get(field)
        if value:
            grouped.setdefault(str(value), []).append(str(row["ticker"]))
    return {key: sorted(value) for key, value in sorted(grouped.items())}


def _anomaly_flag_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in results:
        flags = str(row.get("anomaly_flags") or "")
        for flag in [item for item in flags.split(",") if item]:
            counts[flag] = counts.get(flag, 0) + 1
    return dict(sorted(counts.items()))


def _percentages(results: list[dict[str, Any]]) -> dict[str, float]:
    total = len(results)
    if total == 0:
        return {}
    return {
        "covers_oldest_fundamentals_period_pct": round(100.0 * sum(1 for row in results if row.get("covers_oldest_fundamentals_period")) / total, 2),
        "covers_fetch_lower_bound_pct": round(100.0 * sum(1 for row in results if row.get("covers_fetch_lower_bound")) / total, 2),
        "source_failed_pct": round(100.0 * _count_status(results, "SOURCE_FAILED") / total, 2),
        "parse_failed_pct": round(100.0 * _count_status(results, "PARSE_FAILED") / total, 2),
        "no_yahoo_rows_pct": round(100.0 * _count_status(results, "NO_YAHOO_ROWS") / total, 2),
        "capped_limit_pct": round(100.0 * sum(1 for row in results if row.get("limit_was_capped")) / total, 2),
    }


def _planning_classification(row: dict[str, Any]) -> str:
    if row.get("source_status") == "SOURCE_FAILED":
        return "BACKFILL_SOURCE_FAILED"
    if row.get("source_status") == "PARSE_FAILED":
        return "BACKFILL_PARSE_FAILED"
    if row.get("coverage_status") == "NO_YAHOO_ROWS" or row.get("completed_qualifying_count") == 0:
        return "BACKFILL_NO_YAHOO_ROWS"
    if row.get("covers_fetch_lower_bound"):
        return "BACKFILL_READY_FULL_HISTORY"
    if row.get("covers_oldest_fundamentals_period"):
        return "BACKFILL_READY_PARTIAL_MARGIN_ONLY"
    return "BACKFILL_PARTIAL_ACTUAL_HISTORY"


def _uncovered_actual_days(row: dict[str, Any]) -> int | None:
    oldest_required = row.get("oldest_required_period_end_date")
    oldest_completed = row.get("oldest_completed_announcement_date")
    if not oldest_required or not oldest_completed:
        return None
    try:
        return max((date.fromisoformat(str(oldest_completed)) - date.fromisoformat(str(oldest_required))).days, 0)
    except ValueError:
        return None


def _uncovered_actual_quarters(days: int | None) -> int | None:
    if days is None:
        return None
    return int((days + 91) // 92)


def _anomaly_flags(row: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if row.get("source_status") == "SOURCE_FAILED":
        flags.append("SOURCE_FAILED")
    if row.get("source_status") == "PARSE_FAILED":
        flags.append("PARSE_FAILED")
    if row.get("coverage_status") == "NO_YAHOO_ROWS":
        flags.append("NO_YAHOO_ROWS")
    if row.get("limit_was_capped"):
        flags.append("CAPPED_LIMIT")
    if (row.get("duplicate_count") or 0) > 0:
        flags.append("DUPLICATE_YAHOO_TIMESTAMPS")
    if (row.get("invalid_count") or 0) > 0:
        flags.append("INVALID_YAHOO_TIMESTAMPS")
    if row.get("completed_qualifying_count") and not row.get("covers_oldest_fundamentals_period"):
        flags.append("ACTUAL_HISTORY_GAP")
    if (
        row.get("completed_qualifying_count") is not None
        and row.get("fundamentals_row_count", 0) >= 16
        and (row.get("completed_qualifying_count") or 0) < 4
    ):
        flags.append("LOW_COMPLETED_ROWS_FOR_LONG_HISTORY")
    return flags


def _elapsed_seconds(started_at_utc: str, completed_at_utc: str) -> float | None:
    try:
        started = datetime.fromisoformat(started_at_utc.replace("Z", "+00:00"))
        completed = datetime.fromisoformat(completed_at_utc.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((completed - started).total_seconds(), 3)


def run_audit(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if args.sleep_seconds < 0:
        raise ValueError("SLEEP_SECONDS_MUST_BE_NON_NEGATIVE")
    if args.max_retries < 0:
        raise ValueError("MAX_RETRIES_MUST_BE_NON_NEGATIVE")
    if args.progress_every < 0:
        raise ValueError("PROGRESS_EVERY_MUST_BE_NON_NEGATIVE")
    sleep_min, sleep_max = _sleep_range(args)
    rate_limit_backoffs = _parse_backoffs(args.rate_limit_backoff_seconds)
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    current_identity = db_identity(db_path)
    with open_readonly_db(db_path) as conn:
        tickers = select_tickers(args, conn)
    resume_results: dict[str, dict[str, Any]] = {}
    if args.resume_from_json:
        resume_results = load_resume_artifact(Path(args.resume_from_json), current_identity)
    started_at_utc = utc_now_text()
    started_perf = time.perf_counter()
    results: list[dict[str, Any]] = []
    for idx, ticker in enumerate(tickers):
        previous = resume_results.get(ticker)
        if previous is not None and previous.get("source_status") not in {"SOURCE_FAILED", "PARSE_FAILED"}:
            results.append(previous)
            checkpoint = _build_payload(current_identity, tickers, results, args, started_at_utc, utc_now_text(), complete=len(results) == len(tickers))
            _write_outputs(args, checkpoint)
            _print_progress(results, len(tickers), started_perf, args.progress_every, args.progress_log, forced=len(results) == len(tickers), resumed=True)
            continue
        if idx > 0 and not args.no_network:
            delay = _next_sleep_seconds(sleep_min, sleep_max)
            if delay > 0:
                time.sleep(delay)
        results.append(
            audit_ticker(
                ticker,
                db_path=db_path,
                no_network=args.no_network,
                max_retries=args.max_retries,
                sleep_seconds=args.sleep_seconds,
                rate_limit_backoffs=rate_limit_backoffs,
            )
        )
        checkpoint = _build_payload(current_identity, tickers, results, args, started_at_utc, utc_now_text(), complete=len(results) == len(tickers))
        _write_outputs(args, checkpoint)
        _print_progress(results, len(tickers), started_perf, args.progress_every, args.progress_log, forced=len(results) == len(tickers), resumed=False)
    completed_at_utc = utc_now_text()
    payload = _build_payload(current_identity, tickers, results, args, started_at_utc, completed_at_utc, complete=True)
    _write_outputs(args, payload)
    return payload, 0


def _build_payload(
    current_identity: dict[str, Any],
    tickers: list[str],
    results: list[dict[str, Any]],
    args: argparse.Namespace,
    started_at_utc: str,
    completed_at_utc: str,
    *,
    complete: bool,
) -> dict[str, Any]:
    return {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "database_identity": current_identity,
        "arguments": {
            "tickers": tickers,
            "no_network": bool(args.no_network),
            "sleep_seconds": args.sleep_seconds,
            "sleep_min_seconds": args.sleep_min_seconds,
            "sleep_max_seconds": args.sleep_max_seconds,
            "rate_limit_backoff_seconds": args.rate_limit_backoff_seconds,
            "max_retries": args.max_retries,
            "random_seed": args.random_seed,
        },
        "checkpoint": {
            "complete": complete,
            "processed_tickers": len(results),
            "remaining_tickers": max(len(tickers) - len(results), 0),
            "last_ticker": results[-1]["ticker"] if results else None,
        },
        "summary": aggregate_results(results, total_tickers=len(tickers), started_at_utc=started_at_utc, completed_at_utc=completed_at_utc),
        "results": results,
    }


def _write_outputs(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    if args.output_json:
        _write_json_atomic(Path(args.output_json), payload)
    if args.output_csv:
        _write_csv_atomic(Path(args.output_csv), payload["results"])
    if args.output_summary_json:
        _write_json_atomic(Path(args.output_summary_json), payload["summary"])


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n")
        handle.flush()
    tmp.replace(path)


def _write_csv_atomic(path: Path, results: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PER_TICKER_FIELDS)
        writer.writeheader()
        writer.writerows([{field: row.get(field) for field in PER_TICKER_FIELDS} for row in results])
        handle.flush()
    tmp.replace(path)


def _print_progress(
    results: list[dict[str, Any]],
    total_tickers: int,
    started_perf: float,
    progress_every: int,
    progress_log: str | None,
    *,
    forced: bool,
    resumed: bool,
) -> None:
    processed = len(results)
    if processed == 0:
        return
    if not forced and (progress_every == 0 or processed % progress_every != 0):
        return
    elapsed = max(time.perf_counter() - started_perf, 0.001)
    rate = processed / elapsed
    remaining = max(total_tickers - processed, 0)
    eta_seconds = remaining / rate if rate > 0 else None
    success = sum(1 for row in results if row.get("source_status") not in {"SOURCE_FAILED", "PARSE_FAILED", "NOT_REQUESTED"})
    actual_history = sum(1 for row in results if (row.get("completed_qualifying_count") or 0) > 0)
    failures = sum(1 for row in results if row.get("source_status") in {"SOURCE_FAILED", "PARSE_FAILED"})
    capped = sum(1 for row in results if row.get("limit_was_capped"))
    prefix = "AUDIT_PROGRESS_RESUME" if resumed else "AUDIT_PROGRESS"
    message = (
        f"{prefix} processed={processed}/{total_tickers} success={success} actual_history={actual_history} "
        f"failures={failures} capped={capped} elapsed_seconds={round(elapsed, 1)} "
        f"eta_seconds={None if eta_seconds is None else round(eta_seconds, 1)}"
    )
    print(message, file=sys.stderr)
    if progress_log:
        _append_progress_log(Path(progress_log), message)


def _append_progress_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{utc_now_text()} {message}\n")


def _sleep_range(args: argparse.Namespace) -> tuple[float, float]:
    sleep_min = args.sleep_seconds if args.sleep_min_seconds is None else args.sleep_min_seconds
    sleep_max = args.sleep_seconds if args.sleep_max_seconds is None else args.sleep_max_seconds
    if sleep_min < 0 or sleep_max < 0:
        raise ValueError("SLEEP_RANGE_MUST_BE_NON_NEGATIVE")
    if sleep_min > sleep_max:
        raise ValueError("SLEEP_MIN_MUST_NOT_EXCEED_SLEEP_MAX")
    return sleep_min, sleep_max


def _next_sleep_seconds(sleep_min: float, sleep_max: float) -> float:
    if sleep_min == sleep_max:
        return sleep_min
    return random.uniform(sleep_min, sleep_max)


def _parse_backoffs(value: str) -> list[float]:
    backoffs: list[float] = []
    for raw_part in str(value).split(","):
        part = raw_part.strip()
        if not part:
            continue
        backoff = float(part)
        if backoff < 0:
            raise ValueError("RATE_LIMIT_BACKOFF_MUST_BE_NON_NEGATIVE")
        backoffs.append(backoff)
    return backoffs


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
