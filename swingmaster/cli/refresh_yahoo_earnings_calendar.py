from __future__ import annotations

import argparse
import csv
import json
import random
import sqlite3
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_calendar import (
    SOURCE_YAHOO,
    _date_part,
    _is_completed_yahoo_row,
    new_york_today_from_utc,
    record_earnings_calendar_check_failure,
    select_future_yahoo_estimate,
    upsert_earnings_calendar,
)
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker
from swingmaster.fundamentals.quarter_completeness import utc_timestamp, validate_temp_path


class CalendarFetchError(RuntimeError):
    def __init__(self, status: str, message: str, attempt_rows: list[dict[str, Any]]) -> None:
        super().__init__(message)
        self.status = status
        self.attempt_rows = attempt_rows


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
    parser.add_argument("--max-retries", type=int, default=3, help="Total attempts per ticker")
    parser.add_argument("--rate-limit-backoff-seconds", default="30,60,120")
    parser.add_argument("--request-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-ticker-elapsed-seconds", type=float, default=90.0)
    parser.add_argument("--max-run-elapsed-seconds", type=float, default=None)
    parser.add_argument("--stop-after-consecutive-failures", type=int, default=25)
    parser.add_argument("--checkpoint-json", default=None)
    parser.add_argument("--summary-json", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--attempts-csv", default=None)
    parser.add_argument("--resume-from-json", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--backup", default=None)
    parser.add_argument("--backup-already-created", action="store_true", help=argparse.SUPPRESS)
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
    if args.request_timeout_seconds <= 0:
        raise ValueError("REQUEST_TIMEOUT_MUST_BE_POSITIVE")
    if args.max_retries < 1:
        raise ValueError("MAX_RETRIES_MUST_BE_AT_LEAST_ONE")
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    output_root = validate_temp_path(
        Path(args.output_root) if args.output_root else Path("temp") / "earnings_calendar_and_ingestion_status" / utc_timestamp()
    )
    checkpoint_json = validate_temp_path(Path(args.checkpoint_json)) if args.checkpoint_json else output_root / "calendar_checkpoint.json"
    summary_json = validate_temp_path(Path(args.summary_json)) if args.summary_json else output_root / "calendar_summary.json"
    output_csv = validate_temp_path(Path(args.output_csv)) if args.output_csv else output_root / "calendar_rows.csv"
    attempts_csv = validate_temp_path(Path(args.attempts_csv)) if args.attempts_csv else output_root / "calendar_attempts.csv"
    backup_path = validate_temp_path(Path(args.backup)) if args.backup else output_root / "backups" / f"{db_path.name}.pre_earnings_calendar.bak"
    if args.resume_from_json:
        validate_temp_path(Path(args.resume_from_json), must_exist=True)

    tickers = _select_tickers(db_path, args)
    if args.apply and args.backup_already_created:
        validate_temp_path(backup_path, must_exist=True)
    if args.apply and not args.backup_already_created:
        _backup(db_path, backup_path)
    if args.apply:
        run_migration(db_path)

    today_new_york = new_york_today_from_utc()
    rows: list[dict[str, Any]] = []
    attempt_rows: list[dict[str, Any]] = []
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
        "timeout_count": 0,
        "rate_limited_count": 0,
        "network_error_count": 0,
        "interrupted_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
        "unchanged_count": 0,
    }
    rng = random.Random(args.random_seed)
    backoffs = [float(item) for item in str(args.rate_limit_backoff_seconds).split(",") if item]
    started_monotonic = time.perf_counter()
    resumed = _load_resume(Path(args.resume_from_json)) if args.resume_from_json else {"rows": [], "attempt_rows": []}
    rows.extend(resumed["rows"])
    attempt_rows.extend(resumed["attempt_rows"])
    completed_tickers = {
        normalize_ticker(str(row["ticker"]))
        for row in rows
        if str(row.get("result_status") or row.get("calendar_status") or "").startswith("SUCCESS")
        or row.get("calendar_status") in {"UPCOMING", "DUE_TODAY", "DATE_PASSED_EVENT_NOT_FOUND", "COMPLETED_EVENT_FOUND", "NO_CURRENT_ESTIMATE"}
    }
    consecutive_failures = 0
    for index, ticker in enumerate(tickers, start=1):
        if ticker in completed_tickers:
            continue
        if args.max_run_elapsed_seconds is not None and time.perf_counter() - started_monotonic > args.max_run_elapsed_seconds:
            counts["partial_stop_reason"] = "MAX_RUN_ELAPSED_SECONDS"  # type: ignore[assignment]
            _write_checkpoint(checkpoint_json, counts, rows, attempt_rows)
            _write_json(summary_json, counts)
            _write_csv(output_csv, rows)
            _write_csv(attempts_csv, attempt_rows)
            return 2
        print(f"PROGRESS ticker={ticker} index={index}/{len(tickers)}", flush=True)
        observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        ticker_started = time.perf_counter()
        try:
            source_rows, result_status, attempts = _fetch_yahoo_rows(
                ticker,
                max_attempts=args.max_retries,
                backoffs=backoffs,
                request_timeout_seconds=args.request_timeout_seconds,
                max_ticker_elapsed_seconds=args.max_ticker_elapsed_seconds,
            )
            attempt_rows.extend(attempts)
            estimate = select_future_yahoo_estimate(source_rows, today_new_york=today_new_york, ticker=ticker)
            if result_status == "SUCCESS":
                result_status = _success_status(source_rows, estimate)
            counts["successful_source_tickers"] += 1
            counts["future_estimate_found_count" if estimate else "no_current_estimate_count"] += 1
            consecutive_failures = 0
        except KeyboardInterrupt:
            counts["interrupted_count"] += 1
            attempt_rows.append(_interrupted_attempt_row(ticker))
            _write_checkpoint(checkpoint_json, counts, rows, attempt_rows)
            _write_json(summary_json, counts)
            _write_csv(output_csv, rows)
            _write_csv(attempts_csv, attempt_rows)
            return 130
        except CalendarFetchError as exc:
            attempt_rows.extend(exc.attempt_rows)
            estimate = None
            result_status = exc.status
            _increment_failure_count(counts, result_status)
            if args.apply:
                with sqlite3.connect(str(db_path)) as conn:
                    record_earnings_calendar_check_failure(
                        conn,
                        market="usa",
                        ticker=ticker,
                        observed_at_utc=observed_at,
                        failure_status=result_status,
                    )
                    conn.commit()
            rows.append({"ticker": ticker, "result_status": result_status, "error": str(exc), "elapsed_seconds": round(time.perf_counter() - ticker_started, 3)})
            _write_checkpoint(checkpoint_json, counts, rows, attempt_rows)
            consecutive_failures += 1
            if consecutive_failures >= args.stop_after_consecutive_failures:
                counts["partial_stop_reason"] = "STOP_AFTER_CONSECUTIVE_FAILURES"  # type: ignore[assignment]
                _write_json(summary_json, counts)
                _write_csv(output_csv, rows)
                _write_csv(attempts_csv, attempt_rows)
                return 2
            continue
        except Exception as exc:  # pragma: no cover - live source defensive path
            estimate = None
            result_status = _classify_exception(exc)
            _increment_failure_count(counts, result_status)
            if args.apply:
                with sqlite3.connect(str(db_path)) as conn:
                    record_earnings_calendar_check_failure(
                        conn,
                        market="usa",
                        ticker=ticker,
                        observed_at_utc=observed_at,
                        failure_status=result_status,
                    )
                    conn.commit()
            rows.append({"ticker": ticker, "result_status": result_status, "error": str(exc), "elapsed_seconds": round(time.perf_counter() - ticker_started, 3)})
            _write_checkpoint(checkpoint_json, counts, rows, attempt_rows)
            consecutive_failures += 1
            if consecutive_failures >= args.stop_after_consecutive_failures:
                counts["partial_stop_reason"] = "STOP_AFTER_CONSECUTIVE_FAILURES"  # type: ignore[assignment]
                _write_json(summary_json, counts)
                _write_csv(output_csv, rows)
                _write_csv(attempts_csv, attempt_rows)
                return 2
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
            status = result_status
        _count_status(counts, status)
        rows.append(
            {
                "ticker": ticker,
                "calendar_status": status,
                "result_status": result_status,
                "estimated_announcement_at": estimate.estimated_announcement_at if estimate else None,
                "estimated_announcement_date": estimate.estimated_announcement_date if estimate else None,
                "source": SOURCE_YAHOO,
                "elapsed_seconds": round(time.perf_counter() - ticker_started, 3),
            }
        )
        _write_checkpoint(checkpoint_json, counts, rows, attempt_rows)
        if index < len(tickers):
            time.sleep(rng.uniform(args.sleep_min_seconds, args.sleep_max_seconds))

    _write_json(summary_json, counts)
    _write_csv(output_csv, rows)
    _write_csv(attempts_csv, attempt_rows)
    if args.json_output:
        print(json.dumps(counts, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        for key, value in counts.items():
            print(f"SUMMARY {key}={value}")
        print(f"ARTIFACT checkpoint_json={checkpoint_json}")
        print(f"ARTIFACT summary_json={summary_json}")
        print(f"ARTIFACT output_csv={output_csv}")
        print(f"ARTIFACT attempts_csv={attempts_csv}")
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


def _fetch_yahoo_rows(
    ticker: str,
    *,
    max_attempts: int,
    backoffs: list[float],
    request_timeout_seconds: float,
    max_ticker_elapsed_seconds: float,
) -> tuple[list[dict[str, Any]], str, list[dict[str, Any]]]:
    attempts: list[dict[str, Any]] = []
    ticker_started = time.perf_counter()
    for attempt in range(1, max_attempts + 1):
        if time.perf_counter() - ticker_started > max_ticker_elapsed_seconds:
            raise TimeoutError("MAX_TICKER_ELAPSED_SECONDS")
        started = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        perf_started = time.perf_counter()
        retry_sleep = 0.0
        try:
            source_rows = fetch_yahoo_earnings_calendar_rows(
                ticker,
                timeout=request_timeout_seconds,
                limit=12,
            )
            finished = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            attempts.append(
                _attempt_row(
                    ticker=ticker,
                    attempt=attempt,
                    started=started,
                    finished=finished,
                    elapsed=time.perf_counter() - perf_started,
                    result_status="SUCCESS",
                    source_rows=source_rows,
                    retry_sleep_seconds=0.0,
                )
            )
            return source_rows, "SUCCESS", attempts
        except Exception as exc:
            status = _classify_exception(exc)
            if attempt < max_attempts and status in {"TIMEOUT", "RATE_LIMITED", "NETWORK_ERROR", "SOURCE_ERROR"}:
                retry_sleep = _retry_sleep(status, attempt, backoffs)
            finished = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            attempts.append(
                _attempt_row(
                    ticker=ticker,
                    attempt=attempt,
                    started=started,
                    finished=finished,
                    elapsed=time.perf_counter() - perf_started,
                    result_status=status,
                    source_rows=[],
                    exception=exc,
                    retry_sleep_seconds=retry_sleep,
                )
            )
            if attempt >= max_attempts or status == "PARSE_ERROR":
                raise CalendarFetchError(status, str(exc), attempts) from exc
            if retry_sleep > 0:
                time.sleep(retry_sleep)
    raise RuntimeError("UNREACHABLE_YAHOO_FETCH_STATE")


def fetch_yahoo_earnings_calendar_rows(ticker: str, *, timeout: float, limit: int) -> list[dict[str, Any]]:
    import pandas as pd
    import yfinance as yf
    from bs4 import BeautifulSoup

    if limit > 100:
        raise ValueError("YAHOO_EARNINGS_LIMIT_TOO_HIGH")
    size = 25 if limit <= 25 else 50 if limit <= 50 else 100
    normalized = normalize_ticker(ticker)
    url = f"https://finance.yahoo.com/calendar/earnings?symbol={normalized}&offset=0&size={size}"
    response = yf.Ticker(normalized)._data.cache_get(url, timeout=timeout)
    if getattr(response, "status_code", 200) == 429:
        raise RuntimeError("YAHOO_RATE_LIMITED_429")
    text = getattr(response, "text", "")
    soup = BeautifulSoup(text, "html.parser")
    table = soup.find("table")
    if table is None:
        return []
    try:
        dataframe = pd.read_html(StringIO(str(table)), na_values=["-"])[0]
    except Exception as exc:
        raise ValueError(f"YAHOO_EARNINGS_PARSE_FAILED:{exc}") from exc
    if "Earnings Date" not in dataframe.columns:
        raise ValueError("YAHOO_EARNINGS_PARSE_FAILED:MISSING_EARNINGS_DATE")
    if "Symbol" in dataframe.columns:
        dataframe = dataframe.drop(["Symbol"], axis=1)
    if "Company" in dataframe.columns:
        dataframe = dataframe.drop(["Company"], axis=1)
    dataframe.rename(columns={"Surprise (%)": "Surprise(%)"}, inplace=True)
    dataframe = dataframe.dropna(subset="Earnings Date")
    return dataframe.to_dict("records")


def _backup(db_path: Path, backup_path: Path) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    if backup_path.exists():
        return
    with sqlite3.connect(str(db_path)) as source, sqlite3.connect(str(backup_path)) as target:
        source.backup(target)


def _calendar_row(conn: sqlite3.Connection, ticker: str) -> tuple[Any, ...] | None:
    return conn.execute(
        """
        SELECT calendar_status, estimated_announcement_at, date_change_count, completed_earnings_event_id
        FROM rc_earnings_calendar
        WHERE market = 'usa' AND ticker = ? AND source = ?
        """,
        (normalize_ticker(ticker), SOURCE_YAHOO),
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


def _success_status(source_rows: list[dict[str, Any]], estimate: Any) -> str:
    if estimate is not None:
        return "SUCCESS_FUTURE_ESTIMATE"
    completed = sum(1 for row in source_rows if _is_completed_yahoo_row(row))
    return "SUCCESS_COMPLETED_ONLY" if completed else "SUCCESS_NO_CURRENT_ESTIMATE"


def _classify_exception(exc: Exception) -> str:
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    if isinstance(exc, TimeoutError) or "timeout" in message or "timed out" in message:
        return "TIMEOUT"
    if "rate" in message or "429" in message or "too many" in message or "ratelimit" in name:
        return "RATE_LIMITED"
    if "parse" in message or isinstance(exc, ValueError):
        return "PARSE_ERROR"
    if "dns" in message or "resolve" in message or "connection" in message or "network" in message:
        return "NETWORK_ERROR"
    return "SOURCE_ERROR"


def _increment_failure_count(counts: dict[str, int], status: str) -> None:
    mapping = {
        "TIMEOUT": "timeout_count",
        "RATE_LIMITED": "rate_limited_count",
        "NETWORK_ERROR": "network_error_count",
        "PARSE_ERROR": "parse_failed_count",
        "SOURCE_ERROR": "source_failed_count",
        "INTERRUPTED": "interrupted_count",
    }
    counts[mapping.get(status, "source_failed_count")] += 1


def _retry_sleep(status: str, attempt: int, backoffs: list[float]) -> float:
    if status == "RATE_LIMITED" and backoffs:
        return backoffs[min(attempt - 1, len(backoffs) - 1)]
    if status in {"TIMEOUT", "NETWORK_ERROR", "SOURCE_ERROR"}:
        return float(min(5 * attempt, 15))
    return 0.0


def _attempt_row(
    *,
    ticker: str,
    attempt: int,
    started: str,
    finished: str,
    elapsed: float,
    result_status: str,
    source_rows: list[dict[str, Any]],
    retry_sleep_seconds: float,
    exception: Exception | None = None,
) -> dict[str, Any]:
    selected = None
    try:
        estimate = select_future_yahoo_estimate(source_rows, today_new_york=new_york_today_from_utc(), ticker=ticker)
        selected = estimate.estimated_announcement_at if estimate else None
    except Exception:
        selected = None
    return {
        "ticker": normalize_ticker(ticker),
        "attempt": attempt,
        "request_started_at_utc": started,
        "request_finished_at_utc": finished,
        "elapsed_seconds": round(elapsed, 3),
        "result_status": result_status,
        "future_rows_seen": _future_rows_seen(source_rows),
        "completed_rows_seen": sum(1 for row in source_rows if _is_completed_yahoo_row(row)),
        "selected_estimate": selected,
        "exception_type": type(exception).__name__ if exception else None,
        "exception_message": str(exception) if exception else None,
        "retry_sleep_seconds": retry_sleep_seconds,
    }


def _interrupted_attempt_row(ticker: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "ticker": normalize_ticker(ticker),
        "attempt": None,
        "request_started_at_utc": now,
        "request_finished_at_utc": now,
        "elapsed_seconds": 0,
        "result_status": "INTERRUPTED",
        "future_rows_seen": 0,
        "completed_rows_seen": 0,
        "selected_estimate": None,
        "exception_type": "KeyboardInterrupt",
        "exception_message": "INTERRUPTED",
        "retry_sleep_seconds": 0,
    }


def _future_rows_seen(source_rows: list[dict[str, Any]]) -> int:
    today = new_york_today_from_utc()
    count = 0
    for row in source_rows:
        value = _date_part(row.get("Earnings Date"))
        if value is not None and value >= today and not _is_completed_yahoo_row(row):
            count += 1
    return count


def _load_resume(path: Path) -> dict[str, list[dict[str, Any]]]:
    payload = json.loads(validate_temp_path(path, must_exist=True).read_text(encoding="utf-8"))
    return {
        "rows": [dict(row) for row in payload.get("rows", [])],
        "attempt_rows": [dict(row) for row in payload.get("attempt_rows", [])],
    }


def _write_checkpoint(path: Path, counts: dict[str, Any], rows: list[dict[str, Any]], attempt_rows: list[dict[str, Any]]) -> None:
    _write_json(path, {"summary": counts, "rows": rows, "attempt_rows": attempt_rows})


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not rows:
            return
        fieldnames = list(rows[0].keys())
        for row in rows[1:]:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
