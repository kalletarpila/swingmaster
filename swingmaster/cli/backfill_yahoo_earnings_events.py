from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import shutil
import sqlite3
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.earnings_event_repo import (
    apply_earnings_event_upsert,
    count_events_for_ticker,
    plan_earnings_event_upsert,
    summary_to_dict,
    verify_earnings_event_table,
)
from swingmaster.fundamentals.earnings_events import (
    YAHOO_SOURCE,
    default_fundamentals_usa_db_path,
    fetch_yahoo_earnings_events,
    normalize_ticker,
    open_readonly_db,
    plan_earnings_history_range,
    plan_yahoo_earnings_limit,
    repository_root,
)
from swingmaster.infra.sqlite.db import get_connection


ARTIFACT_SCHEMA_VERSION = 1
BATCH_ELIGIBLE_CLASSIFICATIONS = {
    "BACKFILL_READY_FULL_HISTORY",
    "BACKFILL_READY_PARTIAL_MARGIN_ONLY",
    "BACKFILL_PARTIAL_ACTUAL_HISTORY",
}
FAILED_SOURCE_STATUSES = {"SOURCE_FAILED", "PARSE_FAILED"}
PER_TICKER_FIELDS = (
    "ticker",
    "classification",
    "fetched_record_count",
    "eligible_record_count",
    "inserted_count",
    "updated_count",
    "unchanged_count",
    "skipped_count",
    "duplicate_count",
    "transaction_status",
    "coverage_status",
    "source_status",
    "attempt_count",
    "elapsed_seconds",
    "error_type",
    "error_message",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Guarded resumable Yahoo earnings-event batch backfill")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--audit-json", required=True)
    parser.add_argument("--ticker", action="append", default=None)
    parser.add_argument("--tickers-file", default=None)
    parser.add_argument("--first-n", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--include-classification", action="append", default=None)
    parser.add_argument("--exclude-classification", action="append", default=None)
    parser.add_argument("--sleep-min-seconds", type=float, default=0.8)
    parser.add_argument("--sleep-max-seconds", type=float, default=1.4)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--rate-limit-backoff-seconds", default="30,60,120")
    parser.add_argument("--checkpoint-json", required=True)
    parser.add_argument("--summary-json", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--resume-from-json", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--prebatch-backup", default=None)
    parser.add_argument("--skip-prebatch-backup", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def temp_root() -> Path:
    return repository_root() / "temp"


def validate_temp_path(path: Path, *, must_exist: bool = False) -> Path:
    resolved = path.expanduser().resolve()
    root = temp_root().resolve()
    if must_exist and not resolved.exists():
        raise ValueError(f"PATH_DOES_NOT_EXIST:{resolved}")
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"RUNTIME_PATH_OUTSIDE_TEMP:{resolved}") from exc
    return resolved


def db_identity(db_path: Path) -> dict[str, Any]:
    stat = db_path.stat()
    return {"path": str(db_path.resolve()), "size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def file_identity(path: Path) -> dict[str, Any]:
    resolved = validate_temp_path(path, must_exist=True)
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    stat = resolved.stat()
    return {"path": str(resolved), "size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns, "sha256": digest.hexdigest()}


def load_audit_payload(path: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    resolved = validate_temp_path(path, must_exist=True)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("INVALID_AUDIT_RESULTS")
    rows = {normalize_ticker(str(row["ticker"])): dict(row) for row in results if "ticker" in row}
    return payload, rows


def select_candidates(args: argparse.Namespace, audit_rows: dict[str, dict[str, Any]]) -> list[str]:
    include = set(args.include_classification or BATCH_ELIGIBLE_CLASSIFICATIONS)
    exclude = set(args.exclude_classification or ())
    if args.ticker:
        selected = [normalize_ticker(ticker) for ticker in args.ticker]
    elif args.tickers_file:
        path = validate_temp_path(Path(args.tickers_file), must_exist=True)
        selected = [normalize_ticker(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        selected = []
        for ticker, row in audit_rows.items():
            classification = str(row.get("planning_classification") or "")
            completed = int(row.get("completed_qualifying_count") or 0)
            if classification in include and classification not in exclude and completed > 0:
                selected.append(ticker)
    selected = sorted(dict.fromkeys(selected))
    selected = [
        ticker
        for ticker in selected
        if ticker in audit_rows
        and str(audit_rows[ticker].get("planning_classification") or "") in include
        and str(audit_rows[ticker].get("planning_classification") or "") not in exclude
        and int(audit_rows[ticker].get("completed_qualifying_count") or 0) > 0
    ]
    if args.sample_size is not None:
        if args.sample_size < 0:
            raise ValueError("SAMPLE_SIZE_MUST_BE_NON_NEGATIVE")
        rng = random.Random(args.random_seed)
        selected = sorted(rng.sample(selected, min(args.sample_size, len(selected))))
    if args.first_n is not None:
        if args.first_n < 0:
            raise ValueError("FIRST_N_MUST_BE_NON_NEGATIVE")
        selected = selected[: args.first_n]
    return selected


def run_batch(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    _validate_args(args)
    apply_mode = bool(args.apply)
    execution_mode = "apply" if apply_mode else "dry-run"
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    audit_payload, audit_rows = load_audit_payload(Path(args.audit_json))
    audit_identity = file_identity(Path(args.audit_json))
    selected = select_candidates(args, audit_rows)
    checkpoint_path = validate_temp_path(Path(args.checkpoint_json))
    summary_path = validate_temp_path(Path(args.summary_json))
    csv_path = validate_temp_path(Path(args.output_csv))
    current_db_identity = db_identity(db_path)
    resume_rows: dict[str, dict[str, Any]] = {}
    resume_payload: dict[str, Any] | None = None
    if args.resume_from_json:
        resume_payload = _load_resume(Path(args.resume_from_json), current_db_identity, audit_identity, execution_mode)
        resume_rows = {normalize_ticker(str(row["ticker"])): dict(row) for row in resume_payload.get("per_ticker_results", [])}

    pre_counts = database_counts(db_path)
    backup_info = _prepare_backup(args, db_path, pre_counts, apply_mode, resume_payload)
    run_id = (resume_payload or {}).get("run_id") or datetime.now(timezone.utc).strftime("YAHOO_EARNINGS_BATCH_%Y%m%dT%H%M%SZ")
    started_at = (resume_payload or {}).get("started_at_utc") or utc_now_text()
    started_perf = time.perf_counter()
    results_by_ticker = {ticker: row for ticker, row in resume_rows.items() if ticker in selected}
    exit_code = 0

    for index, ticker in enumerate(selected):
        previous = results_by_ticker.get(ticker)
        if previous is not None and previous.get("transaction_status") in {"DRY_RUN", "COMMITTED"} and previous.get("source_status") not in FAILED_SOURCE_STATUSES:
            _write_checkpoint(checkpoint_path, summary_path, csv_path, run_id, current_db_identity, audit_identity, backup_info, execution_mode, selected, results_by_ticker, started_at)
            continue
        if index > 0 and args.sleep_max_seconds > 0:
            time.sleep(_jitter(args.sleep_min_seconds, args.sleep_max_seconds))
        row, stop = process_ticker(
            ticker,
            classification=str(audit_rows[ticker].get("planning_classification") or ""),
            db_path=db_path,
            apply_mode=apply_mode,
            max_retries=args.max_retries,
            sleep_seconds=args.sleep_min_seconds,
            rate_limit_backoffs=_parse_backoffs(args.rate_limit_backoff_seconds),
        )
        results_by_ticker[ticker] = row
        _write_checkpoint(checkpoint_path, summary_path, csv_path, run_id, current_db_identity, audit_identity, backup_info, execution_mode, selected, results_by_ticker, started_at)
        if stop:
            exit_code = 1
            break
    payload = build_payload(run_id, current_db_identity, audit_identity, backup_info, execution_mode, selected, results_by_ticker, started_at)
    payload["elapsed_seconds"] = round(time.perf_counter() - started_perf, 3)
    _write_checkpoint(checkpoint_path, summary_path, csv_path, run_id, current_db_identity, audit_identity, backup_info, execution_mode, selected, results_by_ticker, started_at, complete=True)
    return payload, exit_code


def _validate_args(args: argparse.Namespace) -> None:
    if args.dry_run and args.apply:
        raise ValueError("DRY_RUN_AND_APPLY_ARE_MUTUALLY_EXCLUSIVE")
    if args.sleep_min_seconds < 0 or args.sleep_max_seconds < 0 or args.sleep_min_seconds > args.sleep_max_seconds:
        raise ValueError("INVALID_SLEEP_RANGE")
    if args.max_retries < 0:
        raise ValueError("MAX_RETRIES_MUST_BE_NON_NEGATIVE")
    for attr in ("checkpoint_json", "summary_json", "output_csv"):
        validate_temp_path(Path(getattr(args, attr)))
    if args.prebatch_backup:
        validate_temp_path(Path(args.prebatch_backup), must_exist=args.skip_prebatch_backup)
    if args.skip_prebatch_backup and not args.prebatch_backup and not args.resume_from_json:
        raise ValueError("SKIP_PREBATCH_BACKUP_REQUIRES_VERIFIED_BACKUP_CONTEXT")


def _load_resume(path: Path, db_id: dict[str, Any], audit_id: dict[str, Any], execution_mode: str) -> dict[str, Any]:
    resolved = validate_temp_path(path, must_exist=True)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if payload.get("artifact_schema_version") != ARTIFACT_SCHEMA_VERSION:
        raise ValueError("INCOMPATIBLE_CHECKPOINT_SCHEMA_VERSION")
    if (payload.get("database_identity") or {}).get("path") != db_id.get("path"):
        raise ValueError("INCOMPATIBLE_CHECKPOINT_DATABASE_IDENTITY")
    if (payload.get("audit_artifact_identity") or {}).get("sha256") != audit_id.get("sha256"):
        raise ValueError("INCOMPATIBLE_CHECKPOINT_AUDIT_ARTIFACT")
    if payload.get("execution_mode") != execution_mode:
        raise ValueError("INCOMPATIBLE_CHECKPOINT_EXECUTION_MODE")
    return payload


def _prepare_backup(args: argparse.Namespace, db_path: Path, pre_counts: dict[str, int], apply_mode: bool, resume_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not apply_mode:
        return {"path": None, "verified": False, "created": False}
    if resume_payload and resume_payload.get("prebatch_backup_verified"):
        backup_path = validate_temp_path(Path(str(resume_payload.get("prebatch_backup_path"))), must_exist=True)
        return {"path": str(backup_path), "verified": True, "created": False}
    if args.skip_prebatch_backup:
        backup_path = validate_temp_path(Path(str(args.prebatch_backup)), must_exist=True)
        _verify_backup(backup_path, pre_counts)
        return {"path": str(backup_path), "verified": True, "created": False}
    backup_path = validate_temp_path(Path(args.prebatch_backup)) if args.prebatch_backup else _default_backup_path(db_path)
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as src, sqlite3.connect(str(backup_path)) as dst:
        src.backup(dst)
    _verify_backup(backup_path, pre_counts)
    return {"path": str(backup_path), "verified": True, "created": True}


def _default_backup_path(db_path: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return validate_temp_path(temp_root() / "yahoo_earnings_batch_backfill" / timestamp / "backups" / f"{db_path.name}.{timestamp}.bak")


def _verify_backup(path: Path, expected_counts: dict[str, int]) -> None:
    if path.stat().st_size <= 0:
        raise RuntimeError("BACKUP_FILE_EMPTY")
    counts = database_counts(path)
    if counts != expected_counts:
        raise RuntimeError(f"BACKUP_COUNT_MISMATCH expected={expected_counts} actual={counts}")


def database_counts(path: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
        if quick != "ok":
            raise RuntimeError(f"SQLITE_QUICK_CHECK_FAILED:{quick}")
        return {
            "quarterly_rows": int(conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]),
            "event_rows": int(conn.execute("SELECT COUNT(*) FROM rc_earnings_event").fetchone()[0]),
            "duplicate_natural_keys": int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM (
                      SELECT market, ticker, announcement_at, source, COUNT(*) AS c
                      FROM rc_earnings_event
                      GROUP BY market, ticker, announcement_at, source
                      HAVING c > 1
                    )
                    """
                ).fetchone()[0]
            ),
        }


def process_ticker(
    ticker: str,
    *,
    classification: str,
    db_path: Path,
    apply_mode: bool,
    max_retries: int,
    sleep_seconds: float,
    rate_limit_backoffs: list[float],
) -> tuple[dict[str, Any], bool]:
    started = time.perf_counter()
    with open_readonly_db(db_path) as conn:
        verify_earnings_event_table(conn)
        range_plan = plan_earnings_history_range(conn, ticker)
    if range_plan.fetch_lower_bound is None:
        return _failure_row(ticker, classification, "NO_FUNDAMENTALS_HISTORY", None, 0, started), False
    limit_plan = plan_yahoo_earnings_limit(range_plan.fetch_lower_bound)
    source_result = None
    attempt_count = 0
    for attempt in range(max_retries + 1):
        attempt_count = attempt + 1
        source_result = fetch_yahoo_earnings_events(ticker=ticker, range_plan=range_plan, limit_plan=limit_plan, include_future=False)
        if source_result.status not in FAILED_SOURCE_STATUSES:
            break
        if attempt < max_retries:
            error_type = classify_error(source_result.status, source_result.error_message)
            backoff = rate_limit_backoffs[attempt] if error_type == "RATE_LIMIT" and attempt < len(rate_limit_backoffs) else max(sleep_seconds, 0.0) * (attempt + 1)
            if backoff > 0:
                time.sleep(backoff)
    if source_result is None or source_result.status in FAILED_SOURCE_STATUSES:
        return _failure_row(ticker, classification, source_result.status if source_result else "SOURCE_FAILED", None if source_result is None else source_result.error_message, attempt_count, started), False
    try:
        if apply_mode:
            with get_connection(str(db_path)) as conn:
                conn.execute("PRAGMA busy_timeout=5000")
                before = count_events_for_ticker(conn, ticker=source_result.normalized_ticker)
                apply_summary = apply_earnings_event_upsert(conn, source_result.records, ticker=source_result.normalized_ticker, applied_at_utc=utc_now_text())
                after = count_events_for_ticker(conn, ticker=source_result.normalized_ticker)
            if after != before + apply_summary.inserted_count:
                row = _result_row(ticker, classification, source_result, apply_summary, attempt_count, started)
                row["transaction_status"] = "VERIFICATION_FAILED"
                row["error_type"] = "DB_VERIFICATION_FAILED"
                row["error_message"] = f"PERSISTED_COUNT_MISMATCH expected={before + apply_summary.inserted_count} actual={after}"
                return row, True
        else:
            with open_readonly_db(db_path) as conn:
                apply_summary = plan_earnings_event_upsert(conn, source_result.records, ticker=source_result.normalized_ticker, dry_run=True)
        return _result_row(ticker, classification, source_result, apply_summary, attempt_count, started), False
    except Exception as exc:
        row = _failure_row(ticker, classification, "DB_TRANSACTION_FAILED", str(exc), attempt_count, started)
        row["transaction_status"] = "FAILED"
        return row, True


def _result_row(ticker: str, classification: str, source_result: Any, apply_summary: Any, attempt_count: int, started: float) -> dict[str, Any]:
    summary = summary_to_dict(apply_summary)
    return {
        "ticker": normalize_ticker(ticker),
        "classification": classification,
        "fetched_record_count": summary["fetched_record_count"],
        "eligible_record_count": summary["eligible_record_count"],
        "inserted_count": summary["inserted_count"],
        "updated_count": summary["updated_count"],
        "unchanged_count": summary["unchanged_count"],
        "skipped_count": summary["skipped_count"],
        "duplicate_count": summary["duplicate_count"],
        "transaction_status": summary["transaction_status"],
        "coverage_status": source_result.coverage.coverage_status,
        "source_status": source_result.status,
        "attempt_count": attempt_count,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "error_type": None,
        "error_message": None,
    }


def _failure_row(ticker: str, classification: str, source_status: str, error_message: str | None, attempt_count: int, started: float) -> dict[str, Any]:
    return {
        "ticker": normalize_ticker(ticker),
        "classification": classification,
        "fetched_record_count": 0,
        "eligible_record_count": 0,
        "inserted_count": 0,
        "updated_count": 0,
        "unchanged_count": 0,
        "skipped_count": 0,
        "duplicate_count": 0,
        "transaction_status": "NOT_STARTED",
        "coverage_status": source_status,
        "source_status": source_status,
        "attempt_count": attempt_count,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "error_type": classify_error(source_status, error_message),
        "error_message": error_message,
    }


def classify_error(source_status: str, error_message: str | None) -> str | None:
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


def _jitter(min_seconds: float, max_seconds: float) -> float:
    if min_seconds == max_seconds:
        return min_seconds
    return random.uniform(min_seconds, max_seconds)


def _parse_backoffs(value: str) -> list[float]:
    backoffs: list[float] = []
    for raw in str(value).split(","):
        item = raw.strip()
        if not item:
            continue
        parsed = float(item)
        if parsed < 0:
            raise ValueError("RATE_LIMIT_BACKOFF_MUST_BE_NON_NEGATIVE")
        backoffs.append(parsed)
    return backoffs


def build_payload(
    run_id: str,
    db_id: dict[str, Any],
    audit_id: dict[str, Any],
    backup_info: dict[str, Any],
    execution_mode: str,
    selected: list[str],
    results_by_ticker: dict[str, dict[str, Any]],
    started_at: str,
) -> dict[str, Any]:
    ordered_results = [results_by_ticker[ticker] for ticker in selected if ticker in results_by_ticker]
    successful = [row for row in ordered_results if row.get("transaction_status") in {"DRY_RUN", "COMMITTED"} and row.get("source_status") not in FAILED_SOURCE_STATUSES]
    failed = [row for row in ordered_results if row.get("source_status") in FAILED_SOURCE_STATUSES or row.get("transaction_status") in {"FAILED", "VERIFICATION_FAILED"}]
    summary = {
        "selected_ticker_count": len(selected),
        "completed_tickers": len(ordered_results),
        "successful_tickers": len(successful),
        "failed_tickers": len(failed),
        "eligible_record_count": sum(int(row.get("eligible_record_count") or 0) for row in ordered_results),
        "inserted_count": sum(int(row.get("inserted_count") or 0) for row in ordered_results),
        "updated_count": sum(int(row.get("updated_count") or 0) for row in ordered_results),
        "unchanged_count": sum(int(row.get("unchanged_count") or 0) for row in ordered_results),
        "skipped_count": sum(int(row.get("skipped_count") or 0) for row in ordered_results),
        "duplicate_count": sum(int(row.get("duplicate_count") or 0) for row in ordered_results),
        "source_failures": sum(1 for row in ordered_results if row.get("source_status") == "SOURCE_FAILED"),
        "parse_failures": sum(1 for row in ordered_results if row.get("source_status") == "PARSE_FAILED"),
        "classifications": _counts_by(ordered_results, "classification"),
    }
    return {
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "run_id": run_id,
        "database_identity": db_id,
        "audit_artifact_identity": audit_id,
        "prebatch_backup_path": backup_info.get("path"),
        "prebatch_backup_verified": bool(backup_info.get("verified")),
        "execution_mode": execution_mode,
        "selected_ticker_count": len(selected),
        "completed_tickers": [row["ticker"] for row in ordered_results],
        "successful_tickers": [row["ticker"] for row in successful],
        "failed_tickers": [row["ticker"] for row in failed],
        "summary": summary,
        "per_ticker_results": ordered_results,
        "started_at_utc": started_at,
        "last_checkpoint_at_utc": utc_now_text(),
    }


def _counts_by(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(field)
        if value:
            counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _write_checkpoint(
    checkpoint_path: Path,
    summary_path: Path,
    csv_path: Path,
    run_id: str,
    db_id: dict[str, Any],
    audit_id: dict[str, Any],
    backup_info: dict[str, Any],
    execution_mode: str,
    selected: list[str],
    results_by_ticker: dict[str, dict[str, Any]],
    started_at: str,
    *,
    complete: bool = False,
) -> None:
    payload = build_payload(run_id, db_id, audit_id, backup_info, execution_mode, selected, results_by_ticker, started_at)
    payload["checkpoint_complete"] = complete
    _write_json_atomic(checkpoint_path, payload)
    _write_json_atomic(summary_path, payload["summary"])
    _write_csv_atomic(csv_path, payload["per_ticker_results"])


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n")
        handle.flush()
    tmp.replace(path)


def _write_csv_atomic(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PER_TICKER_FIELDS)
        writer.writeheader()
        writer.writerows([{field: row.get(field) for field in PER_TICKER_FIELDS} for row in rows])
        handle.flush()
    tmp.replace(path)


def restore_readiness_copy(backup_path: Path, destination: Path) -> dict[str, int]:
    backup = validate_temp_path(backup_path, must_exist=True)
    target = validate_temp_path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(backup, target)
    return database_counts(target)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload, exit_code = run_batch(args)
    except Exception as exc:
        error = {"status": "ERROR", "error_type": type(exc).__name__, "error_message": str(exc)}
        if args.json_output:
            print(json.dumps(error, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
        else:
            print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if args.json_output:
        print(json.dumps(payload["summary"], sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        for key, value in payload["summary"].items():
            print(f"SUMMARY {key}={value}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
