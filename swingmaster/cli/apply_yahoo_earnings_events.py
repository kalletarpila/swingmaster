from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
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
    DEFAULT_SAFETY_MARGIN_DAYS,
    assess_earnings_coverage,
    default_fundamentals_usa_db_path,
    fetch_yahoo_earnings_events,
    open_readonly_db,
    plan_earnings_history_range,
    plan_yahoo_earnings_limit,
)
from swingmaster.infra.sqlite.db import get_connection


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Guarded single-ticker Yahoo earnings-event apply")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--safety-margin-days", type=int, default=DEFAULT_SAFETY_MARGIN_DAYS)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--include-future", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    parser.add_argument("--backup", default=None, help="Optional backup file or directory for apply mode")
    return parser.parse_args(argv)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def create_sqlite_backup(db_path: Path, backup_arg: str | None = None) -> Path:
    source_path = db_path.expanduser().resolve()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if backup_arg:
        requested = Path(backup_arg).expanduser()
        backup_path = requested / f"{source_path.name}.{timestamp}.bak" if requested.suffix == "" else requested
    else:
        backup_path = source_path.with_name(f"{source_path.name}.{timestamp}.bak")
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(source_path)) as src, sqlite3.connect(str(backup_path)) as dst:
        src.backup(dst)
    return backup_path.resolve()


def build_apply_summary(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if args.dry_run and args.apply:
        raise ValueError("DRY_RUN_AND_APPLY_ARE_MUTUALLY_EXCLUSIVE")
    apply_mode = bool(args.apply)
    dry_run = not apply_mode
    backup_already_created = bool(getattr(args, "backup_already_created", False))
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    backup_path: Path | None = None

    with open_readonly_db(db_path) as conn:
        verify_earnings_event_table(conn)
        range_plan = plan_earnings_history_range(
            conn,
            args.ticker,
            safety_margin_days=args.safety_margin_days,
            manual_start_date=args.start_date,
        )
        if range_plan.status == "NO_FUNDAMENTALS_HISTORY" and args.start_date is None:
            coverage = assess_earnings_coverage((), range_plan=range_plan)
            return {
                "mode": "dry-run" if dry_run else "apply",
                "database_path": str(db_path),
                "backup_path": None,
                "range_plan": asdict(range_plan),
                "limit_plan": None,
                "source": None,
                "coverage": asdict(coverage),
                "apply_summary": None,
                "persisted_count_after": 0,
            }, 0

    if range_plan.fetch_lower_bound is None:
        raise RuntimeError("FETCH_LOWER_BOUND_NOT_AVAILABLE")
    limit_plan = plan_yahoo_earnings_limit(range_plan.fetch_lower_bound, manual_limit=args.limit)
    source_result = fetch_yahoo_earnings_events(
        ticker=args.ticker,
        range_plan=range_plan,
        limit_plan=limit_plan,
        include_future=args.include_future,
    )
    if source_result.status in {"SOURCE_FAILED", "PARSE_FAILED"}:
        summary = _summary_payload(db_path, None, range_plan, limit_plan, source_result, None, "apply" if apply_mode else "dry-run", None)
        return summary, 1

    if dry_run:
        with open_readonly_db(db_path) as conn:
            apply_summary = plan_earnings_event_upsert(
                conn,
                source_result.records,
                ticker=source_result.normalized_ticker,
                dry_run=True,
            )
            persisted_count_after = count_events_for_ticker(conn, ticker=source_result.normalized_ticker)
        return _summary_payload(db_path, None, range_plan, limit_plan, source_result, apply_summary, "dry-run", persisted_count_after), 0

    if not backup_already_created:
        backup_path = create_sqlite_backup(db_path, args.backup)
    before_count = 0
    after_count = 0
    with get_connection(str(db_path)) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        verify_earnings_event_table(conn)
        before_count = count_events_for_ticker(conn, ticker=source_result.normalized_ticker)
        apply_summary = apply_earnings_event_upsert(
            conn,
            source_result.records,
            ticker=source_result.normalized_ticker,
            applied_at_utc=utc_now_text(),
        )
        after_count = count_events_for_ticker(conn, ticker=source_result.normalized_ticker)
    expected_after = before_count + apply_summary.inserted_count
    if after_count != expected_after:
        payload = _summary_payload(db_path, backup_path, range_plan, limit_plan, source_result, apply_summary, "apply", after_count)
        payload["verification_error"] = f"PERSISTED_COUNT_MISMATCH expected={expected_after} actual={after_count}"
        return payload, 1
    return _summary_payload(db_path, backup_path, range_plan, limit_plan, source_result, apply_summary, "apply", after_count), 0


def _summary_payload(
    db_path: Path,
    backup_path: Path | None,
    range_plan: Any,
    limit_plan: Any,
    source_result: Any,
    apply_summary: Any,
    mode: str,
    persisted_count_after: int | None,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "database_path": str(db_path),
        "backup_path": None if backup_path is None else str(backup_path),
        "range_plan": asdict(range_plan),
        "limit_plan": asdict(limit_plan),
        "source": {
            "status": source_result.status,
            "error_message": source_result.error_message,
            "source_observed_at_utc": source_result.source_observed_at_utc,
            "requested_limit": source_result.requested_limit,
            "raw_yahoo_row_count": source_result.diagnostics.raw_row_count,
            "completed_qualifying_count": source_result.diagnostics.completed_qualifying_count,
            "future_unreported_count": source_result.diagnostics.unreported_count,
            "duplicate_count": source_result.diagnostics.duplicate_count,
            "invalid_count": source_result.diagnostics.invalid_count,
            "actual_columns": list(source_result.diagnostics.actual_columns),
        },
        "coverage": asdict(source_result.coverage),
        "apply_summary": None if apply_summary is None else summary_to_dict(apply_summary),
        "persisted_count_after": persisted_count_after,
    }


def print_text(summary: dict[str, Any]) -> None:
    for key in ("mode", "database_path", "backup_path", "persisted_count_after"):
        print(f"{key}: {summary.get(key)}")
    for section in ("range_plan", "limit_plan", "source", "coverage", "apply_summary"):
        print(f"{section}:")
        value = summary.get(section)
        if isinstance(value, dict):
            for key, item in value.items():
                print(f"  {key}: {item}")
        else:
            print(f"  {value}")
    if "verification_error" in summary:
        print(f"verification_error: {summary['verification_error']}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary, exit_code = build_apply_summary(args)
    except Exception as exc:
        if args.json_output:
            print(json.dumps({"status": "ERROR", "error_type": type(exc).__name__, "error_message": str(exc)}, sort_keys=True, separators=(",", ":")))
        else:
            print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if args.json_output:
        print(json.dumps(summary, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        print_text(summary)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
