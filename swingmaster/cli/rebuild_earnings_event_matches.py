from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_event_matching import DEFAULT_MAX_REPORTING_DELAY_DAYS
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.quarter_earnings_match_repo import (
    apply_rebuild,
    build_desired_matches,
    content_hash,
    create_verified_backup,
    database_counts,
    dry_run_rebuild,
    match_table_exists,
    representative_rows,
    temp_root,
    validate_temp_path,
    verify_match_table,
    write_json_atomic,
    write_outcomes_csv,
)


REPRESENTATIVE_TICKERS = ["AAPL", "MSFT", "JPM", "XOM", "NVDA", "GIS", "LMT", "BBY", "ARWR", "DGXX", "AVNS"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild persisted quarterly fundamentals to earnings-event matches")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--max-delay-days", type=int, default=DEFAULT_MAX_REPORTING_DELAY_DAYS)
    confidence = parser.add_mutually_exclusive_group()
    confidence.add_argument("--include-low-confidence", action="store_true", default=True)
    confidence.add_argument("--exclude-low-confidence", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", default=None)
    parser.add_argument("--checkpoint-json", default=None)
    parser.add_argument("--summary-json", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = run_cli(args)
        if args.json_output:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            _print_summary(payload["summary"])
            if payload.get("summary_json"):
                print(f"summary_json: {payload['summary_json']}")
            if payload.get("output_csv"):
                print(f"output_csv: {payload['output_csv']}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def run_cli(args: argparse.Namespace) -> dict[str, Any]:
    apply_mode = bool(args.apply)
    dry_run = not apply_mode
    include_low_confidence = not bool(args.exclude_low_confidence)
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    artifact_dir = _artifact_dir("apply" if apply_mode else "dry_run")
    checkpoint_json = validate_temp_path(Path(args.checkpoint_json)) if args.checkpoint_json else artifact_dir / "checkpoint.json"
    summary_json = validate_temp_path(Path(args.summary_json)) if args.summary_json else artifact_dir / "summary.json"
    output_csv = validate_temp_path(Path(args.output_csv)) if args.output_csv else artifact_dir / "matches.csv"
    backup_info: dict[str, Any] = {"path": None, "verified": False, "created": False}

    if apply_mode:
        backup_path = validate_temp_path(Path(args.backup)) if args.backup else artifact_dir.parent / "backups" / _backup_name(db_path)
        backup_info = create_verified_backup(db_path, backup_path)
        run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        source_counts_before = database_counts(db_path)
        before_hash = content_hash(conn)
        desired, outcomes = build_desired_matches(
            conn,
            max_delay_days=args.max_delay_days,
            include_low_confidence=include_low_confidence,
        )
        write_json_atomic(
            checkpoint_json,
            {
                "mode": "apply" if apply_mode else "dry-run",
                "database_path": str(db_path),
                "max_delay_days": args.max_delay_days,
                "include_low_confidence": include_low_confidence,
                "desired_match_count": len(desired),
                "outcome_count": len(outcomes),
            },
        )
        write_outcomes_csv(output_csv, outcomes)
        if dry_run:
            summary = dry_run_rebuild(
                conn,
                max_delay_days=args.max_delay_days,
                include_low_confidence=include_low_confidence,
            )
            source_counts_after = database_counts(db_path)
        else:
            summary = apply_rebuild(
                conn,
                max_delay_days=args.max_delay_days,
                include_low_confidence=include_low_confidence,
                backup_verified=bool(backup_info["verified"]),
            )
            source_counts_after = database_counts(db_path)
        after_hash = content_hash(conn)
        verification = (
            verify_match_table(conn, max_delay_days=args.max_delay_days)
            if match_table_exists(conn)
            else {"match_table_exists": False, "quick_check": str(conn.execute("PRAGMA quick_check").fetchone()[0])}
        )
        representative = representative_rows(conn, REPRESENTATIVE_TICKERS)

    payload = {
        "mode": "apply" if apply_mode else "dry-run",
        "database_path": str(db_path),
        "backup": backup_info,
        "checkpoint_json": str(checkpoint_json),
        "summary_json": str(summary_json),
        "output_csv": str(output_csv),
        "summary": summary,
        "source_counts_before": source_counts_before,
        "source_counts_after": source_counts_after,
        "content_hash_before": before_hash,
        "content_hash_after": after_hash,
        "verification": verification,
        "representative_tickers": representative,
    }
    write_json_atomic(summary_json, payload)
    return payload


def _artifact_dir(mode: str) -> Path:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")
    return validate_temp_path(temp_root() / "earnings_event_match_persistence" / timestamp / mode)


def _backup_name(db_path: Path) -> str:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")
    return f"{db_path.name}.{timestamp}.bak"


def _print_summary(summary: dict[str, Any]) -> None:
    for key, value in summary.items():
        print(f"SUMMARY {key}={value}")


if __name__ == "__main__":
    raise SystemExit(main())
