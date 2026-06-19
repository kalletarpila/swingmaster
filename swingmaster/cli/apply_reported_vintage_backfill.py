from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from swingmaster.cli import dry_run_reported_vintage_backfill as dry_run
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)


APPLY_RUN_ID_PREFIX = "reported-vintage-legacy-backfill"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply guarded legacy reported vintage backfill")
    parser.add_argument("--fundamentals-db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", required=True, help="Market code, e.g. usa")
    parser.add_argument("--as-of-date", required=True, help="Latest quarterly period cutoff in YYYY-MM-DD format")
    parser.add_argument(
        "--legacy-availability-policy",
        required=True,
        choices=dry_run.AVAILABILITY_POLICY_CHOICES,
        help="Explicit legacy availability policy",
    )
    parser.add_argument("--legacy-available-at-utc", default=None, help="Required by live_safe_legacy_baseline")
    parser.add_argument("--legacy-availability-lag-days", type=int, default=None)
    parser.add_argument("--verified-availability-file", default=None)
    parser.add_argument("--expected-vintage-rows", type=int, required=True)
    parser.add_argument("--expected-provenance-rows", type=int, required=True)
    parser.add_argument("--confirm-write", action="store_true", help="Required to write the real DB")
    return parser.parse_args()


def open_write_db(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"SQLite database not found: {resolved}")
    uri = f"file:{quote(str(resolved))}?mode=rw"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def apply_backfill(
    fundamentals_db_path: Path,
    market: str,
    as_of_date: str,
    legacy_availability_policy: str,
    legacy_available_at_utc: str | None,
    legacy_availability_lag_days: int | None,
    verified_availability_file: Path | None,
    expected_vintage_rows: int,
    expected_provenance_rows: int,
    confirm_write: bool,
    apply_timestamp_utc: str | None = None,
) -> dict[str, Any]:
    if not confirm_write:
        raise ValueError("--confirm-write is required")
    if expected_vintage_rows < 0 or expected_provenance_rows < 0:
        raise ValueError("expected row counts must be non-negative")

    apply_timestamp_utc = apply_timestamp_utc or _utc_now()
    report = dry_run.run_dry_run(
        fundamentals_db_path=fundamentals_db_path,
        market=market,
        as_of_date=as_of_date,
        legacy_availability_policy=legacy_availability_policy,
        legacy_available_at_utc=legacy_available_at_utc,
        legacy_availability_lag_days=legacy_availability_lag_days,
        verified_availability_file=verified_availability_file,
    )
    _require_ready_report(report, expected_vintage_rows, expected_provenance_rows)

    market = dry_run.normalize_market(market)
    availability_policy = dry_run.build_availability_policy_context(
        policy=legacy_availability_policy,
        legacy_available_at_utc=legacy_available_at_utc,
        legacy_availability_lag_days=legacy_availability_lag_days,
        verified_availability_file=verified_availability_file,
    )
    run_id = f"{APPLY_RUN_ID_PREFIX}:{market}:{as_of_date}:{apply_timestamp_utc}"

    with open_write_db(fundamentals_db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            latest_rows = dry_run.load_latest_quarterly_rows(conn, None, None, as_of_date)
            vintage_rows_written = 0
            provenance_rows_written = 0
            for row in latest_rows:
                candidate = dry_run.build_candidate_vintage_preview(conn, row, market, run_id, availability_policy)
                if candidate["status"] != "PLANNED":
                    raise RuntimeError(f"APPLY_CANDIDATE_NOT_PLANNED:{candidate['skip_reason']}")
                vintage_row = build_vintage_row(row, candidate, run_id, apply_timestamp_utc)
                provenance_rows = build_provenance_rows(row, candidate, run_id, apply_timestamp_utc)
                vintage_rows_written += insert_quarterly_vintage_row(conn, vintage_row)
                provenance_rows_written += insert_quarterly_field_provenance_rows(conn, provenance_rows)
            if vintage_rows_written != expected_vintage_rows:
                raise RuntimeError(f"VINTAGE_ROWS_WRITTEN_MISMATCH:{vintage_rows_written}!={expected_vintage_rows}")
            if provenance_rows_written != expected_provenance_rows:
                raise RuntimeError(f"PROVENANCE_ROWS_WRITTEN_MISMATCH:{provenance_rows_written}!={expected_provenance_rows}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return {
        "status": "APPLY_COMPLETE",
        "fundamentals_db": str(fundamentals_db_path.expanduser().resolve()),
        "market": market,
        "as_of_date": as_of_date,
        "legacy_availability_policy": legacy_availability_policy,
        "legacy_available_at_utc": legacy_available_at_utc,
        "run_id": run_id,
        "vintage_rows_written": vintage_rows_written,
        "provenance_rows_written": provenance_rows_written,
        "dry_run_summary": report["summary"],
    }


def build_vintage_row(
    latest_row: dict[str, Any],
    candidate: dict[str, Any],
    run_id: str,
    apply_timestamp_utc: str,
) -> dict[str, Any]:
    row = {
        "ticker": candidate["ticker"],
        "market": candidate["market"],
        "period_end_date": candidate["period_end_date"],
        "statement_vintage_id": candidate["statement_vintage_id"],
        "source_provider": "UNKNOWN_LEGACY",
        "source_document_id": None,
        "source_hash": candidate["source_hash"],
        "revision_number": 1,
        "is_restated": 0,
        "supersedes_vintage_id": None,
        "availability_quality": candidate["availability_quality"],
        "filed_at_utc": None,
        "available_at_utc": candidate["available_at_utc"],
        "ingested_at_utc": apply_timestamp_utc,
        "provider_observed_at_utc": None,
        "run_id": run_id,
        "provider_run_id": None,
        "normalization_run_id": latest_row.get("run_id"),
        "enrichment_run_id": None,
        "currency": latest_row.get("currency"),
        "created_at_utc": apply_timestamp_utc,
        "updated_at_utc": None,
    }
    for field_name in dry_run.FINANCIAL_FIELDS:
        row[field_name] = latest_row.get(field_name)
    return row


def build_provenance_rows(
    latest_row: dict[str, Any],
    candidate: dict[str, Any],
    run_id: str,
    apply_timestamp_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field_name in dry_run.FINANCIAL_FIELDS:
        field_value = latest_row.get(field_name)
        if field_value is None:
            continue
        rows.append(
            {
                "ticker": candidate["ticker"],
                "market": candidate["market"],
                "period_end_date": candidate["period_end_date"],
                "statement_vintage_id": candidate["statement_vintage_id"],
                "field_name": field_name,
                "field_value": field_value,
                "source_provider": "UNKNOWN_LEGACY",
                "source_table": "rc_fundamental_quarterly",
                "source_row_ref": f"{candidate['market']}:{candidate['ticker']}:{candidate['period_end_date']}",
                "source_document_id": None,
                "source_hash": candidate["source_hash"],
                "provenance_role": "LEGACY_BASELINE",
                "merge_action": "LEGACY_BACKFILL_BASELINE",
                "old_value": None,
                "new_value": field_value,
                "available_at_utc": candidate["available_at_utc"],
                "created_at_utc": apply_timestamp_utc,
                "run_id": run_id,
                "enrichment_run_id": None,
            }
        )
    return rows


def _require_ready_report(report: dict[str, Any], expected_vintage_rows: int, expected_provenance_rows: int) -> None:
    summary = report["summary"]
    if summary["overall_status"] != dry_run.STATUS_DRY_RUN_READY:
        raise RuntimeError(f"DRY_RUN_NOT_READY:{summary['overall_status']}")
    if summary["planned_vintage_rows"] != expected_vintage_rows:
        raise RuntimeError(f"EXPECTED_VINTAGE_ROWS_MISMATCH:{summary['planned_vintage_rows']}!={expected_vintage_rows}")
    if summary["planned_provenance_rows"] != expected_provenance_rows:
        raise RuntimeError(
            f"EXPECTED_PROVENANCE_ROWS_MISMATCH:{summary['planned_provenance_rows']}!={expected_provenance_rows}"
        )
    if summary["blocked_rows"] != 0:
        raise RuntimeError(f"DRY_RUN_BLOCKED_ROWS:{summary['blocked_rows']}")
    if summary["requires_policy_decision_rows"] != 0:
        raise RuntimeError(f"DRY_RUN_POLICY_DECISION_ROWS:{summary['requires_policy_decision_rows']}")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    args = parse_args()
    try:
        summary = apply_backfill(
            fundamentals_db_path=Path(args.fundamentals_db),
            market=args.market,
            as_of_date=args.as_of_date,
            legacy_availability_policy=args.legacy_availability_policy,
            legacy_available_at_utc=args.legacy_available_at_utc,
            legacy_availability_lag_days=args.legacy_availability_lag_days,
            verified_availability_file=Path(args.verified_availability_file) if args.verified_availability_file else None,
            expected_vintage_rows=args.expected_vintage_rows,
            expected_provenance_rows=args.expected_provenance_rows,
            confirm_write=args.confirm_write,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
