from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from swingmaster.cli.dry_run_provider_vintage_for_reported_mismatch import run_dry_run
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)


APPROVAL_TOKEN = "USER_APPROVES_GIS_PROVIDER_VINTAGE_APPLY"
SEC_PROVIDER = "sec_edgar"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply one provider-derived vintage candidate for a reported mismatch")
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--period-end-date", required=True)
    parser.add_argument("--available-at-utc", required=True)
    parser.add_argument("--ingested-at-utc", required=True)
    parser.add_argument("--vintage-run-id", required=True)
    parser.add_argument("--expected-statement-vintage-id", required=True)
    parser.add_argument("--expected-vintage-count", type=int, required=True)
    parser.add_argument("--expected-provenance-count", type=int, required=True)
    parser.add_argument("--approval-token", required=True)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def apply_provider_vintage_for_reported_mismatch(
    *,
    fundamentals_db: Path,
    market: str,
    ticker: str,
    period_end_date: str,
    available_at_utc: str,
    ingested_at_utc: str,
    vintage_run_id: str,
    expected_statement_vintage_id: str,
    expected_vintage_count: int,
    expected_provenance_count: int,
    approval_token: str,
) -> dict[str, Any]:
    if approval_token != APPROVAL_TOKEN:
        raise ValueError("APPROVAL_REQUIRED")
    if expected_vintage_count != 1:
        raise ValueError(f"EXPECTED_VINTAGE_COUNT_UNSUPPORTED:{expected_vintage_count}")
    if expected_provenance_count <= 0:
        raise ValueError(f"EXPECTED_PROVENANCE_COUNT_INVALID:{expected_provenance_count}")

    db_path = fundamentals_db.expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"FUNDAMENTALS_DB_NOT_FOUND:{db_path}")

    dry_run_summary = run_dry_run(
        fundamentals_db=db_path,
        market=market,
        ticker=ticker,
        period_end_date=period_end_date,
        available_at_utc=available_at_utc,
        ingested_at_utc=ingested_at_utc,
        vintage_run_id=vintage_run_id,
        sample_limit=expected_provenance_count,
    )
    _validate_dry_run_gates(
        dry_run_summary,
        expected_statement_vintage_id=expected_statement_vintage_id,
        expected_vintage_count=expected_vintage_count,
        expected_provenance_count=expected_provenance_count,
    )
    vintage_row = dict(dry_run_summary["candidate_vintage_row"])
    provenance_rows = [dict(row) for row in dry_run_summary["candidate_provenance_rows_sample"]]
    before_counts = _counts(db_path)
    backup_path = _backup_sqlite_db(db_path, vintage_run_id)

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        try:
            _validate_write_gates(
                conn,
                ticker=str(dry_run_summary["ticker"]),
                market=str(dry_run_summary["market"]),
                period_end_date=str(dry_run_summary["period_end_date"]),
                statement_vintage_id=expected_statement_vintage_id,
                source_hash=str(dry_run_summary["candidate_source_hash"]),
            )
            vintage_rows_inserted = insert_quarterly_vintage_row(conn, vintage_row)
            provenance_rows_inserted = insert_quarterly_field_provenance_rows(conn, provenance_rows)
            if vintage_rows_inserted != expected_vintage_count:
                raise ValueError(
                    f"VINTAGE_INSERT_COUNT_MISMATCH:expected={expected_vintage_count}:actual={vintage_rows_inserted}"
                )
            if provenance_rows_inserted != expected_provenance_count:
                raise ValueError(
                    "PROVENANCE_INSERT_COUNT_MISMATCH:"
                    f"expected={expected_provenance_count}:actual={provenance_rows_inserted}"
                )
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()

    after_counts = _counts(db_path)
    return {
        "apply": {
            "applied": True,
            "backup_path": str(backup_path),
            "vintage_rows_inserted": vintage_rows_inserted,
            "provenance_rows_inserted": provenance_rows_inserted,
        },
        "before_counts": before_counts,
        "after_counts": after_counts,
        "count_deltas": {
            "latest": after_counts["latest"] - before_counts["latest"],
            "vintage": after_counts["vintage"] - before_counts["vintage"],
            "provenance": after_counts["provenance"] - before_counts["provenance"],
        },
        "dry_run_summary": _compact_dry_run_summary(dry_run_summary),
    }


def _validate_dry_run_gates(
    summary: dict[str, Any],
    *,
    expected_statement_vintage_id: str,
    expected_vintage_count: int,
    expected_provenance_count: int,
) -> None:
    checks = {
        "overall_status": summary.get("overall_status") == "DRY_RUN_READY",
        "planned_vintage_rows": summary.get("planned_vintage_rows") == expected_vintage_count,
        "planned_provenance_rows": summary.get("planned_provenance_rows") == expected_provenance_count,
        "unknown_provenance_count": summary.get("unknown_provenance_count") == 0,
        "candidate_statement_vintage_id": summary.get("candidate_statement_vintage_id") == expected_statement_vintage_id,
        "duplicate_candidate_statement_vintage_id_count": summary.get("duplicate_candidate_statement_vintage_id_count") == 0,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise ValueError("DRY_RUN_GATE_FAILED:" + ",".join(failed))
    if len(summary.get("candidate_provenance_rows_sample") or []) != expected_provenance_count:
        raise ValueError("DRY_RUN_PROVENANCE_ROWS_NOT_FULLY_LOADED")


def _validate_write_gates(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    market: str,
    period_end_date: str,
    statement_vintage_id: str,
    source_hash: str,
) -> None:
    if _count(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage WHERE statement_vintage_id = ?", (statement_vintage_id,)):
        raise ValueError("WRITE_GATE_FAILED_STATEMENT_VINTAGE_ID_EXISTS")
    same_source_hash = _count(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_vintage
        WHERE ticker = ?
          AND market = ?
          AND period_end_date = ?
          AND source_provider = ?
          AND source_hash = ?
        """,
        (ticker, market, period_end_date, SEC_PROVIDER, source_hash),
    )
    if same_source_hash:
        raise ValueError("WRITE_GATE_FAILED_PROVIDER_SOURCE_HASH_EXISTS")


def _backup_sqlite_db(db_path: Path, vintage_run_id: str) -> Path:
    safe_run_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in vintage_run_id)
    backup_path = db_path.with_suffix(db_path.suffix + f".provider_vintage_apply.{safe_run_id}.bak")
    if backup_path.exists():
        raise FileExistsError(f"BACKUP_ALREADY_EXISTS:{backup_path}")
    source = sqlite3.connect(str(db_path))
    try:
        dest = sqlite3.connect(str(backup_path))
        try:
            source.backup(dest)
        finally:
            dest.close()
    finally:
        source.close()
    return backup_path


def _counts(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(str(db_path)) as conn:
        return {
            "latest": _count(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly"),
            "vintage": _count(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage"),
            "provenance": _count(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance"),
        }


def _count(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0)


def _compact_dry_run_summary(summary: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "overall_status",
        "ticker",
        "market",
        "period_end_date",
        "latest_value_total_debt",
        "visible_vintage_value_total_debt",
        "candidate_value_total_debt",
        "candidate_statement_vintage_id",
        "candidate_source_hash",
        "planned_vintage_rows",
        "planned_provenance_rows",
        "unknown_provenance_count",
        "unknown_provenance_fields",
        "total_debt_component_sum",
        "total_debt_component_values",
        "duplicate_candidate_statement_vintage_id_count",
    )
    return {key: summary.get(key) for key in keys}


def _print_text(result: dict[str, Any]) -> None:
    for section, values in result.items():
        if isinstance(values, dict):
            for key, value in values.items():
                print(f"SUMMARY {section}.{key}={value}")
        else:
            print(f"SUMMARY {section}={values}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = apply_provider_vintage_for_reported_mismatch(
            fundamentals_db=Path(args.fundamentals_db),
            market=args.market,
            ticker=args.ticker,
            period_end_date=args.period_end_date,
            available_at_utc=args.available_at_utc,
            ingested_at_utc=args.ingested_at_utc,
            vintage_run_id=args.vintage_run_id,
            expected_statement_vintage_id=args.expected_statement_vintage_id,
            expected_vintage_count=args.expected_vintage_count,
            expected_provenance_count=args.expected_provenance_count,
            approval_token=args.approval_token,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
