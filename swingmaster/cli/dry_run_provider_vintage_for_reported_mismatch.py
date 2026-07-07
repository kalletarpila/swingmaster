from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS
from swingmaster.fundamentals.reported_sec_latest_writer_vintage import (
    build_latest_writer_sec_vintage_candidate,
)
from swingmaster.fundamentals.reported_vintage_reader import get_pit_quarterly_vintage


DEBT_COMPONENT_FIELDS = (
    "LongTermDebtCurrent",
    "LongTermDebtNoncurrent",
    "ShortTermBorrowings",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run a provider-derived vintage candidate for one reported mismatch")
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--period-end-date", required=True)
    parser.add_argument("--available-at-utc", required=True)
    parser.add_argument("--ingested-at-utc", required=True)
    parser.add_argument("--vintage-run-id", required=True)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--sample-limit", type=int, default=20)
    return parser.parse_args(argv)


def _connect_read_only(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.expanduser().resolve()
    conn = sqlite3.connect(f"file:{resolved.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _rows_to_dicts(rows: list[sqlite3.Row], sample_limit: int) -> list[dict[str, Any]]:
    return [_row_to_dict(row) or {} for row in rows[:sample_limit]]


def _latest_row(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> dict[str, Any] | None:
    return _row_to_dict(
        conn.execute(
            """
            SELECT *
            FROM rc_fundamental_quarterly
            WHERE ticker = ?
              AND period_end_date = ?
            """,
            (ticker, period_end_date),
        ).fetchone()
    )


def _sec_raw_rows(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_statement_raw
        WHERE ticker = ?
          AND period_end_date = ?
          AND source = 'sec_edgar'
        ORDER BY statement_type ASC, field_name ASC, retrieved_at_utc ASC
        """,
        (ticker, period_end_date),
    ).fetchall()
    return [_row_to_dict(row) or {} for row in rows]


def _existing_statement_id_count(conn: sqlite3.Connection, statement_vintage_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_vintage
        WHERE statement_vintage_id = ?
        """,
        (statement_vintage_id,),
    ).fetchone()
    return int(row[0] or 0)


def _value_equal(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def _mismatched_fields(latest: dict[str, Any], vintage: dict[str, Any] | None) -> list[str]:
    if vintage is None:
        return []
    fields = [*REPORTED_FINANCIAL_FIELDS, "currency"]
    return [field for field in fields if not _value_equal(latest.get(field), vintage.get(field))]


def _base_field_name(field_name: object) -> str:
    return str(field_name).split("|", 1)[0]


def _debt_component_values(sec_raw_rows: list[dict[str, Any]]) -> dict[str, float]:
    values: dict[str, float] = {}
    for row in sec_raw_rows:
        base = _base_field_name(row.get("field_name"))
        if base not in DEBT_COMPONENT_FIELDS or base in values:
            continue
        value = row.get("field_value")
        if value is not None:
            values[base] = float(value)
    return values


def _status(
    *,
    latest: dict[str, Any] | None,
    sec_raw_rows: list[dict[str, Any]],
    candidate: dict[str, Any] | None,
    duplicate_count: int,
) -> tuple[str, str]:
    if latest is None:
        return "DRY_RUN_BLOCKED_NO_LATEST_ROW", "No latest row exists for ticker/period."
    if not sec_raw_rows:
        return "DRY_RUN_BLOCKED_NO_SEC_EVIDENCE", "No SEC raw evidence exists for ticker/period."
    if candidate is None:
        return "UNKNOWN", "Candidate could not be built."
    if duplicate_count > 0:
        return "DRY_RUN_BLOCKED_DUPLICATE_VINTAGE", "Candidate statement_vintage_id already exists."
    unknown_count = int(candidate.get("unknown_provenance_count") or 0)
    if unknown_count > 0:
        return "DRY_RUN_READY_WITH_UNKNOWN_PROVENANCE", "Candidate built but has unknown provenance fields."
    return "DRY_RUN_READY", "Candidate built with complete SEC latest-writer provenance."


def _candidate_or_none(
    *,
    latest: dict[str, Any] | None,
    sec_raw_rows: list[dict[str, Any]],
    market: str,
    available_at_utc: str,
    ingested_at_utc: str,
    vintage_run_id: str,
) -> dict[str, Any] | None:
    if latest is None or not sec_raw_rows:
        return None
    return build_latest_writer_sec_vintage_candidate(
        latest_row=latest,
        sec_raw_rows=sec_raw_rows,
        market=market,
        available_at_utc=available_at_utc,
        ingested_at_utc=ingested_at_utc,
        vintage_run_id=vintage_run_id,
        source_run_id=str(latest.get("run_id") or ""),
    )


def run_dry_run(
    *,
    fundamentals_db: Path,
    market: str,
    ticker: str,
    period_end_date: str,
    available_at_utc: str,
    ingested_at_utc: str,
    vintage_run_id: str,
    sample_limit: int,
) -> dict[str, Any]:
    normalized_market = market.strip().lower()
    normalized_ticker = ticker.strip().upper()
    with _connect_read_only(fundamentals_db) as conn:
        latest = _latest_row(conn, normalized_ticker, period_end_date)
        visible_vintage = _row_to_dict(
            get_pit_quarterly_vintage(
                conn,
                normalized_ticker,
                period_end_date,
                available_at_utc,
                normalized_market,
            )
        )
        sec_rows = _sec_raw_rows(conn, normalized_ticker, period_end_date)
        candidate = _candidate_or_none(
            latest=latest,
            sec_raw_rows=sec_rows,
            market=normalized_market,
            available_at_utc=available_at_utc,
            ingested_at_utc=ingested_at_utc,
            vintage_run_id=vintage_run_id,
        )
        candidate_statement_id = str(candidate.get("statement_vintage_id")) if candidate else ""
        duplicate_count = _existing_statement_id_count(conn, candidate_statement_id) if candidate_statement_id else 0

    status, reason = _status(latest=latest, sec_raw_rows=sec_rows, candidate=candidate, duplicate_count=duplicate_count)
    debt_components = _debt_component_values(sec_rows)
    debt_sum = sum(debt_components.values()) if debt_components else None
    candidate_vintage_row = candidate.get("vintage_row") if candidate else None
    provenance_rows = list(candidate.get("provenance_rows", [])) if candidate else []
    unknown_fields = list(candidate.get("unknown_provenance_fields", [])) if candidate else []
    mismatches = _mismatched_fields(latest, visible_vintage) if latest else []
    return {
        "market": normalized_market,
        "ticker": normalized_ticker,
        "period_end_date": period_end_date,
        "available_at_utc": available_at_utc,
        "ingested_at_utc": ingested_at_utc,
        "vintage_run_id": vintage_run_id,
        "latest_value_total_debt": latest.get("total_debt") if latest else None,
        "visible_vintage_value_total_debt": visible_vintage.get("total_debt") if visible_vintage else None,
        "candidate_value_total_debt": candidate_vintage_row.get("total_debt") if candidate_vintage_row else None,
        "mismatched_fields": mismatches,
        "visible_vintage_statement_vintage_id": visible_vintage.get("statement_vintage_id") if visible_vintage else None,
        "visible_vintage_run_id": visible_vintage.get("run_id") if visible_vintage else None,
        "visible_vintage_available_at_utc": visible_vintage.get("available_at_utc") if visible_vintage else None,
        "total_debt_component_values": debt_components,
        "total_debt_component_sum": debt_sum,
        "planned_vintage_rows": 1 if candidate else 0,
        "planned_provenance_rows": len(provenance_rows),
        "unknown_provenance_fields": unknown_fields,
        "unknown_provenance_count": len(unknown_fields),
        "candidate_statement_vintage_id": candidate_statement_id,
        "candidate_source_hash": str(candidate.get("source_hash")) if candidate else "",
        "candidate_source_provider": candidate_vintage_row.get("source_provider") if candidate_vintage_row else None,
        "candidate_vintage_row": candidate_vintage_row,
        "candidate_provenance_rows_sample": provenance_rows[:sample_limit],
        "sec_raw_fact_count": len(sec_rows),
        "sec_raw_facts_sample": sec_rows[:sample_limit],
        "duplicate_candidate_statement_vintage_id_count": duplicate_count,
        "overall_status": status,
        "recommended_next_action": reason,
    }


def _print_text(summary: dict[str, Any]) -> None:
    for key, value in summary.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value, sort_keys=True)
        print(f"SUMMARY {key}={value}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = run_dry_run(
            fundamentals_db=Path(args.fundamentals_db),
            market=args.market,
            ticker=args.ticker,
            period_end_date=args.period_end_date,
            available_at_utc=args.available_at_utc,
            ingested_at_utc=args.ingested_at_utc,
            vintage_run_id=args.vintage_run_id,
            sample_limit=args.sample_limit,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(json.dumps({"summary": summary}, indent=2, sort_keys=True))
    else:
        _print_text(summary)
    return 0 if str(summary.get("overall_status", "")).startswith("DRY_RUN_READY") else 1


if __name__ == "__main__":
    raise SystemExit(main())
