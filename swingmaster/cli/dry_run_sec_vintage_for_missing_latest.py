from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.build_quarterly import build_quarterly_rows
from swingmaster.fundamentals.reported_quarterly_dual_write import (
    REPORTED_FINANCIAL_FIELDS,
    build_field_provenance_rows,
    build_quarterly_vintage_row_from_latest,
)
from swingmaster.fundamentals.reported_sec_vintage_metadata import (
    build_sec_field_source_map,
    build_sec_vintage_metadata,
)
from swingmaster.fundamentals.sec_reconstruction_provenance import (
    reconstruct_quarterly_rows_with_provenance,
)


READY = "READY"
BLOCKED_NO_SEC_RAW = "BLOCKED_NO_SEC_RAW"
BLOCKED_RECONSTRUCTION_MISMATCH = "BLOCKED_RECONSTRUCTION_MISMATCH"
BLOCKED_INCOMPLETE_PROVENANCE = "BLOCKED_INCOMPLETE_PROVENANCE"
BLOCKED_DUPLICATE_VINTAGE = "BLOCKED_DUPLICATE_VINTAGE"
SKIPPED_ALREADY_HAS_VINTAGE = "SKIPPED_ALREADY_HAS_VINTAGE"
UNKNOWN = "UNKNOWN"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        result = run_dry_run(
            fundamentals_db=args.fundamentals_db,
            market=args.market,
            source_run_id=args.source_run_id,
            available_at_utc=args.available_at_utc,
            ingested_at_utc=args.ingested_at_utc,
            vintage_run_id=args.vintage_run_id,
            ticker=args.ticker,
            sample_limit=args.sample_limit,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_text(result))

    if args.fail_if_blocked and result["summary"]["blocked_rows"] > 0:
        return 1
    return 0


def run_dry_run(
    *,
    fundamentals_db: str,
    market: str,
    source_run_id: str | None,
    available_at_utc: str,
    ingested_at_utc: str,
    vintage_run_id: str,
    ticker: str | None = None,
    sample_limit: int = 20,
) -> dict[str, Any]:
    db_path = Path(fundamentals_db)
    if not db_path.exists():
        raise FileNotFoundError(f"FUNDAMENTALS_DB_NOT_FOUND:{fundamentals_db}")

    conn = _connect_read_only(db_path)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        latest_missing_vintage_rows = _count_latest_missing_vintage(conn, source_run_id, ticker)
        latest_rows = _load_latest_rows(conn, source_run_id, ticker)
        samples: list[dict[str, Any]] = []
        counters = {
            READY: 0,
            BLOCKED_NO_SEC_RAW: 0,
            BLOCKED_RECONSTRUCTION_MISMATCH: 0,
            BLOCKED_INCOMPLETE_PROVENANCE: 0,
            BLOCKED_DUPLICATE_VINTAGE: 0,
            SKIPPED_ALREADY_HAS_VINTAGE: 0,
            UNKNOWN: 0,
        }
        planned_provenance_rows = 0
        planned_vintage_rows = 0

        for latest_row in latest_rows:
            candidate = _evaluate_candidate(
                conn,
                latest_row=latest_row,
                market=market,
                available_at_utc=available_at_utc,
                ingested_at_utc=ingested_at_utc,
                vintage_run_id=vintage_run_id,
            )
            counters[candidate["status"]] = counters.get(candidate["status"], 0) + 1
            if candidate["status"] == READY:
                planned_vintage_rows += 1
                planned_provenance_rows += int(candidate["provenance_field_count"])
            if len(samples) < sample_limit:
                samples.append(candidate)

        blocked_rows = sum(
            counters[status]
            for status in (
                BLOCKED_NO_SEC_RAW,
                BLOCKED_RECONSTRUCTION_MISMATCH,
                BLOCKED_INCOMPLETE_PROVENANCE,
                BLOCKED_DUPLICATE_VINTAGE,
                UNKNOWN,
            )
        )
        skipped_rows = counters[SKIPPED_ALREADY_HAS_VINTAGE]
        ready_rows = counters[READY]
        if not latest_rows:
            overall_status = "NO_CANDIDATES"
        elif blocked_rows:
            overall_status = "DRY_RUN_BLOCKED"
        else:
            overall_status = "DRY_RUN_READY"

        return {
            "summary": {
                "fundamentals_db": str(db_path),
                "market": market,
                "source_run_id": source_run_id,
                "available_at_utc": available_at_utc,
                "ingested_at_utc": ingested_at_utc,
                "vintage_run_id": vintage_run_id,
                "latest_missing_vintage_rows": latest_missing_vintage_rows,
                "candidates_checked": len(latest_rows),
                "ready_rows": ready_rows,
                "blocked_rows": blocked_rows,
                "skipped_rows": skipped_rows,
                "planned_vintage_rows": planned_vintage_rows,
                "planned_provenance_rows": planned_provenance_rows,
                "duplicate_vintage_rows": counters[BLOCKED_DUPLICATE_VINTAGE],
                "reconstruction_mismatch_rows": counters[BLOCKED_RECONSTRUCTION_MISMATCH],
                "incomplete_provenance_rows": counters[BLOCKED_INCOMPLETE_PROVENANCE],
                "no_sec_raw_rows": counters[BLOCKED_NO_SEC_RAW],
                "already_has_vintage_rows": skipped_rows,
                "overall_status": overall_status,
            },
            "samples": samples,
        }
    finally:
        conn.close()


def _evaluate_candidate(
    conn: sqlite3.Connection,
    *,
    latest_row: sqlite3.Row,
    market: str,
    available_at_utc: str,
    ingested_at_utc: str,
    vintage_run_id: str,
) -> dict[str, Any]:
    ticker = str(latest_row["ticker"]).upper()
    period_end_date = str(latest_row["period_end_date"])
    latest = _row_to_dict(latest_row)
    latest_non_null = _non_null_financial_count(latest)
    base = {
        "ticker": ticker,
        "period_end_date": period_end_date,
        "statement_vintage_id": None,
        "source_hash": None,
        "latest_non_null_field_count": latest_non_null,
        "provenance_field_count": 0,
        "status": UNKNOWN,
        "reason": None,
    }

    if _has_vintage_for_latest(conn, ticker, period_end_date, market):
        return {**base, "status": SKIPPED_ALREADY_HAS_VINTAGE, "reason": "matching vintage row already exists"}

    if not _has_exact_sec_raw(conn, ticker, period_end_date):
        return {**base, "status": BLOCKED_NO_SEC_RAW, "reason": "no exact SEC raw rows for ticker + period_end_date"}

    raw_rows = _load_sec_raw_rows_for_ticker(conn, ticker)
    try:
        reconstructed_rows, provenance_by_key = reconstruct_quarterly_rows_with_provenance(
            raw_rows,
            ticker,
            vintage_run_id,
            ingested_at_utc,
        )
        normalized_rows = build_quarterly_rows(reconstructed_rows, vintage_run_id)
    except Exception as exc:
        return {**base, "status": BLOCKED_RECONSTRUCTION_MISMATCH, "reason": f"SEC reconstruction failed: {exc}"}

    normalized_by_key = {
        (str(row["ticker"]).upper(), str(row["period_end_date"])): row
        for row in normalized_rows
    }
    key = (ticker, period_end_date)
    normalized = normalized_by_key.get(key)
    if normalized is None:
        return {**base, "status": BLOCKED_RECONSTRUCTION_MISMATCH, "reason": "SEC reconstruction did not produce target period"}

    mismatches = _financial_mismatches(latest, normalized)
    if mismatches:
        return {
            **base,
            "status": BLOCKED_RECONSTRUCTION_MISMATCH,
            "reason": "reconstructed fields differ: " + ", ".join(mismatches),
        }

    field_to_facts = provenance_by_key.get(key, {})
    missing_provenance = [
        field_name
        for field_name in REPORTED_FINANCIAL_FIELDS
        if normalized.get(field_name) is not None and not field_to_facts.get(field_name)
    ]
    if missing_provenance:
        return {
            **base,
            "status": BLOCKED_INCOMPLETE_PROVENANCE,
            "reason": "missing provenance for: " + ", ".join(missing_provenance),
        }

    contributing_facts = _flatten_contributing_facts(field_to_facts)
    metadata = build_sec_vintage_metadata(
        market=market,
        ticker=ticker,
        period_end_date=period_end_date,
        normalized_row=normalized,
        contributing_facts=contributing_facts,
        available_at_utc=available_at_utc,
        ingested_at_utc=ingested_at_utc,
        run_id=vintage_run_id,
        normalization_run_id=vintage_run_id,
    )
    if _statement_vintage_id_exists(conn, str(metadata["statement_vintage_id"])):
        return {
            **base,
            "statement_vintage_id": metadata["statement_vintage_id"],
            "source_hash": metadata["source_hash"],
            "status": BLOCKED_DUPLICATE_VINTAGE,
            "reason": "statement_vintage_id already exists",
        }

    field_source_map = build_sec_field_source_map(
        normalized_row=normalized,
        field_to_contributing_facts=field_to_facts,
    )
    vintage_row = build_quarterly_vintage_row_from_latest(normalized, metadata)
    provenance_rows = build_field_provenance_rows(
        str(metadata["statement_vintage_id"]),
        vintage_row,
        str(metadata["source_provider"]),
        field_source_map=field_source_map,
        run_id=str(metadata["run_id"]),
    )
    if len(provenance_rows) != latest_non_null:
        return {
            **base,
            "statement_vintage_id": metadata["statement_vintage_id"],
            "source_hash": metadata["source_hash"],
            "provenance_field_count": len(provenance_rows),
            "status": BLOCKED_INCOMPLETE_PROVENANCE,
            "reason": f"planned provenance rows {len(provenance_rows)} != latest non-null fields {latest_non_null}",
        }

    return {
        **base,
        "statement_vintage_id": metadata["statement_vintage_id"],
        "source_hash": metadata["source_hash"],
        "provenance_field_count": len(provenance_rows),
        "status": READY,
    }


def _connect_read_only(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _load_latest_rows(
    conn: sqlite3.Connection,
    source_run_id: str | None,
    ticker: str | None,
) -> list[sqlite3.Row]:
    where_parts = []
    params: list[Any] = []
    if ticker is None:
        where_parts.append(
            """
            NOT EXISTS (
                SELECT 1
                FROM rc_fundamental_quarterly_vintage v
                WHERE v.ticker = q.ticker
                  AND v.period_end_date = q.period_end_date
            )
            """
        )
    if source_run_id:
        where_parts.append("run_id = ?")
        params.append(source_run_id)
    if ticker:
        where_parts.append("ticker = ?")
        params.append(ticker.upper())
    sql = f"""
        SELECT *
        FROM rc_fundamental_quarterly q
        WHERE {" AND ".join(where_parts)}
        ORDER BY ticker ASC, period_end_date ASC
    """
    return conn.execute(sql, params).fetchall()


def _count_latest_missing_vintage(
    conn: sqlite3.Connection,
    source_run_id: str | None,
    ticker: str | None,
) -> int:
    where_parts = [
        """
        NOT EXISTS (
            SELECT 1
            FROM rc_fundamental_quarterly_vintage v
            WHERE v.ticker = q.ticker
              AND v.period_end_date = q.period_end_date
        )
        """
    ]
    params: list[Any] = []
    if source_run_id:
        where_parts.append("q.run_id = ?")
        params.append(source_run_id)
    if ticker:
        where_parts.append("q.ticker = ?")
        params.append(ticker.upper())
    row = conn.execute(
        f"SELECT COUNT(*) FROM rc_fundamental_quarterly q WHERE {' AND '.join(where_parts)}",
        params,
    ).fetchone()
    return int(row[0])


def _has_vintage_for_latest(conn: sqlite3.Connection, ticker: str, period_end_date: str, market: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM rc_fundamental_quarterly_vintage
        WHERE ticker = ?
          AND period_end_date = ?
          AND market = ?
        LIMIT 1
        """,
        (ticker, period_end_date, market),
    ).fetchone()
    return row is not None


def _has_exact_sec_raw(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM rc_fundamental_statement_raw
        WHERE ticker = ?
          AND period_end_date = ?
          AND source = 'sec_edgar'
          AND period_type = 'sec_fact'
        LIMIT 1
        """,
        (ticker, period_end_date),
    ).fetchone()
    return row is not None


def _load_sec_raw_rows_for_ticker(conn: sqlite3.Connection, ticker: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            ticker,
            statement_type,
            period_end_date,
            period_type,
            field_name,
            field_value,
            currency,
            source,
            retrieved_at_utc,
            run_id
        FROM rc_fundamental_statement_raw
        WHERE ticker = ?
          AND source = 'sec_edgar'
          AND period_type = 'sec_fact'
        ORDER BY ticker ASC, statement_type ASC, period_end_date ASC, field_name ASC
        """,
        (ticker,),
    ).fetchall()


def _statement_vintage_id_exists(conn: sqlite3.Connection, statement_vintage_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM rc_fundamental_quarterly_vintage
        WHERE statement_vintage_id = ?
        LIMIT 1
        """,
        (statement_vintage_id,),
    ).fetchone()
    return row is not None


def _financial_mismatches(latest: Mapping[str, Any], normalized: Mapping[str, Any]) -> list[str]:
    mismatches = []
    for field_name in (*REPORTED_FINANCIAL_FIELDS, "currency"):
        if not _values_equal(latest.get(field_name), normalized.get(field_name)):
            mismatches.append(field_name)
    return mismatches


def _values_equal(left: Any, right: Any) -> bool:
    if left is None and right in (None, ""):
        return True
    if right is None and left in (None, ""):
        return True
    if isinstance(left, (int, float)) or isinstance(right, (int, float)):
        try:
            return abs(float(left) - float(right)) < 1e-9
        except (TypeError, ValueError):
            return False
    return left == right


def _non_null_financial_count(row: Mapping[str, Any]) -> int:
    return sum(1 for field_name in REPORTED_FINANCIAL_FIELDS if row.get(field_name) is not None)


def _flatten_contributing_facts(
    field_to_facts: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[Mapping[str, Any]]:
    facts: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for field_name in sorted(field_to_facts):
        for fact in field_to_facts[field_name]:
            key = json.dumps(dict(fact), sort_keys=True, default=str)
            if key in seen:
                continue
            seen.add(key)
            facts.append(fact)
    return facts


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _format_text(result: Mapping[str, Any]) -> str:
    summary = result["summary"]
    lines = [
        "SEC-derived vintage dry-run for latest rows missing vintage",
        f"fundamentals_db: {summary['fundamentals_db']}",
        f"market: {summary['market']}",
        f"source_run_id: {summary['source_run_id']}",
        f"latest_missing_vintage_rows: {summary['latest_missing_vintage_rows']}",
        f"candidates_checked: {summary['candidates_checked']}",
        f"ready_rows: {summary['ready_rows']}",
        f"blocked_rows: {summary['blocked_rows']}",
        f"skipped_rows: {summary['skipped_rows']}",
        f"planned_vintage_rows: {summary['planned_vintage_rows']}",
        f"planned_provenance_rows: {summary['planned_provenance_rows']}",
        f"overall_status: {summary['overall_status']}",
        "samples:",
    ]
    for sample in result["samples"]:
        reason = f" reason={sample['reason']}" if sample.get("reason") else ""
        lines.append(
            f"- {sample['ticker']} {sample['period_end_date']} {sample['status']} "
            f"provenance={sample['provenance_field_count']}{reason}"
        )
    return "\n".join(lines)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--source-run-id")
    parser.add_argument("--available-at-utc", required=True)
    parser.add_argument("--ingested-at-utc", required=True)
    parser.add_argument("--vintage-run-id", required=True)
    parser.add_argument("--ticker")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--fail-if-blocked", action="store_true")
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
