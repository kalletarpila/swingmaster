from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from swingmaster.cli.dry_run_sec_vintage_for_missing_latest import (
    CANDIDATE_MODE_LATEST_WRITER,
    run_dry_run,
)
from swingmaster.fundamentals.reported_sec_latest_writer_vintage import (
    build_latest_writer_sec_vintage_candidate,
)
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)


APPROVAL_TOKEN = "USER_APPROVES_SEC_LATEST_WRITER_VINTAGE_APPLY"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.approval_token != APPROVAL_TOKEN:
            result = run_dry_run(
                fundamentals_db=args.fundamentals_db,
                market=args.market,
                source_run_id=args.source_run_id,
                available_at_utc=args.available_at_utc,
                ingested_at_utc=args.ingested_at_utc,
                vintage_run_id=args.vintage_run_id,
                ticker=args.ticker,
                sample_limit=args.sample_limit,
                candidate_mode=CANDIDATE_MODE_LATEST_WRITER,
            )
            result["apply"] = {
                "applied": False,
                "reason": "approval token missing or invalid; dry-run only",
            }
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        result = apply_latest_writer_sec_vintage(
            fundamentals_db=args.fundamentals_db,
            market=args.market,
            source_run_id=args.source_run_id,
            available_at_utc=args.available_at_utc,
            ingested_at_utc=args.ingested_at_utc,
            vintage_run_id=args.vintage_run_id,
            expected_count=args.expected_count,
            ticker=args.ticker,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def apply_latest_writer_sec_vintage(
    *,
    fundamentals_db: str,
    market: str,
    source_run_id: str,
    available_at_utc: str,
    ingested_at_utc: str,
    vintage_run_id: str,
    expected_count: int | None,
    ticker: str | None = None,
) -> dict[str, Any]:
    if expected_count is None:
        raise ValueError("SEC_LATEST_WRITER_VINTAGE_APPLY_EXPECTED_COUNT_REQUIRED")
    db_path = Path(fundamentals_db)
    if not db_path.exists():
        raise FileNotFoundError(f"FUNDAMENTALS_DB_NOT_FOUND:{fundamentals_db}")

    dry_run = run_dry_run(
        fundamentals_db=str(db_path),
        market=market,
        source_run_id=source_run_id,
        available_at_utc=available_at_utc,
        ingested_at_utc=ingested_at_utc,
        vintage_run_id=vintage_run_id,
        ticker=ticker,
        sample_limit=expected_count,
        candidate_mode=CANDIDATE_MODE_LATEST_WRITER,
    )
    summary = dry_run["summary"]
    if int(summary["candidates_checked"]) != expected_count:
        raise ValueError(
            f"SEC_LATEST_WRITER_VINTAGE_APPLY_EXPECTED_COUNT_MISMATCH:"
            f"expected={expected_count}:actual={summary['candidates_checked']}"
        )
    if int(summary["blocked_rows"]) != 0:
        raise ValueError("SEC_LATEST_WRITER_VINTAGE_APPLY_BLOCKED_CANDIDATES")

    backup_path = db_path.with_suffix(db_path.suffix + ".sec_latest_writer_vintage_apply.bak")
    shutil.copy2(db_path, backup_path)

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        latest_rows = _load_latest_rows(conn, source_run_id, ticker)
        vintage_rows_inserted = 0
        provenance_rows_inserted = 0
        for latest_row in latest_rows:
            ticker_value = str(latest_row["ticker"]).upper()
            raw_rows = _load_sec_raw_rows_for_ticker(conn, ticker_value)
            candidate = build_latest_writer_sec_vintage_candidate(
                latest_row={key: latest_row[key] for key in latest_row.keys()},
                sec_raw_rows=raw_rows,
                market=market,
                available_at_utc=available_at_utc,
                ingested_at_utc=ingested_at_utc,
                vintage_run_id=vintage_run_id,
                source_run_id=source_run_id,
            )
            vintage_rows_inserted += insert_quarterly_vintage_row(conn, candidate["vintage_row"])
            provenance_rows_inserted += insert_quarterly_field_provenance_rows(conn, candidate["provenance_rows"])
        conn.commit()

    return {
        "apply": {
            "applied": True,
            "backup_path": str(backup_path),
            "vintage_rows_inserted": vintage_rows_inserted,
            "provenance_rows_inserted": provenance_rows_inserted,
        },
        "dry_run_summary": summary,
    }


def _load_latest_rows(conn: sqlite3.Connection, source_run_id: str, ticker: str | None) -> list[sqlite3.Row]:
    where_parts = [
        """
        NOT EXISTS (
            SELECT 1
            FROM rc_fundamental_quarterly_vintage v
            WHERE v.ticker = q.ticker
              AND v.period_end_date = q.period_end_date
        )
        """,
        "q.run_id = ?",
    ]
    params: list[Any] = [source_run_id]
    if ticker:
        where_parts.append("q.ticker = ?")
        params.append(ticker.upper())
    return conn.execute(
        f"""
        SELECT *
        FROM rc_fundamental_quarterly q
        WHERE {" AND ".join(where_parts)}
        ORDER BY ticker ASC, period_end_date ASC
        """,
        params,
    ).fetchall()


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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--candidate-mode", choices=(CANDIDATE_MODE_LATEST_WRITER,), required=True)
    parser.add_argument("--available-at-utc", required=True)
    parser.add_argument("--ingested-at-utc", required=True)
    parser.add_argument("--vintage-run-id", required=True)
    parser.add_argument("--approval-token")
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--ticker")
    parser.add_argument("--sample-limit", type=int, default=20)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
