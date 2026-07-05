from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


READY_NOOP = "READY_NOOP"
PARITY_DRIFT = "PARITY_DRIFT"
DUPLICATE_VINTAGE = "DUPLICATE_VINTAGE"
PENDING_YAHOO_AWARE_ACTION = "PENDING_YAHOO_AWARE_ACTION"
UNKNOWN = "UNKNOWN"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only quarter_update vintage readiness/no-op smoke")
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--source-run-id", default=None)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_preflight(
            fundamentals_db=Path(args.fundamentals_db),
            market=args.market,
            source_run_id=args.source_run_id,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_text(result))
    return 0 if result["overall_status"] == READY_NOOP else 1


def run_preflight(
    *,
    fundamentals_db: Path,
    market: str,
    source_run_id: str | None = None,
) -> dict[str, Any]:
    db_path = fundamentals_db.expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"FUNDAMENTALS_DB_NOT_FOUND:{db_path}")

    normalized_market = market.strip().lower()
    if not normalized_market:
        raise ValueError("MARKET_REQUIRED")

    with _connect_read_only(db_path) as conn:
        _validate_required_tables(conn)
        quick_check = _quick_check(conn)
        latest_row_count = _count(conn, "rc_fundamental_quarterly")
        vintage_row_count = _count(conn, "rc_fundamental_quarterly_vintage")
        provenance_row_count = _count(conn, "rc_fundamental_quarterly_field_provenance")
        latest_without_vintage_count = _latest_without_vintage_count(
            conn,
            market=normalized_market,
            source_run_id=source_run_id,
        )
        vintage_without_latest_count = _vintage_without_latest_count(conn, market=normalized_market)
        duplicate_statement_vintage_id_count = _duplicate_statement_vintage_id_count(conn)
        sec_missing_latest_candidates = latest_without_vintage_count
        yahoo_aware_pending_action_count = _yahoo_aware_pending_action_count(
            latest_without_vintage_count=latest_without_vintage_count,
            vintage_without_latest_count=vintage_without_latest_count,
            duplicate_statement_vintage_id_count=duplicate_statement_vintage_id_count,
        )
        overall_status = _overall_status(
            quick_check=quick_check,
            latest_without_vintage_count=latest_without_vintage_count,
            vintage_without_latest_count=vintage_without_latest_count,
            duplicate_statement_vintage_id_count=duplicate_statement_vintage_id_count,
            yahoo_aware_pending_action_count=yahoo_aware_pending_action_count,
        )
        return {
            "fundamentals_db": str(db_path),
            "market": normalized_market,
            "source_run_id": source_run_id,
            "query_only": _query_only_state(conn),
            "quick_check": quick_check,
            "latest_row_count": latest_row_count,
            "vintage_row_count": vintage_row_count,
            "provenance_row_count": provenance_row_count,
            "latest_without_vintage_count": latest_without_vintage_count,
            "vintage_without_latest_count": vintage_without_latest_count,
            "duplicate_statement_vintage_id_count": duplicate_statement_vintage_id_count,
            "sec_missing_latest_candidates": sec_missing_latest_candidates,
            "yahoo_aware_pending_action_count": yahoo_aware_pending_action_count,
            "overall_status": overall_status,
        }


def _connect_read_only(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _validate_required_tables(conn: sqlite3.Connection) -> None:
    required = {
        "rc_fundamental_quarterly",
        "rc_fundamental_quarterly_vintage",
        "rc_fundamental_quarterly_field_provenance",
    }
    existing = {
        str(row["name"])
        for row in conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        )
    }
    missing = sorted(required - existing)
    if missing:
        raise RuntimeError("FUNDAMENTAL_VINTAGE_TABLES_MISSING:" + ",".join(missing))


def _quick_check(conn: sqlite3.Connection) -> str:
    row = conn.execute("PRAGMA quick_check").fetchone()
    return str(row[0]) if row is not None else UNKNOWN


def _count(conn: sqlite3.Connection, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0])


def _latest_without_vintage_count(
    conn: sqlite3.Connection,
    *,
    market: str,
    source_run_id: str | None,
) -> int:
    where_parts = [
        """
        NOT EXISTS (
            SELECT 1
            FROM rc_fundamental_quarterly_vintage v
            WHERE v.ticker = q.ticker
              AND v.period_end_date = q.period_end_date
              AND v.market = ?
        )
        """
    ]
    params: list[Any] = [market]
    if source_run_id:
        where_parts.append("q.run_id = ?")
        params.append(source_run_id)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly q
        WHERE {" AND ".join(where_parts)}
        """,
        params,
    ).fetchone()
    return int(row[0])


def _vintage_without_latest_count(conn: sqlite3.Connection, *, market: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_vintage v
        WHERE v.market = ?
          AND NOT EXISTS (
              SELECT 1
              FROM rc_fundamental_quarterly q
              WHERE q.ticker = v.ticker
                AND q.period_end_date = v.period_end_date
          )
        """,
        (market,),
    ).fetchone()
    return int(row[0])


def _duplicate_statement_vintage_id_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT statement_vintage_id
            FROM rc_fundamental_quarterly_vintage
            GROUP BY statement_vintage_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return int(row[0])


def _yahoo_aware_pending_action_count(
    *,
    latest_without_vintage_count: int,
    vintage_without_latest_count: int,
    duplicate_statement_vintage_id_count: int,
) -> int:
    if latest_without_vintage_count or vintage_without_latest_count or duplicate_statement_vintage_id_count:
        return latest_without_vintage_count + vintage_without_latest_count + duplicate_statement_vintage_id_count
    return 0


def _overall_status(
    *,
    quick_check: str,
    latest_without_vintage_count: int,
    vintage_without_latest_count: int,
    duplicate_statement_vintage_id_count: int,
    yahoo_aware_pending_action_count: int,
) -> str:
    if quick_check != "ok":
        return UNKNOWN
    if duplicate_statement_vintage_id_count:
        return DUPLICATE_VINTAGE
    if latest_without_vintage_count or vintage_without_latest_count:
        return PARITY_DRIFT
    if yahoo_aware_pending_action_count:
        return PENDING_YAHOO_AWARE_ACTION
    return READY_NOOP


def _query_only_state(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA query_only").fetchone()
    return int(row[0])


def _format_text(result: dict[str, Any]) -> str:
    keys = (
        "fundamentals_db",
        "market",
        "source_run_id",
        "query_only",
        "quick_check",
        "latest_row_count",
        "vintage_row_count",
        "provenance_row_count",
        "latest_without_vintage_count",
        "vintage_without_latest_count",
        "duplicate_statement_vintage_id_count",
        "sec_missing_latest_candidates",
        "yahoo_aware_pending_action_count",
        "overall_status",
    )
    return "\n".join(f"{key}: {result[key]}" for key in keys)


if __name__ == "__main__":
    raise SystemExit(main())
