from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.reported_final_mixed_vintage import (
    build_final_mixed_source_hash,
    build_final_mixed_statement_vintage_id,
    merge_final_mixed_field_source_maps,
)


FINANCIAL_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only preflight for one final mixed vintage candidate")
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--ticker", default=None)
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--available-at-utc", required=True)
    parser.add_argument("--format", choices=("json",), default="json")
    return parser.parse_args(argv)


def run_preflight(
    *,
    fundamentals_db: Path,
    market: str,
    ticker: str | None,
    as_of_date: str,
    available_at_utc: str,
) -> dict[str, Any]:
    with _connect_read_only(fundamentals_db) as conn:
        selected_ticker = _select_ticker(conn, market=market, ticker=ticker)
        if selected_ticker is None:
            return _result(status="NO_SOURCE_ROW", market=market, ticker=ticker, as_of_date=as_of_date)
        source_row = _latest_quarterly_row(conn, market=market, ticker=selected_ticker, as_of_date=as_of_date)
        if source_row is None:
            return _result(status="NO_SOURCE_ROW", market=market, ticker=selected_ticker, as_of_date=as_of_date)

        period_end_date = str(source_row["period_end_date"])
        legacy_vintage = _latest_legacy_vintage(conn, market=market, ticker=selected_ticker, period_end_date=period_end_date)
        if legacy_vintage is None:
            return _result(
                status="NO_LEGACY_VINTAGE",
                market=market,
                ticker=selected_ticker,
                period_end_date=period_end_date,
                as_of_date=as_of_date,
            )

        provenance_rows = _provenance_rows(conn, str(legacy_vintage["statement_vintage_id"]))
        if not provenance_rows:
            return _result(
                status="NO_PROVENANCE",
                market=market,
                ticker=selected_ticker,
                period_end_date=period_end_date,
                legacy_statement_vintage_id=legacy_vintage["statement_vintage_id"],
                as_of_date=as_of_date,
            )

        normalized_row = _normalized_row_from_latest(source_row)
        source_map = _source_map_from_provenance(provenance_rows)
        source_hash = build_final_mixed_source_hash(
            market=market,
            ticker=selected_ticker,
            period_end_date=period_end_date,
            normalized_row=normalized_row,
            sec_field_source_map=source_map,
            yahoo_field_source_map={},
            fallback_audit_rows=[],
        )
        statement_vintage_id = build_final_mixed_statement_vintage_id(
            market=market,
            ticker=selected_ticker,
            period_end_date=period_end_date,
            source_hash=source_hash,
        )
        duplicate_exists = _statement_vintage_exists(conn, statement_vintage_id)
        merged_source_map = merge_final_mixed_field_source_maps(
            normalized_row=normalized_row,
            sec_field_source_map=source_map,
            yahoo_field_source_map={},
        )
        has_yahoo_fallback = any(row["source_provider"] == "yahoo" for row in provenance_rows) and any(
            row["merge_action"] == "YAHOO_FILLED_MISSING" for row in provenance_rows
        )
        if duplicate_exists:
            status = "DUPLICATE_FINAL_MIXED"
        elif not has_yahoo_fallback:
            status = "INPUTS_INCOMPLETE_FOR_TRUE_FINAL_MIXED"
        else:
            status = "READY_FOR_GUARDED_WRITE"

        return _result(
            status=status,
            market=market,
            ticker=selected_ticker,
            period_end_date=period_end_date,
            as_of_date=as_of_date,
            available_at_utc=available_at_utc,
            legacy_statement_vintage_id=legacy_vintage["statement_vintage_id"],
            statement_vintage_id=statement_vintage_id,
            source_hash=source_hash,
            provenance_rows=len(provenance_rows),
            provenance_field_count=len(merged_source_map),
            duplicate_final_mixed=duplicate_exists,
            legacy_baseline_only=not has_yahoo_fallback,
            query_only=_query_only_state(conn),
        )


def _connect_read_only(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(str(path))
    uri = f"file:{path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _select_ticker(conn: sqlite3.Connection, *, market: str, ticker: str | None) -> str | None:
    if ticker is not None and ticker.strip():
        row = conn.execute(
            """
            SELECT ticker
            FROM rc_fundamental_quarterly
            WHERE ticker = ? AND EXISTS (
                SELECT 1
                FROM rc_fundamental_quarterly_vintage v
                WHERE v.ticker = rc_fundamental_quarterly.ticker
                  AND v.market = ?
            )
            ORDER BY ticker ASC
            LIMIT 1
            """,
            (ticker.strip().upper(), market),
        ).fetchone()
        return None if row is None else str(row["ticker"])
    row = conn.execute(
        """
        SELECT q.ticker
        FROM rc_fundamental_quarterly q
        WHERE EXISTS (
            SELECT 1
            FROM rc_fundamental_quarterly_vintage v
            WHERE v.ticker = q.ticker
              AND v.market = ?
        )
        ORDER BY q.ticker ASC
        LIMIT 1
        """,
        (market,),
    ).fetchone()
    return None if row is None else str(row["ticker"])


def _latest_quarterly_row(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    as_of_date: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT q.*
        FROM rc_fundamental_quarterly q
        WHERE q.ticker = ?
          AND q.period_end_date <= ?
          AND EXISTS (
              SELECT 1
              FROM rc_fundamental_quarterly_vintage v
              WHERE v.ticker = q.ticker
                AND v.period_end_date = q.period_end_date
                AND v.market = ?
          )
        ORDER BY q.period_end_date DESC
        LIMIT 1
        """,
        (ticker, as_of_date, market),
    ).fetchone()


def _latest_legacy_vintage(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    period_end_date: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM rc_fundamental_quarterly_vintage
        WHERE ticker = ?
          AND period_end_date = ?
          AND market = ?
          AND source_provider != 'mixed_sec_yahoo'
        ORDER BY available_at_utc DESC, revision_number DESC, statement_vintage_id DESC
        LIMIT 1
        """,
        (ticker, period_end_date, market),
    ).fetchone()


def _provenance_rows(conn: sqlite3.Connection, statement_vintage_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM rc_fundamental_quarterly_field_provenance
        WHERE statement_vintage_id = ?
        ORDER BY field_name ASC
        """,
        (statement_vintage_id,),
    ).fetchall()


def _statement_vintage_exists(conn: sqlite3.Connection, statement_vintage_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM rc_fundamental_quarterly_vintage WHERE statement_vintage_id = ? LIMIT 1",
        (statement_vintage_id,),
    ).fetchone()
    return row is not None


def _normalized_row_from_latest(row: sqlite3.Row) -> dict[str, Any]:
    normalized = {
        "ticker": row["ticker"],
        "period_end_date": row["period_end_date"],
        "currency": row["currency"],
        "run_id": row["run_id"],
    }
    for field_name in FINANCIAL_FIELDS:
        normalized[field_name] = row[field_name]
    return normalized


def _source_map_from_provenance(rows: list[sqlite3.Row]) -> dict[str, dict[str, Any]]:
    source_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        field_name = str(row["field_name"])
        if field_name not in FINANCIAL_FIELDS:
            continue
        source_map[field_name] = {
            "source_provider": row["source_provider"],
            "source_table": row["source_table"],
            "source_row_ref": row["source_row_ref"],
            "source_document_id": row["source_document_id"],
            "source_hash": row["source_hash"],
            "provenance_role": row["provenance_role"],
            "merge_action": row["merge_action"],
            "old_value": row["old_value"],
            "new_value": row["new_value"],
            "available_at_utc": row["available_at_utc"],
            "created_at_utc": row["created_at_utc"],
            "run_id": row["run_id"],
            "enrichment_run_id": row["enrichment_run_id"],
        }
    return source_map


def _query_only_state(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA query_only").fetchone()
    return int(row[0])


def _result(**values: Any) -> dict[str, Any]:
    result = {
        "status": values.pop("status"),
        "market": values.pop("market", None),
        "ticker": values.pop("ticker", None),
        "period_end_date": values.pop("period_end_date", None),
        "as_of_date": values.pop("as_of_date", None),
        "source_hash": None,
        "statement_vintage_id": None,
    }
    result.update(values)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_preflight(
        fundamentals_db=Path(args.fundamentals_db),
        market=str(args.market).lower(),
        ticker=args.ticker,
        as_of_date=args.as_of_date,
        available_at_utc=args.available_at_utc,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
