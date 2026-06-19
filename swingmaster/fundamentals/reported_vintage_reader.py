from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Any


class ReportedVintageSchemaError(RuntimeError):
    """Raised when the reported vintage schema is not available."""


def list_quarterly_vintages(
    conn: sqlite3.Connection,
    ticker: str,
    period_end_date: str,
    market: str | None = None,
) -> list[sqlite3.Row]:
    normalized_ticker = _require_normalized_ticker(ticker)
    _require_text(period_end_date, "period_end_date")
    where_sql, params = _ticker_period_where(normalized_ticker, period_end_date, market)
    return _fetch_rows(
        conn,
        f"""
        SELECT *
        FROM rc_fundamental_quarterly_vintage
        WHERE {where_sql}
        ORDER BY available_at_utc ASC, revision_number ASC, statement_vintage_id ASC
        """,
        params,
    )


def get_latest_quarterly_vintage(
    conn: sqlite3.Connection,
    ticker: str,
    period_end_date: str,
    market: str | None = None,
) -> sqlite3.Row | None:
    normalized_ticker = _require_normalized_ticker(ticker)
    _require_text(period_end_date, "period_end_date")
    where_sql, params = _ticker_period_where(normalized_ticker, period_end_date, market)
    return _fetch_one(
        conn,
        f"""
        SELECT *
        FROM rc_fundamental_quarterly_vintage
        WHERE {where_sql}
        ORDER BY available_at_utc DESC, revision_number DESC, statement_vintage_id DESC
        LIMIT 1
        """,
        params,
    )


def get_pit_quarterly_vintage(
    conn: sqlite3.Connection,
    ticker: str,
    period_end_date: str,
    decision_cutoff_utc: str,
    market: str | None = None,
) -> sqlite3.Row | None:
    normalized_ticker = _require_normalized_ticker(ticker)
    _require_text(period_end_date, "period_end_date")
    _require_text(decision_cutoff_utc, "decision_cutoff_utc")
    where_sql, params = _ticker_period_where(normalized_ticker, period_end_date, market)
    return _fetch_one(
        conn,
        f"""
        SELECT *
        FROM rc_fundamental_quarterly_vintage
        WHERE {where_sql}
          AND available_at_utc <= ?
        ORDER BY available_at_utc DESC, revision_number DESC, statement_vintage_id DESC
        LIMIT 1
        """,
        (*params, decision_cutoff_utc),
    )


def get_quarterly_field_provenance(
    conn: sqlite3.Connection,
    statement_vintage_id: str,
) -> list[sqlite3.Row]:
    _require_text(statement_vintage_id, "statement_vintage_id")
    return _fetch_rows(
        conn,
        """
        SELECT *
        FROM rc_fundamental_quarterly_field_provenance
        WHERE statement_vintage_id = ?
        ORDER BY ticker ASC, period_end_date ASC, field_name ASC, source_provider ASC, provenance_role ASC
        """,
        (statement_vintage_id,),
    )


def get_latest_period_for_ticker(
    conn: sqlite3.Connection,
    ticker: str,
    market: str | None = None,
    decision_cutoff_utc: str | None = None,
) -> str | None:
    normalized_ticker = _require_normalized_ticker(ticker)
    params: list[Any] = [normalized_ticker]
    where_parts = ["ticker = ?"]
    if market is not None:
        where_parts.append("market = ?")
        params.append(market)
    if decision_cutoff_utc is not None:
        _require_text(decision_cutoff_utc, "decision_cutoff_utc")
        where_parts.append("available_at_utc <= ?")
        params.append(decision_cutoff_utc)
    row = _fetch_one(
        conn,
        f"""
        SELECT period_end_date
        FROM rc_fundamental_quarterly_vintage
        WHERE {" AND ".join(where_parts)}
        ORDER BY period_end_date DESC, available_at_utc DESC, revision_number DESC, statement_vintage_id DESC
        LIMIT 1
        """,
        tuple(params),
    )
    if row is None:
        return None
    return str(row["period_end_date"])


def _ticker_period_where(ticker: str, period_end_date: str, market: str | None) -> tuple[str, tuple[Any, ...]]:
    if market is None:
        return "ticker = ? AND period_end_date = ?", (ticker, period_end_date)
    return "ticker = ? AND period_end_date = ? AND market = ?", (ticker, period_end_date, market)


def _fetch_rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> list[sqlite3.Row]:
    return _with_row_factory(conn, lambda: conn.execute(sql, params).fetchall())


def _fetch_one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> sqlite3.Row | None:
    return _with_row_factory(conn, lambda: conn.execute(sql, params).fetchone())


def _with_row_factory(conn: sqlite3.Connection, operation: Callable[[], Any]) -> Any:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return operation()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            raise ReportedVintageSchemaError("REPORTED_VINTAGE_SCHEMA_MISSING") from exc
        raise
    finally:
        conn.row_factory = previous_row_factory


def _require_normalized_ticker(value: Any) -> str:
    ticker = _normalize_ticker(value)
    if ticker is None:
        raise ValueError("REPORTED_VINTAGE_REQUIRED_FIELD_MISSING:ticker")
    return ticker


def _normalize_ticker(value: Any) -> str | None:
    if value is None:
        return None
    ticker = str(value).strip().upper()
    if not ticker:
        return None
    return ticker


def _require_text(value: Any, field_name: str) -> None:
    if value is None or not str(value).strip():
        raise ValueError(f"REPORTED_VINTAGE_REQUIRED_FIELD_MISSING:{field_name}")
