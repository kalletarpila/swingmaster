from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import sqlite3
from typing import Any


_EVENT_RAW_FIELDS = (
    "trend_state",
    "dow_label_high",
    "dow_label_low",
    "last_high_label",
    "last_high_label_date",
    "last_high_label_price",
    "last_low_label",
    "last_low_label_date",
    "last_low_label_price",
    "active_bos_high_date",
    "active_bos_high_price",
    "active_bos_low_date",
    "active_bos_low_price",
    "bos_up_count",
    "bos_down_count",
    "break_signal",
    "break_level_date",
    "break_level_price",
    "break_close_price",
    "reset_marker",
    "reset_reason",
    "structure_epoch_id",
    "structure_epoch_start_date",
)


@dataclass
class StockDowStructureRawExport:
    context_snapshot_rows: list[dict[str, Any]]
    recent_event_rows_60td: list[dict[str, Any]]

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        return asdict(self)


def read_stock_dow_structure_raw_export(
    analysis_db_path: str,
    osakedata_db_path: str,
    ticker: str,
    as_of_date: str,
    market: str | None = None,
    pivot_radius: int = 3,
    price_source: str = "close",
    recent_window_trading_days: int = 60,
) -> StockDowStructureRawExport:
    analysis_path = Path(analysis_db_path).resolve()
    osakedata_path = Path(osakedata_db_path).resolve()

    with _connect_readonly(analysis_path) as analysis_conn, _connect_readonly(osakedata_path) as osakedata_conn:
        status_row = _fetch_status_row(
            analysis_conn,
            ticker=ticker,
            market=market,
            pivot_radius=pivot_radius,
            price_source=price_source,
        )
        resolved_market = market if market is not None else (status_row["market"] if status_row is not None else None)
        latest_valid_close_date = _fetch_latest_valid_close_date(
            osakedata_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=resolved_market,
        )
        coverage_status, coverage_reason = _derive_coverage_status(
            status_row=status_row,
            latest_valid_close_date=latest_valid_close_date,
        )
        valid_trading_dates_desc = _fetch_valid_trading_dates(
            osakedata_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=resolved_market,
            limit=recent_window_trading_days,
        )
        window_meta = _derive_window_metadata(
            valid_trading_dates_desc,
            recent_window_trading_days=recent_window_trading_days,
        )
        latest_event_row = _fetch_latest_confirmed_event_row(
            analysis_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=resolved_market,
            pivot_radius=pivot_radius,
            price_source=price_source,
        )
        recent_event_db_rows = _fetch_recent_event_rows(
            analysis_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=resolved_market,
            pivot_radius=pivot_radius,
            price_source=price_source,
            window_start_date=window_meta["recent_event_window_start_date"],
        )
        warning_flags = _derive_warning_flags(
            status_row=status_row,
            coverage_status=coverage_status,
            latest_event_row=latest_event_row,
        )
        context_row = _build_context_snapshot_row(
            ticker=ticker,
            market=resolved_market,
            as_of_date=as_of_date,
            price_source=price_source,
            pivot_radius=pivot_radius,
            status_row=status_row,
            coverage_status=coverage_status,
            coverage_reason=coverage_reason,
            latest_valid_close_date=latest_valid_close_date,
            latest_event_row=latest_event_row,
            warning_flags=warning_flags,
            window_meta=window_meta,
        )
        recent_event_rows = _build_recent_event_sequence_rows(
            event_rows=recent_event_db_rows,
            ticker=ticker,
            market=resolved_market,
            as_of_date=as_of_date,
            price_source=price_source,
            pivot_radius=pivot_radius,
            window_meta=window_meta,
        )

    return StockDowStructureRawExport(
        context_snapshot_rows=[context_row],
        recent_event_rows_60td=recent_event_rows,
    )


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_status_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    market: str | None,
    pivot_radius: int,
    price_source: str,
) -> sqlite3.Row | None:
    if market is not None:
        query = """
        SELECT *
        FROM stock_dow_structure_status
        WHERE ticker = ?
          AND market = ?
          AND price_source = ?
          AND pivot_radius = ?
        LIMIT 1
        """
        return conn.execute(query, (ticker, market, price_source, pivot_radius)).fetchone()

    query = """
    SELECT *
    FROM stock_dow_structure_status
    WHERE ticker = ?
      AND price_source = ?
      AND pivot_radius = ?
    ORDER BY updated_at_utc DESC, calculated_through_date DESC, market ASC
    LIMIT 1
    """
    return conn.execute(query, (ticker, price_source, pivot_radius)).fetchone()


def _fetch_latest_valid_close_date(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str | None,
) -> str | None:
    query = """
    SELECT MAX(pvm)
    FROM osakedata
    WHERE osake = ?
      AND close IS NOT NULL
      AND pvm <= ?
    """
    params: list[Any] = [ticker, as_of_date]
    if market is not None:
        query += " AND market = ?"
        params.append(market)
    row = conn.execute(query, params).fetchone()
    return None if row is None else row[0]


def _fetch_valid_trading_dates(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str | None,
    limit: int,
) -> list[str]:
    query = """
    SELECT pvm
    FROM osakedata
    WHERE osake = ?
      AND close IS NOT NULL
      AND pvm <= ?
    """
    params: list[Any] = [ticker, as_of_date]
    if market is not None:
        query += " AND market = ?"
        params.append(market)
    query += " ORDER BY pvm DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return [row[0] for row in rows]


def _fetch_latest_confirmed_event_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str | None,
    pivot_radius: int,
    price_source: str,
) -> sqlite3.Row | None:
    query = """
    SELECT *
    FROM stock_dow_structure_events
    WHERE ticker = ?
      AND price_source = ?
      AND pivot_radius = ?
      AND confirmed_as_of_date <= ?
    """
    params: list[Any] = [ticker, price_source, pivot_radius, as_of_date]
    if market is not None:
        query += " AND market = ?"
        params.append(market)
    query += " ORDER BY confirmed_as_of_date DESC, id DESC LIMIT 1"
    return conn.execute(query, params).fetchone()


def _fetch_recent_event_rows(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str | None,
    pivot_radius: int,
    price_source: str,
    window_start_date: str | None,
) -> list[sqlite3.Row]:
    if window_start_date is None:
        return []
    query = """
    SELECT *
    FROM stock_dow_structure_events
    WHERE ticker = ?
      AND price_source = ?
      AND pivot_radius = ?
      AND confirmed_as_of_date >= ?
      AND confirmed_as_of_date <= ?
    """
    params: list[Any] = [ticker, price_source, pivot_radius, window_start_date, as_of_date]
    if market is not None:
        query += " AND market = ?"
        params.append(market)
    query += " ORDER BY confirmed_as_of_date ASC, id ASC"
    return conn.execute(query, params).fetchall()


def _derive_window_metadata(
    valid_trading_dates_desc: list[str],
    *,
    recent_window_trading_days: int,
) -> dict[str, Any]:
    if not valid_trading_dates_desc:
        return {
            "recent_event_window_trading_days": recent_window_trading_days,
            "recent_event_available_trading_days": 0,
            "recent_event_window_start_date": None,
            "recent_event_window_end_date": None,
        }
    return {
        "recent_event_window_trading_days": recent_window_trading_days,
        "recent_event_available_trading_days": len(valid_trading_dates_desc),
        "recent_event_window_start_date": valid_trading_dates_desc[-1],
        "recent_event_window_end_date": valid_trading_dates_desc[0],
    }


def _derive_coverage_status(
    *,
    status_row: sqlite3.Row | None,
    latest_valid_close_date: str | None,
) -> tuple[str, str]:
    if latest_valid_close_date is None:
        return "NO_VALID_CLOSE_DATA", "no valid close data on or before as_of_date"
    if status_row is None:
        return "MISSING_STATUS", "status row not found"
    if status_row["last_status"] == "ERROR":
        return "ERROR", "last status is ERROR"
    calculated_through_date = status_row["calculated_through_date"]
    if calculated_through_date is None:
        return "ERROR", "status calculated_through_date is missing"
    if calculated_through_date < latest_valid_close_date:
        return "STALE", "calculated_through_date is older than latest valid close date"
    if calculated_through_date >= latest_valid_close_date and status_row["last_status"] == "OK":
        return "OK", "calculated_through_date covers latest valid close date"
    return "ERROR", "unsupported status state"


def _derive_warning_flags(
    *,
    status_row: sqlite3.Row | None,
    coverage_status: str,
    latest_event_row: sqlite3.Row | None,
) -> list[str]:
    flags: list[str] = []
    if coverage_status == "MISSING_STATUS":
        flags.append("MISSING_STATUS")
    if coverage_status == "STALE":
        flags.append("STALE_COVERAGE")
    if coverage_status == "NO_VALID_CLOSE_DATA":
        flags.append("NO_VALID_CLOSE_DATA")
    if status_row is not None and status_row["last_status"] == "ERROR":
        flags.append("LAST_RUN_ERROR")
    if latest_event_row is None:
        flags.append("NO_EVENT_FOUND")
    return flags


def _build_context_snapshot_row(
    *,
    ticker: str,
    market: str | None,
    as_of_date: str,
    price_source: str,
    pivot_radius: int,
    status_row: sqlite3.Row | None,
    coverage_status: str,
    coverage_reason: str,
    latest_valid_close_date: str | None,
    latest_event_row: sqlite3.Row | None,
    warning_flags: list[str],
    window_meta: dict[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "ticker": ticker,
        "market": market,
        "as_of_date": as_of_date,
        "price_source": price_source,
        "pivot_radius": pivot_radius,
        "status_found": status_row is not None,
        "status_last_status": None if status_row is None else status_row["last_status"],
        "calculated_from_date": None if status_row is None else status_row["calculated_from_date"],
        "calculated_through_date": None if status_row is None else status_row["calculated_through_date"],
        "latest_valid_close_date_on_or_before_as_of_date": latest_valid_close_date,
        "coverage_status": coverage_status,
        "coverage_reason": coverage_reason,
        "latest_event_found": latest_event_row is not None,
        "latest_event_id": None if latest_event_row is None else latest_event_row["id"],
        "latest_event_type": None if latest_event_row is None else latest_event_row["event_type"],
        "latest_event_date": None if latest_event_row is None else latest_event_row["event_date"],
        "latest_confirmed_as_of_date": None if latest_event_row is None else latest_event_row["confirmed_as_of_date"],
        "recent_event_window_trading_days": window_meta["recent_event_window_trading_days"],
        "recent_event_available_trading_days": window_meta["recent_event_available_trading_days"],
        "recent_event_window_start_date": window_meta["recent_event_window_start_date"],
        "recent_event_window_end_date": window_meta["recent_event_window_end_date"],
        "dow_warning_flags": warning_flags,
    }
    for field_name in _EVENT_RAW_FIELDS:
        row[field_name] = None if latest_event_row is None else latest_event_row[field_name]
    return row


def _build_recent_event_sequence_rows(
    *,
    event_rows: list[sqlite3.Row],
    ticker: str,
    market: str | None,
    as_of_date: str,
    price_source: str,
    pivot_radius: int,
    window_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, event_row in enumerate(event_rows, start=1):
        rows.append(
            {
                "ticker": ticker,
                "market": market,
                "as_of_date": as_of_date,
                "price_source": price_source,
                "pivot_radius": pivot_radius,
                "sequence_window_trading_days": window_meta["recent_event_window_trading_days"],
                "sequence_available_trading_days": window_meta["recent_event_available_trading_days"],
                "sequence_window_start_date": window_meta["recent_event_window_start_date"],
                "sequence_window_end_date": window_meta["recent_event_window_end_date"],
                "sequence_index": index,
                "event_id": event_row["id"],
                "event_date": event_row["event_date"],
                "confirmed_as_of_date": event_row["confirmed_as_of_date"],
                "event_type": event_row["event_type"],
                "open": event_row["open"],
                "high": event_row["high"],
                "low": event_row["low"],
                "close": event_row["close"],
                "volume": event_row["volume"],
                "structure_price": event_row["structure_price"],
                "dow_label_high": event_row["dow_label_high"],
                "dow_label_low": event_row["dow_label_low"],
                "trend_state": event_row["trend_state"],
                "last_high_label": event_row["last_high_label"],
                "last_high_label_date": event_row["last_high_label_date"],
                "last_high_label_price": event_row["last_high_label_price"],
                "last_low_label": event_row["last_low_label"],
                "last_low_label_date": event_row["last_low_label_date"],
                "last_low_label_price": event_row["last_low_label_price"],
                "active_bos_high_date": event_row["active_bos_high_date"],
                "active_bos_high_price": event_row["active_bos_high_price"],
                "active_bos_low_date": event_row["active_bos_low_date"],
                "active_bos_low_price": event_row["active_bos_low_price"],
                "bos_up_count": event_row["bos_up_count"],
                "bos_down_count": event_row["bos_down_count"],
                "break_signal": event_row["break_signal"],
                "break_level_date": event_row["break_level_date"],
                "break_level_price": event_row["break_level_price"],
                "break_close_price": event_row["break_close_price"],
                "reset_marker": event_row["reset_marker"],
                "reset_reason": event_row["reset_reason"],
                "structure_epoch_id": event_row["structure_epoch_id"],
                "structure_epoch_start_date": event_row["structure_epoch_start_date"],
                "calc_version": event_row["calc_version"],
                "run_id": event_row["run_id"],
                "created_at_utc": event_row["created_at_utc"],
            }
        )
    return rows
