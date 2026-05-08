from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import sqlite3
from typing import Any


_SIGNAL_FIELDS = (
    "bullish_strength",
    "bearish_strength",
    "hidden_bullish_strength",
    "hidden_bearish_strength",
    "rsi",
    "is_bullish_divergence",
    "is_bearish_divergence",
    "is_hidden_bullish_divergence",
    "is_hidden_bearish_divergence",
    "is_bullish_divergence_r2",
    "is_bearish_divergence_r2",
    "is_hidden_bullish_divergence_r2",
    "is_hidden_bearish_divergence_r2",
    "is_bullish_divergence_r3",
    "is_bearish_divergence_r3",
    "is_hidden_bullish_divergence_r3",
    "is_hidden_bearish_divergence_r3",
    "pivot_gap",
    "pivot_drop_pct",
    "pivot_gap_r2",
    "pivot_drop_pct_r2",
    "hidden_pivot_gap_r2",
    "hidden_pivot_drop_pct_r2",
    "pivot2_date_r2",
    "pivot_gap_r3",
    "pivot_drop_pct_r3",
    "hidden_pivot_gap_r3",
    "hidden_pivot_drop_pct_r3",
    "pivot2_date_r3",
)

_EVENT_DEFINITIONS: tuple[dict[str, str], ...] = (
    {
        "flag": "is_bullish_divergence_r2",
        "divergence_pattern": "Bullish Divergence R2",
        "divergence_group": "BULLISH_DIVERGENCE",
        "divergence_variant": "REGULAR",
        "divergence_direction": "BULLISH",
        "divergence_radius": "R2",
        "signal_strength_field": "bullish_strength",
        "pivot_gap_field": "pivot_gap_r2",
        "pivot_drop_pct_field": "pivot_drop_pct_r2",
        "pivot2_date_field": "pivot2_date_r2",
    },
    {
        "flag": "is_bearish_divergence_r2",
        "divergence_pattern": "Bearish Divergence R2",
        "divergence_group": "BEARISH_DIVERGENCE",
        "divergence_variant": "REGULAR",
        "divergence_direction": "BEARISH",
        "divergence_radius": "R2",
        "signal_strength_field": "bearish_strength",
        "pivot_gap_field": "pivot_gap_r2",
        "pivot_drop_pct_field": "pivot_drop_pct_r2",
        "pivot2_date_field": "pivot2_date_r2",
    },
    {
        "flag": "is_hidden_bullish_divergence_r2",
        "divergence_pattern": "Hidden Bullish Divergence R2",
        "divergence_group": "HIDDEN_BULLISH_DIVERGENCE",
        "divergence_variant": "HIDDEN",
        "divergence_direction": "BULLISH",
        "divergence_radius": "R2",
        "signal_strength_field": "hidden_bullish_strength",
        "pivot_gap_field": "hidden_pivot_gap_r2",
        "pivot_drop_pct_field": "hidden_pivot_drop_pct_r2",
        "pivot2_date_field": "pivot2_date_r2",
    },
    {
        "flag": "is_hidden_bearish_divergence_r2",
        "divergence_pattern": "Hidden Bearish Divergence R2",
        "divergence_group": "HIDDEN_BEARISH_DIVERGENCE",
        "divergence_variant": "HIDDEN",
        "divergence_direction": "BEARISH",
        "divergence_radius": "R2",
        "signal_strength_field": "hidden_bearish_strength",
        "pivot_gap_field": "hidden_pivot_gap_r2",
        "pivot_drop_pct_field": "hidden_pivot_drop_pct_r2",
        "pivot2_date_field": "pivot2_date_r2",
    },
    {
        "flag": "is_bullish_divergence_r3",
        "divergence_pattern": "Bullish Divergence R3",
        "divergence_group": "BULLISH_DIVERGENCE",
        "divergence_variant": "REGULAR",
        "divergence_direction": "BULLISH",
        "divergence_radius": "R3",
        "signal_strength_field": "bullish_strength",
        "pivot_gap_field": "pivot_gap_r3",
        "pivot_drop_pct_field": "pivot_drop_pct_r3",
        "pivot2_date_field": "pivot2_date_r3",
    },
    {
        "flag": "is_bearish_divergence_r3",
        "divergence_pattern": "Bearish Divergence R3",
        "divergence_group": "BEARISH_DIVERGENCE",
        "divergence_variant": "REGULAR",
        "divergence_direction": "BEARISH",
        "divergence_radius": "R3",
        "signal_strength_field": "bearish_strength",
        "pivot_gap_field": "pivot_gap_r3",
        "pivot_drop_pct_field": "pivot_drop_pct_r3",
        "pivot2_date_field": "pivot2_date_r3",
    },
    {
        "flag": "is_hidden_bullish_divergence_r3",
        "divergence_pattern": "Hidden Bullish Divergence R3",
        "divergence_group": "HIDDEN_BULLISH_DIVERGENCE",
        "divergence_variant": "HIDDEN",
        "divergence_direction": "BULLISH",
        "divergence_radius": "R3",
        "signal_strength_field": "hidden_bullish_strength",
        "pivot_gap_field": "hidden_pivot_gap_r3",
        "pivot_drop_pct_field": "hidden_pivot_drop_pct_r3",
        "pivot2_date_field": "pivot2_date_r3",
    },
    {
        "flag": "is_hidden_bearish_divergence_r3",
        "divergence_pattern": "Hidden Bearish Divergence R3",
        "divergence_group": "HIDDEN_BEARISH_DIVERGENCE",
        "divergence_variant": "HIDDEN",
        "divergence_direction": "BEARISH",
        "divergence_radius": "R3",
        "signal_strength_field": "hidden_bearish_strength",
        "pivot_gap_field": "hidden_pivot_gap_r3",
        "pivot_drop_pct_field": "hidden_pivot_drop_pct_r3",
        "pivot2_date_field": "pivot2_date_r3",
    },
)


@dataclass
class DivergenceSignalRawExport:
    divergence_context_snapshot_rows: list[dict[str, Any]]
    divergence_signal_rows_60td: list[dict[str, Any]]

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        return asdict(self)


def read_divergence_signal_raw_export(
    analysis_db_path: str,
    osakedata_db_path: str,
    ticker: str,
    as_of_date: str,
    market: str | None = None,
    recent_window_trading_days: int = 60,
) -> DivergenceSignalRawExport:
    analysis_path = Path(analysis_db_path).resolve()
    osakedata_path = Path(osakedata_db_path).resolve()

    with _connect_readonly(analysis_path) as analysis_conn, _connect_readonly(osakedata_path) as osakedata_conn:
        valid_trading_dates_desc = _fetch_valid_trading_dates(
            osakedata_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=market,
            limit=recent_window_trading_days,
        )
        window_meta = _derive_window_metadata(
            valid_trading_dates_desc,
            recent_window_trading_days=recent_window_trading_days,
        )
        latest_valid_close_date = _fetch_latest_valid_close_date(
            osakedata_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=market,
        )
        latest_row = _fetch_latest_row(analysis_conn, ticker=ticker, as_of_date=as_of_date)
        latest_signal_row = _fetch_latest_signal_row(analysis_conn, ticker=ticker, as_of_date=as_of_date)
        latest_divergence_date = None if latest_row is None else latest_row["date"]
        coverage_status, coverage_reason = _derive_coverage_status(
            latest_valid_close_date=latest_valid_close_date,
            latest_divergence_date=latest_divergence_date,
        )
        warning_flags = _derive_warning_flags(
            coverage_status=coverage_status,
            latest_signal_row=latest_signal_row,
        )
        signal_rows_db = _fetch_recent_signal_rows(
            analysis_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            window_start_date=window_meta["sequence_window_start_date"],
        )
        context_row = _build_context_snapshot_row(
            ticker=ticker,
            market=market,
            as_of_date=as_of_date,
            window_meta=window_meta,
            latest_valid_close_date=latest_valid_close_date,
            latest_divergence_date=latest_divergence_date,
            coverage_status=coverage_status,
            coverage_reason=coverage_reason,
            latest_row=latest_row,
            latest_signal_row=latest_signal_row,
            warning_flags=warning_flags,
        )
        signal_rows = _build_signal_rows(
            signal_rows=signal_rows_db,
            ticker=ticker,
            market=market,
            as_of_date=as_of_date,
            window_meta=window_meta,
        )

    return DivergenceSignalRawExport(
        divergence_context_snapshot_rows=[context_row],
        divergence_signal_rows_60td=signal_rows,
    )


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


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


def _derive_window_metadata(
    valid_trading_dates_desc: list[str],
    *,
    recent_window_trading_days: int,
) -> dict[str, Any]:
    if not valid_trading_dates_desc:
        return {
            "sequence_window_trading_days": recent_window_trading_days,
            "sequence_available_trading_days": 0,
            "sequence_window_start_date": None,
            "sequence_window_end_date": None,
        }
    return {
        "sequence_window_trading_days": recent_window_trading_days,
        "sequence_available_trading_days": len(valid_trading_dates_desc),
        "sequence_window_start_date": valid_trading_dates_desc[-1],
        "sequence_window_end_date": valid_trading_dates_desc[0],
    }


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


def _fetch_latest_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
) -> sqlite3.Row | None:
    query = """
    SELECT *
    FROM divergence_data
    WHERE ticker = ?
      AND date <= ?
    ORDER BY date DESC
    LIMIT 1
    """
    return conn.execute(query, (ticker, as_of_date)).fetchone()


def _fetch_latest_signal_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
) -> sqlite3.Row | None:
    query = f"""
    SELECT *
    FROM divergence_data
    WHERE ticker = ?
      AND date <= ?
      AND ({_signal_exists_sql()})
    ORDER BY date DESC
    LIMIT 1
    """
    return conn.execute(query, (ticker, as_of_date)).fetchone()


def _fetch_recent_signal_rows(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    window_start_date: str | None,
) -> list[sqlite3.Row]:
    if window_start_date is None:
        return []
    query = f"""
    SELECT *
    FROM divergence_data
    WHERE ticker = ?
      AND date >= ?
      AND date <= ?
      AND ({_signal_exists_sql()})
    ORDER BY date ASC
    """
    return conn.execute(query, (ticker, window_start_date, as_of_date)).fetchall()


def _signal_exists_sql() -> str:
    return """
        COALESCE(is_bullish_divergence_r2, 0) = 1
        OR COALESCE(is_bearish_divergence_r2, 0) = 1
        OR COALESCE(is_hidden_bullish_divergence_r2, 0) = 1
        OR COALESCE(is_hidden_bearish_divergence_r2, 0) = 1
        OR COALESCE(is_bullish_divergence_r3, 0) = 1
        OR COALESCE(is_bearish_divergence_r3, 0) = 1
        OR COALESCE(is_hidden_bullish_divergence_r3, 0) = 1
        OR COALESCE(is_hidden_bearish_divergence_r3, 0) = 1
    """


def _signal_exists(row: sqlite3.Row | None) -> bool:
    if row is None:
        return False
    return any(
        (
            int(row["is_bullish_divergence_r2"] or 0) == 1,
            int(row["is_bearish_divergence_r2"] or 0) == 1,
            int(row["is_hidden_bullish_divergence_r2"] or 0) == 1,
            int(row["is_hidden_bearish_divergence_r2"] or 0) == 1,
            int(row["is_bullish_divergence_r3"] or 0) == 1,
            int(row["is_bearish_divergence_r3"] or 0) == 1,
            int(row["is_hidden_bullish_divergence_r3"] or 0) == 1,
            int(row["is_hidden_bearish_divergence_r3"] or 0) == 1,
        )
    )


def _derive_coverage_status(
    *,
    latest_valid_close_date: str | None,
    latest_divergence_date: str | None,
) -> tuple[str, str]:
    if latest_valid_close_date is None:
        return "NO_VALID_CLOSE_DATA", "no valid close data on or before as_of_date"
    if latest_divergence_date is None:
        return "MISSING", "no divergence row found on or before as_of_date"
    if latest_divergence_date >= latest_valid_close_date:
        return "OK", "latest divergence date covers latest valid close date"
    if latest_divergence_date < latest_valid_close_date:
        return "STALE", "latest divergence date is older than latest valid close date"
    return "ERROR", "unsupported divergence coverage state"


def _derive_warning_flags(
    *,
    coverage_status: str,
    latest_signal_row: sqlite3.Row | None,
) -> list[str]:
    flags: list[str] = []
    if coverage_status == "MISSING":
        flags.append("DIVERGENCE_MISSING")
    if coverage_status == "STALE":
        flags.append("DIVERGENCE_STALE")
    if coverage_status == "NO_VALID_CLOSE_DATA":
        flags.append("NO_VALID_CLOSE_DATA")
    if latest_signal_row is None:
        flags.append("NO_SIGNAL_FOUND")
    return flags


def _build_context_snapshot_row(
    *,
    ticker: str,
    market: str | None,
    as_of_date: str,
    window_meta: dict[str, Any],
    latest_valid_close_date: str | None,
    latest_divergence_date: str | None,
    coverage_status: str,
    coverage_reason: str,
    latest_row: sqlite3.Row | None,
    latest_signal_row: sqlite3.Row | None,
    warning_flags: list[str],
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "market": market,
        "as_of_date": as_of_date,
        "sequence_window_trading_days": window_meta["sequence_window_trading_days"],
        "sequence_available_trading_days": window_meta["sequence_available_trading_days"],
        "sequence_window_start_date": window_meta["sequence_window_start_date"],
        "sequence_window_end_date": window_meta["sequence_window_end_date"],
        "latest_valid_close_date_on_or_before_as_of_date": latest_valid_close_date,
        "latest_divergence_date_on_or_before_as_of_date": latest_divergence_date,
        "divergence_coverage_status": coverage_status,
        "divergence_coverage_reason": coverage_reason,
        "latest_row_found": latest_row is not None,
        "latest_row_date": None if latest_row is None else latest_row["date"],
        "latest_row_bullish_strength": None if latest_row is None else latest_row["bullish_strength"],
        "latest_row_bearish_strength": None if latest_row is None else latest_row["bearish_strength"],
        "latest_row_hidden_bullish_strength": None if latest_row is None else latest_row["hidden_bullish_strength"],
        "latest_row_hidden_bearish_strength": None if latest_row is None else latest_row["hidden_bearish_strength"],
        "latest_row_rsi": None if latest_row is None else latest_row["rsi"],
        "latest_row_is_bullish_divergence_r2": None if latest_row is None else latest_row["is_bullish_divergence_r2"],
        "latest_row_is_bearish_divergence_r2": None if latest_row is None else latest_row["is_bearish_divergence_r2"],
        "latest_row_is_hidden_bullish_divergence_r2": None if latest_row is None else latest_row["is_hidden_bullish_divergence_r2"],
        "latest_row_is_hidden_bearish_divergence_r2": None if latest_row is None else latest_row["is_hidden_bearish_divergence_r2"],
        "latest_row_is_bullish_divergence_r3": None if latest_row is None else latest_row["is_bullish_divergence_r3"],
        "latest_row_is_bearish_divergence_r3": None if latest_row is None else latest_row["is_bearish_divergence_r3"],
        "latest_row_is_hidden_bullish_divergence_r3": None if latest_row is None else latest_row["is_hidden_bullish_divergence_r3"],
        "latest_row_is_hidden_bearish_divergence_r3": None if latest_row is None else latest_row["is_hidden_bearish_divergence_r3"],
        "latest_signal_found": latest_signal_row is not None,
        "latest_signal_date": None if latest_signal_row is None else latest_signal_row["date"],
        "latest_signal_pattern": None if latest_signal_row is None else _first_signal_event(latest_signal_row)["divergence_pattern"],
        "latest_signal_group": None if latest_signal_row is None else _first_signal_event(latest_signal_row)["divergence_group"],
        "latest_signal_radius": None if latest_signal_row is None else _first_signal_event(latest_signal_row)["divergence_radius"],
        "latest_signal_source_flag": None if latest_signal_row is None else _first_signal_event(latest_signal_row)["source_flag"],
        "divergence_warning_flags": warning_flags,
    }


def _build_signal_rows(
    *,
    signal_rows: list[sqlite3.Row],
    ticker: str,
    market: str | None,
    as_of_date: str,
    window_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sequence_index = 1
    for row in signal_rows:
        for event_row in _expand_signal_events(
            row,
            ticker=ticker,
            market=market,
            as_of_date=as_of_date,
            window_meta=window_meta,
            sequence_index_start=sequence_index,
        ):
            rows.append(event_row)
            sequence_index += 1
    return rows


def _expand_signal_events(
    row: sqlite3.Row,
    *,
    ticker: str,
    market: str | None,
    as_of_date: str,
    window_meta: dict[str, Any],
    sequence_index_start: int,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    sequence_index = sequence_index_start
    for event_def in _EVENT_DEFINITIONS:
        if int(row[event_def["flag"]] or 0) != 1:
            continue
        events.append(
            {
                "ticker": ticker,
                "market": market,
                "as_of_date": as_of_date,
                "sequence_window_trading_days": window_meta["sequence_window_trading_days"],
                "sequence_available_trading_days": window_meta["sequence_available_trading_days"],
                "sequence_window_start_date": window_meta["sequence_window_start_date"],
                "sequence_window_end_date": window_meta["sequence_window_end_date"],
                "sequence_index": sequence_index,
                "signal_date": row["date"],
                "divergence_pattern": event_def["divergence_pattern"],
                "divergence_group": event_def["divergence_group"],
                "divergence_variant": event_def["divergence_variant"],
                "divergence_direction": event_def["divergence_direction"],
                "divergence_radius": event_def["divergence_radius"],
                "signal_strength": row[event_def["signal_strength_field"]],
                "rsi": row["rsi"],
                "pivot_gap": row[event_def["pivot_gap_field"]],
                "pivot_drop_pct": row[event_def["pivot_drop_pct_field"]],
                "pivot2_date": row[event_def["pivot2_date_field"]],
                "source_flag": event_def["flag"],
            }
        )
        sequence_index += 1
    return events


def _first_signal_event(row: sqlite3.Row) -> dict[str, Any] | None:
    events = _expand_signal_events(
        row,
        ticker=str(row["ticker"]),
        market=None,
        as_of_date=str(row["date"]),
        window_meta={
            "sequence_window_trading_days": 0,
            "sequence_available_trading_days": 0,
            "sequence_window_start_date": None,
            "sequence_window_end_date": None,
        },
        sequence_index_start=1,
    )
    return None if not events else events[0]
