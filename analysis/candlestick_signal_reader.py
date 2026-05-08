from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import sqlite3
from typing import Any


_BULLISH_PATTERNS = (
    "Hammer",
    "Bullish Engulfing",
    "Piercing Pattern",
    "Three White Soldiers",
    "Morning Star",
    "Dragonfly Doji",
)
_BEARISH_PATTERNS = (
    "Bearish Engulfing",
    "Shooting Star",
    "Dark Cloud Cover",
    "Evening Star",
    "Hanging Man",
)
_ALLOWED_PATTERNS = _BULLISH_PATTERNS + _BEARISH_PATTERNS


@dataclass
class CandlestickSignalRawExport:
    candlestick_event_rows_60td: list[dict[str, Any]]

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        return asdict(self)


def read_candlestick_signal_raw_export(
    analysis_db_path: str,
    osakedata_db_path: str,
    ticker: str,
    as_of_date: str,
    market: str | None = None,
    recent_window_trading_days: int = 60,
) -> CandlestickSignalRawExport:
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
        finding_rows = _fetch_recent_finding_rows(
            analysis_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            window_start_date=window_meta["sequence_window_start_date"],
        )
        event_rows = _build_candlestick_event_rows(
            finding_rows=finding_rows,
            ticker=ticker,
            market=market,
            as_of_date=as_of_date,
            window_meta=window_meta,
        )
    return CandlestickSignalRawExport(candlestick_event_rows_60td=event_rows)


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


def _fetch_recent_finding_rows(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    window_start_date: str | None,
) -> list[sqlite3.Row]:
    if window_start_date is None:
        return []
    placeholders = ", ".join("?" for _ in _ALLOWED_PATTERNS)
    query = f"""
    SELECT id, ticker, date, pattern, signal_strength, rsi14, created_at
    FROM analysis_findings
    WHERE ticker = ?
      AND date >= ?
      AND date <= ?
      AND pattern IN ({placeholders})
    ORDER BY date ASC, id ASC
    """
    params: list[Any] = [ticker, window_start_date, as_of_date, *_ALLOWED_PATTERNS]
    return conn.execute(query, params).fetchall()


def _build_candlestick_event_rows(
    *,
    finding_rows: list[sqlite3.Row],
    ticker: str,
    market: str | None,
    as_of_date: str,
    window_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, finding_row in enumerate(finding_rows, start=1):
        rows.append(
            {
                "ticker": ticker,
                "market": market,
                "as_of_date": as_of_date,
                "sequence_window_trading_days": window_meta["sequence_window_trading_days"],
                "sequence_available_trading_days": window_meta["sequence_available_trading_days"],
                "sequence_window_start_date": window_meta["sequence_window_start_date"],
                "sequence_window_end_date": window_meta["sequence_window_end_date"],
                "sequence_index": index,
                "finding_id": finding_row["id"],
                "signal_date": finding_row["date"],
                "pattern": finding_row["pattern"],
                "pattern_group": _pattern_group_for(finding_row["pattern"]),
                "signal_strength": finding_row["signal_strength"],
                "rsi14": finding_row["rsi14"],
                "created_at": finding_row["created_at"],
            }
        )
    return rows


def _pattern_group_for(pattern: str) -> str:
    if pattern in _BULLISH_PATTERNS:
        return "BULLISH_CANDLE"
    if pattern in _BEARISH_PATTERNS:
        return "BEARISH_CANDLE"
    raise ValueError(f"Unsupported pattern: {pattern}")
