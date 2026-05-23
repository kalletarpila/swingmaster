from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
import sqlite3
from typing import Any


TECHNICAL_SIGNAL_RELEVANCE_COLUMNS: tuple[str, ...] = (
    "ticker",
    "timeframe",
    "signal_date",
    "signal_confirmed_as_of_date",
    "signal_name",
    "signal_close_price",
    "signal_direction",
    "signal_family",
    "signal_source_type",
    "signal_source_id",
    "dow_trend_state",
    "dow_context_state",
    "latest_bos_direction",
    "bars_since_latest_bos",
    "latest_reset_reason",
    "bars_since_latest_reset",
    "near_latest_pivot",
    "near_active_bos_level",
    "is_trend_aligned",
    "is_counter_trend",
    "relevance_class",
    "relevance_reason",
    "relevance_rule_version",
    "mapping_version",
    "reason_version",
    "rule_trace",
    "created_at_utc",
    "run_id",
)


@dataclass
class TechnicalSignalRelevanceRawExport:
    run_id: str
    ticker: str
    timeframe: str
    as_of_date: str
    lookback_days: int
    rows: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def read_technical_signal_relevance_raw_export(
    analysis_db_path: str,
    ticker: str,
    run_id: str,
    as_of_date: str,
    lookback_days: int = 45,
    timeframe: str = "1d",
) -> TechnicalSignalRelevanceRawExport:
    validated_db_path = _validate_non_empty_text("analysis_db_path", analysis_db_path)
    validated_run_id = _validate_non_empty_text("run_id", run_id)
    validated_ticker = _validate_non_empty_text("ticker", ticker)
    validated_timeframe = _validate_non_empty_text("timeframe", timeframe)
    validated_as_of_date = _validate_date_text(as_of_date)
    validated_lookback_days = _validate_lookback_days(lookback_days)
    lookback_start_date = (date.fromisoformat(validated_as_of_date) - timedelta(days=validated_lookback_days)).isoformat()
    analysis_path = Path(validated_db_path).resolve()

    with _connect_readonly(analysis_path) as conn:
        rows = _fetch_relevance_rows(
            conn,
            ticker=validated_ticker,
            run_id=validated_run_id,
            timeframe=validated_timeframe,
            as_of_date=validated_as_of_date,
            lookback_start_date=lookback_start_date,
        )

    return TechnicalSignalRelevanceRawExport(
        run_id=validated_run_id,
        ticker=validated_ticker,
        timeframe=validated_timeframe,
        as_of_date=validated_as_of_date,
        lookback_days=validated_lookback_days,
        rows=rows,
    )


def _validate_non_empty_text(field_name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _validate_date_text(value: str) -> str:
    normalized = _validate_non_empty_text("as_of_date", value)
    try:
        date.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("as_of_date must be a valid YYYY-MM-DD string") from exc
    return normalized


def _validate_lookback_days(value: int) -> int:
    if value < 0:
        raise ValueError("lookback_days must be >= 0")
    return value


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_relevance_rows(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    run_id: str,
    timeframe: str,
    as_of_date: str,
    lookback_start_date: str,
) -> list[dict[str, Any]]:
    columns_sql = ", ".join(TECHNICAL_SIGNAL_RELEVANCE_COLUMNS)
    query = f"""
    SELECT {columns_sql}
    FROM technical_signal_relevance
    WHERE run_id = ?
      AND ticker = ?
      AND timeframe = ?
      AND signal_confirmed_as_of_date <= ?
      AND signal_confirmed_as_of_date >= ?
    ORDER BY
      signal_confirmed_as_of_date DESC,
      signal_date DESC,
      signal_name ASC,
      signal_source_type ASC,
      COALESCE(signal_source_id, '') ASC
    """
    try:
        db_rows = conn.execute(query, (run_id, ticker, timeframe, as_of_date, lookback_start_date)).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            raise RuntimeError("TECHNICAL_SIGNAL_RELEVANCE_TABLE_NOT_FOUND") from exc
        raise
    return [_row_to_dict(row) for row in db_rows]


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {column: row[column] for column in TECHNICAL_SIGNAL_RELEVANCE_COLUMNS}
