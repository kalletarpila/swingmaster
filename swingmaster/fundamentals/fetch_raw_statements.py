from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import sqlite3


SUPPORTED_STATEMENT_TYPES = ("income", "balance", "cashflow")
PERIOD_TYPE = "quarterly"
SOURCE_NAME = "yfinance"


def _get_yfinance_module() -> Any:
    import yfinance as yf

    return yf


def fetch_quarterly_statements_raw(ticker: str) -> dict[str, pd.DataFrame]:
    try:
        yf = _get_yfinance_module()
        yf_ticker = yf.Ticker(ticker)
        statements = {
            "income": yf_ticker.quarterly_income_stmt,
            "balance": yf_ticker.quarterly_balance_sheet,
            "cashflow": yf_ticker.quarterly_cashflow,
        }
    except Exception as exc:
        raise RuntimeError(f"FUNDAMENTAL_FETCH_FAILED:{ticker}:{type(exc).__name__}:{exc}") from exc
    return {statement_type: _normalize_statement_frame(dataframe) for statement_type, dataframe in statements.items()}


def _normalize_statement_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("FUNDAMENTAL_STATEMENT_NOT_DATAFRAME")
    normalized = dataframe.copy()
    normalized.index = normalized.index.map(str)
    normalized.columns = [_normalize_period_end_date(column_name) for column_name in normalized.columns]
    return normalized


def _normalize_period_end_date(value: Any) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def validate_non_empty_statements(statements: dict[str, pd.DataFrame]) -> None:
    for statement_type in SUPPORTED_STATEMENT_TYPES:
        dataframe = statements.get(statement_type)
        if dataframe is None or dataframe.empty:
            raise RuntimeError(f"FUNDAMENTAL_STATEMENT_EMPTY:{statement_type}")


def count_statement_rows(dataframe: pd.DataFrame) -> int:
    return len(dataframe.index) * len(dataframe.columns)


def insert_raw_statement_rows(
    conn: sqlite3.Connection,
    ticker: str,
    statement_type: str,
    dataframe: pd.DataFrame,
    run_id: str,
) -> int:
    retrieved_at_utc = datetime.utcnow().replace(microsecond=0).isoformat()
    rows: list[tuple[object, ...]] = []
    for field_name in dataframe.index:
        for period_end_date in dataframe.columns:
            field_value = _normalize_field_value(dataframe.at[field_name, period_end_date])
            rows.append(
                (
                    ticker,
                    statement_type,
                    period_end_date,
                    PERIOD_TYPE,
                    field_name,
                    field_value,
                    None,
                    SOURCE_NAME,
                    retrieved_at_utc,
                    run_id,
                )
            )
    conn.executemany(
        """
        INSERT OR REPLACE INTO rc_fundamental_statement_raw (
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _normalize_field_value(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)
