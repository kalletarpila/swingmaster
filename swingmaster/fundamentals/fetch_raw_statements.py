from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import sqlite3


SUPPORTED_STATEMENT_TYPES = ("income", "balance", "cashflow")
PERIOD_TYPE = "quarterly"
SOURCE_NAME = "yfinance"
CANDIDATE_PATHS = {
    "income": (
        "ticker.quarterly_income_stmt",
        "ticker.quarterly_financials",
        'ticker.get_income_stmt(freq="quarterly")',
    ),
    "balance": (
        "ticker.quarterly_balance_sheet",
        'ticker.get_balance_sheet(freq="quarterly")',
    ),
    "cashflow": (
        "ticker.quarterly_cashflow",
        'ticker.get_cash_flow(freq="quarterly")',
    ),
}


def _get_yfinance_module() -> Any:
    import yfinance as yf

    return yf


def fetch_quarterly_statements_raw(ticker: str) -> dict[str, pd.DataFrame]:
    try:
        yf = _get_yfinance_module()
        yf_ticker = yf.Ticker(ticker)
        candidates = inspect_quarterly_statement_candidates(yf_ticker)
    except Exception as exc:
        raise RuntimeError(f"FUNDAMENTAL_FETCH_FAILED:{ticker}:{type(exc).__name__}:{exc}") from exc
    return {
        statement_type: _select_best_statement_candidate(candidates[statement_type])
        for statement_type in SUPPORTED_STATEMENT_TYPES
    }


def inspect_quarterly_statement_candidates(yf_ticker: Any) -> dict[str, list[dict[str, Any]]]:
    return {
        "income": [
            _build_candidate("ticker.quarterly_income_stmt", getattr(yf_ticker, "quarterly_income_stmt", pd.DataFrame())),
            _build_candidate("ticker.quarterly_financials", getattr(yf_ticker, "quarterly_financials", pd.DataFrame())),
            _build_candidate(
                'ticker.get_income_stmt(freq="quarterly")',
                _call_optional_method(yf_ticker, "get_income_stmt"),
            ),
        ],
        "balance": [
            _build_candidate(
                "ticker.quarterly_balance_sheet",
                getattr(yf_ticker, "quarterly_balance_sheet", pd.DataFrame()),
            ),
            _build_candidate(
                'ticker.get_balance_sheet(freq="quarterly")',
                _call_optional_method(yf_ticker, "get_balance_sheet"),
            ),
        ],
        "cashflow": [
            _build_candidate("ticker.quarterly_cashflow", getattr(yf_ticker, "quarterly_cashflow", pd.DataFrame())),
            _build_candidate(
                'ticker.get_cash_flow(freq="quarterly")',
                _call_optional_method(yf_ticker, "get_cash_flow"),
            ),
        ],
    }


def _call_optional_method(yf_ticker: Any, method_name: str) -> pd.DataFrame:
    method = getattr(yf_ticker, method_name, None)
    if method is None:
        return pd.DataFrame()
    return method(freq="quarterly")


def _build_candidate(path_name: str, dataframe: pd.DataFrame) -> dict[str, Any]:
    normalized = _normalize_statement_frame(dataframe)
    period_columns = list(normalized.columns)
    return {
        "path_name": path_name,
        "dataframe": normalized,
        "shape": normalized.shape,
        "empty": normalized.empty,
        "period_count": len(period_columns),
        "first_period": period_columns[0] if period_columns else None,
        "last_period": period_columns[-1] if period_columns else None,
        "index_head_10": list(normalized.index[:10]),
    }


def _select_best_statement_candidate(candidates: list[dict[str, Any]]) -> pd.DataFrame:
    best_candidate: dict[str, Any] | None = None
    for candidate in candidates:
        if candidate["empty"]:
            continue
        if best_candidate is None or int(candidate["period_count"]) > int(best_candidate["period_count"]):
            best_candidate = candidate
    if best_candidate is None:
        return pd.DataFrame()
    return best_candidate["dataframe"]


def _normalize_statement_frame(dataframe: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("FUNDAMENTAL_STATEMENT_NOT_DATAFRAME")
    normalized = dataframe.copy()
    normalized.index = normalized.index.map(str)
    normalized.columns = [_normalize_period_end_date(column_name) for column_name in normalized.columns]
    normalized = normalized.reindex(sorted(normalized.columns), axis=1)
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
