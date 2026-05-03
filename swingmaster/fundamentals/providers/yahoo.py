from __future__ import annotations

from typing import Any

import pandas as pd


def _get_yfinance_module() -> Any:
    import yfinance as yf

    return yf


class YahooFinanceClient:
    def __init__(self) -> None:
        self._yf = _get_yfinance_module()

    def get_quarterly_income_stmt(self, symbol: str) -> pd.DataFrame:
        return self._normalize_statement_frame(getattr(self._get_ticker(symbol), "quarterly_income_stmt", pd.DataFrame()))

    def get_quarterly_balance_sheet(self, symbol: str) -> pd.DataFrame:
        return self._normalize_statement_frame(
            getattr(self._get_ticker(symbol), "quarterly_balance_sheet", pd.DataFrame())
        )

    def get_quarterly_cashflow(self, symbol: str) -> pd.DataFrame:
        return self._normalize_statement_frame(getattr(self._get_ticker(symbol), "quarterly_cashflow", pd.DataFrame()))

    def get_info(self, symbol: str) -> dict[str, Any]:
        return self._normalize_mapping(getattr(self._get_ticker(symbol), "info", {}))

    def get_fast_info(self, symbol: str) -> dict[str, Any]:
        fast_info = getattr(self._get_ticker(symbol), "fast_info", {})
        try:
            return self._normalize_mapping(dict(fast_info))
        except Exception:
            return self._normalize_mapping(fast_info)

    def get_raw_payload(self, symbol: str) -> dict[str, Any]:
        ticker = self._get_ticker(symbol)
        return {
            "info": self._normalize_mapping(getattr(ticker, "info", {})),
            "fast_info": self._normalize_fast_info(getattr(ticker, "fast_info", {})),
            "quarterly_income_stmt": self._statement_to_payload(
                getattr(ticker, "quarterly_income_stmt", pd.DataFrame())
            ),
            "quarterly_balance_sheet": self._statement_to_payload(
                getattr(ticker, "quarterly_balance_sheet", pd.DataFrame())
            ),
            "quarterly_cashflow": self._statement_to_payload(
                getattr(ticker, "quarterly_cashflow", pd.DataFrame())
            ),
        }

    def _get_ticker(self, symbol: str) -> Any:
        return self._yf.Ticker(symbol)

    def _normalize_fast_info(self, fast_info: Any) -> dict[str, Any]:
        try:
            return self._normalize_mapping(dict(fast_info))
        except Exception:
            return self._normalize_mapping(fast_info)

    def _normalize_mapping(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            items = value.items()
        else:
            try:
                items = dict(value).items()
            except Exception:
                return {}
        normalized: dict[str, Any] = {}
        for key, item in items:
            normalized[str(key)] = self._normalize_scalar(item)
        return normalized

    def _statement_to_payload(self, dataframe: Any) -> dict[str, Any]:
        normalized = self._normalize_statement_frame(dataframe)
        return {
            "index": [str(item) for item in normalized.index],
            "columns": [str(item) for item in normalized.columns],
            "data": [
                [self._normalize_scalar(normalized.iat[row_idx, col_idx]) for col_idx in range(len(normalized.columns))]
                for row_idx in range(len(normalized.index))
            ],
        }

    def _normalize_statement_frame(self, dataframe: Any) -> pd.DataFrame:
        if not isinstance(dataframe, pd.DataFrame):
            return pd.DataFrame()
        normalized = dataframe.copy()
        normalized.index = normalized.index.map(str)
        normalized.columns = [pd.Timestamp(column_name).strftime("%Y-%m-%d") for column_name in normalized.columns]
        normalized = normalized.reindex(sorted(normalized.columns), axis=1)
        return normalized

    def _normalize_scalar(self, value: Any) -> Any:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
