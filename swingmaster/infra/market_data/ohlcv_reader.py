"""Abstractions and helpers for OHLCV data access."""

from __future__ import annotations

import re
import sqlite3
from typing import List


_IDENT_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_identifier(name: str) -> str:
    if not _IDENT_PATTERN.fullmatch(name):
        raise ValueError("Invalid identifier")
    return name


class OhlcvReader:
    def __init__(
        self,
        conn: sqlite3.Connection,
        table_name: str = "daily_ohlcv",
        ticker_col: str = "ticker",
        date_col: str = "date",
        close_col: str = "close",
    ) -> None:
        self._conn = conn
        self._table_name = _validate_identifier(table_name)
        self._ticker_col = _validate_identifier(ticker_col)
        self._date_col = _validate_identifier(date_col)
        self._close_col = _validate_identifier(close_col)

    def has_row(self, ticker: str, date: str) -> bool:
        query = (
            f"SELECT 1 FROM {self._table_name} "
            f"WHERE {self._ticker_col}=? AND {self._date_col}=? LIMIT 1"
        )
        row = self._conn.execute(query, (ticker, date)).fetchone()
        return row is not None

    def get_last_n_closes(self, ticker: str, as_of_date: str, n: int) -> List[float]:
        pairs = self.get_last_n_date_closes(ticker, as_of_date, n)
        return [close for _, close in pairs]

    def get_last_n_date_closes(
        self, ticker: str, as_of_date: str, n: int
    ) -> List[tuple[str, float]]:
        query = (
            f"SELECT {self._date_col}, {self._close_col} FROM {self._table_name} "
            f"WHERE {self._ticker_col}=? AND {self._date_col}<=? "
            f"ORDER BY {self._date_col} DESC LIMIT ?"
        )
        rows = self._conn.execute(query, (ticker, as_of_date, n)).fetchall()
        return [(row[0], float(row[1])) for row in rows]
