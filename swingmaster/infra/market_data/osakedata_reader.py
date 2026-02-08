"""Reader for osakedata market data (OHLCV) from SQLite."""

from __future__ import annotations

import re
import sqlite3
from typing import List, Tuple

_IDENT_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_identifier(name: str) -> str:
    if not _IDENT_PATTERN.fullmatch(name):
        raise ValueError("Invalid identifier")
    return name


class OsakeDataReader:
    def __init__(self, conn: sqlite3.Connection, table_name: str = "osakedata") -> None:
        self._conn = conn
        self._table = _validate_identifier(table_name)

    def get_last_n_closes(self, ticker: str, as_of_date: str, n: int) -> List[float]:
        _validate_positive_n(n)
        _validate_non_empty("ticker", ticker)
        _validate_non_empty("as_of_date", as_of_date)
        rows = self._fetch_ohlc(ticker, as_of_date, n, columns="close")
        return [float(row[0]) for row in rows]

    def get_last_n_ohlc(
        self, ticker: str, as_of_date: str, n: int
    ) -> List[Tuple[str, float, float, float, float, float]]:
        _validate_positive_n(n)
        _validate_non_empty("ticker", ticker)
        _validate_non_empty("as_of_date", as_of_date)
        rows = self._fetch_ohlc(ticker, as_of_date, n)
        return [self._convert_row(row) for row in rows]

    def get_last_n_ohlc_required(
        self, ticker: str, as_of_date: str, n: int, require_row_on_date: bool = False
    ) -> List[Tuple[str, float, float, float, float, float]]:
        _validate_positive_n(n)
        _validate_non_empty("ticker", ticker)
        _validate_non_empty("as_of_date", as_of_date)
        if require_row_on_date and not self._has_row_on_date(ticker, as_of_date):
            return []
        rows = self._fetch_ohlc(ticker, as_of_date, n)
        return [self._convert_row(row) for row in rows]

    def list_trading_days(self, date_from: str, date_to: str) -> List[str]:
        _validate_non_empty("date_from", date_from)
        _validate_non_empty("date_to", date_to)
        if date_from > date_to:
            raise ValueError("date_from must be <= date_to")
        query = (
            f"SELECT DISTINCT pvm FROM {self._table} "
            "WHERE pvm>=? AND pvm<=? "
            "ORDER BY pvm"
        )
        rows = self._conn.execute(query, (date_from, date_to)).fetchall()
        return [row[0] for row in rows]

    def _fetch_ohlc(self, ticker: str, as_of_date: str, n: int, columns: str = "pvm, open, high, low, close, volume"):
        query = (
            f"SELECT {columns} FROM {self._table} "
            "WHERE osake=? AND pvm<=? "
            "ORDER BY pvm DESC LIMIT ?"
        )
        return self._conn.execute(query, (ticker, as_of_date, n)).fetchall()

    def _has_row_on_date(self, ticker: str, as_of_date: str) -> bool:
        query = f"SELECT 1 FROM {self._table} WHERE osake=? AND pvm=? LIMIT 1"
        row = self._conn.execute(query, (ticker, as_of_date)).fetchone()
        return row is not None

    def _convert_row(self, row: Tuple) -> Tuple[str, float, float, float, float, float]:
        return (
            row[0],
            float(row[1]),
            float(row[2]),
            float(row[3]),
            float(row[4]),
            float(row[5]),
        )


def ensure_osakedata_indexes(conn: sqlite3.Connection, table_name: str = "osakedata") -> None:
    tbl = _validate_identifier(table_name)
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{tbl}_osake_pvm "
        f"ON {tbl} (osake, pvm)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{tbl}_pvm "
        f"ON {tbl} (pvm)"
    )


def _validate_positive_n(n: int) -> None:
    if n <= 0:
        raise ValueError("n must be positive")


def _validate_non_empty(name: str, value: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{name} must be non-empty")
