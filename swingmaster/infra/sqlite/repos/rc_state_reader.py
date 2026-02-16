"""SQLite reader for rc_state_daily history queries."""

from __future__ import annotations

import sqlite3
from typing import Optional, Tuple


class RcStateReader:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._has_state_attrs_json = self._table_has_column("rc_state_daily", "state_attrs_json")

    def _table_has_column(self, table_name: str, column_name: str) -> bool:
        try:
            rows = self._conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        except Exception:
            return False
        for row in rows:
            try:
                if row["name"] == column_name:
                    return True
            except Exception:
                if len(row) > 1 and row[1] == column_name:
                    return True
        return False

    def get_latest_before(
        self, ticker: str, as_of_date: str
    ) -> Optional[Tuple[str, Optional[int], int, Optional[str]]]:
        if self._has_state_attrs_json:
            row = self._conn.execute(
                """
                SELECT state, confidence, age, state_attrs_json
                FROM rc_state_daily
                WHERE ticker = ? AND date < ?
                ORDER BY date DESC
                LIMIT 1
                """,
                (ticker, as_of_date),
            ).fetchone()
        else:
            row = self._conn.execute(
                """
                SELECT state, confidence, age, NULL AS state_attrs_json
                FROM rc_state_daily
                WHERE ticker = ? AND date < ?
                ORDER BY date DESC
                LIMIT 1
                """,
                (ticker, as_of_date),
            ).fetchone()

        if row is None:
            return None

        state_value: str = row["state"]
        confidence_value: Optional[int] = row["confidence"]
        age_value: int = row["age"]
        status_value: Optional[str] = row["state_attrs_json"]
        return state_value, confidence_value, age_value, status_value
