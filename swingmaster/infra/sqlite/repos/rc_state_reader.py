from __future__ import annotations

import sqlite3
from typing import Optional, Tuple


class RcStateReader:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def get_latest_before(
        self, ticker: str, as_of_date: str
    ) -> Optional[Tuple[str, Optional[int], int, Optional[str]]]:
        row = self._conn.execute(
            """
            SELECT state, confidence, age
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
        return state_value, confidence_value, age_value, None
