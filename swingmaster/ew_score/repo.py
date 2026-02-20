from __future__ import annotations

import sqlite3
from typing import Any


class RcEwScoreDailyRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def ensure_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rc_ew_score_daily (
              ticker TEXT NOT NULL,
              date TEXT NOT NULL,
              ew_score_day3 REAL NOT NULL,
              ew_level_day3 INTEGER NOT NULL,
              ew_rule TEXT NOT NULL,
              inputs_json TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now')),
              PRIMARY KEY (ticker, date)
            )
            """
        )
        self._conn.commit()

    def upsert_row(
        self,
        ticker: str,
        date: str,
        ew_score_day3: float,
        ew_level_day3: int,
        ew_rule: str,
        inputs_json: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO rc_ew_score_daily (
              ticker,
              date,
              ew_score_day3,
              ew_level_day3,
              ew_rule,
              inputs_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, date) DO UPDATE SET
              ew_score_day3 = excluded.ew_score_day3,
              ew_level_day3 = excluded.ew_level_day3,
              ew_rule = excluded.ew_rule,
              inputs_json = excluded.inputs_json
            """,
            (ticker, date, ew_score_day3, ew_level_day3, ew_rule, inputs_json),
        )
        self._conn.commit()

    def get_row(self, ticker: str, date: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            """
            SELECT
              ticker,
              date,
              ew_score_day3,
              ew_level_day3,
              ew_rule,
              inputs_json,
              created_at
            FROM rc_ew_score_daily
            WHERE ticker = ? AND date = ?
            """,
            (ticker, date),
        )
        row = cur.fetchone()
        if row is None:
            return None
        columns = [col[0] for col in cur.description or []]
        return dict(zip(columns, row))
