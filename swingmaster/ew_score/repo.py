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
              ew_score_fastpass REAL,
              ew_level_fastpass INTEGER,
              ew_rule TEXT NOT NULL,
              inputs_json TEXT NOT NULL,
              created_at TEXT NOT NULL DEFAULT (datetime('now')),
              PRIMARY KEY (ticker, date)
            )
            """
        )
        cols = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(rc_ew_score_daily)").fetchall()
        }
        if "ew_score_fastpass" not in cols:
            self._conn.execute(
                "ALTER TABLE rc_ew_score_daily ADD COLUMN ew_score_fastpass REAL"
            )
        if "ew_level_fastpass" not in cols:
            self._conn.execute(
                "ALTER TABLE rc_ew_score_daily ADD COLUMN ew_level_fastpass INTEGER"
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
              ew_score_fastpass,
              ew_level_fastpass,
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

    def upsert_fastpass_row(
        self,
        ticker: str,
        date: str,
        ew_score_fastpass: float,
        ew_level_fastpass: int,
        ew_rule: str,
        inputs_json: str,
    ) -> None:
        existing = self._conn.execute(
            """
            SELECT ew_score_day3, ew_level_day3
            FROM rc_ew_score_daily
            WHERE ticker = ? AND date = ?
            """,
            (ticker, date),
        ).fetchone()
        ew_score_day3 = float(existing[0]) if existing is not None else 0.0
        ew_level_day3 = int(existing[1]) if existing is not None else 0
        self._conn.execute(
            """
            INSERT INTO rc_ew_score_daily (
              ticker,
              date,
              ew_score_day3,
              ew_level_day3,
              ew_score_fastpass,
              ew_level_fastpass,
              ew_rule,
              inputs_json,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(ticker, date) DO UPDATE SET
              ew_score_fastpass = excluded.ew_score_fastpass,
              ew_level_fastpass = excluded.ew_level_fastpass,
              ew_rule = excluded.ew_rule,
              inputs_json = excluded.inputs_json,
              created_at = excluded.created_at
            """,
            (
                ticker,
                date,
                ew_score_day3,
                ew_level_day3,
                ew_score_fastpass,
                ew_level_fastpass,
                ew_rule,
                inputs_json,
            ),
        )
        self._conn.commit()
