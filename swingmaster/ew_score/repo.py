from __future__ import annotations

import sqlite3
from typing import Any


def ensure_rc_ew_score_daily_dual_mode_columns(conn: sqlite3.Connection) -> None:
    table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'rc_ew_score_daily'
        """
    ).fetchone()
    if table_exists is None:
        raise ValueError("rc_ew_score_daily table does not exist")

    cols = {row[1] for row in conn.execute("PRAGMA table_info(rc_ew_score_daily)").fetchall()}
    if "ew_score_rolling" not in cols:
        conn.execute("ALTER TABLE rc_ew_score_daily ADD COLUMN ew_score_rolling REAL")
    if "ew_level_rolling" not in cols:
        conn.execute("ALTER TABLE rc_ew_score_daily ADD COLUMN ew_level_rolling INTEGER")
    if "ew_rule_fastpass" not in cols:
        conn.execute("ALTER TABLE rc_ew_score_daily ADD COLUMN ew_rule_fastpass TEXT")
    if "ew_rule_rolling" not in cols:
        conn.execute("ALTER TABLE rc_ew_score_daily ADD COLUMN ew_rule_rolling TEXT")
    if "inputs_json_fastpass" not in cols:
        conn.execute("ALTER TABLE rc_ew_score_daily ADD COLUMN inputs_json_fastpass TEXT")
    if "inputs_json_rolling" not in cols:
        conn.execute("ALTER TABLE rc_ew_score_daily ADD COLUMN inputs_json_rolling TEXT")
    conn.commit()


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
        ensure_rc_ew_score_daily_dual_mode_columns(self._conn)

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
            SELECT ew_score_day3, ew_level_day3, ew_rule, inputs_json
            FROM rc_ew_score_daily
            WHERE ticker = ? AND date = ?
            """,
            (ticker, date),
        ).fetchone()
        ew_score_day3 = (
            float(existing[0]) if existing is not None and existing[0] is not None else 0.0
        )
        ew_level_day3 = (
            int(existing[1]) if existing is not None and existing[1] is not None else 0
        )
        legacy_ew_rule = str(existing[2]) if existing is not None else ""
        legacy_inputs_json = str(existing[3]) if existing is not None else "{}"
        self._conn.execute(
            """
            INSERT INTO rc_ew_score_daily (
              ticker,
              date,
              ew_score_day3,
              ew_level_day3,
              ew_score_fastpass,
              ew_level_fastpass,
              ew_rule_fastpass,
              inputs_json_fastpass,
              ew_rule,
              inputs_json,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(ticker, date) DO UPDATE SET
              ew_score_fastpass = excluded.ew_score_fastpass,
              ew_level_fastpass = excluded.ew_level_fastpass,
              ew_rule_fastpass = excluded.ew_rule_fastpass,
              inputs_json_fastpass = excluded.inputs_json_fastpass
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
                legacy_ew_rule,
                legacy_inputs_json,
            ),
        )
        self._conn.commit()

    def upsert_rolling_row(
        self,
        ticker: str,
        date: str,
        ew_score_rolling: float,
        ew_level_rolling: int,
        ew_rule_rolling: str,
        inputs_json_rolling: str,
    ) -> None:
        existing = self._conn.execute(
            """
            SELECT ew_score_day3, ew_level_day3, ew_rule, inputs_json
            FROM rc_ew_score_daily
            WHERE ticker = ? AND date = ?
            """,
            (ticker, date),
        ).fetchone()
        ew_score_day3 = (
            float(existing[0]) if existing is not None and existing[0] is not None else 0.0
        )
        ew_level_day3 = (
            int(existing[1]) if existing is not None and existing[1] is not None else 0
        )
        legacy_ew_rule = str(existing[2]) if existing is not None else ""
        legacy_inputs_json = str(existing[3]) if existing is not None else "{}"
        self._conn.execute(
            """
            INSERT INTO rc_ew_score_daily (
              ticker,
              date,
              ew_score_day3,
              ew_level_day3,
              ew_score_rolling,
              ew_level_rolling,
              ew_rule_rolling,
              inputs_json_rolling,
              ew_rule,
              inputs_json,
              created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(ticker, date) DO UPDATE SET
              ew_score_rolling = excluded.ew_score_rolling,
              ew_level_rolling = excluded.ew_level_rolling,
              ew_rule_rolling = excluded.ew_rule_rolling,
              inputs_json_rolling = excluded.inputs_json_rolling
            """,
            (
                ticker,
                date,
                ew_score_day3,
                ew_level_day3,
                ew_score_rolling,
                ew_level_rolling,
                ew_rule_rolling,
                inputs_json_rolling,
                legacy_ew_rule,
                legacy_inputs_json,
            ),
        )
        self._conn.commit()
