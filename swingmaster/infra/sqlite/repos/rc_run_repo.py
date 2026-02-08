"""SQLite repository for run metadata (rc_run)."""

from __future__ import annotations

import sqlite3


class RcRunRepo:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def insert_run(
        self,
        run_id: str,
        created_at: str,
        engine_version: str,
        policy_id: str,
        policy_version: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO rc_run (run_id, created_at, engine_version, policy_id, policy_version)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, created_at, engine_version, policy_id, policy_version),
        )
