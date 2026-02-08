"""SQLite connection helpers for read-only access."""

from __future__ import annotations

import sqlite3


def get_readonly_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn
