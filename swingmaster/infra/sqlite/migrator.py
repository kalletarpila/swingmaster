from __future__ import annotations

import sqlite3
from pathlib import Path


def apply_migrations(conn: sqlite3.Connection) -> None:
    migration_path = Path(__file__).resolve().parent / "migrations" / "001_init.sql"
    sql_text = migration_path.read_text()
    conn.executescript(sql_text)
