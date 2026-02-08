"""SQLite schema migration helpers for rc_state and related tables.

Responsibilities:
  - Create/upgrade schema deterministically.
Must not:
  - Embed business logic; migrations only.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


def apply_migrations(conn: sqlite3.Connection) -> None:
    migrations_dir = Path(__file__).resolve().parent / "migrations"
    for migration in sorted(migrations_dir.glob("*.sql")):
        sql_text = migration.read_text()
        conn.executescript(sql_text)
