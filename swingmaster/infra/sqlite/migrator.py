"""SQLite schema migration helpers for rc_state and related tables.

Responsibilities:
  - Create/upgrade schema deterministically.
Must not:
  - Embed business logic; migrations only.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


MACRO_MIGRATIONS = {
    "012_rc_macro_source_raw.sql",
    "013_macro_source_daily.sql",
    "014_rc_risk_appetite_daily.sql",
    "015_macro_source_daily_is_forward_filled.sql",
}


def _apply_selected_migrations(conn: sqlite3.Connection, migration_names: set[str] | None = None) -> None:
    migrations_dir = Path(__file__).resolve().parent / "migrations"
    for migration in sorted(migrations_dir.glob("*.sql")):
        if migration_names is not None and migration.name not in migration_names:
            continue
        sql_text = migration.read_text()
        try:
            conn.executescript(sql_text)
        except sqlite3.OperationalError as exc:
            msg = str(exc).lower()
            if "duplicate column name" in msg:
                continue
            raise


def apply_migrations(conn: sqlite3.Connection) -> None:
    _apply_selected_migrations(conn)


def apply_macro_migrations(conn: sqlite3.Connection) -> None:
    _apply_selected_migrations(conn, MACRO_MIGRATIONS)
