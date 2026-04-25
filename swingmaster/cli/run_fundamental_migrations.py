from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


REQUIRED_TABLES = (
    "rc_fundamental_schema_version",
    "rc_fundamental_run",
    "rc_fundamental_statement_raw",
    "rc_fundamental_quarterly",
    "rc_fundamental_ttm",
)
SCHEMA_VERSION = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply SwingMaster fundamentals SQLite schema migrations")
    parser.add_argument("--db", required=True, help="SQLite database path")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def get_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "001_create_fundamentals_schema.sql"


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def apply_fundamental_migration(conn: sqlite3.Connection, migration_file: Path) -> None:
    sql_text = migration_file.read_text(encoding="utf-8")
    conn.executescript(sql_text)
    conn.commit()


def validate_fundamental_schema(conn: sqlite3.Connection) -> int:
    existing_tables = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            """
        )
    }
    missing_tables = [table_name for table_name in REQUIRED_TABLES if table_name not in existing_tables]
    if missing_tables:
        raise RuntimeError(f"FUNDAMENTAL_TABLES_MISSING:{','.join(missing_tables)}")

    version_row = conn.execute(
        """
        SELECT 1
        FROM rc_fundamental_schema_version
        WHERE version = ?
        """,
        (SCHEMA_VERSION,),
    ).fetchone()
    if version_row is None:
        raise RuntimeError(f"FUNDAMENTAL_SCHEMA_VERSION_MISSING:{SCHEMA_VERSION}")

    return len(REQUIRED_TABLES)


def run_migration(db_path: Path) -> tuple[Path, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    migration_file = get_migration_file_path()
    with sqlite3.connect(str(db_path)) as conn:
        apply_fundamental_migration(conn, migration_file)
        tables_created = validate_fundamental_schema(conn)
    return migration_file, tables_created


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    migration_file, tables_created = run_migration(db_path)
    _summary(db_path=str(db_path))
    _summary(migration_file=str(migration_file))
    _summary(tables_created=tables_created)
    _summary(schema_version=SCHEMA_VERSION)
    _summary(status="ok")


if __name__ == "__main__":
    main()
