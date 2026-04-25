from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli.run_fundamental_migrations import (
    REQUIRED_TABLES,
    SCHEMA_VERSION,
    get_migration_file_path,
    run_migration,
    validate_fundamental_schema,
)


def test_run_migration_creates_required_tables_and_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_test.db"

    migration_file_first, tables_created_first = run_migration(db_path)
    migration_file_second, tables_created_second = run_migration(db_path)

    assert migration_file_first == get_migration_file_path()
    assert migration_file_second == get_migration_file_path()
    assert tables_created_first == len(REQUIRED_TABLES)
    assert tables_created_second == len(REQUIRED_TABLES)

    with sqlite3.connect(str(db_path)) as conn:
        table_names = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                """
            )
        }
        for table_name in REQUIRED_TABLES:
            assert table_name in table_names

        schema_versions = conn.execute(
            """
            SELECT version
            FROM rc_fundamental_schema_version
            ORDER BY version
            """
        ).fetchall()
        assert schema_versions == [(SCHEMA_VERSION,)]
        assert validate_fundamental_schema(conn) == len(REQUIRED_TABLES)
