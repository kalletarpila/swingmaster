from __future__ import annotations

import sqlite3
from pathlib import Path


V3_SCHEMA_VERSION = 1
V3_REQUIRED_TABLES = (
    "v3_schema_version",
    "v3_run",
    "v3_company",
    "v3_provider_symbol_alias",
    "v3_quarter",
    "v3_quarter_fundamentals",
    "v3_provider_q_acquisition",
    "v3_result_calendar",
    "v3_operational_action",
    "v3_event",
    "v3_ttm",
    "v3_score",
    "v3_valuation",
    "v3_migration_audit",
    "v3_resolution_issue",
)
V3_RAW_CACHE_TABLES = ("v3_raw_cache_entry",)

V3_RAW_CACHE_DDL = """
CREATE TABLE IF NOT EXISTS v3_raw_cache_entry (
    raw_cache_id INTEGER PRIMARY KEY,
    provider TEXT NOT NULL CHECK (provider IN ('YAHOO', 'LEGACY', 'V2', 'SEC', 'SIMFIN')),
    provider_symbol TEXT NOT NULL,
    fetch_run_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    UNIQUE (provider, provider_symbol, fetch_run_id, payload_hash)
);
"""


def migration_file_path() -> Path:
    return Path(__file__).resolve().parents[1] / "infra" / "sqlite" / "migrations" / "038_create_fundamentals_v3_schema.sql"


def apply_v3_schema(conn: sqlite3.Connection, *, include_raw_cache: bool = False) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    sql_text = migration_file_path().read_text()
    conn.executescript(sql_text)
    if include_raw_cache:
        conn.executescript(V3_RAW_CACHE_DDL)
    conn.commit()


def run_v3_schema_migration(db_path: Path, *, include_raw_cache: bool = False) -> tuple[Path, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        apply_v3_schema(conn, include_raw_cache=include_raw_cache)
        return migration_file_path(), validate_v3_schema(conn, include_raw_cache=include_raw_cache)


def validate_v3_schema(conn: sqlite3.Connection, *, include_raw_cache: bool = False) -> int:
    expected = set(V3_REQUIRED_TABLES)
    if include_raw_cache:
        expected.update(V3_RAW_CACHE_TABLES)
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()
    found = {str(row[0]) for row in rows}
    missing = sorted(expected - found)
    if missing:
        raise RuntimeError("FUNDAMENTALS_V3_SCHEMA_TABLES_MISSING:" + ",".join(missing))
    version_rows = conn.execute(
        """
        SELECT version
        FROM v3_schema_version
        ORDER BY version
        """
    ).fetchall()
    versions = [int(row[0]) for row in version_rows]
    if versions != [V3_SCHEMA_VERSION]:
        raise RuntimeError("FUNDAMENTALS_V3_UNSUPPORTED_SCHEMA_VERSION:" + ",".join(str(version) for version in versions))
    return len(expected)
