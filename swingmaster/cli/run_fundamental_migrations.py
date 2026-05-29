from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


REQUIRED_TABLES = (
    "rc_fundamental_schema_version",
    "rc_fundamental_run",
    "rc_fundamental_statement_raw",
    "rc_fundamental_quarterly",
    "rc_fundamental_quarter_state",
    "rc_fundamental_quarterly_enrichment_audit",
    "rc_fundamental_ttm",
    "rc_fundamental_score_percentile",
    "rc_fundamental_finnhub_raw",
    "rc_fundamental_yahoo_raw",
    "rc_fundamental_yahoo_quarterly",
    "rc_fundamental_valuation",
    "rc_fundamental_reporting_frequency_classification",
    "rc_fundamental_missing_period_recovery_check",
)
SCHEMA_VERSION = 1
TTM_COMPONENT_COLUMNS = (
    ("growth_component", "REAL"),
    ("margin_component", "REAL"),
    ("margin_trend_component", "REAL"),
    ("fcf_component", "REAL"),
    ("leverage_component", "REAL"),
    ("dilution_component", "REAL"),
    ("lifecycle_component", "REAL"),
    ("consistency_component", "REAL"),
    ("score_rule", "TEXT"),
    ("fundamental_score_lifecycle", "REAL"),
    ("score_rule_lifecycle", "TEXT"),
    ("growth_component_lifecycle", "REAL"),
    ("margin_component_lifecycle", "REAL"),
    ("margin_trend_component_lifecycle", "REAL"),
    ("fcf_component_lifecycle", "REAL"),
    ("leverage_component_lifecycle", "REAL"),
    ("dilution_component_lifecycle", "REAL"),
    ("lifecycle_component_lifecycle", "REAL"),
    ("consistency_component_lifecycle", "REAL"),
)
PERCENTILE_LIFECYCLE_COLUMNS = (
    ("sector_rank_blended", "INTEGER"),
    ("industry_rank_blended", "INTEGER"),
    ("fundamental_score_percentile_global_lifecycle_weighted", "REAL"),
    ("fundamental_score_percentile_sector_lifecycle_weighted", "REAL"),
    ("fundamental_score_percentile_industry_lifecycle_weighted", "REAL"),
    ("fundamental_score_percentile_blended_lifecycle_weighted", "REAL"),
    ("sector_rank_blended_lifecycle_weighted", "INTEGER"),
    ("industry_rank_blended_lifecycle_weighted", "INTEGER"),
    ("percentile_lifecycle_weight_rule", "TEXT"),
)
VALUATION_V2_COLUMNS = (
    ("valuation_fcf_yield", "REAL"),
    ("valuation_ebit_margin", "REAL"),
    ("adjusted_expensive_threshold", "REAL"),
    ("valuation_model_version", "TEXT"),
)
VALUATION_V21_COLUMNS = (
    ("valuation_fundamental_as_of_date", "TEXT"),
    ("valuation_fundamental_staleness_days", "INTEGER"),
)
VALUATION_V22_COLUMNS = (
    ("debt_assumed_zero", "INTEGER"),
    ("cash_assumed_zero", "INTEGER"),
)
QUARTERLY_ENRICHMENT_AUDIT_V2_COLUMNS = (
    ("matched_yahoo_period_end_date", "TEXT"),
    ("match_method", "TEXT"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply SwingMaster fundamentals SQLite schema migrations")
    parser.add_argument("--db", required=True, help="SQLite database path")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def get_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "001_create_fundamentals_schema.sql"


def get_finnhub_audit_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "016_rc_fundamental_finnhub_raw.sql"


def get_yahoo_audit_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "017_rc_fundamental_yahoo_raw.sql"


def get_yahoo_quarterly_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "018_rc_fundamental_yahoo_quarterly.sql"


def get_valuation_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "019_rc_fundamental_valuation.sql"


def get_valuation_v2_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "020_rc_fundamental_valuation_v2.sql"


def get_valuation_v21_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "021_rc_fundamental_valuation_v21.sql"


def get_valuation_v22_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "022_rc_fundamental_valuation_v22.sql"


def get_quarterly_enrichment_audit_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "023_rc_fundamental_quarterly_enrichment_audit.sql"


def get_quarterly_enrichment_audit_v2_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "024_rc_fundamental_quarterly_enrichment_audit_v2.sql"


def get_quarter_state_migration_file_path() -> Path:
    return Path(__file__).resolve().parent.parent / "infra" / "sqlite" / "migrations" / "025_rc_fundamental_quarter_state.sql"


def get_reporting_frequency_classification_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "026_rc_fundamental_reporting_frequency_classification.sql"
    )


def get_missing_period_recovery_check_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "027_rc_fundamental_missing_period_recovery_check.sql"
    )


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def apply_fundamental_migration(conn: sqlite3.Connection, migration_file: Path) -> None:
    migration_files = (
        migration_file,
        get_finnhub_audit_migration_file_path(),
        get_yahoo_audit_migration_file_path(),
        get_yahoo_quarterly_migration_file_path(),
        get_valuation_migration_file_path(),
        get_valuation_v2_migration_file_path(),
        get_valuation_v21_migration_file_path(),
        get_valuation_v22_migration_file_path(),
        get_quarterly_enrichment_audit_migration_file_path(),
        get_quarterly_enrichment_audit_v2_migration_file_path(),
        get_quarter_state_migration_file_path(),
        get_reporting_frequency_classification_migration_file_path(),
        get_missing_period_recovery_check_migration_file_path(),
    )
    for current_migration_file in migration_files:
        sql_text = current_migration_file.read_text(encoding="utf-8")
        conn.executescript(sql_text)
    ensure_ttm_component_columns(conn)
    ensure_percentile_lifecycle_columns(conn)
    ensure_valuation_v2_columns(conn)
    ensure_valuation_v21_columns(conn)
    ensure_valuation_v22_columns(conn)
    ensure_quarterly_enrichment_audit_v2_columns(conn)
    conn.commit()


def ensure_ttm_component_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_ttm)
            """
        )
    }
    for column_name, column_type in TTM_COMPONENT_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE rc_fundamental_ttm ADD COLUMN {column_name} {column_type}")
        existing_columns.add(column_name)


def ensure_percentile_lifecycle_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_score_percentile)
            """
        )
    }
    for column_name, column_type in PERCENTILE_LIFECYCLE_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE rc_fundamental_score_percentile ADD COLUMN {column_name} {column_type}")
        existing_columns.add(column_name)


def ensure_valuation_v2_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_valuation)
            """
        )
    }
    for column_name, column_type in VALUATION_V2_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE rc_fundamental_valuation ADD COLUMN {column_name} {column_type}")
        existing_columns.add(column_name)


def ensure_valuation_v21_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_valuation)
            """
        )
    }
    for column_name, column_type in VALUATION_V21_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE rc_fundamental_valuation ADD COLUMN {column_name} {column_type}")
        existing_columns.add(column_name)


def ensure_valuation_v22_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_valuation)
            """
        )
    }
    for column_name, column_type in VALUATION_V22_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE rc_fundamental_valuation ADD COLUMN {column_name} {column_type}")
        existing_columns.add(column_name)


def ensure_quarterly_enrichment_audit_v2_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_quarterly_enrichment_audit)
            """
        )
    }
    for column_name, column_type in QUARTERLY_ENRICHMENT_AUDIT_V2_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE rc_fundamental_quarterly_enrichment_audit ADD COLUMN {column_name} {column_type}")
        existing_columns.add(column_name)


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

    ttm_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_ttm)
            """
        )
    }
    missing_ttm_columns = [
        column_name for column_name, _column_type in TTM_COMPONENT_COLUMNS if column_name not in ttm_columns
    ]
    if missing_ttm_columns:
        raise RuntimeError(f"FUNDAMENTAL_TTM_COLUMNS_MISSING:{','.join(missing_ttm_columns)}")

    percentile_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_score_percentile)
            """
        )
    }
    missing_percentile_columns = [
        column_name for column_name, _column_type in PERCENTILE_LIFECYCLE_COLUMNS if column_name not in percentile_columns
    ]
    if missing_percentile_columns:
        raise RuntimeError(f"FUNDAMENTAL_PERCENTILE_COLUMNS_MISSING:{','.join(missing_percentile_columns)}")

    valuation_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_valuation)
            """
        )
    }
    missing_valuation_columns = [
        column_name for column_name, _column_type in VALUATION_V2_COLUMNS if column_name not in valuation_columns
    ]
    if missing_valuation_columns:
        raise RuntimeError(f"FUNDAMENTAL_VALUATION_COLUMNS_MISSING:{','.join(missing_valuation_columns)}")

    missing_valuation_v21_columns = [
        column_name for column_name, _column_type in VALUATION_V21_COLUMNS if column_name not in valuation_columns
    ]
    if missing_valuation_v21_columns:
        raise RuntimeError(f"FUNDAMENTAL_VALUATION_V21_COLUMNS_MISSING:{','.join(missing_valuation_v21_columns)}")

    missing_valuation_v22_columns = [
        column_name for column_name, _column_type in VALUATION_V22_COLUMNS if column_name not in valuation_columns
    ]
    if missing_valuation_v22_columns:
        raise RuntimeError(f"FUNDAMENTAL_VALUATION_V22_COLUMNS_MISSING:{','.join(missing_valuation_v22_columns)}")

    quarterly_enrichment_audit_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_quarterly_enrichment_audit)
            """
        )
    }
    missing_quarterly_enrichment_audit_v2_columns = [
        column_name
        for column_name, _column_type in QUARTERLY_ENRICHMENT_AUDIT_V2_COLUMNS
        if column_name not in quarterly_enrichment_audit_columns
    ]
    if missing_quarterly_enrichment_audit_v2_columns:
        raise RuntimeError(
            "FUNDAMENTAL_QUARTERLY_ENRICHMENT_AUDIT_V2_COLUMNS_MISSING:"
            + ",".join(missing_quarterly_enrichment_audit_v2_columns)
        )

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
