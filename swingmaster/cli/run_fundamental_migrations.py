from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


REQUIRED_TABLES = (
    "rc_fundamental_schema_version",
    "rc_fundamental_run",
    "rc_fundamental_statement_raw",
    "rc_earnings_calendar",
    "rc_fundamental_quarterly",
    "rc_fundamental_quarter_ingestion_status",
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
    "rc_fundamental_quarterly_vintage",
    "rc_fundamental_quarterly_field_provenance",
    "rc_earnings_event",
    "rc_fundamental_quarter_earnings_match",
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
TTM_EFFECTIVE_DATE_COLUMNS = (
    ("effective_trading_date", "TEXT"),
    ("effective_date_status", "TEXT"),
    ("effective_date_policy", "TEXT"),
    ("effective_date_source_period_end", "TEXT"),
    ("effective_date_match_confidence", "TEXT"),
    ("effective_date_component_count", "INTEGER"),
)
TTM_SCORE_EFFECTIVE_DATE_COLUMNS = (
    ("score_effective_trading_date", "TEXT"),
    ("score_effective_date_status", "TEXT"),
    ("score_effective_date_policy", "TEXT"),
    ("score_effective_date_source_ttm_as_of_date", "TEXT"),
)
TTM_NET_DEBT_TO_EBIT_COLUMNS = (
    ("net_debt_to_ebit", "REAL"),
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
QUARTERLY_VINTAGE_REQUIRED_COLUMNS = (
    "ticker",
    "market",
    "period_end_date",
    "statement_vintage_id",
    "source_provider",
    "source_document_id",
    "source_hash",
    "revision_number",
    "is_restated",
    "supersedes_vintage_id",
    "availability_quality",
    "filed_at_utc",
    "available_at_utc",
    "ingested_at_utc",
    "provider_observed_at_utc",
    "run_id",
    "provider_run_id",
    "normalization_run_id",
    "enrichment_run_id",
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
    "currency",
    "created_at_utc",
    "updated_at_utc",
)
QUARTERLY_FIELD_PROVENANCE_REQUIRED_COLUMNS = (
    "ticker",
    "market",
    "period_end_date",
    "statement_vintage_id",
    "field_name",
    "field_value",
    "source_provider",
    "source_table",
    "source_row_ref",
    "source_document_id",
    "source_hash",
    "provenance_role",
    "merge_action",
    "old_value",
    "new_value",
    "available_at_utc",
    "created_at_utc",
    "run_id",
    "enrichment_run_id",
)
EARNINGS_EVENT_REQUIRED_COLUMNS = (
    "id",
    "market",
    "ticker",
    "announcement_at",
    "announcement_date",
    "announcement_session",
    "is_reported",
    "reported_eps",
    "estimated_eps",
    "surprise_pct",
    "source",
    "source_observed_at_utc",
    "source_timezone",
    "created_at_utc",
    "updated_at_utc",
)
QUARTER_EARNINGS_MATCH_REQUIRED_COLUMNS = (
    "id",
    "market",
    "ticker",
    "period_end_date",
    "earnings_event_id",
    "announcement_at",
    "announcement_date",
    "announcement_session",
    "effective_trading_date",
    "effective_date_status",
    "reporting_delay_days",
    "matching_status",
    "matching_confidence",
    "matching_method",
    "candidate_count",
    "availability_policy",
    "matcher_version",
    "created_at_utc",
    "updated_at_utc",
)
QUARTER_INGESTION_STATUS_REQUIRED_COLUMNS = (
    "id",
    "market",
    "ticker",
    "period_end_date",
    "earnings_event_id",
    "announcement_date",
    "effective_trading_date",
    "ingestion_status",
    "basic_status",
    "quarter_basic_complete",
    "ttm_input_complete",
    "score_history_complete",
    "valuation_input_ready",
    "historical_research_ready",
    "available_basic_field_count",
    "missing_basic_fields",
    "missing_core_fields_json",
    "missing_ttm_fields_json",
    "missing_score_fields_json",
    "data_quality_warnings_json",
    "supported_source_field_count",
    "source_non_null_field_count",
    "persisted_matching_field_count",
    "retry_recommendation",
    "last_fetch_status",
    "last_fetch_source",
    "last_source_observed_at_utc",
    "source_confirmation_status",
    "source_confirmation_source",
    "last_sec_checked_at_utc",
    "sec_confirmation_run_id",
    "last_checked_at_utc",
    "assessment_policy_version",
    "ingestion_evidence_type",
    "run_id",
    "assessed_at_utc",
    "created_at_utc",
    "updated_at_utc",
)
EARNINGS_CALENDAR_REQUIRED_COLUMNS = (
    "id",
    "market",
    "ticker",
    "estimated_announcement_at",
    "estimated_announcement_date",
    "estimated_session",
    "calendar_status",
    "source",
    "source_observed_at_utc",
    "first_observed_at_utc",
    "last_observed_at_utc",
    "previous_estimated_announcement_at",
    "date_change_count",
    "completed_earnings_event_id",
    "calendar_last_checked_at_utc",
    "calendar_check_status",
    "calendar_last_failed_at_utc",
    "calendar_failure_count",
    "created_at_utc",
    "updated_at_utc",
)
EARNINGS_CALENDAR_CHECK_STATE_COLUMNS = (
    ("calendar_last_checked_at_utc", "TEXT"),
    ("calendar_check_status", "TEXT"),
    ("calendar_last_failed_at_utc", "TEXT"),
    ("calendar_failure_count", "INTEGER NOT NULL DEFAULT 0"),
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


def get_quarterly_vintage_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "028_rc_fundamental_quarterly_vintage.sql"
    )


def get_earnings_event_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "029_rc_earnings_event.sql"
    )


def get_quarter_earnings_match_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "030_rc_fundamental_quarter_earnings_match.sql"
    )


def get_ttm_effective_date_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "031_rc_fundamental_ttm_effective_date.sql"
    )


def get_score_effective_date_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "032_rc_fundamental_score_effective_date.sql"
    )


def get_ttm_net_debt_to_ebit_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "033_rc_fundamental_ttm_net_debt_to_ebit.sql"
    )


def get_quarter_ingestion_status_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "034_rc_fundamental_quarter_ingestion_status.sql"
    )


def get_earnings_calendar_migration_file_path() -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "infra"
        / "sqlite"
        / "migrations"
        / "035_rc_earnings_calendar.sql"
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
        get_quarterly_vintage_migration_file_path(),
        get_earnings_event_migration_file_path(),
        get_quarter_earnings_match_migration_file_path(),
        get_ttm_effective_date_migration_file_path(),
        get_score_effective_date_migration_file_path(),
        get_ttm_net_debt_to_ebit_migration_file_path(),
        get_quarter_ingestion_status_migration_file_path(),
        get_earnings_calendar_migration_file_path(),
    )
    for current_migration_file in migration_files:
        sql_text = current_migration_file.read_text(encoding="utf-8")
        conn.executescript(sql_text)
    ensure_ttm_component_columns(conn)
    ensure_ttm_effective_date_columns(conn)
    ensure_score_effective_date_columns(conn)
    ensure_ttm_net_debt_to_ebit_columns(conn)
    ensure_percentile_lifecycle_columns(conn)
    ensure_valuation_v2_columns(conn)
    ensure_valuation_v21_columns(conn)
    ensure_valuation_v22_columns(conn)
    ensure_quarterly_enrichment_audit_v2_columns(conn)
    ensure_quarter_ingestion_status_schema(conn)
    ensure_earnings_calendar_check_state_columns(conn)
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


def ensure_ttm_effective_date_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_ttm)
            """
        )
    }
    for column_name, column_type in TTM_EFFECTIVE_DATE_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE rc_fundamental_ttm ADD COLUMN {column_name} {column_type}")
        existing_columns.add(column_name)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_ticker_effective_date
        ON rc_fundamental_ttm(ticker, effective_trading_date)
        """
    )


def ensure_score_effective_date_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_ttm)
            """
        )
    }
    for column_name, column_type in TTM_SCORE_EFFECTIVE_DATE_COLUMNS:
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE rc_fundamental_ttm ADD COLUMN {column_name} {column_type}")
        existing_columns.add(column_name)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_score_effective_date
        ON rc_fundamental_ttm(ticker, score_effective_trading_date)
        """
    )


def ensure_ttm_net_debt_to_ebit_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_ttm)
            """
        )
    }
    for column_name, column_type in TTM_NET_DEBT_TO_EBIT_COLUMNS:
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


def ensure_quarter_ingestion_status_schema(conn: sqlite3.Connection) -> None:
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_quarter_ingestion_status)
            """
        )
    }
    if "id" in existing_columns and set(QUARTER_INGESTION_STATUS_REQUIRED_COLUMNS).issubset(existing_columns):
        _create_quarter_ingestion_status_indexes(conn)
        return

    conn.execute("ALTER TABLE rc_fundamental_quarter_ingestion_status RENAME TO rc_fundamental_quarter_ingestion_status_legacy")
    conn.executescript(get_quarter_ingestion_status_migration_file_path().read_text(encoding="utf-8"))
    legacy_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_quarter_ingestion_status_legacy)
            """
        )
    }
    select_exprs = []
    for column in QUARTER_INGESTION_STATUS_REQUIRED_COLUMNS:
        if column == "id":
            select_exprs.append("NULL AS id")
        elif column in legacy_columns:
            select_exprs.append(column)
        elif column == "basic_status":
            select_exprs.append(
                "CASE WHEN quarter_basic_complete = 1 THEN 'BASIC_COMPLETE' ELSE 'FUNDAMENTALS_PARTIAL' END AS basic_status"
            )
        elif column == "ingestion_status":
            select_exprs.append("'UNKNOWN_HISTORICAL_INGEST_COMPLETENESS' AS ingestion_status")
        elif column == "valuation_input_ready":
            select_exprs.append("0 AS valuation_input_ready")
        elif column == "historical_research_ready":
            select_exprs.append("0 AS historical_research_ready")
        elif column == "available_basic_field_count":
            select_exprs.append("0 AS available_basic_field_count")
        elif column == "missing_basic_fields":
            select_exprs.append("'[]' AS missing_basic_fields")
        elif column in {"supported_source_field_count", "source_non_null_field_count", "persisted_matching_field_count"}:
            select_exprs.append("NULL AS " + column)
        elif column in {
            "last_fetch_status",
            "last_fetch_source",
            "last_source_observed_at_utc",
            "source_confirmation_source",
            "last_sec_checked_at_utc",
            "sec_confirmation_run_id",
        }:
            select_exprs.append("NULL AS " + column)
        elif column == "source_confirmation_status":
            select_exprs.append("'SOURCE_CONFIRMATION_UNKNOWN' AS source_confirmation_status")
        elif column == "last_checked_at_utc":
            select_exprs.append("assessed_at_utc AS last_checked_at_utc")
        elif column == "ingestion_evidence_type":
            select_exprs.append("'CURRENT_DB_STATE_ONLY' AS ingestion_evidence_type")
        elif column == "created_at_utc":
            select_exprs.append("assessed_at_utc AS created_at_utc")
        else:
            select_exprs.append("NULL AS " + column)
    insert_columns = ", ".join(QUARTER_INGESTION_STATUS_REQUIRED_COLUMNS)
    conn.execute(
        f"""
        INSERT OR REPLACE INTO rc_fundamental_quarter_ingestion_status ({insert_columns})
        SELECT {', '.join(select_exprs)}
        FROM rc_fundamental_quarter_ingestion_status_legacy
        """
    )
    conn.execute("DROP TABLE rc_fundamental_quarter_ingestion_status_legacy")
    _create_quarter_ingestion_status_indexes(conn)


def ensure_earnings_calendar_check_state_columns(conn: sqlite3.Connection) -> None:
    columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_earnings_calendar)
            """
        )
    }
    for column_name, column_type in EARNINGS_CALENDAR_CHECK_STATE_COLUMNS:
        if column_name not in columns:
            conn.execute(f"ALTER TABLE rc_earnings_calendar ADD COLUMN {column_name} {column_type}")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_earnings_calendar_check_status
        ON rc_earnings_calendar(market, calendar_check_status, calendar_last_checked_at_utc)
        """
    )


def _create_quarter_ingestion_status_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_market_period
        ON rc_fundamental_quarter_ingestion_status(market, period_end_date);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_ticker_period
        ON rc_fundamental_quarter_ingestion_status(ticker, period_end_date);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_ingestion_status
        ON rc_fundamental_quarter_ingestion_status(ingestion_status);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_basic_status
        ON rc_fundamental_quarter_ingestion_status(basic_status);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_quarter_basic
        ON rc_fundamental_quarter_ingestion_status(quarter_basic_complete);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_ttm_input
        ON rc_fundamental_quarter_ingestion_status(ttm_input_complete);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_score_history
        ON rc_fundamental_quarter_ingestion_status(score_history_complete);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_last_checked
        ON rc_fundamental_quarter_ingestion_status(last_checked_at_utc);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_earnings_event
        ON rc_fundamental_quarter_ingestion_status(earnings_event_id);
        CREATE INDEX IF NOT EXISTS idx_fundamental_qis_source_confirmation
        ON rc_fundamental_quarter_ingestion_status(source_confirmation_status, last_sec_checked_at_utc);
        """
    )


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

    missing_ttm_effective_date_columns = [
        column_name for column_name, _column_type in TTM_EFFECTIVE_DATE_COLUMNS if column_name not in ttm_columns
    ]
    if missing_ttm_effective_date_columns:
        raise RuntimeError(
            "FUNDAMENTAL_TTM_EFFECTIVE_DATE_COLUMNS_MISSING:"
            + ",".join(missing_ttm_effective_date_columns)
        )

    missing_score_effective_date_columns = [
        column_name for column_name, _column_type in TTM_SCORE_EFFECTIVE_DATE_COLUMNS if column_name not in ttm_columns
    ]
    if missing_score_effective_date_columns:
        raise RuntimeError(
            "FUNDAMENTAL_SCORE_EFFECTIVE_DATE_COLUMNS_MISSING:"
            + ",".join(missing_score_effective_date_columns)
        )

    missing_ttm_net_debt_to_ebit_columns = [
        column_name for column_name, _column_type in TTM_NET_DEBT_TO_EBIT_COLUMNS if column_name not in ttm_columns
    ]
    if missing_ttm_net_debt_to_ebit_columns:
        raise RuntimeError(
            "FUNDAMENTAL_TTM_NET_DEBT_TO_EBIT_COLUMNS_MISSING:"
            + ",".join(missing_ttm_net_debt_to_ebit_columns)
        )

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

    quarterly_vintage_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_quarterly_vintage)
            """
        )
    }
    missing_quarterly_vintage_columns = [
        column_name for column_name in QUARTERLY_VINTAGE_REQUIRED_COLUMNS if column_name not in quarterly_vintage_columns
    ]
    if missing_quarterly_vintage_columns:
        raise RuntimeError(
            "FUNDAMENTAL_QUARTERLY_VINTAGE_COLUMNS_MISSING:"
            + ",".join(missing_quarterly_vintage_columns)
        )

    field_provenance_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_quarterly_field_provenance)
            """
        )
    }
    missing_field_provenance_columns = [
        column_name
        for column_name in QUARTERLY_FIELD_PROVENANCE_REQUIRED_COLUMNS
        if column_name not in field_provenance_columns
    ]
    if missing_field_provenance_columns:
        raise RuntimeError(
            "FUNDAMENTAL_QUARTERLY_FIELD_PROVENANCE_COLUMNS_MISSING:"
            + ",".join(missing_field_provenance_columns)
        )

    earnings_event_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_earnings_event)
            """
        )
    }
    missing_earnings_event_columns = [
        column_name for column_name in EARNINGS_EVENT_REQUIRED_COLUMNS if column_name not in earnings_event_columns
    ]
    if missing_earnings_event_columns:
        raise RuntimeError(
            "EARNINGS_EVENT_COLUMNS_MISSING:" + ",".join(missing_earnings_event_columns)
        )

    quarter_earnings_match_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_quarter_earnings_match)
            """
        )
    }
    missing_quarter_earnings_match_columns = [
        column_name for column_name in QUARTER_EARNINGS_MATCH_REQUIRED_COLUMNS if column_name not in quarter_earnings_match_columns
    ]
    if missing_quarter_earnings_match_columns:
        raise RuntimeError(
            "QUARTER_EARNINGS_MATCH_COLUMNS_MISSING:" + ",".join(missing_quarter_earnings_match_columns)
        )

    quarter_ingestion_status_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_quarter_ingestion_status)
            """
        )
    }
    missing_quarter_ingestion_status_columns = [
        column_name
        for column_name in QUARTER_INGESTION_STATUS_REQUIRED_COLUMNS
        if column_name not in quarter_ingestion_status_columns
    ]
    if missing_quarter_ingestion_status_columns:
        raise RuntimeError(
            "QUARTER_INGESTION_STATUS_COLUMNS_MISSING:"
            + ",".join(missing_quarter_ingestion_status_columns)
        )

    earnings_calendar_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_earnings_calendar)
            """
        )
    }
    missing_earnings_calendar_columns = [
        column_name for column_name in EARNINGS_CALENDAR_REQUIRED_COLUMNS if column_name not in earnings_calendar_columns
    ]
    if missing_earnings_calendar_columns:
        raise RuntimeError(
            "EARNINGS_CALENDAR_COLUMNS_MISSING:" + ",".join(missing_earnings_calendar_columns)
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
