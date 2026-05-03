from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli.run_fundamental_migrations import (
    PERCENTILE_LIFECYCLE_COLUMNS,
    REQUIRED_TABLES,
    SCHEMA_VERSION,
    TTM_COMPONENT_COLUMNS,
    VALUATION_V2_COLUMNS,
    VALUATION_V21_COLUMNS,
    VALUATION_V22_COLUMNS,
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
        ttm_columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(rc_fundamental_ttm)
                """
            )
        }
        for column_name, _column_type in TTM_COMPONENT_COLUMNS:
            assert column_name in ttm_columns
        table_row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='rc_fundamental_valuation'
            """
        ).fetchone()
        assert table_row == ("rc_fundamental_valuation",)
        valuation_columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(rc_fundamental_valuation)
                """
            )
        }
        for column_name, _column_type in VALUATION_V2_COLUMNS:
            assert column_name in valuation_columns
        for column_name, _column_type in VALUATION_V21_COLUMNS:
            assert column_name in valuation_columns
        for column_name, _column_type in VALUATION_V22_COLUMNS:
            assert column_name in valuation_columns


def test_run_migration_adds_missing_ttm_component_columns_to_existing_db(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_existing.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_schema_version (
                version INTEGER PRIMARY KEY,
                applied_at_utc TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_run (
                run_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                mode TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT,
                tickers_total INTEGER,
                tickers_processed INTEGER,
                notes TEXT
            );
            CREATE TABLE rc_fundamental_statement_raw (
                ticker TEXT NOT NULL,
                statement_type TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                period_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_value REAL,
                currency TEXT,
                source TEXT NOT NULL,
                retrieved_at_utc TEXT NOT NULL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, statement_type, period_end_date, field_name)
            );
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                revenue REAL,
                gross_profit REAL,
                operating_income REAL,
                ebit REAL,
                ebitda REAL,
                net_income REAL,
                operating_cashflow REAL,
                capex REAL,
                free_cashflow REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL,
                currency TEXT,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, period_end_date)
            );
            CREATE TABLE rc_fundamental_ttm (
                ticker TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                latest_period_end_date TEXT NOT NULL,
                revenue_ttm REAL,
                revenue_growth_ttm_yoy REAL,
                ebit_ttm REAL,
                ebit_growth_ttm_yoy REAL,
                ebit_margin_ttm REAL,
                ebit_margin_trend_4q REAL,
                gross_margin_trend_4q REAL,
                fcf_ttm REAL,
                fcf_margin_ttm REAL,
                fcf_margin_trend_4q REAL,
                net_debt REAL,
                net_debt_to_ebitda REAL,
                share_dilution_yoy REAL,
                lifecycle_class TEXT,
                fundamental_score REAL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, as_of_date)
            );
            INSERT INTO rc_fundamental_schema_version (version, applied_at_utc)
            VALUES (1, '2026-04-25T00:00:00Z');
            """
        )
        conn.commit()

    run_migration(db_path)
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        ttm_columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(rc_fundamental_ttm)
                """
            )
        }
        for column_name, _column_type in TTM_COMPONENT_COLUMNS:
            assert column_name in ttm_columns


def test_run_migration_creates_latest_ttm_view_with_component_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_latest_view.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        view_row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='view' AND name='rc_fundamental_latest'
            """
        ).fetchone()
        assert view_row == ("rc_fundamental_latest",)

        conn.executemany(
            """
            INSERT INTO rc_fundamental_ttm (
                ticker,
                as_of_date,
                latest_period_end_date,
                fundamental_score,
                growth_component,
                consistency_component,
                run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("AAPL", "2025-03-29", "2025-03-29", 70.0, 5.0, 2.0, "RUN1"),
                ("AAPL", "2025-06-28", "2025-06-28", 72.0, 5.0, 4.0, "RUN1"),
                ("MSFT", "2025-03-31", "2025-03-31", 79.0, 15.0, 6.0, "RUN1"),
                ("MSFT", "2025-06-30", "2025-06-30", 75.0, 15.0, 4.0, "RUN1"),
            ],
        )
        conn.commit()

        latest_rows = conn.execute(
            """
            SELECT ticker, as_of_date, fundamental_score, growth_component, consistency_component
            FROM rc_fundamental_latest
            ORDER BY ticker ASC
            """
        ).fetchall()
        assert latest_rows == [
            ("AAPL", "2025-06-28", 72.0, 5.0, 4.0),
            ("MSFT", "2025-06-30", 75.0, 15.0, 4.0),
        ]


def test_run_migration_creates_percentile_score_table(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_percentile_table.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        table_row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='rc_fundamental_score_percentile'
            """
        ).fetchone()
        assert table_row == ("rc_fundamental_score_percentile",)
        percentile_columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(rc_fundamental_score_percentile)
                """
            )
        }
        for column_name, _column_type in PERCENTILE_LIFECYCLE_COLUMNS:
            assert column_name in percentile_columns

        for column_name in (
            "sector_rank_blended",
            "industry_rank_blended",
            "sector_rank_blended_lifecycle_weighted",
            "industry_rank_blended_lifecycle_weighted",
        ):
            assert column_name in percentile_columns


def test_run_migration_adds_missing_percentile_lifecycle_columns_to_existing_db(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_existing_percentile.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_schema_version (
                version INTEGER PRIMARY KEY,
                applied_at_utc TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_run (
                run_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                mode TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT,
                tickers_total INTEGER,
                tickers_processed INTEGER,
                notes TEXT
            );
            CREATE TABLE rc_fundamental_statement_raw (
                ticker TEXT NOT NULL,
                statement_type TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                period_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_value REAL,
                currency TEXT,
                source TEXT NOT NULL,
                retrieved_at_utc TEXT NOT NULL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, statement_type, period_end_date, field_name)
            );
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                revenue REAL,
                gross_profit REAL,
                operating_income REAL,
                ebit REAL,
                ebitda REAL,
                net_income REAL,
                operating_cashflow REAL,
                capex REAL,
                free_cashflow REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL,
                currency TEXT,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, period_end_date)
            );
            CREATE TABLE rc_fundamental_ttm (
                ticker TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                latest_period_end_date TEXT NOT NULL,
                revenue_ttm REAL,
                revenue_growth_ttm_yoy REAL,
                ebit_ttm REAL,
                ebit_growth_ttm_yoy REAL,
                ebit_margin_ttm REAL,
                ebit_margin_trend_4q REAL,
                gross_margin_trend_4q REAL,
                fcf_ttm REAL,
                fcf_margin_ttm REAL,
                fcf_margin_trend_4q REAL,
                net_debt REAL,
                net_debt_to_ebitda REAL,
                share_dilution_yoy REAL,
                lifecycle_class TEXT,
                fundamental_score REAL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, as_of_date)
            );
            CREATE TABLE rc_fundamental_score_percentile (
                ticker TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                target_date TEXT NOT NULL,
                sector TEXT,
                industry TEXT,
                rule_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                universe_size INTEGER NOT NULL,
                sector_size INTEGER,
                industry_size INTEGER,
                growth_pct_global REAL,
                growth_pct_sector REAL,
                growth_pct_industry REAL,
                margin_pct_global REAL,
                margin_pct_sector REAL,
                margin_pct_industry REAL,
                margin_trend_pct_global REAL,
                margin_trend_pct_sector REAL,
                margin_trend_pct_industry REAL,
                fcf_pct_global REAL,
                fcf_pct_sector REAL,
                fcf_pct_industry REAL,
                leverage_pct_global REAL,
                leverage_pct_sector REAL,
                leverage_pct_industry REAL,
                dilution_pct_global REAL,
                dilution_pct_sector REAL,
                dilution_pct_industry REAL,
                consistency_pct_global REAL,
                consistency_pct_sector REAL,
                consistency_pct_industry REAL,
                fundamental_score_percentile_global REAL,
                fundamental_score_percentile_sector REAL,
                fundamental_score_percentile_industry REAL,
                fundamental_score_percentile_blended REAL,
                created_at_utc TEXT NOT NULL,
                PRIMARY KEY (ticker, target_date, rule_id)
            );
            INSERT INTO rc_fundamental_schema_version (version, applied_at_utc)
            VALUES (1, '2026-04-25T00:00:00Z');
            """
        )
        conn.commit()

    run_migration(db_path)
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        percentile_columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(rc_fundamental_score_percentile)
                """
            )
        }
        for column_name, _column_type in PERCENTILE_LIFECYCLE_COLUMNS:
            assert column_name in percentile_columns
