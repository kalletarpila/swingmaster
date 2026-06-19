from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli.run_fundamental_migrations import (
    PERCENTILE_LIFECYCLE_COLUMNS,
    QUARTERLY_FIELD_PROVENANCE_REQUIRED_COLUMNS,
    REQUIRED_TABLES,
    SCHEMA_VERSION,
    TTM_COMPONENT_COLUMNS,
    QUARTERLY_ENRICHMENT_AUDIT_V2_COLUMNS,
    QUARTERLY_VINTAGE_REQUIRED_COLUMNS,
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
        quarterly_enrichment_audit_columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(rc_fundamental_quarterly_enrichment_audit)
                """
            )
        }
        for column_name, _column_type in QUARTERLY_ENRICHMENT_AUDIT_V2_COLUMNS:
            assert column_name in quarterly_enrichment_audit_columns
        quarter_state_row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='rc_fundamental_quarter_state'
            """
        ).fetchone()
        assert quarter_state_row == ("rc_fundamental_quarter_state",)


def test_run_migration_creates_reporting_frequency_and_recovery_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_schema_parity.db"

    run_migration(db_path)

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
        assert "rc_fundamental_reporting_frequency_classification" in REQUIRED_TABLES
        assert "rc_fundamental_missing_period_recovery_check" in REQUIRED_TABLES
        assert "rc_fundamental_reporting_frequency_classification" in table_names
        assert "rc_fundamental_missing_period_recovery_check" in table_names


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


def test_run_migration_creates_quarterly_vintage_and_field_provenance_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_vintage.db"

    run_migration(db_path)
    run_migration(db_path)

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
        assert "rc_fundamental_quarterly_vintage" in REQUIRED_TABLES
        assert "rc_fundamental_quarterly_field_provenance" in REQUIRED_TABLES
        assert "rc_fundamental_quarterly_vintage" in table_names
        assert "rc_fundamental_quarterly_field_provenance" in table_names
        assert "rc_fundamental_quarterly" in table_names

        vintage_columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(rc_fundamental_quarterly_vintage)
                """
            )
        }
        field_provenance_columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(rc_fundamental_quarterly_field_provenance)
                """
            )
        }
        for column_name in QUARTERLY_VINTAGE_REQUIRED_COLUMNS:
            assert column_name in vintage_columns
        for column_name in QUARTERLY_FIELD_PROVENANCE_REQUIRED_COLUMNS:
            assert column_name in field_provenance_columns


def test_quarterly_vintage_primary_key_blocks_duplicate_vintage_identity(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_vintage_pk.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_vintage(conn, statement_vintage_id="AAPL_2026Q1_V1")
        try:
            _insert_quarterly_vintage(conn, statement_vintage_id="AAPL_2026Q1_V1")
        except sqlite3.IntegrityError:
            duplicate_blocked = True
        else:
            duplicate_blocked = False

    assert duplicate_blocked is True


def test_quarterly_vintage_allows_multiple_vintages_for_same_ticker_period(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_vintage_multi.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_vintage(conn, statement_vintage_id="AAPL_2026Q1_V1", revision_number=1)
        _insert_quarterly_vintage(conn, statement_vintage_id="AAPL_2026Q1_V2", revision_number=2)
        rows = conn.execute(
            """
            SELECT statement_vintage_id, revision_number
            FROM rc_fundamental_quarterly_vintage
            WHERE ticker = 'AAPL' AND period_end_date = '2026-03-31'
            ORDER BY statement_vintage_id
            """
        ).fetchall()

    assert rows == [("AAPL_2026Q1_V1", 1), ("AAPL_2026Q1_V2", 2)]


def test_field_provenance_allows_primary_and_fallback_rows_for_same_field_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_field_provenance.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_field_provenance(
            conn,
            source_provider="sec_edgar",
            provenance_role="PRIMARY",
            merge_action="RETAINED_PRIMARY",
            field_value=100.0,
        )
        _insert_field_provenance(
            conn,
            source_provider="yahoo",
            provenance_role="FALLBACK_FILL",
            merge_action="FILLED_MISSING",
            field_value=100.0,
        )
        rows = conn.execute(
            """
            SELECT source_provider, provenance_role, merge_action
            FROM rc_fundamental_quarterly_field_provenance
            WHERE ticker = 'AAPL'
              AND period_end_date = '2026-03-31'
              AND statement_vintage_id = 'AAPL_2026Q1_V1'
              AND field_name = 'revenue'
            ORDER BY source_provider
            """
        ).fetchall()

    assert rows == [
        ("sec_edgar", "PRIMARY", "RETAINED_PRIMARY"),
        ("yahoo", "FALLBACK_FILL", "FILLED_MISSING"),
    ]


def test_quarterly_vintage_indexes_exist(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_vintage_indexes.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        vintage_indexes = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA index_list(rc_fundamental_quarterly_vintage)
                """
            )
        }
        field_provenance_indexes = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA index_list(rc_fundamental_quarterly_field_provenance)
                """
            )
        }

    assert "idx_fundamental_quarterly_vintage_ticker_period" in vintage_indexes
    assert "idx_fundamental_quarterly_vintage_ticker_available" in vintage_indexes
    assert "idx_fundamental_quarterly_vintage_ticker_period_available" in vintage_indexes
    assert "idx_fundamental_quarterly_field_prov_vintage" in field_provenance_indexes
    assert "idx_fundamental_quarterly_field_prov_run_id" in field_provenance_indexes


def _insert_quarterly_vintage(
    conn: sqlite3.Connection,
    *,
    statement_vintage_id: str,
    revision_number: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_vintage (
            ticker,
            market,
            period_end_date,
            statement_vintage_id,
            source_provider,
            source_hash,
            revision_number,
            available_at_utc,
            ingested_at_utc,
            revenue,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAPL",
            "usa",
            "2026-03-31",
            statement_vintage_id,
            "sec_edgar",
            "hash1",
            revision_number,
            "2026-04-30T00:00:00Z",
            "2026-04-30T01:00:00Z",
            100.0,
            "2026-04-30T01:00:00Z",
        ),
    )


def _insert_field_provenance(
    conn: sqlite3.Connection,
    *,
    source_provider: str,
    provenance_role: str,
    merge_action: str,
    field_value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_field_provenance (
            ticker,
            market,
            period_end_date,
            statement_vintage_id,
            field_name,
            field_value,
            source_provider,
            source_table,
            source_hash,
            provenance_role,
            merge_action,
            available_at_utc,
            created_at_utc,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAPL",
            "usa",
            "2026-03-31",
            "AAPL_2026Q1_V1",
            "revenue",
            field_value,
            source_provider,
            "fixture",
            f"{source_provider}_hash",
            provenance_role,
            merge_action,
            "2026-04-30T00:00:00Z",
            "2026-04-30T01:00:00Z",
            "RUN1",
        ),
    )
