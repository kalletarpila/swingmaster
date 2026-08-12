from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.historical_backfill_planner import (
    ACTION_NEEDS_SEC_AND_YAHOO,
    ACTION_NEEDS_SEC_HISTORY_REFRESH,
    ACTION_NEEDS_YAHOO_RECENT_ENRICHMENT,
    ACTION_NO_ACTION_COMPLETE,
    ACTION_OFFLINE_MERGE_AVAILABLE,
    ACTION_PARTIAL_BEST_AVAILABLE,
    ACTION_RETRYABLE_FAILURE,
    ACTION_TARGET_IDENTITY_REVIEW,
    TARGET_DETERMINISTIC,
    TARGET_IDENTITY_REVIEW,
    build_historical_backfill_plan,
    build_target_inventory,
    plan_content_hash,
)
from swingmaster.fundamentals.historical_backfill_source_policy import (
    SEC_SUPPORTED_FIELDS,
    YAHOO_SUPPORTED_FIELDS,
    merge_sec_yahoo_fields,
)


def test_historical_backfill_result_ledger_schema_created(tmp_path: Path) -> None:
    db_path = tmp_path / "ledger.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(rc_fundamental_historical_backfill_result)")}
    assert {
        "market",
        "ticker",
        "target_period_end_date",
        "result_status",
        "result_reason",
        "actionable",
        "exhausted",
        "sec_evidence_state",
        "yahoo_evidence_state",
        "run_id",
    }.issubset(columns)


def test_source_policy_keeps_sec_operating_income_out_of_ebit_and_yahoo_fills_nulls() -> None:
    assert "operating_income" in SEC_SUPPORTED_FIELDS
    assert "ebit" not in SEC_SUPPORTED_FIELDS
    assert "ebit" in YAHOO_SUPPORTED_FIELDS
    assert "ebitda" in YAHOO_SUPPORTED_FIELDS

    merged = merge_sec_yahoo_fields(
        existing_row={"revenue": None, "operating_income": None, "ebit": None, "ebitda": None},
        sec_row={"revenue": 100.0, "operating_income": 20.0, "ebit": 999.0, "ebitda": 888.0},
        yahoo_row={"revenue": 101.0, "operating_income": 21.0, "ebit": 18.0, "ebitda": 30.0},
    )
    assert merged["revenue"] == 100.0
    assert merged["operating_income"] == 20.0
    assert merged["ebit"] == 18.0
    assert merged["ebitda"] == 30.0


def test_target_inventory_uses_fiscal_sec_metadata_and_flags_ambiguous_match(tmp_path: Path) -> None:
    db_path = tmp_path / "inventory.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        _insert_sec_fact(conn, "AAPL", "2026-03-28", "Revenues|form=10-Q|unit=USD|fy=2026|fp=Q2|frame=CY2026Q1|start=2025-12-28|filed=2026-05-01", 100.0)
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_earnings_match (
                market, ticker, period_end_date, earnings_event_id, announcement_at,
                announcement_date, announcement_session, effective_trading_date,
                effective_date_status, reporting_delay_days, matching_status,
                matching_confidence, matching_method, candidate_count,
                availability_policy, matcher_version, created_at_utc, updated_at_utc
            ) VALUES ('usa', 'ODD', '2026-02-15', 1, '2026-04-01T12:00:00Z',
                '2026-04-01', 'BEFORE_MARKET', '2026-04-01', 'KNOWN', 45,
                'MATCHED', 'LOW', 'fixture', 2, 'fixture', 'fixture',
                '2026-04-01T12:00:00Z', '2026-04-01T12:00:00Z')
            """
        )
        _insert_quarter(conn, "KNOWN", "2026-03-31", complete=False, run_id="BASE")
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_earnings_match (
                market, ticker, period_end_date, earnings_event_id, announcement_at,
                announcement_date, announcement_session, effective_trading_date,
                effective_date_status, reporting_delay_days, matching_status,
                matching_confidence, matching_method, candidate_count,
                availability_policy, matcher_version, created_at_utc, updated_at_utc
            ) VALUES ('usa', 'KNOWN', '2026-03-31', 2, '2026-04-20T12:00:00Z',
                '2026-04-20', 'BEFORE_MARKET', '2026-04-20', 'KNOWN', 20,
                'MATCHED', 'LOW', 'fixture', 2, 'fixture', 'fixture',
                '2026-04-20T12:00:00Z', '2026-04-20T12:00:00Z')
            """
        )
        conn.commit()
        inventory = build_target_inventory(conn, tickers=["AAPL", "ODD", "KNOWN"])

    by_key = {(row.ticker, row.target_period_end_date): row for row in inventory}
    assert by_key[("AAPL", "2026-03-28")].fiscal_year == "2026"
    assert by_key[("AAPL", "2026-03-28")].fiscal_quarter == "Q2"
    assert by_key[("AAPL", "2026-03-28")].target_identity_status == TARGET_DETERMINISTIC
    assert by_key[("ODD", "2026-02-15")].target_identity_status == TARGET_IDENTITY_REVIEW
    assert by_key[("KNOWN", "2026-03-31")].target_identity_status == TARGET_DETERMINISTIC


def test_planner_classifies_actions_and_aggregates_provider_calls_once_per_ticker(tmp_path: Path) -> None:
    db_path = tmp_path / "planner.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarter(conn, "COMP", "2026-03-31", complete=True, run_id="BASE")
        _insert_status(conn, "COMP", "2026-03-31", "QUARTER_BASIC_COMPLETE", quarter_basic=1)

        _insert_quarter(conn, "CACHE", "2026-03-31", complete=False, run_id="BASE")
        _insert_yahoo_quarter(conn, "CACHE", "2026-03-31", revenue=10.0, ebit=2.0, free_cashflow=1.0, cash=4.0, total_debt=3.0, shares_outstanding=100.0)

        _insert_quarter(conn, "YHOO", "2025-06-30", complete=False, run_id="BASE")
        _insert_quarter(conn, "YHOO", "2025-09-30", complete=False, run_id="BASE")
        _insert_quarter(conn, "YHOO", "2025-12-31", complete=False, run_id="BASE")
        _insert_quarter(conn, "YHOO", "2026-03-31", complete=False, run_id="BASE")
        for period in ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"):
            _insert_sec_fact(conn, "YHOO", period, f"Revenues|form=10-Q|unit=USD|fy=2026|fp=Q1|frame=CY2026Q1|start=2026-01-01|filed=2026-05-01", 100.0)

        _insert_quarter(conn, "SECN", "2024-03-31", complete=False, run_id="BASE")
        _insert_quarter(conn, "SECN", "2024-06-30", complete=False, run_id="BASE")
        _insert_quarter(conn, "SECN", "2024-09-30", complete=False, run_id="BASE")
        _insert_quarter(conn, "SECN", "2024-12-31", complete=False, run_id="BASE")
        _insert_quarter(conn, "SECN", "2025-03-31", complete=False, run_id="BASE")
        _insert_quarter(conn, "SECN", "2025-06-30", complete=False, run_id="BASE")

        _insert_quarter(conn, "BEST", "2023-03-31", complete=False, run_id="BASE")
        _insert_quarter(conn, "BEST", "2025-03-31", complete=False, run_id="BASE")
        _insert_quarter(conn, "BEST", "2025-06-30", complete=False, run_id="BASE")
        _insert_quarter(conn, "BEST", "2025-09-30", complete=False, run_id="BASE")
        _insert_quarter(conn, "BEST", "2025-12-31", complete=False, run_id="BASE")
        _insert_sec_fact(conn, "BEST", "2023-03-31", "Revenues|form=10-Q|unit=USD|fy=2023|fp=Q1|frame=CY2023Q1|start=2023-01-01|filed=2023-05-01", 100.0)

        _insert_quarter(conn, "FAIL", "2026-03-31", complete=False, run_id="BASE")
        _insert_status(conn, "FAIL", "2026-03-31", "FETCH_FAILED", quarter_basic=0)

        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_earnings_match (
                market, ticker, period_end_date, earnings_event_id, announcement_at,
                announcement_date, announcement_session, effective_trading_date,
                effective_date_status, reporting_delay_days, matching_status,
                matching_confidence, matching_method, candidate_count,
                availability_policy, matcher_version, created_at_utc, updated_at_utc
            ) VALUES ('usa', 'REVIEW', '2026-03-31', 1, '2026-04-01T12:00:00Z',
                '2026-04-01', 'BEFORE_MARKET', '2026-04-01', 'KNOWN', 45,
                'MATCHED', 'LOW', 'fixture', 2, 'fixture', 'fixture',
                '2026-04-01T12:00:00Z', '2026-04-01T12:00:00Z')
            """
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        plan = build_historical_backfill_plan(conn, yahoo_recent_targets=4)
        replay = build_historical_backfill_plan(conn, yahoo_recent_targets=4)

    by_key = {(row["ticker"], row["target_period_end_date"]): row for row in plan["quarter_plan"]}
    assert by_key[("COMP", "2026-03-31")]["proposed_quarter_action"] == ACTION_NO_ACTION_COMPLETE
    assert by_key[("CACHE", "2026-03-31")]["proposed_quarter_action"] == ACTION_OFFLINE_MERGE_AVAILABLE
    assert by_key[("SECN", "2024-03-31")]["proposed_quarter_action"] == ACTION_NEEDS_SEC_HISTORY_REFRESH
    assert by_key[("SECN", "2025-06-30")]["proposed_quarter_action"] == ACTION_NEEDS_SEC_AND_YAHOO
    assert by_key[("YHOO", "2026-03-31")]["proposed_quarter_action"] == ACTION_NEEDS_YAHOO_RECENT_ENRICHMENT
    assert by_key[("BEST", "2023-03-31")]["proposed_quarter_action"] == ACTION_PARTIAL_BEST_AVAILABLE
    assert by_key[("FAIL", "2026-03-31")]["proposed_quarter_action"] == ACTION_RETRYABLE_FAILURE
    assert by_key[("REVIEW", "2026-03-31")]["proposed_quarter_action"] == ACTION_TARGET_IDENTITY_REVIEW

    ticker_plan = {row["ticker"]: row for row in plan["ticker_provider_plan"]}
    assert ticker_plan["YHOO"]["yahoo_fetch_needed"] == 1
    assert ticker_plan["YHOO"]["yahoo_reason_count"] == 4
    assert ticker_plan["SECN"]["sec_fetch_needed"] == 1
    assert ticker_plan["SECN"]["sec_reason_count"] == 6
    assert plan_content_hash(plan) == plan_content_hash(replay)


def _insert_quarter(conn: sqlite3.Connection, ticker: str, period: str, *, complete: bool, run_id: str) -> None:
    values = {
        "revenue": 100.0 if complete else 100.0,
        "ebit": 10.0 if complete else None,
        "free_cashflow": 8.0 if complete else None,
        "cash": 20.0 if complete else None,
        "total_debt": 30.0 if complete else None,
        "shares_outstanding": 1000.0 if complete else None,
    }
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker, period_end_date, revenue, ebit, free_cashflow,
            cash, total_debt, shares_outstanding, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            period,
            values["revenue"],
            values["ebit"],
            values["free_cashflow"],
            values["cash"],
            values["total_debt"],
            values["shares_outstanding"],
            run_id,
        ),
    )


def _insert_status(conn: sqlite3.Connection, ticker: str, period: str, status: str, *, quarter_basic: int) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarter_ingestion_status (
            market, ticker, period_end_date, ingestion_status, basic_status,
            quarter_basic_complete, ttm_input_complete, score_history_complete,
            valuation_input_ready, historical_research_ready, available_basic_field_count,
            missing_basic_fields, missing_core_fields_json, missing_ttm_fields_json,
            missing_score_fields_json, data_quality_warnings_json, retry_recommendation,
            source_confirmation_status, last_checked_at_utc, assessment_policy_version,
            ingestion_evidence_type, run_id, assessed_at_utc, created_at_utc, updated_at_utc
        ) VALUES ('usa', ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, ?, '[]', '[]', '[]',
            'RERUN_CHECK_THEN_PLAN_UPDATE', 'SOURCE_CONFIRMATION_UNKNOWN',
            '2026-01-01T00:00:00Z', 'fixture', 'MANAGED_UPDATE_ATTEMPT',
            'STATUS_FIXTURE', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
            '2026-01-01T00:00:00Z')
        """,
        (
            ticker,
            period,
            status,
            "BASIC_COMPLETE" if quarter_basic else "BASIC_PARTIAL",
            quarter_basic,
            "[]" if quarter_basic else json.dumps(["ebit", "free_cashflow", "cash", "total_debt", "shares_outstanding"]),
            "[]" if quarter_basic else json.dumps(["ebit", "free_cashflow", "cash", "total_debt", "shares_outstanding"]),
        ),
    )


def _insert_yahoo_quarter(conn: sqlite3.Connection, ticker: str, period: str, **fields: float | None) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_yahoo_quarterly (
            market, symbol, period_end_date, revenue, ebit, ebitda, free_cashflow,
            cash, total_debt, shares_outstanding, source_run_id, run_id, created_at_utc
        ) VALUES ('usa', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RAW', 'YQTR', '2026-01-01T00:00:00Z')
        """,
        (
            ticker,
            period,
            fields.get("revenue"),
            fields.get("ebit"),
            fields.get("ebitda"),
            fields.get("free_cashflow"),
            fields.get("cash"),
            fields.get("total_debt"),
            fields.get("shares_outstanding"),
        ),
    )


def _insert_sec_fact(conn: sqlite3.Connection, ticker: str, period: str, field_name: str, value: float) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_statement_raw (
            ticker, statement_type, period_end_date, field_name, field_value,
            source, period_type, run_id, retrieved_at_utc
        ) VALUES (?, 'income', ?, ?, ?, 'sec_edgar', 'sec_fact', 'SEC_FIXTURE', '2026-01-01T00:00:00Z')
        """,
        (ticker, period, field_name, value),
    )
