from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.fundamentals.earnings_event_matching import (
    EarningsAnnouncement,
    QuarterPeriod,
    announcement_effective_trading_date,
    audit_universe,
    field_effective_trading_date,
    inspect_ticker,
    match_periods_to_events,
    validate_temp_path,
)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
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
        CREATE TABLE rc_earnings_event (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT NOT NULL,
            ticker TEXT NOT NULL,
            announcement_at TEXT NOT NULL,
            announcement_date TEXT NOT NULL,
            announcement_session TEXT NOT NULL,
            is_reported INTEGER NOT NULL,
            reported_eps REAL,
            estimated_eps REAL,
            surprise_pct REAL,
            source TEXT NOT NULL,
            source_observed_at_utc TEXT NOT NULL,
            source_timezone TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        CREATE TABLE rc_fundamental_quarterly_field_provenance (
            ticker TEXT NOT NULL,
            market TEXT,
            period_end_date TEXT NOT NULL,
            statement_vintage_id TEXT NOT NULL,
            field_name TEXT NOT NULL,
            field_value REAL,
            source_provider TEXT NOT NULL,
            source_table TEXT,
            source_row_ref TEXT,
            source_document_id TEXT,
            source_hash TEXT,
            provenance_role TEXT NOT NULL,
            merge_action TEXT NOT NULL,
            old_value REAL,
            new_value REAL,
            available_at_utc TEXT,
            created_at_utc TEXT NOT NULL,
            run_id TEXT,
            enrichment_run_id TEXT
        );
        CREATE TABLE rc_fundamental_quarterly_vintage (
            ticker TEXT NOT NULL,
            market TEXT,
            period_end_date TEXT NOT NULL,
            statement_vintage_id TEXT NOT NULL,
            source_provider TEXT NOT NULL,
            source_document_id TEXT,
            source_hash TEXT,
            revision_number INTEGER NOT NULL DEFAULT 1,
            is_restated INTEGER NOT NULL DEFAULT 0,
            supersedes_vintage_id TEXT,
            availability_quality TEXT NOT NULL DEFAULT 'ESTIMATED',
            filed_at_utc TEXT,
            available_at_utc TEXT NOT NULL,
            ingested_at_utc TEXT NOT NULL,
            provider_observed_at_utc TEXT,
            run_id TEXT,
            provider_run_id TEXT,
            normalization_run_id TEXT,
            enrichment_run_id TEXT,
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
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT
        );
        """
    )
    return conn


def _period(text: str, ticker: str = "AAPL") -> QuarterPeriod:
    return QuarterPeriod(ticker=ticker, period_end_date=text, non_null_field_count=12)


def _event(text: str, session: str = "AFTER_MARKET", ticker: str = "AAPL") -> EarningsAnnouncement:
    return EarningsAnnouncement(
        ticker=ticker,
        announcement_at=f"{text}T16:00:00-04:00",
        announcement_date=text,
        announcement_session=session,
        reported_eps=1.0,
        estimated_eps=0.9,
        source_observed_at_utc="2026-07-31T00:00:00Z",
    )


def test_basic_quarter_to_event_matching_and_effective_date() -> None:
    matches = match_periods_to_events(
        [_period("2024-03-31"), _period("2024-06-30")],
        [_event("2024-05-02"), _event("2024-08-01")],
    )

    assert [match.match_status for match in matches] == ["MATCHED_HIGH_CONFIDENCE", "MATCHED_HIGH_CONFIDENCE"]
    assert matches[0].reporting_delay_days == 32
    assert matches[0].announcement_effective_trading_date == "2024-05-03"


def test_sequence_preservation_missing_extra_and_partial_history() -> None:
    matches = match_periods_to_events(
        [_period("2024-03-31"), _period("2024-06-30"), _period("2024-09-30")],
        [_event("2024-05-01"), _event("2025-02-01")],
        max_delay_days=120,
    )

    assert matches[0].match_status == "MATCHED_HIGH_CONFIDENCE"
    assert matches[1].match_status == "UNMATCHED_OUTSIDE_WINDOW"
    assert matches[2].match_status == "UNMATCHED_OUTSIDE_WINDOW"


def test_ambiguity_handling_for_multiple_events_inside_period_gap() -> None:
    matches = match_periods_to_events(
        [_period("2024-03-31"), _period("2024-06-30")],
        [_event("2024-04-15"), _event("2024-05-15"), _event("2024-08-01")],
    )

    assert matches[0].match_status == "AMBIGUOUS_MULTIPLE_EVENTS"
    assert matches[1].match_status == "MATCHED_HIGH_CONFIDENCE"


def test_field_availability_classifications_and_source_replacement() -> None:
    conn = _conn()
    _insert_quarter(conn, "AAPL", "2024-03-31")
    _insert_provenance(conn, "AAPL", "2024-03-31", "revenue", 100.0, "yahoo", "2024-05-01T20:00:00Z", "v1")
    _insert_provenance(conn, "AAPL", "2024-03-31", "revenue", 100.0, "sec_edgar", "2024-05-05T00:00:00Z", "v2")
    _insert_provenance(conn, "AAPL", "2024-03-31", "ebit", 20.0, "sec_edgar", "2024-05-05T00:00:00Z", "v2")
    _insert_provenance(conn, "AAPL", "2024-03-31", "net_income", 15.0, "UNKNOWN_LEGACY", "2026-06-19T00:00:00Z", "v0")

    inspected = inspect_ticker(conn, "AAPL")
    availability = {
        (item["period_end_date"], item["field_name"]): item
        for item in inspected["field_availability"]
    }

    revenue = availability[("2024-03-31", "revenue")]
    assert revenue["first_source_provider"] == "yahoo"
    assert revenue["latest_source_provider"] == "sec_edgar"
    assert revenue["first_available_at_utc"] == "2024-05-01T20:00:00Z"
    assert revenue["availability_status"] == "FIELD_AVAILABILITY_FILING_BOUND"
    assert availability[("2024-03-31", "net_income")]["availability_status"] == "HISTORICAL_TIMING_NOT_RECONSTRUCTABLE"


def test_consumer_specific_row_readiness_and_unknown_historical_availability() -> None:
    conn = _conn()
    _insert_quarter(conn, "AAPL", "2024-03-31")
    for field in ("revenue", "gross_profit", "ebit", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding"):
        _insert_provenance(conn, "AAPL", "2024-03-31", field, 1.0, "yahoo", "2024-05-01T20:00:00Z", f"v-{field}")
    _insert_provenance(conn, "AAPL", "2024-03-31", "net_income", 1.0, "UNKNOWN_LEGACY", "2026-06-19T00:00:00Z", "v0")

    readiness = inspect_ticker(conn, "AAPL")["row_readiness"][0]

    assert readiness["consumer_available_at"]["ttm"] == "2024-05-01T20:00:00Z"
    assert readiness["consumer_available_at"]["score"] == "2024-05-01T20:00:00Z"
    assert readiness["row_state"] == "MINIMUM_SCORING_FIELDS_AVAILABLE"


def test_effective_date_rules() -> None:
    assert announcement_effective_trading_date("2024-05-03", "BEFORE_MARKET") == "2024-05-03"
    assert announcement_effective_trading_date("2024-05-03", "DURING_MARKET") == "2024-05-03"
    assert announcement_effective_trading_date("2024-05-03", "AFTER_MARKET") == "2024-05-06"
    assert announcement_effective_trading_date("2024-05-03", "UNKNOWN") is None
    assert field_effective_trading_date("2024-05-03T22:15:00Z") == "2024-05-03"


def test_readonly_audit_does_not_write_and_is_deterministic(tmp_path: Path) -> None:
    db_path = tmp_path / "fixture.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _create_file_db_schema(conn)
    _insert_quarter(conn, "AAPL", "2024-03-31")
    _insert_event(conn, "AAPL", "2024-05-01")
    _insert_provenance(conn, "AAPL", "2024-03-31", "revenue", 100.0, "yahoo", "2024-05-01T20:00:00Z", "v1")
    conn.commit()
    before = conn.total_changes
    conn.close()

    first = audit_universe(db_path, tickers=["AAPL"])
    second = audit_universe(db_path, tickers=["AAPL"])

    assert first["per_ticker"] == second["per_ticker"]
    conn = sqlite3.connect(db_path)
    assert conn.total_changes == 0
    assert before > 0


def test_temp_only_runtime_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from swingmaster.fundamentals import earnings_event_matching

    temp_root = tmp_path / "repo" / "temp"
    temp_root.mkdir(parents=True)
    monkeypatch.setattr(earnings_event_matching, "temp_root", lambda: temp_root)

    assert validate_temp_path(temp_root / "audit" / "out.json") == (temp_root / "audit" / "out.json").resolve()
    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        validate_temp_path(tmp_path / "outside.json")


def _create_file_db_schema(conn: sqlite3.Connection) -> None:
    memory = _conn()
    sql = (
        ";\n".join(
            row[0]
            for row in memory.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL"
            )
        )
        + ";"
    )
    conn.executescript(sql)


def _insert_quarter(conn: sqlite3.Connection, ticker: str, period: str) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker, period_end_date, revenue, gross_profit, operating_income, ebit, ebitda,
            net_income, operating_cashflow, capex, free_cashflow, cash, total_debt,
            shares_outstanding, currency, run_id
        ) VALUES (?, ?, 100, 50, 25, 20, 22, 15, 12, -3, 9, 40, 10, 1000, 'USD', 'test')
        """,
        (ticker, period),
    )


def _insert_event(conn: sqlite3.Connection, ticker: str, announcement_date: str) -> None:
    conn.execute(
        """
        INSERT INTO rc_earnings_event (
            market, ticker, announcement_at, announcement_date, announcement_session, is_reported,
            reported_eps, estimated_eps, surprise_pct, source, source_observed_at_utc,
            source_timezone, created_at_utc, updated_at_utc
        ) VALUES ('usa', ?, ?, ?, 'AFTER_MARKET', 1, 1.0, 0.9, 10.0, 'YAHOO_FINANCE',
                  '2026-07-31T00:00:00Z', 'America/New_York', '2026-07-31T00:00:00Z', '2026-07-31T00:00:00Z')
        """,
        (ticker, f"{announcement_date}T16:00:00-04:00", announcement_date),
    )


def _insert_provenance(
    conn: sqlite3.Connection,
    ticker: str,
    period: str,
    field_name: str,
    value: float,
    source: str,
    available_at: str,
    vintage_id: str,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_field_provenance (
            ticker, market, period_end_date, statement_vintage_id, field_name, field_value,
            source_provider, source_table, source_row_ref, source_document_id, source_hash,
            provenance_role, merge_action, old_value, new_value, available_at_utc,
            created_at_utc, run_id, enrichment_run_id
        ) VALUES (?, 'usa', ?, ?, ?, ?, ?, NULL, NULL, NULL, 'hash',
                  'PRIMARY_REPORTED', 'RETAINED', NULL, ?, ?, ?, 'test', NULL)
        """,
        (ticker, period, vintage_id, field_name, value, source, value, available_at, available_at),
    )
