from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from swingmaster.cli.audit_fundamental_effective_date_usage import main as cli_main
from swingmaster.fundamentals.earnings_events import repository_root
from swingmaster.fundamentals.effective_date_audit import (
    audit_effective_date_usage,
    classify_consumer,
    classify_severity,
    compare_selection,
    open_readonly_db,
    ttm_component_effective_date,
    usa_quarterly_universe,
)


def test_current_vs_safe_selection_between_period_end_and_announcement() -> None:
    db = _build_db()
    _insert_quarter(db, "AAPL", "2026-03-31")
    _insert_quarter(db, "AAPL", "2026-06-30")
    _insert_match(db, "AAPL", "2026-03-31", "2026-04-28")
    _insert_match(db, "AAPL", "2026-06-30", "2026-07-31")

    with open_readonly_db(db) as conn:
        row = compare_selection(conn, "AAPL", "2026-07-15")

    assert row.current_selected_period_end == "2026-06-30"
    assert row.safe_selected_period_end == "2026-03-31"
    assert row.period_selection_differs is True
    assert row.lookahead_days == 16


def test_selection_on_effective_date_and_after_effective_date() -> None:
    db = _build_db()
    _insert_quarter(db, "MSFT", "2026-03-31")
    _insert_match(db, "MSFT", "2026-03-31", "2026-04-25")

    with open_readonly_db(db) as conn:
        on_date = compare_selection(conn, "MSFT", "2026-04-25")
        after_date = compare_selection(conn, "MSFT", "2026-05-01")

    assert on_date.safe_selected_period_end == "2026-03-31"
    assert on_date.period_selection_differs is False
    assert after_date.safe_selected_period_end == "2026-03-31"
    assert after_date.lookahead_days is None


def test_unmatched_latest_quarter_excluded_from_safe_selection() -> None:
    db = _build_db()
    _insert_quarter(db, "JPM", "2026-03-31")
    _insert_quarter(db, "JPM", "2026-06-30")
    _insert_match(db, "JPM", "2026-03-31", "2026-04-14")

    with open_readonly_db(db) as conn:
        row = compare_selection(conn, "JPM", "2026-07-15")

    assert row.current_selected_period_end == "2026-06-30"
    assert row.current_selected_effective_date is None
    assert row.safe_selected_period_end == "2026-03-31"
    assert row.period_selection_differs is True


def test_null_effective_date_is_not_safe_available() -> None:
    db = _build_db()
    _insert_quarter(db, "XOM", "2026-03-31")
    _insert_match(db, "XOM", "2026-03-31", None, effective_date_status="NO_TRADING_CALENDAR_DATE")

    with open_readonly_db(db) as conn:
        row = compare_selection(conn, "XOM", "2026-05-01")

    assert row.current_selected_period_end == "2026-03-31"
    assert row.safe_selected_period_end is None
    assert row.effective_date_status == "NO_TRADING_CALENDAR_DATE"


def test_multiple_quarters_and_irregular_fiscal_calendar() -> None:
    db = _build_db()
    for period, effective in [
        ("2025-01-31", "2025-03-05"),
        ("2025-04-30", "2025-06-04"),
        ("2025-07-31", "2025-09-03"),
    ]:
        _insert_quarter(db, "NVDA", period)
        _insert_match(db, "NVDA", period, effective)

    with open_readonly_db(db) as conn:
        row = compare_selection(conn, "NVDA", "2025-08-15")

    assert row.current_selected_period_end == "2025-07-31"
    assert row.safe_selected_period_end == "2025-04-30"


def test_ttm_max_component_effective_date() -> None:
    db = _build_db()
    for period, effective in [
        ("2025-03-31", "2025-04-20"),
        ("2025-06-30", "2025-07-25"),
        ("2025-09-30", "2025-10-24"),
        ("2025-12-31", "2026-02-01"),
    ]:
        _insert_quarter(db, "AAPL", period)
        _insert_match(db, "AAPL", period, effective)

    with open_readonly_db(db) as conn:
        assert ttm_component_effective_date(conn, "AAPL", "2025-12-31") == "2026-02-01"


def test_confidence_and_status_are_reported() -> None:
    db = _build_db()
    _insert_quarter(db, "LOW", "2026-03-31")
    _insert_match(db, "LOW", "2026-03-31", "2026-05-15", confidence="LOW", matching_status="MATCHED_LOW_CONFIDENCE")

    with open_readonly_db(db) as conn:
        row = compare_selection(conn, "LOW", "2026-04-15")

    assert row.matching_confidence == "LOW"
    assert row.matching_status == "MATCHED_LOW_CONFIDENCE"
    assert row.effective_date_status == "RESOLVED_SAME_TRADING_DAY"


def test_classification_helpers() -> None:
    assert classify_consumer("current", reads_fundamentals=True, historical=False) == "CURRENT_ONLY_NO_CHANGE"
    assert classify_consumer("historical derived", reads_fundamentals=True, historical=True) == "DERIVED_TABLE_NEEDS_REBUILD_POLICY"
    assert classify_severity("CURRENT_ONLY_NO_CHANGE") == "CURRENT_STATE_ONLY"
    assert classify_severity("HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE") == "MATERIAL_HISTORICAL_LOOKAHEAD"


def test_deterministic_audit_output_and_usa_universe_excludes_nokia_he() -> None:
    db = _build_db()
    _insert_quarter(db, "AAPL", "2026-03-31")
    _insert_match(db, "AAPL", "2026-03-31", "2026-04-28")
    _insert_quarter(db, "NOKIA.HE", "2026-03-31")

    first = audit_effective_date_usage(db, as_of_date="2026-04-01")
    second = audit_effective_date_usage(db, as_of_date="2026-04-01")

    assert first["comparisons"] == second["comparisons"]
    assert first["universe_impact"] == second["universe_impact"]
    with open_readonly_db(db) as conn:
        assert usa_quarterly_universe(conn) == ["AAPL"]


def test_cli_temp_outputs_and_no_database_writes() -> None:
    db = _build_db()
    _insert_quarter(db, "AAPL", "2026-03-31")
    _insert_match(db, "AAPL", "2026-03-31", "2026-04-28")
    root = _runtime_root() / "cli"

    before = _counts(db)
    assert cli_main(["--fundamentals-db", str(db), "--ticker", "AAPL", "--as-of-date", "2026-04-01", "--output-root", str(root)]) == 0
    after = _counts(db)

    assert before == after
    assert (root / "summary.json").exists()
    assert (root / "comparisons.csv").exists()
    assert (root / "consumer_dependency_map.csv").exists()


def test_cli_rejects_paths_outside_temp() -> None:
    db = _build_db()
    bad_root = repository_root() / "effective-date-outside-temp"

    try:
        cli_main(["--fundamentals-db", str(db), "--output-root", str(bad_root)])
    except ValueError as exc:
        assert "RUNTIME_PATH_OUTSIDE_TEMP" in str(exc)
    else:
        raise AssertionError("expected outside-temp path rejection")


def _build_db() -> Path:
    path = _runtime_root() / f"{uuid.uuid4().hex}.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT,
                period_end_date TEXT,
                revenue REAL,
                gross_profit REAL,
                operating_income REAL,
                ebit REAL,
                ebitda REAL,
                operating_cashflow REAL,
                free_cashflow REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL
            );
            CREATE TABLE rc_fundamental_quarter_earnings_match (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market TEXT,
                ticker TEXT,
                period_end_date TEXT,
                earnings_event_id INTEGER,
                announcement_at TEXT,
                announcement_date TEXT,
                announcement_session TEXT,
                effective_trading_date TEXT,
                effective_date_status TEXT,
                reporting_delay_days INTEGER,
                matching_status TEXT,
                matching_confidence TEXT,
                matching_method TEXT,
                candidate_count INTEGER,
                availability_policy TEXT,
                matcher_version TEXT,
                created_at_utc TEXT,
                updated_at_utc TEXT
            );
            CREATE TABLE rc_earnings_event (id INTEGER PRIMARY KEY, ticker TEXT);
            CREATE TABLE rc_fundamental_ttm (
                ticker TEXT,
                as_of_date TEXT,
                latest_period_end_date TEXT
            );
            CREATE TABLE rc_fundamental_score_percentile (
                ticker TEXT,
                as_of_date TEXT,
                target_date TEXT
            );
            CREATE TABLE rc_fundamental_valuation (
                ticker TEXT,
                as_of_date TEXT,
                valuation_fundamental_as_of_date TEXT
            );
            """
        )
    return path


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamental_effective_date_audit" / "tests"


def _insert_quarter(db: Path, ticker: str, period: str) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly(
                ticker, period_end_date, revenue, gross_profit, operating_income,
                ebit, ebitda, operating_cashflow, free_cashflow, cash,
                total_debt, shares_outstanding
            ) VALUES (?, ?, 100, 50, 20, 20, 22, 18, 15, 10, 5, 1000)
            """,
            (ticker, period),
        )


def _insert_match(
    db: Path,
    ticker: str,
    period: str,
    effective: str | None,
    *,
    confidence: str = "HIGH",
    matching_status: str = "MATCHED_HIGH_CONFIDENCE",
    effective_date_status: str = "RESOLVED_SAME_TRADING_DAY",
) -> None:
    reporting_delay_days = 0 if effective is None else (date_from_text(effective) - date_from_text(period)).days
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_earnings_match(
                market, ticker, period_end_date, earnings_event_id, announcement_at,
                announcement_date, announcement_session, effective_trading_date,
                effective_date_status, reporting_delay_days, matching_status,
                matching_confidence, matching_method, candidate_count, availability_policy,
                matcher_version, created_at_utc, updated_at_utc
            ) VALUES ('usa', ?, ?, 1, ?, ?, 'bmo', ?, ?, ?, ?, ?, 'fixture', 1,
                      'EARNINGS_EFFECTIVE_DATE_ASSUMED', 'fixture', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (ticker, period, effective or period, effective or period, effective, effective_date_status, reporting_delay_days, matching_status, confidence),
        )


def date_from_text(value: str):
    from datetime import date

    return date.fromisoformat(value)


def _counts(db: Path) -> dict[str, int]:
    with sqlite3.connect(db) as conn:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("rc_fundamental_quarterly", "rc_fundamental_quarter_earnings_match", "rc_earnings_event")
        }
