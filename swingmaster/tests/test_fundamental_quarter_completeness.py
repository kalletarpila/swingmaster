from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from swingmaster.cli.audit_fundamental_quarter_completeness import main as audit_cli_main
from swingmaster.fundamentals.quarter_completeness import (
    assess_quarter_completeness,
    assess_ticker_quarter_history,
    audit_quarter_completeness,
    calendar_status_transition,
    ingestion_status_transition,
    repository_root,
)


def test_complete_partial_incomplete_empty_and_invalid_period() -> None:
    complete = assess_quarter_completeness(_row("GOOD", "2024-03-31", revenue=100, ebit=12, gross_profit=40, free_cashflow=9, shares_outstanding=10))
    partial = assess_quarter_completeness(_row("PART", "2024-03-31", revenue=100, ebit=12, free_cashflow=8))
    incomplete = assess_quarter_completeness(_row("BAD", "2024-03-31", revenue=100))
    empty = assess_quarter_completeness(_row("EMPTY", "2024-03-31"))
    invalid = assess_quarter_completeness(_row("INV", "not-a-date", revenue=100))

    assert complete.basic_status == "BASIC_COMPLETE"
    assert partial.basic_status == "BASIC_PARTIAL"
    assert incomplete.basic_status == "BASIC_INCOMPLETE"
    assert empty.basic_status == "EMPTY_OR_PLACEHOLDER"
    assert invalid.basic_status == "NOT_ASSESSABLE"


def test_consumer_readiness_can_differ_and_optional_fields_do_not_block_basic_complete() -> None:
    row = assess_quarter_completeness(
        _row("DIFF", "2024-03-31", revenue=100, ebit=12, gross_profit=40, free_cashflow=9, shares_outstanding=10)
    )

    assert row.basic_status == "BASIC_COMPLETE"
    assert row.ttm_ready is True
    assert row.score_input_ready is True
    assert row.valuation_input_ready is True
    assert "ebitda" in row.missing_core_fields

    no_shares = assess_quarter_completeness(_row("NOSH", "2024-03-31", revenue=100, ebit=12, free_cashflow=9))
    assert no_shares.basic_status == "BASIC_PARTIAL"
    assert no_shares.ttm_ready is True
    assert no_shares.score_input_ready is True
    assert no_shares.valuation_input_ready is False


def test_retry_recommendation_and_ticker_classifications() -> None:
    matched_empty = assess_quarter_completeness(_row("R", "2025-03-31", earnings_event_id=1))
    assert matched_empty.retry_recommendation == "RETRY_YAHOO_AND_SEC"

    latest_bad = [
        assess_quarter_completeness(_row("T", "2023-03-31", revenue=100, ebit=5, free_cashflow=4)),
        assess_quarter_completeness(_row("T", "2023-06-30", revenue=100, ebit=5, free_cashflow=4)),
        assess_quarter_completeness(_row("T", "2023-09-30", revenue=100, ebit=5, free_cashflow=4)),
        assess_quarter_completeness(_row("T", "2023-12-31")),
    ]
    assert assess_ticker_quarter_history(latest_bad)["classification"] == "LATEST_QUARTER_INCOMPLETE"

    old_gap = [
        assess_quarter_completeness(_row("O", "2020-03-31")),
        assess_quarter_completeness(_row("O", "2023-03-31", revenue=100, ebit=5, free_cashflow=4)),
        assess_quarter_completeness(_row("O", "2023-06-30", revenue=100, ebit=5, free_cashflow=4)),
        assess_quarter_completeness(_row("O", "2023-09-30", revenue=100, ebit=5, free_cashflow=4)),
        assess_quarter_completeness(_row("O", "2023-12-31", revenue=100, ebit=5, free_cashflow=4)),
    ]
    assert assess_ticker_quarter_history(old_gap)["classification"] == "RECENT_HISTORY_USABLE_OLD_GAPS"


def test_earnings_relationship_aggregation_and_cli_no_write() -> None:
    db = _build_db()
    _insert_quarter(db, "AAA", "2024-03-31", revenue=100, ebit=10, gross_profit=40, free_cashflow=8, shares_outstanding=10)
    _insert_quarter(db, "BBB", "2024-03-31")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO rc_earnings_event(id, market, ticker, announcement_at, announcement_date, announcement_session, is_reported, source, source_observed_at_utc, source_timezone, created_at_utc, updated_at_utc) VALUES (1,'usa','BBB','2024-04-20T12:00:00-04:00','2024-04-20','DURING_MARKET',1,'YAHOO_FINANCE','2024-04-21T00:00:00Z','America/New_York','2024-04-21T00:00:00Z','2024-04-21T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO rc_fundamental_quarter_earnings_match(market, ticker, period_end_date, earnings_event_id, announcement_at, announcement_date, announcement_session, effective_trading_date, effective_date_status, reporting_delay_days, matching_status, matching_confidence, matching_method, candidate_count, availability_policy, matcher_version, created_at_utc, updated_at_utc) VALUES ('usa','BBB','2024-03-31',1,'2024-04-20T12:00:00-04:00','2024-04-20','DURING_MARKET','2024-04-22','RESOLVED',22,'MATCHED','HIGH','nearest',1,'event_effective_date','v1','2024-04-21T00:00:00Z','2024-04-21T00:00:00Z')"
        )

    before = _counts(db)
    payload = audit_quarter_completeness(db)
    assert payload["summary"]["basic_complete_count"] == 1
    assert payload["summary"]["empty_or_placeholder_count"] == 1
    assert payload["summary"]["earnings_relationship"]["quarterly_rows_with_earnings_match_but_incomplete"] == 1
    assert before == _counts(db)

    root = _runtime_root() / f"cli-{uuid.uuid4().hex}"
    assert audit_cli_main(["--fundamentals-db", str(db), "--output-root", str(root)]) == 0
    assert before == _counts(db)
    assert (root / "all_quarters.csv").exists()
    assert (root / "ticker_summary.csv").exists()
    assert (root / "field_completeness.csv").exists()
    assert (root / "retry_candidates.csv").exists()
    assert (root / "latest_quarter_issues.csv").exists()
    assert json.loads((root / "summary.json").read_text(encoding="utf-8"))["database_content_unchanged"] is True


def test_cli_rejects_paths_outside_temp_and_resume_path_must_be_temp() -> None:
    db = _build_db()
    bad_root = repository_root() / "outside-quarter-audit"
    try:
        audit_cli_main(["--fundamentals-db", str(db), "--output-root", str(bad_root)])
    except ValueError as exc:
        assert "RUNTIME_PATH_OUTSIDE_TEMP" in str(exc)
    else:
        raise AssertionError("expected outside-temp path rejection")


def test_future_calendar_and_ingestion_status_transitions() -> None:
    assert calendar_status_transition(estimated_announcement_date="2026-08-06", today="2026-08-06", completed_event_found=False).calendar_status == "DUE_TODAY"
    assert calendar_status_transition(estimated_announcement_date="2026-08-05", today="2026-08-06", completed_event_found=False).calendar_status == "DATE_PASSED_EVENT_NOT_FOUND"
    assert calendar_status_transition(estimated_announcement_date="2026-08-07", today="2026-08-06", completed_event_found=True).calendar_status == "COMPLETED_EVENT_FOUND"
    assert calendar_status_transition(estimated_announcement_date="2026-08-08", previous_estimated_announcement_date="2026-08-07", today="2026-08-06", completed_event_found=False).calendar_status == "DATE_CHANGED"

    complete = assess_quarter_completeness(_row("GOOD", "2024-03-31", revenue=100, ebit=12, gross_profit=40, free_cashflow=9, shares_outstanding=10))
    assert ingestion_status_transition(published=False, fetched=False, fetch_failed=False, assessment=None).ingestion_status == "NOT_PUBLISHED"
    assert ingestion_status_transition(published=True, fetched=False, fetch_failed=False, assessment=None).ingestion_status == "PUBLISHED_DATA_NOT_FETCHED"
    assert ingestion_status_transition(published=True, fetched=True, fetch_failed=True, assessment=complete).ingestion_status == "FETCH_FAILED"
    assert ingestion_status_transition(published=True, fetched=True, fetch_failed=False, assessment=complete).ingestion_status == "BASIC_COMPLETE"
    assert ingestion_status_transition(published=True, fetched=True, fetch_failed=False, assessment=complete, source_compare_complete=True).ingestion_status == "INGEST_COMPLETE"
    assert ingestion_status_transition(published=True, fetched=True, fetch_failed=False, assessment=complete, source_compare_complete=True, historical=True).ingestion_status == "UNKNOWN_HISTORICAL_INGEST_COMPLETENESS"


def _row(ticker: str, period: str, **values: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": ticker,
        "period_end_date": period,
        "revenue": None,
        "gross_profit": None,
        "operating_income": None,
        "ebit": None,
        "ebitda": None,
        "net_income": None,
        "operating_cashflow": None,
        "capex": None,
        "free_cashflow": None,
        "cash": None,
        "total_debt": None,
        "shares_outstanding": None,
        "currency": "USD",
        "run_id": "TEST",
        "earnings_event_id": None,
        "announcement_date": None,
        "effective_trading_date": None,
    }
    row.update(values)
    return row


def _build_db() -> Path:
    path = _runtime_root() / f"{uuid.uuid4().hex}.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
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
            CREATE TABLE rc_fundamental_ttm (ticker TEXT, as_of_date TEXT);
            CREATE TABLE rc_earnings_event (
                id INTEGER PRIMARY KEY,
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
            CREATE TABLE rc_fundamental_quarter_earnings_match (
                id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                earnings_event_id INTEGER NOT NULL,
                announcement_at TEXT NOT NULL,
                announcement_date TEXT NOT NULL,
                announcement_session TEXT NOT NULL,
                effective_trading_date TEXT,
                effective_date_status TEXT NOT NULL,
                reporting_delay_days INTEGER NOT NULL,
                matching_status TEXT NOT NULL,
                matching_confidence TEXT NOT NULL,
                matching_method TEXT NOT NULL,
                candidate_count INTEGER NOT NULL,
                availability_policy TEXT NOT NULL,
                matcher_version TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            """
        )
    return path


def _insert_quarter(db: Path, ticker: str, period: str, **values: float) -> None:
    row = _row(ticker, period, **values)
    columns = [
        "ticker",
        "period_end_date",
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
        "run_id",
    ]
    with sqlite3.connect(db) as conn:
        conn.execute(
            f"INSERT INTO rc_fundamental_quarterly({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            [row[column] for column in columns],
        )


def _counts(db: Path) -> dict[str, int]:
    with sqlite3.connect(db) as conn:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("rc_fundamental_quarterly", "rc_fundamental_ttm", "rc_earnings_event", "rc_fundamental_quarter_earnings_match")
        }


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamental_quarter_completeness_audit" / "tests"
