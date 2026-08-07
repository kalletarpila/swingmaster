from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import audit_fundamental_quarter_refresh_decisions
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.quarter_refresh_decision import (
    DECISION_FETCH_NEW_QUARTER,
    DECISION_NO_ACTION_COMPLETE,
    DECISION_NO_ACTION_INACTIVE_SECURITY,
    DECISION_NO_ACTION_UPCOMING,
    DECISION_RETRY_FETCH_FAILED,
    DECISION_RETRY_PARTIAL_QUARTER,
    DECISION_REVIEW_AMBIGUOUS_PERIOD,
    DECISION_REVIEW_DATE_PASSED_NO_EVENT,
    DECISION_REVIEW_NO_CALENDAR_ESTIMATE,
    DECISION_WATCH_DUE_TODAY,
    PRIORITY_P1_FETCH_NOW,
    PRIORITY_P2_RETRY,
    PRIORITY_P5_NO_ACTION,
    build_quarter_refresh_decisions,
    classify_quarter_refresh_decision,
    open_readonly_db,
    summarize_quarter_refresh_decisions,
)


def test_upcoming_due_today_date_passed_and_no_estimate_decisions(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="UPCOMING", estimated_date="2026-10-29")
    _seed_ticker(db_path, "MSFT", calendar_status="DUE_TODAY", estimated_date="2026-08-07")
    _seed_ticker(db_path, "PAST", calendar_status="DATE_PASSED_EVENT_NOT_FOUND", estimated_date="2026-08-05")
    _seed_ticker(db_path, "NONE", calendar_status="NO_CURRENT_ESTIMATE", estimated_date=None)

    rows = _decisions(db_path)
    assert rows["AAPL"].decision == DECISION_NO_ACTION_UPCOMING
    assert rows["MSFT"].decision == DECISION_WATCH_DUE_TODAY
    assert rows["PAST"].decision == DECISION_REVIEW_DATE_PASSED_NO_EVENT
    assert rows["NONE"].decision == DECISION_REVIEW_NO_CALENDAR_ESTIMATE
    assert rows["MSFT"].eligible_for_future_auto_fetch == 0


def test_due_today_completed_event_missing_quarter_fetches_without_inventing_period(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="DUE_TODAY", estimated_date="2026-08-07")
    _seed_event(db_path, "AAPL", 7, "2026-08-07")
    _seed_state(db_path, "AAPL", detected_period="2026-06-30")

    row = _decisions(db_path)["AAPL"]
    assert row.decision == DECISION_FETCH_NEW_QUARTER
    assert row.decision_priority == PRIORITY_P1_FETCH_NOW
    assert row.target_period_end_date == "2026-06-30"
    assert row.eligible_for_future_auto_fetch == 1


def test_completed_event_without_period_mapping_is_ambiguous(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="DUE_TODAY", estimated_date="2026-08-07")
    _seed_event(db_path, "AAPL", 7, "2026-08-07")

    row = _decisions(db_path)["AAPL"]
    assert row.decision == DECISION_REVIEW_AMBIGUOUS_PERIOD
    assert row.target_period_end_date is None
    assert row.eligible_for_future_auto_fetch == 0


def test_completed_event_complete_quarter_no_action_even_when_ttm_or_score_incomplete(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="DUE_TODAY", estimated_date="2026-08-07")
    _seed_event(db_path, "AAPL", 7, "2026-08-07")
    _seed_match(db_path, "AAPL", event_id=7, period="2026-06-30")
    _seed_quarter(db_path, "AAPL", "2026-06-30")
    _seed_ingestion_status(db_path, "AAPL", "2026-06-30", quarter_basic=1, ttm=0, score=0)

    row = _decisions(db_path)["AAPL"]
    assert row.decision == DECISION_NO_ACTION_COMPLETE
    assert row.quarter_basic_complete == 1
    assert row.ttm_input_complete == 0
    assert row.score_history_complete == 0
    assert row.eligible_for_future_auto_fetch == 0


def test_completed_event_partial_quarter_retries(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="DUE_TODAY", estimated_date="2026-08-07")
    _seed_event(db_path, "AAPL", 7, "2026-08-07")
    _seed_match(db_path, "AAPL", event_id=7, period="2026-06-30")
    _seed_quarter(db_path, "AAPL", "2026-06-30", complete=False)
    _seed_ingestion_status(db_path, "AAPL", "2026-06-30", quarter_basic=0, missing='["revenue","ebit"]')

    row = _decisions(db_path)["AAPL"]
    assert row.decision == DECISION_RETRY_PARTIAL_QUARTER
    assert row.decision_priority == PRIORITY_P2_RETRY
    assert row.missing_basic_fields == '["revenue","ebit"]'
    assert row.eligible_for_future_auto_fetch == 1


def test_fetch_failed_has_retry_precedence_over_partial(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="DUE_TODAY", estimated_date="2026-08-07")
    _seed_event(db_path, "AAPL", 7, "2026-08-07")
    _seed_match(db_path, "AAPL", event_id=7, period="2026-06-30")
    _seed_quarter(db_path, "AAPL", "2026-06-30", complete=False)
    _seed_ingestion_status(db_path, "AAPL", "2026-06-30", quarter_basic=0, ingestion_status="FETCH_FAILED")

    assert _decisions(db_path)["AAPL"].decision == DECISION_RETRY_FETCH_FAILED


def test_date_passed_with_confirmed_event_can_fetch_when_period_is_safe(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="DATE_PASSED_EVENT_NOT_FOUND", estimated_date="2026-08-06")
    _seed_event(db_path, "AAPL", 7, "2026-08-07")
    _seed_state(db_path, "AAPL", detected_period="2026-06-30")

    row = _decisions(db_path)["AAPL"]
    assert row.decision == DECISION_FETCH_NEW_QUARTER
    assert row.target_period_end_date == "2026-06-30"


def test_inactive_security_suppresses_fetch_candidates() -> None:
    row = classify_quarter_refresh_decision(
        market="usa",
        ticker="OLD",
        decision_date="2026-08-07",
        ohlcv_stale_days=14,
        latest_ohlcv_date="2026-07-01",
        ohlcv_age_days=37,
        market_data_activity_status="STALE_OR_INACTIVE",
        fundamental_fetch_enabled=0,
        last_assessed_at_utc="2026-08-07T00:00:00Z",
        calendar={"calendar_status": "DUE_TODAY", "estimated_announcement_date": "2026-08-07"},
        latest_event={"id": 1, "announcement_date": "2026-08-07"},
        latest_db_period_end_date="2026-03-31",
        detected_source_period_end_date="2026-06-30",
        matched_latest_event_period_end_date=None,
        matched_quarter_status=None,
    )
    assert row.decision == DECISION_NO_ACTION_INACTIVE_SECURITY
    assert row.decision_priority == PRIORITY_P5_NO_ACTION
    assert row.inactive_with_fetch_candidate_before_suppression == 1
    assert row.eligible_for_future_auto_fetch == 0
    assert row.decision_before_activity_suppression == DECISION_FETCH_NEW_QUARTER


def test_summary_is_deterministic_and_counts_auto_fetch(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="DUE_TODAY", estimated_date="2026-08-07")
    _seed_event(db_path, "AAPL", 7, "2026-08-07")
    _seed_state(db_path, "AAPL", detected_period="2026-06-30")
    _seed_ticker(db_path, "MSFT", calendar_status="UPCOMING", estimated_date="2026-10-29")

    rows = list(_decisions(db_path).values())
    assert summarize_quarter_refresh_decisions(rows) == summarize_quarter_refresh_decisions(rows)
    summary = summarize_quarter_refresh_decisions(rows)
    assert summary["fetch_new_quarter"] == 1
    assert summary["eligible_for_future_auto_fetch_count"] == 1
    assert summary["no_action_upcoming"] == 1
    assert summary["active_fetch_count"] == 2
    assert summary["stale_or_inactive_count"] == 0


def test_stale_ohlcv_suppresses_previous_review_decisions(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "OLD1", calendar_status="NO_CURRENT_ESTIMATE", estimated_date=None)
    _seed_ticker(db_path, "OLD2", calendar_status="DATE_PASSED_EVENT_NOT_FOUND", estimated_date="2026-08-06")
    ohlcv_path = db_path.with_name("osakedata.db")
    _seed_ohlcv(ohlcv_path, ["OLD1", "OLD2"], latest_date="2026-07-01")

    with open_readonly_db(db_path) as conn, open_readonly_db(ohlcv_path) as ohlcv_conn:
        rows = build_quarter_refresh_decisions(
            conn,
            ohlcv_conn=ohlcv_conn,
            decision_date="2026-08-07",
            ohlcv_stale_days=14,
        )

    by_ticker = {row.ticker: row for row in rows}
    assert by_ticker["OLD1"].decision == DECISION_NO_ACTION_INACTIVE_SECURITY
    assert by_ticker["OLD1"].decision_before_activity_suppression == DECISION_REVIEW_NO_CALENDAR_ESTIMATE
    assert by_ticker["OLD2"].decision == DECISION_NO_ACTION_INACTIVE_SECURITY
    assert by_ticker["OLD2"].decision_before_activity_suppression == DECISION_REVIEW_DATE_PASSED_NO_EVENT
    summary = summarize_quarter_refresh_decisions(rows)
    assert summary["active_fetch_count"] == 0
    assert summary["stale_or_inactive_count"] == 2
    assert summary["no_ohlcv_count"] == 0
    assert summary["ohlcv_age_over_30_days"] == 2
    assert summary["suppressed_review_no_calendar_estimate_tickers"] == ["OLD1"]
    assert summary["suppressed_review_date_passed_no_event_tickers"] == ["OLD2"]


def test_missing_and_bucketed_ohlcv_activity_counts(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    for ticker in ("A0", "A8", "A15", "A31", "MISS"):
        _seed_ticker(db_path, ticker, calendar_status="UPCOMING", estimated_date="2026-10-29")
    ohlcv_path = db_path.with_name("osakedata.db")
    _seed_ohlcv(ohlcv_path, ["A0"], latest_date="2026-08-07")
    _append_ohlcv(ohlcv_path, "A8", latest_date="2026-07-30")
    _append_ohlcv(ohlcv_path, "A15", latest_date="2026-07-23")
    _append_ohlcv(ohlcv_path, "A31", latest_date="2026-07-07")

    with open_readonly_db(db_path) as conn, open_readonly_db(ohlcv_path) as ohlcv_conn:
        summary = summarize_quarter_refresh_decisions(
            build_quarter_refresh_decisions(
                conn,
                ohlcv_conn=ohlcv_conn,
                decision_date="2026-08-07",
                ohlcv_stale_days=14,
            )
        )

    assert summary["active_fetch_count"] == 2
    assert summary["stale_or_inactive_count"] == 3
    assert summary["no_ohlcv_count"] == 1
    assert summary["ohlcv_age_0_7_days"] == 1
    assert summary["ohlcv_age_8_14_days"] == 1
    assert summary["ohlcv_age_15_30_days"] == 1
    assert summary["ohlcv_age_over_30_days"] == 1


def test_readonly_build_does_not_write_database(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="UPCOMING", estimated_date="2026-10-29")
    before = _counts(db_path)
    with open_readonly_db(db_path) as conn:
        build_quarter_refresh_decisions(conn)
    assert _counts(db_path) == before


def test_cli_writes_temp_only_artifacts_and_filters(tmp_path: Path) -> None:
    db_path = _db(tmp_path)
    _seed_ticker(db_path, "AAPL", calendar_status="UPCOMING", estimated_date="2026-10-29")
    root = Path("/home/kalle/projects/swingmaster/temp/quarter_refresh_decision_tests/cli")
    assert (
        audit_fundamental_quarter_refresh_decisions.main(
            [
                "--fundamentals-db",
                str(db_path),
                "--ohlcv-db",
                str(_ohlcv_db(tmp_path, ["AAPL"])),
                "--decision-date",
                "2026-08-07",
                "--output-root",
                str(root),
                "--decision",
                DECISION_NO_ACTION_UPCOMING,
                "--json",
            ]
        )
        == 0
    )
    assert (root / "quarter_refresh_decisions.csv").exists()
    assert (root / "quarter_refresh_decision_summary.json").exists()
    payload = json.loads((root / "quarter_refresh_decision_summary.json").read_text(encoding="utf-8"))
    assert payload["total_tickers"] == 1
    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        audit_fundamental_quarter_refresh_decisions.main(
            [
                "--fundamentals-db",
                str(db_path),
                "--ohlcv-db",
                str(_ohlcv_db(tmp_path, ["AAPL"])),
                "--decision-date",
                "2026-08-07",
                "--output-root",
                str(tmp_path / "outside"),
            ]
        )


def _db(tmp_path: Path) -> Path:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    return db_path


def _decisions(db_path: Path) -> dict[str, object]:
    ohlcv_path = db_path.with_name("osakedata.db")
    _seed_ohlcv(ohlcv_path, _fundamental_tickers(db_path), latest_date="2026-08-07")
    with open_readonly_db(db_path) as conn:
        with open_readonly_db(ohlcv_path) as ohlcv_conn:
            return {
                row.ticker: row
                for row in build_quarter_refresh_decisions(
                    conn,
                    ohlcv_conn=ohlcv_conn,
                    decision_date="2026-08-07",
                    ohlcv_stale_days=14,
                )
            }


def _ohlcv_db(tmp_path: Path, tickers: list[str], *, latest_date: str = "2026-08-07") -> Path:
    path = tmp_path / "osakedata_cli.db"
    _seed_ohlcv(path, tickers, latest_date=latest_date)
    return path


def _seed_ohlcv(path: Path, tickers: list[str], *, latest_date: str) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS osakedata (
                id INTEGER PRIMARY KEY,
                osake TEXT,
                pvm TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                market TEXT NOT NULL DEFAULT 'usa',
                sector TEXT,
                industry TEXT
            )
            """
        )
        conn.execute("DELETE FROM osakedata")
        for ticker in tickers:
            conn.execute("INSERT INTO osakedata(osake, pvm, close, volume, market) VALUES (?, ?, 1, 100, 'usa')", (ticker, latest_date))


def _append_ohlcv(path: Path, ticker: str, *, latest_date: str) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute("INSERT INTO osakedata(osake, pvm, close, volume, market) VALUES (?, ?, 1, 100, 'usa')", (ticker, latest_date))


def _fundamental_tickers(db_path: Path) -> list[str]:
    with sqlite3.connect(str(db_path)) as conn:
        return [str(row[0]) for row in conn.execute("SELECT DISTINCT ticker FROM rc_fundamental_quarterly")]


def _seed_ticker(db_path: Path, ticker: str, *, calendar_status: str, estimated_date: str | None) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO rc_fundamental_quarterly(ticker, period_end_date, run_id) VALUES (?, '2026-03-31', 'TEST')",
            (ticker,),
        )
        conn.execute(
            """
            INSERT INTO rc_earnings_calendar (
                market, ticker, estimated_announcement_at, estimated_announcement_date, estimated_session,
                calendar_status, source, source_observed_at_utc, first_observed_at_utc, last_observed_at_utc,
                date_change_count, created_at_utc, updated_at_utc
            ) VALUES (
                'usa', ?, ?, ?, 'UNKNOWN', ?, 'YAHOO_FINANCE',
                '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z',
                0, '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z'
            )
            """,
            (ticker, estimated_date, estimated_date, calendar_status),
        )


def _seed_event(db_path: Path, ticker: str, event_id: int, announcement_date: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session,
                is_reported, reported_eps, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (?, 'usa', ?, ?, ?, 'UNKNOWN', 1, 1.23, 'YAHOO_FINANCE',
                '2026-08-07T00:00:00Z', 'America/New_York', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (event_id, ticker, f"{announcement_date}T16:00:00-04:00", announcement_date),
        )


def _seed_state(db_path: Path, ticker: str, *, detected_period: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_state (
                ticker, market, primary_source, latest_db_period_end_date, detected_source_period_end_date,
                new_quarter_available, last_updated_at_utc
            ) VALUES (?, 'usa', 'sec_edgar', '2026-03-31', ?, 1, '2026-08-07T00:00:00Z')
            """,
            (ticker, detected_period),
        )


def _seed_match(db_path: Path, ticker: str, *, event_id: int, period: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_earnings_match (
                market, ticker, period_end_date, earnings_event_id, announcement_at, announcement_date,
                announcement_session, effective_date_status, reporting_delay_days, matching_status,
                matching_confidence, matching_method, candidate_count, availability_policy, matcher_version,
                created_at_utc, updated_at_utc
            ) VALUES ('usa', ?, ?, ?, '2026-08-07T16:00:00-04:00', '2026-08-07',
                'UNKNOWN', 'RESOLVED', 1, 'MATCHED', 'HIGH', 'nearest', 1, 'event_effective_date',
                'test', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (ticker, period, event_id),
        )


def _seed_quarter(db_path: Path, ticker: str, period: str, *, complete: bool = True) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        if complete:
            conn.execute(
                """
                INSERT INTO rc_fundamental_quarterly (
                    ticker, period_end_date, revenue, ebit, free_cashflow, cash, total_debt,
                    shares_outstanding, run_id
                ) VALUES (?, ?, 100, 10, 5, 20, 2, 1000, 'TEST')
                """,
                (ticker, period),
            )
        else:
            conn.execute(
                "INSERT INTO rc_fundamental_quarterly(ticker, period_end_date, run_id) VALUES (?, ?, 'TEST')",
                (ticker, period),
            )


def _seed_ingestion_status(
    db_path: Path,
    ticker: str,
    period: str,
    *,
    quarter_basic: int,
    ttm: int = 0,
    score: int = 0,
    missing: str = "[]",
    ingestion_status: str = "UNKNOWN_HISTORICAL_INGEST_COMPLETENESS",
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_ingestion_status (
                market, ticker, period_end_date, ingestion_status, basic_status,
                quarter_basic_complete, ttm_input_complete, score_history_complete,
                valuation_input_ready, historical_research_ready, missing_basic_fields,
                missing_core_fields_json, missing_ttm_fields_json, missing_score_fields_json,
                data_quality_warnings_json, retry_recommendation, last_checked_at_utc,
                assessment_policy_version, ingestion_evidence_type, run_id, assessed_at_utc,
                created_at_utc, updated_at_utc
            ) VALUES (
                'usa', ?, ?, ?, 'TEST', ?, ?, ?, 0, 0, ?, '[]', '[]', '[]', '[]',
                'TEST', '2026-08-07T00:00:00Z', 'test', 'CURRENT_DB_STATE_ONLY',
                'TEST', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z'
            )
            """,
            (ticker, period, ingestion_status, quarter_basic, ttm, score, missing),
        )


def _counts(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(str(db_path)) as conn:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in (
                "rc_earnings_calendar",
                "rc_earnings_event",
                "rc_fundamental_quarterly",
                "rc_fundamental_quarter_ingestion_status",
                "rc_fundamental_quarter_state",
            )
        }
