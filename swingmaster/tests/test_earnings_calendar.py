from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_calendar import (
    EarningsCalendarEstimate,
    new_york_today_from_utc,
    select_future_yahoo_estimate,
    upsert_earnings_calendar,
)


def test_calendar_upsert_new_same_changed_due_and_passed(tmp_path: Path) -> None:
    db_path = tmp_path / "calendar.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        status = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="aapl",
            estimate=EarningsCalendarEstimate("AAPL", "2026-08-10 16:00:00", "2026-08-10", "AFTER_MARKET"),
            observed_at_utc="2026-08-06T12:00:00Z",
            today_new_york="2026-08-06",
        )
        assert status == "UPCOMING"

        same_status = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="AAPL",
            estimate=EarningsCalendarEstimate("AAPL", "2026-08-10 16:00:00", "2026-08-10", "AFTER_MARKET"),
            observed_at_utc="2026-08-06T13:00:00Z",
            today_new_york="2026-08-06",
        )
        assert same_status == "UPCOMING"
        assert conn.execute("SELECT date_change_count FROM rc_earnings_calendar").fetchone()[0] == 0

        changed_status = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="AAPL",
            estimate=EarningsCalendarEstimate("AAPL", "2026-08-11 16:00:00", "2026-08-11", "AFTER_MARKET"),
            observed_at_utc="2026-08-06T14:00:00Z",
            today_new_york="2026-08-06",
        )
        assert changed_status == "UPCOMING"
        previous, changes = conn.execute(
            "SELECT previous_estimated_announcement_at, date_change_count FROM rc_earnings_calendar"
        ).fetchone()
        assert previous == "2026-08-10 16:00:00"
        assert changes == 1

        due_status = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="AAPL",
            estimate=EarningsCalendarEstimate("AAPL", "2026-08-06 09:00:00", "2026-08-06", "BEFORE_MARKET"),
            observed_at_utc="2026-08-06T15:00:00Z",
            today_new_york="2026-08-06",
        )
        assert due_status == "DUE_TODAY"

        passed_status = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="AAPL",
            estimate=EarningsCalendarEstimate("AAPL", "2026-08-05 09:00:00", "2026-08-05", "BEFORE_MARKET"),
            observed_at_utc="2026-08-06T16:00:00Z",
            today_new_york="2026-08-06",
        )
        assert passed_status == "DATE_PASSED_EVENT_NOT_FOUND"


def test_calendar_completed_event_and_no_estimate(tmp_path: Path) -> None:
    db_path = tmp_path / "calendar_completed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        no_estimate = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="MSFT",
            estimate=None,
            observed_at_utc="2026-08-06T12:00:00Z",
            today_new_york="2026-08-06",
        )
        assert no_estimate == "NO_CURRENT_ESTIMATE"
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session,
                is_reported, reported_eps, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (
                42, 'usa', 'MSFT', '2026-08-05T16:05:00-04:00', '2026-08-05', 'AFTER_MARKET',
                1, 2.50, 'YAHOO_FINANCE', '2026-08-05T21:00:00Z', 'America/New_York',
                '2026-08-05T21:00:00Z', '2026-08-05T21:00:00Z'
            )
            """
        )
        completed = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="MSFT",
            estimate=EarningsCalendarEstimate("MSFT", "2026-08-10 16:00:00", "2026-08-10", "AFTER_MARKET"),
            observed_at_utc="2026-08-06T13:00:00Z",
            today_new_york="2026-08-06",
        )
        assert completed == "COMPLETED_EVENT_FOUND"
        assert conn.execute("SELECT completed_earnings_event_id FROM rc_earnings_calendar").fetchone()[0] == 42


def test_yahoo_parser_ignores_completed_rows_and_uses_new_york_date() -> None:
    estimate = select_future_yahoo_estimate(
        [
            {"Earnings Date": "2026-08-05 16:00:00", "Reported EPS": None},
            {"Earnings Date": "2026-08-07 09:00:00 BMO", "Reported EPS": None},
            {"Earnings Date": "2026-08-06 16:00:00 AMC", "Reported EPS": 1.23},
        ],
        today_new_york="2026-08-06",
        ticker="AAPL",
    )
    assert estimate is not None
    assert estimate.estimated_announcement_date == "2026-08-07"
    assert estimate.estimated_session == "BEFORE_MARKET"
    assert new_york_today_from_utc(datetime(2026, 8, 6, 3, 0, tzinfo=timezone.utc)) == "2026-08-05"
