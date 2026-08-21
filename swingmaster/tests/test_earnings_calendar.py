from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import json

import pytest

from swingmaster.cli import refresh_yahoo_earnings_calendar
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
        assert completed == "UPCOMING"
        status, completed_id = conn.execute("SELECT calendar_status, completed_earnings_event_id FROM rc_earnings_calendar").fetchone()
        assert status == "UPCOMING"
        assert completed_id == 42


def test_completed_reconciliation_without_next_estimate_is_no_current_estimate(tmp_path: Path) -> None:
    db_path = tmp_path / "calendar_completed_no_next.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
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
        status = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="MSFT",
            estimate=None,
            observed_at_utc="2026-08-06T13:00:00Z",
            today_new_york="2026-08-06",
        )
        row = conn.execute("SELECT calendar_status, estimated_announcement_date, completed_earnings_event_id FROM rc_earnings_calendar").fetchone()
        assert status == "NO_CURRENT_ESTIMATE"
        assert row == ("NO_CURRENT_ESTIMATE", None, 42)


def test_completed_reconciliation_does_not_suppress_due_today(tmp_path: Path) -> None:
    db_path = tmp_path / "calendar_completed_due_today.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session,
                is_reported, reported_eps, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (
                42, 'usa', 'AAPL', '2026-05-01T16:05:00-04:00', '2026-05-01', 'AFTER_MARKET',
                1, 1.50, 'YAHOO_FINANCE', '2026-05-01T21:00:00Z', 'America/New_York',
                '2026-05-01T21:00:00Z', '2026-05-01T21:00:00Z'
            )
            """
        )
        status = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="AAPL",
            estimate=EarningsCalendarEstimate("AAPL", "2026-08-06 16:00:00", "2026-08-06", "AFTER_MARKET"),
            observed_at_utc="2026-08-06T13:00:00Z",
            today_new_york="2026-08-06",
        )
        row = conn.execute("SELECT calendar_status, estimated_announcement_date, completed_earnings_event_id FROM rc_earnings_calendar").fetchone()
        assert status == "DUE_TODAY"
        assert row == ("DUE_TODAY", "2026-08-06", 42)


def test_passed_estimate_with_completed_event_is_no_current_estimate(tmp_path: Path) -> None:
    db_path = tmp_path / "calendar_past_completed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session,
                is_reported, reported_eps, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (
                42, 'usa', 'AAPL', '2026-08-05T16:05:00-04:00', '2026-08-05', 'AFTER_MARKET',
                1, 1.50, 'YAHOO_FINANCE', '2026-08-05T21:00:00Z', 'America/New_York',
                '2026-08-05T21:00:00Z', '2026-08-05T21:00:00Z'
            )
            """
        )
        status = upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="AAPL",
            estimate=EarningsCalendarEstimate("AAPL", "2026-08-05 16:00:00", "2026-08-05", "AFTER_MARKET"),
            observed_at_utc="2026-08-06T13:00:00Z",
            today_new_york="2026-08-06",
        )
        assert status == "NO_CURRENT_ESTIMATE"


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


def test_yahoo_parser_selects_nearest_future_from_larger_result_set() -> None:
    rows = [
        {"Earnings Date": "2026-07-01 16:00:00", "Reported EPS": 1.23},
        {"Earnings Date": "2026-11-01 16:00:00", "Reported EPS": None},
        {"Earnings Date": "2026-08-10 08:00:00 BMO", "Reported EPS": None},
        {"Earnings Date": "2026-08-06 16:00:00 AMC", "Reported EPS": 1.30},
        {"Earnings Date": "2026-09-15 16:00:00", "Reported EPS": None},
    ]
    estimate = select_future_yahoo_estimate(rows, today_new_york="2026-08-06", ticker="AAPL")
    assert estimate is not None
    assert estimate.estimated_announcement_at == "2026-08-10 08:00:00 BMO"
    assert estimate.estimated_session == "BEFORE_MARKET"


def test_calendar_refresh_future_wins_over_historical_completed_event(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "future_wins.db"
    _seed_calendar_db(db_path, ["AAPL"])
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session,
                is_reported, reported_eps, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (
                7, 'usa', 'AAPL', '2026-05-01T16:05:00-04:00', '2026-05-01', 'AFTER_MARKET',
                1, 1.50, 'YAHOO_FINANCE', '2026-05-01T21:00:00Z', 'America/New_York',
                '2026-05-01T21:00:00Z', '2026-05-01T21:00:00Z'
            )
            """
        )

    def fake_fetch(_ticker: str, **_kwargs: object) -> list[dict[str, object]]:
        return [
            {"Earnings Date": "2026-05-01 16:00:00", "Reported EPS": 1.50},
            {"Earnings Date": "2026-08-10 16:00:00", "Reported EPS": None},
        ]

    monkeypatch.setattr(refresh_yahoo_earnings_calendar, "fetch_yahoo_earnings_calendar_rows", fake_fetch)
    monkeypatch.setattr(refresh_yahoo_earnings_calendar, "new_york_today_from_utc", lambda *_args, **_kwargs: "2026-08-06")
    monkeypatch.setattr(refresh_yahoo_earnings_calendar.time, "sleep", lambda _seconds: None)
    root = _temp_root("future_wins")
    assert (
        refresh_yahoo_earnings_calendar.main(
            [
                "--fundamentals-db",
                str(db_path),
                "--apply",
                "--output-root",
                str(root),
                "--sleep-min-seconds",
                "0",
                "--sleep-max-seconds",
                "0",
            ]
        )
        == 0
    )
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute("SELECT calendar_status, estimated_announcement_date, completed_earnings_event_id FROM rc_earnings_calendar").fetchone()
    assert row == ("UPCOMING", "2026-08-10", 7)


def test_calendar_refresh_completed_only_response(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "completed_only.db"
    _seed_calendar_db(db_path, ["AAPL"])

    def fake_fetch(_ticker: str, **_kwargs: object) -> list[dict[str, object]]:
        return [{"Earnings Date": "2026-05-01 16:00:00", "Reported EPS": 1.50}]

    monkeypatch.setattr(refresh_yahoo_earnings_calendar, "fetch_yahoo_earnings_calendar_rows", fake_fetch)
    monkeypatch.setattr(refresh_yahoo_earnings_calendar.time, "sleep", lambda _seconds: None)
    root = _temp_root("completed_only")
    assert (
        refresh_yahoo_earnings_calendar.main(
            [
                "--fundamentals-db",
                str(db_path),
                "--apply",
                "--output-root",
                str(root),
                "--sleep-min-seconds",
                "0",
                "--sleep-max-seconds",
                "0",
            ]
        )
        == 0
    )
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT calendar_status, estimated_announcement_date FROM rc_earnings_calendar").fetchone() == ("NO_CURRENT_ESTIMATE", None)


def test_calendar_refresh_future_only_response(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "future_only.db"
    _seed_calendar_db(db_path, ["AAPL"])

    def fake_fetch(_ticker: str, **_kwargs: object) -> list[dict[str, object]]:
        return [{"Earnings Date": "2026-08-10 16:00:00", "Reported EPS": None}]

    monkeypatch.setattr(refresh_yahoo_earnings_calendar, "fetch_yahoo_earnings_calendar_rows", fake_fetch)
    monkeypatch.setattr(refresh_yahoo_earnings_calendar, "new_york_today_from_utc", lambda *_args, **_kwargs: "2026-08-06")
    monkeypatch.setattr(refresh_yahoo_earnings_calendar.time, "sleep", lambda _seconds: None)
    root = _temp_root("future_only")
    assert (
        refresh_yahoo_earnings_calendar.main(
            [
                "--fundamentals-db",
                str(db_path),
                "--apply",
                "--output-root",
                str(root),
                "--sleep-min-seconds",
                "0",
                "--sleep-max-seconds",
                "0",
            ]
        )
        == 0
    )
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT calendar_status, estimated_announcement_date FROM rc_earnings_calendar").fetchone() == ("UPCOMING", "2026-08-10")


def test_calendar_refresh_timeout_then_success_and_resume(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "refresh.db"
    _seed_calendar_db(db_path, ["AAPL", "MSFT"])
    root = _temp_root("timeout_resume")
    calls: list[str] = []

    def fake_fetch(ticker: str, **_kwargs: object) -> list[dict[str, object]]:
        calls.append(ticker)
        if ticker == "AAPL" and calls.count("AAPL") == 1:
            raise TimeoutError("timed out")
        return [{"Earnings Date": "2026-08-10 16:00:00", "Reported EPS": None}]

    monkeypatch.setattr(refresh_yahoo_earnings_calendar, "fetch_yahoo_earnings_calendar_rows", fake_fetch)
    monkeypatch.setattr(refresh_yahoo_earnings_calendar.time, "sleep", lambda _seconds: None)
    assert (
        refresh_yahoo_earnings_calendar.main(
            [
                "--fundamentals-db",
                str(db_path),
                "--dry-run",
                "--output-root",
                str(root),
                "--max-retries",
                "3",
                "--sleep-min-seconds",
                "0",
                "--sleep-max-seconds",
                "0",
            ]
        )
        == 0
    )
    checkpoint = json.loads((root / "calendar_checkpoint.json").read_text(encoding="utf-8"))
    assert [row["result_status"] for row in checkpoint["attempt_rows"][:2]] == ["TIMEOUT", "SUCCESS"]

    second_root = _temp_root("timeout_resume_second")
    assert (
        refresh_yahoo_earnings_calendar.main(
            [
                "--fundamentals-db",
                str(db_path),
                "--dry-run",
                "--output-root",
                str(second_root),
                "--resume-from-json",
                str(root / "calendar_checkpoint.json"),
                "--sleep-min-seconds",
                "0",
                "--sleep-max-seconds",
                "0",
            ]
        )
        == 0
    )
    assert calls.count("AAPL") == 2


def test_calendar_refresh_failures_do_not_overwrite_existing_row(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "preserve.db"
    _seed_calendar_db(db_path, ["AAPL"])
    with sqlite3.connect(str(db_path)) as conn:
        upsert_earnings_calendar(
            conn,
            market="usa",
            ticker="AAPL",
            estimate=EarningsCalendarEstimate("AAPL", "2026-08-10", "2026-08-10", "UNKNOWN"),
            observed_at_utc="2026-08-06T00:00:00Z",
            today_new_york="2026-08-06",
        )
        before = conn.execute("SELECT calendar_status, estimated_announcement_date FROM rc_earnings_calendar").fetchone()

    def fake_fetch(_ticker: str, **_kwargs: object) -> list[dict[str, object]]:
        raise ConnectionError("network unreachable")

    monkeypatch.setattr(refresh_yahoo_earnings_calendar, "fetch_yahoo_earnings_calendar_rows", fake_fetch)
    monkeypatch.setattr(refresh_yahoo_earnings_calendar.time, "sleep", lambda _seconds: None)
    root = _temp_root("preserve_failure")
    exit_code = refresh_yahoo_earnings_calendar.main(
        [
            "--fundamentals-db",
            str(db_path),
            "--apply",
            "--output-root",
            str(root),
            "--max-retries",
            "1",
            "--sleep-min-seconds",
            "0",
            "--sleep-max-seconds",
            "0",
            "--stop-after-consecutive-failures",
            "10",
        ]
    )
    assert exit_code == 0
    with sqlite3.connect(str(db_path)) as conn:
        after = conn.execute("SELECT calendar_status, estimated_announcement_date FROM rc_earnings_calendar").fetchone()
    assert after == before
    attempts = (root / "calendar_attempts.csv").read_text(encoding="utf-8")
    assert "NETWORK_ERROR" in attempts


def test_calendar_refresh_repeated_timeout_rate_limit_parse_and_interrupt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    assert refresh_yahoo_earnings_calendar._classify_exception(TimeoutError("timeout")) == "TIMEOUT"
    assert refresh_yahoo_earnings_calendar._classify_exception(RuntimeError("429 too many requests")) == "RATE_LIMITED"
    assert refresh_yahoo_earnings_calendar._classify_exception(ConnectionError("DNS resolve failed")) == "NETWORK_ERROR"
    assert refresh_yahoo_earnings_calendar._classify_exception(ValueError("parse failed")) == "PARSE_ERROR"

    db_path = tmp_path / "interrupt.db"
    _seed_calendar_db(db_path, ["AAPL"])

    def interrupted(_ticker: str, **_kwargs: object) -> list[dict[str, object]]:
        raise KeyboardInterrupt

    monkeypatch.setattr(refresh_yahoo_earnings_calendar, "fetch_yahoo_earnings_calendar_rows", interrupted)
    root = _temp_root("interrupt")
    assert (
        refresh_yahoo_earnings_calendar.main(
            [
                "--fundamentals-db",
                str(db_path),
                "--dry-run",
                "--output-root",
                str(root),
                "--sleep-min-seconds",
                "0",
                "--sleep-max-seconds",
                "0",
            ]
        )
        == 130
    )
    payload = json.loads((root / "calendar_checkpoint.json").read_text(encoding="utf-8"))
    assert payload["attempt_rows"][-1]["result_status"] == "INTERRUPTED"


def _seed_calendar_db(db_path: Path, tickers: list[str]) -> None:
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        for ticker in tickers:
            conn.execute(
                "INSERT INTO rc_fundamental_quarterly(ticker, period_end_date, run_id) VALUES (?, '2026-06-30', 'TEST')",
                (ticker,),
            )


def _temp_root(name: str) -> Path:
    root = Path("/home/kalle/projects/swingmaster/temp/yahoo_earnings_calendar_reliability/tests") / name
    root.mkdir(parents=True, exist_ok=True)
    return root
