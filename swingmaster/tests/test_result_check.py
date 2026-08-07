from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals import result_check


def _migrated_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    return db_path


def _ohlcv_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "osakedata.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE osakedata (market TEXT NOT NULL, osake TEXT NOT NULL, pvm TEXT NOT NULL)")
        conn.executemany(
            "INSERT INTO osakedata (market, osake, pvm) VALUES ('usa', ?, ?)",
            [("AAPL", "2026-08-07"), ("MSFT", "2026-08-06"), ("STALE", "2026-07-01")],
        )
        conn.commit()
    return db_path


def _insert_quarter(db_path: Path, ticker: str, period: str = "2026-03-31") -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker, period_end_date, revenue, ebit, free_cashflow, cash, total_debt, shares_outstanding, run_id
            ) VALUES (?, ?, 100, 10, 8, 20, 5, 10, 'FIXTURE')
            """,
            (ticker, period),
        )
        conn.commit()


def _insert_calendar(db_path: Path, ticker: str, status: str, estimated_date: str | None) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_calendar (
                market, ticker, estimated_announcement_at, estimated_announcement_date, estimated_session,
                calendar_status, source, source_observed_at_utc, first_observed_at_utc, last_observed_at_utc,
                created_at_utc, updated_at_utc
            ) VALUES ('usa', ?, ?, ?, 'AMC', ?, 'fixture', '2026-08-07T00:00:00Z',
                '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (ticker, f"{estimated_date}T20:00:00Z" if estimated_date else None, estimated_date, status),
        )
        conn.commit()


def _insert_event_and_match(db_path: Path, ticker: str, event_date: str, period: str, event_id: int = 1) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session, is_reported,
                reported_eps, estimated_eps, surprise_pct, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (?, 'usa', ?, ?, ?, 'AMC', 1, 1.0, 0.9, 10.0, 'fixture',
                '2026-08-07T00:00:00Z', 'UTC', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (event_id, ticker, f"{event_date}T20:00:00Z", event_date),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_earnings_match (
                market, ticker, period_end_date, earnings_event_id, announcement_at, announcement_date,
                announcement_session, effective_trading_date, effective_date_status, reporting_delay_days,
                matching_status, matching_confidence, matching_method, candidate_count, availability_policy,
                matcher_version, created_at_utc, updated_at_utc
            ) VALUES ('usa', ?, ?, ?, ?, ?, 'AMC', ?, 'SAME_DAY', 37, 'MATCHED', 'HIGH',
                'fixture', 1, 'fixture', 'fixture', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (ticker, period, event_id, f"{event_date}T20:00:00Z", event_date, event_date),
        )
        conn.commit()


def test_result_check_builds_executable_plan_after_completed_event(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_plan"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    _insert_event_and_match(fundamentals_db, "AAPL", "2026-08-07", "2026-06-30")

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers: {"stage": {"stage": "calendar_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", lambda root, db, tickers: {"stage": {"stage": "completed_event_refresh", "status": "SUCCESS"}, "summary": {"selected_tickers": len(tickers)}})
    monkeypatch.setattr(result_check, "_run_match_rebuild", lambda root, db, enabled: {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS", "enabled": enabled}, "summary": {}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "SUCCESS"
    assert result["plan"]["candidate_count"] == 1
    assert result["plan"]["candidates"][0]["ticker"] == "AAPL"
    assert result["plan"]["candidates"][0]["target_period_end_date"] == "2026-06-30"
    assert result_check.validate_candidate_hash(result["plan"])
    assert Path(result["artifact_paths"]["plan_json"]).exists()


def test_stale_ohlcv_suppresses_provider_candidate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_stale"
    _insert_quarter(fundamentals_db, "STALE")
    _insert_calendar(fundamentals_db, "STALE", "DUE_TODAY", "2026-08-07")
    _insert_event_and_match(fundamentals_db, "STALE", "2026-08-07", "2026-06-30")

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers: {"stage": {"stage": "calendar_refresh", "status": "SUCCESS", "selected_tickers": len(tickers)}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", lambda root, db, tickers: {"stage": {"stage": "completed_event_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_match_rebuild", lambda root, db, enabled: {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS"}, "summary": {}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["STALE"],
    )

    assert result["check_status"] == "SUCCESS"
    assert result["plan"]["candidate_count"] == 0
    assert result["plan"]["candidates"] == []


def test_completed_event_candidate_selection_uses_due_today_and_recent_passed(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    for ticker in ("AAPL", "MSFT", "OLD", "FUTURE"):
        _insert_quarter(fundamentals_db, ticker)
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    _insert_calendar(fundamentals_db, "MSFT", "DATE_PASSED_EVENT_NOT_FOUND", "2026-08-04")
    _insert_calendar(fundamentals_db, "OLD", "DATE_PASSED_EVENT_NOT_FOUND", "2026-07-01")
    _insert_calendar(fundamentals_db, "FUTURE", "UPCOMING", "2026-08-20")

    selected = result_check.select_completed_event_refresh_candidates(
        fundamentals_db=fundamentals_db,
        tickers=["AAPL", "MSFT", "OLD", "FUTURE"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )

    assert selected == ["AAPL", "MSFT"]


def test_partial_event_refresh_disables_executable_plan(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_partial"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    _insert_event_and_match(fundamentals_db, "AAPL", "2026-08-07", "2026-06-30")

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers: {"stage": {"stage": "calendar_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", lambda root, db, tickers: {"stage": {"stage": "completed_event_refresh", "status": "PARTIAL"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_match_rebuild", lambda root, db, enabled: {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS", "enabled": enabled}, "summary": {}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "PARTIAL"
    assert result["plan"]["candidate_count"] == 0


def test_ambiguous_target_period_is_manual_review_not_executable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_ambiguous"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    with sqlite3.connect(str(fundamentals_db)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                market, ticker, announcement_at, announcement_date, announcement_session, is_reported,
                reported_eps, estimated_eps, surprise_pct, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES ('usa', 'AAPL', '2026-08-07T20:00:00Z', '2026-08-07', 'AMC', 1,
                1.0, 0.9, 10.0, 'fixture', '2026-08-07T00:00:00Z', 'UTC',
                '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """
        )
        conn.commit()

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers: {"stage": {"stage": "calendar_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", lambda root, db, tickers: {"stage": {"stage": "completed_event_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_match_rebuild", lambda root, db, enabled: {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS"}, "summary": {}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "SUCCESS"
    assert result["plan"]["candidate_count"] == 0
    manual_review = Path(result["artifact_paths"]["manual_review_csv"]).read_text(encoding="utf-8")
    assert "REVIEW_AMBIGUOUS_PERIOD" in manual_review


def test_failed_calendar_refresh_writes_failed_empty_plan(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_failed"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers: {"stage": {"stage": "calendar_refresh", "status": "FAILED"}, "summary": {"error": "boom"}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "FAILED"
    assert result["plan"]["candidate_count"] == 0
    assert json.loads(Path(result["artifact_paths"]["plan_json"]).read_text(encoding="utf-8"))["check_status"] == "FAILED"


def test_output_root_must_be_under_temp(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        result_check.run_manual_result_check(
            fundamentals_db=fundamentals_db,
            ohlcv_db=tmp_path / "missing_osakedata.db",
            decision_date="2026-08-07",
            output_root=tmp_path / "outside_temp",
            tickers=["AAPL"],
        )
