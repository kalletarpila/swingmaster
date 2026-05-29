from __future__ import annotations

import csv
import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_missing_period_recovery_check
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_classification_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    market: str,
    missing_period_end_dates: str,
    run_id: str = "RF_RUN_1",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_reporting_frequency_classification (
            ticker,
            market,
            as_of_date,
            lookback_months,
            reporting_frequency_class,
            inferred_reporting_frequency,
            has_valid_ttm_coverage,
            reason,
            period_count_in_lookback,
            observed_period_end_dates,
            expected_period_end_dates,
            missing_period_end_dates,
            missing_period_count,
            source_data_max_period_end_date,
            classifier_version,
            run_id,
            created_at_utc
        ) VALUES (?, ?, '2026-05-29', 30, 'QUARTERLY_MISSING_SOURCE_PERIOD', 'INSUFFICIENT', 0, 'QUARTERLY_PATTERN_WITH_MISSING_RECENT_PERIOD', 5, '2024-12-31,2025-03-31,2025-06-30,2025-12-31,2026-03-31', '2024-12-31,2025-03-31,2025-06-30,2025-09-30,2025-12-31,2026-03-31', ?, 1, '2026-03-31', 'reporting_frequency_v1', ?, '2026-05-29T00:00:00+00:00')
        """,
        (ticker, market, missing_period_end_dates, run_id),
    )


def test_missing_period_found_in_yahoo(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "recovery_found.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_classification_row(conn, ticker="TIETO.HE", market="omxh", missing_period_end_dates="2025-09-30")
        conn.commit()

    class _FakeYahooFinanceClient:
        def get_raw_payload(self, symbol: str) -> dict:
            assert symbol == "TIETO.HE"
            return {
                "quarterly_income_stmt": {
                    "index": ["Total Revenue"],
                    "columns": ["2025-06-30", "2025-09-30", "2025-12-31"],
                    "data": [[10.0, 11.0, 12.0]],
                },
                "quarterly_balance_sheet": {"index": [], "columns": [], "data": []},
                "quarterly_cashflow": {"index": [], "columns": [], "data": []},
            }

    monkeypatch.setattr(
        run_fundamental_missing_period_recovery_check,
        "YahooFinanceClient",
        lambda: _FakeYahooFinanceClient(),
    )

    rows, summary = run_fundamental_missing_period_recovery_check.build_recovery_rows(
        db_path=db_path,
        market="omxh",
        classification_run_id="RF_RUN_1",
        limit=None,
    )

    assert rows == [
        {
            "ticker": "TIETO.HE",
            "market": "omxh",
            "classification_run_id": "RF_RUN_1",
            "classification_as_of_date": "2026-05-29",
            "missing_period_end_date": "2025-09-30",
            "recovery_status": "FOUND_IN_YAHOO",
            "has_core_fields": 1,
            "found_period_end_dates": "2025-06-30,2025-09-30,2025-12-31",
            "reason": "MISSING_PERIOD_FOUND_IN_YAHOO",
            "checked_at_utc": rows[0]["checked_at_utc"],
        }
    ]
    assert summary["classification_rows_checked"] == 1
    assert summary["missing_periods_checked"] == 1
    assert summary["found_in_yahoo_count"] == 1


def test_missing_period_still_missing_in_yahoo(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "recovery_missing.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_classification_row(conn, ticker="TIETO.HE", market="omxh", missing_period_end_dates="2025-09-30")
        conn.commit()

    class _FakeYahooFinanceClient:
        def get_raw_payload(self, symbol: str) -> dict:
            return {
                "quarterly_income_stmt": {
                    "index": ["Total Revenue"],
                    "columns": ["2025-06-30", "2025-12-31"],
                    "data": [[10.0, 12.0]],
                },
                "quarterly_balance_sheet": {"index": [], "columns": [], "data": []},
                "quarterly_cashflow": {"index": [], "columns": [], "data": []},
            }

    monkeypatch.setattr(
        run_fundamental_missing_period_recovery_check,
        "YahooFinanceClient",
        lambda: _FakeYahooFinanceClient(),
    )

    rows, summary = run_fundamental_missing_period_recovery_check.build_recovery_rows(
        db_path=db_path,
        market="omxh",
        classification_run_id="RF_RUN_1",
        limit=None,
    )

    assert rows[0]["recovery_status"] == "STILL_MISSING"
    assert rows[0]["has_core_fields"] == 0
    assert rows[0]["reason"] == "MISSING_PERIOD_STILL_MISSING"
    assert summary["still_missing_count"] == 1


def test_yahoo_fetch_failure(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "recovery_fetch_failed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_classification_row(conn, ticker="TIETO.HE", market="omxh", missing_period_end_dates="2025-09-30")
        conn.commit()

    class _FakeYahooFinanceClient:
        def get_raw_payload(self, symbol: str) -> dict:
            raise RuntimeError(f"FETCH_FAILED:{symbol}")

    monkeypatch.setattr(
        run_fundamental_missing_period_recovery_check,
        "YahooFinanceClient",
        lambda: _FakeYahooFinanceClient(),
    )

    rows, summary = run_fundamental_missing_period_recovery_check.build_recovery_rows(
        db_path=db_path,
        market="omxh",
        classification_run_id="RF_RUN_1",
        limit=None,
    )

    assert rows[0]["recovery_status"] == "FETCH_FAILED"
    assert rows[0]["reason"] == "YAHOO_FETCH_FAILED"
    assert summary["fetch_failed_count"] == 1


def test_malformed_missing_periods_are_reported_deterministically(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "recovery_no_missing.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_classification_row(conn, ticker="TIETO.HE", market="omxh", missing_period_end_dates="bad-date")
        conn.commit()

    class _FakeYahooFinanceClient:
        def get_raw_payload(self, symbol: str) -> dict:
            raise AssertionError("Yahoo should not be called when missing periods are not parseable")

    monkeypatch.setattr(
        run_fundamental_missing_period_recovery_check,
        "YahooFinanceClient",
        lambda: _FakeYahooFinanceClient(),
    )

    rows, summary = run_fundamental_missing_period_recovery_check.build_recovery_rows(
        db_path=db_path,
        market="omxh",
        classification_run_id="RF_RUN_1",
        limit=None,
    )

    assert rows[0]["recovery_status"] == "NO_MISSING_PERIODS"
    assert rows[0]["reason"] == "NO_PARSEABLE_MISSING_PERIODS"
    assert summary["missing_periods_checked"] == 0
    assert summary["no_missing_periods_count"] == 1


def test_cli_csv_output_contains_required_fields_and_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "recovery_cli_csv.db"
    run_migration(db_path)
    output_path = tmp_path / "recovery.csv"
    monkeypatch.setattr(
        run_fundamental_missing_period_recovery_check,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="omxh",
            classification_run_id="RF_RUN_1",
            output=str(output_path),
            format="csv",
            limit=5,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_missing_period_recovery_check,
        "build_recovery_rows",
        lambda **kwargs: (
            [
                {
                    "ticker": "TIETO.HE",
                    "market": "omxh",
                    "classification_run_id": "RF_RUN_1",
                    "classification_as_of_date": "2026-05-29",
                    "missing_period_end_date": "2025-09-30",
                    "recovery_status": "FOUND_IN_YAHOO",
                    "has_core_fields": 1,
                    "found_period_end_dates": "2025-06-30,2025-09-30,2025-12-31",
                    "reason": "MISSING_PERIOD_FOUND_IN_YAHOO",
                    "checked_at_utc": "2026-05-29T12:00:00+00:00",
                }
            ],
            {
                "market": "omxh",
                "classification_run_id": "RF_RUN_1",
                "classification_rows_checked": 1,
                "missing_periods_checked": 1,
                "found_in_yahoo_count": 1,
                "found_in_yahoo_incomplete_count": 0,
                "still_missing_count": 0,
                "fetch_failed_count": 0,
                "parse_failed_count": 0,
                "no_missing_periods_count": 0,
            },
        ),
    )

    run_fundamental_missing_period_recovery_check.main()
    out = capsys.readouterr().out

    with output_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "ticker": "TIETO.HE",
            "market": "omxh",
            "classification_run_id": "RF_RUN_1",
            "classification_as_of_date": "2026-05-29",
            "missing_period_end_date": "2025-09-30",
            "recovery_status": "FOUND_IN_YAHOO",
            "has_core_fields": "1",
            "found_period_end_dates": "2025-06-30,2025-09-30,2025-12-31",
            "reason": "MISSING_PERIOD_FOUND_IN_YAHOO",
            "checked_at_utc": "2026-05-29T12:00:00+00:00",
        }
    ]
    assert "SUMMARY market=omxh" in out
    assert "SUMMARY classification_run_id=RF_RUN_1" in out
    assert "SUMMARY found_in_yahoo_count=1" in out
    assert "SUMMARY output_path=" in out
    assert "SUMMARY limit=5" in out


def test_limit_applies_to_classification_rows_checked(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "recovery_limit.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_classification_row(conn, ticker="A.HE", market="omxh", missing_period_end_dates="2025-09-30")
        _insert_classification_row(conn, ticker="B.HE", market="omxh", missing_period_end_dates="2025-09-30")
        conn.commit()

    class _FakeYahooFinanceClient:
        def get_raw_payload(self, symbol: str) -> dict:
            return {
                "quarterly_income_stmt": {
                    "index": ["Total Revenue"],
                    "columns": ["2025-06-30"],
                    "data": [[10.0]],
                },
                "quarterly_balance_sheet": {"index": [], "columns": [], "data": []},
                "quarterly_cashflow": {"index": [], "columns": [], "data": []},
            }

    monkeypatch.setattr(
        run_fundamental_missing_period_recovery_check,
        "YahooFinanceClient",
        lambda: _FakeYahooFinanceClient(),
    )

    rows, summary = run_fundamental_missing_period_recovery_check.build_recovery_rows(
        db_path=db_path,
        market="omxh",
        classification_run_id="RF_RUN_1",
        limit=1,
    )

    assert len(rows) == 1
    assert rows[0]["ticker"] == "A.HE"
    assert summary["classification_rows_checked"] == 1
