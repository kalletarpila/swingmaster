from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_state
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_quarterly_row(db_path: Path, ticker: str, period_end_date: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                run_id
            ) VALUES (?, ?, ?)
            """,
            (ticker, period_end_date, "FIXTURE"),
        )
        conn.commit()


def test_sync_from_quarterly_creates_state_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_state_sync.db"
    run_migration(db_path)
    _insert_quarterly_row(db_path, "AAPL", "2025-12-28")
    _insert_quarterly_row(db_path, "AAPL", "2026-03-29")
    _insert_quarterly_row(db_path, "NOKIA.HE", "2025-12-31")

    with sqlite3.connect(str(db_path)) as conn:
        rows = run_fundamental_quarter_state.load_latest_quarter_rows(conn, None)
        rows_updated = run_fundamental_quarter_state.upsert_state_from_quarterly(
            conn,
            rows,
            run_id="SYNC1",
            updated_at_utc="2026-05-05T00:00:00+00:00",
        )
        conn.commit()

    assert rows_updated == 2
    with sqlite3.connect(str(db_path)) as conn:
        state_rows = conn.execute(
            """
            SELECT ticker, market, primary_source, latest_db_period_end_date, new_quarter_available, last_ingest_run_id
            FROM rc_fundamental_quarter_state
            ORDER BY ticker
            """
        ).fetchall()
    assert state_rows == [
        ("AAPL", "usa", "sec_edgar", "2026-03-29", 0, "SYNC1"),
        ("NOKIA.HE", "omxh", "yahoo", "2025-12-31", 0, "SYNC1"),
    ]


def test_mark_detected_period_sets_flag(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_state_detect.db"
    run_migration(db_path)
    _insert_quarterly_row(db_path, "AAPL", "2025-12-28")

    with sqlite3.connect(str(db_path)) as conn:
        rows = run_fundamental_quarter_state.load_latest_quarter_rows(conn, "AAPL")
        run_fundamental_quarter_state.upsert_state_from_quarterly(
            conn,
            rows,
            run_id="SYNC1",
            updated_at_utc="2026-05-05T00:00:00+00:00",
        )
        rows_updated = run_fundamental_quarter_state.mark_detected_period(
            conn,
            ticker="AAPL",
            market=None,
            detected_period_end_date="2026-03-29",
            run_id="DETECT1",
            updated_at_utc="2026-05-05T01:00:00+00:00",
        )
        conn.commit()

    assert rows_updated == 1
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT ticker, latest_db_period_end_date, detected_source_period_end_date, new_quarter_available, last_detection_run_id
            FROM rc_fundamental_quarter_state
            WHERE ticker='AAPL'
            """
        ).fetchone()
    assert row == ("AAPL", "2025-12-28", "2026-03-29", 1, "DETECT1")


def test_mark_detected_period_uses_explicit_market_override(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_state_market_override.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        rows_updated = run_fundamental_quarter_state.mark_detected_period(
            conn,
            ticker="nok",
            market="omxh",
            detected_period_end_date="2026-03-31",
            run_id="DETECT_MARKET",
            updated_at_utc="2026-05-05T01:00:00+00:00",
        )
        conn.commit()

    assert rows_updated == 1
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT ticker, market, primary_source, detected_source_period_end_date, new_quarter_available
            FROM rc_fundamental_quarter_state
            WHERE ticker='NOK'
            """
        ).fetchone()
    assert row == ("NOK", "omxh", "yahoo", "2026-03-31", 1)


def test_acknowledge_ingested_updates_latest_date_and_clears_flag(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_state_ack.db"
    run_migration(db_path)
    _insert_quarterly_row(db_path, "AAPL", "2025-12-28")

    with sqlite3.connect(str(db_path)) as conn:
        rows = run_fundamental_quarter_state.load_latest_quarter_rows(conn, "AAPL")
        run_fundamental_quarter_state.upsert_state_from_quarterly(
            conn,
            rows,
            run_id="SYNC1",
            updated_at_utc="2026-05-05T00:00:00+00:00",
        )
        run_fundamental_quarter_state.mark_detected_period(
            conn,
            ticker="AAPL",
            market=None,
            detected_period_end_date="2026-03-29",
            run_id="DETECT1",
            updated_at_utc="2026-05-05T01:00:00+00:00",
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                run_id
            ) VALUES (?, ?, ?)
            """,
            ("AAPL", "2026-03-29", "INGEST1"),
        )
        rows = run_fundamental_quarter_state.load_latest_quarter_rows(conn, "AAPL")
        rows_updated = run_fundamental_quarter_state.acknowledge_ingested(
            conn,
            rows,
            run_id="INGEST1",
            updated_at_utc="2026-05-05T02:00:00+00:00",
        )
        conn.commit()

    assert rows_updated == 1
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT ticker, latest_db_period_end_date, detected_source_period_end_date, new_quarter_available, last_ingest_run_id
            FROM rc_fundamental_quarter_state
            WHERE ticker='AAPL'
            """
        ).fetchone()
    assert row == ("AAPL", "2026-03-29", None, 0, "INGEST1")


def test_main_requires_ticker_for_mark_detected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_state_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_quarter_state,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            run_id="RUN1",
            ticker=None,
            market=None,
            sync_from_quarterly=False,
            mark_detected_period="2026-03-31",
            acknowledge_ingested=False,
        ),
    )

    with pytest.raises(SystemExit, match="FUNDAMENTAL_QUARTER_STATE_TICKER_REQUIRED_FOR_MARK_DETECTED"):
        run_fundamental_quarter_state.main()
