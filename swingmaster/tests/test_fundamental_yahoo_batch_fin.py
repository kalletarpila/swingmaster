from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli.run_fundamental_yahoo_batch_fin import (
    load_omxh_ticker_universe,
    resolve_failure_log_path,
    run_yahoo_batch_fin,
)


def test_load_omxh_ticker_universe_is_distinct_and_sorted(tmp_path: Path) -> None:
    osakedata_db_path = tmp_path / "osakedata.db"
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        _insert_osakedata_row(conn, "NOKIA.HE", "2026-01-01", "omxh")
        _insert_osakedata_row(conn, "KNEBV.HE", "2026-01-01", "omxh")
        _insert_osakedata_row(conn, "NOKIA.HE", "2026-01-02", "omxh")
        _insert_osakedata_row(conn, "ERICA.ST", "2026-01-01", "omxs")

    assert load_omxh_ticker_universe(osakedata_db_path) == [
        "KNEBV.HE",
        "NOKIA.HE",
    ]


def test_run_yahoo_batch_fin_applies_limit_in_sorted_order(monkeypatch, tmp_path: Path) -> None:
    osakedata_db_path = tmp_path / "osakedata_limit.db"
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        _insert_osakedata_row(conn, "NOKIA.HE", "2026-01-01", "omxh")
        _insert_osakedata_row(conn, "KNEBV.HE", "2026-01-01", "omxh")
        _insert_osakedata_row(conn, "UPM.HE", "2026-01-01", "omxh")

    processed_symbols: list[str] = []

    def _process_symbol(**kwargs: object) -> dict[str, object]:
        processed_symbols.append(str(kwargs["symbol"]))
        return {
            "quarterly_rows_written": 5,
            "ttm_rows_written": 2,
            "lifecycle_rows_written": 2,
            "score_rows_written": 2,
        }

    monkeypatch.setattr(
        "swingmaster.cli.run_fundamental_yahoo_batch_fin.process_symbol",
        _process_symbol,
    )

    summary = run_yahoo_batch_fin(
        db_path=tmp_path / "fundamentals_fin.db",
        osakedata_db_path=osakedata_db_path,
        run_id="RUN_LIMIT",
        limit=2,
        dry_run=False,
        replace_symbol=False,
    )

    assert processed_symbols == ["KNEBV.HE", "NOKIA.HE"]
    assert summary == {
        "market": "omxh",
        "universe_size": 3,
        "symbols_processed": 2,
        "symbols_ok": 2,
        "symbols_error": 0,
        "quarterly_rows_written_total": 10,
        "ttm_rows_written_total": 4,
        "lifecycle_rows_written_total": 4,
        "score_rows_written_total": 4,
        "dry_run": "false",
        "run_id": "RUN_LIMIT",
    }


def test_run_yahoo_batch_fin_continues_after_symbol_error(monkeypatch, capsys, tmp_path: Path) -> None:
    osakedata_db_path = tmp_path / "osakedata_error.db"
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        _insert_osakedata_row(conn, "KNEBV.HE", "2026-01-01", "omxh")
        _insert_osakedata_row(conn, "NOKIA.HE", "2026-01-01", "omxh")
        _insert_osakedata_row(conn, "UPM.HE", "2026-01-01", "omxh")

    def _process_symbol(**kwargs: object) -> dict[str, object]:
        symbol = str(kwargs["symbol"])
        if symbol == "NOKIA.HE":
            raise RuntimeError("boom")
        return {
            "quarterly_rows_written": 5,
            "ttm_rows_written": 2,
            "lifecycle_rows_written": 2,
            "score_rows_written": 2,
        }

    monkeypatch.setattr(
        "swingmaster.cli.run_fundamental_yahoo_batch_fin.process_symbol",
        _process_symbol,
    )

    summary = run_yahoo_batch_fin(
        db_path=tmp_path / "fundamentals_fin.db",
        osakedata_db_path=osakedata_db_path,
        run_id="RUN_ERROR",
        limit=None,
        dry_run=False,
        replace_symbol=False,
    )

    assert summary == {
        "market": "omxh",
        "universe_size": 3,
        "symbols_processed": 3,
        "symbols_ok": 2,
        "symbols_error": 1,
        "quarterly_rows_written_total": 10,
        "ttm_rows_written_total": 4,
        "lifecycle_rows_written_total": 4,
        "score_rows_written_total": 4,
        "dry_run": "false",
        "run_id": "RUN_ERROR",
    }
    assert capsys.readouterr().err.strip().splitlines() == [
        "ERROR symbol=NOKIA.HE message=boom",
    ]
    assert resolve_failure_log_path(tmp_path / "fundamentals_fin.db", "RUN_ERROR").read_text(encoding="utf-8") == (
        "NOKIA.HE\tboom\n"
    )


def test_run_yahoo_batch_fin_sleeps_between_tickers(monkeypatch, tmp_path: Path) -> None:
    osakedata_db_path = tmp_path / "osakedata_sleep.db"
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        _insert_osakedata_row(conn, "KNEBV.HE", "2026-01-01", "omxh")
        _insert_osakedata_row(conn, "NOKIA.HE", "2026-01-01", "omxh")
        _insert_osakedata_row(conn, "UPM.HE", "2026-01-01", "omxh")

    sleep_calls: list[float] = []

    def _process_symbol(**kwargs: object) -> dict[str, object]:
        return {
            "quarterly_rows_written": 0,
            "ttm_rows_written": 0,
            "lifecycle_rows_written": 0,
            "score_rows_written": 0,
        }

    monkeypatch.setattr(
        "swingmaster.cli.run_fundamental_yahoo_batch_fin.process_symbol",
        _process_symbol,
    )
    monkeypatch.setattr(
        "swingmaster.cli.run_fundamental_yahoo_batch_fin.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    run_yahoo_batch_fin(
        db_path=tmp_path / "fundamentals_fin.db",
        osakedata_db_path=osakedata_db_path,
        run_id="RUN_SLEEP",
        limit=None,
        dry_run=False,
        replace_symbol=False,
    )

    assert sleep_calls == [0.5, 0.5]


def test_run_yahoo_batch_fin_does_not_write_failure_file_when_no_errors(monkeypatch, tmp_path: Path) -> None:
    osakedata_db_path = tmp_path / "osakedata_ok.db"
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        _insert_osakedata_row(conn, "KNEBV.HE", "2026-01-01", "omxh")

    monkeypatch.setattr(
        "swingmaster.cli.run_fundamental_yahoo_batch_fin.process_symbol",
        lambda **kwargs: {
            "quarterly_rows_written": 1,
            "ttm_rows_written": 0,
            "lifecycle_rows_written": 0,
            "score_rows_written": 0,
        },
    )

    run_yahoo_batch_fin(
        db_path=tmp_path / "fundamentals_fin.db",
        osakedata_db_path=osakedata_db_path,
        run_id="RUN_OK",
        limit=None,
        dry_run=False,
        replace_symbol=False,
    )

    assert not resolve_failure_log_path(tmp_path / "fundamentals_fin.db", "RUN_OK").exists()


def _create_osakedata_db(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE osakedata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                osake TEXT,
                pvm TEXT,
                market TEXT
            )
            """
        )
        conn.commit()


def _insert_osakedata_row(conn: sqlite3.Connection, osake: str, pvm: str, market: str) -> None:
    conn.execute(
        """
        INSERT INTO osakedata (osake, pvm, market)
        VALUES (?, ?, ?)
        """,
        (osake, pvm, market),
    )
    conn.commit()
