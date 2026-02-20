from __future__ import annotations

import sqlite3

from swingmaster.cli.run_range_universe import maybe_run_ew_score


def test_maybe_run_ew_score_helper_creates_rows_when_enabled(tmp_path) -> None:
    rc_db = tmp_path / "rc_test.db"
    os_db = tmp_path / "os_test.db"

    rc_conn = sqlite3.connect(str(rc_db))
    rc_conn.execute(
        """
        CREATE TABLE rc_state_daily (
          ticker TEXT,
          date TEXT,
          state TEXT
        )
        """
    )
    rc_conn.execute(
        """
        CREATE TABLE rc_pipeline_episode (
          ticker TEXT,
          entry_window_date TEXT,
          entry_window_exit_date TEXT
        )
        """
    )
    rc_conn.execute(
        "INSERT INTO rc_state_daily (ticker, date, state) VALUES (?, ?, ?)",
        ("AAA", "2020-01-15", "ENTRY_WINDOW"),
    )
    rc_conn.execute(
        "INSERT INTO rc_pipeline_episode (ticker, entry_window_date, entry_window_exit_date) VALUES (?, ?, ?)",
        ("AAA", "2020-01-10", None),
    )
    rc_conn.commit()

    os_conn = sqlite3.connect(str(os_db))
    os_conn.execute(
        """
        CREATE TABLE osakedata (
          osake TEXT,
          pvm TEXT,
          open REAL,
          high REAL,
          low REAL,
          close REAL,
          volume INTEGER,
          market TEXT
        )
        """
    )
    os_conn.executemany(
        """
        INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAA", "2020-01-10", 100.0, 100.0, 100.0, 100.0, 1, "FIN"),
            ("AAA", "2020-01-13", 101.0, 101.0, 101.0, 101.0, 1, "FIN"),
            ("AAA", "2020-01-14", 102.0, 102.0, 102.0, 102.0, 1, "FIN"),
            ("AAA", "2020-01-15", 104.0, 104.0, 104.0, 104.0, 1, "FIN"),
        ],
    )
    os_conn.commit()
    os_conn.close()

    dates_processed, total_rows_written = maybe_run_ew_score(
        rc_conn=rc_conn,
        osakedata_db_path=str(os_db),
        date_from="2020-01-13",
        date_to="2020-01-15",
        rule_id="EW_SCORE_DAY3_V1_FIN",
        enabled=False,
        print_rows=False,
    )
    assert dates_processed == 0
    assert total_rows_written == 0
    missing_table = rc_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='rc_ew_score_daily'"
    ).fetchone()
    assert missing_table is None

    dates_processed, total_rows_written = maybe_run_ew_score(
        rc_conn=rc_conn,
        osakedata_db_path=str(os_db),
        date_from="2020-01-13",
        date_to="2020-01-15",
        rule_id="EW_SCORE_DAY3_V1_FIN",
        enabled=True,
        print_rows=False,
    )
    assert dates_processed == 3
    assert total_rows_written >= 1

    row = rc_conn.execute(
        """
        SELECT ticker, date, ew_level_day3
        FROM rc_ew_score_daily
        WHERE ticker = ? AND date = ?
        """,
        ("AAA", "2020-01-15"),
    ).fetchone()
    assert row is not None
