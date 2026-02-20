from __future__ import annotations

import sqlite3

from swingmaster.ew_score.compute import compute_and_store_ew_scores_range
from swingmaster.ew_score.repo import RcEwScoreDailyRepo


def test_compute_and_store_ew_scores_range_two_dates() -> None:
    rc_conn = sqlite3.connect(":memory:")
    os_conn = sqlite3.connect(":memory:")

    rc_conn.execute(
        """
        CREATE TABLE rc_state_daily (
          ticker TEXT,
          date TEXT,
          state TEXT,
          state_attrs_json TEXT
        )
        """
    )
    rc_conn.execute(
        """
        CREATE TABLE rc_pipeline_episode (
          ticker TEXT,
          entry_window_date TEXT,
          entry_window_exit_date TEXT,
          peak60_growth_pct_close_ew_to_peak REAL,
          ew_confirm_confirmed INTEGER
        )
        """
    )
    rc_conn.executemany(
        "INSERT INTO rc_state_daily (ticker, date, state, state_attrs_json) VALUES (?, ?, ?, ?)",
        [
            ("AAA", "2020-01-13", "ENTRY_WINDOW", None),
            ("AAA", "2020-01-15", "ENTRY_WINDOW", None),
        ],
    )
    rc_conn.execute(
        """
        INSERT INTO rc_pipeline_episode (
          ticker, entry_window_date, entry_window_exit_date,
          peak60_growth_pct_close_ew_to_peak, ew_confirm_confirmed
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ("AAA", "2020-01-10", None, None, None),
    )
    rc_conn.commit()

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
            ("AAA", "2020-01-10", 100.0, 100.0, 100.0, 100.0, 100, "FIN"),
            ("AAA", "2020-01-13", 99.0, 99.0, 99.0, 99.0, 100, "FIN"),
            ("AAA", "2020-01-14", 101.0, 101.0, 101.0, 101.0, 100, "FIN"),
            ("AAA", "2020-01-15", 102.0, 102.0, 102.0, 102.0, 100, "FIN"),
        ],
    )
    os_conn.commit()

    n = compute_and_store_ew_scores_range(
        rc_conn=rc_conn,
        osakedata_conn=os_conn,
        date_from="2020-01-13",
        date_to="2020-01-15",
        rule_id="EW_SCORE_DAY3_V1_FIN",
        print_rows=False,
    )
    assert n == 2

    repo = RcEwScoreDailyRepo(rc_conn)
    row_13 = repo.get_row("AAA", "2020-01-13")
    row_15 = repo.get_row("AAA", "2020-01-15")
    assert row_13 is not None
    assert row_15 is not None

    assert row_13["ew_level_day3"] == 0
    assert row_15["ew_level_day3"] in (2, 3)
