from __future__ import annotations

import sqlite3

from swingmaster.ew_score.daily_list import fetch_daily_production_rows


def test_fetch_daily_production_rows_join_and_ordering() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE rc_state_daily (
          ticker TEXT,
          date TEXT,
          state TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE rc_ew_score_daily (
          ticker TEXT,
          date TEXT,
          ew_level_day3 INTEGER,
          ew_score_day3 REAL,
          ew_rule TEXT
        )
        """
    )

    conn.executemany(
        "INSERT INTO rc_state_daily (ticker, date, state) VALUES (?, ?, ?)",
        [
            ("AAA", "2026-01-19", "ENTRY_WINDOW"),
            ("BBB", "2026-01-19", "ENTRY_WINDOW"),
            ("CCC", "2026-01-19", "NO_TRADE"),
        ],
    )
    conn.execute(
        """
        INSERT INTO rc_ew_score_daily (ticker, date, ew_level_day3, ew_score_day3, ew_rule)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("AAA", "2026-01-19", 2, 0.91, "EW_SCORE_DAY3_V1_FIN"),
    )
    conn.commit()

    rows = fetch_daily_production_rows(conn, "2026-01-19")
    assert len(rows) == 3

    assert rows[0]["ticker"] == "AAA"
    assert rows[0]["state"] == "ENTRY_WINDOW"
    assert rows[0]["ew_level_day3"] == 2
    assert rows[0]["ew_score_day3"] == 0.91
    assert rows[0]["ew_rule"] == "EW_SCORE_DAY3_V1_FIN"

    by_ticker = {r["ticker"]: r for r in rows}
    assert by_ticker["BBB"]["ew_level_day3"] is None
    assert by_ticker["BBB"]["ew_score_day3"] is None
    assert by_ticker["BBB"]["ew_rule"] is None
    assert by_ticker["CCC"]["state"] == "NO_TRADE"
    assert by_ticker["CCC"]["ew_score_day3"] is None
