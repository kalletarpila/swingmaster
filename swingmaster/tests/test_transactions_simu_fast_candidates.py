from __future__ import annotations

import sqlite3

from swingmaster.cli.run_transactions_simu_fast import fetch_new_notrade_candidates, inspect_schema


def test_fetch_new_notrade_candidates_uses_entry_window_to_no_trade_transitions() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE rc_transition (
          ticker TEXT NOT NULL,
          date TEXT NOT NULL,
          from_state TEXT,
          to_state TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE rc_pipeline_episode (
          ticker TEXT NOT NULL,
          downtrend_entry_date TEXT NOT NULL,
          entry_window_date TEXT NOT NULL,
          entry_window_exit_date TEXT,
          entry_window_exit_state TEXT,
          close_at_ew_exit REAL
        )
        """
    )

    conn.execute(
        """
        INSERT INTO rc_transition (ticker, date, from_state, to_state)
        VALUES ('AAA', '2026-01-20', 'ENTRY_WINDOW', 'NO_TRADE')
        """
    )
    conn.execute(
        """
        INSERT INTO rc_transition (ticker, date, from_state, to_state)
        VALUES ('BBB', '2026-01-20', 'STABILIZING', 'NO_TRADE')
        """
    )
    conn.execute(
        """
        INSERT INTO rc_pipeline_episode (
          ticker, downtrend_entry_date, entry_window_date, entry_window_exit_date, entry_window_exit_state, close_at_ew_exit
        ) VALUES ('AAA', '2026-01-01', '2026-01-10', '2026-01-20', 'NO_TRADE', 12.34)
        """
    )
    conn.execute(
        """
        INSERT INTO rc_pipeline_episode (
          ticker, downtrend_entry_date, entry_window_date, entry_window_exit_date, entry_window_exit_state, close_at_ew_exit
        ) VALUES ('BBB', '2026-01-03', '2026-01-11', '2026-01-20', 'NO_TRADE', 99.99)
        """
    )

    schema = inspect_schema(conn)
    rows = fetch_new_notrade_candidates(conn, "2026-01-20", "2026-01-20", schema)

    assert len(rows) == 1
    assert rows[0]["section"] == "NEW_NOTRADE"
    assert rows[0]["ticker"] == "AAA"
    assert rows[0]["from_state"] == "ENTRY_WINDOW"
    assert rows[0]["to_state"] == "NO_TRADE"
    assert rows[0]["buy_price"] == 12.34
