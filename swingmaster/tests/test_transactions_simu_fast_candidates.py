from __future__ import annotations

import sqlite3

from swingmaster.cli.run_transactions_simu_fast import (
    fetch_new_notrade_candidates,
    fetch_new_pass_candidates,
    inspect_schema,
)


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
    rows = fetch_new_notrade_candidates(conn, "2026-01-20", "2026-01-20", schema, None)

    assert len(rows) == 1
    assert rows[0]["section"] == "NEW_NOTRADE"
    assert rows[0]["ticker"] == "AAA"
    assert rows[0]["from_state"] == "ENTRY_WINDOW"
    assert rows[0]["to_state"] == "NO_TRADE"
    assert rows[0]["buy_price"] == 12.34


def test_fetch_new_pass_candidates_assigns_dual_buy_badge_from_scores() -> None:
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
        CREATE TABLE rc_ew_score_daily (
          ticker TEXT NOT NULL,
          date TEXT NOT NULL,
          ew_score_fastpass REAL,
          ew_level_fastpass INTEGER,
          ew_score_rolling REAL,
          ew_level_rolling INTEGER,
          ew_score_up20_meta REAL,
          ew_score_fail10_hgb REAL
        )
        """
    )
    fixtures = [
        ("AAA", 0.85, 0.19, "DUAL_PREMIUM"),
        ("AAB", 0.75, 0.25, "DUAL_ELITE"),
        ("AAC", 0.67, 0.31, "DUAL_STRONG"),
        ("AAD", 0.60, 0.35, "DUAL_QUALIFIED"),
    ]
    for ticker, up20, fail10, _badge in fixtures:
        conn.execute(
            """
            INSERT INTO rc_transition (ticker, date, from_state, to_state)
            VALUES (?, '2026-01-20', 'ENTRY_WINDOW', 'PASS')
            """,
            (ticker,),
        )
        conn.execute(
            """
            INSERT INTO rc_pipeline_episode (
              ticker, downtrend_entry_date, entry_window_date, entry_window_exit_date, entry_window_exit_state, close_at_ew_exit
            ) VALUES (?, '2026-01-01', '2026-01-10', '2026-01-20', 'PASS', 12.34)
            """,
            (ticker,),
        )
        conn.execute(
            """
            INSERT INTO rc_ew_score_daily (
              ticker, date, ew_score_fastpass, ew_level_fastpass, ew_score_up20_meta, ew_score_fail10_hgb
            ) VALUES (?, '2026-01-10', 0.81, 1, ?, ?)
            """,
            (ticker, up20, fail10),
        )

    schema = inspect_schema(conn)
    rows = fetch_new_pass_candidates(
        conn,
        "2026-01-20",
        "2026-01-20",
        schema,
        ("ew_score_up20_meta", "ew_score_fail10_hgb"),
    )

    by_ticker = {str(row["ticker"]): row for row in rows}
    assert len(by_ticker) == 4
    for ticker, _up20, _fail10, expected_badge in fixtures:
        assert by_ticker[ticker]["section"] == "NEW_PASS"
        assert by_ticker[ticker]["dual_buy_badge"] == expected_badge


def test_fetch_new_pass_candidates_populates_probabilistic_fields_by_regime() -> None:
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
          episode_id TEXT NOT NULL,
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
        CREATE TABLE rc_episode_regime (
          episode_id TEXT NOT NULL,
          ew_exit_regime_combined TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE rc_episode_model_score (
          episode_id TEXT NOT NULL,
          model_id TEXT NOT NULL,
          predicted_probability REAL
        )
        """
    )

    fixtures = [
        ("AAA", "EPI_AAA", "BULL", 0.22, 0.41),
        ("BBB", "EPI_BBB", "BEAR", None, 0.13),
    ]
    for ticker, episode_id, regime, fail10_prob, up20_prob in fixtures:
        conn.execute(
            """
            INSERT INTO rc_transition (ticker, date, from_state, to_state)
            VALUES (?, '2026-01-20', 'ENTRY_WINDOW', 'PASS')
            """,
            (ticker,),
        )
        conn.execute(
            """
            INSERT INTO rc_pipeline_episode (
              episode_id, ticker, downtrend_entry_date, entry_window_date, entry_window_exit_date, entry_window_exit_state, close_at_ew_exit
            ) VALUES (?, ?, '2026-01-01', '2026-01-10', '2026-01-20', 'PASS', 12.34)
            """,
            (episode_id, ticker),
        )
        conn.execute(
            """
            INSERT INTO rc_episode_regime (episode_id, ew_exit_regime_combined)
            VALUES (?, ?)
            """,
            (episode_id, regime),
        )
        if fail10_prob is not None:
            conn.execute(
                """
                INSERT INTO rc_episode_model_score (episode_id, model_id, predicted_probability)
                VALUES (?, 'FAIL10_BULL_HGB_V1', ?)
                """,
                (episode_id, fail10_prob),
            )
        bull_up = up20_prob if regime == "BULL" else 0.99
        bear_up = up20_prob if regime == "BEAR" else 0.99
        conn.execute(
            """
            INSERT INTO rc_episode_model_score (episode_id, model_id, predicted_probability)
            VALUES (?, 'UP20_BULL_HGB_V1', ?)
            """,
            (episode_id, bull_up),
        )
        conn.execute(
            """
            INSERT INTO rc_episode_model_score (episode_id, model_id, predicted_probability)
            VALUES (?, 'UP20_BEAR_HGB_V1', ?)
            """,
            (episode_id, bear_up),
        )

    schema = inspect_schema(conn)
    rows = fetch_new_pass_candidates(conn, "2026-01-20", "2026-01-20", schema, None)
    by_ticker = {str(row["ticker"]): row for row in rows}

    assert by_ticker["AAA"]["regime"] == "BULL"
    assert by_ticker["AAA"]["entry_window_exit_state"] == "PASS"
    assert by_ticker["AAA"]["fail10_prob"] == 0.22
    assert by_ticker["AAA"]["up20_prob"] == 0.41

    assert by_ticker["BBB"]["regime"] == "BEAR"
    assert by_ticker["BBB"]["entry_window_exit_state"] == "PASS"
    assert by_ticker["BBB"]["fail10_prob"] is None
    assert by_ticker["BBB"]["up20_prob"] == 0.13
