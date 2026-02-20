from __future__ import annotations

import json
import math
import sqlite3

import pytest

from swingmaster.ew_score.compute import compute_and_store_ew_scores
from swingmaster.ew_score.model_config import load_model_config
from swingmaster.ew_score.repo import RcEwScoreDailyRepo


def test_compute_and_store_ew_scores_progressive_prefix() -> None:
    rc_conn = sqlite3.connect(":memory:")
    os_conn = sqlite3.connect(":memory:")
    repo = RcEwScoreDailyRepo(rc_conn)

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
            ("AAA", "2020-01-15", "ENTRY_WINDOW", None),
            ("BBB", "2020-01-15", "ENTRY_WINDOW", None),
        ],
    )
    rc_conn.executemany(
        """
        INSERT INTO rc_pipeline_episode (
          ticker, entry_window_date, entry_window_exit_date,
          peak60_growth_pct_close_ew_to_peak, ew_confirm_confirmed
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("AAA", "2020-01-10", None, None, None),
            ("BBB", "2020-01-10", None, None, None),
        ],
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
            ("AAA", "2020-01-13", 101.0, 101.0, 101.0, 101.0, 100, "FIN"),
            ("AAA", "2020-01-14", 103.0, 103.0, 103.0, 103.0, 100, "FIN"),
            ("AAA", "2020-01-15", 104.0, 104.0, 104.0, 104.0, 100, "FIN"),
            ("BBB", "2020-01-10", 20.0, 20.0, 20.0, 20.0, 100, "FIN"),
            ("BBB", "2020-01-13", 18.0, 18.0, 18.0, 18.0, 100, "FIN"),
        ],
    )
    os_conn.commit()

    n = compute_and_store_ew_scores(
        rc_conn=rc_conn,
        osakedata_conn=os_conn,
        as_of_date="2020-01-15",
        rule_id="EW_SCORE_DAY3_V1_FIN",
        repo=repo,
        print_rows=False,
    )
    assert n == 2

    aaa = repo.get_row("AAA", "2020-01-15")
    bbb = repo.get_row("BBB", "2020-01-15")
    assert aaa is not None
    assert bbb is not None
    assert aaa["ew_level_day3"] == 3
    assert bbb["ew_level_day3"] == 0

    aaa_inputs = json.loads(aaa["inputs_json"])
    bbb_inputs = json.loads(bbb["inputs_json"])
    required_keys = {
        "rule_id",
        "beta0",
        "beta1",
        "as_of_date",
        "entry_window_date",
        "entry_window_exit_date",
        "rn_available",
        "close_day0",
        "close_prefix",
        "r_prefix_pct",
    }
    assert required_keys.issubset(set(aaa_inputs.keys()))
    assert required_keys.issubset(set(bbb_inputs.keys()))

    assert aaa_inputs["rn_available"] == 4
    assert bbb_inputs["rn_available"] == 2
    assert aaa_inputs["level3_score_threshold"] == pytest.approx(0.47, abs=1e-12)

    expected_r_aaa = 100.0 * (104.0 / 100.0 - 1.0)
    expected_r_bbb = 100.0 * (18.0 / 20.0 - 1.0)
    assert aaa_inputs["r_prefix_pct"] == pytest.approx(expected_r_aaa, abs=1e-12)
    assert bbb_inputs["r_prefix_pct"] == pytest.approx(expected_r_bbb, abs=1e-12)

    cfg = load_model_config("EW_SCORE_DAY3_V1_FIN")
    expected_score_aaa = 1.0 / (1.0 + math.exp(-(cfg.beta0 + cfg.beta1 * expected_r_aaa)))
    expected_score_bbb = 1.0 / (1.0 + math.exp(-(cfg.beta0 + cfg.beta1 * expected_r_bbb)))
    assert aaa["ew_score_day3"] == pytest.approx(expected_score_aaa, abs=1e-12)
    assert bbb["ew_score_day3"] == pytest.approx(expected_score_bbb, abs=1e-12)
