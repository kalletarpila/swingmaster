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
            ("AAA", "2020-01-16", "ENTRY_WINDOW", None),
            ("BBB", "2020-01-16", "ENTRY_WINDOW", None),
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
            ("AAA", "2020-01-16", 105.0, 105.0, 105.0, 105.0, 100, "FIN"),
            ("BBB", "2020-01-10", 20.0, 20.0, 20.0, 20.0, 100, "FIN"),
            ("BBB", "2020-01-13", 24.0, 24.0, 24.0, 24.0, 100, "FIN"),
        ],
    )
    os_conn.commit()

    n = compute_and_store_ew_scores(
        rc_conn=rc_conn,
        osakedata_conn=os_conn,
        as_of_date="2020-01-16",
        rule_id="EW_SCORE_DAY3_V1_FIN",
        repo=repo,
        print_rows=False,
    )
    assert n == 2

    aaa = repo.get_row("AAA", "2020-01-16")
    bbb = repo.get_row("BBB", "2020-01-16")
    assert aaa is not None
    assert bbb is not None
    assert aaa["ew_level_day3"] == 3
    assert bbb["ew_level_day3"] == 1

    aaa_inputs = json.loads(aaa["inputs_json"])
    bbb_inputs = json.loads(bbb["inputs_json"])
    required_keys = {
        "rule_id",
        "beta0",
        "beta1",
        "as_of_date",
        "entry_window_date",
        "entry_window_exit_date",
        "rows_total",
        "pvm_day0",
        "pvm_today",
        "close_day0",
        "close_today",
        "r_prefix_pct",
    }
    assert required_keys.issubset(set(aaa_inputs.keys()))
    assert required_keys.issubset(set(bbb_inputs.keys()))

    assert aaa_inputs["rows_total"] == 5
    assert bbb_inputs["rows_total"] == 2
    assert aaa_inputs["pvm_day0"] == "2020-01-10"
    assert aaa_inputs["pvm_today"] == "2020-01-16"
    assert aaa_inputs["level3_score_threshold"] == pytest.approx(0.47, abs=1e-12)

    expected_r_aaa = 100.0 * (105.0 / 100.0 - 1.0)
    expected_r_bbb = 100.0 * (24.0 / 20.0 - 1.0)
    assert aaa_inputs["r_prefix_pct"] == pytest.approx(expected_r_aaa, abs=1e-12)
    assert bbb_inputs["r_prefix_pct"] == pytest.approx(expected_r_bbb, abs=1e-12)

    cfg = load_model_config("EW_SCORE_DAY3_V1_FIN")
    expected_score_aaa = 1.0 / (1.0 + math.exp(-(cfg.beta0 + cfg.beta1 * expected_r_aaa)))
    expected_score_bbb = 1.0 / (1.0 + math.exp(-(cfg.beta0 + cfg.beta1 * expected_r_bbb)))
    assert aaa["ew_score_day3"] == pytest.approx(expected_score_aaa, abs=1e-12)
    assert bbb["ew_score_day3"] == pytest.approx(expected_score_bbb, abs=1e-12)


def test_compute_and_store_ew_scores_fastpass_usa_small_writes_fastpass_columns() -> None:
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
            ("AAA", "2020-01-09", "STABILIZING", None),
            ("AAA", "2020-01-10", "ENTRY_WINDOW", '{"decline_profile":"UNKNOWN","entry_quality":"A"}'),
            ("AAA", "2020-01-13", "ENTRY_WINDOW", None),
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
            ("AAA", "2020-01-09", 100.0, 100.0, 100.0, 100.0, 100, "usa"),
            ("AAA", "2020-01-10", 105.0, 105.0, 105.0, 105.0, 100, "usa"),
        ],
    )
    os_conn.commit()

    n = compute_and_store_ew_scores(
        rc_conn=rc_conn,
        osakedata_conn=os_conn,
        as_of_date="2020-01-13",
        rule_id="EW_SCORE_FASTPASS_V1_USA_SMALL",
        repo=repo,
        print_rows=False,
    )
    assert n == 1

    row = repo.get_row("AAA", "2020-01-13")
    assert row is not None
    assert row["ew_rule"] == "EW_SCORE_FASTPASS_V1_USA_SMALL"
    assert row["ew_score_fastpass"] == pytest.approx(0.8279645997813881, abs=1e-12)
    assert row["ew_level_fastpass"] == 1

    payload = json.loads(row["inputs_json"])
    assert payload["rule_id"] == "EW_SCORE_FASTPASS_V1_USA_SMALL"
    assert payload["beta0"] == pytest.approx(0.002991128723180779, abs=1e-12)
    assert payload["threshold"] == pytest.approx(0.60, abs=1e-12)
    assert payload["entry_date"] == "2020-01-10"
    assert payload["last_stab_date"] == "2020-01-09"
    assert payload["close_entry"] == pytest.approx(105.0, abs=1e-12)
    assert payload["close_last_stab"] == pytest.approx(100.0, abs=1e-12)
    assert payload["r_stab_to_entry_pct"] == pytest.approx(5.0, abs=1e-12)
    assert payload["decline_profile"] == "UNKNOWN"
    assert payload["entry_quality"] == "A"
    assert payload["score_raw_z"] == pytest.approx(1.5712701287231807, abs=1e-12)
