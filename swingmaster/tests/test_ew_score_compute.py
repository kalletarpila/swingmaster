from __future__ import annotations

import json
import math
import sqlite3

import pytest

from swingmaster.ew_score.compute import FASTPASS_V1_SE_BETA0, FASTPASS_V1_SE_THRESHOLD, compute_and_store_ew_scores, _score_fastpass_v1_se
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
    assert row["ew_score_fastpass"] == pytest.approx(0.8279645997813881, abs=1e-12)
    assert row["ew_level_fastpass"] == 1

    fastpass_row = rc_conn.execute(
        """
        SELECT ew_rule_fastpass, inputs_json_fastpass
        FROM rc_ew_score_daily
        WHERE ticker = ? AND date = ?
        """,
        ("AAA", "2020-01-13"),
    ).fetchone()
    assert fastpass_row is not None
    assert fastpass_row[0] == "EW_SCORE_FASTPASS_V1_USA_SMALL"
    payload = json.loads(fastpass_row[1])
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


def test_router_market_modes_call_correct_upsert() -> None:
    class SpyRepo:
        def __init__(self) -> None:
            self.rolling_calls = 0
            self.fastpass_calls = 0
            self.legacy_calls = 0

        def ensure_schema(self) -> None:
            return None

        def upsert_rolling_row(self, **kwargs) -> None:
            self.rolling_calls += 1

        def upsert_fastpass_row(self, **kwargs) -> None:
            self.fastpass_calls += 1

        def upsert_row(self, **kwargs) -> None:
            self.legacy_calls += 1

    # FIN: rolling ON, fastpass OFF
    rc_fin = sqlite3.connect(":memory:")
    os_fin = sqlite3.connect(":memory:")
    rc_fin.execute(
        "CREATE TABLE rc_state_daily (ticker TEXT, date TEXT, state TEXT, state_attrs_json TEXT)"
    )
    rc_fin.execute(
        "CREATE TABLE rc_pipeline_episode (ticker TEXT, entry_window_date TEXT, entry_window_exit_date TEXT, peak60_growth_pct_close_ew_to_peak REAL, ew_confirm_confirmed INTEGER)"
    )
    rc_fin.execute(
        "INSERT INTO rc_state_daily (ticker, date, state, state_attrs_json) VALUES ('AAA','2020-01-10','ENTRY_WINDOW','{}')"
    )
    rc_fin.execute(
        "INSERT INTO rc_pipeline_episode (ticker, entry_window_date, entry_window_exit_date, peak60_growth_pct_close_ew_to_peak, ew_confirm_confirmed) VALUES ('AAA','2020-01-10',NULL,NULL,NULL)"
    )
    rc_fin.commit()
    os_fin.execute(
        "CREATE TABLE osakedata (osake TEXT, pvm TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, market TEXT)"
    )
    os_fin.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("AAA", "2020-01-10", 100.0, 100.0, 100.0, 100.0, 100, "omxh"),
        ],
    )
    os_fin.commit()
    spy_fin = SpyRepo()
    compute_and_store_ew_scores(
        rc_conn=rc_fin,
        osakedata_conn=os_fin,
        as_of_date="2020-01-10",
        rule_id="EW_SCORE_DAY3_V1_FIN",
        repo=spy_fin,  # type: ignore[arg-type]
        print_rows=False,
    )
    assert spy_fin.rolling_calls == 1
    assert spy_fin.fastpass_calls == 0

    # USA: fastpass ON, rolling OFF
    rc_usa = sqlite3.connect(":memory:")
    os_usa = sqlite3.connect(":memory:")
    rc_usa.execute(
        "CREATE TABLE rc_state_daily (ticker TEXT, date TEXT, state TEXT, state_attrs_json TEXT)"
    )
    rc_usa.execute(
        "CREATE TABLE rc_pipeline_episode (ticker TEXT, entry_window_date TEXT, entry_window_exit_date TEXT, peak60_growth_pct_close_ew_to_peak REAL, ew_confirm_confirmed INTEGER)"
    )
    rc_usa.executemany(
        "INSERT INTO rc_state_daily (ticker, date, state, state_attrs_json) VALUES (?, ?, ?, ?)",
        [
            ("BBB", "2020-01-09", "STABILIZING", None),
            ("BBB", "2020-01-10", "ENTRY_WINDOW", '{"decline_profile":"UNKNOWN","entry_quality":"A"}'),
        ],
    )
    rc_usa.execute(
        "INSERT INTO rc_pipeline_episode (ticker, entry_window_date, entry_window_exit_date, peak60_growth_pct_close_ew_to_peak, ew_confirm_confirmed) VALUES ('BBB','2020-01-10',NULL,NULL,NULL)"
    )
    rc_usa.commit()
    os_usa.execute(
        "CREATE TABLE osakedata (osake TEXT, pvm TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, market TEXT)"
    )
    os_usa.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("BBB", "2020-01-09", 100.0, 100.0, 100.0, 100.0, 100, "usa"),
            ("BBB", "2020-01-10", 105.0, 105.0, 105.0, 105.0, 100, "usa"),
        ],
    )
    os_usa.commit()
    spy_usa = SpyRepo()
    compute_and_store_ew_scores(
        rc_conn=rc_usa,
        osakedata_conn=os_usa,
        as_of_date="2020-01-10",
        rule_id="EW_SCORE_DAY3_V1_FIN",
        repo=spy_usa,  # type: ignore[arg-type]
        print_rows=False,
    )
    assert spy_usa.fastpass_calls == 1
    assert spy_usa.rolling_calls == 0


def test_score_fastpass_v1_se_deterministic() -> None:
    assert FASTPASS_V1_SE_THRESHOLD == pytest.approx(0.65, abs=1e-12)

    r_stab_to_entry_pct = 2.5
    downtrend_origin = "SLOW"
    downtrend_entry_type = "TREND_SOFT"
    decline_profile = "SLOW_DRIFT"
    stabilization_phase = "EARLY_REVERSAL"
    entry_gate = "EARLY_STAB_MA20"
    entry_quality = "B"

    z_expected = FASTPASS_V1_SE_BETA0
    z_expected += 0.4235956974235532 * r_stab_to_entry_pct
    z_expected += -0.5567538554589132
    z_expected += 0.6865754008841269
    z_expected += 2.290586835702952
    z_expected += -0.3624012303920593
    z_expected += 0.1382594006923378
    z_expected += 0.1382594006923378
    score_expected = 1.0 / (1.0 + math.exp(-z_expected))

    z_actual, score_actual = _score_fastpass_v1_se(
        r_stab_to_entry_pct=r_stab_to_entry_pct,
        downtrend_origin=downtrend_origin,
        downtrend_entry_type=downtrend_entry_type,
        decline_profile=decline_profile,
        stabilization_phase=stabilization_phase,
        entry_gate=entry_gate,
        entry_quality=entry_quality,
    )
    assert z_actual == pytest.approx(z_expected, abs=1e-12)
    assert score_actual == pytest.approx(score_expected, abs=1e-12)
