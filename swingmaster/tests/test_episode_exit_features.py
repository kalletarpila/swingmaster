from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

from swingmaster.cli import run_episode_exit_features
from swingmaster.episode_exit_features.production import (
    _PriceCache,
    build_episode_exit_feature_row,
    compute_and_store_episode_exit_features,
)
from swingmaster.infra.sqlite.migrator import apply_migrations


def _create_pipeline_episode_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE rc_pipeline_episode (
          episode_id TEXT PRIMARY KEY,
          ticker TEXT NOT NULL,
          entry_window_date TEXT,
          entry_window_exit_date TEXT
        )
        """
    )


def _create_osakedata(path: Path, *, days: int, future_spike: bool = False) -> tuple[str, str]:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE osakedata (
          id INTEGER PRIMARY KEY,
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
    start = date(2024, 1, 1)
    payload: list[tuple[object, ...]] = []
    for i in range(days):
        dt = (start + timedelta(days=i)).isoformat()
        close = 100.0 + i
        open_ = close - 1.0
        high = close + 2.0
        low = close - 2.0
        vol = 1000 + i
        if future_spike and i == days - 1:
            close = 10000.0
            open_ = 10000.0
            high = 10050.0
            low = 9950.0
            vol = 999999
        payload.append(("AAA", dt, open_, high, low, close, vol, "usa"))
        idx_close = 3000.0 + i
        ndx_close = 9000.0 + 2.0 * i
        payload.append(("^GSPC", dt, idx_close, idx_close + 1, idx_close - 1, idx_close, 1_000_000 + i, "usa"))
        payload.append(("^NDX", dt, ndx_close, ndx_close + 2, ndx_close - 2, ndx_close, 2_000_000 + i, "usa"))
    conn.executemany(
        """
        INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()
    conn.close()
    exit_idx = min(220, days - 1)
    entry_idx = max(0, exit_idx - 40)
    entry = (start + timedelta(days=entry_idx)).isoformat()
    exit_ = (start + timedelta(days=exit_idx)).isoformat()
    return entry, exit_


def test_rc_episode_exit_features_table_created() -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='rc_episode_exit_features'"
    ).fetchone()
    assert row is not None


def test_feature_builder_uses_ew_exit_date_as_of_date(tmp_path: Path) -> None:
    os_db = tmp_path / "os.db"
    entry_date, exit_date = _create_osakedata(os_db, days=260)
    os_conn = sqlite3.connect(str(os_db))
    os_conn.row_factory = sqlite3.Row
    cache = _PriceCache(os_conn)
    row = build_episode_exit_feature_row(
        episode_id="EP1",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=cache,
    )
    os_conn.close()
    assert row is not None
    assert row["as_of_date"] == exit_date
    assert row["entry_window_exit_date"] == exit_date


def test_ew_window_age_days_computed_from_entry_and_exit_dates(tmp_path: Path) -> None:
    os_db = tmp_path / "os.db"
    entry_date, exit_date = _create_osakedata(os_db, days=260)
    os_conn = sqlite3.connect(str(os_db))
    os_conn.row_factory = sqlite3.Row
    cache = _PriceCache(os_conn)
    row = build_episode_exit_feature_row(
        episode_id="EP1",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=cache,
    )
    os_conn.close()
    assert row is not None
    assert row["ew_window_age_days"] == 40
    assert float(row["ew_window_age_pct_of_10"]) == 4.0


def test_feature_builder_does_not_use_future_bars(tmp_path: Path) -> None:
    os_db_a = tmp_path / "os_a.db"
    os_db_b = tmp_path / "os_b.db"
    entry_date, exit_date = _create_osakedata(os_db_a, days=260, future_spike=False)
    _create_osakedata(os_db_b, days=260, future_spike=True)

    conn_a = sqlite3.connect(str(os_db_a))
    conn_a.row_factory = sqlite3.Row
    row_a = build_episode_exit_feature_row(
        episode_id="EP1",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=_PriceCache(conn_a),
    )
    conn_a.close()

    conn_b = sqlite3.connect(str(os_db_b))
    conn_b.row_factory = sqlite3.Row
    row_b = build_episode_exit_feature_row(
        episode_id="EP1",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=_PriceCache(conn_b),
    )
    conn_b.close()

    assert row_a is not None and row_b is not None
    assert row_a["close_vs_ma20_pct"] == row_b["close_vs_ma20_pct"]
    assert row_a["ret_5d"] == row_b["ret_5d"]
    assert row_a["distance_from_52w_high_pct"] == row_b["distance_from_52w_high_pct"]


def test_cli_populates_rc_episode_exit_features_for_episode(monkeypatch, tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    os_db = tmp_path / "os.db"
    entry_date, exit_date = _create_osakedata(os_db, days=260)

    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    _create_pipeline_episode_table(conn)
    conn.execute(
        """
        INSERT INTO rc_pipeline_episode (episode_id, ticker, entry_window_date, entry_window_exit_date)
        VALUES (?, ?, ?, ?)
        """,
        ("EP1", "AAA", entry_date, exit_date),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        run_episode_exit_features,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "rc_db": str(rc_db),
                "osakedata_db": str(os_db),
                "mode": "upsert",
                "date_from": None,
                "date_to": None,
                "computed_at": "2026-03-07T00:00:00+00:00",
            },
        )(),
    )
    run_episode_exit_features.main()

    conn = sqlite3.connect(str(rc_db))
    row = conn.execute(
        """
        SELECT episode_id, as_of_date, close_vs_ma20_pct
        FROM rc_episode_exit_features
        WHERE episode_id='EP1'
        """
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "EP1"
    assert row[1] == exit_date
    assert row[2] is not None


def test_cli_upsert_is_idempotent(monkeypatch, tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    os_db = tmp_path / "os.db"
    entry_date, exit_date = _create_osakedata(os_db, days=260)

    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    _create_pipeline_episode_table(conn)
    conn.execute(
        "INSERT INTO rc_pipeline_episode (episode_id, ticker, entry_window_date, entry_window_exit_date) VALUES (?, ?, ?, ?)",
        ("EP1", "AAA", entry_date, exit_date),
    )
    conn.commit()
    conn.close()

    args = type(
        "Args",
        (),
        {
            "rc_db": str(rc_db),
            "osakedata_db": str(os_db),
            "mode": "upsert",
            "date_from": None,
            "date_to": None,
            "computed_at": "2026-03-07T00:00:00+00:00",
        },
    )()
    monkeypatch.setattr(run_episode_exit_features, "parse_args", lambda: args)
    run_episode_exit_features.main()
    run_episode_exit_features.main()

    conn = sqlite3.connect(str(rc_db))
    count = conn.execute("SELECT COUNT(*) FROM rc_episode_exit_features").fetchone()[0]
    conn.close()
    assert count == 1


def test_missing_history_yields_null_for_long_window_features(tmp_path: Path) -> None:
    os_db = tmp_path / "os.db"
    entry_date, exit_date = _create_osakedata(os_db, days=80)
    os_conn = sqlite3.connect(str(os_db))
    os_conn.row_factory = sqlite3.Row
    row = build_episode_exit_feature_row(
        episode_id="EP1",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=_PriceCache(os_conn),
    )
    os_conn.close()
    assert row is not None
    assert row["close_vs_ma200_pct"] is None
    assert row["distance_from_52w_high_pct"] is None


def test_v1_feature_correctness_examples(tmp_path: Path) -> None:
    os_db = tmp_path / "os.db"
    entry_date, exit_date = _create_osakedata(os_db, days=260)
    os_conn = sqlite3.connect(str(os_db))
    os_conn.row_factory = sqlite3.Row
    row = build_episode_exit_feature_row(
        episode_id="EP1",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=_PriceCache(os_conn),
    )
    os_conn.close()
    assert row is not None

    close_today = 320.0
    ma20 = sum(range(301, 321)) / 20.0
    expected_close_vs_ma20 = close_today / ma20 - 1.0
    assert abs(float(row["close_vs_ma20_pct"]) - expected_close_vs_ma20) < 1e-12

    expected_ret_5d = 320.0 / 315.0 - 1.0
    assert abs(float(row["ret_5d"]) - expected_ret_5d) < 1e-12

    assert row["atr14_pct"] is not None
    assert row["volume_vs_avg20"] is not None

    expected_body_pct = abs(320.0 - 319.0) / (322.0 - 318.0)
    assert abs(float(row["body_pct_of_range"]) - expected_body_pct) < 1e-12

    expected_index_ret_5d = ((3220.0 / 3215.0 - 1.0) + (9440.0 / 9430.0 - 1.0)) / 2.0
    assert abs(float(row["index_ret_5d"]) - expected_index_ret_5d) < 1e-12


def test_compute_and_store_episode_exit_features_insert_missing_mode(tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    os_db = tmp_path / "os.db"
    entry_date, exit_date = _create_osakedata(os_db, days=260)

    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    _create_pipeline_episode_table(conn)
    conn.execute(
        "INSERT INTO rc_pipeline_episode (episode_id, ticker, entry_window_date, entry_window_exit_date) VALUES (?, ?, ?, ?)",
        ("EP1", "AAA", entry_date, exit_date),
    )
    conn.commit()

    first = compute_and_store_episode_exit_features(
        conn,
        osakedata_db_path=str(os_db),
        mode="insert-missing",
        computed_at="2026-03-07T00:00:00+00:00",
    )
    second = compute_and_store_episode_exit_features(
        conn,
        osakedata_db_path=str(os_db),
        mode="insert-missing",
        computed_at="2026-03-08T00:00:00+00:00",
    )
    conn.close()

    assert first.inserted == 1
    assert second.inserted == 0
    assert second.updated == 0


def test_index_context_combined_all_index_features(tmp_path: Path) -> None:
    os_db = tmp_path / "os_combined.db"
    entry_date, exit_date = _create_osakedata(os_db, days=260)
    os_conn = sqlite3.connect(str(os_db))
    os_conn.row_factory = sqlite3.Row
    row = build_episode_exit_feature_row(
        episode_id="EP_INDEX",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=_PriceCache(os_conn),
    )
    os_conn.close()
    assert row is not None

    # On exit day (i=220): GSPC close=3220, NDX close=9440.
    g_ret_1d = 3220.0 / 3219.0 - 1.0
    n_ret_1d = 9440.0 / 9438.0 - 1.0
    g_ret_5d = 3220.0 / 3215.0 - 1.0
    n_ret_5d = 9440.0 / 9430.0 - 1.0
    g_ret_20d = 3220.0 / 3200.0 - 1.0
    n_ret_20d = 9440.0 / 9400.0 - 1.0

    g_ma50 = sum(3171 + j for j in range(50)) / 50.0
    n_ma50 = sum(9342 + 2 * j for j in range(50)) / 50.0
    g_ma200 = sum(3021 + j for j in range(200)) / 200.0
    n_ma200 = sum(9042 + 2 * j for j in range(200)) / 200.0
    g_vs_ma50 = 3220.0 / g_ma50 - 1.0
    n_vs_ma50 = 9440.0 / n_ma50 - 1.0
    g_vs_ma200 = 3220.0 / g_ma200 - 1.0
    n_vs_ma200 = 9440.0 / n_ma200 - 1.0

    expected_ret_1d = (g_ret_1d + n_ret_1d) / 2.0
    expected_ret_5d = (g_ret_5d + n_ret_5d) / 2.0
    expected_ret_20d = (g_ret_20d + n_ret_20d) / 2.0
    expected_vs_ma50 = (g_vs_ma50 + n_vs_ma50) / 2.0
    expected_vs_ma200 = (g_vs_ma200 + n_vs_ma200) / 2.0

    assert abs(float(row["index_ret_1d"]) - expected_ret_1d) < 1e-12
    assert abs(float(row["index_ret_5d"]) - expected_ret_5d) < 1e-12
    assert abs(float(row["index_ret_20d"]) - expected_ret_20d) < 1e-12
    assert abs(float(row["index_close_vs_ma50_pct"]) - expected_vs_ma50) < 1e-12
    assert abs(float(row["index_close_vs_ma200_pct"]) - expected_vs_ma200) < 1e-12
    assert row["index_volatility_10d"] is not None


def test_index_context_combined_requires_both_indices(tmp_path: Path) -> None:
    os_db = tmp_path / "os_missing_ndx.db"
    conn = sqlite3.connect(str(os_db))
    conn.execute(
        """
        CREATE TABLE osakedata (
          id INTEGER PRIMARY KEY,
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
    start = date(2024, 1, 1)
    payload: list[tuple[object, ...]] = []
    for i in range(260):
        dt = (start + timedelta(days=i)).isoformat()
        close = 100.0 + i
        payload.append(("AAA", dt, close - 1, close + 2, close - 2, close, 1000 + i, "usa"))
        g = 3000.0 + i
        payload.append(("^GSPC", dt, g, g + 1, g - 1, g, 1_000_000 + i, "usa"))
        # Skip NDX on as-of day only.
        if i != 220:
            n = 9000.0 + 2 * i
            payload.append(("^NDX", dt, n, n + 2, n - 2, n, 2_000_000 + i, "usa"))
    conn.executemany(
        """
        INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()
    conn.close()

    entry_date = (start + timedelta(days=180)).isoformat()
    exit_date = (start + timedelta(days=220)).isoformat()
    os_conn = sqlite3.connect(str(os_db))
    os_conn.row_factory = sqlite3.Row
    row = build_episode_exit_feature_row(
        episode_id="EP_INDEX_NULL",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=_PriceCache(os_conn),
    )
    os_conn.close()

    assert row is not None
    assert row["index_ret_1d"] is None
    assert row["index_ret_5d"] is None
    assert row["index_ret_20d"] is None
    assert row["index_close_vs_ma50_pct"] is None
    assert row["index_close_vs_ma200_pct"] is None
    assert row["index_volatility_10d"] is None


def test_down_leg_and_rebound_feature_semantics(tmp_path: Path) -> None:
    os_db = tmp_path / "os_downleg.db"
    conn = sqlite3.connect(str(os_db))
    conn.execute(
        """
        CREATE TABLE osakedata (
          id INTEGER PRIMARY KEY,
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
    start = date(2024, 1, 1)
    payload: list[tuple[object, ...]] = []
    for i in range(25):
        dt = (start + timedelta(days=i)).isoformat()
        close = 100.0 + i
        open_ = close - 1.0
        high = close + 2.0
        low = close - 2.0
        # Pre-low peak (inside last-20 window, before local low).
        if i == 15:
            high = 150.0
        # Local low near the end of the window.
        if i == 23:
            open_ = 73.0
            high = 74.0
            low = 70.0
            close = 72.0
        # As-of day (one bar after local low) for rebound/bars-since-low checks.
        if i == 24:
            open_ = 83.0
            high = 86.0
            low = 82.0
            close = 84.0
        payload.append(("AAA", dt, open_, high, low, close, 1000 + i, "usa"))
        idx_close = 3000.0 + i
        payload.append(("^GSPC", dt, idx_close, idx_close + 1, idx_close - 1, idx_close, 1_000_000 + i, "usa"))
        ndx_close = 9000.0 + 2.0 * i
        payload.append(("^NDX", dt, ndx_close, ndx_close + 2, ndx_close - 2, ndx_close, 2_000_000 + i, "usa"))
    conn.executemany(
        """
        INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()
    conn.close()

    entry_date = (start + timedelta(days=10)).isoformat()
    exit_date = (start + timedelta(days=24)).isoformat()
    os_conn = sqlite3.connect(str(os_db))
    os_conn.row_factory = sqlite3.Row
    row = build_episode_exit_feature_row(
        episode_id="EP_DOWNLEG",
        ticker="AAA",
        entry_window_date=entry_date,
        entry_window_exit_date=exit_date,
        as_of_date=exit_date,
        computed_at="2026-03-07T00:00:00+00:00",
        price_cache=_PriceCache(os_conn),
    )
    os_conn.close()

    assert row is not None
    # Window = last 20 bars (days 5..24). Local low is day 23 (low=70), one bar before as-of.
    assert row["bars_since_local_low"] == 1
    # Rebound from local low to as-of close: 84/70 - 1 = 0.2
    assert abs(float(row["rebound_from_low_pct"]) - 0.2) < 1e-12
    # Pre-low peak is highest high before local low inside same 20-bar window: 150 on day 15.
    assert abs(float(row["down_leg_depth_pct"]) - (70.0 / 150.0 - 1.0)) < 1e-12
    # down_leg_length_bars = bars from pre-low peak (day 15) to local low (day 23) = 8
    assert row["down_leg_length_bars"] == 8
