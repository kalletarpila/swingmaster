from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.regime.production import (
    RC_EPISODE_REGIME_TABLE,
    RC_MARKET_REGIME_DAILY_TABLE,
    compute_and_store_market_regimes,
)


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


def _make_osakedata_with_crash(path: Path) -> dict[str, str]:
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
    days = 240
    payload: list[tuple[object, ...]] = []
    key_dates: dict[str, str] = {}
    for i in range(days):
        d = start + timedelta(days=i)
        dt = d.isoformat()
        base_sp = 100.0 + i
        base_ndx = 300.0 + i * 1.5
        if i == 210:
            key_dates["d210"] = dt
            base_sp = 120.0
            base_ndx = 260.0
        if i == 211:
            key_dates["d211"] = dt
            base_sp = 118.0
            base_ndx = 258.0
        if i == 212:
            key_dates["d212"] = dt
        payload.append(("^GSPC", dt, base_sp, base_sp, base_sp, base_sp, 1000000 + i, "usa"))
        payload.append(("^NDX", dt, base_ndx, base_ndx, base_ndx, base_ndx, 2000000 + i, "usa"))
    conn.executemany(
        """
        INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()
    conn.close()
    return key_dates


def test_compute_and_store_market_regimes_maps_entry_and_exit(tmp_path: Path) -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    _create_pipeline_episode_table(conn)
    key_dates = _make_osakedata_with_crash(tmp_path / "osakedata.db")

    conn.executemany(
        """
        INSERT INTO rc_pipeline_episode (episode_id, ticker, entry_window_date, entry_window_exit_date)
        VALUES (?, ?, ?, ?)
        """,
        [
            ("EP1", "AAA", key_dates["d210"], key_dates["d211"]),
            ("EP2", "BBB", key_dates["d211"], key_dates["d212"]),
        ],
    )
    conn.commit()

    summary = compute_and_store_market_regimes(
        conn,
        osakedata_db_path=str(tmp_path / "osakedata.db"),
        market="usa",
        regime_version="REGIME_TEST_V1",
        crash_confirm_days=2,
        mode="upsert",
        computed_at="2026-03-07T00:00:00+00:00",
    )

    assert summary.rows_daily_source > 0
    assert summary.rows_episode_source == 2

    daily_row_210 = conn.execute(
        f"""
        SELECT regime_combined
        FROM {RC_MARKET_REGIME_DAILY_TABLE}
        WHERE trade_date=? AND market='usa' AND regime_version='REGIME_TEST_V1'
        """,
        (key_dates["d210"],),
    ).fetchone()
    daily_row_211 = conn.execute(
        f"""
        SELECT regime_combined
        FROM {RC_MARKET_REGIME_DAILY_TABLE}
        WHERE trade_date=? AND market='usa' AND regime_version='REGIME_TEST_V1'
        """,
        (key_dates["d211"],),
    ).fetchone()
    assert daily_row_210 is not None and daily_row_210[0] == "SIDEWAYS"
    assert daily_row_211 is not None and daily_row_211[0] == "CRASH_ALERT"

    ep1 = conn.execute(
        f"""
        SELECT
          ew_entry_regime_combined,
          ew_exit_regime_combined
        FROM {RC_EPISODE_REGIME_TABLE}
        WHERE episode_id='EP1' AND regime_version='REGIME_TEST_V1'
        """
    ).fetchone()
    ep2 = conn.execute(
        f"""
        SELECT
          ew_entry_regime_combined,
          ew_exit_regime_combined
        FROM {RC_EPISODE_REGIME_TABLE}
        WHERE episode_id='EP2' AND regime_version='REGIME_TEST_V1'
        """
    ).fetchone()
    assert ep1 is not None and ep1[0] == "SIDEWAYS" and ep1[1] == "CRASH_ALERT"
    assert ep2 is not None and ep2[0] == "CRASH_ALERT"


def test_compute_and_store_market_regimes_insert_missing_keeps_old_rows(tmp_path: Path) -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    _create_pipeline_episode_table(conn)
    key_dates = _make_osakedata_with_crash(tmp_path / "osakedata.db")
    conn.execute(
        """
        INSERT INTO rc_pipeline_episode (episode_id, ticker, entry_window_date, entry_window_exit_date)
        VALUES (?, ?, ?, ?)
        """,
        ("EP1", "AAA", key_dates["d210"], key_dates["d211"]),
    )
    conn.commit()

    compute_and_store_market_regimes(
        conn,
        osakedata_db_path=str(tmp_path / "osakedata.db"),
        market="usa",
        regime_version="REGIME_TEST_V1",
        crash_confirm_days=2,
        mode="upsert",
        computed_at="2026-03-07T00:00:00+00:00",
    )
    before = conn.execute(
        f"""
        SELECT computed_at
        FROM {RC_EPISODE_REGIME_TABLE}
        WHERE episode_id='EP1' AND regime_version='REGIME_TEST_V1'
        """
    ).fetchone()
    assert before is not None and before[0] == "2026-03-07T00:00:00+00:00"

    summary = compute_and_store_market_regimes(
        conn,
        osakedata_db_path=str(tmp_path / "osakedata.db"),
        market="usa",
        regime_version="REGIME_TEST_V1",
        crash_confirm_days=2,
        mode="insert-missing",
        computed_at="2026-03-08T00:00:00+00:00",
    )
    assert summary.rows_daily_changed == 0
    assert summary.rows_episode_changed == 0

    after = conn.execute(
        f"""
        SELECT computed_at
        FROM {RC_EPISODE_REGIME_TABLE}
        WHERE episode_id='EP1' AND regime_version='REGIME_TEST_V1'
        """
    ).fetchone()
    assert after is not None and after[0] == "2026-03-07T00:00:00+00:00"
