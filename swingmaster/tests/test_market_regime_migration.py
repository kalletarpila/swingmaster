from __future__ import annotations

import sqlite3

from swingmaster.infra.sqlite.migrator import apply_migrations


def test_apply_migrations_creates_market_regime_tables() -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)

    daily = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='rc_market_regime_daily'
        """
    ).fetchone()
    episode = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='rc_episode_regime'
        """
    ).fetchone()
    assert daily is not None
    assert episode is not None

    daily_cols = {
        str(row[1]): str(row[2])
        for row in conn.execute("PRAGMA table_info(rc_market_regime_daily)")
    }
    assert daily_cols["trade_date"] == "TEXT"
    assert daily_cols["regime_combined"] == "TEXT"
    assert daily_cols["crash_confirm_days"] == "INTEGER"

    episode_cols = {
        str(row[1]): str(row[2])
        for row in conn.execute("PRAGMA table_info(rc_episode_regime)")
    }
    assert episode_cols["episode_id"] == "TEXT"
    assert episode_cols["ew_entry_regime_combined"] == "TEXT"
    assert episode_cols["ew_exit_regime_combined"] == "TEXT"
