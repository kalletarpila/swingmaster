from __future__ import annotations

import sqlite3

from swingmaster.infra.sqlite.migrator import apply_migrations


def test_apply_migrations_creates_dual_inference_current_table() -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)

    table = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'rc_episode_model_dual_inference_current'
        """
    ).fetchone()
    assert table is not None

    cols = {str(row[1]): str(row[2]) for row in conn.execute("PRAGMA table_info(rc_episode_model_dual_inference_current)")}
    assert cols["episode_id"] == "TEXT"
    assert cols["score_up20_meta_v1"] == "REAL"
    assert cols["score_fail10_60d_close_hgb"] == "REAL"
    assert cols["model_version"] == "TEXT"
    assert cols["computed_at"] == "TEXT"
