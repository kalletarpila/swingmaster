from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli import run_episode_dual_inference


def _create_source_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE rc_episode_model_inference_rank_meta_v1 (
          episode_id TEXT,
          score_meta_v1_up20_60d_close REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE rc_episode_model_full_inference_no_dow_scores_hgb_fail10 (
          episode_id TEXT,
          score_pred REAL
        )
        """
    )


def test_main_populates_dual_inference_table_from_source_db(monkeypatch, tmp_path: Path) -> None:
    target_db = tmp_path / "target.db"
    source_db = tmp_path / "source.db"

    source_conn = sqlite3.connect(str(source_db))
    _create_source_tables(source_conn)
    source_conn.executemany(
        """
        INSERT INTO rc_episode_model_inference_rank_meta_v1 (episode_id, score_meta_v1_up20_60d_close)
        VALUES (?, ?)
        """,
        [("EP1", 0.77), ("EP2", 0.55)],
    )
    source_conn.executemany(
        """
        INSERT INTO rc_episode_model_full_inference_no_dow_scores_hgb_fail10 (episode_id, score_pred)
        VALUES (?, ?)
        """,
        [("EP1", 0.22), ("EP2", 0.40)],
    )
    source_conn.commit()
    source_conn.close()

    monkeypatch.setattr(
        run_episode_dual_inference,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "rc_db": str(target_db),
                "source_db": str(source_db),
                "up20_source_table": "rc_episode_model_inference_rank_meta_v1",
                "up20_source_column": "score_meta_v1_up20_60d_close",
                "fail10_source_table": "rc_episode_model_full_inference_no_dow_scores_hgb_fail10",
                "fail10_source_column": "score_pred",
                "mode": "upsert",
                "model_version": "DUAL_V1",
                "computed_at": "2026-03-05T12:00:00+00:00",
            },
        )(),
    )
    run_episode_dual_inference.main()

    target_conn = sqlite3.connect(str(target_db))
    rows = target_conn.execute(
        """
        SELECT episode_id, score_up20_meta_v1, score_fail10_60d_close_hgb, model_version, computed_at
        FROM rc_episode_model_dual_inference_current
        ORDER BY episode_id
        """
    ).fetchall()
    target_conn.close()
    assert rows == [
        ("EP1", 0.77, 0.22, "DUAL_V1", "2026-03-05T12:00:00+00:00"),
        ("EP2", 0.55, 0.4, "DUAL_V1", "2026-03-05T12:00:00+00:00"),
    ]


def test_write_dual_rows_insert_missing_does_not_overwrite_existing(tmp_path: Path) -> None:
    db_path = tmp_path / "target.db"
    conn = sqlite3.connect(str(db_path))
    run_episode_dual_inference.ensure_dual_table(conn)
    conn.execute(
        """
        INSERT INTO rc_episode_model_dual_inference_current (
          episode_id, score_up20_meta_v1, score_fail10_60d_close_hgb, model_version, computed_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ("EP1", 0.50, 0.50, "OLD", "2026-01-01T00:00:00+00:00"),
    )
    conn.commit()

    changed = run_episode_dual_inference.write_dual_rows(
        target_conn=conn,
        rows=[("EP1", 0.99, 0.01), ("EP2", 0.60, 0.35)],
        model_version="NEW",
        computed_at="2026-03-05T12:00:00+00:00",
        mode="insert-missing",
    )
    rows = conn.execute(
        """
        SELECT episode_id, score_up20_meta_v1, score_fail10_60d_close_hgb, model_version
        FROM rc_episode_model_dual_inference_current
        ORDER BY episode_id
        """
    ).fetchall()
    conn.close()

    assert changed == 1
    assert rows == [
        ("EP1", 0.5, 0.5, "OLD"),
        ("EP2", 0.6, 0.35, "NEW"),
    ]
