from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import joblib
import numpy as np
import pytest

from swingmaster.cli import run_up20_bear_score
from swingmaster.episode_exit_features.production import FEATURE_COLUMNS
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.research import up20_bear_training
from swingmaster.research.up20_bear_scoring import (
    SCORE_TABLE,
    compute_and_store_up20_bear_scores,
)


class DummyProbModel:
    def predict_proba(self, x):  # type: ignore[no-untyped-def]
        arr = np.asarray(x, dtype=float)
        z = np.clip(arr[:, 0], -6.0, 6.0)
        p = 1.0 / (1.0 + np.exp(-z))
        return np.column_stack([1.0 - p, p])


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


def _insert_episode(
    conn: sqlite3.Connection,
    *,
    episode_id: str,
    ticker: str,
    entry_window_date: str,
    exit_date: str,
    regime: str,
    regime_version: str,
    feature_value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_pipeline_episode (
          episode_id, ticker, entry_window_date, entry_window_exit_date
        ) VALUES (?, ?, ?, ?)
        """,
        (episode_id, ticker, entry_window_date, exit_date),
    )
    feature_cols = [
        "episode_id",
        "ticker",
        "entry_window_date",
        "entry_window_exit_date",
        "as_of_date",
        "computed_at",
        *FEATURE_COLUMNS,
    ]
    values: list[object] = [
        episode_id,
        ticker,
        entry_window_date,
        exit_date,
        exit_date,
        "2026-03-07T00:00:00+00:00",
    ]
    for i in range(len(FEATURE_COLUMNS)):
        values.append(feature_value + (i * 0.001))
    conn.execute(
        f"INSERT INTO rc_episode_exit_features ({', '.join(feature_cols)}) VALUES ({', '.join('?' for _ in feature_cols)})",
        values,
    )
    conn.execute(
        """
        INSERT INTO rc_episode_regime (
          episode_id, market, regime_version,
          ew_entry_date, ew_entry_regime_combined, ew_entry_sp500_state, ew_entry_ndx_state,
          ew_exit_date, ew_exit_regime_combined, ew_exit_sp500_state, ew_exit_ndx_state,
          computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            episode_id,
            "usa",
            regime_version,
            entry_window_date,
            regime,
            regime,
            regime,
            exit_date,
            regime,
            regime,
            regime,
            "2026-03-07T00:00:00+00:00",
        ),
    )


def _seed_scoring_dataset(conn: sqlite3.Connection, regime_version: str) -> None:
    _create_pipeline_episode_table(conn)
    _insert_episode(
        conn,
        episode_id="EP_BEAR_1",
        ticker="AAA",
        entry_window_date="2024-01-05",
        exit_date="2024-01-10",
        regime="BEAR",
        regime_version=regime_version,
        feature_value=1.0,
    )
    _insert_episode(
        conn,
        episode_id="EP_BEAR_2",
        ticker="BBB",
        entry_window_date="2024-01-06",
        exit_date="2024-01-11",
        regime="BEAR",
        regime_version=regime_version,
        feature_value=-1.0,
    )
    _insert_episode(
        conn,
        episode_id="EP_BULL_1",
        ticker="CCC",
        entry_window_date="2024-01-07",
        exit_date="2024-01-12",
        regime="BULL",
        regime_version=regime_version,
        feature_value=0.5,
    )
    conn.commit()


def _write_model_artifacts(
    model_dir: Path,
    *,
    feature_order: list[str] | None = None,
    selected_candidate: str = "UP20_BEAR_HGB_V1",
) -> None:
    model_dir.mkdir(parents=True, exist_ok=True)
    feature_columns = feature_order if feature_order is not None else list(FEATURE_COLUMNS)
    (model_dir / "UP20_BEAR_FEATURE_LIST_V1.json").write_text(
        json.dumps({"feature_columns": feature_columns, "feature_count": len(feature_columns)}, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    (model_dir / "UP20_BEAR_MODEL_EVAL_V1.json").write_text(
        json.dumps(
            {
                "previous_selected_production_candidate": "UP20_BEAR_HGB_V1",
                "evaluation_recommended_candidate": selected_candidate,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    joblib.dump(DummyProbModel(), model_dir / "UP20_BEAR_HGB_V1.joblib")


def test_up20_bear_scoring_filters_to_bear_regime_only(tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_scoring_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _write_model_artifacts(model_dir)

    conn = sqlite3.connect(str(rc_db))
    summary = compute_and_store_up20_bear_scores(
        conn,
        model_dir=model_dir,
        regime_version=regime_version,
        mode="upsert",
        scored_at="2026-03-07T00:00:00+00:00",
    )
    rows = conn.execute(f"SELECT episode_id FROM {SCORE_TABLE} ORDER BY episode_id ASC").fetchall()
    conn.close()

    assert summary.episodes_eligible == 2
    assert [str(r[0]) for r in rows] == ["EP_BEAR_1", "EP_BEAR_2"]


def test_up20_bear_scoring_uses_saved_feature_order(tmp_path: Path, monkeypatch) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_scoring_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _write_model_artifacts(model_dir)
    expected_order = list(FEATURE_COLUMNS)

    class SpyModel:
        def predict_proba(self, x):  # type: ignore[no-untyped-def]
            assert list(x.columns) == expected_order
            p = np.full(len(x), 0.5, dtype=float)
            return np.column_stack([1.0 - p, p])

    from swingmaster.research import up20_bear_scoring

    monkeypatch.setattr(
        up20_bear_scoring,
        "_load_model",
        lambda _model_dir, _model_id: (SpyModel(), "HGB", str((model_dir / "UP20_BEAR_HGB_V1.joblib").resolve())),
    )

    conn = sqlite3.connect(str(rc_db))
    compute_and_store_up20_bear_scores(
        conn,
        model_dir=model_dir,
        regime_version=regime_version,
        mode="upsert",
        scored_at="2026-03-07T00:00:00+00:00",
    )
    conn.close()


def test_up20_bear_scoring_writes_probability_to_score_table(tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_scoring_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _write_model_artifacts(model_dir, selected_candidate="UP20_BEAR_HGB_V1")

    conn = sqlite3.connect(str(rc_db))
    summary = compute_and_store_up20_bear_scores(
        conn,
        model_dir=model_dir,
        regime_version=regime_version,
        mode="upsert",
        scored_at="2026-03-07T00:00:00+00:00",
    )
    rows = conn.execute(
        f"""
        SELECT episode_id, model_id, regime_used, target_name, predicted_probability
        FROM {SCORE_TABLE}
        ORDER BY episode_id ASC
        """
    ).fetchall()
    conn.close()

    assert summary.scores_inserted == 2
    assert len(rows) == 2
    assert all(str(r[1]) == "UP20_BEAR_HGB_V1" for r in rows)
    assert all(str(r[2]) == "BEAR" for r in rows)
    assert all(str(r[3]) == "UP20" for r in rows)
    assert all(0.0 <= float(r[4]) <= 1.0 for r in rows)


def test_up20_bear_scoring_cli_is_idempotent(monkeypatch, tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_scoring_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _write_model_artifacts(model_dir)

    args = type(
        "Args",
        (),
        {
            "rc_db": str(rc_db),
            "regime_version": regime_version,
            "model_dir": str(model_dir),
            "mode": "upsert",
            "date_from": None,
            "date_to": None,
            "osakedata_db": str(tmp_path / "missing_os.db"),
            "scored_at": "2026-03-07T00:00:00+00:00",
        },
    )()
    monkeypatch.setattr(run_up20_bear_score, "parse_args", lambda: args)

    run_up20_bear_score.main()
    run_up20_bear_score.main()

    conn = sqlite3.connect(str(rc_db))
    count = conn.execute(f"SELECT COUNT(*) FROM {SCORE_TABLE}").fetchone()[0]
    conn.close()
    assert count == 2


def test_up20_bear_scoring_fails_on_feature_order_mismatch(tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_scoring_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _write_model_artifacts(model_dir, feature_order=list(reversed(FEATURE_COLUMNS)))

    conn = sqlite3.connect(str(rc_db))
    with pytest.raises(RuntimeError, match="UP20_BEAR_FEATURE_ORDER_MISMATCH"):
        compute_and_store_up20_bear_scores(
            conn,
            model_dir=model_dir,
            regime_version=regime_version,
            mode="upsert",
        )
    conn.close()


def test_up20_bear_scoring_does_not_retrain_model(tmp_path: Path, monkeypatch) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_scoring_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _write_model_artifacts(model_dir)

    monkeypatch.setattr(
        up20_bear_training,
        "train_and_compare_up20_bear",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not retrain")),
    )

    conn = sqlite3.connect(str(rc_db))
    summary = compute_and_store_up20_bear_scores(
        conn,
        model_dir=model_dir,
        regime_version=regime_version,
        mode="upsert",
        scored_at="2026-03-07T00:00:00+00:00",
    )
    conn.close()

    assert summary.scores_inserted == 2
