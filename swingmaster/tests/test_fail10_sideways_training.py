from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np

from swingmaster.cli import run_fail10_sideways_training
from swingmaster.episode_exit_features.production import FEATURE_COLUMNS
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.research import fail10_sideways_training
from swingmaster.research.fail10_sideways_training import (
    MODEL_ID_CATBOOST,
    MODEL_ID_HGB,
    load_fail10_sideways_dataset,
    select_preferred_candidate,
    train_and_compare_fail10_sideways,
)


def _create_pipeline_episode_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE rc_pipeline_episode (
          episode_id TEXT PRIMARY KEY,
          ticker TEXT NOT NULL,
          entry_window_exit_date TEXT,
          post60_growth_pct_close_ew_exit_to_peak REAL
        )
        """
    )


def _insert_episode(
    conn: sqlite3.Connection,
    *,
    episode_id: str,
    ticker: str,
    exit_date: str,
    growth: float,
    regime: str,
    regime_version: str,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_pipeline_episode (
          episode_id, ticker, entry_window_exit_date, post60_growth_pct_close_ew_exit_to_peak
        ) VALUES (?, ?, ?, ?)
        """,
        (episode_id, ticker, exit_date, growth),
    )
    cols = [
        "episode_id",
        "ticker",
        "entry_window_date",
        "entry_window_exit_date",
        "as_of_date",
        "computed_at",
        *FEATURE_COLUMNS,
    ]
    placeholders = ", ".join("?" for _ in cols)
    values: list[object] = [
        episode_id,
        ticker,
        "2020-01-01",
        exit_date,
        exit_date,
        "2026-03-07T00:00:00+00:00",
    ]
    base = 1.0 if growth < 10.0 else -1.0
    for i, _ in enumerate(FEATURE_COLUMNS, start=1):
        values.append(base + i * 0.001)
    conn.execute(
        f"INSERT INTO rc_episode_exit_features ({', '.join(cols)}) VALUES ({placeholders})",
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
            "2020-01-01",
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


def _seed_train_valid_test_sideways_dataset(conn: sqlite3.Connection, regime_version: str) -> None:
    _create_pipeline_episode_table(conn)
    rows = [
        ("EP2020A", "T2020A", "2020-01-10", 5.0, "SIDEWAYS"),
        ("EP2020B", "T2020B", "2020-02-10", 30.0, "SIDEWAYS"),
        ("EP2021A", "T2021A", "2021-01-10", 5.0, "SIDEWAYS"),
        ("EP2021B", "T2021B", "2021-02-10", 30.0, "SIDEWAYS"),
        ("EP2022A", "T2022A", "2022-01-10", 5.0, "SIDEWAYS"),
        ("EP2022B", "T2022B", "2022-02-10", 30.0, "SIDEWAYS"),
        ("EP2023A", "T2023A", "2023-01-10", 5.0, "SIDEWAYS"),
        ("EP2023B", "T2023B", "2023-02-10", 30.0, "SIDEWAYS"),
        ("EP2023C", "T2023C", "2023-03-10", 5.0, "SIDEWAYS"),
        ("EP2023D", "T2023D", "2023-04-10", 30.0, "SIDEWAYS"),
        ("EP2024A", "T2024A", "2024-01-10", 5.0, "SIDEWAYS"),
        ("EP2024B", "T2024B", "2024-02-10", 30.0, "SIDEWAYS"),
        ("EP2025A", "T2025A", "2025-01-10", 5.0, "SIDEWAYS"),
        ("EP2025B", "T2025B", "2025-02-10", 30.0, "SIDEWAYS"),
        ("EPBULL1", "TBULL1", "2023-05-10", 5.0, "BULL"),
        ("EPBULL2", "TBULL2", "2024-05-10", 30.0, "BULL"),
    ]
    for episode_id, ticker, exit_date, growth, regime in rows:
        _insert_episode(
            conn,
            episode_id=episode_id,
            ticker=ticker,
            exit_date=exit_date,
            growth=growth,
            regime=regime,
            regime_version=regime_version,
        )
    conn.commit()


def test_fail10_sideways_dataset_filters_to_sideways_regime_only() -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_train_valid_test_sideways_dataset(conn, regime_version)
    ds = load_fail10_sideways_dataset(conn, regime_version=regime_version)
    assert not ds.frame.empty
    assert set(ds.frame["episode_id"].tolist()) == {
        "EP2020A",
        "EP2020B",
        "EP2021A",
        "EP2021B",
        "EP2022A",
        "EP2022B",
        "EP2023A",
        "EP2023B",
        "EP2023C",
        "EP2023D",
        "EP2024A",
        "EP2024B",
        "EP2025A",
        "EP2025B",
    }
    conn.close()


def test_fail10_sideways_dataset_uses_year_split_by_ew_exit_date() -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_train_valid_test_sideways_dataset(conn, regime_version)
    ds = load_fail10_sideways_dataset(conn, regime_version=regime_version)
    split_map = dict(ds.frame[["episode_id", "split_bucket"]].itertuples(index=False, name=None))
    assert split_map["EP2020A"] == "train"
    assert split_map["EP2021A"] == "train"
    assert split_map["EP2022A"] == "train"
    assert split_map["EP2023A"] == "valid"
    assert split_map["EP2024A"] == "test"
    assert split_map["EP2025A"] == "test"
    conn.close()


def test_fail10_sideways_training_excludes_non_feature_columns(tmp_path: Path, monkeypatch) -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_train_valid_test_sideways_dataset(conn, regime_version)

    class _FakeModel:
        def fit(self, x, y):  # type: ignore[no-untyped-def]
            return self

        def predict_proba(self, x):  # type: ignore[no-untyped-def]
            p = np.full(len(x), 0.5, dtype=float)
            return np.column_stack([1.0 - p, p])

        def save_model(self, path: str) -> None:
            Path(path).write_text("fake-catboost", encoding="utf-8")

    def _fit_catboost_spy(x, y):  # type: ignore[no-untyped-def]
        assert list(x.columns) == FEATURE_COLUMNS
        assert "episode_id" not in x.columns
        assert "entry_window_exit_date" not in x.columns
        assert "label_fail10" not in x.columns
        return _FakeModel()

    def _fit_hgb_spy(x, y):  # type: ignore[no-untyped-def]
        assert list(x.columns) == FEATURE_COLUMNS
        return _FakeModel()

    monkeypatch.setattr(fail10_sideways_training, "_fit_catboost", _fit_catboost_spy)
    monkeypatch.setattr(fail10_sideways_training, "_fit_hgb", _fit_hgb_spy)
    monkeypatch.setattr(
        fail10_sideways_training.joblib,
        "dump",
        lambda model, path: Path(path).write_text("fake-hgb", encoding="utf-8"),
    )

    metrics = train_and_compare_fail10_sideways(
        conn,
        out_dir=tmp_path / "out",
        regime_version=regime_version,
        computed_at="2026-03-07T00:00:00+00:00",
    )
    assert metrics.feature_count == len(FEATURE_COLUMNS)
    conn.close()


def test_fail10_sideways_training_cli_runs_and_saves_both_artifacts(monkeypatch, tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    out_dir = tmp_path / "artifacts"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_train_valid_test_sideways_dataset(conn, regime_version)
    conn.close()

    monkeypatch.setattr(
        run_fail10_sideways_training,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "rc_db": str(rc_db),
                "regime_version": regime_version,
                "out_dir": str(out_dir),
                "computed_at": "2026-03-07T00:00:00+00:00",
            },
        )(),
    )
    run_fail10_sideways_training.main()
    assert (out_dir / f"{MODEL_ID_CATBOOST}.cbm").exists()
    assert (out_dir / f"{MODEL_ID_HGB}.joblib").exists()
    assert (out_dir / f"{MODEL_ID_CATBOOST}.meta.json").exists()
    assert (out_dir / f"{MODEL_ID_HGB}.meta.json").exists()
    assert (out_dir / "FAIL10_SIDEWAYS_FEATURE_LIST_V1.json").exists()
    comparison_path = out_dir / "FAIL10_SIDEWAYS_MODEL_COMPARISON_V1.json"
    assert comparison_path.exists()
    comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
    assert comparison["selected_production_candidate"] in {MODEL_ID_CATBOOST, MODEL_ID_HGB}


def test_fail10_sideways_training_comparison_selects_preferred_model_deterministically() -> None:
    winner = select_preferred_candidate(
        auc_test_catboost=0.75,
        auc_test_hgb=0.72,
        auc_valid_catboost=0.70,
        auc_valid_hgb=0.80,
    )
    assert winner == MODEL_ID_CATBOOST
    tie_test_winner = select_preferred_candidate(
        auc_test_catboost=0.75,
        auc_test_hgb=0.75,
        auc_valid_catboost=0.71,
        auc_valid_hgb=0.73,
    )
    assert tie_test_winner == MODEL_ID_HGB
    full_tie_winner = select_preferred_candidate(
        auc_test_catboost=0.75,
        auc_test_hgb=0.75,
        auc_valid_catboost=0.73,
        auc_valid_hgb=0.73,
    )
    assert full_tie_winner == MODEL_ID_CATBOOST


def test_fail10_sideways_training_is_deterministic_given_same_inputs(tmp_path: Path) -> None:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_train_valid_test_sideways_dataset(conn, regime_version)
    m1 = train_and_compare_fail10_sideways(
        conn,
        out_dir=tmp_path / "run1",
        regime_version=regime_version,
        computed_at="2026-03-07T00:00:00+00:00",
    )
    m2 = train_and_compare_fail10_sideways(
        conn,
        out_dir=tmp_path / "run2",
        regime_version=regime_version,
        computed_at="2026-03-07T00:00:00+00:00",
    )
    conn.close()
    assert m1 == m2
