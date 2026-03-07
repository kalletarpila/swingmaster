from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import numpy as np
import pytest

from swingmaster.cli import run_fail10_bull_eval
from swingmaster.episode_exit_features.production import FEATURE_COLUMNS
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.research.fail10_bull_evaluation import (
    EVAL_FILE_NAME,
    _compute_threshold_metrics,
    _compute_top_bucket_metrics,
    _recommend_candidate,
    evaluate_fail10_bull_models,
)
from swingmaster.research.fail10_bull_training import (
    load_fail10_bull_dataset,
    train_and_compare_fail10_bull,
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
        f"INSERT INTO rc_episode_exit_features ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
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


def _seed_dataset(conn: sqlite3.Connection, regime_version: str) -> None:
    _create_pipeline_episode_table(conn)
    rows = [
        ("EP2020A", "T2020A", "2020-01-10", 5.0, "BULL"),
        ("EP2020B", "T2020B", "2020-02-10", 30.0, "BULL"),
        ("EP2021A", "T2021A", "2021-01-10", 5.0, "BULL"),
        ("EP2021B", "T2021B", "2021-02-10", 30.0, "BULL"),
        ("EP2022A", "T2022A", "2022-01-10", 5.0, "BULL"),
        ("EP2022B", "T2022B", "2022-02-10", 30.0, "BULL"),
        ("EP2023A", "T2023A", "2023-01-10", 5.0, "BULL"),
        ("EP2023B", "T2023B", "2023-02-10", 30.0, "BULL"),
        ("EP2023C", "T2023C", "2023-03-10", 5.0, "BULL"),
        ("EP2023D", "T2023D", "2023-04-10", 30.0, "BULL"),
        ("EP2024A", "T2024A", "2024-01-10", 5.0, "BULL"),
        ("EP2024B", "T2024B", "2024-02-10", 30.0, "BULL"),
        ("EP2025A", "T2025A", "2025-01-10", 5.0, "BULL"),
        ("EP2025B", "T2025B", "2025-02-10", 30.0, "BULL"),
        ("EPBEAR1", "TBEAR1", "2023-05-10", 5.0, "BEAR"),
        ("EPBEAR2", "TBEAR2", "2024-05-10", 30.0, "BEAR"),
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


def _prepare_models(rc_db: Path, model_dir: Path, regime_version: str) -> None:
    conn = sqlite3.connect(str(rc_db))
    train_and_compare_fail10_bull(
        conn,
        out_dir=model_dir,
        regime_version=regime_version,
        computed_at="2026-03-07T00:00:00+00:00",
    )
    conn.close()


def test_fail10_bull_eval_uses_same_bull_only_dataset_logic(tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _prepare_models(rc_db, model_dir, regime_version)

    conn = sqlite3.connect(str(rc_db))
    eval_summary = evaluate_fail10_bull_models(
        conn,
        model_dir=model_dir,
        regime_version=regime_version,
        computed_at="2026-03-07T00:00:00+00:00",
    )
    ds = load_fail10_bull_dataset(conn, regime_version=regime_version)
    conn.close()

    expected_valid = int((ds.frame["split_bucket"] == "valid").sum())
    expected_test = int((ds.frame["split_bucket"] == "test").sum())
    assert eval_summary.n_valid == expected_valid
    assert eval_summary.n_test == expected_test
    assert eval_summary.n_valid == 4
    assert eval_summary.n_test == 4


def test_fail10_bull_eval_uses_same_feature_order_as_training(tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _prepare_models(rc_db, model_dir, regime_version)

    feature_path = model_dir / "FAIL10_BULL_FEATURE_LIST_V1.json"
    payload = json.loads(feature_path.read_text(encoding="utf-8"))
    payload["feature_columns"] = list(reversed(payload["feature_columns"]))
    feature_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    conn = sqlite3.connect(str(rc_db))
    with pytest.raises(RuntimeError, match="FAIL10_BULL_FEATURE_ORDER_MISMATCH"):
        evaluate_fail10_bull_models(
            conn,
            model_dir=model_dir,
            regime_version=regime_version,
        )
    conn.close()


def test_fail10_bull_eval_computes_threshold_metrics() -> None:
    y = np.array([1, 0, 1, 0, 1], dtype=int)
    score = np.array([0.90, 0.65, 0.55, 0.20, 0.80], dtype=float)
    growth = np.array([6.0, 30.0, 8.0, -4.0, 5.0], dtype=float)
    base_rate = float(np.mean(y))
    out = _compute_threshold_metrics(y=y, score=score, growth=growth, base_rate=base_rate)
    m = out["0.60"]
    assert m["selected_count"] == 3
    assert m["selection_rate"] == pytest.approx(0.6, abs=1e-12)
    assert m["precision"] == pytest.approx(2.0 / 3.0, abs=1e-12)
    assert m["lift"] == pytest.approx((2.0 / 3.0) / base_rate, abs=1e-12)


def test_fail10_bull_eval_computes_top_bucket_metrics() -> None:
    y = np.array([1, 0, 1, 0, 1, 0, 1, 0, 1, 0], dtype=int)
    score = np.array([0.99, 0.95, 0.80, 0.10, 0.40, 0.30, 0.70, 0.20, 0.60, 0.50], dtype=float)
    growth = np.array([4.0, 60.0, 7.0, -5.0, 9.0, 20.0, 8.0, 3.0, 6.0, 35.0], dtype=float)
    out = _compute_top_bucket_metrics(y=y, score=score, growth=growth)
    top10 = out["top_10pct"]
    top20 = out["top_20pct"]
    assert top10["selected_count"] == 1
    assert top10["selection_rate"] == pytest.approx(0.1, abs=1e-12)
    assert top10["fail10_rate"] == pytest.approx(1.0, abs=1e-12)
    assert top20["selected_count"] == 2
    assert top20["selection_rate"] == pytest.approx(0.2, abs=1e-12)
    assert top20["fail10_rate"] == pytest.approx(0.5, abs=1e-12)


def test_fail10_bull_eval_cli_runs_and_writes_eval_json(monkeypatch, tmp_path: Path) -> None:
    rc_db = tmp_path / "rc.db"
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    regime_version = "REGIME_TEST_V1"
    _seed_dataset(conn, regime_version)
    conn.close()

    model_dir = tmp_path / "models"
    _prepare_models(rc_db, model_dir, regime_version)
    out_dir = tmp_path / "eval"

    monkeypatch.setattr(
        run_fail10_bull_eval,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "rc_db": str(rc_db),
                "regime_version": regime_version,
                "model_dir": str(model_dir),
                "out_dir": str(out_dir),
                "computed_at": "2026-03-07T00:00:00+00:00",
            },
        )(),
    )
    run_fail10_bull_eval.main()
    assert (out_dir / EVAL_FILE_NAME).exists()


def test_fail10_bull_eval_recommendation_is_deterministic() -> None:
    kwargs = {
        "previous_selected": "FAIL10_BULL_CATBOOST_V1",
        "auc_test_catboost": 0.70,
        "auc_test_hgb": 0.72,
        "top10_fail10_catboost": 0.85,
        "top10_fail10_hgb": 0.80,
        "avg_growth_top10_catboost": 4.0,
        "avg_growth_top10_hgb": 8.0,
        "gt50_top10_catboost": 0.05,
        "gt50_top10_hgb": 0.10,
    }
    first = _recommend_candidate(**kwargs)
    second = _recommend_candidate(**kwargs)
    assert first == second
    assert first[0] == "FAIL10_BULL_CATBOOST_V1"

