from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli import run_sideways_model_scores
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.research.fail10_sideways_scoring import Fail10SidewaysScoreSummary
from swingmaster.research.up20_bull_scoring import SCORE_TABLE
from swingmaster.research.up20_sideways_scoring import Up20SidewaysScoreSummary


def _insert_score_row(
    conn: sqlite3.Connection,
    *,
    episode_id: str,
    model_id: str,
    ticker: str,
    exit_date: str,
    prob: float,
    target_name: str,
) -> None:
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {SCORE_TABLE} (
          episode_id,
          model_id,
          ticker,
          entry_window_date,
          entry_window_exit_date,
          as_of_date,
          regime_used,
          model_family,
          target_name,
          feature_version,
          regime_version,
          artifact_path,
          predicted_probability,
          scored_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            episode_id,
            model_id,
            ticker,
            "2024-01-01",
            exit_date,
            exit_date,
            "SIDEWAYS",
            "HGB",
            target_name,
            "EPISODE_EXIT_FEATURES_V1",
            "REGIME_TEST_V1",
            "/tmp/model",
            prob,
            "2026-03-07T00:00:00+00:00",
        ),
    )


def _parse_summary(output: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in output.splitlines():
        if not line.startswith("SUMMARY "):
            continue
        payload = line[len("SUMMARY ") :]
        if "=" not in payload:
            continue
        key, value = payload.split("=", 1)
        out[key] = value
    return out


def test_run_sideways_model_scores_reuses_existing_up20_and_fail10_scoring(
    monkeypatch,
    tmp_path: Path,
) -> None:
    rc_db = tmp_path / "rc.db"
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    conn.close()

    calls: dict[str, int] = {"up20": 0, "fail10": 0}

    def _up20_stub(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["up20"] += 1
        return Up20SidewaysScoreSummary(0, 0, 0, 0, 0, "UP20_SIDEWAYS_HGB_V1", "SIDEWAYS", 61)

    def _fail10_stub(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls["fail10"] += 1
        return Fail10SidewaysScoreSummary(
            0, 0, 0, 0, 0, "FAIL10_SIDEWAYS_CATBOOST_V1", "SIDEWAYS", 61
        )

    monkeypatch.setattr(run_sideways_model_scores, "compute_and_store_up20_sideways_scores", _up20_stub)
    monkeypatch.setattr(run_sideways_model_scores, "compute_and_store_fail10_sideways_scores", _fail10_stub)
    monkeypatch.setattr(
        run_sideways_model_scores,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "rc_db": str(rc_db),
                "regime_version": "REGIME_TEST_V1",
                "model_dir": str(model_dir),
                "mode": "upsert",
                "date_from": None,
                "date_to": None,
                "osakedata_db": str(tmp_path / "os.db"),
                "scored_at": "2026-03-07T00:00:00+00:00",
                "up20_min_prob": None,
                "fail10_max_prob": None,
            },
        )(),
    )

    run_sideways_model_scores.main()
    assert calls == {"up20": 1, "fail10": 1}


def test_run_sideways_model_scores_reports_both_score_counts(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    rc_db = tmp_path / "rc.db"
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    _insert_score_row(
        conn,
        episode_id="EP1",
        model_id="UP20_SIDEWAYS_HGB_V1",
        ticker="AAA",
        exit_date="2024-01-10",
        prob=0.70,
        target_name="UP20",
    )
    _insert_score_row(
        conn,
        episode_id="EP1",
        model_id="FAIL10_SIDEWAYS_CATBOOST_V1",
        ticker="AAA",
        exit_date="2024-01-10",
        prob=0.20,
        target_name="FAIL10",
    )
    _insert_score_row(
        conn,
        episode_id="EP2",
        model_id="UP20_SIDEWAYS_HGB_V1",
        ticker="BBB",
        exit_date="2024-01-11",
        prob=0.65,
        target_name="UP20",
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        run_sideways_model_scores,
        "compute_and_store_up20_sideways_scores",
        lambda *args, **kwargs: Up20SidewaysScoreSummary(
            0, 0, 0, 0, 0, "UP20_SIDEWAYS_HGB_V1", "SIDEWAYS", 61
        ),
    )
    monkeypatch.setattr(
        run_sideways_model_scores,
        "compute_and_store_fail10_sideways_scores",
        lambda *args, **kwargs: Fail10SidewaysScoreSummary(
            0, 0, 0, 0, 0, "FAIL10_SIDEWAYS_CATBOOST_V1", "SIDEWAYS", 61
        ),
    )
    monkeypatch.setattr(
        run_sideways_model_scores,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "rc_db": str(rc_db),
                "regime_version": "REGIME_TEST_V1",
                "model_dir": str(model_dir),
                "mode": "upsert",
                "date_from": None,
                "date_to": None,
                "osakedata_db": str(tmp_path / "os.db"),
                "scored_at": "2026-03-07T00:00:00+00:00",
                "up20_min_prob": None,
                "fail10_max_prob": None,
            },
        )(),
    )
    run_sideways_model_scores.main()
    summary = _parse_summary(capsys.readouterr().out)
    assert summary["up20_scores_total"] == "2"
    assert summary["fail10_scores_total"] == "1"
    assert summary["episodes_with_both_scores"] == "1"


def test_run_sideways_model_scores_applies_up20_and_fail10_thresholds_correctly(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    rc_db = tmp_path / "rc.db"
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    # EP1 passes both, EP2 fails FAIL10, EP3 fails UP20.
    _insert_score_row(
        conn,
        episode_id="EP1",
        model_id="UP20_SIDEWAYS_HGB_V1",
        ticker="AAA",
        exit_date="2024-01-10",
        prob=0.70,
        target_name="UP20",
    )
    _insert_score_row(
        conn,
        episode_id="EP1",
        model_id="FAIL10_SIDEWAYS_CATBOOST_V1",
        ticker="AAA",
        exit_date="2024-01-10",
        prob=0.20,
        target_name="FAIL10",
    )
    _insert_score_row(
        conn,
        episode_id="EP2",
        model_id="UP20_SIDEWAYS_HGB_V1",
        ticker="BBB",
        exit_date="2024-01-11",
        prob=0.80,
        target_name="UP20",
    )
    _insert_score_row(
        conn,
        episode_id="EP2",
        model_id="FAIL10_SIDEWAYS_CATBOOST_V1",
        ticker="BBB",
        exit_date="2024-01-11",
        prob=0.50,
        target_name="FAIL10",
    )
    _insert_score_row(
        conn,
        episode_id="EP3",
        model_id="UP20_SIDEWAYS_HGB_V1",
        ticker="CCC",
        exit_date="2024-01-12",
        prob=0.55,
        target_name="UP20",
    )
    _insert_score_row(
        conn,
        episode_id="EP3",
        model_id="FAIL10_SIDEWAYS_CATBOOST_V1",
        ticker="CCC",
        exit_date="2024-01-12",
        prob=0.10,
        target_name="FAIL10",
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        run_sideways_model_scores,
        "compute_and_store_up20_sideways_scores",
        lambda *args, **kwargs: Up20SidewaysScoreSummary(
            0, 0, 0, 0, 0, "UP20_SIDEWAYS_HGB_V1", "SIDEWAYS", 61
        ),
    )
    monkeypatch.setattr(
        run_sideways_model_scores,
        "compute_and_store_fail10_sideways_scores",
        lambda *args, **kwargs: Fail10SidewaysScoreSummary(
            0, 0, 0, 0, 0, "FAIL10_SIDEWAYS_CATBOOST_V1", "SIDEWAYS", 61
        ),
    )
    monkeypatch.setattr(
        run_sideways_model_scores,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "rc_db": str(rc_db),
                "regime_version": "REGIME_TEST_V1",
                "model_dir": str(model_dir),
                "mode": "upsert",
                "date_from": None,
                "date_to": None,
                "osakedata_db": str(tmp_path / "os.db"),
                "scored_at": "2026-03-07T00:00:00+00:00",
                "up20_min_prob": 0.60,
                "fail10_max_prob": 0.35,
            },
        )(),
    )
    run_sideways_model_scores.main()
    summary = _parse_summary(capsys.readouterr().out)
    assert summary["episodes_passing_up20_threshold"] == "2"
    assert summary["episodes_passing_fail10_threshold"] == "2"
    assert summary["episodes_passing_both_thresholds"] == "1"


def test_run_sideways_model_scores_is_idempotent(monkeypatch, tmp_path: Path, capsys) -> None:
    rc_db = tmp_path / "rc.db"
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    conn.close()

    def _up20_writer(conn, **kwargs):  # type: ignore[no-untyped-def]
        _insert_score_row(
            conn,
            episode_id="EP1",
            model_id="UP20_SIDEWAYS_HGB_V1",
            ticker="AAA",
            exit_date="2024-01-10",
            prob=0.70,
            target_name="UP20",
        )
        conn.commit()
        return Up20SidewaysScoreSummary(0, 0, 1, 0, 0, "UP20_SIDEWAYS_HGB_V1", "SIDEWAYS", 61)

    def _fail10_writer(conn, **kwargs):  # type: ignore[no-untyped-def]
        _insert_score_row(
            conn,
            episode_id="EP1",
            model_id="FAIL10_SIDEWAYS_CATBOOST_V1",
            ticker="AAA",
            exit_date="2024-01-10",
            prob=0.20,
            target_name="FAIL10",
        )
        conn.commit()
        return Fail10SidewaysScoreSummary(
            0, 0, 1, 0, 0, "FAIL10_SIDEWAYS_CATBOOST_V1", "SIDEWAYS", 61
        )

    monkeypatch.setattr(run_sideways_model_scores, "compute_and_store_up20_sideways_scores", _up20_writer)
    monkeypatch.setattr(run_sideways_model_scores, "compute_and_store_fail10_sideways_scores", _fail10_writer)

    args = type(
        "Args",
        (),
        {
            "rc_db": str(rc_db),
            "regime_version": "REGIME_TEST_V1",
            "model_dir": str(model_dir),
            "mode": "upsert",
            "date_from": None,
            "date_to": None,
            "osakedata_db": str(tmp_path / "os.db"),
            "scored_at": "2026-03-07T00:00:00+00:00",
            "up20_min_prob": None,
            "fail10_max_prob": None,
        },
    )()
    monkeypatch.setattr(run_sideways_model_scores, "parse_args", lambda: args)

    run_sideways_model_scores.main()
    _ = _parse_summary(capsys.readouterr().out)
    run_sideways_model_scores.main()
    summary2 = _parse_summary(capsys.readouterr().out)

    conn = sqlite3.connect(str(rc_db))
    rows = conn.execute(f"SELECT COUNT(*) FROM {SCORE_TABLE}").fetchone()[0]
    conn.close()
    assert rows == 2
    assert summary2["episodes_with_both_scores"] == "1"


def test_run_sideways_model_scores_respects_date_filters(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    rc_db = tmp_path / "rc.db"
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(rc_db))
    apply_migrations(conn)
    _insert_score_row(
        conn,
        episode_id="EP1",
        model_id="UP20_SIDEWAYS_HGB_V1",
        ticker="AAA",
        exit_date="2024-01-10",
        prob=0.70,
        target_name="UP20",
    )
    _insert_score_row(
        conn,
        episode_id="EP1",
        model_id="FAIL10_SIDEWAYS_CATBOOST_V1",
        ticker="AAA",
        exit_date="2024-01-10",
        prob=0.20,
        target_name="FAIL10",
    )
    _insert_score_row(
        conn,
        episode_id="EP2",
        model_id="UP20_SIDEWAYS_HGB_V1",
        ticker="BBB",
        exit_date="2024-03-10",
        prob=0.70,
        target_name="UP20",
    )
    _insert_score_row(
        conn,
        episode_id="EP2",
        model_id="FAIL10_SIDEWAYS_CATBOOST_V1",
        ticker="BBB",
        exit_date="2024-03-10",
        prob=0.20,
        target_name="FAIL10",
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        run_sideways_model_scores,
        "compute_and_store_up20_sideways_scores",
        lambda *args, **kwargs: Up20SidewaysScoreSummary(
            0, 0, 0, 0, 0, "UP20_SIDEWAYS_HGB_V1", "SIDEWAYS", 61
        ),
    )
    monkeypatch.setattr(
        run_sideways_model_scores,
        "compute_and_store_fail10_sideways_scores",
        lambda *args, **kwargs: Fail10SidewaysScoreSummary(
            0, 0, 0, 0, 0, "FAIL10_SIDEWAYS_CATBOOST_V1", "SIDEWAYS", 61
        ),
    )
    monkeypatch.setattr(
        run_sideways_model_scores,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "rc_db": str(rc_db),
                "regime_version": "REGIME_TEST_V1",
                "model_dir": str(model_dir),
                "mode": "upsert",
                "date_from": "2024-01-01",
                "date_to": "2024-01-31",
                "osakedata_db": str(tmp_path / "os.db"),
                "scored_at": "2026-03-07T00:00:00+00:00",
                "up20_min_prob": None,
                "fail10_max_prob": None,
            },
        )(),
    )
    run_sideways_model_scores.main()
    summary = _parse_summary(capsys.readouterr().out)
    assert summary["episodes_with_both_scores"] == "1"
    assert summary["up20_scores_total"] == "1"
    assert summary["fail10_scores_total"] == "1"
