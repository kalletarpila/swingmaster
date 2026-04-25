from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_score_explain
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_score_explain import build_explain_rows, format_explain_output, load_rows_for_explain
from swingmaster.fundamentals.score import run_fundamental_scoring


def test_explain_output_for_one_mature_row(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_explain_one.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE", None)
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        rows = load_rows_for_explain(conn, "AAPL", None)
        explain_rows = build_explain_rows(rows)
    out = format_explain_output("AAPL", explain_rows)
    assert "FUNDAMENTAL SCORE EXPLAIN" in out
    assert "growth_component" in out
    assert "margin_component" in out
    assert "stored_fundamental_score" in out
    assert "recomputed_fundamental_score" in out
    assert explain_rows[0]["stored_fundamental_score"] == explain_rows[0]["recomputed_fundamental_score"]


def test_limit_keeps_latest_rows_in_ascending_order(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_explain_limit.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-06-30", None, 0.32, None, 0.28, 0.30, None, "MATURE", 71.0)
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.25, 0.05, 0.02, 0.01, None, 0.01, "GROWTH", 50.0)
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.40, 0.30, 0.05, 0.25, -0.10, -0.03, "MATURE", 100.0)
        conn.commit()
        rows = load_rows_for_explain(conn, "AAPL", 2)
    as_of_dates = [row["as_of_date"] for row in rows]
    assert as_of_dates == ["2025-09-30", "2025-12-31"]


def test_limit_uses_full_history_for_consistency_recompute(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_explain_limit_full_history.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2024-03-31", 0.18, 0.20, 0.01, 0.10, 1.0, 0.01, "MATURE", None)
        _insert_ttm_row(conn, "AAPL", "2024-06-30", 0.20, 0.22, 0.02, 0.11, 1.0, 0.01, "MATURE", None)
        _insert_ttm_row(conn, "AAPL", "2024-09-30", 0.21, 0.23, 0.03, 0.12, 1.0, 0.01, "MATURE", None)
        _insert_ttm_row(conn, "AAPL", "2024-12-31", 0.19, 0.24, 0.04, 0.13, 1.0, 0.01, "MATURE", None)
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.22, 0.25, 0.05, 0.14, 1.0, 0.01, "MATURE", None)
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.24, 0.26, 0.06, 0.15, 1.0, 0.01, "MATURE", None)
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.23, 0.27, 0.07, 0.16, 1.0, 0.01, "MATURE", None)
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.25, 0.28, 0.08, 0.17, 1.0, 0.01, "MATURE", None)
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        rows = load_rows_for_explain(conn, "AAPL", 5)
        explain_rows = build_explain_rows(rows)
    assert [row["as_of_date"] for row in explain_rows] == [
        "2024-12-31",
        "2025-03-31",
        "2025-06-30",
        "2025-09-30",
        "2025-12-31",
    ]
    out = format_explain_output("AAPL", explain_rows)
    assert "2024-09-30" not in out
    assert "2024-06-30" not in out


def test_stored_score_null(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_explain_null.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE", None)
        conn.commit()
        explain_rows = build_explain_rows(load_rows_for_explain(conn, "AAPL", None))
    out = format_explain_output("AAPL", explain_rows)
    assert "stored_fundamental_score" in out
    assert "NULL" in out
    assert "recomputed_fundamental_score" in out


def test_mismatch_detection(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_explain_mismatch.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE", 72.0)
        conn.commit()
        with pytest.raises(RuntimeError, match="^FUNDAMENTAL_SCORE_MISMATCH:AAPL:2025-12-31$"):
            build_explain_rows(load_rows_for_explain(conn, "AAPL", None))


def test_no_ttm_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_explain_empty.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(RuntimeError, match="^FUNDAMENTAL_TTM_NOT_FOUND:AAPL$"):
            load_rows_for_explain(conn, "AAPL", None)


def test_read_only_behavior(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_explain_read_only.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE", None)
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        before = conn.execute(
            "SELECT lifecycle_class, fundamental_score FROM rc_fundamental_ttm WHERE ticker='AAPL'"
        ).fetchone()
        explain_rows = build_explain_rows(load_rows_for_explain(conn, "AAPL", None))
        _ = format_explain_output("AAPL", explain_rows)
        after = conn.execute(
            "SELECT lifecycle_class, fundamental_score FROM rc_fundamental_ttm WHERE ticker='AAPL'"
        ).fetchone()
    assert before == after


def test_cli_output_and_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_explain_cli.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE", None)
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)

    monkeypatch.setattr(
        run_fundamental_score_explain,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(db_path),
                "ticker": "AAPL",
                "limit": None,
            },
        )(),
    )

    run_fundamental_score_explain.main()
    out = capsys.readouterr().out
    assert "FUNDAMENTAL SCORE EXPLAIN" in out
    assert "SUMMARY rows_explained=1" in out
    assert "SUMMARY mismatches=0" in out


def _insert_ttm_row(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    revenue_growth_ttm_yoy: float | None,
    ebit_margin_ttm: float | None,
    ebit_margin_trend_4q: float | None,
    fcf_margin_ttm: float | None,
    net_debt_to_ebitda: float | None,
    share_dilution_yoy: float | None,
    lifecycle_class: str | None,
    fundamental_score: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_ttm (
            ticker,
            as_of_date,
            latest_period_end_date,
            revenue_ttm,
            revenue_growth_ttm_yoy,
            ebit_ttm,
            ebit_growth_ttm_yoy,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            gross_margin_trend_4q,
            fcf_ttm,
            fcf_margin_ttm,
            fcf_margin_trend_4q,
            net_debt,
            net_debt_to_ebitda,
            share_dilution_yoy,
            lifecycle_class,
            fundamental_score,
            run_id
        ) VALUES (?, ?, ?, 1000.0, ?, NULL, NULL, ?, ?, NULL, NULL, ?, NULL, NULL, ?, ?, ?, ?, 'TTM_RUN_V1')
        """,
        (
            ticker,
            as_of_date,
            as_of_date,
            revenue_growth_ttm_yoy,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            fcf_margin_ttm,
            net_debt_to_ebitda,
            share_dilution_yoy,
            lifecycle_class,
            fundamental_score,
        ),
    )
