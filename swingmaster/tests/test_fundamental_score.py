from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_score
from swingmaster.cli.run_fundamental_score import main as score_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.score import run_fundamental_scoring


def test_score_mature_high_quality(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_mature.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(
            conn,
            ticker="AAPL",
            as_of_date="2025-12-31",
            revenue_growth_ttm_yoy=None,
            ebit_margin_ttm=0.32,
            ebit_margin_trend_4q=None,
            fcf_margin_ttm=0.28,
            net_debt_to_ebitda=0.30,
            share_dilution_yoy=None,
            lifecycle_class="MATURE",
        )
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert score == 71.0


def test_score_startup(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_startup.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.45, -0.10, 0.03, -0.20, None, 0.08, "STARTUP")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert score == 33.0


def test_score_distressed_clamps_at_zero(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_distressed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", -0.20, -0.50, -0.20, -0.40, 5.0, 0.10, "DISTRESSED")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert score == 0.0


def test_score_max_score_case(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_max.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.40, 0.30, 0.05, 0.25, -0.10, -0.03, "MATURE")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert score == 100.0


def test_score_dry_run_does_not_update_db(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_dry_run.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        conn.commit()
        rows_scored, min_score, max_score, avg_score = run_fundamental_scoring(conn, "AAPL", dry_run=True)
        assert rows_scored == 1
        assert min_score == 71.0
        assert max_score == 71.0
        assert avg_score == 71.0
        score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert score is None


def test_score_lifecycle_class_untouched(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_untouched.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "MATURE"


def test_score_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_idempotent.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute("SELECT COUNT(*), fundamental_score FROM rc_fundamental_ttm").fetchone()
        assert row == (1, 71.0)


def test_score_all_tickers_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_all.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        _insert_ttm_row(conn, "MSFT", "2025-12-31", 0.25, 0.05, 0.02, 0.01, None, 0.01, "GROWTH")
        conn.commit()
        rows_scored, min_score, max_score, avg_score = run_fundamental_scoring(conn, None, dry_run=False)
        assert rows_scored == 2
        assert min_score is not None
        assert max_score is not None
        assert avg_score is not None
        row_count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_ttm WHERE fundamental_score IS NOT NULL"
        ).fetchone()[0]
        assert row_count == 2


def test_score_raises_when_no_ttm_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_empty.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(RuntimeError, match="^FUNDAMENTAL_TTM_NOT_FOUND$"):
            run_fundamental_scoring(conn, None, dry_run=False)


def test_cli_score_summary_all(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_cli.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        _insert_ttm_row(conn, "MSFT", "2025-12-31", 0.25, 0.05, 0.02, 0.01, None, 0.01, "GROWTH")
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_score,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(db_path),
                "ticker": None,
                "run_id": "FUND_SCORE_USA_V1",
                "dry_run": True,
            },
        )(),
    )

    score_main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY rule_id=FUND_SCORE_RULE_V1",
        "SUMMARY ticker=ALL",
        "SUMMARY rows_scored=2",
        "SUMMARY min_score=50.0",
        "SUMMARY max_score=71.0",
        "SUMMARY avg_score=60.5",
        f"SUMMARY db_path={db_path.resolve()}",
        "SUMMARY run_id=FUND_SCORE_USA_V1",
        "SUMMARY status=dry-run",
    ]


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
        ) VALUES (?, ?, ?, 1000.0, ?, NULL, NULL, ?, ?, NULL, NULL, ?, NULL, NULL, ?, ?, ?, NULL, 'TTM_RUN_V1')
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
        ),
    )
