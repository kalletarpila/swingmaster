from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_lifecycle
from swingmaster.cli.run_fundamental_lifecycle import main as lifecycle_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.lifecycle import run_lifecycle_classification


def test_lifecycle_transition_classification(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_transition.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(
            conn,
            ticker="AAPL",
            as_of_date="2025-12-31",
            revenue_ttm=1000.0,
            revenue_growth_ttm_yoy=None,
            ebit_margin_ttm=0.06,
            ebit_margin_trend_4q=None,
            fcf_margin_ttm=0.07,
        )
        conn.commit()
        rows_classified, _ = run_lifecycle_classification(conn, "AAPL", dry_run=False)
        assert rows_classified == 1
        lifecycle_class = conn.execute(
            "SELECT lifecycle_class FROM rc_fundamental_ttm WHERE ticker='AAPL'"
        ).fetchone()[0]
        assert lifecycle_class == "TRANSITION"


def test_lifecycle_distressed(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_distressed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, -0.30, None, -0.25)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "DISTRESSED"


def test_lifecycle_startup(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_startup.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, 0.40, -0.10, None, -0.05)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "STARTUP"


def test_lifecycle_mature_still_preferred_over_transition(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_mature.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, 0.18, None, 0.06)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "MATURE"


def test_lifecycle_declining_overrides_transition(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_declining.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, -0.10, 0.07, None, 0.05)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "DECLINING"


def test_lifecycle_scaling_still_works(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_scaling.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, 0.12, 0.02, 0.04, 0.02)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "SCALING"


def test_lifecycle_distressed_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_distressed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, -0.25, None, -0.30)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "DISTRESSED"


def test_lifecycle_fallback_still_unclassified(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_unclassified.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, None, None, None)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "UNCLASSIFIED"


def test_lifecycle_dry_run_does_not_update_db(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_dry_run.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, 0.30, 0.01, 0.20)
        conn.commit()
        rows_classified, class_counts = run_lifecycle_classification(conn, "AAPL", dry_run=True)
        assert rows_classified == 1
        assert class_counts["MATURE"] == 1
        assert class_counts["TRANSITION"] == 0
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class is None


def test_lifecycle_fundamental_score_untouched(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_score.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, 0.30, 0.01, 0.20, fundamental_score=77.0)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        fundamental_score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert fundamental_score == 77.0


def test_lifecycle_raises_when_no_ttm_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_empty.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(RuntimeError, match="^FUNDAMENTAL_TTM_NOT_FOUND$"):
            run_lifecycle_classification(conn, None, dry_run=False)


def test_cli_lifecycle_summary_all(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_cli.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, 0.30, 0.01, 0.20)
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_lifecycle,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(db_path),
                "ticker": None,
                "run_id": "FUND_LIFECYCLE_USA_V1",
                "dry_run": True,
            },
        )(),
    )

    lifecycle_main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY rule_id=FUND_LIFECYCLE_RULE_V2",
        "SUMMARY ticker=ALL",
        "SUMMARY rows_classified=1",
        "SUMMARY class_STARTUP=0",
        "SUMMARY class_GROWTH=0",
        "SUMMARY class_SCALING=0",
        "SUMMARY class_MATURE=1",
        "SUMMARY class_TRANSITION=0",
        "SUMMARY class_DECLINING=0",
        "SUMMARY class_DISTRESSED=0",
        "SUMMARY class_UNCLASSIFIED=0",
        f"SUMMARY db_path={db_path.resolve()}",
        "SUMMARY run_id=FUND_LIFECYCLE_USA_V1",
        "SUMMARY status=dry-run",
    ]


def _insert_ttm_row(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    revenue_ttm: float | None,
    revenue_growth_ttm_yoy: float | None,
    ebit_margin_ttm: float | None,
    ebit_margin_trend_4q: float | None,
    fcf_margin_ttm: float | None,
    fundamental_score: float | None = None,
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
        ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, ?, 'TTM_RUN_V1')
        """,
        (
            ticker,
            as_of_date,
            as_of_date,
            revenue_ttm,
            revenue_growth_ttm_yoy,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            fcf_margin_ttm,
            fundamental_score,
        ),
    )
