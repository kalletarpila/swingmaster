from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_lifecycle
from swingmaster.cli.run_fundamental_lifecycle import main as lifecycle_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.lifecycle import FUND_LIFECYCLE_RULE_V1, classify_lifecycle, run_lifecycle_classification


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
            ebit_margin_ttm=-0.99,
            ebit_margin_trend_4q=-0.99,
            ebitda_margin_ttm=0.06,
            ebitda_margin_trend_4q=0.0,
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
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, -0.99, None, -0.25, ebitda_margin_ttm=-0.31)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "DISTRESSED"


def test_lifecycle_startup(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_startup.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, 0.40, 0.99, None, -0.05, ebitda_margin_ttm=-0.01)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "STARTUP"


def test_lifecycle_mature_still_preferred_over_transition(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_mature.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, -0.99, None, 0.06, ebitda_margin_ttm=0.25)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "MATURE"


def test_lifecycle_declining_overrides_transition(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_declining.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, -0.10, 0.99, None, 0.05, ebitda_margin_ttm=0.07)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "DECLINING"


def test_lifecycle_scaling_still_works(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_scaling.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, 0.12, -0.99, -0.99, 0.02, ebitda_margin_ttm=0.02, ebitda_margin_trend_4q=0.04)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert lifecycle_class == "SCALING"


def test_lifecycle_distressed_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_distressed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, -0.25, None, -0.30, ebitda_margin_ttm=-0.31)
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


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"revenue_growth_ttm_yoy": 0.50, "ebitda_margin_ttm": -0.31, "ebitda_margin_trend_4q": 0.20, "fcf_margin_ttm": -0.21}, "DISTRESSED"),
        ({"revenue_growth_ttm_yoy": 0.40, "ebitda_margin_ttm": -0.01, "ebitda_margin_trend_4q": 0.20, "fcf_margin_ttm": -0.01}, "STARTUP"),
        ({"revenue_growth_ttm_yoy": 0.21, "ebitda_margin_ttm": 0.149, "ebitda_margin_trend_4q": 0.20, "fcf_margin_ttm": 0.10}, "GROWTH"),
        ({"revenue_growth_ttm_yoy": 0.11, "ebitda_margin_ttm": 0.01, "ebitda_margin_trend_4q": 0.01, "fcf_margin_ttm": 0.01}, "SCALING"),
        ({"revenue_growth_ttm_yoy": None, "ebitda_margin_ttm": 0.25, "ebitda_margin_trend_4q": -0.50, "fcf_margin_ttm": 0.05}, "MATURE"),
        ({"revenue_growth_ttm_yoy": None, "ebitda_margin_ttm": 0.10, "ebitda_margin_trend_4q": -0.07, "fcf_margin_ttm": 0.01}, "TRANSITION"),
        ({"revenue_growth_ttm_yoy": -0.06, "ebitda_margin_ttm": 0.10, "ebitda_margin_trend_4q": 0.00, "fcf_margin_ttm": 0.01}, "DECLINING"),
        ({"revenue_growth_ttm_yoy": 0.00, "ebitda_margin_ttm": None, "ebitda_margin_trend_4q": None, "fcf_margin_ttm": None}, "UNCLASSIFIED"),
    ],
)
def test_lifecycle_ebitda_l2_boundaries_and_precedence(row: dict[str, float | None], expected: str) -> None:
    assert classify_lifecycle(row) == expected


def test_lifecycle_dry_run_does_not_update_db(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_dry_run.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, 0.30, 0.01, 0.20, ebitda_margin_ttm=0.30)
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
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, 0.30, 0.01, 0.20, ebitda_margin_ttm=0.30, fundamental_score=77.0)
        conn.commit()
        run_lifecycle_classification(conn, "AAPL", dry_run=False)
        fundamental_score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert fundamental_score == 77.0


def test_lifecycle_empty_scope_updates_zero_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_lifecycle_empty_scope.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, 0.30, 0.01, 0.20, ebitda_margin_ttm=0.30)
        conn.commit()
        rows_classified, _class_counts = run_lifecycle_classification(
            conn,
            "AAPL",
            dry_run=False,
            as_of_dates=[],
            skip_unchanged=True,
        )
        lifecycle_class = conn.execute("SELECT lifecycle_class FROM rc_fundamental_ttm").fetchone()[0]
        assert rows_classified == 0
        assert lifecycle_class is None


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
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 1000.0, None, 0.30, 0.01, 0.20, ebitda_margin_ttm=0.30)
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
        f"SUMMARY rule_id={FUND_LIFECYCLE_RULE_V1}",
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
    ebitda_margin_ttm: float | None = None,
    ebitda_margin_trend_4q: float | None = None,
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
            ebitda_ttm,
            ebitda_margin_ttm,
            ebitda_margin_trend_4q,
            gross_margin_trend_4q,
            fcf_ttm,
            fcf_margin_ttm,
            fcf_margin_trend_4q,
            net_debt,
            net_debt_to_ebit,
            share_dilution_yoy,
            lifecycle_class,
            fundamental_score,
            run_id
        ) VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, NULL, NULL, NULL, NULL, ?, 'TTM_RUN_V1')
        """,
        (
            ticker,
            as_of_date,
            as_of_date,
            revenue_ttm,
            revenue_growth_ttm_yoy,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            100.0 if (ebitda_margin_ttm if ebitda_margin_ttm is not None else ebit_margin_ttm) is not None else None,
            ebit_margin_ttm if ebitda_margin_ttm is None else ebitda_margin_ttm,
            ebit_margin_trend_4q if ebitda_margin_trend_4q is None else ebitda_margin_trend_4q,
            fcf_margin_ttm,
            fundamental_score,
        ),
    )
