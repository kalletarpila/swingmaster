from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_score
from swingmaster.cli.run_fundamental_score import main as score_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.score import (
    ACTIVE_FUND_SCORE_LIFECYCLE_RULE,
    ACTIVE_FUND_SCORE_RULE,
    SCORE_NOT_READY,
    SCORE_PROFILE_UNSUPPORTED,
    assess_latest_q_core_fields,
    assess_score_readiness,
    compute_consistency_component,
    explain_score_components,
    run_fundamental_scoring,
)


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
        assert score == 61.0


def test_score_leverage_prefers_net_debt_to_ebitda_over_ebit() -> None:
    row = {
        "revenue_growth_ttm_yoy": None,
        "ebit_margin_ttm": 0.10,
        "ebit_margin_trend_4q": None,
        "ebitda_margin_ttm": 0.10,
        "ebitda_margin_trend_4q": None,
        "fcf_margin_ttm": 0.10,
        "net_debt_to_ebitda": 0.5,
        "net_debt_to_ebit": 4.0,
        "share_dilution_yoy": None,
        "lifecycle_class": None,
    }

    components = explain_score_components(row)

    assert components["leverage_component"] == 12.0


def test_score_writes_all_component_columns_and_rule(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_components.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(
            conn,
            ticker="AAPL",
            as_of_date="2025-03-31",
            revenue_growth_ttm_yoy=0.40,
            ebit_margin_ttm=0.30,
            ebit_margin_trend_4q=0.05,
            fcf_margin_ttm=0.25,
            net_debt_to_ebitda=-0.10,
            share_dilution_yoy=-0.03,
            lifecycle_class="MATURE",
        )
        _insert_ttm_row(
            conn,
            ticker="AAPL",
            as_of_date="2025-06-30",
            revenue_growth_ttm_yoy=0.40,
            ebit_margin_ttm=0.30,
            ebit_margin_trend_4q=0.05,
            fcf_margin_ttm=0.25,
            net_debt_to_ebitda=-0.10,
            share_dilution_yoy=-0.03,
            lifecycle_class="MATURE",
        )
        _insert_ttm_row(
            conn,
            ticker="AAPL",
            as_of_date="2025-09-30",
            revenue_growth_ttm_yoy=0.40,
            ebit_margin_ttm=0.30,
            ebit_margin_trend_4q=0.05,
            fcf_margin_ttm=0.25,
            net_debt_to_ebitda=-0.10,
            share_dilution_yoy=-0.03,
            lifecycle_class="MATURE",
        )
        _insert_ttm_row(
            conn,
            ticker="AAPL",
            as_of_date="2025-12-31",
            revenue_growth_ttm_yoy=0.40,
            ebit_margin_ttm=0.30,
            ebit_margin_trend_4q=0.05,
            fcf_margin_ttm=0.25,
            net_debt_to_ebitda=-0.10,
            share_dilution_yoy=-0.03,
            lifecycle_class="MATURE",
        )
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                growth_component,
                margin_component,
                margin_trend_component,
                fcf_component,
                leverage_component,
                dilution_component,
                lifecycle_component,
                consistency_component,
                score_rule,
                fundamental_score_lifecycle,
                growth_component_lifecycle,
                margin_component_lifecycle,
                margin_trend_component_lifecycle,
                fcf_component_lifecycle,
                leverage_component_lifecycle,
                dilution_component_lifecycle,
                lifecycle_component_lifecycle,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row == (
            92.0,
            15.0,
            12.0,
            10.0,
            15.0,
            15.0,
            10.0,
            5.0,
            10.0,
            ACTIVE_FUND_SCORE_RULE,
            97.95,
            14.25,
            13.200000000000001,
            10.0,
            17.25,
            15.75,
            11.0,
            5.0,
            11.5,
            ACTIVE_FUND_SCORE_LIFECYCLE_RULE,
        )


def test_component_column_sum_equals_fundamental_score(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_component_sum.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.20, 0.20, 0.02, 0.10, 1.0, 0.01, "GROWTH")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.22, 0.21, 0.02, 0.11, 1.0, 0.01, "GROWTH")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.24, 0.22, 0.03, 0.12, 1.0, 0.01, "GROWTH")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.28, 0.24, 0.05, 0.14, 1.0, 0.01, "GROWTH")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                growth_component
                + margin_component
                + margin_trend_component
                + fcf_component
                + leverage_component
                + dilution_component
                + lifecycle_component
                + consistency_component,
                score_rule
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row == (67.0, 67.0, ACTIVE_FUND_SCORE_RULE)


def test_lifecycle_score_changes_for_mature_overlay(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_same.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.20, 0.20, 0.02, 0.10, 1.0, 0.01, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.22, 0.21, 0.02, 0.11, 1.0, 0.01, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.24, 0.22, 0.03, 0.12, 1.0, 0.01, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.28, 0.24, 0.05, 0.14, 1.0, 0.01, "MATURE")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component,
                growth_component_lifecycle,
                consistency_component,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row[0] == 70.0
        assert row[1] == pytest.approx(74.0)
        assert row[2] == 12.0
        assert row[3] == pytest.approx(11.4)
        assert row[4] == 6.0
        assert row[5] == pytest.approx(6.9)
        assert row[6] == ACTIVE_FUND_SCORE_LIFECYCLE_RULE


def test_lifecycle_score_differs_from_baseline_when_lifecycle_is_scaling(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_scaling.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.20, 0.20, 0.02, 0.10, 1.0, 0.01, "SCALING")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.22, 0.21, 0.02, 0.11, 1.0, 0.01, "SCALING")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.24, 0.22, 0.03, 0.12, 1.0, 0.01, "SCALING")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.28, 0.24, 0.05, 0.14, 1.0, 0.01, "SCALING")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component_lifecycle,
                margin_component_lifecycle,
                margin_trend_component_lifecycle,
                fcf_component_lifecycle,
                leverage_component_lifecycle,
                dilution_component_lifecycle,
                lifecycle_component_lifecycle,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row == (
            69.0,
            74.0,
            15.0,
            7.2,
            12.5,
            10.8,
            12.0,
            5.0,
            4.0,
            7.5,
            ACTIVE_FUND_SCORE_LIFECYCLE_RULE,
        )


def test_startup_lifecycle_weighting(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_startup_weighting.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.30, 0.01, 0.02, 0.01, 3.0, 0.01, "STARTUP")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.30, 0.01, 0.02, 0.01, 3.0, 0.01, "STARTUP")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.30, 0.01, 0.02, 0.01, 3.0, 0.01, "STARTUP")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.30, 0.01, 0.02, 0.01, 3.0, 0.01, "STARTUP")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component,
                growth_component_lifecycle,
                margin_component,
                margin_component_lifecycle,
                consistency_component,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row[0] == 43.0
        assert row[1] == pytest.approx(45.5)
        assert row[2:] == (
            15.0,
            21.0,
            4.0,
            2.4,
            10.0,
            11.5,
            ACTIVE_FUND_SCORE_LIFECYCLE_RULE,
        )


def test_distressed_lifecycle_weighting(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_distressed_weighting.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DISTRESSED")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DISTRESSED")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DISTRESSED")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DISTRESSED")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component,
                growth_component_lifecycle,
                margin_component,
                margin_component_lifecycle,
                leverage_component,
                leverage_component_lifecycle,
                fcf_component,
                fcf_component_lifecycle,
                consistency_component,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row[0] == 58.0
        assert row[1] == pytest.approx(55.1)
        assert row[2] == 15.0
        assert row[3] == pytest.approx(10.5)
        assert row[4] == 8.0
        assert row[5] == pytest.approx(4.8)
        assert row[6] == 12.0
        assert row[7] == pytest.approx(16.8)
        assert row[8] == 12.0
        assert row[9] == pytest.approx(15.0)
        assert row[10] == 10.0
        assert row[11] == pytest.approx(12.0)
        assert row[12] == ACTIVE_FUND_SCORE_LIFECYCLE_RULE


def test_distressed_additive_penalty(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_distressed_penalty.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DISTRESSED")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DISTRESSED")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DISTRESSED")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DISTRESSED")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component_lifecycle,
                margin_component_lifecycle,
                leverage_component_lifecycle,
                fcf_component_lifecycle,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row[0] == 58.0
        assert row[1] == pytest.approx(55.1)
        assert row[2] == pytest.approx(10.5)
        assert row[3] == pytest.approx(4.8)
        assert row[4] == pytest.approx(16.8)
        assert row[5] == pytest.approx(15.0)
        assert row[6] == pytest.approx(12.0)
        assert row[7] == ACTIVE_FUND_SCORE_LIFECYCLE_RULE


def test_transition_lifecycle_weighting(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_transition_weighting.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "TRANSITION")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "TRANSITION")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "TRANSITION")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "TRANSITION")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component,
                growth_component_lifecycle,
                margin_trend_component,
                margin_trend_component_lifecycle,
                consistency_component,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row[0] == 65.0
        assert row[1] == pytest.approx(71.3)
        assert row[2] == 12.0
        assert row[3] == pytest.approx(13.8)
        assert row[4] == 6.0
        assert row[5] == pytest.approx(8.1)
        assert row[6] == 10.0
        assert row[7] == pytest.approx(12.0)
        assert row[8] == ACTIVE_FUND_SCORE_LIFECYCLE_RULE


def test_declining_lifecycle_weighting_and_penalty(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_declining_weighting.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DECLINING")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DECLINING")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DECLINING")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.30, 0.16, 0.02, 0.10, 1.0, 0.01, "DECLINING")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component_lifecycle,
                margin_trend_component_lifecycle,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row[0] == 63.0
        assert row[1] == pytest.approx(51.45)
        assert row[2] == pytest.approx(9.75)
        assert row[3] == pytest.approx(4.2)
        assert row[4] == pytest.approx(8.0)
        assert row[5] == ACTIVE_FUND_SCORE_LIFECYCLE_RULE


def test_growth_lifecycle_weighting(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_growth_weighting.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "GROWTH")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "GROWTH")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "GROWTH")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "GROWTH")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component,
                growth_component_lifecycle,
                margin_component,
                margin_component_lifecycle,
                consistency_component,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row[0] == 67.0
        assert row[1] == pytest.approx(70.2)
        assert row[2] == 12.0
        assert row[3] == pytest.approx(13.2)
        assert row[4] == 8.0
        assert row[5] == pytest.approx(8.4)
        assert row[6] == 10.0
        assert row[7] == pytest.approx(11.0)
        assert row[8] == ACTIVE_FUND_SCORE_LIFECYCLE_RULE


def test_mature_lifecycle_weighting(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_lifecycle_mature_weighting.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.20, 0.16, 0.02, 0.10, 1.0, 0.01, "MATURE")
        conn.commit()

        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        row = conn.execute(
            """
            SELECT
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component,
                growth_component_lifecycle,
                margin_component,
                margin_component_lifecycle,
                fcf_component,
                fcf_component_lifecycle,
                dilution_component,
                dilution_component_lifecycle,
                consistency_component,
                consistency_component_lifecycle,
                score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2025-12-31'
            """
        ).fetchone()
        assert row[0] == 70.0
        assert row[1] == pytest.approx(74.6)
        assert row[2] == 12.0
        assert row[3] == pytest.approx(11.4)
        assert row[4] == 8.0
        assert row[5] == pytest.approx(8.8)
        assert row[6] == 12.0
        assert row[7] == pytest.approx(13.8)
        assert row[8] == 5.0
        assert row[9] == pytest.approx(5.5)
        assert row[10] == 10.0
        assert row[11] == pytest.approx(11.5)
        assert row[12] == ACTIVE_FUND_SCORE_LIFECYCLE_RULE


def test_score_startup(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_startup.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.45, -0.10, 0.03, -0.20, None, 0.08, "STARTUP")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert score == 24.0


def test_score_distressed_clamps_at_zero(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_distressed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", -0.20, -0.50, -0.20, -0.40, 5.0, 0.10, "DISTRESSED")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert score == 0.0


def test_extreme_share_dilution_is_treated_as_none_for_scoring(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_extreme_dilution.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, 2.78, "MATURE")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        score, dilution_component, raw_share_dilution_yoy = conn.execute(
            """
            SELECT fundamental_score, dilution_component, share_dilution_yoy
            FROM rc_fundamental_ttm
            """
        ).fetchone()
        assert score == 61.0
        assert dilution_component == 5.0
        assert raw_share_dilution_yoy == 2.78


def test_score_max_score_case(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_max.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-03-31", 0.40, 0.35, 0.10, 0.25, -0.10, -0.03, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-06-30", 0.40, 0.35, 0.10, 0.25, -0.10, -0.03, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-09-30", 0.40, 0.35, 0.10, 0.25, -0.10, -0.03, "MATURE")
        _insert_ttm_row(conn, "AAPL", "2025-12-31", 0.40, 0.35, 0.10, 0.25, -0.10, -0.03, "MATURE")
        conn.commit()
        run_fundamental_scoring(conn, "AAPL", dry_run=False)
        score = conn.execute(
            "SELECT fundamental_score FROM rc_fundamental_ttm WHERE ticker='AAPL' AND as_of_date='2025-12-31'"
        ).fetchone()[0]
        assert score == 100.0


def test_score_dry_run_does_not_update_db(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_dry_run.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        conn.commit()
        rows_scored, min_score, max_score, avg_score = run_fundamental_scoring(conn, "AAPL", dry_run=True)
        assert rows_scored == 1
        assert min_score == 61.0
        assert max_score == 61.0
        assert avg_score == 61.0
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
        assert row == (1, 61.0)


def test_score_all_tickers_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_all.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        _insert_ttm_row(conn, "MSFT", "2025-12-31", 0.25, 0.05, 0.02, 0.01, None, 0.01, "GROWTH")
        conn.commit()
        rows_scored, min_score, max_score, avg_score = run_fundamental_scoring(conn, None, dry_run=False)
        assert rows_scored == 2
        assert min_score == 41.0
        assert max_score == 61.0
        assert avg_score == 51.0
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


def test_score_empty_scope_updates_zero_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_empty_scope.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        conn.commit()
        rows_scored, min_score, max_score, avg_score = run_fundamental_scoring(
            conn,
            "AAPL",
            dry_run=False,
            as_of_dates=[],
            skip_unchanged=True,
        )
        score = conn.execute("SELECT fundamental_score FROM rc_fundamental_ttm").fetchone()[0]
        assert rows_scored == 0
        assert min_score is None
        assert max_score is None
        assert avg_score is None
        assert score is None


def test_score_skip_unchanged_compares_loaded_component_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_score_skip_unchanged.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAPL", "2025-12-31", None, 0.32, None, 0.28, 0.30, None, "MATURE")
        conn.commit()

        rows_scored, *_ = run_fundamental_scoring(conn, "AAPL", dry_run=False)
        assert rows_scored == 1

        rows_scored, min_score, max_score, avg_score = run_fundamental_scoring(
            conn,
            "AAPL",
            dry_run=False,
            as_of_dates=["2025-12-31"],
            skip_unchanged=True,
        )
        assert rows_scored == 0
        assert min_score is None
        assert max_score is None
        assert avg_score is None


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
        f"SUMMARY rule_id={ACTIVE_FUND_SCORE_RULE}",
        "SUMMARY ticker=ALL",
        "SUMMARY rows_scored=2",
        "SUMMARY min_score=41.0",
        "SUMMARY max_score=61.0",
        "SUMMARY avg_score=51.0",
        f"SUMMARY db_path={db_path.resolve()}",
        "SUMMARY run_id=FUND_SCORE_USA_V1",
        "SUMMARY status=dry-run",
    ]


def test_consistency_component_stable_dataset() -> None:
    history = [
        _history_row("2025-03-31", 0.20, 0.30, 0.25),
        _history_row("2025-06-30", 0.20, 0.30, 0.25),
        _history_row("2025-09-30", 0.20, 0.30, 0.25),
        _history_row("2025-12-31", 0.20, 0.30, 0.25),
    ]
    assert compute_consistency_component(history) == 10


def test_consistency_component_moderate_variance_dataset() -> None:
    history = [
        _history_row("2025-03-31", 0.20, 0.20, 0.20),
        _history_row("2025-06-30", 0.22, 0.22, 0.22),
        _history_row("2025-09-30", 0.24, 0.24, 0.24),
        _history_row("2025-12-31", 0.28, 0.28, 0.28),
    ]
    assert compute_consistency_component(history) == 6


def test_consistency_component_high_variance_dataset() -> None:
    history = [
        _history_row("2025-03-31", 0.10, 0.10, 0.10),
        _history_row("2025-06-30", 0.30, 0.30, 0.30),
        _history_row("2025-09-30", 0.05, 0.05, 0.05),
        _history_row("2025-12-31", 0.40, 0.40, 0.40),
    ]
    assert compute_consistency_component(history) == 0


@pytest.mark.parametrize(
    ("margin", "expected"),
    [
        (-0.01, 0.0),
        (0.0, 4.0),
        (0.149999, 4.0),
        (0.15, 8.0),
        (0.249999, 8.0),
        (0.25, 12.0),
        (0.349999, 12.0),
        (0.35, 15.0),
        (None, 0.0),
    ],
)
def test_ebitda_margin_component_boundaries(margin: float | None, expected: float) -> None:
    row = _score_row(ebitda_margin_ttm=margin, ebit_margin_ttm=-0.99)

    components = explain_score_components(row)

    assert components["margin_component"] == expected


@pytest.mark.parametrize(
    ("trend", "expected"),
    [
        (-0.01, 2.0),
        (0.0, 6.0),
        (0.039999, 6.0),
        (0.04, 10.0),
        (0.099999, 10.0),
        (0.10, 15.0),
        (None, 6.0),
    ],
)
def test_ebitda_margin_trend_component_boundaries(trend: float | None, expected: float) -> None:
    row = _score_row(ebitda_margin_trend_4q=trend, ebit_margin_trend_4q=-0.99)

    components = explain_score_components(row)

    assert components["margin_trend_component"] == expected


def test_null_ebitda_margin_trend_keeps_score_but_blocks_score_ready() -> None:
    history = [_score_row(as_of_date=f"2025-0{index}-30") for index in range(1, 4)]
    latest = _score_row(ebitda_margin_trend_4q=None)

    components = explain_score_components(latest, history + [latest])
    readiness = assess_score_readiness(latest, history + [latest])

    assert components["margin_trend_component"] == 6.0
    assert components["fundamental_score_recomputed"] > 0.0
    assert readiness.score_ready is False
    assert readiness.score_profile_status == SCORE_NOT_READY
    assert "EBITDA_MARGIN_TREND_READY" in readiness.missing_reasons


def test_consistency_component_uses_ebitda_margin_not_ebit_margin() -> None:
    history = [
        _history_row("2025-03-31", 0.20, 0.01, 0.25, ebitda_margin_ttm=0.30),
        _history_row("2025-06-30", 0.20, 0.90, 0.25, ebitda_margin_ttm=0.30),
        _history_row("2025-09-30", 0.20, -0.50, 0.25, ebitda_margin_ttm=0.30),
        _history_row("2025-12-31", 0.20, 0.50, 0.25, ebitda_margin_ttm=0.30),
    ]

    assert compute_consistency_component(history) == 10


def test_score_readiness_complete_and_missing_cases() -> None:
    history = [_score_row(as_of_date=f"2025-Q{index}") for index in range(1, 4)]
    latest = _score_row()

    ready = assess_score_readiness(latest, history + [latest])
    missing_leverage = assess_score_readiness(
        {**latest, "net_debt_to_ebitda": None, "net_debt_to_ebit": None},
        history + [latest],
    )
    unsupported = assess_score_readiness(latest, history + [latest], score_profile="BANK")

    assert ready.score_ready is True
    assert missing_leverage.score_ready is False
    assert "LEVERAGE_READY" in missing_leverage.missing_reasons
    assert unsupported.score_profile_status == SCORE_PROFILE_UNSUPPORTED


def test_latest_q_core_fields_are_current_quarter_only() -> None:
    complete, missing = assess_latest_q_core_fields(
        {
            "ticker": "AAPL",
            "period_end_date": "2026-06-30",
            "revenue": 1.0,
            "ebitda": 1.0,
            "free_cashflow": 0.0,
            "cash": 0.0,
            "total_debt": 0.0,
            "shares_outstanding": 10.0,
        }
    )
    bad_shares, bad_missing = assess_latest_q_core_fields(
        {
            "ticker": "AAPL",
            "period_end_date": "2026-06-30",
            "revenue": 1.0,
            "ebitda": 1.0,
            "free_cashflow": 1.0,
            "cash": 1.0,
            "total_debt": 1.0,
            "shares_outstanding": 0.0,
        }
    )

    assert complete is True
    assert missing == ()
    assert bad_shares is False
    assert bad_missing == ("shares_outstanding",)


def test_consistency_component_insufficient_observations() -> None:
    history = [
        _history_row("2025-09-30", 0.20, 0.30, 0.25),
        _history_row("2025-12-31", 0.22, 0.31, 0.26),
    ]
    assert compute_consistency_component(history) == 0


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
            ebitda_ttm,
            ebitda_margin_ttm,
            ebitda_margin_trend_4q,
            gross_margin_trend_4q,
            fcf_ttm,
            fcf_margin_ttm,
            fcf_margin_trend_4q,
            net_debt,
            net_debt_to_ebit,
            net_debt_to_ebitda,
            share_dilution_yoy,
            lifecycle_class,
            fundamental_score,
            run_id
        ) VALUES (?, ?, ?, 1000.0, ?, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL, ?, NULL, NULL, ?, ?, ?, ?, NULL, 'TTM_RUN_V1')
        """,
        (
            ticker,
            as_of_date,
            as_of_date,
            revenue_growth_ttm_yoy,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            100.0 if ebit_margin_ttm is not None else None,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            fcf_margin_ttm,
            net_debt_to_ebitda,
            net_debt_to_ebitda,
            share_dilution_yoy,
            lifecycle_class,
        ),
    )


def _history_row(
    as_of_date: str,
    revenue_growth_ttm_yoy: float | None,
    ebit_margin_ttm: float | None,
    fcf_margin_ttm: float | None,
    *,
    ebitda_margin_ttm: float | None = None,
) -> dict[str, float | str | None]:
    return {
        "ticker": "AAPL",
        "as_of_date": as_of_date,
        "revenue_growth_ttm_yoy": revenue_growth_ttm_yoy,
        "ebit_margin_ttm": ebit_margin_ttm,
        "ebitda_margin_ttm": ebit_margin_ttm if ebitda_margin_ttm is None else ebitda_margin_ttm,
        "fcf_margin_ttm": fcf_margin_ttm,
    }


def _score_row(**overrides: float | str | None) -> dict[str, float | str | None]:
    row: dict[str, float | str | None] = {
        "ticker": "AAPL",
        "as_of_date": "2025-12-31",
        "revenue_growth_ttm_yoy": 0.20,
        "ebit_margin_ttm": -0.99,
        "ebit_margin_trend_4q": -0.99,
        "ebitda_margin_ttm": 0.25,
        "ebitda_margin_trend_4q": 0.04,
        "fcf_margin_ttm": 0.10,
        "net_debt_to_ebitda": 1.0,
        "net_debt_to_ebit": None,
        "share_dilution_yoy": 0.01,
        "lifecycle_class": "MATURE",
    }
    row.update(overrides)
    return row
