from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_score_percentile as run_fundamental_score_percentile_cli
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_score_percentile import main as percentile_main
from swingmaster.fundamentals.score_percentile import (
    FUND_SCORE_PERCENTILE_V2_PRE,
    FUND_SCORE_PERCENTILE_V2_2_LIFECYCLE_MULT_PRE,
    build_percentile_rows,
    compute_blended_percentile_score,
    compute_lifecycle_weighted_percentile_score,
    compute_percentiles,
    compute_weighted_percentile_score,
    load_latest_percentile_snapshot,
    resolve_industry_min_size,
    resolve_min_universe_size,
    run_fundamental_score_percentile,
)


def test_latest_snapshot_selection_uses_latest_row_at_or_before_target_date(tmp_path: Path) -> None:
    fundamentals_db_path = tmp_path / "fundamentals_snapshot.db"
    osakedata_db_path = tmp_path / "osakedata_snapshot.db"
    run_migration(fundamentals_db_path)
    _create_osakedata_db(osakedata_db_path)

    with sqlite3.connect(str(fundamentals_db_path)) as fundamentals_conn, sqlite3.connect(str(osakedata_db_path)) as osakedata_conn:
        _insert_meta_row(osakedata_conn, "AAPL", "usa", "Tech", "Hardware")
        _insert_meta_row(osakedata_conn, "MSFT", "usa", "Tech", "Software")
        _insert_percentile_ttm_row(fundamentals_conn, "AAPL", "2025-03-31", 0.10, 0.10, 0.10, 0.10, 1.0, 0.01, 8.0, 70.0)
        _insert_percentile_ttm_row(fundamentals_conn, "AAPL", "2025-06-30", 0.20, 0.20, 0.20, 0.20, 0.9, 0.02, 9.0, 75.0)
        _insert_percentile_ttm_row(fundamentals_conn, "AAPL", "2025-09-30", 0.30, 0.30, 0.30, 0.30, 0.8, 0.03, 10.0, 80.0)
        _insert_percentile_ttm_row(fundamentals_conn, "MSFT", "2025-05-31", 0.15, 0.15, 0.15, 0.15, 0.7, 0.01, 7.0, 72.0)
        fundamentals_conn.commit()

        snapshot_rows = load_latest_percentile_snapshot(
            fundamentals_conn=fundamentals_conn,
            osakedata_conn=osakedata_conn,
            target_date="2025-07-01",
            market="usa",
        )
        assert [(row.ticker, row.as_of_date) for row in snapshot_rows] == [
            ("AAPL", "2025-06-30"),
            ("MSFT", "2025-05-31"),
        ]


def test_latest_snapshot_metadata_lookup_is_scoped_by_market(tmp_path: Path) -> None:
    fundamentals_db_path = tmp_path / "fundamentals_market_meta.db"
    osakedata_db_path = tmp_path / "osakedata_market_meta.db"
    run_migration(fundamentals_db_path)
    _create_osakedata_db(osakedata_db_path)

    with sqlite3.connect(str(fundamentals_db_path)) as fundamentals_conn, sqlite3.connect(str(osakedata_db_path)) as osakedata_conn:
        _insert_meta_row(osakedata_conn, "NOKIA.HE", "omxh", "Industrials", "Equipment")
        _insert_percentile_ttm_row(fundamentals_conn, "NOKIA.HE", "2025-12-31", 0.10, 0.10, 0.10, 0.10, 1.0, 0.01, 8.0, 70.0)
        fundamentals_conn.commit()

        usa_rows = load_latest_percentile_snapshot(
            fundamentals_conn=fundamentals_conn,
            osakedata_conn=osakedata_conn,
            target_date="2025-12-31",
            market="usa",
        )
        omxh_rows = load_latest_percentile_snapshot(
            fundamentals_conn=fundamentals_conn,
            osakedata_conn=osakedata_conn,
            target_date="2025-12-31",
            market="omxh",
        )

    assert [(row.ticker, row.sector, row.industry) for row in usa_rows] == [("NOKIA.HE", None, None)]
    assert [(row.ticker, row.sector, row.industry) for row in omxh_rows] == [("NOKIA.HE", "Industrials", "Equipment")]


def test_global_percentile_higher_is_better_ranks_correctly() -> None:
    percentiles = compute_percentiles(
        values=[("LOW", 1.0), ("MID", 2.0), ("HIGH", 3.0)],
        higher_is_better=True,
    )
    assert percentiles == {
        "LOW": 0.0,
        "MID": 50.0,
        "HIGH": 100.0,
    }


def test_lower_is_better_percentile_inverts_correctly() -> None:
    percentiles = compute_percentiles(
        values=[("LOW", 1.0), ("MID", 2.0), ("HIGH", 3.0)],
        higher_is_better=False,
    )
    assert percentiles == {
        "LOW": 100.0,
        "MID": 50.0,
        "HIGH": 0.0,
    }


def test_tie_handling_uses_average_rank() -> None:
    percentiles = compute_percentiles(
        values=[("LOW", 1.0), ("TIE1", 2.0), ("TIE2", 2.0)],
        higher_is_better=True,
    )
    assert percentiles["LOW"] == 0.0
    assert percentiles["TIE1"] == pytest.approx(75.0)
    assert percentiles["TIE2"] == pytest.approx(75.0)


def test_null_raw_values_produce_null_percentiles_and_are_excluded(tmp_path: Path) -> None:
    rows = [
        _snapshot_row("AAA", "2025-12-31", growth=1.0),
        _snapshot_row("BBB", "2025-12-31", growth=None),
        _snapshot_row("CCC", "2025-12-31", growth=3.0),
    ]
    percentile_rows = build_percentile_rows(
        snapshot_rows=rows,
        target_date="2025-12-31",
        rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
        run_id="RUN1",
        created_at_utc="2026-04-25T00:00:00Z",
    )
    by_ticker = {row["ticker"]: row for row in percentile_rows}
    assert by_ticker["AAA"]["growth_pct_global"] == 0.0
    assert by_ticker["CCC"]["growth_pct_global"] == 100.0
    assert by_ticker["BBB"]["growth_pct_global"] is None


def test_sector_percentiles_are_null_below_minimum_size() -> None:
    rows = [
        _snapshot_row("AAA", "2025-12-31", sector="Tech"),
        _snapshot_row("BBB", "2025-12-31", sector="Tech"),
        _snapshot_row("CCC", "2025-12-31", sector="Tech"),
    ]
    percentile_rows = build_percentile_rows(
        snapshot_rows=rows,
        target_date="2025-12-31",
        rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
        run_id="RUN1",
        created_at_utc="2026-04-25T00:00:00Z",
    )
    row = percentile_rows[0]
    assert row["sector_size"] == 3
    assert row["growth_pct_sector"] is None
    assert row["fundamental_score_percentile_sector"] is None


def test_industry_percentiles_are_null_below_minimum_size() -> None:
    rows = [
        _snapshot_row("AAA", "2025-12-31", sector="Tech", industry="Hardware"),
        _snapshot_row("BBB", "2025-12-31", sector="Tech", industry="Hardware"),
        _snapshot_row("CCC", "2025-12-31", sector="Tech", industry="Hardware"),
    ]
    percentile_rows = build_percentile_rows(
        snapshot_rows=rows,
        target_date="2025-12-31",
        rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
        run_id="RUN1",
        created_at_utc="2026-04-25T00:00:00Z",
    )
    row = percentile_rows[0]
    assert row["industry_size"] == 3
    assert row["growth_pct_industry"] is None
    assert row["fundamental_score_percentile_industry"] is None


def test_omxh_industry_percentiles_are_computed_at_five_company_threshold() -> None:
    rows = [
        _snapshot_row(
            f"T{i:02d}",
            "2025-12-31",
            growth=float(i),
            sector="Tech",
            industry="Hardware",
        )
        for i in range(5)
    ]
    percentile_rows = build_percentile_rows(
        snapshot_rows=rows,
        target_date="2025-12-31",
        rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
        run_id="RUN1",
        created_at_utc="2026-04-25T00:00:00Z",
        market="omxh",
    )
    row = next(item for item in percentile_rows if item["ticker"] == "T00")
    assert row["industry_size"] == 5
    assert row["growth_pct_industry"] == pytest.approx(0.0)
    assert row["fundamental_score_percentile_industry"] is not None


def test_sector_and_industry_percentiles_are_computed_at_minimum_size_threshold() -> None:
    rows = [
        _snapshot_row(
            f"T{i:02d}",
            "2025-12-31",
            growth=float(i),
            sector="Tech",
            industry="Hardware",
        )
        for i in range(10)
    ]
    percentile_rows = build_percentile_rows(
        snapshot_rows=rows,
        target_date="2025-12-31",
        rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
        run_id="RUN1",
        created_at_utc="2026-04-25T00:00:00Z",
    )
    row = next(item for item in percentile_rows if item["ticker"] == "T00")
    assert row["sector_size"] == 10
    assert row["industry_size"] == 10
    assert row["growth_pct_sector"] == pytest.approx(0.0)
    assert row["growth_pct_industry"] == pytest.approx(0.0)
    assert row["fundamental_score_percentile_sector"] is not None
    assert row["fundamental_score_percentile_industry"] is not None


def test_partition_ranks_are_computed_with_ticker_tie_break_and_null_below_minimum() -> None:
    rows = [
        _snapshot_row("AAA", "2025-12-31", growth=10.0, sector="Tech", industry="Software"),
        _snapshot_row("AAB", "2025-12-31", growth=10.0, sector="Tech", industry="Software"),
        _snapshot_row("AAC", "2025-12-31", growth=9.0, sector="Tech", industry="Software"),
        _snapshot_row("AAD", "2025-12-31", growth=8.0, sector="Tech", industry="Software"),
        _snapshot_row("AAE", "2025-12-31", growth=7.0, sector="Tech", industry="Software"),
        _snapshot_row("AAF", "2025-12-31", growth=6.0, sector="Tech", industry="Software"),
        _snapshot_row("AAG", "2025-12-31", growth=5.0, sector="Tech", industry="Software"),
        _snapshot_row("AAH", "2025-12-31", growth=4.0, sector="Tech", industry="Software"),
        _snapshot_row("AAI", "2025-12-31", growth=3.0, sector="Tech", industry="Software"),
        _snapshot_row("AAJ", "2025-12-31", growth=2.0, sector="Tech", industry="Software"),
        _snapshot_row("SM1", "2025-12-31", growth=1.0, sector="Small", industry="Tiny"),
        _snapshot_row("SM2", "2025-12-31", growth=2.0, sector="Small", industry="Tiny"),
        _snapshot_row("SM3", "2025-12-31", growth=3.0, sector="Small", industry="Tiny"),
    ]
    percentile_rows = build_percentile_rows(
        snapshot_rows=rows,
        target_date="2025-12-31",
        rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
        run_id="RUN1",
        created_at_utc="2026-04-25T00:00:00Z",
    )
    by_ticker = {row["ticker"]: row for row in percentile_rows}
    assert by_ticker["AAA"]["sector_rank_blended"] == 1
    assert by_ticker["AAB"]["sector_rank_blended"] == 2
    assert by_ticker["AAA"]["industry_rank_blended"] == 1
    assert by_ticker["AAB"]["industry_rank_blended"] == 2
    assert by_ticker["AAA"]["sector_rank_blended_lifecycle_weighted"] == 1
    assert by_ticker["AAB"]["sector_rank_blended_lifecycle_weighted"] == 2
    assert by_ticker["SM1"]["sector_rank_blended"] is None
    assert by_ticker["SM1"]["industry_rank_blended"] is None


def test_weighted_score_renormalizes_available_factors_only() -> None:
    score = compute_weighted_percentile_score(
        {
            "growth": 100.0,
            "margin": 0.0,
            "margin_trend": 50.0,
            "fcf": None,
            "consistency": 100.0,
            "leverage": None,
            "dilution": 100.0,
        }
    )
    expected = (
        20.0 * 100.0
        + 15.0 * 0.0
        + 10.0 * 50.0
        + 20.0 * 100.0
        + 7.5 * 100.0
    ) / (20.0 + 15.0 + 10.0 + 20.0 + 7.5)
    assert score == pytest.approx(expected)


def test_weighted_score_requires_minimum_factor_count() -> None:
    score = compute_weighted_percentile_score(
        {
            "growth": 100.0,
            "margin": 50.0,
            "margin_trend": None,
            "fcf": None,
            "consistency": None,
            "leverage": 25.0,
            "dilution": None,
        }
    )
    assert score is None


def test_blended_score_renormalizes_missing_levels() -> None:
    blended = compute_blended_percentile_score(
        {
            "global": 80.0,
            "sector": None,
            "industry": 60.0,
        }
    )
    expected = (0.40 * 80.0 + 0.25 * 60.0) / (0.40 + 0.25)
    assert blended == pytest.approx(expected)


def test_lifecycle_multiplier_applied_correctly() -> None:
    score = compute_lifecycle_weighted_percentile_score(
        {
            "growth": 100.0,
            "margin": 50.0,
            "margin_trend": 25.0,
            "fcf": 80.0,
            "consistency": 60.0,
            "leverage": 40.0,
            "dilution": 20.0,
        },
        "SCALING",
    )
    expected = (
        100.0 * (0.20 * 1.15)
        + 50.0 * (0.15 * 0.95)
        + 25.0 * (0.10 * 1.20)
        + 80.0 * (0.20 * 1.05)
        + 60.0 * (0.20 * 1.10)
        + 40.0 * (0.075 * 1.00)
        + 20.0 * (0.075 * 1.00)
    ) / (
        (0.20 * 1.15)
        + (0.15 * 0.95)
        + (0.10 * 1.20)
        + (0.20 * 1.05)
        + (0.20 * 1.10)
        + (0.075 * 1.00)
        + (0.075 * 1.00)
    )
    assert score == pytest.approx(expected)


def test_transition_lifecycle_multiplier_applied_correctly() -> None:
    score = compute_lifecycle_weighted_percentile_score(
        {
            "growth": 100.0,
            "margin": 50.0,
            "margin_trend": 25.0,
            "fcf": 80.0,
            "consistency": 60.0,
            "leverage": 40.0,
            "dilution": 20.0,
        },
        "TRANSITION",
    )
    expected = (
        100.0 * (0.20 * 1.05)
        + 50.0 * (0.15 * 1.05)
        + 25.0 * (0.10 * 1.10)
        + 80.0 * (0.20 * 1.15)
        + 60.0 * (0.20 * 1.20)
        + 40.0 * (0.075 * 1.00)
        + 20.0 * (0.075 * 1.05)
    ) / (
        (0.20 * 1.05)
        + (0.15 * 1.05)
        + (0.10 * 1.10)
        + (0.20 * 1.15)
        + (0.20 * 1.20)
        + (0.075 * 1.00)
        + (0.075 * 1.05)
    )
    assert score == pytest.approx(expected)


def test_declining_adjustment_applied() -> None:
    score = compute_lifecycle_weighted_percentile_score(
        {
            "growth": 50.0,
            "margin": 50.0,
            "margin_trend": 50.0,
            "fcf": 50.0,
            "consistency": 50.0,
            "leverage": 50.0,
            "dilution": 50.0,
        },
        "DECLINING",
    )
    assert score == pytest.approx(47.0)


def test_distressed_adjustment_applied() -> None:
    score = compute_lifecycle_weighted_percentile_score(
        {
            "growth": 50.0,
            "margin": 50.0,
            "margin_trend": 50.0,
            "fcf": 50.0,
            "consistency": 50.0,
            "leverage": 50.0,
            "dilution": 50.0,
        },
        "DISTRESSED",
    )
    assert score == pytest.approx(46.0)


def test_unclassified_lifecycle_weighted_score_is_unchanged() -> None:
    baseline = compute_weighted_percentile_score(
        {
            "growth": 70.0,
            "margin": 60.0,
            "margin_trend": 50.0,
            "fcf": 40.0,
            "consistency": 30.0,
            "leverage": 20.0,
            "dilution": 10.0,
        }
    )
    lifecycle_weighted = compute_lifecycle_weighted_percentile_score(
        {
            "growth": 70.0,
            "margin": 60.0,
            "margin_trend": 50.0,
            "fcf": 40.0,
            "consistency": 30.0,
            "leverage": 20.0,
            "dilution": 10.0,
        },
        "UNCLASSIFIED",
    )
    assert lifecycle_weighted == baseline


def test_lifecycle_weighted_score_clamps_to_range() -> None:
    high_score = compute_lifecycle_weighted_percentile_score(
        {
            "growth": 100.0,
            "margin": 100.0,
            "margin_trend": 100.0,
            "fcf": 100.0,
            "consistency": 100.0,
            "leverage": 100.0,
            "dilution": 100.0,
        },
        "SCALING",
    )
    low_score = compute_lifecycle_weighted_percentile_score(
        {
            "growth": 0.0,
            "margin": 0.0,
            "margin_trend": 0.0,
            "fcf": 0.0,
            "consistency": 0.0,
            "leverage": 0.0,
            "dilution": 0.0,
        },
        "DISTRESSED",
    )
    assert high_score == 100.0
    assert low_score == 0.0


def test_missing_component_renormalization_works_for_lifecycle_weighted_score() -> None:
    score = compute_lifecycle_weighted_percentile_score(
        {
            "growth": 100.0,
            "margin": None,
            "margin_trend": 50.0,
            "fcf": 0.0,
            "consistency": 100.0,
            "leverage": None,
            "dilution": 50.0,
        },
        "GROWTH",
    )
    expected = (
        100.0 * (0.20 * 1.15)
        + 50.0 * (0.10 * 1.10)
        + 0.0 * (0.20 * 1.00)
        + 100.0 * (0.20 * 1.10)
        + 50.0 * (0.075 * 1.00)
    ) / (
        (0.20 * 1.15)
        + (0.10 * 1.10)
        + (0.20 * 1.00)
        + (0.20 * 1.10)
        + (0.075 * 1.00)
    )
    assert score == pytest.approx(expected)


def test_blended_lifecycle_weighted_score_is_computed_correctly() -> None:
    rows = [
        _snapshot_row("AAA", "2025-12-31", lifecycle_class="MATURE", sector="Tech", industry="Software"),
    ] * 60
    percentile_rows = build_percentile_rows(
        snapshot_rows=rows,
        target_date="2025-12-31",
        rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
        run_id="RUN1",
        created_at_utc="2026-04-25T00:00:00Z",
    )
    row = percentile_rows[0]
    assert row["percentile_lifecycle_weight_rule"] == FUND_SCORE_PERCENTILE_V2_2_LIFECYCLE_MULT_PRE
    assert row["fundamental_score_percentile_global_lifecycle_weighted"] is not None
    assert row["fundamental_score_percentile_sector_lifecycle_weighted"] is not None
    assert row["fundamental_score_percentile_industry_lifecycle_weighted"] is not None
    expected_blended = compute_blended_percentile_score(
        {
            "global": row["fundamental_score_percentile_global_lifecycle_weighted"],
            "sector": row["fundamental_score_percentile_sector_lifecycle_weighted"],
            "industry": row["fundamental_score_percentile_industry_lifecycle_weighted"],
        }
    )
    assert row["fundamental_score_percentile_blended_lifecycle_weighted"] == pytest.approx(expected_blended)


def test_dry_run_writes_no_rows(tmp_path: Path) -> None:
    fundamentals_db_path = tmp_path / "fundamentals_dry_run.db"
    osakedata_db_path = tmp_path / "osakedata_dry_run.db"
    run_migration(fundamentals_db_path)
    _create_osakedata_db(osakedata_db_path)

    with sqlite3.connect(str(fundamentals_db_path)) as fundamentals_conn, sqlite3.connect(str(osakedata_db_path)) as osakedata_conn:
        _insert_large_percentile_dataset(fundamentals_conn, osakedata_conn, row_count=500)
        summary = run_fundamental_score_percentile(
            fundamentals_conn=fundamentals_conn,
            osakedata_conn=osakedata_conn,
            target_date="2026-04-25",
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            run_id="RUN_DRY",
            market="usa",
            created_at_utc="2026-04-25T00:00:00Z",
            dry_run=True,
        )
        row_count = fundamentals_conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_score_percentile"
        ).fetchone()[0]
        assert summary["rows_computed"] == 500
        assert summary["rows_written"] == 0
        assert summary["lifecycle_weighted_rows_computed"] == 500
        assert summary["lifecycle_weighted_rows_written"] == 0
        assert row_count == 0


def test_resolve_min_universe_size_is_market_specific() -> None:
    assert resolve_min_universe_size("usa") == 500
    assert resolve_min_universe_size("omxh") == 50
    assert resolve_min_universe_size("fin") == 500
    assert resolve_min_universe_size("se") == 500


def test_resolve_industry_min_size_is_market_specific() -> None:
    assert resolve_industry_min_size("usa") == 10
    assert resolve_industry_min_size("omxh") == 5
    assert resolve_industry_min_size("fin") == 10


def test_fin_market_uses_lower_minimum_universe_size(tmp_path: Path) -> None:
    fundamentals_db_path = tmp_path / "fundamentals_fin_min.db"
    osakedata_db_path = tmp_path / "osakedata_fin_min.db"
    run_migration(fundamentals_db_path)
    _create_osakedata_db(osakedata_db_path)

    with sqlite3.connect(str(fundamentals_db_path)) as fundamentals_conn, sqlite3.connect(str(osakedata_db_path)) as osakedata_conn:
        _insert_large_percentile_dataset(
            fundamentals_conn,
            osakedata_conn,
            row_count=50,
            market="omxh",
        )
        summary = run_fundamental_score_percentile(
            fundamentals_conn=fundamentals_conn,
            osakedata_conn=osakedata_conn,
            target_date="2026-04-25",
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            run_id="RUN_FIN_50",
            market="omxh",
            created_at_utc="2026-04-25T00:00:00Z",
            dry_run=True,
        )
        assert summary["universe_size"] == 50
        assert summary["rows_computed"] == 50


def test_cli_writes_expected_rows_and_summary_status_ok(monkeypatch, capsys, tmp_path: Path) -> None:
    fundamentals_db_path = tmp_path / "fundamentals_cli.db"
    osakedata_db_path = tmp_path / "osakedata_cli.db"
    run_migration(fundamentals_db_path)
    _create_osakedata_db(osakedata_db_path)

    with sqlite3.connect(str(fundamentals_db_path)) as fundamentals_conn, sqlite3.connect(str(osakedata_db_path)) as osakedata_conn:
        _insert_large_percentile_dataset(fundamentals_conn, osakedata_conn, row_count=500)

    monkeypatch.setattr(
        run_fundamental_score_percentile_cli,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(fundamentals_db_path),
                "osakedata_db": str(osakedata_db_path),
                "as_of_date": "2026-04-25",
                "rule_id": FUND_SCORE_PERCENTILE_V2_PRE,
                "run_id": "RUN_WRITE",
                "market": "usa",
                "created_at_utc": "2026-04-25T00:00:00Z",
                "dry_run": False,
            },
        )(),
    )

    percentile_main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        f"SUMMARY db_path={fundamentals_db_path.resolve()}",
        f"SUMMARY osakedata_db_path={osakedata_db_path.resolve()}",
        "SUMMARY target_date=2026-04-25",
        f"SUMMARY rule_id={FUND_SCORE_PERCENTILE_V2_PRE}",
        "SUMMARY run_id=RUN_WRITE",
        "SUMMARY market=usa",
        "SUMMARY universe_size=500",
        "SUMMARY rows_computed=500",
        "SUMMARY rows_written=500",
        "SUMMARY lifecycle_weighted_rows_computed=500",
        "SUMMARY lifecycle_weighted_rows_written=500",
        "SUMMARY sector_count=1",
        "SUMMARY industry_count=1",
        "SUMMARY dry_run=false",
        "SUMMARY status=ok",
    ]

    with sqlite3.connect(str(fundamentals_db_path)) as fundamentals_conn:
        row_count = fundamentals_conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_score_percentile"
        ).fetchone()[0]
        sample_row = fundamentals_conn.execute(
            """
            SELECT ticker, target_date, rule_id, fundamental_score_percentile_blended
            FROM rc_fundamental_score_percentile
            ORDER BY ticker ASC
            LIMIT 1
            """
        ).fetchone()
        assert row_count == 500
        assert sample_row[1] == "2026-04-25"
        assert sample_row[2] == FUND_SCORE_PERCENTILE_V2_PRE
        assert sample_row[3] is not None


def _create_osakedata_db(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE ticker_meta (
                ticker TEXT PRIMARY KEY,
                market TEXT,
                sector TEXT,
                industry TEXT
            )
            """
        )
        conn.commit()


def _insert_meta_row(
    conn: sqlite3.Connection,
    ticker: str,
    market: str,
    sector: str | None,
    industry: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO ticker_meta (
            ticker,
            market,
            sector,
            industry
        ) VALUES (?, ?, ?, ?)
        """,
        (ticker, market, sector, industry),
    )


def _insert_percentile_ttm_row(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    growth: float | None,
    margin: float | None,
    margin_trend: float | None,
    fcf: float | None,
    leverage: float | None,
    dilution: float | None,
    consistency: float | None,
    lifecycle_score: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_ttm (
            ticker,
            as_of_date,
            latest_period_end_date,
            revenue_growth_ttm_yoy,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            fcf_margin_ttm,
            net_debt_to_ebitda,
            share_dilution_yoy,
            consistency_component_lifecycle,
            fundamental_score_lifecycle,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            as_of_date,
            as_of_date,
            growth,
            margin,
            margin_trend,
            fcf,
            leverage,
            dilution,
            consistency,
            lifecycle_score,
            "TTM_RUN_V1",
        ),
    )


def _snapshot_row(
    ticker: str,
    as_of_date: str,
    *,
    growth: float | None = 1.0,
    margin: float | None = 1.0,
    margin_trend: float | None = 1.0,
    fcf: float | None = 1.0,
    leverage: float | None = 1.0,
    dilution: float | None = 1.0,
    consistency: float | None = 1.0,
    lifecycle_score: float = 70.0,
    lifecycle_class: str | None = "UNCLASSIFIED",
    sector: str | None = None,
    industry: str | None = None,
):
    from swingmaster.fundamentals.score_percentile import PercentileSnapshotRow

    return PercentileSnapshotRow(
        ticker=ticker,
        as_of_date=as_of_date,
        revenue_growth_ttm_yoy=growth,
        ebit_margin_ttm=margin,
        ebit_margin_trend_4q=margin_trend,
        fcf_margin_ttm=fcf,
        net_debt_to_ebitda=leverage,
        share_dilution_yoy=dilution,
        consistency_component_lifecycle=consistency,
        fundamental_score_lifecycle=lifecycle_score,
        lifecycle_class=lifecycle_class,
        sector=sector,
        industry=industry,
    )


def _insert_large_percentile_dataset(
    fundamentals_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    row_count: int,
    market: str = "usa",
) -> None:
    for index in range(row_count):
        ticker = f"T{index:04d}"
        _insert_percentile_ttm_row(
            fundamentals_conn,
            ticker=ticker,
            as_of_date="2026-03-31",
            growth=float(index),
            margin=float(index) / 1000.0,
            margin_trend=float(index) / 2000.0,
            fcf=float(index) / 1500.0,
            leverage=float(row_count - index),
            dilution=float(row_count - index) / 1000.0,
            consistency=float(index % 20),
            lifecycle_score=60.0 + float(index % 40),
        )
        _insert_meta_row(
            osakedata_conn,
            ticker=ticker,
            market=market,
            sector="Technology",
            industry="Software",
        )
    fundamentals_conn.commit()
