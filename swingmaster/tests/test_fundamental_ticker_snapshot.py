from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

from swingmaster.cli import run_fundamental_ticker_snapshot
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_ticker_snapshot import (
    FUND_SCORE_PERCENTILE_V2_PRE,
    build_snapshot_matrix,
    ensure_snapshot_csv_written,
    format_snapshot_matrix,
    main as ticker_snapshot_main,
)


def test_build_snapshot_matrix_cli_output_and_csv(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(
            conn,
            ticker="VRT",
            as_of_date="2025-06-30",
            lifecycle_class="SCALING",
            fundamental_score=73.0,
            fundamental_score_lifecycle=77.6,
            growth_component=15.0,
            margin_component=12.0,
            margin_trend_component=6.0,
            fcf_component=15.0,
            consistency_component=4.0,
            leverage_component=12.0,
            dilution_component=10.0,
            growth=0.50,
            margin=0.30,
            margin_trend=0.20,
            fcf=0.25,
            fcf_trend=0.05,
            leverage=2.0,
            dilution=0.02,
            latest_period_end_date="2025-06-30",
        )
        _insert_ttm_row(
            conn,
            ticker="VRT",
            as_of_date="2025-09-30",
            lifecycle_class="SCALING",
            fundamental_score=71.0,
            fundamental_score_lifecycle=75.1,
            growth_component=15.0,
            margin_component=12.0,
            margin_trend_component=6.0,
            fcf_component=15.0,
            consistency_component=6.0,
            leverage_component=12.0,
            dilution_component=10.0,
            growth=0.60,
            margin=0.31,
            margin_trend=0.21,
            fcf=0.26,
            fcf_trend=0.06,
            leverage=1.5,
            dilution=0.03,
            latest_period_end_date="2025-09-30",
        )
        _insert_ttm_row(
            conn,
            ticker="VRT",
            as_of_date="2025-12-31",
            lifecycle_class="SCALING",
            fundamental_score=67.0,
            fundamental_score_lifecycle=71.1,
            growth_component=15.0,
            margin_component=12.0,
            margin_trend_component=6.0,
            fcf_component=15.0,
            consistency_component=8.0,
            leverage_component=12.0,
            dilution_component=10.0,
            growth=0.70,
            margin=0.32,
            margin_trend=0.22,
            fcf=0.27,
            fcf_trend=0.07,
            leverage=1.0,
            dilution=0.04,
            latest_period_end_date="2025-12-31",
        )
        _insert_ttm_row(
            conn,
            ticker="VRT",
            as_of_date="2026-03-31",
            lifecycle_class="SCALING",
            fundamental_score=74.0,
            fundamental_score_lifecycle=77.8,
            growth_component=15.0,
            margin_component=15.0,
            margin_trend_component=9.0,
            fcf_component=15.0,
            consistency_component=10.0,
            leverage_component=12.0,
            dilution_component=10.0,
            growth=0.80,
            margin=0.33,
            margin_trend=0.23,
            fcf=0.28,
            fcf_trend=0.08,
            leverage=0.8,
            dilution=0.05,
            latest_period_end_date="2026-03-31",
        )
        for as_of_date, growth, margin, margin_trend, fcf, leverage, dilution in (
            ("2025-06-30", 0.20, 0.10, 0.05, 0.08, 3.0, 0.10),
            ("2025-09-30", 0.30, 0.11, 0.06, 0.09, 2.5, 0.09),
            ("2025-12-31", 0.40, 0.12, 0.07, 0.10, 2.0, 0.08),
            ("2026-03-31", 0.50, 0.13, 0.08, 0.11, 1.5, 0.07),
        ):
            _insert_ttm_row(
                conn,
                ticker="PEER",
                as_of_date=as_of_date,
                lifecycle_class="MATURE",
                fundamental_score=50.0,
                fundamental_score_lifecycle=50.0,
                growth_component=9.0,
                margin_component=9.0,
                margin_trend_component=5.0,
                fcf_component=9.0,
                consistency_component=4.0,
                leverage_component=8.0,
                dilution_component=8.0,
                growth=growth,
                margin=margin,
                margin_trend=margin_trend,
                fcf=fcf,
                fcf_trend=0.02,
                leverage=leverage,
                dilution=dilution,
                latest_period_end_date=as_of_date,
            )

        for period_end_date, revenue, operating_income, free_cashflow, shares_outstanding, total_debt in (
            ("2025-06-30", 1000.0, 300.0, 250.0, 10.0, 20.0),
            ("2025-09-30", 1100.0, 310.0, 260.0, 10.5, 19.0),
            ("2025-12-31", 1200.0, 320.0, 270.0, 11.0, 18.0),
            ("2026-03-31", 1300.0, 330.0, 280.0, 11.5, 17.0),
        ):
            _insert_quarterly_row(conn, "VRT", period_end_date, revenue, operating_income, free_cashflow, shares_outstanding, total_debt)

        for target_date, score in (
            ("2025-06-30", 80.10),
            ("2025-09-30", 80.20),
            ("2025-12-31", 80.30),
            ("2026-03-31", 80.40),
        ):
            _insert_percentile_row(conn, "VRT", target_date, target_date, "Technology", "Electrical Equipment", score, score + 0.5)
            _insert_percentile_row(conn, "PEER", target_date, target_date, "Technology", "Electrical Equipment", 70.00, 70.00)
        conn.commit()

        matrix_rows = build_snapshot_matrix(conn, "VRT", 4, FUND_SCORE_PERCENTILE_V2_PRE, None)
        output = format_snapshot_matrix(matrix_rows)

    assert "ticker;VRT;VRT;VRT;VRT" in output
    assert "quarter;2025-06-30;2025-09-30;2025-12-31;2026-03-31" in output
    assert "lifecycle_class;SCALING;SCALING;SCALING;SCALING" in output
    assert "fundamental_score_v1;73.00;71.00;67.00;74.00" in output
    assert "fundamental_score_v2_lifecycle;77.60;75.10;71.10;77.80" in output
    assert "score_lifecycle_delta;4.60;4.10;4.10;3.80" in output
    assert "growth_component (max 15p);15.00;15.00;15.00;15.00" in output
    assert "margin_component (max 15p);12.00;12.00;12.00;15.00" in output
    assert "revenue_growth_ttm_yoy;0.50;0.60;0.70;0.80" in output
    assert "fundamental_score_percentile_blended;80.10;80.20;80.30;80.40" in output
    assert "fundamental_score_percentile_blended_lifecycle_weighted;80.60;80.70;80.80;80.90" in output
    assert "percentile_rank_bucket;Top 20%;Top 20%;Top 20%;Top 20%" in output
    assert "percentile_lifecycle_delta;0.50;0.50;0.50;0.50" in output
    assert "growth_pct_global;90.00;90.00;90.00;90.00" in output
    assert "revenue;1000.00;1100.00;1200.00;1300.00" in output
    assert "margin_trend_delta_4q;;;;0.03" in output
    assert "fcf_margin_trend_delta_4q;;;;0.03" in output
    assert "shares_outstanding_delta_4q;;;;1.50" in output
    assert "net_debt_to_ebitda_delta_4q;;;;-1.20" in output
    assert "percentile_delta_4q;;;;0.80" in output
    assert "score_delta_4q;;;;0.20" in output
    assert "lifecycle_transition_4q;;;;SCALING to SCALING" in output
    assert "sector_rank_position;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2 (Technology)" in output
    assert "industry_rank_position;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2 (Electrical Equipment)" in output

    monkeypatch.setattr(
        run_fundamental_ticker_snapshot,
        "parse_args",
        lambda: SimpleNamespace(
            db=str(db_path),
            ticker="VRT",
            quarters=4,
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            percentile_target_date=None,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")
    ensured_csv = ensure_snapshot_csv_written(matrix_rows, "VRT", "2026-04-27")
    assert ensured_csv.exists()
    assert ensured_csv.stat().st_size > 0
    ticker_snapshot_main()
    cli_output = capsys.readouterr().out.strip()
    assert cli_output == output
    cli_csv_path = tmp_path / "ticker_fundamentals" / "VRT_2026-04-27.csv"
    assert cli_csv_path.exists()
    cli_csv_content = cli_csv_path.read_text(encoding="utf-8")
    assert "fundamental_score_v1;73,00;71,00;67,00;74,00" in cli_csv_content
    assert "growth_component (max 15p);15,00;15,00;15,00;15,00" in cli_csv_content
    assert "percentile_rank_bucket;Top 20%;Top 20%;Top 20%;Top 20%" in cli_csv_content
    assert "percentile_delta_4q;;;;0,80" in cli_csv_content


def _insert_ttm_row(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    lifecycle_class: str,
    fundamental_score: float,
    fundamental_score_lifecycle: float,
    growth_component: float,
    margin_component: float,
    margin_trend_component: float,
    fcf_component: float,
    consistency_component: float,
    leverage_component: float,
    dilution_component: float,
    growth: float,
    margin: float,
    margin_trend: float,
    fcf: float,
    fcf_trend: float,
    leverage: float,
    dilution: float,
    latest_period_end_date: str,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_ttm (
            ticker, as_of_date, latest_period_end_date,
            revenue_growth_ttm_yoy, ebit_margin_ttm, ebit_margin_trend_4q, fcf_margin_ttm, fcf_margin_trend_4q,
            net_debt_to_ebitda, share_dilution_yoy, lifecycle_class,
            fundamental_score, fundamental_score_lifecycle,
            growth_component, margin_component, margin_trend_component, fcf_component,
            consistency_component, leverage_component, dilution_component, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            as_of_date,
            latest_period_end_date,
            growth,
            margin,
            margin_trend,
            fcf,
            fcf_trend,
            leverage,
            dilution,
            lifecycle_class,
            fundamental_score,
            fundamental_score_lifecycle,
            growth_component,
            margin_component,
            margin_trend_component,
            fcf_component,
            consistency_component,
            leverage_component,
            dilution_component,
            "TTM_RUN_V1",
        ),
    )


def _insert_quarterly_row(
    conn: sqlite3.Connection,
    ticker: str,
    period_end_date: str,
    revenue: float,
    operating_income: float,
    free_cashflow: float,
    shares_outstanding: float,
    total_debt: float,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker, period_end_date, revenue, operating_income, free_cashflow, shares_outstanding, total_debt, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ticker, period_end_date, revenue, operating_income, free_cashflow, shares_outstanding, total_debt, "Q_RUN_V1"),
    )


def _insert_percentile_row(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    target_date: str,
    sector: str,
    industry: str,
    blended: float,
    blended_lifecycle: float,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_score_percentile (
            ticker, as_of_date, target_date, sector, industry, rule_id, run_id,
            universe_size, sector_size, industry_size,
            growth_pct_global, margin_pct_global, margin_trend_pct_global, fcf_pct_global,
            leverage_pct_global, dilution_pct_global, consistency_pct_global,
            fundamental_score_percentile_global, fundamental_score_percentile_sector,
            fundamental_score_percentile_industry, fundamental_score_percentile_blended,
            created_at_utc,
            fundamental_score_percentile_global_lifecycle_weighted,
            fundamental_score_percentile_sector_lifecycle_weighted,
            fundamental_score_percentile_industry_lifecycle_weighted,
            fundamental_score_percentile_blended_lifecycle_weighted,
            percentile_lifecycle_weight_rule
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            as_of_date,
            target_date,
            sector,
            industry,
            FUND_SCORE_PERCENTILE_V2_PRE,
            "PCT_RUN_V1",
            2,
            2,
            2,
            90.0,
            80.0,
            70.0,
            60.0,
            50.0,
            40.0,
            30.0,
            blended - 1.0,
            blended + 1.0,
            blended - 2.0,
            blended,
            "2026-04-25T00:00:00Z",
            blended_lifecycle - 1.0,
            blended_lifecycle + 1.0,
            blended_lifecycle - 2.0,
            blended_lifecycle,
            "FUND_SCORE_PERCENTILE_V2_2_LIFECYCLE_MULT_PRE",
        ),
    )
