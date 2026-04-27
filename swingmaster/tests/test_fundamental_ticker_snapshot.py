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
    write_snapshot_csv,
)


def test_build_snapshot_matrix_and_cli_output(monkeypatch, capsys, tmp_path: Path) -> None:
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
            growth=0.50,
            margin=0.30,
            margin_trend=0.20,
            fcf=0.25,
            leverage=2.0,
            dilution=0.02,
        )
        _insert_ttm_row(
            conn,
            ticker="VRT",
            as_of_date="2025-09-30",
            lifecycle_class="SCALING",
            fundamental_score=71.0,
            fundamental_score_lifecycle=75.1,
            growth=0.60,
            margin=0.31,
            margin_trend=0.21,
            fcf=0.26,
            leverage=1.5,
            dilution=0.03,
        )
        _insert_ttm_row(
            conn,
            ticker="VRT",
            as_of_date="2025-12-31",
            lifecycle_class="SCALING",
            fundamental_score=67.0,
            fundamental_score_lifecycle=71.1,
            growth=0.70,
            margin=0.32,
            margin_trend=0.22,
            fcf=0.27,
            leverage=1.0,
            dilution=0.04,
        )
        _insert_ttm_row(
            conn,
            ticker="VRT",
            as_of_date="2026-03-31",
            lifecycle_class="SCALING",
            fundamental_score=74.0,
            fundamental_score_lifecycle=77.8,
            growth=0.80,
            margin=0.33,
            margin_trend=0.23,
            fcf=0.28,
            leverage=0.8,
            dilution=0.05,
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
                growth=growth,
                margin=margin,
                margin_trend=margin_trend,
                fcf=fcf,
                leverage=leverage,
                dilution=dilution,
            )

        for target_date, score in (
            ("2025-06-30", 80.10),
            ("2025-09-30", 80.20),
            ("2025-12-31", 80.30),
            ("2026-03-31", 80.40),
        ):
            _insert_percentile_row(
                conn,
                ticker="VRT",
                as_of_date=target_date,
                target_date=target_date,
                sector="Technology",
                industry="Electrical Equipment",
                blended=score,
                blended_lifecycle=score + 0.5,
                sector_size=2,
                industry_size=2,
                global_score=score - 1.0,
                sector_score=score + 1.0,
                industry_score=score - 2.0,
                global_lifecycle=score - 0.5,
                sector_lifecycle=score + 1.5,
                industry_lifecycle=score - 1.5,
            )
            _insert_percentile_row(
                conn,
                ticker="PEER",
                as_of_date=target_date,
                target_date=target_date,
                sector="Technology",
                industry="Electrical Equipment",
                blended=70.00,
                blended_lifecycle=70.00,
                sector_size=2,
                industry_size=2,
                global_score=70.00,
                sector_score=70.00,
                industry_score=70.00,
                global_lifecycle=70.00,
                sector_lifecycle=70.00,
                industry_lifecycle=70.00,
            )
        conn.commit()

        matrix_rows = build_snapshot_matrix(conn, "VRT", 4, FUND_SCORE_PERCENTILE_V2_PRE, None)
        output = format_snapshot_matrix(matrix_rows)

    assert "ticker|VRT|VRT|VRT|VRT" in output
    assert "quarter|2025-06-30|2025-09-30|2025-12-31|2026-03-31" in output
    assert "fundamental_score_v1|73.00|71.00|67.00|74.00" in output
    assert "fundamental_score_v2_lifecycle|77.60|75.10|71.10|77.80" in output
    assert "growth_pct|100.00|100.00|100.00|100.00" in output
    assert "leverage_pct|100.00|100.00|100.00|100.00" in output
    assert "score_pct_blended_v2_pre|80.10|80.20|80.30|80.40" in output
    assert "score_pct_blended_v2_lifecycle_weighted|80.60|80.70|80.80|80.90" in output
    assert "sector_rank_position|Sijalla 1/2|Sijalla 1/2|Sijalla 1/2|Sijalla 1/2 (Technology)" in output
    assert "industry_rank_position|Sijalla 1/2|Sijalla 1/2|Sijalla 1/2|Sijalla 1/2 (Electrical Equipment)" in output

    csv_path = write_snapshot_csv(matrix_rows, "VRT", "2026-04-27")
    csv_content = csv_path.read_text(encoding="utf-8")
    assert "fundamental_score_v1;73,00;71,00;67,00;74,00" in csv_content
    assert "industry_rank_position;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2 (Electrical Equipment)" in csv_content
    ensured_csv_path = ensure_snapshot_csv_written(matrix_rows, "VRT", "2026-04-27")
    assert ensured_csv_path.exists()
    assert ensured_csv_path.stat().st_size > 0

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
    ticker_snapshot_main()
    cli_output = capsys.readouterr().out.strip()
    assert cli_output == output
    cli_csv_path = tmp_path / "ticker_fundamentals" / "VRT_2026-04-27.csv"
    assert cli_csv_path.exists()
    cli_csv_content = cli_csv_path.read_text(encoding="utf-8")
    assert "score_pct_blended_v2_pre;80,10;80,20;80,30;80,40" in cli_csv_content


def _insert_ttm_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    lifecycle_class: str,
    fundamental_score: float,
    fundamental_score_lifecycle: float,
    growth: float,
    margin: float,
    margin_trend: float,
    fcf: float,
    leverage: float,
    dilution: float,
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
            lifecycle_class,
            fundamental_score,
            fundamental_score_lifecycle,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            lifecycle_class,
            fundamental_score,
            fundamental_score_lifecycle,
            "TTM_RUN_V1",
        ),
    )


def _insert_percentile_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    target_date: str,
    sector: str,
    industry: str,
    blended: float,
    blended_lifecycle: float,
    sector_size: int,
    industry_size: int,
    global_score: float,
    sector_score: float,
    industry_score: float,
    global_lifecycle: float,
    sector_lifecycle: float,
    industry_lifecycle: float,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_score_percentile (
            ticker,
            as_of_date,
            target_date,
            sector,
            industry,
            rule_id,
            run_id,
            universe_size,
            sector_size,
            industry_size,
            growth_pct_global,
            growth_pct_sector,
            growth_pct_industry,
            margin_pct_global,
            margin_pct_sector,
            margin_pct_industry,
            margin_trend_pct_global,
            margin_trend_pct_sector,
            margin_trend_pct_industry,
            fcf_pct_global,
            fcf_pct_sector,
            fcf_pct_industry,
            leverage_pct_global,
            leverage_pct_sector,
            leverage_pct_industry,
            dilution_pct_global,
            dilution_pct_sector,
            dilution_pct_industry,
            consistency_pct_global,
            consistency_pct_sector,
            consistency_pct_industry,
            fundamental_score_percentile_global,
            fundamental_score_percentile_sector,
            fundamental_score_percentile_industry,
            fundamental_score_percentile_blended,
            created_at_utc,
            fundamental_score_percentile_global_lifecycle_weighted,
            fundamental_score_percentile_sector_lifecycle_weighted,
            fundamental_score_percentile_industry_lifecycle_weighted,
            fundamental_score_percentile_blended_lifecycle_weighted,
            percentile_lifecycle_weight_rule
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            sector_size,
            industry_size,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            90.0,
            global_score,
            sector_score,
            industry_score,
            blended,
            "2026-04-25T00:00:00Z",
            global_lifecycle,
            sector_lifecycle,
            industry_lifecycle,
            blended_lifecycle,
            "FUND_SCORE_PERCENTILE_V2_2_LIFECYCLE_MULT_PRE",
        ),
    )
