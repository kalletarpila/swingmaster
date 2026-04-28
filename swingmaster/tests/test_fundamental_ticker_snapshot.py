from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
import pytest

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
    assert "score_delta_qoq;;-2.50;-4.00;6.70" in output
    assert "percentile_delta_qoq;;0.10;0.10;0.10" in output
    assert "margin_trend_delta_qoq;;0.01;0.01;0.01" in output
    assert "fcf_margin_trend_delta_qoq;;0.01;0.01;0.01" in output
    assert "consistency_delta_qoq;;0.00;0.00;0.00" in output
    assert "growth_pct_global_delta_qoq;;0.00;0.00;0.00" in output
    assert "shares_outstanding_delta_4q;;;;1.50" in output
    assert "net_debt_to_ebitda_delta_4q;;;;-1.20" in output
    assert "percentile_delta_4q;;;;0.80" in output
    assert "score_delta_4q;;;;0.20" in output
    assert "lifecycle_transition_4q;;;;SCALING to SCALING" in output
    assert "sector_rank_position;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2 (Technology)" in output
    assert "industry_rank_position;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2;Sijalla 1/2 (Electrical Equipment)" in output
    assert output.index("fcf_margin_trend_delta_4q;;;;0.03") < output.index("score_delta_qoq;;-2.50;-4.00;6.70")
    assert output.index("growth_pct_global_delta_qoq;;0.00;0.00;0.00") < output.index("shares_outstanding_delta_4q;;;;1.50")

    monkeypatch.setattr(
        run_fundamental_ticker_snapshot,
        "parse_args",
        lambda: SimpleNamespace(
            db=str(db_path),
            ticker="VRT",
            quarters=4,
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            percentile_target_date=None,
            ohlcv_db=None,
            price_behavior_snapshot=False,
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
    assert "score_delta_qoq;;-2,50;-4,00;6,70" in cli_csv_content
    assert "percentile_delta_qoq;;0,10;0,10;0,10" in cli_csv_content
    assert "percentile_delta_4q;;;;0,80" in cli_csv_content


def test_price_behavior_snapshot_requires_ohlcv_db(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_ticker_snapshot,
        "parse_args",
        lambda: SimpleNamespace(
            db=str(db_path),
            ticker="VRT",
            quarters=4,
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            percentile_target_date=None,
            ohlcv_db=None,
            price_behavior_snapshot=True,
        ),
    )
    with pytest.raises(RuntimeError, match="PRICE_BEHAVIOR_SNAPSHOT_REQUIRES_OHLCV_DB"):
        ticker_snapshot_main()


def test_price_behavior_snapshot_stdout_and_csv(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)

    with sqlite3.connect(str(db_path)) as conn:
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
        _insert_quarterly_row(conn, "VRT", "2025-12-31", 1200.0, 320.0, 270.0, 11.0, 18.0)
        _insert_quarterly_row(conn, "VRT", "2026-03-31", 1300.0, 330.0, 280.0, 11.5, 17.0)
        _insert_percentile_row(conn, "VRT", "2025-12-31", "2025-12-31", "Technology", "Electrical Equipment", 80.30, 80.80)
        _insert_percentile_row(conn, "VRT", "2026-03-31", "2026-03-31", "Technology", "Electrical Equipment", 80.40, 80.90)
        conn.commit()

    _insert_ohlcv_series(
        ohlcv_db_path,
        "VRT",
        300,
        anchor_close=400.0,
        anchor_date="2026-04-30",
        report_date="2026-03-31",
        report_day_close=300.0,
        close_1_after_report=303.0,
        close_3_after_report=309.0,
        close_20_after_report=330.0,
        volume_before_report=100.0,
        volume_after_report=250.0,
    )
    _insert_ohlcv_series(
        ohlcv_db_path,
        "^GSPC",
        300,
        anchor_close=200.0,
        anchor_date="2026-04-30",
        report_date="2026-03-31",
        report_day_close=180.0,
        close_1_after_report=181.0,
        close_3_after_report=183.0,
        close_20_after_report=190.0,
        return_6m_pct=14.59,
    )

    monkeypatch.setattr(
        run_fundamental_ticker_snapshot,
        "parse_args",
        lambda: SimpleNamespace(
            db=str(db_path),
            ticker="VRT",
            quarters=2,
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            percentile_target_date=None,
            ohlcv_db=str(ohlcv_db_path),
            price_behavior_snapshot=True,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")
    ticker_snapshot_main()
    cli_output = capsys.readouterr().out.strip()

    assert "price_behavior_snapshot" in cli_output
    assert "price_behavior_as_of_date;2026-04-30" in cli_output
    assert "price_return_3m_pct;9.29" in cli_output
    assert "price_return_6m_pct;20.48" in cli_output
    assert "price_return_12m_pct;48.15" in cli_output
    assert "distance_from_52w_high_pct;-9.09" in cli_output
    assert "relative_strength_6m_vs_sp500_pct;5.89" in cli_output
    assert "price_return_since_last_report_pct;33.33" in cli_output
    assert "relative_return_vs_sp500_since_last_report_pct;22.22" in cli_output
    assert "earnings_reaction_1d_pct;1.00" in cli_output
    assert "earnings_reaction_3d_pct;3.00" in cli_output
    assert "post_earnings_drift_20d_pct;10.00" in cli_output
    assert "volume_ratio_since_last_report_vs_3m_avg;2.50" in cli_output
    assert "price_return_3m_pct;9.29;" not in cli_output
    assert cli_output.index("earnings_reaction_1d_pct;1.00") < cli_output.index("post_earnings_drift_20d_pct;10.00")
    assert cli_output.index("earnings_reaction_3d_pct;3.00") < cli_output.index("post_earnings_drift_20d_pct;10.00")

    cli_csv_path = tmp_path / "ticker_fundamentals" / "VRT_2026-04-27.csv"
    cli_csv_content = cli_csv_path.read_text(encoding="utf-8")
    assert "price_behavior_snapshot" in cli_csv_content
    assert "price_behavior_as_of_date;2026-04-30" in cli_csv_content
    assert "price_return_3m_pct;9,29" in cli_csv_content
    assert "relative_strength_6m_vs_sp500_pct;5,89" in cli_csv_content
    assert "earnings_reaction_1d_pct;1,00" in cli_csv_content
    assert "earnings_reaction_3d_pct;3,00" in cli_csv_content


def test_price_behavior_snapshot_missing_benchmark_and_future_data(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(
            conn,
            ticker="VRT",
            as_of_date="2026-04-09",
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
            latest_period_end_date="2026-04-09",
        )
        _insert_quarterly_row(conn, "VRT", "2026-04-09", 1300.0, 330.0, 280.0, 11.5, 17.0)
        _insert_percentile_row(conn, "VRT", "2026-04-09", "2026-04-09", "Technology", "Electrical Equipment", 80.40, 80.90)
        conn.commit()

    _insert_ohlcv_series(
        ohlcv_db_path,
        "VRT",
        260,
        anchor_close=400.0,
        anchor_date="2026-04-10",
        report_date="2026-04-09",
        report_day_close=390.0,
        close_1_after_report=395.0,
        close_3_after_report=405.0,
        close_20_after_report=410.0,
    )

    monkeypatch.setattr(
        run_fundamental_ticker_snapshot,
        "parse_args",
        lambda: SimpleNamespace(
            db=str(db_path),
            ticker="VRT",
            quarters=1,
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            percentile_target_date=None,
            ohlcv_db=str(ohlcv_db_path),
            price_behavior_snapshot=True,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")
    ticker_snapshot_main()
    cli_output = capsys.readouterr().out.strip()

    assert "relative_strength_6m_vs_sp500_pct;" in cli_output
    assert "relative_strength_6m_vs_sp500_pct;;" not in cli_output
    assert "post_earnings_drift_20d_pct;" in cli_output
    assert "post_earnings_drift_20d_pct;;" not in cli_output
    for line in cli_output.splitlines():
        if line.startswith("relative_strength_6m_vs_sp500_pct;"):
            assert line == "relative_strength_6m_vs_sp500_pct;"
        if line.startswith("relative_return_vs_sp500_since_last_report_pct;"):
            assert line == "relative_return_vs_sp500_since_last_report_pct;"
        if line.startswith("earnings_reaction_1d_pct;"):
            assert line != "earnings_reaction_1d_pct;"
        if line.startswith("earnings_reaction_3d_pct;"):
            assert line == "earnings_reaction_3d_pct;"
        if line.startswith("post_earnings_drift_20d_pct;"):
            assert line == "post_earnings_drift_20d_pct;"


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


def _create_ohlcv_schema(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE osakedata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                osake TEXT,
                pvm TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                market TEXT DEFAULT 'usa',
                UNIQUE(osake, pvm)
            )
            """
        )
        conn.commit()


def _insert_ohlcv_series(
    db_path: Path,
    ticker: str,
    length: int,
    anchor_close: float,
    anchor_date: str,
    report_date: str,
    report_day_close: float,
    close_1_after_report: float,
    close_3_after_report: float,
    close_20_after_report: float,
    return_3m_pct: float = 9.29,
    return_6m_pct: float = 20.48,
    return_12m_pct: float = 48.15,
    high_multiplier: float = 1.10,
    volume_before_report: float = 100.0,
    volume_after_report: float = 250.0,
) -> None:
    anchor_day = date.fromisoformat(anchor_date)
    start_day = anchor_day - timedelta(days=length - 1)
    report_day = date.fromisoformat(report_date)
    report_index = (report_day - start_day).days
    with sqlite3.connect(str(db_path)) as conn:
        for index in range(length):
            date_text = (start_day + timedelta(days=index)).isoformat()
            close = 100.0 + index
            if index == length - 1:
                close = anchor_close
            if index == length - 1 - 63:
                close = anchor_close / (1.0 + return_3m_pct / 100.0)
            if index == length - 1 - 126:
                close = anchor_close / (1.0 + return_6m_pct / 100.0)
            if index == length - 1 - 252:
                close = anchor_close / (1.0 + return_12m_pct / 100.0)
            if index == report_index:
                close = report_day_close
            if index == report_index + 1 and report_index + 1 < length:
                close = close_1_after_report
            if index == report_index + 3 and report_index + 3 < length:
                close = close_3_after_report
            if index == report_index + 20 and report_index + 20 < length:
                close = close_20_after_report
            high = close
            if index == length - 2:
                high = anchor_close * high_multiplier
            volume = volume_before_report
            if index > report_index:
                volume = volume_after_report
            conn.execute(
                """
                INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'usa')
                """,
                (ticker.upper(), date_text, close, high, close, close, volume),
            )
        conn.commit()


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
