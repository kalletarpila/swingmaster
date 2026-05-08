from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
import sys
import pytest

from swingmaster.cli import run_fundamental_ticker_snapshot
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_ticker_snapshot import (
    FUND_SCORE_PERCENTILE_V2_PRE,
    build_snapshot_matrix,
    ensure_snapshot_csv_written,
    format_snapshot_matrix,
    load_latest_valuation_snapshot,
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
        valuation_snapshot = load_latest_valuation_snapshot(conn, "VRT")
        output = format_snapshot_matrix(matrix_rows, valuation_snapshot=valuation_snapshot)

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
    ensured_csv = ensure_snapshot_csv_written(matrix_rows, "VRT", "2026-04-27", valuation_snapshot=valuation_snapshot)
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
    assert "valuation_snapshot" in cli_output
    assert "valuation_date;" in cli_output
    assert "valuation_snapshot" in cli_csv_content
    assert "valuation_date;" in cli_csv_content


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
    assert "valuation_snapshot" in cli_output
    assert cli_output.index("price_behavior_snapshot") < cli_output.index("valuation_snapshot")
    assert "price_return_3m_pct;9.29;" not in cli_output
    assert cli_output.index("earnings_reaction_1d_pct;1.00") < cli_output.index("post_earnings_drift_20d_pct;10.00")
    assert cli_output.index("earnings_reaction_3d_pct;3.00") < cli_output.index("post_earnings_drift_20d_pct;10.00")

    cli_csv_path = tmp_path / "ticker_fundamentals" / "VRT_2026-04-27.csv"
    cli_csv_content = cli_csv_path.read_text(encoding="utf-8")
    assert "price_behavior_snapshot" in cli_csv_content
    assert "price_behavior_as_of_date;2026-04-30" in cli_csv_content
    assert "valuation_snapshot" in cli_csv_content
    assert "price_return_3m_pct;9,29" in cli_csv_content
    assert "relative_strength_6m_vs_sp500_pct;5,89" in cli_csv_content
    assert "earnings_reaction_1d_pct;1,00" in cli_csv_content
    assert "earnings_reaction_3d_pct;3,00" in cli_csv_content


def test_price_behavior_snapshot_uses_omxh_benchmark_for_he_ticker(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_omxh.db"
    ohlcv_db_path = tmp_path / "osakedata_omxh.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(
            conn,
            ticker="NOKIA.HE",
            as_of_date="2026-03-31",
            lifecycle_class="TRANSITION",
            fundamental_score=44.0,
            fundamental_score_lifecycle=47.2,
            growth_component=6.0,
            margin_component=4.0,
            margin_trend_component=6.0,
            fcf_component=8.0,
            consistency_component=0.0,
            leverage_component=15.0,
            dilution_component=5.0,
            growth=0.10,
            margin=0.04,
            margin_trend=0.02,
            fcf=0.07,
            fcf_trend=0.01,
            leverage=-1.43,
            dilution=0.04,
            latest_period_end_date="2026-03-31",
        )
        _insert_quarterly_row(conn, "NOKIA.HE", "2026-03-31", 1300.0, 330.0, 280.0, 11.5, 17.0)
        _insert_percentile_row(conn, "NOKIA.HE", "2026-03-31", "2026-03-31", "Technology", "Communication Equipment", 46.90, 46.74)
        conn.commit()

    _insert_ohlcv_series(
        ohlcv_db_path,
        "NOKIA.HE",
        300,
        anchor_close=400.0,
        anchor_date="2026-04-30",
        report_date="2026-03-31",
        report_day_close=300.0,
        close_1_after_report=303.0,
        close_3_after_report=309.0,
        close_20_after_report=330.0,
        market="omxh",
    )
    _insert_ohlcv_series(
        ohlcv_db_path,
        "^OMXH25",
        300,
        anchor_close=200.0,
        anchor_date="2026-04-30",
        report_date="2026-03-31",
        report_day_close=180.0,
        close_1_after_report=181.0,
        close_3_after_report=183.0,
        close_20_after_report=190.0,
        return_6m_pct=14.59,
        market="omxh",
    )

    monkeypatch.setattr(
        run_fundamental_ticker_snapshot,
        "parse_args",
        lambda: SimpleNamespace(
            db=str(db_path),
            ticker="NOKIA.HE",
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

    assert "relative_strength_6m_vs_sp500_pct;5.89" in cli_output
    assert "relative_return_vs_sp500_since_last_report_pct;22.22" in cli_output


def test_snapshot_keeps_exact_match_quarter_valuation_rows_and_adds_latest_valuation_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_valuation.db"
    run_migration(db_path)

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
        conn.execute(
            """
            INSERT INTO rc_fundamental_valuation (
                ticker, as_of_date, valuation_ev_ebit, valuation_fcf_yield, valuation_ebit_margin, adjusted_expensive_threshold,
                valuation_model_version, valuation_fundamental_as_of_date, valuation_fundamental_staleness_days,
                valuation_bucket, valuation_status, debt_assumed_zero, cash_assumed_zero, market_cap,
                enterprise_value, close_price, shares_outstanding, cash, total_debt, ebit_ttm,
                fundamental_score_lifecycle, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("VRT", "2026-03-31", 12.34, 0.05, 0.23, 28.0, "V2", "2025-12-31", 90, "FAIR", "OK", 1, 0, 100.0, 90.0, 10.0, 10.0, 5.0, 0.0, 7.0, 77.8, "VAL_RUN", "2026-04-25T00:00:00Z"),
        )
        conn.commit()

        matrix_rows = build_snapshot_matrix(conn, "VRT", 2, FUND_SCORE_PERCENTILE_V2_PRE, None)
        valuation_snapshot = load_latest_valuation_snapshot(conn, "VRT")
        output = format_snapshot_matrix(matrix_rows, valuation_snapshot=valuation_snapshot)

    assert "valuation_date;;2026-03-31" in output
    assert "valuation_fundamental_as_of_date;;2025-12-31" in output
    assert "valuation_fundamental_staleness_days;;90" in output
    assert "valuation_ev_ebit;;12.34" in output
    assert "valuation_fcf_yield;;0.05" in output
    assert "valuation_ebit_margin;;0.23" in output
    assert "valuation_bucket;;FAIR" in output
    assert "valuation_status;;OK" in output
    assert "valuation_model_version;;V2" in output
    assert output.index("valuation_status;;OK") < output.index("revenue;1200.00;1300.00")
    assert "valuation_snapshot" in output
    assert "valuation_date;2026-03-31" in output
    assert "valuation_fundamental_as_of_date;2025-12-31" in output
    assert "valuation_fundamental_staleness_days;90" in output
    assert "valuation_ev_ebit;12.34" in output
    assert "valuation_fcf_yield;0.05" in output
    assert "valuation_ebit_margin;0.23" in output
    assert "adjusted_expensive_threshold;28.00" in output
    assert "valuation_debt_assumed_zero;1" in output
    assert "valuation_cash_assumed_zero;0" in output
    assert "valuation_bucket;FAIR" in output
    assert "valuation_status;OK" in output
    assert "valuation_model_version;V2" in output


def test_delta_formatted_treats_empty_string_as_missing() -> None:
    from swingmaster.cli.run_fundamental_ticker_snapshot import _delta_formatted

    assert _delta_formatted("", "10.0") == ""
    assert _delta_formatted("10.0", "") == ""


def test_valuation_snapshot_uses_latest_row_by_ticker(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_valuation_latest.db"
    run_migration(db_path)

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
        _insert_quarterly_row(conn, "VRT", "2025-12-31", 1200.0, 320.0, 270.0, 11.0, 18.0)
        conn.execute(
            """
            INSERT INTO rc_fundamental_valuation (
                ticker, as_of_date, valuation_ev_ebit, valuation_fcf_yield, valuation_ebit_margin, adjusted_expensive_threshold,
                valuation_model_version, valuation_fundamental_as_of_date, valuation_fundamental_staleness_days,
                valuation_bucket, valuation_status, debt_assumed_zero, cash_assumed_zero, market_cap,
                enterprise_value, close_price, shares_outstanding, cash, total_debt, ebit_ttm,
                fundamental_score_lifecycle, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("VRT", "2025-12-31", 10.0, 0.07, 0.20, 28.0, "V2", "2025-12-31", 0, "CHEAP", "OK", 0, 0, 100.0, 90.0, 10.0, 10.0, 5.0, 0.0, 7.0, 71.1, "VAL_RUN_1", "2026-04-25T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_valuation (
                ticker, as_of_date, valuation_ev_ebit, valuation_fcf_yield, valuation_ebit_margin, adjusted_expensive_threshold,
                valuation_model_version, valuation_fundamental_as_of_date, valuation_fundamental_staleness_days,
                valuation_bucket, valuation_status, debt_assumed_zero, cash_assumed_zero, market_cap,
                enterprise_value, close_price, shares_outstanding, cash, total_debt, ebit_ttm,
                fundamental_score_lifecycle, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("VRT", "2026-03-31", 12.34, 0.05, 0.23, 28.0, "V2", "2025-12-31", 90, "FAIR", "OK", 1, 0, 100.0, 90.0, 10.0, 10.0, 5.0, 0.0, 7.0, 71.1, "VAL_RUN_2", "2026-04-25T00:00:00Z"),
        )
        conn.commit()

        matrix_rows = build_snapshot_matrix(conn, "VRT", 1, FUND_SCORE_PERCENTILE_V2_PRE, None)
        valuation_snapshot = load_latest_valuation_snapshot(conn, "VRT")
        output = format_snapshot_matrix(matrix_rows, valuation_snapshot=valuation_snapshot)

    assert "valuation_snapshot" in output
    assert "valuation_date;2026-03-31" in output
    assert "valuation_bucket;FAIR" in output


def test_valuation_snapshot_block_present_with_empty_values_when_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_valuation_missing.db"
    run_migration(db_path)

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
        _insert_quarterly_row(conn, "VRT", "2025-12-31", 1200.0, 320.0, 270.0, 11.0, 18.0)
        conn.commit()

        matrix_rows = build_snapshot_matrix(conn, "VRT", 1, FUND_SCORE_PERCENTILE_V2_PRE, None)
        valuation_snapshot = load_latest_valuation_snapshot(conn, "VRT")
        output = format_snapshot_matrix(matrix_rows, valuation_snapshot=valuation_snapshot)

    assert "valuation_snapshot" in output
    assert "valuation_date;" in output
    assert "valuation_fundamental_as_of_date;" in output
    assert "valuation_fundamental_staleness_days;" in output
    assert "valuation_ev_ebit;" in output
    assert "valuation_fcf_yield;" in output
    assert "valuation_ebit_margin;" in output
    assert "adjusted_expensive_threshold;" in output
    assert "valuation_debt_assumed_zero;" in output
    assert "valuation_cash_assumed_zero;" in output
    assert "valuation_bucket;" in output
    assert "valuation_status;" in output
    assert "valuation_model_version;" in output


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


def test_backward_compatibility_without_dow_flag(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_no_dow.db"
    run_migration(db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")

    monkeypatch.setattr(
        run_fundamental_ticker_snapshot,
        "parse_args",
        lambda: SimpleNamespace(
            db=str(db_path),
            ticker="VRT",
            quarters=1,
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            percentile_target_date=None,
            ohlcv_db=None,
            price_behavior_snapshot=False,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out.strip()

    assert "section;dow_context_snapshot" not in cli_output
    assert "section;dow_recent_events_60td" not in cli_output


def test_dow_snapshot_validation_requires_analysis_db(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_fundamental_ticker_snapshot.py",
            "--db",
            "fund.db",
            "--ticker",
            "VRT",
            "--dow-structure-snapshot",
            "--ohlcv-db",
            "os.db",
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        run_fundamental_ticker_snapshot.parse_args()


def test_dow_snapshot_validation_requires_ohlcv_db(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_fundamental_ticker_snapshot.py",
            "--db",
            "fund.db",
            "--ticker",
            "VRT",
            "--dow-structure-snapshot",
            "--dow-analysis-db",
            "analysis.db",
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        run_fundamental_ticker_snapshot.parse_args()


def test_dow_snapshot_appends_sections_and_headers(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_dow.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_dow_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")
    _insert_dow_status(analysis_db_path, "VRT", "usa", "2026-04-30", "OK")
    _insert_dow_event(analysis_db_path, 1, "VRT", "usa", "2026-04-29", "2026-04-30", "TREND_CHANGE")

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
            price_behavior_snapshot=False,
            dow_structure_snapshot=True,
            dow_analysis_db=str(analysis_db_path),
            dow_as_of_date="2026-04-30",
            dow_market="usa",
            dow_pivot_radius=3,
            dow_price_source="close",
            dow_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out.strip()

    assert "section;dow_context_snapshot" in cli_output
    assert "section;dow_recent_events_60td" in cli_output
    assert "ticker;market;as_of_date;price_source;pivot_radius;" in cli_output
    assert "sequence_window_trading_days;sequence_available_trading_days;sequence_window_start_date;sequence_window_end_date;sequence_index;" in cli_output


def test_dow_context_row_uses_latest_confirmed_event_from_event_table(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_dow_context.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_dow_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_ohlcv_close(ohlcv_db_path, "VRT", pvm, 400.0, "usa")
    _insert_dow_status(
        analysis_db_path,
        "VRT",
        "usa",
        "2026-04-30",
        "OK",
        latest_event_date="2026-04-30",
        latest_event_confirmed_as_of_date="2099-01-01",
    )
    _insert_dow_event(analysis_db_path, 1, "VRT", "usa", "2026-04-28", "2026-04-29", "PIVOT_LOW")
    _insert_dow_event(analysis_db_path, 2, "VRT", "usa", "2026-04-29", "2026-04-30", "BOS_UP")

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
            price_behavior_snapshot=False,
            dow_structure_snapshot=True,
            dow_analysis_db=str(analysis_db_path),
            dow_as_of_date="2026-04-30",
            dow_market="usa",
            dow_pivot_radius=3,
            dow_price_source="close",
            dow_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert ";OK;calculated_through_date covers latest valid close date;true;2;BOS_UP;2026-04-29;2026-04-30;" in cli_output
    assert "2099-01-01" not in cli_output


def test_dow_snapshot_no_lookahead_and_recent_sequence_order(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_dow_sequence.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_dow_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    for pvm in ("2026-04-27", "2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_ohlcv_close(ohlcv_db_path, "VRT", pvm, 400.0, "usa")
    _insert_dow_status(analysis_db_path, "VRT", "usa", "2026-04-30", "OK")
    _insert_dow_event(analysis_db_path, 2, "VRT", "usa", "2026-04-29", "2026-04-30", "TREND_CHANGE")
    _insert_dow_event(analysis_db_path, 1, "VRT", "usa", "2026-04-28", "2026-04-29", "PIVOT_LOW")
    _insert_dow_event(analysis_db_path, 3, "VRT", "usa", "2026-04-30", "2026-05-02", "RESET")

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
            price_behavior_snapshot=False,
            dow_structure_snapshot=True,
            dow_analysis_db=str(analysis_db_path),
            dow_as_of_date="2026-04-30",
            dow_market="usa",
            dow_pivot_radius=3,
            dow_price_source="close",
            dow_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "latest_event_type;BOS_UP" not in cli_output
    assert "2026-05-02" not in cli_output
    recent_section = cli_output.split("section;dow_recent_events_60td\n", 1)[1].strip().splitlines()
    event_rows = [line.split(";") for line in recent_section[1:]]
    assert [row[9] for row in event_rows] == ["1", "2"]
    assert [row[10] for row in event_rows] == ["1", "2"]
    assert [row[13] for row in event_rows] == ["PIVOT_LOW", "TREND_CHANGE"]


def test_dow_snapshot_has_no_summary_counts(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_no_summary.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_dow_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")
    _insert_dow_status(analysis_db_path, "VRT", "usa", "2026-04-30", "OK")

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
            price_behavior_snapshot=False,
            dow_structure_snapshot=True,
            dow_analysis_db=str(analysis_db_path),
            dow_as_of_date="2026-04-30",
            dow_market="usa",
            dow_pivot_radius=3,
            dow_price_source="close",
            dow_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "recent_event_summary" not in cli_output
    assert "bos_up_count_60td" not in cli_output
    assert "reset_count_60td" not in cli_output
    assert "trend_change_count_60td" not in cli_output


def test_dow_as_of_date_derives_from_price_behavior_snapshot(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_derived_price_behavior.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_dow_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_series(
        ohlcv_db_path,
        "VRT",
        260,
        anchor_close=400.0,
        anchor_date="2026-04-30",
        report_date="2026-03-31",
        report_day_close=300.0,
        close_1_after_report=303.0,
        close_3_after_report=309.0,
        close_20_after_report=330.0,
    )
    _insert_ohlcv_series(
        ohlcv_db_path,
        "^GSPC",
        260,
        anchor_close=200.0,
        anchor_date="2026-04-30",
        report_date="2026-03-31",
        report_day_close=180.0,
        close_1_after_report=181.0,
        close_3_after_report=183.0,
        close_20_after_report=190.0,
        return_6m_pct=14.59,
    )
    _insert_dow_status(analysis_db_path, "VRT", "usa", "2026-04-30", "OK")
    _insert_dow_event(analysis_db_path, 1, "VRT", "usa", "2026-04-29", "2026-04-30", "TREND_CHANGE")

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
            dow_structure_snapshot=True,
            dow_analysis_db=str(analysis_db_path),
            dow_as_of_date=None,
            dow_market=None,
            dow_pivot_radius=3,
            dow_price_source="close",
            dow_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2099-12-31")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "price_behavior_as_of_date;2026-04-30" in cli_output
    assert ";usa;2026-04-30;close;3;" in cli_output
    assert "2099-12-31;close;3;" not in cli_output


def test_dow_as_of_date_falls_back_to_latest_valid_close_date(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_derived_close.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_dow_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-29", 390.0, "usa")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")
    _insert_dow_status(analysis_db_path, "VRT", "usa", "2026-04-30", "OK")

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
            price_behavior_snapshot=False,
            dow_structure_snapshot=True,
            dow_analysis_db=str(analysis_db_path),
            dow_as_of_date=None,
            dow_market="usa",
            dow_pivot_radius=3,
            dow_price_source="close",
            dow_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2099-12-31")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert ";usa;2026-04-30;close;3;" in cli_output
    assert "2099-12-31;close;3;" not in cli_output


def test_dow_snapshot_does_not_write_to_analysis_or_ohlcv_db(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_no_write.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_dow_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")
    _insert_dow_status(analysis_db_path, "VRT", "usa", "2026-04-30", "OK")
    _insert_dow_event(analysis_db_path, 1, "VRT", "usa", "2026-04-29", "2026-04-30", "TREND_CHANGE")

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
            price_behavior_snapshot=False,
            dow_structure_snapshot=True,
            dow_analysis_db=str(analysis_db_path),
            dow_as_of_date="2026-04-30",
            dow_market="usa",
            dow_pivot_radius=3,
            dow_price_source="close",
            dow_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    before_analysis_events = _count_rows(analysis_db_path, "stock_dow_structure_events")
    before_analysis_status = _count_rows(analysis_db_path, "stock_dow_structure_status")
    before_ohlcv = _count_rows(ohlcv_db_path, "osakedata")

    ticker_snapshot_main()

    assert _count_rows(analysis_db_path, "stock_dow_structure_events") == before_analysis_events
    assert _count_rows(analysis_db_path, "stock_dow_structure_status") == before_analysis_status
    assert _count_rows(ohlcv_db_path, "osakedata") == before_ohlcv


def test_dow_snapshot_prints_empty_recent_event_section(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_empty_recent.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_dow_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")
    _insert_dow_status(analysis_db_path, "VRT", "usa", "2026-04-30", "OK")

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
            price_behavior_snapshot=False,
            dow_structure_snapshot=True,
            dow_analysis_db=str(analysis_db_path),
            dow_as_of_date="2026-04-30",
            dow_market="usa",
            dow_pivot_radius=3,
            dow_price_source="close",
            dow_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "section;dow_context_snapshot" in cli_output
    assert "section;dow_recent_events_60td" in cli_output
    recent_section = cli_output.split("section;dow_recent_events_60td\n", 1)[1]
    recent_lines = recent_section.strip().splitlines()
    assert recent_lines[0].startswith("ticker;market;as_of_date;price_source;pivot_radius;sequence_window_trading_days;")
    assert len(recent_lines) == 1


def test_backward_compatibility_without_candlestick_flag(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_no_candlestick.db"
    run_migration(db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")

    monkeypatch.setattr(
        run_fundamental_ticker_snapshot,
        "parse_args",
        lambda: SimpleNamespace(
            db=str(db_path),
            ticker="VRT",
            quarters=1,
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            percentile_target_date=None,
            ohlcv_db=None,
            price_behavior_snapshot=False,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out.strip()

    assert "section;candlestick_events_60td" not in cli_output


def test_candlestick_snapshot_validation_requires_analysis_db(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_fundamental_ticker_snapshot.py",
            "--db",
            "fund.db",
            "--ticker",
            "VRT",
            "--candlestick-snapshot",
            "--ohlcv-db",
            "os.db",
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        run_fundamental_ticker_snapshot.parse_args()


def test_candlestick_snapshot_validation_requires_ohlcv_db(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_fundamental_ticker_snapshot.py",
            "--db",
            "fund.db",
            "--ticker",
            "VRT",
            "--candlestick-snapshot",
            "--candlestick-analysis-db",
            "analysis.db",
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        run_fundamental_ticker_snapshot.parse_args()


def test_candlestick_snapshot_appends_section_and_rows(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    for pvm in ("2026-04-29", "2026-04-30"):
        _insert_ohlcv_close(ohlcv_db_path, "VRT", pvm, 400.0, "usa")
    _insert_candlestick_finding(analysis_db_path, 1, "VRT", "2026-04-30", "Hammer", 0.75, 28.5)

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
            price_behavior_snapshot=False,
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date="2026-04-30",
            candlestick_market="usa",
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "section;candlestick_events_60td" in cli_output
    assert "ticker;market;as_of_date;sequence_window_trading_days;sequence_available_trading_days;sequence_window_start_date;sequence_window_end_date;sequence_index;finding_id;signal_date;pattern;pattern_group;signal_strength;rsi14;created_at" in cli_output
    assert ";1;1;2026-04-30;Hammer;BULLISH_CANDLE;0.75;28.5;" in cli_output


def test_candlestick_snapshot_no_lookahead_and_combo_exclusion(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick_no_lookahead.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_ohlcv_close(ohlcv_db_path, "VRT", pvm, 400.0, "usa")
    _insert_candlestick_finding(analysis_db_path, 1, "VRT", "2026-04-29", "Hammer", 0.75, 28.5)
    _insert_candlestick_finding(analysis_db_path, 2, "VRT", "2026-05-01", "Bullish Engulfing", 0.65, 32.0)
    _insert_candlestick_finding(analysis_db_path, 3, "VRT", "2026-04-30", "BullDiv & Hammer", 0.95, 25.0)

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
            price_behavior_snapshot=False,
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date="2026-04-30",
            candlestick_market="usa",
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "Hammer;BULLISH_CANDLE" in cli_output
    assert "2026-05-01" not in cli_output
    assert "BullDiv & Hammer" not in cli_output


def test_candlestick_snapshot_sequence_ordering_through_cli(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick_order.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_ohlcv_close(ohlcv_db_path, "VRT", pvm, 400.0, "usa")
    _insert_candlestick_finding(analysis_db_path, 3, "VRT", "2026-04-30", "Shooting Star", 0.45, 71.0)
    _insert_candlestick_finding(analysis_db_path, 1, "VRT", "2026-04-29", "Hammer", 0.75, 28.5)
    _insert_candlestick_finding(analysis_db_path, 2, "VRT", "2026-04-30", "Morning Star", 0.55, 33.0)

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
            price_behavior_snapshot=False,
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date="2026-04-30",
            candlestick_market="usa",
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out
    recent_section = cli_output.split("section;candlestick_events_60td\n", 1)[1].strip().splitlines()
    event_rows = [line.split(";") for line in recent_section[1:]]

    assert [row[7] for row in event_rows] == ["1", "2", "3"]
    assert [row[8] for row in event_rows] == ["1", "2", "3"]
    assert [row[10] for row in event_rows] == ["Hammer", "Morning Star", "Shooting Star"]


def test_candlestick_snapshot_empty_recent_section(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick_empty.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")

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
            price_behavior_snapshot=False,
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date="2026-04-30",
            candlestick_market="usa",
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "section;candlestick_events_60td" in cli_output
    recent_section = cli_output.split("section;candlestick_events_60td\n", 1)[1]
    recent_lines = recent_section.strip().splitlines()
    assert recent_lines[0].startswith("ticker;market;as_of_date;sequence_window_trading_days;")
    assert len(recent_lines) == 1


def test_candlestick_snapshot_has_no_summary_counts(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick_no_summary.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")

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
            price_behavior_snapshot=False,
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date="2026-04-30",
            candlestick_market="usa",
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "recent_event_summary" not in cli_output
    assert "bullish_count_60td" not in cli_output
    assert "bearish_count_60td" not in cli_output
    assert "combo_count_60td" not in cli_output


def test_candlestick_as_of_date_derives_from_price_behavior_snapshot(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick_derived_pb.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_series(
        ohlcv_db_path,
        "VRT",
        260,
        anchor_close=400.0,
        anchor_date="2026-04-30",
        report_date="2026-03-31",
        report_day_close=300.0,
        close_1_after_report=303.0,
        close_3_after_report=309.0,
        close_20_after_report=330.0,
    )
    _insert_ohlcv_series(
        ohlcv_db_path,
        "^GSPC",
        260,
        anchor_close=200.0,
        anchor_date="2026-04-30",
        report_date="2026-03-31",
        report_day_close=180.0,
        close_1_after_report=181.0,
        close_3_after_report=183.0,
        close_20_after_report=190.0,
        return_6m_pct=14.59,
    )
    _insert_candlestick_finding(analysis_db_path, 1, "VRT", "2026-04-30", "Hammer", 0.75, 28.5)

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
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date=None,
            candlestick_market=None,
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2099-12-31")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert "price_behavior_as_of_date;2026-04-30" in cli_output
    assert ";usa;2026-04-30;60;" in cli_output
    assert "2099-12-31;60;" not in cli_output


def test_candlestick_as_of_date_falls_back_to_latest_valid_close_date(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick_derived_close.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-29", 390.0, "usa")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")
    _insert_candlestick_finding(analysis_db_path, 1, "VRT", "2026-04-30", "Hammer", 0.75, 28.5)

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
            price_behavior_snapshot=False,
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date=None,
            candlestick_market="usa",
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2099-12-31")

    ticker_snapshot_main()
    cli_output = capsys.readouterr().out

    assert ";usa;2026-04-30;60;" in cli_output
    assert "2099-12-31;60;" not in cli_output


def test_candlestick_snapshot_csv_uses_decimal_comma(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick_csv.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")
    _insert_candlestick_finding(analysis_db_path, 1, "VRT", "2026-04-30", "Hammer", 0.75, 28.5)

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
            price_behavior_snapshot=False,
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date="2026-04-30",
            candlestick_market="usa",
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    ticker_snapshot_main()
    _ = capsys.readouterr().out
    csv_content = (tmp_path / "ticker_fundamentals" / "VRT_2026-04-27.csv").read_text(encoding="utf-8")

    assert "section;candlestick_events_60td" in csv_content
    assert "Hammer;BULLISH_CANDLE;0,75;28,5;" in csv_content


def test_candlestick_snapshot_does_not_write_to_analysis_or_ohlcv_db(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ticker_snapshot_candlestick_no_write.db"
    analysis_db_path = tmp_path / "analysis.db"
    ohlcv_db_path = tmp_path / "osakedata.db"
    run_migration(db_path)
    _create_ohlcv_schema(ohlcv_db_path)
    _create_candlestick_analysis_schema(analysis_db_path)
    _insert_minimal_snapshot_rows(db_path, ticker="VRT", as_of_date="2026-03-31")
    _insert_ohlcv_close(ohlcv_db_path, "VRT", "2026-04-30", 400.0, "usa")
    _insert_candlestick_finding(analysis_db_path, 1, "VRT", "2026-04-30", "Hammer", 0.75, 28.5)

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
            price_behavior_snapshot=False,
            candlestick_snapshot=True,
            candlestick_analysis_db=str(analysis_db_path),
            candlestick_as_of_date="2026-04-30",
            candlestick_market="usa",
            candlestick_recent_window_trading_days=60,
        ),
    )
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "CSV_OUTPUT_DIR", tmp_path / "ticker_fundamentals")
    monkeypatch.setattr(run_fundamental_ticker_snapshot, "resolve_output_date", lambda: "2026-04-27")

    before_findings = _count_rows(analysis_db_path, "analysis_findings")
    before_ohlcv = _count_rows(ohlcv_db_path, "osakedata")

    ticker_snapshot_main()

    assert _count_rows(analysis_db_path, "analysis_findings") == before_findings
    assert _count_rows(ohlcv_db_path, "osakedata") == before_ohlcv


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


def _insert_minimal_snapshot_rows(db_path: Path, *, ticker: str, as_of_date: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(
            conn,
            ticker=ticker,
            as_of_date=as_of_date,
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
            latest_period_end_date=as_of_date,
        )
        _insert_quarterly_row(conn, ticker, as_of_date, 1300.0, 330.0, 280.0, 11.5, 17.0)
        _insert_percentile_row(conn, ticker, as_of_date, as_of_date, "Technology", "Electrical Equipment", 80.40, 80.90)
        conn.commit()


def _create_dow_analysis_schema(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE stock_dow_structure_status (
                ticker TEXT,
                market TEXT,
                price_source TEXT,
                pivot_radius INTEGER,
                calculated_from_date TEXT,
                calculated_through_date TEXT,
                latest_ohlcv_date_at_run TEXT,
                latest_event_date TEXT,
                latest_event_confirmed_as_of_date TEXT,
                last_run_id TEXT,
                last_run_mode TEXT,
                last_rows_deleted INTEGER,
                last_rows_inserted INTEGER,
                last_status TEXT,
                last_error_message TEXT,
                updated_at_utc TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_dow_structure_events (
                id INTEGER PRIMARY KEY,
                ticker TEXT,
                market TEXT,
                event_date TEXT,
                confirmed_as_of_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                price_source TEXT,
                structure_price REAL,
                pivot_radius INTEGER,
                event_type TEXT,
                dow_label_high TEXT,
                dow_label_low TEXT,
                trend_state TEXT,
                active_bos_high_date TEXT,
                active_bos_high_price REAL,
                active_bos_low_date TEXT,
                active_bos_low_price REAL,
                last_high_label TEXT,
                last_high_label_date TEXT,
                last_high_label_price REAL,
                last_low_label TEXT,
                last_low_label_date TEXT,
                last_low_label_price REAL,
                bos_up_count INTEGER,
                bos_down_count INTEGER,
                break_signal TEXT,
                break_level_date TEXT,
                break_level_price REAL,
                break_close_price REAL,
                reset_marker TEXT,
                reset_reason TEXT,
                structure_epoch_id INTEGER,
                structure_epoch_start_date TEXT,
                calc_version TEXT,
                run_id TEXT,
                created_at_utc TEXT
            )
            """
        )
        conn.commit()


def _create_candlestick_analysis_schema(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE analysis_findings (
                id INTEGER PRIMARY KEY,
                ticker TEXT,
                date TEXT,
                pattern TEXT,
                signal_strength REAL,
                rsi14 REAL,
                created_at TEXT
            )
            """
        )
        conn.commit()


def _insert_dow_status(
    db_path: Path,
    ticker: str,
    market: str,
    calculated_through_date: str,
    last_status: str,
    latest_event_date: str | None = None,
    latest_event_confirmed_as_of_date: str | None = None,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO stock_dow_structure_status (
                ticker, market, price_source, pivot_radius, calculated_from_date, calculated_through_date,
                latest_ohlcv_date_at_run, latest_event_date, latest_event_confirmed_as_of_date,
                last_run_id, last_run_mode, last_rows_deleted, last_rows_inserted, last_status, last_error_message, updated_at_utc
            ) VALUES (?, ?, 'close', 3, '2026-01-01', ?, ?, ?, ?, 'RUN1', 'incremental', 0, 0, ?, NULL, '2026-04-30T00:00:00Z')
            """,
            (
                ticker,
                market,
                calculated_through_date,
                calculated_through_date,
                latest_event_date,
                latest_event_confirmed_as_of_date,
                last_status,
            ),
        )
        conn.commit()


def _insert_dow_event(
    db_path: Path,
    event_id: int,
    ticker: str,
    market: str,
    event_date: str,
    confirmed_as_of_date: str,
    event_type: str,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO stock_dow_structure_events (
                id, ticker, market, event_date, confirmed_as_of_date, open, high, low, close, volume,
                price_source, structure_price, pivot_radius, event_type, dow_label_high, dow_label_low, trend_state,
                active_bos_high_date, active_bos_high_price, active_bos_low_date, active_bos_low_price,
                last_high_label, last_high_label_date, last_high_label_price, last_low_label, last_low_label_date,
                last_low_label_price, bos_up_count, bos_down_count, break_signal, break_level_date, break_level_price,
                break_close_price, reset_marker, reset_reason, structure_epoch_id, structure_epoch_start_date,
                calc_version, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, 1.0, 2.0, 0.5, 1.5, 1000, 'close', 1.5, 3, ?, 'HH', 'HL', 'UP', NULL, NULL, NULL, NULL, 'HH', ?, 2.0, 'HL', ?, 1.0, 1, 0, NULL, NULL, NULL, NULL, NULL, NULL, 1, '2026-01-01', 'stock_dow_v1', 'RUN1', '2026-04-30T00:00:00Z')
            """,
            (event_id, ticker, market, event_date, confirmed_as_of_date, event_type, event_date, event_date),
        )
        conn.commit()


def _insert_ohlcv_close(db_path: Path, ticker: str, pvm: str, close: float | None, market: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
            VALUES (?, ?, 1.0, 2.0, 0.5, ?, 1000, ?)
            """,
            (ticker.upper(), pvm, close, market),
        )
        conn.commit()


def _insert_candlestick_finding(
    db_path: Path,
    finding_id: int,
    ticker: str,
    signal_date: str,
    pattern: str,
    signal_strength: float | None,
    rsi14: float | None,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO analysis_findings (id, ticker, date, pattern, signal_strength, rsi14, created_at)
            VALUES (?, ?, ?, ?, ?, ?, '2026-04-30T00:00:00Z')
            """,
            (finding_id, ticker, signal_date, pattern, signal_strength, rsi14),
        )
        conn.commit()


def _count_rows(db_path: Path, table_name: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0])


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
    market: str = "usa",
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ticker.upper(), date_text, close, high, close, close, volume, market),
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
