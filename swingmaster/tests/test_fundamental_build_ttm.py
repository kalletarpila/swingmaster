from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_build_ttm
from swingmaster.cli.run_fundamental_build_ttm import main as build_ttm_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.build_ttm import build_and_insert_ttm_rows


def test_build_ttm_successful_partial_with_4_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ttm_partial.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        for row in [
            ("2024-03-31", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-06-30", 110.0, 33.0, 44.0, 22.0, 11.0, 51.0, 201.0, 1001.0),
            ("2024-09-30", 120.0, 36.0, 48.0, 24.0, 12.0, 52.0, 202.0, 1002.0),
            ("2024-12-31", 130.0, 39.0, 52.0, 26.0, 13.0, 53.0, 203.0, 1003.0),
        ]:
            _insert_quarterly_row(conn, "AAPL", row, "Q_RUN_V1")
        conn.commit()

        quarterly_rows, ttm_rows_written, first_as_of_date, last_as_of_date = build_and_insert_ttm_rows(
            conn=conn,
            ticker="AAPL",
            run_id="FUND_BUILD_TTM_AAPL_V1",
            dry_run=False,
        )

        assert quarterly_rows == 4
        assert ttm_rows_written == 1
        assert first_as_of_date == "2024-12-31"
        assert last_as_of_date == "2024-12-31"

        row = conn.execute(
            """
            SELECT
                as_of_date,
                latest_period_end_date,
                revenue_ttm,
                ebit_ttm,
                ebit_margin_ttm,
                revenue_growth_ttm_yoy,
                ebit_growth_ttm_yoy,
                ebit_margin_trend_4q,
                gross_margin_trend_4q,
                fcf_margin_trend_4q,
                lifecycle_class,
                fundamental_score,
                run_id
            FROM rc_fundamental_ttm
            """
        ).fetchone()
        assert row == (
            "2024-12-31",
            "2024-12-31",
            460.0,
            138.0,
            0.3,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "FUND_BUILD_TTM_AAPL_V1",
        )


def test_build_ttm_full_trend_and_yoy_with_8_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ttm_full.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        rows = [
            ("2023-03-31", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2023-06-30", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2023-09-30", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2023-12-31", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-03-31", 150.0, 60.0, 70.0, 30.0, 20.0, 80.0, 260.0, 1100.0),
            ("2024-06-30", 150.0, 60.0, 70.0, 30.0, 20.0, 80.0, 260.0, 1100.0),
            ("2024-09-30", 150.0, 60.0, 70.0, 30.0, 20.0, 80.0, 260.0, 1100.0),
            ("2024-12-31", 150.0, 60.0, 70.0, 30.0, 20.0, 80.0, 260.0, 1100.0),
        ]
        for row in rows:
            _insert_quarterly_row(conn, "AAPL", row, "Q_RUN_V1")
        conn.commit()

        quarterly_rows, ttm_rows_written, first_as_of_date, last_as_of_date = build_and_insert_ttm_rows(
            conn=conn,
            ticker="AAPL",
            run_id="FUND_BUILD_TTM_AAPL_V1",
            dry_run=False,
        )

        assert quarterly_rows == 8
        assert ttm_rows_written == 5
        assert first_as_of_date == "2023-12-31"
        assert last_as_of_date == "2024-12-31"

        last_row = conn.execute(
            """
            SELECT
                revenue_growth_ttm_yoy,
                ebit_growth_ttm_yoy,
                ebit_margin_trend_4q,
                fcf_margin_trend_4q,
                gross_margin_trend_4q,
                net_debt,
                net_debt_to_ebitda,
                share_dilution_yoy
            FROM rc_fundamental_ttm
            WHERE ticker='AAPL' AND as_of_date='2024-12-31'
            """
        ).fetchone()
        assert last_row[0] is not None
        assert last_row[1] is not None
        assert last_row[2] is not None
        assert last_row[3] is not None
        assert last_row[4] is not None
        assert last_row[5] == 180.0
        assert last_row[6] is not None
        assert last_row[7] == 0.1


def test_build_ttm_raises_when_insufficient_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ttm_insufficient.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        for row in [
            ("2024-03-31", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-06-30", 110.0, 33.0, 44.0, 22.0, 11.0, 51.0, 201.0, 1001.0),
            ("2024-09-30", 120.0, 36.0, 48.0, 24.0, 12.0, 52.0, 202.0, 1002.0),
        ]:
            _insert_quarterly_row(conn, "AAPL", row, "Q_RUN_V1")
        conn.commit()

        with pytest.raises(RuntimeError, match="^FUNDAMENTAL_TTM_INSUFFICIENT_ROWS:AAPL$"):
            build_and_insert_ttm_rows(conn=conn, ticker="AAPL", run_id="FUND_BUILD_TTM_AAPL_V1", dry_run=False)


def test_build_ttm_null_handling_ignores_partial_nulls(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ttm_nulls.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        rows = [
            ("2024-03-31", 100.0, None, None, None, None, 50.0, 200.0, 1000.0),
            ("2024-06-30", None, 30.0, None, 20.0, None, 50.0, 200.0, 1000.0),
            ("2024-09-30", 120.0, None, None, None, None, 50.0, 200.0, 1000.0),
            ("2024-12-31", None, 40.0, None, 30.0, None, 50.0, 200.0, 1000.0),
        ]
        for row in rows:
            _insert_quarterly_row(conn, "AAPL", row, "Q_RUN_V1")
        conn.commit()

        build_and_insert_ttm_rows(conn=conn, ticker="AAPL", run_id="FUND_BUILD_TTM_AAPL_V1", dry_run=False)

        row = conn.execute(
            """
            SELECT revenue_ttm, ebit_ttm, ebitda_ttm_missing, net_debt_to_ebitda, share_dilution_yoy
            FROM (
                SELECT
                    revenue_ttm,
                    ebit_ttm,
                    NULL AS ebitda_ttm_missing,
                    net_debt_to_ebitda,
                    share_dilution_yoy
                FROM rc_fundamental_ttm
            )
            """
        ).fetchone()
        assert row == (220.0, 70.0, None, None, None)


def test_build_ttm_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ttm_idempotent.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        for row in [
            ("2024-03-31", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-06-30", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-09-30", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-12-31", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
        ]:
            _insert_quarterly_row(conn, "AAPL", row, "Q_RUN_V1")
        conn.commit()

        build_and_insert_ttm_rows(conn=conn, ticker="AAPL", run_id="FUND_BUILD_TTM_AAPL_V1", dry_run=False)
        build_and_insert_ttm_rows(conn=conn, ticker="AAPL", run_id="FUND_BUILD_TTM_AAPL_V1", dry_run=False)

        row_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0]
        assert row_count == 1


def test_cli_build_ttm_dry_run_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamental_ttm_cli_dry_run.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        for row in [
            ("2024-03-31", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-06-30", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-09-30", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
            ("2024-12-31", 100.0, 30.0, 40.0, 20.0, 10.0, 50.0, 200.0, 1000.0),
        ]:
            _insert_quarterly_row(conn, "AAPL", row, "Q_RUN_V1")
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_build_ttm,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(db_path),
                "ticker": "AAPL",
                "run_id": "FUND_BUILD_TTM_AAPL_V1",
                "dry_run": True,
            },
        )(),
    )

    build_ttm_main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY ticker=AAPL",
        "SUMMARY quarterly_rows=4",
        "SUMMARY ttm_rows_written=1",
        "SUMMARY first_as_of_date=2024-12-31",
        "SUMMARY last_as_of_date=2024-12-31",
        f"SUMMARY db_path={db_path.resolve()}",
        "SUMMARY run_id=FUND_BUILD_TTM_AAPL_V1",
        "SUMMARY status=dry-run",
    ]


def _insert_quarterly_row(
    conn: sqlite3.Connection,
    ticker: str,
    row: tuple[object, ...],
    run_id: str,
) -> None:
    period_end_date, revenue, ebit, ebitda, gross_profit, free_cashflow, cash, total_debt, shares_outstanding = row
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            ebit,
            ebitda,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            currency,
            run_id
        ) VALUES (?, ?, ?, ?, NULL, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, NULL, ?)
        """,
        (
            ticker,
            period_end_date,
            revenue,
            gross_profit,
            ebit,
            ebitda,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            run_id,
        ),
    )
