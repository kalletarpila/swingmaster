from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_build_quarterly import main as build_quarterly_main
from swingmaster.cli import run_fundamental_build_quarterly
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.build_quarterly import build_and_insert_quarterly_rows


def test_build_quarterly_successful_normalization(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_success.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_raw_row(conn, "AAPL", "income", "2024-12-31", "Total Revenue", 1000.0)
        _insert_raw_row(conn, "AAPL", "income", "2024-12-31", "Operating Income", 150.0)
        _insert_raw_row(conn, "AAPL", "income", "2024-12-31", "Net Income", 120.0)
        _insert_raw_row(conn, "AAPL", "cashflow", "2024-12-31", "Operating Cash Flow", 200.0)
        _insert_raw_row(conn, "AAPL", "cashflow", "2024-12-31", "Capital Expenditure", -50.0)
        _insert_raw_row(conn, "AAPL", "balance", "2024-12-31", "Cash And Cash Equivalents", 300.0)
        _insert_raw_row(conn, "AAPL", "balance", "2024-12-31", "Total Debt", 80.0)
        _insert_raw_row(conn, "AAPL", "balance", "2024-12-31", "Ordinary Shares Number", 10000.0)
        conn.commit()

        periods_detected, rows_written = build_and_insert_quarterly_rows(
            conn=conn,
            ticker="AAPL",
            run_id="FUND_BUILD_Q_AAPL_V1",
            dry_run=False,
        )

        assert periods_detected == 1
        assert rows_written == 1

        row = conn.execute(
            """
            SELECT
                ticker,
                period_end_date,
                revenue,
                operating_income,
                ebit,
                net_income,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                shares_outstanding,
                currency,
                run_id
            FROM rc_fundamental_quarterly
            """
        ).fetchone()
        assert row == (
            "AAPL",
            "2024-12-31",
            1000.0,
            150.0,
            150.0,
            120.0,
            200.0,
            -50.0,
            150.0,
            300.0,
            80.0,
            10000.0,
            None,
            "FUND_BUILD_Q_AAPL_V1",
        )


def test_build_quarterly_maps_sec_cash_name_and_sums_split_debt_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_sec_names.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_raw_row(
            conn,
            "VRT",
            "balance",
            "2025-12-31",
            "CashAndCashEquivalentsAtCarryingValue|form=10-K|unit=USD|fy=2025|fp=FY",
            1728400000.0,
        )
        _insert_raw_row(
            conn,
            "VRT",
            "balance",
            "2025-12-31",
            "LongTermDebtCurrent|form=10-K|unit=USD|fy=2025|fp=FY",
            20900000.0,
        )
        _insert_raw_row(
            conn,
            "VRT",
            "balance",
            "2025-12-31",
            "LongTermDebtNoncurrent|form=10-K|unit=USD|fy=2025|fp=FY",
            2892100000.0,
        )
        conn.commit()

        periods_detected, rows_written = build_and_insert_quarterly_rows(
            conn=conn,
            ticker="VRT",
            run_id="FUND_BUILD_Q_VRT_V1",
            dry_run=False,
        )

        assert periods_detected == 1
        assert rows_written == 1

        row = conn.execute(
            """
            SELECT cash, total_debt
            FROM rc_fundamental_quarterly
            WHERE ticker='VRT' AND period_end_date='2025-12-31'
            """
        ).fetchone()
        assert row == (1728400000.0, 2913000000.0)


def test_build_quarterly_maps_additional_sec_fact_names(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_additional_sec_fact_names.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_raw_row(
            conn,
            "MSFT",
            "income",
            "2025-12-31",
            "RevenueFromContractWithCustomerExcludingAssessedTax|form=10-Q|unit=USD|fy=2026|fp=Q2",
            81273000000.0,
        )
        _insert_raw_row(
            conn,
            "MSFT",
            "income",
            "2025-12-31",
            "OperatingIncomeLoss|form=10-Q|unit=USD|fy=2026|fp=Q2",
            31303000000.0,
        )
        _insert_raw_row(
            conn,
            "MSFT",
            "cashflow",
            "2025-12-31",
            "NetCashProvidedByUsedInOperatingActivities|form=10-Q|unit=USD|fy=2026|fp=Q2",
            40213000000.0,
        )
        _insert_raw_row(
            conn,
            "MSFT",
            "cashflow",
            "2025-12-31",
            "PaymentsToAcquirePropertyPlantAndEquipment|form=10-Q|unit=USD|fy=2026|fp=Q2",
            -1287000000.0,
        )
        _insert_raw_row(
            conn,
            "MSFT",
            "balance",
            "2025-12-31",
            "CashAndCashEquivalentsAtCarryingValue|form=10-Q|unit=USD|fy=2026|fp=Q2",
            32400000000.0,
        )
        _insert_raw_row(
            conn,
            "MSFT",
            "balance",
            "2025-12-31",
            "LongTermDebtCurrent|form=10-Q|unit=USD|fy=2026|fp=Q2",
            3749000000.0,
        )
        _insert_raw_row(
            conn,
            "MSFT",
            "balance",
            "2025-12-31",
            "LongTermDebtNoncurrent|form=10-Q|unit=USD|fy=2026|fp=Q2",
            40109000000.0,
        )
        _insert_raw_row(
            conn,
            "MSFT",
            "balance",
            "2025-12-31",
            "ShortTermBorrowings|form=10-Q|unit=USD|fy=2026|fp=Q2",
            500000000.0,
        )
        conn.commit()

        periods_detected, rows_written = build_and_insert_quarterly_rows(
            conn=conn,
            ticker="MSFT",
            run_id="FUND_BUILD_Q_MSFT_V1",
            dry_run=False,
        )

        assert periods_detected == 1
        assert rows_written == 1

        row = conn.execute(
            """
            SELECT
                revenue,
                operating_income,
                ebit,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt
            FROM rc_fundamental_quarterly
            WHERE ticker='MSFT' AND period_end_date='2025-12-31'
            """
        ).fetchone()
        assert row == (
            81273000000.0,
            31303000000.0,
            31303000000.0,
            40213000000.0,
            -1287000000.0,
            38926000000.0,
            32400000000.0,
            44358000000.0,
        )


def test_build_quarterly_uses_union_of_periods(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_union.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_raw_row(conn, "AAPL", "balance", "2024-09-30", "Cash And Cash Equivalents", 300.0)
        _insert_raw_row(conn, "AAPL", "cashflow", "2024-09-30", "Operating Cash Flow", 200.0)
        _insert_raw_row(conn, "AAPL", "income", "2024-12-31", "Total Revenue", 1000.0)
        conn.commit()

        periods_detected, rows_written = build_and_insert_quarterly_rows(
            conn=conn,
            ticker="AAPL",
            run_id="FUND_BUILD_Q_AAPL_V1",
            dry_run=False,
        )

        assert periods_detected == 2
        assert rows_written == 2

        rows = conn.execute(
            """
            SELECT period_end_date, revenue, operating_cashflow, cash
            FROM rc_fundamental_quarterly
            ORDER BY period_end_date
            """
        ).fetchall()
        assert rows == [
            ("2024-09-30", None, 200.0, 300.0),
            ("2024-12-31", 1000.0, None, None),
        ]


def test_build_quarterly_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_idempotent.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_raw_row(conn, "AAPL", "income", "2024-12-31", "Total Revenue", 1000.0)
        conn.commit()

        build_and_insert_quarterly_rows(conn=conn, ticker="AAPL", run_id="FUND_BUILD_Q_AAPL_V1", dry_run=False)
        build_and_insert_quarterly_rows(conn=conn, ticker="AAPL", run_id="FUND_BUILD_Q_AAPL_V1", dry_run=False)

        row_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        assert row_count == 1


def test_build_quarterly_dry_run_does_not_insert_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_dry_run.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_raw_row(conn, "AAPL", "income", "2024-12-31", "Total Revenue", 1000.0)
        conn.commit()

        periods_detected, rows_written = build_and_insert_quarterly_rows(
            conn=conn,
            ticker="AAPL",
            run_id="FUND_BUILD_Q_AAPL_V1",
            dry_run=True,
        )

        assert periods_detected == 1
        assert rows_written == 1
        row_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        assert row_count == 0


def test_build_quarterly_raises_when_no_raw_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_missing.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(RuntimeError, match="^FUNDAMENTAL_RAW_NOT_FOUND:MSFT$"):
            build_and_insert_quarterly_rows(
                conn=conn,
                ticker="MSFT",
                run_id="FUND_BUILD_Q_MSFT_V1",
                dry_run=False,
            )


def test_cli_build_quarterly_dry_run_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_quarterly_cli_dry_run.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_raw_row(conn, "AAPL", "income", "2024-12-31", "Total Revenue", 1000.0)
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_build_quarterly,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db": str(db_path),
                "ticker": "AAPL",
                "run_id": "FUND_BUILD_Q_AAPL_V1",
                "dry_run": True,
            },
        )(),
    )

    build_quarterly_main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY ticker=AAPL",
        "SUMMARY periods_detected=1",
        "SUMMARY rows_written=1",
        f"SUMMARY db_path={db_path.resolve()}",
        "SUMMARY run_id=FUND_BUILD_Q_AAPL_V1",
        "SUMMARY status=dry-run",
    ]


def _insert_raw_row(
    conn: sqlite3.Connection,
    ticker: str,
    statement_type: str,
    period_end_date: str,
    field_name: str,
    field_value: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_statement_raw (
            ticker,
            statement_type,
            period_end_date,
            period_type,
            field_name,
            field_value,
            currency,
            source,
            retrieved_at_utc,
            run_id
        ) VALUES (?, ?, ?, 'quarterly', ?, ?, NULL, 'test', '2026-01-01T00:00:00', 'RAW_TEST_V1')
        """,
        (ticker, statement_type, period_end_date, field_name, field_value),
    )
