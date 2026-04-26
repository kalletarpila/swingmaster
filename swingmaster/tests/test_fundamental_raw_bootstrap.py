from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest

from swingmaster.cli import run_fundamental_bootstrap_raw
from swingmaster.cli import run_fundamental_yfinance_diagnostic
from swingmaster.cli.run_fundamental_bootstrap_raw import run_bootstrap_raw
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals import fetch_raw_statements


def test_run_bootstrap_raw_writes_statement_rows_and_is_idempotent(
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fundamentals_test.db"
    run_migration(db_path)

    statement_frame = pd.DataFrame(
        {
            pd.Timestamp("2024-06-30"): [100.0, None],
            pd.Timestamp("2024-03-31"): [90.0, 80.0],
        },
        index=["Total Revenue", "Net Income"],
    )

    class _FakeTicker:
        quarterly_income_stmt = statement_frame
        quarterly_balance_sheet = statement_frame
        quarterly_cashflow = statement_frame

    class _FakeYFinanceModule:
        @staticmethod
        def Ticker(_ticker: str) -> _FakeTicker:
            return _FakeTicker()

    monkeypatch.setattr(fetch_raw_statements, "_get_yfinance_module", lambda: _FakeYFinanceModule())

    statements_loaded_first, rows_written_first = run_bootstrap_raw(
        db_path=db_path,
        ticker="AAPL",
        run_id="FUND_BOOTSTRAP_RAW_AAPL_V1",
        dry_run=False,
    )
    statements_loaded_second, rows_written_second = run_bootstrap_raw(
        db_path=db_path,
        ticker="AAPL",
        run_id="FUND_BOOTSTRAP_RAW_AAPL_V1",
        dry_run=False,
    )

    assert statements_loaded_first == 3
    assert statements_loaded_second == 3
    assert rows_written_first == 12
    assert rows_written_second == 12

    with sqlite3.connect(str(db_path)) as conn:
        row_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_statement_raw
            """
        ).fetchone()[0]
        assert row_count == 12

        statement_types = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT DISTINCT statement_type
                FROM rc_fundamental_statement_raw
                """
            )
        }
        assert statement_types == {"income", "balance", "cashflow"}


def test_fallback_picks_wider_income_dataframe(monkeypatch) -> None:
    narrow_income = pd.DataFrame({"2024-03-31": [1.0], "2024-06-30": [2.0]}, index=["Total Revenue"])
    wide_income = pd.DataFrame(
        {"2024-03-31": [1.0], "2024-06-30": [2.0], "2024-09-30": [3.0], "2024-12-31": [4.0]},
        index=["Total Revenue"],
    )
    balance = pd.DataFrame({"2024-12-31": [1.0]}, index=["Cash And Cash Equivalents"])
    cashflow = pd.DataFrame({"2024-12-31": [1.0]}, index=["Operating Cash Flow"])

    class _FakeTicker:
        quarterly_income_stmt = narrow_income
        quarterly_financials = pd.DataFrame()
        quarterly_balance_sheet = balance
        quarterly_cashflow = cashflow

        @staticmethod
        def get_income_stmt(freq: str) -> pd.DataFrame:
            assert freq == "quarterly"
            return wide_income

        @staticmethod
        def get_balance_sheet(freq: str) -> pd.DataFrame:
            assert freq == "quarterly"
            return balance

        @staticmethod
        def get_cash_flow(freq: str) -> pd.DataFrame:
            assert freq == "quarterly"
            return cashflow

    class _FakeYFinanceModule:
        @staticmethod
        def Ticker(_ticker: str) -> _FakeTicker:
            return _FakeTicker()

    monkeypatch.setattr(fetch_raw_statements, "_get_yfinance_module", lambda: _FakeYFinanceModule())
    statements = fetch_raw_statements.fetch_quarterly_statements_raw("AAPL")
    assert len(statements["income"].columns) == 4


def test_tie_keeps_earlier_candidate(monkeypatch) -> None:
    income = pd.DataFrame({"2024-12-31": [1.0]}, index=["Total Revenue"])
    balance = pd.DataFrame({"2024-12-31": [1.0]}, index=["Cash And Cash Equivalents"])
    earlier_cashflow = pd.DataFrame(
        {"2024-06-30": [1.0], "2024-09-30": [2.0], "2024-12-31": [3.0]},
        index=["Operating Cash Flow"],
    )
    later_cashflow = pd.DataFrame(
        {"2024-03-31": [9.0], "2024-06-30": [8.0], "2024-09-30": [7.0]},
        index=["Operating Cash Flow"],
    )

    class _FakeTicker:
        quarterly_income_stmt = income
        quarterly_financials = income
        quarterly_balance_sheet = balance
        quarterly_cashflow = earlier_cashflow

        @staticmethod
        def get_income_stmt(freq: str) -> pd.DataFrame:
            return income

        @staticmethod
        def get_balance_sheet(freq: str) -> pd.DataFrame:
            return balance

        @staticmethod
        def get_cash_flow(freq: str) -> pd.DataFrame:
            return later_cashflow

    class _FakeYFinanceModule:
        @staticmethod
        def Ticker(_ticker: str) -> _FakeTicker:
            return _FakeTicker()

    monkeypatch.setattr(fetch_raw_statements, "_get_yfinance_module", lambda: _FakeYFinanceModule())
    statements = fetch_raw_statements.fetch_quarterly_statements_raw("AAPL")
    assert list(statements["cashflow"].columns) == ["2024-06-30", "2024-09-30", "2024-12-31"]


def test_run_bootstrap_raw_raises_fundamental_statement_empty_for_empty_income(
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fundamentals_empty.db"
    run_migration(db_path)
    empty_frame = pd.DataFrame()
    populated_frame = pd.DataFrame(
        {pd.Timestamp("2024-06-30"): [100.0]},
        index=["Total Revenue"],
    )

    class _FakeTicker:
        quarterly_income_stmt = empty_frame
        quarterly_financials = empty_frame
        quarterly_balance_sheet = populated_frame
        quarterly_cashflow = populated_frame

        @staticmethod
        def get_income_stmt(freq: str) -> pd.DataFrame:
            return empty_frame

        @staticmethod
        def get_balance_sheet(freq: str) -> pd.DataFrame:
            return populated_frame

        @staticmethod
        def get_cash_flow(freq: str) -> pd.DataFrame:
            return populated_frame

    class _FakeYFinanceModule:
        @staticmethod
        def Ticker(_ticker: str) -> _FakeTicker:
            return _FakeTicker()

    monkeypatch.setattr(fetch_raw_statements, "_get_yfinance_module", lambda: _FakeYFinanceModule())

    with pytest.raises(RuntimeError, match="^FUNDAMENTAL_STATEMENT_EMPTY:income$"):
        run_bootstrap_raw(
            db_path=db_path,
            ticker="AAPL",
            run_id="FUND_BOOTSTRAP_RAW_AAPL_V1",
            dry_run=False,
        )


def test_cli_main_raises_fundamental_fetch_failed_for_fetch_exception(
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fundamentals_fetch_failed.db"
    run_migration(db_path)

    monkeypatch.setattr(
        run_fundamental_bootstrap_raw,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            ticker="AAPL",
            run_id="FUND_BOOTSTRAP_RAW_AAPL_V1",
            dry_run=False,
        ),
    )

    def _raise_fetch_error(_ticker: str) -> dict[str, pd.DataFrame]:
        raise RuntimeError("FUNDAMENTAL_FETCH_FAILED:AAPL:DNSError:Could not resolve host")

    monkeypatch.setattr(run_fundamental_bootstrap_raw, "fetch_quarterly_statements_raw", _raise_fetch_error)

    with pytest.raises(RuntimeError, match="^FUNDAMENTAL_FETCH_FAILED:AAPL:DNSError:Could not resolve host$"):
        run_fundamental_bootstrap_raw.main()


def test_raw_bootstrap_summary_includes_period_counts(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals_summary.db"
    run_migration(db_path)
    income = pd.DataFrame({"2024-09-30": [1.0], "2024-12-31": [2.0]}, index=["Total Revenue"])
    balance = pd.DataFrame({"2024-12-31": [1.0]}, index=["Cash And Cash Equivalents"])
    cashflow = pd.DataFrame(
        {"2024-06-30": [1.0], "2024-09-30": [2.0], "2024-12-31": [3.0]},
        index=["Operating Cash Flow"],
    )
    monkeypatch.setattr(
        run_fundamental_bootstrap_raw,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            ticker="AAPL",
            run_id="FUND_BOOTSTRAP_RAW_AAPL_V1",
            dry_run=True,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_bootstrap_raw,
        "fetch_quarterly_statements_raw",
        lambda _ticker: {"income": income, "balance": balance, "cashflow": cashflow},
    )
    run_fundamental_bootstrap_raw.main()
    out = capsys.readouterr().out
    assert "SUMMARY income_periods=2" in out
    assert "SUMMARY balance_periods=1" in out
    assert "SUMMARY cashflow_periods=3" in out


def test_diagnostic_cli_output(monkeypatch, capsys) -> None:
    income = pd.DataFrame({"2024-09-30": [1.0], "2024-12-31": [2.0]}, index=["Total Revenue"])
    balance = pd.DataFrame({"2024-12-31": [1.0]}, index=["Cash And Cash Equivalents"])
    cashflow = pd.DataFrame({"2024-06-30": [1.0], "2024-09-30": [2.0], "2024-12-31": [3.0]}, index=["Operating Cash Flow"])

    class _FakeTicker:
        quarterly_income_stmt = income
        quarterly_financials = pd.DataFrame()
        quarterly_balance_sheet = balance
        quarterly_cashflow = cashflow

        @staticmethod
        def get_income_stmt(freq: str) -> pd.DataFrame:
            return income

        @staticmethod
        def get_balance_sheet(freq: str) -> pd.DataFrame:
            return balance

        @staticmethod
        def get_cash_flow(freq: str) -> pd.DataFrame:
            return cashflow

    class _FakeYFinanceModule:
        @staticmethod
        def Ticker(_ticker: str) -> _FakeTicker:
            return _FakeTicker()

    monkeypatch.setattr(run_fundamental_yfinance_diagnostic, "_get_yfinance_module", lambda: _FakeYFinanceModule())
    monkeypatch.setattr(
        run_fundamental_yfinance_diagnostic,
        "parse_args",
        lambda: Namespace(ticker="NVDA"),
    )
    run_fundamental_yfinance_diagnostic.main()
    out = capsys.readouterr().out
    assert "path=ticker.quarterly_income_stmt" in out
    assert "shape=(1, 2)" in out
    assert "period_count=2" in out
    assert "SUMMARY max_income_periods=2" in out
    assert "SUMMARY max_balance_periods=1" in out
    assert "SUMMARY max_cashflow_periods=3" in out
