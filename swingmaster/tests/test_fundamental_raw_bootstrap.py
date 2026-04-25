from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

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
