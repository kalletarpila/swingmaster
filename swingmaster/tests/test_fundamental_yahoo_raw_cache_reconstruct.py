from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_yahoo_audit import canonical_json_dumps
from swingmaster.cli.run_fundamental_yahoo_raw_cache_reconstruct import (
    RECONSTRUCT_REASON_NO_MAPPED_VALUE_AT_TARGET_PERIOD,
    run_yahoo_raw_cache_reconstruct,
)
from swingmaster.fundamentals.providers import yahoo as yahoo_provider


def _insert_yahoo_raw_row(
    db_path: Path,
    *,
    symbol: str,
    income: dict,
    balance: dict,
    cashflow: dict,
    run_id: str,
    loaded_at_utc: str,
    market: str = "usa",
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_raw (
                market,
                provider,
                symbol,
                info_json,
                fast_info_json,
                quarterly_income_stmt_json,
                quarterly_balance_sheet_json,
                quarterly_cashflow_json,
                payload_hash,
                status,
                error_message,
                loaded_at_utc,
                run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market,
                "yahoo",
                symbol,
                canonical_json_dumps({"sharesOutstanding": 111.0}),
                canonical_json_dumps({"shares": 111.0}),
                canonical_json_dumps(income),
                canonical_json_dumps(balance),
                canonical_json_dumps(cashflow),
                f"hash-{run_id}",
                "OK",
                None,
                loaded_at_utc,
                run_id,
            ),
        )
        conn.commit()


def _fixture(*, periods: list[str], ebit_values: list[float | None] | None = None) -> tuple[dict, dict, dict]:
    ebit_values = ebit_values if ebit_values is not None else [None for _ in periods]
    income = {
        "index": ["Total Revenue", "Gross Profit", "Operating Income", "EBIT", "EBITDA", "Net Income"],
        "columns": periods,
        "data": [
            [1000.0 + idx for idx, _period in enumerate(periods)],
            [500.0 + idx for idx, _period in enumerate(periods)],
            [10.0 + idx for idx, _period in enumerate(periods)],
            ebit_values,
            [414013000.0 + idx for idx, _period in enumerate(periods)],
            [20.0 + idx for idx, _period in enumerate(periods)],
        ],
    }
    balance = {
        "index": ["Ordinary Shares Number", "Cash And Cash Equivalents", "Total Debt"],
        "columns": periods,
        "data": [
            [111.0 + idx for idx, _period in enumerate(periods)],
            [200.0 + idx for idx, _period in enumerate(periods)],
            [300.0 + idx for idx, _period in enumerate(periods)],
        ],
    }
    cashflow = {
        "index": ["Operating Cash Flow", "Capital Expenditure", "Free Cash Flow"],
        "columns": periods,
        "data": [
            [40.0 + idx for idx, _period in enumerate(periods)],
            [-5.0 - idx for idx, _period in enumerate(periods)],
            [35.0 for _period in periods],
        ],
    }
    return income, balance, cashflow


def test_historical_raw_reconstruction_creates_target_cache_row(tmp_path: Path) -> None:
    db_path = tmp_path / "raw_cache_reconstruct.db"
    run_migration(db_path)
    income, balance, cashflow = _fixture(periods=["2025-02-28"])
    _insert_yahoo_raw_row(
        db_path,
        symbol="KMX",
        income=income,
        balance=balance,
        cashflow=cashflow,
        run_id="RAW1",
        loaded_at_utc="2026-05-05T00:00:00+00:00",
    )

    result = run_yahoo_raw_cache_reconstruct(
        db_path=db_path,
        market="usa",
        symbol="KMX",
        period_end_date="2025-02-28",
        run_id="RECON1",
        dry_run=False,
    )

    assert result["rows_written"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT revenue, operating_income, ebit, ebitda, source_run_id, run_id
            FROM rc_fundamental_yahoo_quarterly
            WHERE market='usa' AND symbol='KMX' AND period_end_date='2025-02-28'
            """
        ).fetchone()
    assert row == (1000.0, 10.0, None, 414013000.0, "RAW1", "RECON1")


def test_no_proxy_operating_income_does_not_flow_to_ebit(tmp_path: Path) -> None:
    db_path = tmp_path / "raw_cache_no_proxy.db"
    run_migration(db_path)
    income, balance, cashflow = _fixture(periods=["2025-02-28"], ebit_values=[None])
    _insert_yahoo_raw_row(
        db_path,
        symbol="KMX",
        income=income,
        balance=balance,
        cashflow=cashflow,
        run_id="RAW1",
        loaded_at_utc="2026-05-05T00:00:00+00:00",
    )

    result = run_yahoo_raw_cache_reconstruct(
        db_path=db_path,
        market="usa",
        symbol="KMX",
        period_end_date="2025-02-28",
        run_id="RECON_NO_PROXY",
        dry_run=True,
    )

    assert result["row"]["operating_income"] == 10.0
    assert result["row"]["ebit"] is None
    assert result["row"]["ebitda"] == 414013000.0


def test_reconstruction_preserves_other_cache_periods_and_updates_only_target(tmp_path: Path) -> None:
    db_path = tmp_path / "raw_cache_exact_scope.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_quarterly (
                market, symbol, period_end_date, revenue, ebitda, source_run_id, run_id, created_at_utc
            ) VALUES
                ('usa', 'KMX', '2025-02-28', 1, 2, 'OLDRAW', 'OLDRECON', '2026-01-01T00:00:00+00:00'),
                ('usa', 'KMX', '2025-05-31', 3, 4, 'OLDRAW', 'OLDRECON', '2026-01-01T00:00:00+00:00'),
                ('usa', 'KMX', '2025-08-31', 5, 6, 'OLDRAW', 'OLDRECON', '2026-01-01T00:00:00+00:00')
            """
        )
        conn.commit()
    income, balance, cashflow = _fixture(periods=["2025-02-28"])
    _insert_yahoo_raw_row(
        db_path,
        symbol="KMX",
        income=income,
        balance=balance,
        cashflow=cashflow,
        run_id="RAW1",
        loaded_at_utc="2026-05-05T00:00:00+00:00",
    )

    run_yahoo_raw_cache_reconstruct(
        db_path=db_path,
        market="usa",
        symbol="KMX",
        period_end_date="2025-02-28",
        run_id="RECON_SCOPE",
        dry_run=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT period_end_date, revenue, ebitda, run_id
            FROM rc_fundamental_yahoo_quarterly
            WHERE market='usa' AND symbol='KMX'
            ORDER BY period_end_date
            """
        ).fetchall()
    assert rows == [
        ("2025-02-28", 1000.0, 414013000.0, "RECON_SCOPE"),
        ("2025-05-31", 3.0, 4.0, "OLDRECON"),
        ("2025-08-31", 5.0, 6.0, "OLDRECON"),
    ]


def test_newest_coherent_snapshot_wins_for_same_period(tmp_path: Path) -> None:
    db_path = tmp_path / "raw_cache_newest_snapshot.db"
    run_migration(db_path)
    old_income, old_balance, old_cashflow = _fixture(periods=["2025-02-28"])
    new_income, new_balance, new_cashflow = _fixture(periods=["2025-02-28"], ebit_values=[99.0])
    new_income["data"][4][0] = 500.0
    _insert_yahoo_raw_row(
        db_path,
        symbol="KMX",
        income=old_income,
        balance=old_balance,
        cashflow=old_cashflow,
        run_id="RAW_OLD",
        loaded_at_utc="2026-05-05T00:00:00+00:00",
    )
    _insert_yahoo_raw_row(
        db_path,
        symbol="KMX",
        income=new_income,
        balance=new_balance,
        cashflow=new_cashflow,
        run_id="RAW_NEW",
        loaded_at_utc="2026-08-12T00:00:00+00:00",
    )

    result = run_yahoo_raw_cache_reconstruct(
        db_path=db_path,
        market="usa",
        symbol="KMX",
        period_end_date="2025-02-28",
        run_id="RECON_NEW",
        dry_run=True,
    )

    assert result["selected_source_run_id"] == "RAW_NEW"
    assert result["row"]["ebit"] == 99.0
    assert result["row"]["ebitda"] == 500.0


def test_reconstruction_does_not_call_network_provider(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "raw_cache_no_network.db"
    run_migration(db_path)
    income, balance, cashflow = _fixture(periods=["2025-02-28"])
    _insert_yahoo_raw_row(
        db_path,
        symbol="KMX",
        income=income,
        balance=balance,
        cashflow=cashflow,
        run_id="RAW1",
        loaded_at_utc="2026-05-05T00:00:00+00:00",
    )

    def fail_provider(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("network provider must not be constructed")

    monkeypatch.setattr(yahoo_provider, "YahooFinanceClient", fail_provider)
    result = run_yahoo_raw_cache_reconstruct(
        db_path=db_path,
        market="usa",
        symbol="KMX",
        period_end_date="2025-02-28",
        run_id="RECON_NO_NETWORK",
        dry_run=True,
    )

    assert result["selected_source_run_id"] == "RAW1"


def test_raw_target_period_with_only_null_mapped_values_reports_insufficient_raw(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "raw_cache_q1_null_values.db"
    run_migration(db_path)
    _insert_yahoo_raw_row(
        db_path,
        symbol="AAPL",
        income={
            "index": ["Total Revenue", "Operating Income", "EBIT", "EBITDA", "Net Income"],
            "columns": ["2025-12-31", "2026-03-31"],
            "data": [
                [1000.0, None],
                [100.0, None],
                [90.0, None],
                [150.0, None],
                [80.0, None],
            ],
        },
        balance={
            "index": ["Ordinary Shares Number", "Cash And Cash Equivalents", "Total Debt"],
            "columns": ["2025-12-31"],
            "data": [[111.0], [200.0], [300.0]],
        },
        cashflow={
            "index": ["Operating Cash Flow", "Capital Expenditure", "Free Cash Flow"],
            "columns": ["2025-12-31"],
            "data": [[40.0], [-5.0], [35.0]],
        },
        run_id="RAW_Q1_NULLS",
        loaded_at_utc="2026-05-05T00:00:00+00:00",
    )

    def fail_provider(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("network provider must not be constructed")

    monkeypatch.setattr(yahoo_provider, "YahooFinanceClient", fail_provider)
    result = run_yahoo_raw_cache_reconstruct(
        db_path=db_path,
        market="usa",
        symbol="AAPL",
        period_end_date="2026-03-31",
        run_id="RECON_Q1_NULLS",
        dry_run=True,
    )

    assert result["status"] == "NOT_RECONSTRUCTABLE"
    assert result["reason"] == RECONSTRUCT_REASON_NO_MAPPED_VALUE_AT_TARGET_PERIOD
    assert result["rows_written"] == 0
    assert result["row"] is None
    assert result["selected_source_run_id"] is None
    assert result["lineage"][0]["period_present"] == 1
    assert result["lineage"][0]["persistable"] == 0
    assert result["lineage"][0]["reason"] == RECONSTRUCT_REASON_NO_MAPPED_VALUE_AT_TARGET_PERIOD
    assert result["lineage"][0]["revenue"] == ""
    assert result["lineage"][0]["operating_income"] == ""
    assert result["lineage"][0]["ebit"] == ""
    assert result["lineage"][0]["ebitda"] == ""
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_yahoo_quarterly
            WHERE market='usa' AND symbol='AAPL' AND period_end_date='2026-03-31'
            """
        ).fetchone()[0]
    assert count == 0


def test_target_with_one_mapped_income_value_persists_partial_row(tmp_path: Path) -> None:
    db_path = tmp_path / "raw_cache_partial_income.db"
    run_migration(db_path)
    _insert_yahoo_raw_row(
        db_path,
        symbol="AAPL",
        income={
            "index": ["Total Revenue", "Operating Income", "EBIT", "EBITDA", "Net Income"],
            "columns": ["2026-03-31"],
            "data": [[1000.0], [None], [None], [None], [None]],
        },
        balance={"index": [], "columns": [], "data": []},
        cashflow={"index": [], "columns": [], "data": []},
        run_id="RAW_PARTIAL",
        loaded_at_utc="2026-05-05T00:00:00+00:00",
    )

    result = run_yahoo_raw_cache_reconstruct(
        db_path=db_path,
        market="usa",
        symbol="AAPL",
        period_end_date="2026-03-31",
        run_id="RECON_PARTIAL",
        dry_run=False,
    )

    assert result["status"] == "RECONSTRUCTED"
    assert result["reason"] == "OK"
    assert result["rows_written"] == 1
    assert result["row"]["revenue"] == 1000.0
    assert result["row"]["operating_income"] is None
    assert result["row"]["ebit"] is None
    assert result["row"]["ebitda"] is None
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT revenue, operating_income, ebit, ebitda, cash, operating_cashflow
            FROM rc_fundamental_yahoo_quarterly
            WHERE market='usa' AND symbol='AAPL' AND period_end_date='2026-03-31'
            """
        ).fetchone()
    assert row == (1000.0, None, None, None, None, None)
