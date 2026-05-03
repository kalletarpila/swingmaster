from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_yahoo_to_quarterly
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_yahoo_quarterly_row(
    conn: sqlite3.Connection,
    *,
    market: str,
    symbol: str,
    period_end_date: str,
    revenue: float | None = None,
    gross_profit: float | None = None,
    operating_income: float | None = None,
    net_income: float | None = None,
    operating_cashflow: float | None = None,
    capex: float | None = None,
    free_cashflow: float | None = None,
    cash: float | None = None,
    total_debt: float | None = None,
    shares_outstanding: float | None = None,
    shares_source: str | None = None,
    shares_quality: str | None = None,
    source_run_id: str = "RAW1",
    run_id: str = "WRITE1",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_yahoo_quarterly (
            market,
            symbol,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            shares_source,
            shares_quality,
            source_run_id,
            run_id,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            market,
            symbol,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            shares_source,
            shares_quality,
            source_run_id,
            run_id,
            "2026-05-03T10:23:06+00:00",
        ),
    )


def test_dry_run_writes_zero_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_to_quarterly_dry.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_yahoo_quarterly_row(conn, market="fin", symbol="NOKIA.HE", period_end_date="2025-03-31", revenue=10.0)
        conn.commit()

    summary = run_fundamental_yahoo_to_quarterly.run_yahoo_to_quarterly(
        db_path=db_path,
        market="fin",
        symbol="NOKIA.HE",
        run_id="BRIDGE1",
        dry_run=True,
        replace_symbol=False,
    )

    assert summary["rows_written"] == 0
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
    assert count == 0


def test_rows_written_in_deterministic_period_order(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_to_quarterly_order.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_yahoo_quarterly_row(conn, market="fin", symbol="NOKIA.HE", period_end_date="2025-12-31", revenue=30.0)
        _insert_yahoo_quarterly_row(conn, market="fin", symbol="NOKIA.HE", period_end_date="2025-03-31", revenue=10.0)
        _insert_yahoo_quarterly_row(conn, market="fin", symbol="NOKIA.HE", period_end_date="2025-06-30", revenue=20.0)
        conn.commit()

    run_fundamental_yahoo_to_quarterly.run_yahoo_to_quarterly(
        db_path=db_path,
        market="fin",
        symbol="NOKIA.HE",
        run_id="BRIDGE2",
        dry_run=False,
        replace_symbol=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT period_end_date, revenue FROM rc_fundamental_quarterly ORDER BY period_end_date"
        ).fetchall()
    assert rows == [("2025-03-31", 10.0), ("2025-06-30", 20.0), ("2025-12-31", 30.0)]


def test_snapshot_only_empty_rows_are_skipped(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_to_quarterly_skip.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_yahoo_quarterly_row(
            conn,
            market="fin",
            symbol="NOKIA.HE",
            period_end_date="2024-12-31",
            shares_outstanding=100.0,
            shares_source="snapshot",
            shares_quality="REVIEW",
        )
        _insert_yahoo_quarterly_row(conn, market="fin", symbol="NOKIA.HE", period_end_date="2025-03-31", revenue=10.0)
        conn.commit()

    summary = run_fundamental_yahoo_to_quarterly.run_yahoo_to_quarterly(
        db_path=db_path,
        market="fin",
        symbol="NOKIA.HE",
        run_id="BRIDGE3",
        dry_run=False,
        replace_symbol=False,
    )

    assert summary["input_rows"] == 2
    assert summary["rows_skipped"] == 1
    assert summary["rows_written"] == 1


def test_replace_symbol_deletes_only_rows_for_selected_ticker(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_to_quarterly_replace.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_yahoo_quarterly_row(conn, market="fin", symbol="NOKIA.HE", period_end_date="2025-03-31", revenue=10.0)
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker, period_end_date, revenue, run_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("NOKIA.HE", "2024-12-31", 1.0, "OLDRUN"),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker, period_end_date, revenue, run_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("OTHER.HE", "2024-12-31", 2.0, "OTHERRUN"),
        )
        conn.commit()

    run_fundamental_yahoo_to_quarterly.run_yahoo_to_quarterly(
        db_path=db_path,
        market="fin",
        symbol="NOKIA.HE",
        run_id="BRIDGE4",
        dry_run=False,
        replace_symbol=True,
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT ticker, period_end_date, revenue, run_id FROM rc_fundamental_quarterly ORDER BY ticker, period_end_date"
        ).fetchall()
    assert rows == [
        ("NOKIA.HE", "2025-03-31", 10.0, "BRIDGE4"),
        ("OTHER.HE", "2024-12-31", 2.0, "OTHERRUN"),
    ]


def test_field_mapping_is_correct(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_to_quarterly_map.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_yahoo_quarterly_row(
            conn,
            market="fin",
            symbol="NOKIA.HE",
            period_end_date="2025-03-31",
            revenue=1000.0,
            gross_profit=400.0,
            operating_income=150.0,
            net_income=120.0,
            operating_cashflow=200.0,
            capex=-50.0,
            free_cashflow=150.0,
            cash=300.0,
            total_debt=80.0,
            shares_outstanding=10000.0,
            shares_source="ordinary_shares_number",
            shares_quality="OK",
        )
        conn.commit()

    run_fundamental_yahoo_to_quarterly.run_yahoo_to_quarterly(
        db_path=db_path,
        market="fin",
        symbol="NOKIA.HE",
        run_id="BRIDGE5",
        dry_run=False,
        replace_symbol=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT
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
            FROM rc_fundamental_quarterly
            """
        ).fetchone()
    assert row == (
        "NOKIA.HE",
        "2025-03-31",
        1000.0,
        400.0,
        150.0,
        150.0,
        None,
        120.0,
        200.0,
        -50.0,
        150.0,
        300.0,
        80.0,
        10000.0,
        None,
        "BRIDGE5",
    )


def test_cli_summary_output(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_to_quarterly_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_yahoo_to_quarterly,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="fin",
            symbol="NOKIA.HE",
            run_id="BRIDGE6",
            dry_run=True,
            replace_symbol=False,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_yahoo_to_quarterly,
        "run_yahoo_to_quarterly",
        lambda **kwargs: {
            "market": "fin",
            "symbol": "NOKIA.HE",
            "source": "yahoo",
            "input_rows": 6,
            "rows_written": 0,
            "rows_skipped": 1,
            "dry_run": "true",
            "replace_symbol": "false",
            "run_id": "BRIDGE6",
        },
    )

    run_fundamental_yahoo_to_quarterly.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY market=fin",
        "SUMMARY symbol=NOKIA.HE",
        "SUMMARY source=yahoo",
        "SUMMARY input_rows=6",
        "SUMMARY rows_written=0",
        "SUMMARY rows_skipped=1",
        "SUMMARY dry_run=true",
        "SUMMARY replace_symbol=false",
        "SUMMARY run_id=BRIDGE6",
    ]
