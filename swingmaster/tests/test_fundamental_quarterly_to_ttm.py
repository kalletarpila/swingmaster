from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_quarterly_to_ttm
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_quarterly_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    period_end_date: str,
    revenue: float | None,
    gross_profit: float | None,
    operating_income: float | None,
    net_income: float | None,
    operating_cashflow: float | None,
    capex: float | None,
    free_cashflow: float | None,
    cash: float | None,
    total_debt: float | None,
    shares_outstanding: float | None,
    run_id: str,
) -> None:
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            operating_income,
            None,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            None,
            run_id,
        ),
    )


def _insert_nokia_five_quarters(conn: sqlite3.Connection) -> None:
    rows = [
        ("2025-03-31", 4390000000.0, 1824000000.0, -21000000.0, -59000000.0, 890000000.0, -169000000.0, 721000000.0, 5543000000.0, 5065000000.0, 5380831000.0),
        ("2025-06-30", 4546000000.0, 1971000000.0, 80000000.0, 90000000.0, 209000000.0, -121000000.0, 88000000.0, 4797000000.0, 4100000000.0, 5379095249.0),
        ("2025-09-30", 4828000000.0, 2110000000.0, 239000000.0, 78000000.0, 597000000.0, -168000000.0, 429000000.0, 4892000000.0, 4065000000.0, 5413481372.0),
        ("2025-12-31", 6125000000.0, 2754000000.0, 437000000.0, 542000000.0, 375000000.0, -148000000.0, 227000000.0, 5462000000.0, 4413000000.0, 5582534171.0),
        ("2026-03-31", 4497000000.0, 1988000000.0, 63000000.0, 86000000.0, 783000000.0, -154000000.0, 629000000.0, 4951000000.0, 3325000000.0, 5582534171.0),
    ]
    for row in rows:
        _insert_quarterly_row(
            conn,
            ticker="NOKIA.HE",
            period_end_date=row[0],
            revenue=row[1],
            gross_profit=row[2],
            operating_income=row[3],
            net_income=row[4],
            operating_cashflow=row[5],
            capex=row[6],
            free_cashflow=row[7],
            cash=row[8],
            total_debt=row[9],
            shares_outstanding=row[10],
            run_id="QTRRUN1",
        )


def test_dry_run_writes_zero_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "quarterly_to_ttm_dry.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_nokia_five_quarters(conn)
        conn.commit()

    summary = run_fundamental_quarterly_to_ttm.run_quarterly_to_ttm(
        db_path=db_path,
        ticker="NOKIA.HE",
        run_id="TTMRUN1",
        dry_run=True,
        replace_ticker=False,
    )

    assert summary == {
        "ticker": "NOKIA.HE",
        "input_quarterly_rows": 5,
        "ttm_rows_built": 2,
        "rows_written": 0,
        "dry_run": "true",
        "run_id": "TTMRUN1",
    }
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0]
    assert count == 0


def test_builds_two_ttm_rows_for_five_quarters(tmp_path: Path) -> None:
    db_path = tmp_path / "quarterly_to_ttm_rows.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_nokia_five_quarters(conn)
        conn.commit()

    summary = run_fundamental_quarterly_to_ttm.run_quarterly_to_ttm(
        db_path=db_path,
        ticker="NOKIA.HE",
        run_id="TTMRUN2",
        dry_run=False,
        replace_ticker=False,
    )

    assert summary["input_quarterly_rows"] == 5
    assert summary["ttm_rows_built"] == 2
    assert summary["rows_written"] == 2
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT ticker, as_of_date, latest_period_end_date, run_id FROM rc_fundamental_ttm ORDER BY as_of_date"
        ).fetchall()
    assert rows == [
        ("NOKIA.HE", "2025-12-31", "2025-12-31", "TTMRUN2"),
        ("NOKIA.HE", "2026-03-31", "2026-03-31", "TTMRUN2"),
    ]


def test_replace_ticker_deletes_existing_rows_for_selected_ticker(tmp_path: Path) -> None:
    db_path = tmp_path / "quarterly_to_ttm_replace.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_nokia_five_quarters(conn)
        conn.execute(
            """
            INSERT INTO rc_fundamental_ttm (
                ticker, as_of_date, latest_period_end_date, run_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("NOKIA.HE", "2025-12-31", "2025-12-31", "OLDRUN"),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_ttm (
                ticker, as_of_date, latest_period_end_date, run_id
            ) VALUES (?, ?, ?, ?)
            """,
            ("OTHER.HE", "2025-12-31", "2025-12-31", "OTHEROLD"),
        )
        conn.commit()

    run_fundamental_quarterly_to_ttm.run_quarterly_to_ttm(
        db_path=db_path,
        ticker="NOKIA.HE",
        run_id="TTMRUN3",
        dry_run=False,
        replace_ticker=True,
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT ticker, as_of_date, run_id FROM rc_fundamental_ttm ORDER BY ticker, as_of_date"
        ).fetchall()
    assert rows == [
        ("NOKIA.HE", "2025-12-31", "TTMRUN3"),
        ("NOKIA.HE", "2026-03-31", "TTMRUN3"),
        ("OTHER.HE", "2025-12-31", "OTHEROLD"),
    ]


def test_cli_summary_output(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "quarterly_to_ttm_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_quarterly_to_ttm,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            ticker="NOKIA.HE",
            run_id="TTMRUN4",
            dry_run=True,
            replace_ticker=False,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_quarterly_to_ttm,
        "run_quarterly_to_ttm",
        lambda **kwargs: {
            "ticker": "NOKIA.HE",
            "input_quarterly_rows": 5,
            "ttm_rows_built": 2,
            "rows_written": 0,
            "dry_run": "true",
            "run_id": "TTMRUN4",
        },
    )

    run_fundamental_quarterly_to_ttm.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY ticker=NOKIA.HE",
        "SUMMARY input_quarterly_rows=5",
        "SUMMARY ttm_rows_built=2",
        "SUMMARY rows_written=0",
        "SUMMARY dry_run=true",
        "SUMMARY run_id=TTMRUN4",
    ]
