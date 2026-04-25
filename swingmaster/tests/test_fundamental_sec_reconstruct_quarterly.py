from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_sec_reconstruct_quarterly
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.sec_reconstruct_quarterly import reconstruct_quarterly_rows


def test_flow_ytd_to_quarterly(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_flow.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-03-31", "Revenues", 100.0, "USD", "2025", "Q1")
        _insert_sec_fact(conn, "NVDA", "income", "2025-06-30", "Revenues", 250.0, "USD", "2025", "Q2")
        _insert_sec_fact(conn, "NVDA", "income", "2025-09-30", "Revenues", 450.0, "USD", "2025", "Q3")
        _insert_sec_fact(conn, "NVDA", "income", "2025-12-31", "Revenues", 700.0, "USD", "2025", "FY")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2025-03-31", 100.0),
        ("2025-06-30", 150.0),
        ("2025-09-30", 200.0),
        ("2025-12-31", 250.0),
    ]


def test_q4_requires_complete_year(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_partial_year.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-03-31", "Revenues", 100.0, "USD", "2025", "Q1")
        _insert_sec_fact(conn, "NVDA", "income", "2025-06-30", "Revenues", 250.0, "USD", "2025", "Q2")
        _insert_sec_fact(conn, "NVDA", "income", "2025-12-31", "Revenues", 700.0, "USD", "2025", "FY")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2025-03-31", 100.0),
        ("2025-06-30", 150.0),
    ]


def test_snapshot_values_copied(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_snapshot.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "balance", "2025-03-31", "CashAndCashEquivalentsAtCarryingValue", 10.0, "USD", "2025", "Q1")
        _insert_sec_fact(conn, "NVDA", "balance", "2025-06-30", "CashAndCashEquivalentsAtCarryingValue", 20.0, "USD", "2025", "Q2")
        _insert_sec_fact(conn, "NVDA", "balance", "2025-09-30", "CashAndCashEquivalentsAtCarryingValue", 30.0, "USD", "2025", "Q3")
        _insert_sec_fact(conn, "NVDA", "balance", "2025-12-31", "CashAndCashEquivalentsAtCarryingValue", 40.0, "USD", "2025", "FY")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    cash_rows = [row for row in rows if row["field_name"] == "Cash And Cash Equivalents"]
    assert [(row["period_end_date"], row["field_value"]) for row in cash_rows] == [
        ("2025-03-31", 10.0),
        ("2025-06-30", 20.0),
        ("2025-09-30", 30.0),
        ("2025-12-31", 40.0),
    ]


def test_capex_sign(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_capex.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "cashflow", "2025-03-31", "PaymentsToAcquirePropertyPlantAndEquipment", 10.0, "USD", "2025", "Q1")
        _insert_sec_fact(conn, "NVDA", "cashflow", "2025-06-30", "PaymentsToAcquirePropertyPlantAndEquipment", 30.0, "USD", "2025", "Q2")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    capex_rows = [row for row in rows if row["field_name"] == "Capital Expenditure"]
    assert [(row["period_end_date"], row["field_value"]) for row in capex_rows] == [
        ("2025-03-31", -10.0),
        ("2025-06-30", -20.0),
    ]


def test_duplicate_fact_selection(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_duplicate.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-03-31", "Revenues", 100.0, "USD", "2025", "Q1", frame="NULL", filed="2025-04-01")
        _insert_sec_fact(conn, "NVDA", "income", "2025-03-31", "Revenues", 120.0, "USD", "2025", "Q1", frame="CY2025Q1", filed="2025-04-30")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert revenues[0]["field_value"] == 120.0


def test_revenue_tag_priority(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_revenue_priority.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-03-31", "Revenues", 100.0, "USD", "2025", "Q1")
        _insert_sec_fact(conn, "NVDA", "income", "2025-03-31", "RevenueFromContractWithCustomerExcludingAssessedTax", 110.0, "USD", "2025", "Q1")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert revenues[0]["field_value"] == 100.0


def test_total_debt_component_sum(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_debt.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "balance", "2025-03-31", "LongTermDebtCurrent", 30.0, "USD", "2025", "Q1")
        _insert_sec_fact(conn, "NVDA", "balance", "2025-03-31", "LongTermDebtNoncurrent", 70.0, "USD", "2025", "Q1")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    debt_rows = [row for row in rows if row["field_name"] == "Total Debt"]
    assert debt_rows[0]["field_value"] == 100.0


def test_dry_run_writes_nothing(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_dry_run.db"
    run_migration(db_path)
    _seed_reconstruction_rows(db_path)
    run_fundamental_sec_reconstruct_quarterly.run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker="NVDA",
        run_id="RUN1",
        retrieved_at_utc="2026-04-25T00:00:00Z",
        dry_run=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_statement_raw WHERE source='sec_edgar' AND period_type='quarterly'"
        ).fetchone()[0]
    assert count == 0


def test_idempotency(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_idempotent.db"
    run_migration(db_path)
    _seed_reconstruction_rows(db_path)
    run_fundamental_sec_reconstruct_quarterly.run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker="NVDA",
        run_id="RUN1",
        retrieved_at_utc="2026-04-25T00:00:00Z",
        dry_run=False,
    )
    run_fundamental_sec_reconstruct_quarterly.run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker="NVDA",
        run_id="RUN1",
        retrieved_at_utc="2026-04-25T00:00:00Z",
        dry_run=False,
    )
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_statement_raw WHERE source='sec_edgar' AND period_type='quarterly'"
        ).fetchone()[0]
    assert count == 4


def test_no_sec_fact_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_empty.db"
    run_migration(db_path)
    with pytest.raises(RuntimeError, match="^SEC_FACT_ROWS_NOT_FOUND:NVDA$"):
        run_fundamental_sec_reconstruct_quarterly.run_sec_reconstruct_quarterly(
            db_path=db_path,
            ticker="NVDA",
            run_id="RUN1",
            retrieved_at_utc="2026-04-25T00:00:00Z",
            dry_run=False,
        )


def test_cli_summary(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_cli.db"
    run_migration(db_path)
    _seed_reconstruction_rows(db_path)
    monkeypatch.setattr(
        run_fundamental_sec_reconstruct_quarterly,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            ticker="NVDA",
            run_id="RUN1",
            retrieved_at_utc="2026-04-25T00:00:00Z",
            dry_run=True,
        ),
    )
    run_fundamental_sec_reconstruct_quarterly.main()
    out = capsys.readouterr().out
    assert "SUMMARY ticker=NVDA" in out
    assert "SUMMARY source=sec_edgar" in out
    assert "SUMMARY period_type=quarterly" in out
    assert "SUMMARY status=dry-run" in out


def _seed_reconstruction_rows(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-03-31", "Revenues", 100.0, "USD", "2025", "Q1")
        _insert_sec_fact(conn, "NVDA", "income", "2025-06-30", "Revenues", 250.0, "USD", "2025", "Q2")
        _insert_sec_fact(conn, "NVDA", "income", "2025-09-30", "Revenues", 450.0, "USD", "2025", "Q3")
        _insert_sec_fact(conn, "NVDA", "income", "2025-12-31", "Revenues", 700.0, "USD", "2025", "FY")
        conn.commit()


def _insert_sec_fact(
    conn: sqlite3.Connection,
    ticker: str,
    statement_type: str,
    period_end_date: str,
    tag: str,
    value: float,
    unit: str,
    fy: str,
    fp: str,
    *,
    form: str = "10-Q",
    frame: str = "CY2025Q1",
    start: str = "2025-01-01",
    filed: str = "2025-04-30",
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
        ) VALUES (?, ?, ?, 'sec_fact', ?, ?, ?, 'sec_edgar', '2026-04-25T00:00:00Z', 'SEC_RAW_V1')
        """,
        (
            ticker.upper(),
            statement_type,
            period_end_date,
            f"{tag}|form={form}|unit={unit}|fy={fy}|fp={fp}|frame={frame}|start={start}|filed={filed}",
            value,
            unit,
        ),
    )
