from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_sec_reconstruct_quarterly
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.sec_reconstruct_quarterly import reconstruct_quarterly_rows


def test_quarterly_facts_preferred_over_ytd_facts(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_quarterly_preferred.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-04-27", "Revenues", 100.0, "USD", "2025", "Q1", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-07-27", "Revenues", 250.0, "USD", "2025", "Q2", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-07-27", "Revenues", 150.0, "USD", "2025", "Q2", frame="CY2025Q2", start="2025-04-28")
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 400.0, "USD", "2025", "FY", form="10-K", start="2025-01-27")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2025-04-27", 100.0),
        ("2025-07-27", 150.0),
    ]


def test_q1_start_null_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_q1_start_null.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2024-04-28", "Revenues", 100.0, "USD", "2024", "Q1", form="10-Q", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2025-01-26", "Revenues", 400.0, "USD", "2024", "FY", form="10-K", start="2024-01-29")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2024-04-28", 100.0),
    ]


def test_q2_start_null_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_q2_start_null.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2024-07-28", "Revenues", 150.0, "USD", "2024", "Q2", form="10-Q", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2025-01-26", "Revenues", 400.0, "USD", "2024", "FY", form="10-K", start="2024-01-29")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2024-07-28", 150.0),
    ]


def test_form_10k_start_null_does_not_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_10k_start_null.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2024-07-28", "Revenues", 150.0, "USD", "2024", "Q2", form="10-K", start="NULL")
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "CashAndCashEquivalentsAtCarryingValue", 10.0, "USD", "2024", "Q2")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    assert [row for row in rows if row["field_name"] == "Total Revenue"] == []


def test_fp_fy_start_null_does_not_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_fy_start_null.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2024-12-31", "Revenues", 700.0, "USD", "2024", "FY", form="10-K", start="NULL")
        _insert_sec_fact(conn, "NVDA", "balance", "2024-12-31", "CashAndCashEquivalentsAtCarryingValue", 10.0, "USD", "2024", "FY")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    assert [row for row in rows if row["field_name"] == "Total Revenue"] == []


def test_ytd_fallback_when_quarterly_fact_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_ytd_fallback.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-04-27", "Revenues", 100.0, "USD", "2025", "Q1", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-07-27", "Revenues", 250.0, "USD", "2025", "Q2", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-10-26", "Revenues", 450.0, "USD", "2025", "Q3", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 700.0, "USD", "2025", "FY", start="2025-01-27")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2025-04-27", 100.0),
        ("2025-07-27", 150.0),
        ("2025-10-26", 200.0),
        ("2026-01-25", 250.0),
    ]


def test_fy_annual_is_not_used_directly_as_q4_when_q1_q3_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_fy_annual_only.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 700.0, "USD", "2025", "FY", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "balance", "2026-01-25", "CashAndCashEquivalentsAtCarryingValue", 10.0, "USD", "2025", "FY")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    assert [row for row in rows if row["field_name"] == "Total Revenue"] == []


def test_fy_quarterly_duration_can_be_used_directly(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_fy_quarterly.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 250.0, "USD", "2025", "FY", start="2025-10-27")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2026-01-25", 250.0),
    ]


def test_nvda_like_mixed_quarterly_ytd_facts(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_nvda_like.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-04-27", "Revenues", 44.0, "USD", "2025", "Q1", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-07-27", "Revenues", 74.0, "USD", "2025", "Q2", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-07-27", "Revenues", 30.0, "USD", "2025", "Q2", frame="CY2025Q2", start="2025-04-28")
        _insert_sec_fact(conn, "NVDA", "income", "2025-10-26", "Revenues", 109.0, "USD", "2025", "Q3", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-10-26", "Revenues", 35.0, "USD", "2025", "Q3", frame="CY2025Q3", start="2025-07-28")
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 159.0, "USD", "2025", "FY", start="2025-01-27")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2025-04-27", 44.0),
        ("2025-07-27", 30.0),
        ("2025-10-26", 35.0),
        ("2026-01-25", 50.0),
    ]


def test_q3_quarterly_duration_preferred_over_q3_ytd(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_q3_quarterly_preferred.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-10-27", "Revenues", 91166000000.0, "USD", "2025", "Q3", start="2024-01-29")
        _insert_sec_fact(conn, "NVDA", "income", "2025-10-27", "Revenues", 35082000000.0, "USD", "2025", "Q3", start="2025-07-29")
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 159000000000.0, "USD", "2025", "FY", form="10-K", start="2025-01-27")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2025-10-27", 35082000000.0),
    ]


def test_flow_reconstruction_builds_two_separate_fiscal_chains(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_two_chains.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2023-04-30", "Revenues", 10.0, "USD", "2025", "Q1", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2023-07-30", "Revenues", 30.0, "USD", "2025", "Q2", start="2023-01-30")
        _insert_sec_fact(conn, "NVDA", "income", "2023-10-29", "Revenues", 60.0, "USD", "2025", "Q3", start="2023-01-30")
        _insert_sec_fact(conn, "NVDA", "income", "2024-01-28", "Revenues", 100.0, "USD", "2025", "FY", form="10-K", start="2023-01-30")
        _insert_sec_fact(conn, "NVDA", "income", "2024-04-28", "Revenues", 20.0, "USD", "2025", "Q1", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2024-07-28", "Revenues", 50.0, "USD", "2025", "Q2", start="2024-01-29")
        _insert_sec_fact(conn, "NVDA", "income", "2024-10-27", "Revenues", 90.0, "USD", "2025", "Q3", start="2024-01-29")
        _insert_sec_fact(conn, "NVDA", "income", "2025-01-26", "Revenues", 140.0, "USD", "2025", "FY", form="10-K", start="2024-01-29")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2023-04-30", 10.0),
        ("2023-07-30", 20.0),
        ("2023-10-29", 30.0),
        ("2024-01-28", 40.0),
        ("2024-04-28", 20.0),
        ("2024-07-28", 30.0),
        ("2024-10-27", 40.0),
        ("2025-01-26", 50.0),
    ]


def test_comparative_chain_does_not_overwrite_current_chain(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_chain_no_overwrite.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2023-04-30", "Revenues", 10.0, "USD", "2025", "Q1", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2023-07-30", "Revenues", 20.0, "USD", "2025", "Q2", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2023-10-29", "Revenues", 30.0, "USD", "2025", "Q3", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2024-01-28", "Revenues", 100.0, "USD", "2025", "FY", form="10-K", start="2023-01-30")
        _insert_sec_fact(conn, "NVDA", "income", "2024-04-28", "Revenues", 200.0, "USD", "2025", "Q1", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2024-07-28", "Revenues", 300.0, "USD", "2025", "Q2", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2024-10-27", "Revenues", 400.0, "USD", "2025", "Q3", start="NULL")
        _insert_sec_fact(conn, "NVDA", "income", "2025-01-26", "Revenues", 1000.0, "USD", "2025", "FY", form="10-K", start="2024-01-29")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = {row["period_end_date"]: row["field_value"] for row in rows if row["field_name"] == "Total Revenue"}
    assert revenues["2024-07-28"] == 300.0
    assert revenues["2025-01-26"] == 100.0


def test_highest_value_must_not_override_shortest_duration(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shortest_duration_preferred.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-07-27", "Revenues", 1000.0, "USD", "2025", "Q2", frame="CY2025Q2_LONG", start="2025-01-01")
        _insert_sec_fact(conn, "NVDA", "income", "2025-07-27", "Revenues", 100.0, "USD", "2025", "Q2", frame="CY2025Q2_SHORT", start="2025-04-28")
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 400.0, "USD", "2025", "FY", form="10-K", start="2025-01-27")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    revenues = [row for row in rows if row["field_name"] == "Total Revenue"]
    assert [(row["period_end_date"], row["field_value"]) for row in revenues] == [
        ("2025-07-27", 100.0),
    ]


def test_cashflow_follows_same_flow_logic(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_cashflow.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            "NVDA",
            "cashflow",
            "2025-04-27",
            "NetCashProvidedByUsedInOperatingActivities",
            100.0,
            "USD",
            "2025",
            "Q1",
            start="2025-01-27",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "cashflow",
            "2025-07-27",
            "NetCashProvidedByUsedInOperatingActivities",
            260.0,
            "USD",
            "2025",
            "Q2",
            start="2025-01-27",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "cashflow",
            "2025-10-26",
            "NetCashProvidedByUsedInOperatingActivities",
            450.0,
            "USD",
            "2025",
            "Q3",
            start="2025-01-27",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "cashflow",
            "2026-01-25",
            "NetCashProvidedByUsedInOperatingActivities",
            700.0,
            "USD",
            "2025",
            "FY",
            start="2025-01-27",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    operating_cf = [row for row in rows if row["field_name"] == "Operating Cash Flow"]
    assert [(row["period_end_date"], row["field_value"]) for row in operating_cf] == [
        ("2025-04-27", 100.0),
        ("2025-07-27", 160.0),
        ("2025-10-26", 190.0),
        ("2026-01-25", 250.0),
    ]


def test_cashflow_quarterly_duration_preferred_over_ytd(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_cashflow_quarterly_preferred.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            "NVDA",
            "cashflow",
            "2025-07-27",
            "NetCashProvidedByUsedInOperatingActivities",
            260.0,
            "USD",
            "2025",
            "Q2",
            start="2025-01-27",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "cashflow",
            "2025-07-27",
            "NetCashProvidedByUsedInOperatingActivities",
            160.0,
            "USD",
            "2025",
            "Q2",
            start="2025-04-28",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "cashflow",
            "2026-01-25",
            "NetCashProvidedByUsedInOperatingActivities",
            700.0,
            "USD",
            "2025",
            "FY",
            form="10-K",
            start="2025-01-27",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    operating_cf = [row for row in rows if row["field_name"] == "Operating Cash Flow"]
    assert [(row["period_end_date"], row["field_value"]) for row in operating_cf] == [
        ("2025-07-27", 160.0),
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


def test_snapshot_tags_unaffected_with_start_null(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_snapshot_start_null.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "CashAndCashEquivalentsAtCarryingValue",
            123.0,
            "USD",
            "2024",
            "Q2",
            start="NULL",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    cash_rows = [row for row in rows if row["field_name"] == "Cash And Cash Equivalents"]
    assert [(row["period_end_date"], row["field_value"]) for row in cash_rows] == [
        ("2024-07-28", 123.0),
    ]


def test_entity_common_stock_shares_outstanding_preferred(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_entity_preferred.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "EntityCommonStockSharesOutstanding", 100.0, "shares", "2024", "Q2")
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "WeightedAverageNumberOfDilutedSharesOutstanding", 90.0, "shares", "2024", "Q2")
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "WeightedAverageNumberOfSharesOutstandingBasic", 80.0, "shares", "2024", "Q2")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 100.0),
    ]


def test_common_stock_shares_outstanding_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_common_stock_fallback.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "CommonStockSharesOutstanding", 95.0, "shares", "2024", "Q2")
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "WeightedAverageNumberOfDilutedSharesOutstanding", 90.0, "shares", "2024", "Q2")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 95.0),
    ]


def test_weighted_average_diluted_shares_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_diluted_fallback.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "WeightedAverageNumberOfDilutedSharesOutstanding", 90.0, "shares", "2024", "Q2")
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "WeightedAverageNumberOfSharesOutstandingBasic", 80.0, "shares", "2024", "Q2")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 90.0),
    ]


def test_weighted_average_shares_prefers_split_adjusted_later_filing(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_split_adjusted.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            2_489_000_000.0,
            "shares",
            "2024",
            "Q2",
            frame="NULL",
            filed="2024-08-28",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            24_890_000_000.0,
            "shares",
            "2024",
            "Q2",
            frame="CY2024Q1",
            filed="2025-08-27",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 24_890_000_000.0),
    ]


def test_weighted_average_shares_prefers_latest_filed_date(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_latest_filed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            90.0,
            "shares",
            "2024",
            "Q2",
            frame="CY2024Q2",
            filed="2024-08-28",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            95.0,
            "shares",
            "2024",
            "Q2",
            frame="CY2024Q2",
            filed="2025-08-27",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 95.0),
    ]


def test_weighted_average_shares_prefers_lexicographically_largest_frame(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_frame_priority.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            90.0,
            "shares",
            "2024",
            "Q2",
            frame="CY2024Q1",
            filed="2025-08-27",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            95.0,
            "shares",
            "2024",
            "Q2",
            frame="CY2024Q2",
            filed="2025-08-27",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 95.0),
    ]


def test_weighted_average_shares_prefers_largest_value_after_other_ties(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_value_priority.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            90.0,
            "shares",
            "2024",
            "Q2",
            frame="CY2024Q2",
            filed="2025-08-27",
            start="2024-04-01",
        )
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            95.0,
            "shares",
            "2024",
            "Q2",
            frame="CY2024Q2",
            filed="2025-08-27",
            start="2024-04-02",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 95.0),
    ]


def test_entity_common_stock_shares_outstanding_ignores_weighted_average_tie_breakers(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_entity_still_wins.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "EntityCommonStockSharesOutstanding", 100.0, "shares", "2024", "Q2")
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            24_890_000_000.0,
            "shares",
            "2024",
            "Q2",
            frame="CY2024Q2",
            filed="2025-08-27",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 100.0),
    ]


def test_basic_shares_final_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_basic_fallback.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "WeightedAverageNumberOfSharesOutstandingBasic", 80.0, "shares", "2024", "Q2")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 80.0),
    ]


def test_shares_are_not_flow_reconstructed(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_not_flow.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            "NVDA",
            "balance",
            "2024-07-28",
            "CommonStockSharesOutstanding",
            95.0,
            "shares",
            "2024",
            "Q2",
            start="2024-01-01",
        )
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 95.0),
    ]


def test_share_fallback_improves_historical_coverage(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_shares_historical_coverage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "balance", "2024-07-28", "CommonStockSharesOutstanding", 95.0, "shares", "2024", "Q2")
        _insert_sec_fact(conn, "NVDA", "balance", "2025-04-27", "EntityCommonStockSharesOutstanding", 100.0, "shares", "2025", "Q1")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    shares_rows = [row for row in rows if row["field_name"] == "Ordinary Shares Number"]
    assert [(row["period_end_date"], row["field_value"]) for row in shares_rows] == [
        ("2024-07-28", 95.0),
        ("2025-04-27", 100.0),
    ]


def test_capex_sign(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_capex.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "cashflow", "2025-04-27", "PaymentsToAcquirePropertyPlantAndEquipment", 10.0, "USD", "2025", "Q1", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "cashflow", "2025-07-27", "PaymentsToAcquirePropertyPlantAndEquipment", 30.0, "USD", "2025", "Q2", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "cashflow", "2026-01-25", "PaymentsToAcquirePropertyPlantAndEquipment", 60.0, "USD", "2025", "FY", form="10-K", start="2025-01-27")
        conn.commit()
        rows = reconstruct_quarterly_rows(
            run_fundamental_sec_reconstruct_quarterly.load_sec_fact_rows(conn, "NVDA"),
            "NVDA",
            "RUN1",
            "2026-04-25T00:00:00Z",
        )
    capex_rows = [row for row in rows if row["field_name"] == "Capital Expenditure"]
    assert [(row["period_end_date"], row["field_value"]) for row in capex_rows] == [
        ("2025-04-27", -10.0),
        ("2025-07-27", -20.0),
    ]


def test_duplicate_fact_selection(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruct_duplicate.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(conn, "NVDA", "income", "2025-04-27", "Revenues", 100.0, "USD", "2025", "Q1", frame="NULL", filed="2025-04-01", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-04-27", "Revenues", 120.0, "USD", "2025", "Q1", frame="CY2025Q1", filed="2025-04-30", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 400.0, "USD", "2025", "FY", form="10-K", start="2025-01-27")
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
        _insert_sec_fact(conn, "NVDA", "income", "2025-04-27", "Revenues", 100.0, "USD", "2025", "Q1", start="2025-01-27")
        _insert_sec_fact(
            conn,
            "NVDA",
            "income",
            "2025-04-27",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            110.0,
            "USD",
            "2025",
            "Q1",
            start="2025-01-27",
        )
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 400.0, "USD", "2025", "FY", form="10-K", start="2025-01-27")
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


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
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
        _insert_sec_fact(conn, "NVDA", "income", "2025-04-27", "Revenues", 100.0, "USD", "2025", "Q1", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-07-27", "Revenues", 250.0, "USD", "2025", "Q2", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2025-10-26", "Revenues", 450.0, "USD", "2025", "Q3", start="2025-01-27")
        _insert_sec_fact(conn, "NVDA", "income", "2026-01-25", "Revenues", 700.0, "USD", "2025", "FY", start="2025-01-27")
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
    frame: str = "NULL",
    start: str = "NULL",
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
