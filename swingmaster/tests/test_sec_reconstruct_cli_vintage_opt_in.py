from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_sec_reconstruct_quarterly import run_sec_reconstruct_quarterly
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)


RECON_RUN_ID = "SEC_RECON_RUN1"
RETRIEVED_AT_UTC = "2026-04-30T00:30:00Z"
AVAILABLE_AT_UTC = "2026-04-30T00:00:00Z"
INGESTED_AT_UTC = "2026-04-30T01:00:00Z"
VINTAGE_RUN_ID = "SEC_VINTAGE_RUN1"
NORMALIZATION_RUN_ID = "SEC_NORM_RUN1"


def test_default_cli_behavior_writes_reconstructed_raw_only_and_no_vintage(tmp_path: Path) -> None:
    db_path = _seed_temp_db(tmp_path)

    sec_fact_count, reconstructed_rows = run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker="aapl",
        run_id=RECON_RUN_ID,
        retrieved_at_utc=RETRIEVED_AT_UTC,
        dry_run=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        quarterly_raw_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_statement_raw
            WHERE ticker = 'AAPL' AND source = 'sec_edgar' AND period_type = 'quarterly'
            """
        ).fetchone()[0]
        latest_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert sec_fact_count == 7
    assert len(reconstructed_rows) == 6
    assert quarterly_raw_count == 6
    assert latest_count == 0
    assert vintage_count == 0
    assert provenance_count == 0


@pytest.mark.parametrize(
    ("missing_kwarg", "expected_name"),
    [
        ("vintage_market", "vintage_market"),
        ("vintage_available_at_utc", "vintage_available_at_utc"),
        ("vintage_ingested_at_utc", "vintage_ingested_at_utc"),
        ("vintage_run_id", "vintage_run_id"),
    ],
)
def test_write_vintage_requires_explicit_metadata_flags(
    tmp_path: Path,
    missing_kwarg: str,
    expected_name: str,
) -> None:
    db_path = _seed_temp_db(tmp_path)
    kwargs = _vintage_kwargs()
    kwargs[missing_kwarg] = None

    with pytest.raises(
        ValueError,
        match=f"SEC_RECONSTRUCT_CLI_VINTAGE_REQUIRED_FIELDS_MISSING:{expected_name}",
    ):
        run_sec_reconstruct_quarterly(
            db_path=db_path,
            ticker="AAPL",
            run_id=RECON_RUN_ID,
            retrieved_at_utc=RETRIEVED_AT_UTC,
            dry_run=False,
            write_vintage=True,
            **kwargs,
        )


def test_write_vintage_writes_latest_vintage_and_sec_provenance(tmp_path: Path) -> None:
    db_path = _seed_temp_db(tmp_path)

    sec_fact_count, reconstructed_rows = run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker="AAPL",
        run_id=RECON_RUN_ID,
        retrieved_at_utc=RETRIEVED_AT_UTC,
        dry_run=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        latest_row = conn.execute(
            """
            SELECT ticker, period_end_date, revenue, net_income, operating_cashflow, capex,
                   free_cashflow, cash, total_debt, run_id
            FROM rc_fundamental_quarterly
            """
        ).fetchone()
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, ticker, market, period_end_date, source_provider,
                   revenue, free_cashflow, total_debt, available_at_utc, ingested_at_utc,
                   run_id, normalization_run_id
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        before = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-04-29T23:59:59Z", market="usa")
        at_available = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", AVAILABLE_AT_UTC, market="usa")
        after = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-04-30T00:00:01Z", market="usa")
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[0]))

    assert sec_fact_count == 7
    assert len(reconstructed_rows) == 6
    assert latest_row == ("AAPL", "2026-03-31", 100.0, 25.0, 35.0, -5.0, 30.0, 80.0, 20.0, VINTAGE_RUN_ID)
    assert vintage_row[1:] == (
        "AAPL",
        "usa",
        "2026-03-31",
        "sec_edgar",
        100.0,
        30.0,
        20.0,
        AVAILABLE_AT_UTC,
        INGESTED_AT_UTC,
        VINTAGE_RUN_ID,
        NORMALIZATION_RUN_ID,
    )
    assert before is None
    assert at_available is not None
    assert after is not None
    assert at_available["statement_vintage_id"] == vintage_row[0]

    by_field = {row["field_name"]: row for row in provenance_rows}
    assert sorted(by_field) == [
        "capex",
        "cash",
        "free_cashflow",
        "net_income",
        "operating_cashflow",
        "revenue",
        "total_debt",
    ]
    for field_name in by_field:
        assert by_field[field_name]["source_provider"] == "sec_edgar"
        assert by_field[field_name]["provenance_role"] == "PRIMARY_REPORTED"
        assert by_field[field_name]["merge_action"] == "SEC_RETAINED"


def test_write_vintage_duplicate_statement_vintage_id_fails(tmp_path: Path) -> None:
    db_path = _seed_temp_db(tmp_path)

    run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker="AAPL",
        run_id=RECON_RUN_ID,
        retrieved_at_utc=RETRIEVED_AT_UTC,
        dry_run=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with pytest.raises(sqlite3.IntegrityError):
        run_sec_reconstruct_quarterly(
            db_path=db_path,
            ticker="AAPL",
            run_id=RECON_RUN_ID,
            retrieved_at_utc=RETRIEVED_AT_UTC,
            dry_run=False,
            write_vintage=True,
            **_vintage_kwargs(),
        )


def test_write_vintage_dry_run_writes_nothing(tmp_path: Path) -> None:
    db_path = _seed_temp_db(tmp_path)

    run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker="AAPL",
        run_id=RECON_RUN_ID,
        retrieved_at_utc=RETRIEVED_AT_UTC,
        dry_run=True,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_statement_raw WHERE period_type = 'quarterly'"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0] == 0


def _seed_temp_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "sec_reconstruct_cli_vintage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        for tag, value, statement_type, start in (
            ("Revenues", 100.0, "income", "2026-01-01"),
            ("NetIncomeLoss", 25.0, "income", "2026-01-01"),
            ("NetCashProvidedByUsedInOperatingActivities", 35.0, "cashflow", "2026-01-01"),
            ("PaymentsToAcquirePropertyPlantAndEquipment", 5.0, "cashflow", "2026-01-01"),
            ("CashAndCashEquivalentsAtCarryingValue", 80.0, "balance", "NULL"),
            ("LongTermDebtCurrent", 5.0, "balance", "NULL"),
            ("LongTermDebtNoncurrent", 15.0, "balance", "NULL"),
        ):
            _insert_sec_fact(conn, tag, value, statement_type, start=start)
        conn.commit()
    return db_path


def _insert_sec_fact(
    conn: sqlite3.Connection,
    tag: str,
    value: float,
    statement_type: str,
    *,
    start: str,
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
        ) VALUES (?, ?, ?, 'sec_fact', ?, ?, 'USD', 'sec_edgar', ?, 'SEC_RAW_RUN1')
        """,
        (
            "AAPL",
            statement_type,
            "2026-03-31",
            f"{tag}|form=10-Q|unit=USD|fy=2026|fp=Q1|frame=CY2026Q1|start={start}|filed=2026-04-29",
            value,
            RETRIEVED_AT_UTC,
        ),
    )


def _vintage_kwargs() -> dict[str, str]:
    return {
        "vintage_market": "usa",
        "vintage_available_at_utc": AVAILABLE_AT_UTC,
        "vintage_ingested_at_utc": INGESTED_AT_UTC,
        "vintage_run_id": VINTAGE_RUN_ID,
        "vintage_normalization_run_id": NORMALIZATION_RUN_ID,
    }
