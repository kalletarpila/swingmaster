from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_yahoo_to_quarterly import run_yahoo_to_quarterly
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)


BRIDGE_RUN_ID = "YAHOO_BRIDGE_RUN1"
AVAILABLE_AT_UTC = "2026-05-03T10:23:06Z"
INGESTED_AT_UTC = "2026-05-03T10:30:00Z"
VINTAGE_RUN_ID = "YAHOO_VINTAGE_RUN1"
NORMALIZATION_RUN_ID = "YAHOO_NORM_RUN1"


def test_default_cli_behavior_writes_latest_only_and_no_vintage(tmp_path: Path) -> None:
    db_path = _seed_temp_db(tmp_path)

    summary = run_yahoo_to_quarterly(
        db_path=db_path,
        market="usa",
        symbol="AAPL",
        run_id=BRIDGE_RUN_ID,
        dry_run=False,
        replace_symbol=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        latest_row = conn.execute(
            """
            SELECT ticker, period_end_date, revenue, net_income, cash, total_debt, run_id
            FROM rc_fundamental_quarterly
            """
        ).fetchone()
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert summary["rows_written"] == 1
    assert latest_row == ("AAPL", "2026-03-31", 100.0, 25.0, 80.0, 20.0, BRIDGE_RUN_ID)
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
        match=f"YAHOO_TO_QUARTERLY_CLI_VINTAGE_REQUIRED_FIELDS_MISSING:{expected_name}",
    ):
        run_yahoo_to_quarterly(
            db_path=db_path,
            market="usa",
            symbol="AAPL",
            run_id=BRIDGE_RUN_ID,
            dry_run=False,
            replace_symbol=False,
            write_vintage=True,
            **kwargs,
        )


def test_write_vintage_writes_latest_vintage_and_yahoo_provenance(tmp_path: Path) -> None:
    db_path = _seed_temp_db(tmp_path)

    summary = run_yahoo_to_quarterly(
        db_path=db_path,
        market="usa",
        symbol="AAPL",
        run_id=BRIDGE_RUN_ID,
        dry_run=False,
        replace_symbol=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        latest_row = conn.execute(
            """
            SELECT ticker, period_end_date, revenue, gross_profit, operating_income, ebit,
                   net_income, operating_cashflow, capex, free_cashflow, cash, total_debt,
                   shares_outstanding, run_id
            FROM rc_fundamental_quarterly
            """
        ).fetchone()
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, ticker, market, period_end_date, source_provider,
                   revenue, net_income, cash, total_debt, available_at_utc, ingested_at_utc,
                   run_id, normalization_run_id
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        before = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:23:05Z", market="usa")
        at_available = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", AVAILABLE_AT_UTC, market="usa")
        after = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:23:07Z", market="usa")
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[0]))

    assert summary["rows_written"] == 1
    assert latest_row == (
        "AAPL",
        "2026-03-31",
        100.0,
        40.0,
        30.0,
        30.0,
        25.0,
        35.0,
        -5.0,
        30.0,
        80.0,
        20.0,
        1000.0,
        VINTAGE_RUN_ID,
    )
    assert vintage_row[1:] == (
        "AAPL",
        "usa",
        "2026-03-31",
        "yahoo",
        100.0,
        25.0,
        80.0,
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
        "ebit",
        "free_cashflow",
        "gross_profit",
        "net_income",
        "operating_cashflow",
        "operating_income",
        "revenue",
        "shares_outstanding",
        "total_debt",
    ]
    for field_name in by_field:
        assert by_field[field_name]["source_provider"] == "yahoo"
        assert by_field[field_name]["provenance_role"] == "PROVIDER_REPORTED"
        assert by_field[field_name]["merge_action"] == "YAHOO_BRIDGED"


def test_write_vintage_duplicate_statement_vintage_id_fails(tmp_path: Path) -> None:
    db_path = _seed_temp_db(tmp_path)

    run_yahoo_to_quarterly(
        db_path=db_path,
        market="usa",
        symbol="AAPL",
        run_id=BRIDGE_RUN_ID,
        dry_run=False,
        replace_symbol=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with pytest.raises(sqlite3.IntegrityError):
        run_yahoo_to_quarterly(
            db_path=db_path,
            market="usa",
            symbol="AAPL",
            run_id=BRIDGE_RUN_ID,
            dry_run=False,
            replace_symbol=False,
            write_vintage=True,
            **_vintage_kwargs(),
        )


def test_write_vintage_dry_run_writes_nothing(tmp_path: Path) -> None:
    db_path = _seed_temp_db(tmp_path)

    summary = run_yahoo_to_quarterly(
        db_path=db_path,
        market="usa",
        symbol="AAPL",
        run_id=BRIDGE_RUN_ID,
        dry_run=True,
        replace_symbol=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0] == 0
    assert summary["rows_written"] == 0


def _seed_temp_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "yahoo_to_quarterly_cli_vintage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_yahoo_quarterly_row(conn)
        conn.commit()
    return db_path


def _insert_yahoo_quarterly_row(conn: sqlite3.Connection) -> None:
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
            "usa",
            "AAPL",
            "2026-03-31",
            100.0,
            40.0,
            30.0,
            25.0,
            35.0,
            -5.0,
            30.0,
            80.0,
            20.0,
            1000.0,
            "ordinary_shares_number",
            "OK",
            "YAHOO_RAW_RUN1",
            "YAHOO_QUARTERLY_RUN1",
            "2026-05-03T10:23:06Z",
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
