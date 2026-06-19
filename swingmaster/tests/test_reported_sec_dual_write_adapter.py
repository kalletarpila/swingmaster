from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_sec_dual_write_adapter import (
    write_sec_reconstructed_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_sec_vintage_metadata import build_sec_source_hash
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)


def test_default_write_vintage_false_writes_only_latest_row(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_latest_only.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[_normalized_row()],
        )
        latest_row = conn.execute("SELECT ticker, period_end_date, revenue, run_id FROM rc_fundamental_quarterly").fetchone()
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert result == {
        "latest_rows_written": 1,
        "vintage_rows_written": 0,
        "field_provenance_rows_written": 0,
    }
    assert latest_row == ("AAPL", "2026-03-31", 100.0, "SEC_LATEST_RUN1")
    assert vintage_count == 0
    assert provenance_count == 0


def test_write_vintage_true_writes_latest_vintage_and_sec_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_vintage.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[_normalized_row()],
            contributing_facts_by_key={_key(): _field_to_facts()},
            write_vintage=True,
            available_at_utc="2026-04-30T00:00:00Z",
            ingested_at_utc="2026-04-30T01:00:00Z",
            run_id="SEC_DUAL_RUN1",
            normalization_run_id="SEC_NORM_RUN1",
        )
        latest_row = conn.execute("SELECT ticker, revenue, run_id FROM rc_fundamental_quarterly").fetchone()
        vintage_row = conn.execute(
            """
            SELECT ticker, market, statement_vintage_id, source_provider, revenue, run_id
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[2]))

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 1
    assert result["field_provenance_rows_written"] == len(provenance_rows)
    assert latest_row == ("AAPL", 100.0, "SEC_DUAL_RUN1")
    assert vintage_row[0] == "AAPL"
    assert vintage_row[1] == "usa"
    assert vintage_row[2].startswith("sec_edgar:usa:AAPL:2026-03-31:")
    assert vintage_row[3] == "sec_edgar"
    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "sec_edgar"
    assert by_field["revenue"]["provenance_role"] == "PRIMARY_REPORTED"
    assert by_field["revenue"]["merge_action"] == "SEC_RETAINED"


def test_write_vintage_true_requires_available_at_utc(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_missing_available.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_SEC_DUAL_WRITE_REQUIRED_FIELD_MISSING:available_at_utc"):
            write_sec_reconstructed_quarterly_rows_with_optional_vintage(
                conn,
                normalized_rows=[_normalized_row()],
                contributing_facts_by_key={_key(): _field_to_facts()},
                write_vintage=True,
                ingested_at_utc="2026-04-30T01:00:00Z",
                run_id="SEC_DUAL_RUN1",
            )


def test_write_vintage_true_requires_contributing_sec_facts_for_row(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_missing_facts.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_SEC_DUAL_WRITE_CONTRIBUTING_FACTS_MISSING:AAPL,2026-03-31"):
            write_sec_reconstructed_quarterly_rows_with_optional_vintage(
                conn,
                normalized_rows=[_normalized_row()],
                contributing_facts_by_key={},
                write_vintage=True,
                available_at_utc="2026-04-30T00:00:00Z",
                ingested_at_utc="2026-04-30T01:00:00Z",
                run_id="SEC_DUAL_RUN1",
            )


def test_generated_statement_vintage_id_follows_sec_format(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_vintage_id.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[_normalized_row()],
            contributing_facts_by_key={_key(): _field_to_facts()},
            write_vintage=True,
            available_at_utc="2026-04-30T00:00:00Z",
            ingested_at_utc="2026-04-30T01:00:00Z",
            run_id="SEC_DUAL_RUN1",
        )
        statement_vintage_id = conn.execute("SELECT statement_vintage_id FROM rc_fundamental_quarterly_vintage").fetchone()[0]

    assert statement_vintage_id.startswith("sec_edgar:usa:AAPL:2026-03-31:")
    assert len(statement_vintage_id.rsplit(":", 1)[1]) == 16


def test_generated_source_hash_is_deterministic(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_source_hash.db"
    run_migration(db_path)
    expected_hash = build_sec_source_hash(
        ticker="AAPL",
        period_end_date="2026-03-31",
        contributing_facts=[fact for facts in _field_to_facts().values() for fact in facts],
        normalized_row=_normalized_row(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[_normalized_row()],
            contributing_facts_by_key={_key(): _field_to_facts()},
            write_vintage=True,
            available_at_utc="2026-04-30T00:00:00Z",
            ingested_at_utc="2026-04-30T01:00:00Z",
            run_id="SEC_DUAL_RUN1",
        )
        actual_hash = conn.execute("SELECT source_hash FROM rc_fundamental_quarterly_vintage").fetchone()[0]

    assert actual_hash == expected_hash


def test_pit_reader_returns_row_at_available_at_and_none_before(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_pit.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[_normalized_row()],
            contributing_facts_by_key={_key(): _field_to_facts()},
            write_vintage=True,
            available_at_utc="2026-04-30T00:00:00Z",
            ingested_at_utc="2026-04-30T01:00:00Z",
            run_id="SEC_DUAL_RUN1",
        )
        before = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-04-29T23:59:59Z", market="usa")
        after = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-04-30T00:00:00Z", market="usa")

    assert before is None
    assert after is not None
    assert after["source_provider"] == "sec_edgar"


def test_missing_contributing_facts_raises_value_error(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_empty_facts.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_SEC_DUAL_WRITE_CONTRIBUTING_FACTS_MISSING:AAPL,2026-03-31"):
            write_sec_reconstructed_quarterly_rows_with_optional_vintage(
                conn,
                normalized_rows=[_normalized_row()],
                contributing_facts_by_key={_key(): {}},
                write_vintage=True,
                available_at_utc="2026-04-30T00:00:00Z",
                ingested_at_utc="2026-04-30T01:00:00Z",
                run_id="SEC_DUAL_RUN1",
            )


def test_duplicate_vintage_insert_raises_integrity_error(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_dual_write_duplicate.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[_normalized_row()],
            contributing_facts_by_key={_key(): _field_to_facts()},
            write_vintage=True,
            available_at_utc="2026-04-30T00:00:00Z",
            ingested_at_utc="2026-04-30T01:00:00Z",
            run_id="SEC_DUAL_RUN1",
        )
        with pytest.raises(sqlite3.IntegrityError):
            write_sec_reconstructed_quarterly_rows_with_optional_vintage(
                conn,
                normalized_rows=[_normalized_row()],
                contributing_facts_by_key={_key(): _field_to_facts()},
                write_vintage=True,
                available_at_utc="2026-04-30T00:00:00Z",
                ingested_at_utc="2026-04-30T01:00:00Z",
                run_id="SEC_DUAL_RUN1",
            )


def test_scaffold_does_not_import_provider_modules() -> None:
    import sys

    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules
    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules
    assert "yfinance" not in sys.modules


def _key() -> tuple[str, str]:
    return "AAPL", "2026-03-31"


def _normalized_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "gross_profit": None,
        "operating_income": None,
        "ebit": None,
        "ebitda": None,
        "net_income": 25.0,
        "operating_cashflow": None,
        "capex": None,
        "free_cashflow": None,
        "cash": 80.0,
        "total_debt": 20.0,
        "shares_outstanding": None,
        "currency": "USD",
        "run_id": "SEC_LATEST_RUN1",
    }
    row.update(overrides)
    return row


def _field_to_facts() -> dict[str, list[dict[str, object]]]:
    return {
        "revenue": [_sec_fact("Revenues", 100.0)],
        "net_income": [_sec_fact("NetIncomeLoss", 25.0)],
        "cash": [_sec_fact("CashAndCashEquivalentsAtCarryingValue", 80.0, statement_type="balance", start="NULL")],
        "total_debt": [_sec_fact("LongTermDebtNoncurrent", 20.0, statement_type="balance", start="NULL")],
    }


def _sec_fact(
    tag: str,
    value: float,
    *,
    statement_type: str = "income",
    start: str = "2026-01-01",
) -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "statement_type": statement_type,
        "period_end_date": "2026-03-31",
        "field_name": (
            f"{tag}|form=10-Q|unit=USD|fy=2026|fp=Q1|frame=CY2026Q1|start={start}|filed=2026-04-29"
        ),
        "field_value": value,
        "currency": "USD",
        "source": "sec_edgar",
        "retrieved_at_utc": "2026-04-30T00:30:00Z",
        "run_id": "SEC_RAW_RUN1",
    }
