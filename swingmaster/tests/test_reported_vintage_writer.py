from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)


def test_insert_one_valid_vintage_row_normalizes_ticker(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_writer.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        rows_written = insert_quarterly_vintage_row(conn, _vintage_row(ticker=" aapl "))
        row = conn.execute(
            """
            SELECT ticker, market, period_end_date, statement_vintage_id, revenue, source_provider, available_at_utc
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()

    assert rows_written == 1
    assert row == ("AAPL", "usa", "2026-03-31", "AAPL_2026Q1_V1", 100.0, "sec_edgar", "2026-04-30T00:00:00Z")


def test_duplicate_vintage_identity_is_rejected(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_writer_duplicate.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(conn, _vintage_row())
        with pytest.raises(sqlite3.IntegrityError):
            insert_quarterly_vintage_row(conn, _vintage_row())

        row_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()[0]

    assert row_count == 1


def test_multiple_vintages_for_same_ticker_and_period_are_allowed(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_writer_multiple.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(conn, _vintage_row(statement_vintage_id="AAPL_2026Q1_V1", revision_number=1))
        insert_quarterly_vintage_row(conn, _vintage_row(statement_vintage_id="AAPL_2026Q1_V2", revision_number=2))
        rows = conn.execute(
            """
            SELECT statement_vintage_id, revision_number
            FROM rc_fundamental_quarterly_vintage
            WHERE ticker = 'AAPL' AND period_end_date = '2026-03-31'
            ORDER BY statement_vintage_id
            """
        ).fetchall()

    assert rows == [("AAPL_2026Q1_V1", 1), ("AAPL_2026Q1_V2", 2)]


def test_insert_sec_primary_field_provenance_row(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_writer_sec_provenance.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        rows_written = insert_quarterly_field_provenance_rows(
            conn,
            [
                _field_provenance_row(
                    source_provider="sec_edgar",
                    provenance_role="PRIMARY",
                    merge_action="RETAINED_PRIMARY",
                    source_hash="sec_hash",
                )
            ],
        )
        row = conn.execute(
            """
            SELECT ticker, field_name, source_provider, provenance_role, merge_action, field_value
            FROM rc_fundamental_quarterly_field_provenance
            """
        ).fetchone()

    assert rows_written == 1
    assert row == ("AAPL", "revenue", "sec_edgar", "PRIMARY", "RETAINED_PRIMARY", 100.0)


def test_insert_yahoo_fallback_provenance_for_same_field_and_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_writer_yahoo_provenance.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        rows_written = insert_quarterly_field_provenance_rows(
            conn,
            [
                _field_provenance_row(
                    source_provider="sec_edgar",
                    provenance_role="PRIMARY",
                    merge_action="RETAINED_PRIMARY",
                    source_hash="sec_hash",
                ),
                _field_provenance_row(
                    source_provider="yahoo",
                    provenance_role="FALLBACK_FILL",
                    merge_action="FILLED_MISSING",
                    source_hash="yahoo_hash",
                ),
            ],
        )
        rows = conn.execute(
            """
            SELECT source_provider, provenance_role, merge_action
            FROM rc_fundamental_quarterly_field_provenance
            WHERE ticker = 'AAPL'
              AND period_end_date = '2026-03-31'
              AND statement_vintage_id = 'AAPL_2026Q1_V1'
              AND field_name = 'revenue'
            ORDER BY source_provider
            """
        ).fetchall()

    assert rows_written == 2
    assert rows == [
        ("sec_edgar", "PRIMARY", "RETAINED_PRIMARY"),
        ("yahoo", "FALLBACK_FILL", "FILLED_MISSING"),
    ]


@pytest.mark.parametrize("missing_field", ["ticker", "statement_vintage_id", "available_at_utc"])
def test_validation_rejects_missing_required_vintage_fields(tmp_path: Path, missing_field: str) -> None:
    db_path = tmp_path / f"reported_vintage_writer_missing_{missing_field}.db"
    run_migration(db_path)
    row = _vintage_row()
    row[missing_field] = ""

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match=f"REPORTED_VINTAGE_REQUIRED_FIELDS_MISSING:.*{missing_field}"):
            insert_quarterly_vintage_row(conn, row)


@pytest.mark.parametrize("missing_field", ["field_name", "provenance_role", "merge_action"])
def test_validation_rejects_missing_required_provenance_fields(tmp_path: Path, missing_field: str) -> None:
    db_path = tmp_path / f"reported_vintage_writer_missing_prov_{missing_field}.db"
    run_migration(db_path)
    row = _field_provenance_row()
    row[missing_field] = ""

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match=f"REPORTED_VINTAGE_REQUIRED_FIELDS_MISSING:.*{missing_field}"):
            insert_quarterly_field_provenance_rows(conn, [row])


def test_existing_quarterly_table_remains_untouched(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_writer_quarterly_untouched.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(conn, _vintage_row())
        insert_quarterly_field_provenance_rows(conn, [_field_provenance_row()])
        row_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarterly
            """
        ).fetchone()[0]

    assert row_count == 0


def _vintage_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "market": "usa",
        "period_end_date": "2026-03-31",
        "statement_vintage_id": "AAPL_2026Q1_V1",
        "source_provider": "sec_edgar",
        "source_document_id": "0000320193-26-000001",
        "source_hash": "sec_hash",
        "revision_number": 1,
        "is_restated": 0,
        "supersedes_vintage_id": None,
        "availability_quality": "FILED",
        "filed_at_utc": "2026-04-29T21:00:00Z",
        "available_at_utc": "2026-04-30T00:00:00Z",
        "ingested_at_utc": "2026-04-30T01:00:00Z",
        "provider_observed_at_utc": "2026-04-30T00:30:00Z",
        "run_id": "RUN1",
        "provider_run_id": "PROVIDER_RUN1",
        "normalization_run_id": "NORM_RUN1",
        "enrichment_run_id": None,
        "revenue": 100.0,
        "gross_profit": 45.0,
        "operating_income": 30.0,
        "ebit": 29.0,
        "ebitda": 34.0,
        "net_income": 25.0,
        "operating_cashflow": 35.0,
        "capex": -5.0,
        "free_cashflow": 30.0,
        "cash": 80.0,
        "total_debt": 20.0,
        "shares_outstanding": 1000.0,
        "currency": "USD",
        "created_at_utc": "2026-04-30T01:00:01Z",
        "updated_at_utc": None,
    }
    row.update(overrides)
    return row


def _field_provenance_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "market": "usa",
        "period_end_date": "2026-03-31",
        "statement_vintage_id": "AAPL_2026Q1_V1",
        "field_name": "revenue",
        "field_value": 100.0,
        "source_provider": "sec_edgar",
        "source_table": "rc_fundamental_statement_raw",
        "source_row_ref": "fixture-row-1",
        "source_document_id": "0000320193-26-000001",
        "source_hash": "sec_hash",
        "provenance_role": "PRIMARY",
        "merge_action": "RETAINED_PRIMARY",
        "old_value": None,
        "new_value": 100.0,
        "available_at_utc": "2026-04-30T00:00:00Z",
        "created_at_utc": "2026-04-30T01:00:01Z",
        "run_id": "RUN1",
        "enrichment_run_id": None,
    }
    row.update(overrides)
    return row
