from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_quarterly_dual_write import (
    build_field_provenance_rows,
    build_quarterly_vintage_row_from_latest,
    write_quarterly_latest_and_vintage,
)
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)


def test_build_quarterly_vintage_row_from_latest_requires_pit_metadata() -> None:
    metadata = _metadata()
    metadata["available_at_utc"] = ""

    with pytest.raises(ValueError, match="REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:.*available_at_utc"):
        build_quarterly_vintage_row_from_latest(_latest_row(), metadata)


def test_build_quarterly_vintage_row_from_latest_normalizes_ticker() -> None:
    vintage_row = build_quarterly_vintage_row_from_latest(_latest_row(ticker=" aapl "), _metadata())

    assert vintage_row["ticker"] == "AAPL"
    assert vintage_row["revenue"] == 100.0


def test_write_quarterly_latest_and_vintage_inserts_latest_and_vintage_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_dual_write.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_quarterly_latest_and_vintage(conn, _latest_row(ticker=" aapl "), _metadata())
        latest_row = conn.execute(
            """
            SELECT ticker, period_end_date, revenue, run_id
            FROM rc_fundamental_quarterly
            """
        ).fetchone()
        vintage_row = conn.execute(
            """
            SELECT ticker, market, period_end_date, statement_vintage_id, revenue, run_id
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 1
    assert result["field_provenance_rows_written"] > 0
    assert latest_row == ("AAPL", "2026-03-31", 100.0, "RUN1")
    assert vintage_row == ("AAPL", "usa", "2026-03-31", "AAPL_2026Q1_V1", 100.0, "RUN1")


def test_duplicate_vintage_insert_raises_even_if_latest_table_would_replace(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_dual_write_duplicate.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_quarterly_latest_and_vintage(conn, _latest_row(revenue=100.0), _metadata())
        with pytest.raises(sqlite3.IntegrityError):
            write_quarterly_latest_and_vintage(conn, _latest_row(revenue=200.0), _metadata())

        latest_revenue = conn.execute(
            """
            SELECT revenue
            FROM rc_fundamental_quarterly
            WHERE ticker = 'AAPL' AND period_end_date = '2026-03-31'
            """
        ).fetchone()[0]
        vintage_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarterly_vintage
            WHERE ticker = 'AAPL' AND period_end_date = '2026-03-31'
            """
        ).fetchone()[0]

    assert latest_revenue == 200.0
    assert vintage_count == 1


def test_pit_reader_can_retrieve_inserted_vintage_at_decision_cutoff(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_dual_write_pit.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_quarterly_latest_and_vintage(conn, _latest_row(), _metadata())
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-01T00:00:00Z",
            market="usa",
        )

    assert row is not None
    assert row["statement_vintage_id"] == "AAPL_2026Q1_V1"


def test_future_vintage_is_not_returned_before_available_at(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_dual_write_future.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_quarterly_latest_and_vintage(
            conn,
            _latest_row(),
            _metadata(available_at_utc="2026-06-01T00:00:00Z"),
        )
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-01T00:00:00Z",
            market="usa",
        )

    assert row is None


def test_field_provenance_rows_are_created_for_non_null_fields() -> None:
    rows = build_field_provenance_rows(
        "AAPL_2026Q1_V1",
        build_quarterly_vintage_row_from_latest(
            _latest_row(revenue=100.0, gross_profit=None, cash=80.0),
            _metadata(),
        ),
        "sec_edgar",
        run_id="RUN1",
    )

    fields = {row["field_name"] for row in rows}
    assert "revenue" in fields
    assert "cash" in fields
    assert "gross_profit" not in fields


def test_field_source_map_marks_sec_retained_and_yahoo_filled_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_dual_write_field_sources.db"
    run_migration(db_path)
    field_source_map = {
        "revenue": {
            "source_provider": "sec_edgar",
            "provenance_role": "PRIMARY",
            "merge_action": "RETAINED_PRIMARY",
            "source_hash": "sec_hash",
        },
        "free_cashflow": {
            "source_provider": "yahoo",
            "provenance_role": "FALLBACK_FILL",
            "merge_action": "FILLED_MISSING",
            "source_hash": "yahoo_hash",
        },
    }

    with sqlite3.connect(str(db_path)) as conn:
        write_quarterly_latest_and_vintage(conn, _latest_row(), _metadata(), field_source_map=field_source_map)
        provenance_rows = get_quarterly_field_provenance(conn, "AAPL_2026Q1_V1")

    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "sec_edgar"
    assert by_field["revenue"]["provenance_role"] == "PRIMARY"
    assert by_field["revenue"]["merge_action"] == "RETAINED_PRIMARY"
    assert by_field["free_cashflow"]["source_provider"] == "yahoo"
    assert by_field["free_cashflow"]["provenance_role"] == "FALLBACK_FILL"
    assert by_field["free_cashflow"]["merge_action"] == "FILLED_MISSING"


def test_null_financial_fields_are_not_converted_to_zero(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_dual_write_nulls.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_quarterly_latest_and_vintage(conn, _latest_row(total_debt=None), _metadata())
        latest_value = conn.execute(
            """
            SELECT total_debt
            FROM rc_fundamental_quarterly
            WHERE ticker = 'AAPL' AND period_end_date = '2026-03-31'
            """
        ).fetchone()[0]
        vintage_value = conn.execute(
            """
            SELECT total_debt
            FROM rc_fundamental_quarterly_vintage
            WHERE ticker = 'AAPL' AND period_end_date = '2026-03-31'
            """
        ).fetchone()[0]
        provenance_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarterly_field_provenance
            WHERE field_name = 'total_debt'
            """
        ).fetchone()[0]

    assert latest_value is None
    assert vintage_value is None
    assert provenance_count == 0


def test_dual_write_helper_does_not_import_provider_modules() -> None:
    import sys

    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules
    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules


def _latest_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
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
        "run_id": "LATEST_RUN1",
    }
    row.update(overrides)
    return row


def _metadata(**overrides: object) -> dict[str, object]:
    metadata: dict[str, object] = {
        "market": "usa",
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
        "created_at_utc": "2026-04-30T01:00:01Z",
        "updated_at_utc": None,
    }
    metadata.update(overrides)
    return metadata
