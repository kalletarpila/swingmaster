from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_reader import (
    ReportedVintageSchemaError,
    get_latest_period_for_ticker,
    get_latest_quarterly_vintage,
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
    list_quarterly_vintages,
)
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)


def test_list_vintages_for_ticker_period_ordered_deterministically(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_list.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_standard_vintages(conn)
        rows = list_quarterly_vintages(conn, " aapl ", "2026-03-31", market="usa")

    assert [row["statement_vintage_id"] for row in rows] == [
        "AAPL_2026Q1_V1",
        "AAPL_2026Q1_V2",
        "AAPL_2026Q1_V3",
    ]


def test_latest_vintage_returns_newest_row(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_latest.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_standard_vintages(conn)
        row = get_latest_quarterly_vintage(conn, "AAPL", "2026-03-31", market="usa")

    assert row is not None
    assert row["statement_vintage_id"] == "AAPL_2026Q1_V3"
    assert row["revenue"] == 130.0


def test_pit_vintage_returns_older_row_when_newer_row_is_after_cutoff(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_pit.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_standard_vintages(conn)
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-10T12:00:00Z",
            market="usa",
        )

    assert row is not None
    assert row["statement_vintage_id"] == "AAPL_2026Q1_V2"
    assert row["available_at_utc"] == "2026-05-05T00:00:00Z"


def test_pit_vintage_returns_none_when_all_rows_are_after_cutoff(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_pit_none.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _insert_standard_vintages(conn)
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-04-01T00:00:00Z",
            market="usa",
        )

    assert row is None


def test_pit_vintage_does_not_fallback_to_latest(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_pit_no_fallback.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(
            conn,
            _vintage_row(
                statement_vintage_id="AAPL_2026Q1_FUTURE",
                available_at_utc="2026-06-01T00:00:00Z",
                revision_number=1,
            ),
        )
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-01T00:00:00Z",
            market="usa",
        )

    assert row is None


def test_ticker_normalization_matches_writer_behavior(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_ticker_normalization.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(conn, _vintage_row(ticker=" aapl "))
        row = get_latest_quarterly_vintage(conn, " aapl ", "2026-03-31", market="usa")

    assert row is not None
    assert row["ticker"] == "AAPL"


def test_market_filter_separates_same_ticker_across_markets(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_market.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(
            conn,
            _vintage_row(
                ticker="NOK",
                market="usa",
                statement_vintage_id="NOK_USA_V1",
                revenue=100.0,
            ),
        )
        insert_quarterly_vintage_row(
            conn,
            _vintage_row(
                ticker="NOK",
                market="omxh",
                statement_vintage_id="NOK_OMXH_V1",
                revenue=200.0,
            ),
        )
        row = get_latest_quarterly_vintage(conn, "nok", "2026-03-31", market="omxh")

    assert row is not None
    assert row["statement_vintage_id"] == "NOK_OMXH_V1"
    assert row["market"] == "omxh"
    assert row["revenue"] == 200.0


def test_field_provenance_rows_are_returned_for_selected_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_provenance.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_field_provenance_rows(
            conn,
            [
                _field_provenance_row(
                    statement_vintage_id="AAPL_2026Q1_V1",
                    source_provider="sec_edgar",
                    provenance_role="PRIMARY",
                    merge_action="RETAINED_PRIMARY",
                ),
                _field_provenance_row(
                    statement_vintage_id="AAPL_2026Q1_V1",
                    source_provider="yahoo",
                    provenance_role="FALLBACK_FILL",
                    merge_action="FILLED_MISSING",
                ),
            ],
        )
        rows = get_quarterly_field_provenance(conn, "AAPL_2026Q1_V1")

    assert [(row["source_provider"], row["provenance_role"], row["merge_action"]) for row in rows] == [
        ("sec_edgar", "PRIMARY", "RETAINED_PRIMARY"),
        ("yahoo", "FALLBACK_FILL", "FILLED_MISSING"),
    ]


def test_missing_vintage_table_gives_clear_error() -> None:
    with sqlite3.connect(":memory:") as conn:
        with pytest.raises(ReportedVintageSchemaError, match="REPORTED_VINTAGE_SCHEMA_MISSING"):
            list_quarterly_vintages(conn, "AAPL", "2026-03-31")


@pytest.mark.parametrize("decision_cutoff_utc", ["", "   ", None])
def test_invalid_decision_cutoff_raises_value_error(tmp_path: Path, decision_cutoff_utc: str | None) -> None:
    db_path = tmp_path / "reported_vintage_reader_invalid_cutoff.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_VINTAGE_REQUIRED_FIELD_MISSING:decision_cutoff_utc"):
            get_pit_quarterly_vintage(
                conn,
                "AAPL",
                "2026-03-31",
                decision_cutoff_utc,  # type: ignore[arg-type]
                market="usa",
            )


def test_latest_period_for_ticker_respects_decision_cutoff(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_reader_latest_period.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(
            conn,
            _vintage_row(
                period_end_date="2026-03-31",
                statement_vintage_id="AAPL_2026Q1_V1",
                available_at_utc="2026-04-30T00:00:00Z",
            ),
        )
        insert_quarterly_vintage_row(
            conn,
            _vintage_row(
                period_end_date="2026-06-30",
                statement_vintage_id="AAPL_2026Q2_V1",
                available_at_utc="2026-08-01T00:00:00Z",
            ),
        )
        latest_period = get_latest_period_for_ticker(
            conn,
            "AAPL",
            market="usa",
            decision_cutoff_utc="2026-07-01T00:00:00Z",
        )

    assert latest_period == "2026-03-31"


def _insert_standard_vintages(conn: sqlite3.Connection) -> None:
    insert_quarterly_vintage_row(
        conn,
        _vintage_row(
            statement_vintage_id="AAPL_2026Q1_V3",
            available_at_utc="2026-05-20T00:00:00Z",
            revision_number=3,
            revenue=130.0,
        ),
    )
    insert_quarterly_vintage_row(
        conn,
        _vintage_row(
            statement_vintage_id="AAPL_2026Q1_V1",
            available_at_utc="2026-04-30T00:00:00Z",
            revision_number=1,
            revenue=100.0,
        ),
    )
    insert_quarterly_vintage_row(
        conn,
        _vintage_row(
            statement_vintage_id="AAPL_2026Q1_V2",
            available_at_utc="2026-05-05T00:00:00Z",
            revision_number=2,
            revenue=120.0,
        ),
    )


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
