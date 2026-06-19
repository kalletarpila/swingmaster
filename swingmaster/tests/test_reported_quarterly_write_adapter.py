from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_quarterly_dual_write import (
    write_normalized_quarterly_rows_with_optional_vintage,
)


def test_default_latest_only_mode_writes_latest_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_latest.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(conn, [_latest_row()])
        latest_row = conn.execute(
            """
            SELECT ticker, period_end_date, revenue, run_id
            FROM rc_fundamental_quarterly
            """
        ).fetchone()

    assert result == {
        "latest_rows_written": 1,
        "vintage_rows_written": 0,
        "field_provenance_rows_written": 0,
    }
    assert latest_row == ("AAPL", "2026-03-31", 100.0, "LATEST_RUN1")


def test_default_latest_only_mode_does_not_write_vintage_or_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_latest_only.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(conn, [_latest_row()])
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert vintage_count == 0
    assert provenance_count == 0


def test_write_vintage_mode_writes_latest_vintage_and_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_dual.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row()],
            write_vintage=True,
            vintage_metadata_by_key={_key(): _metadata()},
        )
        latest_row = conn.execute("SELECT ticker, revenue, run_id FROM rc_fundamental_quarterly").fetchone()
        vintage_row = conn.execute(
            """
            SELECT ticker, market, statement_vintage_id, revenue, run_id
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 1
    assert result["field_provenance_rows_written"] == provenance_count
    assert provenance_count > 0
    assert latest_row == ("AAPL", 100.0, "RUN1")
    assert vintage_row == ("AAPL", "usa", "AAPL_2026Q1_V1", 100.0, "RUN1")


def test_write_vintage_requires_metadata_for_every_row(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_missing_row_metadata.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_QUARTERLY_WRITE_ADAPTER_METADATA_MISSING:AAPL,2026-06-30"):
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [_latest_row(), _latest_row(period_end_date="2026-06-30")],
                write_vintage=True,
                vintage_metadata_by_key={_key(): _metadata()},
            )


def test_write_vintage_requires_available_at_utc(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_missing_available.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:.*available_at_utc"):
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [_latest_row()],
                write_vintage=True,
                vintage_metadata_by_key={_key(): _metadata(available_at_utc="")},
            )


def test_write_vintage_requires_statement_vintage_id(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_missing_vintage_id.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:.*statement_vintage_id"):
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [_latest_row()],
                write_vintage=True,
                vintage_metadata_by_key={_key(): _metadata(statement_vintage_id=None)},
            )


def test_duplicate_vintage_insert_raises_integrity_error(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_duplicate.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row()],
            write_vintage=True,
            vintage_metadata_by_key={_key(): _metadata()},
        )
        with pytest.raises(sqlite3.IntegrityError):
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [_latest_row(revenue=200.0)],
                write_vintage=True,
                vintage_metadata_by_key={_key(): _metadata()},
            )


def test_latest_table_preserves_replace_behavior(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_latest_replace.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(conn, [_latest_row(revenue=100.0)])
        write_normalized_quarterly_rows_with_optional_vintage(conn, [_latest_row(revenue=200.0, run_id="LATEST_RUN2")])
        row = conn.execute("SELECT COUNT(*), revenue, run_id FROM rc_fundamental_quarterly").fetchone()

    assert row == (1, 200.0, "LATEST_RUN2")


def test_vintage_table_preserves_history_and_does_not_replace(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_vintage_history.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row(revenue=100.0)],
            write_vintage=True,
            vintage_metadata_by_key={_key(): _metadata(statement_vintage_id="AAPL_2026Q1_V1", source_hash="hash_v1")},
        )
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row(revenue=200.0)],
            write_vintage=True,
            vintage_metadata_by_key={_key(): _metadata(statement_vintage_id="AAPL_2026Q1_V2", source_hash="hash_v2")},
        )
        latest_revenue = conn.execute("SELECT revenue FROM rc_fundamental_quarterly").fetchone()[0]
        vintage_rows = conn.execute(
            """
            SELECT statement_vintage_id, revenue
            FROM rc_fundamental_quarterly_vintage
            ORDER BY statement_vintage_id
            """
        ).fetchall()

    assert latest_revenue == 200.0
    assert vintage_rows == [("AAPL_2026Q1_V1", 100.0), ("AAPL_2026Q1_V2", 200.0)]


def test_field_provenance_is_generated_for_non_null_financial_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_provenance_non_null.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row(revenue=100.0, gross_profit=None, cash=80.0)],
            write_vintage=True,
            vintage_metadata_by_key={_key(): _metadata()},
        )
        fields = {
            row[0]
            for row in conn.execute(
                """
                SELECT field_name
                FROM rc_fundamental_quarterly_field_provenance
                WHERE statement_vintage_id = 'AAPL_2026Q1_V1'
                """
            ).fetchall()
        }

    assert "revenue" in fields
    assert "cash" in fields
    assert "gross_profit" not in fields


def test_field_source_map_can_mark_mixed_sec_yahoo_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_mixed_provenance.db"
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
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row()],
            write_vintage=True,
            vintage_metadata_by_key={_key(): _metadata()},
            field_source_map_by_key={_key(): field_source_map},
        )
        rows = conn.execute(
            """
            SELECT field_name, source_provider, provenance_role, merge_action
            FROM rc_fundamental_quarterly_field_provenance
            WHERE field_name IN ('revenue', 'free_cashflow')
            ORDER BY field_name
            """
        ).fetchall()

    assert rows == [
        ("free_cashflow", "yahoo", "FALLBACK_FILL", "FILLED_MISSING"),
        ("revenue", "sec_edgar", "PRIMARY", "RETAINED_PRIMARY"),
    ]


def test_wrapper_handles_multiple_rows_deterministically(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_multiple_rows.db"
    run_migration(db_path)
    first = _latest_row(period_end_date="2026-03-31")
    second = _latest_row(period_end_date="2026-06-30", revenue=120.0)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [second, first],
            write_vintage=True,
            vintage_metadata_by_key={
                _key(period_end_date="2026-03-31"): _metadata(statement_vintage_id="AAPL_2026Q1_V1"),
                _key(period_end_date="2026-06-30"): _metadata(
                    statement_vintage_id="AAPL_2026Q2_V1",
                    source_hash="sec_hash_q2",
                ),
            },
        )
        vintages = conn.execute(
            """
            SELECT period_end_date, statement_vintage_id
            FROM rc_fundamental_quarterly_vintage
            ORDER BY period_end_date
            """
        ).fetchall()

    assert result["latest_rows_written"] == 2
    assert result["vintage_rows_written"] == 2
    assert vintages == [("2026-03-31", "AAPL_2026Q1_V1"), ("2026-06-30", "AAPL_2026Q2_V1")]


def test_market_key_is_supported_when_row_provides_market(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_market_key.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row(ticker="nokia.he", market="OMXH")],
            write_vintage=True,
            vintage_metadata_by_key={
                ("omxh", "NOKIA.HE", "2026-03-31"): _metadata(
                    market="omxh",
                    statement_vintage_id="NOKIA_HE_2026Q1_V1",
                )
            },
        )
        vintage_row = conn.execute(
            """
            SELECT ticker, market, statement_vintage_id
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()

    assert result["vintage_rows_written"] == 1
    assert vintage_row == ("NOKIA.HE", "omxh", "NOKIA_HE_2026Q1_V1")


def test_market_key_is_supported_when_only_metadata_provides_market(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_metadata_market_key.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row()],
            write_vintage=True,
            vintage_metadata_by_key={("usa", "AAPL", "2026-03-31"): _metadata()},
        )

    assert result["vintage_rows_written"] == 1


def test_ambiguous_market_metadata_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_quarterly_adapter_ambiguous_market_key.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_QUARTERLY_WRITE_ADAPTER_METADATA_AMBIGUOUS:AAPL,2026-03-31"):
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [_latest_row()],
                write_vintage=True,
                vintage_metadata_by_key={
                    ("usa", "AAPL", "2026-03-31"): _metadata(),
                    ("omxh", "AAPL", "2026-03-31"): _metadata(
                        market="omxh",
                        statement_vintage_id="AAPL_OMXH_2026Q1_V1",
                    ),
                },
            )


def test_adapter_does_not_import_provider_modules() -> None:
    import sys

    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules
    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules


def _key(ticker: str = "AAPL", period_end_date: str = "2026-03-31") -> tuple[str, str]:
    return ticker.upper(), period_end_date


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
