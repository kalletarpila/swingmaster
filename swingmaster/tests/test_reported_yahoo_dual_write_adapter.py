from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_sec_vintage_metadata import build_sec_field_source_map
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)
from swingmaster.fundamentals.reported_yahoo_dual_write_adapter import (
    write_yahoo_fallback_enriched_rows_with_optional_vintage,
    write_yahoo_quarterly_rows_with_optional_vintage,
)


def test_default_write_vintage_false_writes_only_latest_row(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_latest_only.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_yahoo_quarterly_rows_with_optional_vintage(conn, normalized_rows=[_normalized_row()])
        latest_row = conn.execute("SELECT ticker, period_end_date, revenue, run_id FROM rc_fundamental_quarterly").fetchone()
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert result == {
        "latest_rows_written": 1,
        "vintage_rows_written": 0,
        "field_provenance_rows_written": 0,
    }
    assert latest_row == ("AAPL", "2026-03-31", 100.0, "YAHOO_LATEST_RUN1")
    assert vintage_count == 0
    assert provenance_count == 0


def test_yahoo_bridge_write_vintage_true_writes_latest_vintage_and_yahoo_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_bridge.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = _write_yahoo_bridge(conn)
        latest_row = conn.execute("SELECT ticker, revenue, run_id FROM rc_fundamental_quarterly").fetchone()
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, source_provider, revenue
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[0]))

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 1
    assert latest_row == ("AAPL", 100.0, "YAHOO_BRIDGE_RUN1")
    assert vintage_row[0].startswith("yahoo:yahoo_to_generic_bridge:usa:AAPL:2026-03-31:")
    assert vintage_row[1] == "yahoo"
    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "yahoo"
    assert by_field["revenue"]["provenance_role"] == "PROVIDER_REPORTED"
    assert by_field["revenue"]["merge_action"] == "YAHOO_BRIDGED"


@pytest.mark.parametrize("missing_field", ["available_at_utc", "ingested_at_utc", "run_id"])
def test_yahoo_bridge_requires_vintage_inputs(tmp_path: Path, missing_field: str) -> None:
    db_path = tmp_path / f"yahoo_dual_write_bridge_missing_{missing_field}.db"
    run_migration(db_path)
    kwargs = {
        "available_at_utc": "2026-05-03T10:23:06+00:00",
        "ingested_at_utc": "2026-05-03T10:30:00+00:00",
        "run_id": "YAHOO_BRIDGE_RUN1",
    }
    kwargs[missing_field] = None

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match=f"REPORTED_YAHOO_DUAL_WRITE_REQUIRED_FIELD_MISSING:{missing_field}"):
            write_yahoo_quarterly_rows_with_optional_vintage(
                conn,
                normalized_rows=[_normalized_row()],
                yahoo_quarterly_rows_by_key={_key(): _yahoo_quarterly_row()},
                payload_hash_by_key={_key(): "payload_hash_1"},
                write_vintage=True,
                **kwargs,
            )


def test_yahoo_bridge_statement_vintage_id_follows_mode_format(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_bridge_vintage_id.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _write_yahoo_bridge(conn)
        statement_vintage_id = conn.execute("SELECT statement_vintage_id FROM rc_fundamental_quarterly_vintage").fetchone()[0]

    assert statement_vintage_id.startswith("yahoo:yahoo_to_generic_bridge:usa:AAPL:2026-03-31:")
    assert len(statement_vintage_id.rsplit(":", 1)[1]) == 16


def test_yahoo_bridge_pit_reader_returns_row_at_available_at_and_none_before(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_bridge_pit.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _write_yahoo_bridge(conn)
        before = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:23:05+00:00", market="usa")
        after = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:23:06+00:00", market="usa")

    assert before is None
    assert after is not None
    assert after["source_provider"] == "yahoo"


def test_fallback_enrichment_marks_only_yahoo_filled_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_fallback.db"
    run_migration(db_path)
    row = _normalized_row(free_cashflow=30.0, run_id="YAHOO_FALLBACK_RUN1")

    with sqlite3.connect(str(db_path)) as conn:
        write_yahoo_fallback_enriched_rows_with_optional_vintage(
            conn,
            normalized_rows=[row],
            enrichment_audit_rows_by_key={_key(): [_audit_row("free_cashflow", 30.0), _audit_row("total_debt", 20.0)]},
            yahoo_quarterly_rows_by_key={_key(): _yahoo_quarterly_row()},
            payload_hash_by_key={_key(): "payload_hash_1"},
            write_vintage=True,
            available_at_utc="2026-05-03T10:30:00+00:00",
            ingested_at_utc="2026-05-03T10:31:00+00:00",
            run_id="YAHOO_FALLBACK_RUN1",
        )
        statement_vintage_id = conn.execute("SELECT statement_vintage_id FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_rows = get_quarterly_field_provenance(conn, str(statement_vintage_id))

    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "unknown"
    assert by_field["revenue"]["merge_action"] == "SOURCE_NOT_PROVIDED"
    assert by_field["free_cashflow"]["source_provider"] == "yahoo"
    assert by_field["free_cashflow"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert by_field["total_debt"]["source_provider"] == "yahoo"
    assert by_field["total_debt"]["provenance_role"] == "FALLBACK_REPORTED"


def test_fallback_enrichment_does_not_mark_sec_retained_fields_as_yahoo(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_fallback_no_sec_as_yahoo.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        write_yahoo_fallback_enriched_rows_with_optional_vintage(
            conn,
            normalized_rows=[_normalized_row(free_cashflow=30.0)],
            enrichment_audit_rows_by_key={_key(): [_audit_row("free_cashflow", 30.0)]},
            write_vintage=True,
            available_at_utc="2026-05-03T10:30:00+00:00",
            ingested_at_utc="2026-05-03T10:31:00+00:00",
            run_id="YAHOO_FALLBACK_RUN1",
        )
        statement_vintage_id = conn.execute("SELECT statement_vintage_id FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_rows = get_quarterly_field_provenance(conn, str(statement_vintage_id))

    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "unknown"
    assert by_field["revenue"]["merge_action"] == "SOURCE_NOT_PROVIDED"
    assert by_field["free_cashflow"]["source_provider"] == "yahoo"


def test_mixed_sec_yahoo_explicit_field_source_map_preserves_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_mixed.db"
    run_migration(db_path)
    row = _normalized_row(free_cashflow=30.0, run_id="MIXED_RUN1")
    sec_source_map = build_sec_field_source_map(
        normalized_row=row,
        field_to_contributing_facts={
            "revenue": [_sec_fact("Revenues", 100.0)],
            "cash": [_sec_fact("CashAndCashEquivalentsAtCarryingValue", 80.0, statement_type="balance", start="NULL")],
        },
    )

    with sqlite3.connect(str(db_path)) as conn:
        write_yahoo_fallback_enriched_rows_with_optional_vintage(
            conn,
            normalized_rows=[row],
            enrichment_audit_rows_by_key={_key(): [_audit_row("free_cashflow", 30.0)]},
            field_source_map_by_key={_key(): sec_source_map},
            write_vintage=True,
            available_at_utc="2026-05-03T10:30:00+00:00",
            ingested_at_utc="2026-05-03T10:31:00+00:00",
            run_id="MIXED_RUN1",
        )
        statement_vintage_id = conn.execute("SELECT statement_vintage_id FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_rows = get_quarterly_field_provenance(conn, str(statement_vintage_id))

    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "sec_edgar"
    assert by_field["revenue"]["merge_action"] == "SEC_RETAINED"
    assert by_field["cash"]["source_provider"] == "sec_edgar"
    assert by_field["free_cashflow"]["source_provider"] == "yahoo"
    assert by_field["free_cashflow"]["merge_action"] == "YAHOO_FILLED_MISSING"


def test_conflicting_field_provenance_raises_value_error(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_conflict.db"
    run_migration(db_path)
    conflicting_map = {
        "free_cashflow": {
            "source_provider": "sec_edgar",
            "provenance_role": "PRIMARY_REPORTED",
            "merge_action": "SEC_RETAINED",
        }
    }

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_YAHOO_DUAL_WRITE_FIELD_SOURCE_CONFLICT:free_cashflow"):
            write_yahoo_fallback_enriched_rows_with_optional_vintage(
                conn,
                normalized_rows=[_normalized_row(free_cashflow=30.0)],
                enrichment_audit_rows_by_key={_key(): [_audit_row("free_cashflow", 30.0)]},
                field_source_map_by_key={_key(): conflicting_map},
                write_vintage=True,
                available_at_utc="2026-05-03T10:30:00+00:00",
                ingested_at_utc="2026-05-03T10:31:00+00:00",
                run_id="YAHOO_FALLBACK_RUN1",
            )


def test_duplicate_vintage_insert_raises_integrity_error(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_dual_write_duplicate.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _write_yahoo_bridge(conn)
        with pytest.raises(sqlite3.IntegrityError):
            _write_yahoo_bridge(conn)


def test_scaffold_does_not_import_provider_modules() -> None:
    import sys

    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules
    assert "yfinance" not in sys.modules
    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules


def _write_yahoo_bridge(conn: sqlite3.Connection) -> dict[str, int]:
    return write_yahoo_quarterly_rows_with_optional_vintage(
        conn,
        normalized_rows=[_normalized_row(run_id="YAHOO_BRIDGE_RUN1")],
        yahoo_quarterly_rows_by_key={_key(): _yahoo_quarterly_row()},
        payload_hash_by_key={_key(): "payload_hash_1"},
        write_vintage=True,
        available_at_utc="2026-05-03T10:23:06+00:00",
        ingested_at_utc="2026-05-03T10:30:00+00:00",
        run_id="YAHOO_BRIDGE_RUN1",
    )


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
        "run_id": "YAHOO_LATEST_RUN1",
    }
    row.update(overrides)
    return row


def _yahoo_quarterly_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "market": "usa",
        "symbol": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "free_cashflow": 30.0,
        "cash": 80.0,
        "total_debt": 20.0,
        "shares_source": "ordinary_shares_number",
        "shares_quality": "OK",
        "source_run_id": "YRAW1",
        "run_id": "YQTR1",
        "created_at_utc": "2026-05-03T10:23:06+00:00",
    }
    row.update(overrides)
    return row


def _audit_row(field_name: str, new_value: float) -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "field_name": field_name,
        "old_value": None,
        "new_value": new_value,
        "primary_source": "sec_edgar",
        "fallback_source": "yahoo",
        "enrichment_status": "FILLED_FROM_YAHOO",
        "matched_yahoo_period_end_date": "2026-03-31",
        "match_method": "EXACT",
        "run_id": "ENRICH_RUN1",
        "created_at_utc": "2026-05-03T10:30:00+00:00",
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
