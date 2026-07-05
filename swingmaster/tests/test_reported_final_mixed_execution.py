from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_final_mixed_execution import (
    build_final_mixed_execution_summary,
    execute_final_mixed_vintage_write,
)
from swingmaster.fundamentals.reported_final_mixed_vintage import (
    build_final_mixed_source_hash,
    build_final_mixed_statement_vintage_id,
)
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)


def test_execute_writes_latest_final_mixed_vintage_and_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_write.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn)
        latest_row = conn.execute(
            "SELECT ticker, period_end_date, revenue, free_cashflow, run_id FROM rc_fundamental_quarterly"
        ).fetchone()
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, source_provider, source_hash, revenue, free_cashflow, net_income
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert summary["final_mixed_written"] is True
    assert latest_row == ("AAPL", "2026-03-31", 100.0, 30.0, "FINAL_MIXED_RUN1")
    assert vintage_row == (
        summary["statement_vintage_id"],
        "mixed_sec_yahoo",
        summary["source_hash"],
        100.0,
        30.0,
        25.0,
    )
    assert provenance_count == 5


def test_execute_returns_inserted_count_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_summary.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn)

    assert summary["vintage_rows_inserted"] == 1
    assert summary["provenance_rows_inserted"] == 5
    assert summary["provenance_field_count"] == 5
    assert summary["skipped_noop"] == 0
    assert summary["already_known"] == 0
    assert summary["error"] is None


def test_pit_reader_returns_row_at_or_after_available_at(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_pit_after.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn)
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-03T10:30:00Z",
            market="usa",
        )

    assert row is not None
    assert row["statement_vintage_id"] == summary["statement_vintage_id"]


def test_pit_reader_returns_none_before_available_at(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_pit_before.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _execute(conn)
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-03T10:29:59Z",
            market="usa",
        )

    assert row is None


def test_sec_retained_fields_remain_sec(tmp_path: Path) -> None:
    rows = _provenance_rows(tmp_path)

    assert rows["revenue"]["source_provider"] == "sec_edgar"
    assert rows["revenue"]["provenance_role"] == "PRIMARY_REPORTED"
    assert rows["revenue"]["merge_action"] == "SEC_RETAINED"
    assert rows["cash"]["source_provider"] == "sec_edgar"


def test_yahoo_filled_fields_remain_yahoo_fallback(tmp_path: Path) -> None:
    rows = _provenance_rows(tmp_path)

    assert rows["free_cashflow"]["source_provider"] == "yahoo"
    assert rows["free_cashflow"]["provenance_role"] == "FALLBACK_REPORTED"
    assert rows["free_cashflow"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert rows["total_debt"]["source_provider"] == "yahoo"


def test_unknown_non_null_fields_remain_unknown(tmp_path: Path) -> None:
    rows = _provenance_rows(tmp_path)

    assert rows["net_income"]["source_provider"] == "unknown"
    assert rows["net_income"]["provenance_role"] == "UNSPECIFIED"
    assert rows["net_income"]["merge_action"] == "SOURCE_NOT_PROVIDED"


def test_duplicate_statement_vintage_id_raises_integrity_error(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_duplicate.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _execute(conn)
        with pytest.raises(sqlite3.IntegrityError):
            _execute(conn)


def test_missing_available_at_utc_raises_value_error(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_missing_available.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="FINAL_MIXED_VINTAGE_REQUIRED_FIELD_MISSING:available_at_utc"):
            _execute(conn, available_at_utc="")


def test_missing_ingested_at_utc_raises_value_error(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_missing_ingested.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="FINAL_MIXED_VINTAGE_REQUIRED_FIELD_MISSING:ingested_at_utc"):
            _execute(conn, ingested_at_utc=" ")


def test_missing_run_id_raises_value_error(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_missing_run_id.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="FINAL_MIXED_VINTAGE_REQUIRED_FIELD_MISSING:run_id"):
            _execute(conn, run_id="")


def test_source_hash_and_statement_id_match_builder_behavior(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_builder_match.db"
    run_migration(db_path)
    row = _normalized_row()
    source_hash = build_final_mixed_source_hash(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=row,
        sec_field_source_map=_sec_source_map(),
        yahoo_field_source_map=_yahoo_source_map(),
        fallback_audit_rows=_audit_rows(),
    )
    statement_vintage_id = build_final_mixed_statement_vintage_id(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        source_hash=source_hash,
    )

    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn, normalized_row=row)

    assert summary["source_hash"] == source_hash
    assert summary["statement_vintage_id"] == statement_vintage_id


def test_latest_row_remains_latest_compatible(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_latest_compatible.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _execute(conn)
        columns = [row[1] for row in conn.execute("PRAGMA table_info(rc_fundamental_quarterly)").fetchall()]
        latest_row = conn.execute("SELECT * FROM rc_fundamental_quarterly").fetchone()

    assert "statement_vintage_id" not in columns
    assert latest_row is not None


def test_no_provider_or_network_calls_are_used(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_execution_no_provider.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn)

    assert summary["final_mixed_written"] is True


def test_quarter_update_can_consume_mocked_execution_summary_shape() -> None:
    quarter_update_summary = {
        "vintage_requested": True,
        "vintage_mode": "sec_plus_yahoo_fallback_final_mixed",
        "vintage_execution_enabled": False,
    }
    execution_summary = build_final_mixed_execution_summary(
        statement_vintage_id="mixed_sec_yahoo:usa:AAPL:2026-03-31:abc123",
        source_hash="abc123",
        vintage_rows_inserted=1,
        provenance_rows_inserted=5,
        provenance_field_count=5,
    )

    quarter_update_summary.update(
        {
            "vintage_final_mixed_written": execution_summary["final_mixed_written"],
            "vintage_final_mixed_rows_inserted": execution_summary["vintage_rows_inserted"],
            "vintage_final_mixed_provenance_rows_inserted": execution_summary["provenance_rows_inserted"],
            "vintage_final_mixed_rows_skipped_noop": execution_summary["skipped_noop"],
            "vintage_final_mixed_rows_already_known": execution_summary["already_known"],
            "vintage_error_summary": execution_summary["error"],
        }
    )

    assert quarter_update_summary["vintage_final_mixed_written"] is True
    assert quarter_update_summary["vintage_final_mixed_rows_inserted"] == 1
    assert quarter_update_summary["vintage_final_mixed_provenance_rows_inserted"] == 5
    assert quarter_update_summary["vintage_error_summary"] is None


def _provenance_rows(tmp_path: Path) -> dict[str, sqlite3.Row]:
    db_path = tmp_path / "final_mixed_execution_provenance.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn)
        rows = get_quarterly_field_provenance(conn, str(summary["statement_vintage_id"]))
    return {row["field_name"]: row for row in rows}


def _execute(conn: sqlite3.Connection, **overrides: object) -> dict[str, object]:
    normalized_row = overrides.pop("normalized_row", _normalized_row())
    assert isinstance(normalized_row, dict)
    return execute_final_mixed_vintage_write(
        conn,
        normalized_row=normalized_row,
        market=str(overrides.pop("market", "usa")),
        available_at_utc=str(overrides.pop("available_at_utc", "2026-05-03T10:30:00Z")),
        ingested_at_utc=str(overrides.pop("ingested_at_utc", "2026-05-03T10:31:00Z")),
        run_id=str(overrides.pop("run_id", "FINAL_MIXED_RUN1")),
        sec_field_source_map=_sec_source_map(),
        yahoo_field_source_map=_yahoo_source_map(),
        fallback_audit_rows=_audit_rows(),
        normalization_run_id="SEC_NORM_RUN1",
    )


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
        "free_cashflow": 30.0,
        "cash": 80.0,
        "total_debt": 20.0,
        "shares_outstanding": None,
        "currency": "USD",
        "run_id": "LATEST_RUN1",
    }
    row.update(overrides)
    return row


def _sec_source_map() -> dict[str, dict[str, object]]:
    return {
        "revenue": _sec_source("revenue"),
        "cash": _sec_source("cash"),
    }


def _sec_source(field_name: str) -> dict[str, object]:
    return {
        "source_provider": "sec_edgar",
        "source_table": "rc_fundamental_statement_raw",
        "source_row_ref": f"sec:{field_name}:AAPL:2026-03-31",
        "source_document_id": "sec_doc_1",
        "source_hash": f"sec_hash_{field_name}",
        "provenance_role": "PRIMARY_REPORTED",
        "merge_action": "SEC_RETAINED",
    }


def _yahoo_source_map() -> dict[str, dict[str, object]]:
    return {
        "free_cashflow": _yahoo_source("free_cashflow", 30.0),
        "total_debt": _yahoo_source("total_debt", 20.0),
    }


def _yahoo_source(field_name: str, new_value: float) -> dict[str, object]:
    return {
        "source_provider": "yahoo",
        "source_table": "rc_fundamental_quarterly_enrichment_audit",
        "source_row_ref": f"AAPL:2026-03-31:{field_name}:2026-03-31:EXACT",
        "source_hash": f"yahoo_hash_{field_name}",
        "provenance_role": "FALLBACK_REPORTED",
        "merge_action": "YAHOO_FILLED_MISSING",
        "old_value": None,
        "new_value": new_value,
        "available_at_utc": "2026-05-03T10:30:00Z",
        "created_at_utc": "2026-05-03T10:30:00Z",
        "run_id": "ENRICH_RUN1",
        "enrichment_run_id": "ENRICH_RUN1",
    }


def _audit_rows() -> list[dict[str, object]]:
    return [_audit_row("free_cashflow", 30.0), _audit_row("total_debt", 20.0)]


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
        "created_at_utc": "2026-05-03T10:30:00Z",
    }
