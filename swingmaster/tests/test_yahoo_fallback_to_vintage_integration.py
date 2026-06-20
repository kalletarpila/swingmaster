from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)
from swingmaster.fundamentals.reported_yahoo_dual_write_adapter import (
    write_yahoo_fallback_enriched_rows_with_optional_vintage,
    write_yahoo_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_yahoo_vintage_metadata import (
    build_yahoo_source_hash,
    build_yahoo_vintage_metadata,
)


BRIDGE_AVAILABLE_AT_UTC = "2026-05-03T10:23:06+00:00"
BRIDGE_INGESTED_AT_UTC = "2026-05-03T10:30:00+00:00"
FALLBACK_AVAILABLE_AT_UTC = "2026-05-03T10:30:00+00:00"
FALLBACK_INGESTED_AT_UTC = "2026-05-03T10:31:00+00:00"
BRIDGE_RUN_ID = "YAHOO_BRIDGE_RUN1"
FALLBACK_RUN_ID = "YAHOO_FALLBACK_RUN1"
PAYLOAD_HASH = "payload_hash_1"


def test_yahoo_bridge_to_vintage_temp_db_flow(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_bridge_to_vintage.db"
    run_migration(db_path)
    row = _bridge_row()
    yahoo_row = _yahoo_quarterly_row()
    metadata = _bridge_metadata(row, yahoo_row)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_yahoo_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[row],
            yahoo_quarterly_rows_by_key={_key(): yahoo_row},
            payload_hash_by_key={_key(): PAYLOAD_HASH},
            write_vintage=True,
            available_at_utc=BRIDGE_AVAILABLE_AT_UTC,
            ingested_at_utc=BRIDGE_INGESTED_AT_UTC,
            run_id=BRIDGE_RUN_ID,
        )
        latest_row = conn.execute(
            """
            SELECT ticker, period_end_date, revenue, net_income, cash, total_debt, shares_outstanding, run_id
            FROM rc_fundamental_quarterly
            """
        ).fetchone()
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, source_provider, source_hash, revenue, net_income, cash, total_debt
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        before = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:23:05+00:00", market="usa")
        at_available = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", BRIDGE_AVAILABLE_AT_UTC, market="usa")
        after = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:23:07+00:00", market="usa")
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[0]))

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 1
    assert result["field_provenance_rows_written"] == len(provenance_rows)
    assert latest_row == ("AAPL", "2026-03-31", 100.0, 25.0, 80.0, 20.0, 1000.0, BRIDGE_RUN_ID)
    assert vintage_row[0] == metadata["statement_vintage_id"]
    assert vintage_row[1] == "yahoo"
    assert vintage_row[2] == metadata["source_hash"]
    assert vintage_row[3:] == (100.0, 25.0, 80.0, 20.0)
    assert before is None
    assert at_available is not None
    assert after is not None
    assert at_available["statement_vintage_id"] == metadata["statement_vintage_id"]

    by_field = {row["field_name"]: row for row in provenance_rows}
    for field_name in ("revenue", "net_income", "cash", "total_debt", "shares_outstanding"):
        assert by_field[field_name]["source_provider"] == "yahoo"
        assert by_field[field_name]["provenance_role"] == "PROVIDER_REPORTED"
        assert by_field[field_name]["merge_action"] == "YAHOO_BRIDGED"


def test_yahoo_fallback_enrichment_to_vintage_temp_db_flow(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_fallback_to_vintage.db"
    run_migration(db_path)
    row = _fallback_row()
    audit_rows = [_audit_row("free_cashflow", 30.0), _audit_row("total_debt", 20.0)]
    yahoo_row = _yahoo_quarterly_row(free_cashflow=30.0, total_debt=20.0)
    metadata = _fallback_metadata(row, yahoo_row, audit_rows)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_yahoo_fallback_enriched_rows_with_optional_vintage(
            conn,
            normalized_rows=[row],
            enrichment_audit_rows_by_key={_key(): audit_rows},
            yahoo_quarterly_rows_by_key={_key(): yahoo_row},
            payload_hash_by_key={_key(): PAYLOAD_HASH},
            field_source_map_by_key={_key(): _sec_retained_source_map()},
            write_vintage=True,
            available_at_utc=FALLBACK_AVAILABLE_AT_UTC,
            ingested_at_utc=FALLBACK_INGESTED_AT_UTC,
            run_id=FALLBACK_RUN_ID,
        )
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, source_provider, source_hash, revenue, free_cashflow, total_debt
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        before = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:29:59+00:00", market="usa")
        at_available = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", FALLBACK_AVAILABLE_AT_UTC, market="usa")
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[0]))

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 1
    assert vintage_row[0] == metadata["statement_vintage_id"]
    assert vintage_row[1] == "yahoo"
    assert vintage_row[2] == metadata["source_hash"]
    assert vintage_row[3:] == (100.0, 30.0, 20.0)
    assert before is None
    assert at_available is not None

    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["free_cashflow"]["source_provider"] == "yahoo"
    assert by_field["free_cashflow"]["provenance_role"] == "FALLBACK_REPORTED"
    assert by_field["free_cashflow"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert by_field["total_debt"]["source_provider"] == "yahoo"
    assert by_field["total_debt"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert by_field["revenue"]["source_provider"] == "sec_edgar"
    assert by_field["revenue"]["merge_action"] == "SEC_RETAINED"
    assert by_field["cash"]["source_provider"] == "sec_edgar"
    assert by_field["cash"]["merge_action"] == "SEC_RETAINED"
    assert by_field["net_income"]["source_provider"] == "unknown"
    assert by_field["net_income"]["merge_action"] == "SOURCE_NOT_PROVIDED"


def test_yahoo_source_identity_is_stable_and_changes_with_payload_or_fallback_value() -> None:
    bridge_row = _bridge_row()
    yahoo_row = _yahoo_quarterly_row()
    first_bridge = _bridge_metadata(bridge_row, yahoo_row, payload_hash=PAYLOAD_HASH)
    second_bridge = _bridge_metadata(dict(reversed(list(bridge_row.items()))), dict(reversed(list(yahoo_row.items()))), payload_hash=PAYLOAD_HASH)
    changed_payload_bridge = _bridge_metadata(bridge_row, yahoo_row, payload_hash="payload_hash_2")

    assert first_bridge["source_hash"] == second_bridge["source_hash"]
    assert first_bridge["statement_vintage_id"] == second_bridge["statement_vintage_id"]
    assert first_bridge["source_hash"] != changed_payload_bridge["source_hash"]
    assert first_bridge["statement_vintage_id"] != changed_payload_bridge["statement_vintage_id"]

    fallback_row = _fallback_row()
    fallback_audit = [_audit_row("free_cashflow", 30.0)]
    changed_audit = [_audit_row("free_cashflow", 31.0)]
    first_fallback = _fallback_metadata(fallback_row, _yahoo_quarterly_row(), fallback_audit)
    second_fallback = _fallback_metadata(dict(reversed(list(fallback_row.items()))), _yahoo_quarterly_row(), list(reversed(fallback_audit)))
    changed_fallback = _fallback_metadata(_fallback_row(free_cashflow=31.0), _yahoo_quarterly_row(free_cashflow=31.0), changed_audit)

    assert first_fallback["source_hash"] == second_fallback["source_hash"]
    assert first_fallback["statement_vintage_id"] == second_fallback["statement_vintage_id"]
    assert first_fallback["source_hash"] != changed_fallback["source_hash"]
    assert first_fallback["statement_vintage_id"] != changed_fallback["statement_vintage_id"]


def test_yahoo_fallback_missing_audit_metadata_fails_safely(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_fallback_missing_audit.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_YAHOO_DUAL_WRITE_ENRICHMENT_AUDIT_ROWS_REQUIRED"):
            write_yahoo_fallback_enriched_rows_with_optional_vintage(
                conn,
                normalized_rows=[_fallback_row()],
                write_vintage=True,
                available_at_utc=FALLBACK_AVAILABLE_AT_UTC,
                ingested_at_utc=FALLBACK_INGESTED_AT_UTC,
                run_id=FALLBACK_RUN_ID,
            )


def _bridge_metadata(
    normalized_row: dict[str, object],
    yahoo_quarterly_row: dict[str, object],
    *,
    payload_hash: str = PAYLOAD_HASH,
) -> dict[str, object]:
    source_hash = build_yahoo_source_hash(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        yahoo_quarterly_row=yahoo_quarterly_row,
        normalized_row=normalized_row,
        payload_hash=payload_hash,
    )
    return build_yahoo_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=normalized_row,
        available_at_utc=BRIDGE_AVAILABLE_AT_UTC,
        ingested_at_utc=BRIDGE_INGESTED_AT_UTC,
        run_id=BRIDGE_RUN_ID,
        source_hash=source_hash,
        mode="yahoo_to_generic_bridge",
        payload_hash=payload_hash,
        provider_observed_at_utc=BRIDGE_AVAILABLE_AT_UTC,
        provider_run_id=str(yahoo_quarterly_row["source_run_id"]),
        normalization_run_id=str(yahoo_quarterly_row["run_id"]),
    )


def _fallback_metadata(
    normalized_row: dict[str, object],
    yahoo_quarterly_row: dict[str, object],
    audit_rows: list[dict[str, object]],
    *,
    payload_hash: str = PAYLOAD_HASH,
) -> dict[str, object]:
    source_hash = build_yahoo_source_hash(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        yahoo_quarterly_row=yahoo_quarterly_row,
        normalized_row=normalized_row,
        payload_hash=payload_hash,
        enrichment_audit_rows=audit_rows,
    )
    return build_yahoo_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=normalized_row,
        available_at_utc=FALLBACK_AVAILABLE_AT_UTC,
        ingested_at_utc=FALLBACK_INGESTED_AT_UTC,
        run_id=FALLBACK_RUN_ID,
        source_hash=source_hash,
        mode="yahoo_fallback_enrichment",
        payload_hash=payload_hash,
        provider_observed_at_utc=FALLBACK_AVAILABLE_AT_UTC,
        provider_run_id=str(yahoo_quarterly_row["source_run_id"]),
        normalization_run_id=str(yahoo_quarterly_row["run_id"]),
    )


def _key() -> tuple[str, str]:
    return "AAPL", "2026-03-31"


def _bridge_row(**overrides: object) -> dict[str, object]:
    row = _base_row(
        run_id=BRIDGE_RUN_ID,
        revenue=100.0,
        net_income=25.0,
        cash=80.0,
        total_debt=20.0,
        shares_outstanding=1000.0,
    )
    row.update(overrides)
    return row


def _fallback_row(**overrides: object) -> dict[str, object]:
    row = _base_row(
        run_id=FALLBACK_RUN_ID,
        revenue=100.0,
        net_income=25.0,
        free_cashflow=30.0,
        cash=80.0,
        total_debt=20.0,
    )
    row.update(overrides)
    return row


def _base_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": None,
        "gross_profit": None,
        "operating_income": None,
        "ebit": None,
        "ebitda": None,
        "net_income": None,
        "operating_cashflow": None,
        "capex": None,
        "free_cashflow": None,
        "cash": None,
        "total_debt": None,
        "shares_outstanding": None,
        "currency": "USD",
        "run_id": "LATEST_RUN1",
    }
    row.update(overrides)
    return row


def _yahoo_quarterly_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "market": "usa",
        "symbol": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "net_income": 25.0,
        "free_cashflow": 30.0,
        "cash": 80.0,
        "total_debt": 20.0,
        "shares_outstanding": 1000.0,
        "shares_source": "ordinary_shares_number",
        "shares_quality": "OK",
        "source_run_id": "YRAW1",
        "run_id": "YQTR1",
        "created_at_utc": BRIDGE_AVAILABLE_AT_UTC,
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
        "run_id": FALLBACK_RUN_ID,
        "created_at_utc": FALLBACK_AVAILABLE_AT_UTC,
    }


def _sec_retained_source_map() -> dict[str, dict[str, object]]:
    return {
        "revenue": _sec_source_info("revenue"),
        "cash": _sec_source_info("cash"),
    }


def _sec_source_info(field_name: str) -> dict[str, object]:
    return {
        "source_provider": "sec_edgar",
        "source_table": "rc_fundamental_statement_raw",
        "source_row_ref": f"AAPL:2026-03-31:{field_name}",
        "source_hash": f"sec_hash_{field_name}",
        "provenance_role": "PRIMARY_REPORTED",
        "merge_action": "SEC_RETAINED",
    }
