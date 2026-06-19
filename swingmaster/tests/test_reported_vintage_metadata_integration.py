from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_quarterly_dual_write import (
    write_normalized_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_sec_vintage_metadata import (
    build_sec_field_source_map,
    build_sec_vintage_metadata,
)
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)
from swingmaster.fundamentals.reported_yahoo_vintage_metadata import (
    build_yahoo_field_source_map,
    build_yahoo_source_hash,
    build_yahoo_vintage_metadata,
)


def test_sec_metadata_contract_feeds_adapter_and_pit_reader(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_sec_integration.db"
    run_migration(db_path)
    row = _normalized_row()
    facts = [
        _sec_fact("Revenues", 100.0),
        _sec_fact("NetIncomeLoss", 25.0),
        _sec_fact("CashAndCashEquivalentsAtCarryingValue", 80.0, statement_type="balance", start="NULL"),
        _sec_fact("LongTermDebtNoncurrent", 20.0, statement_type="balance", start="NULL"),
    ]
    metadata = build_sec_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=row,
        contributing_facts=facts,
        available_at_utc="2026-04-30T00:00:00Z",
        ingested_at_utc="2026-04-30T01:00:00Z",
        run_id="SEC_NORM_RUN1",
        normalization_run_id="SEC_NORM_RUN1",
    )
    field_source_map = build_sec_field_source_map(
        normalized_row=row,
        field_to_contributing_facts={
            "revenue": [facts[0]],
            "net_income": [facts[1]],
            "cash": [facts[2]],
            "total_debt": [facts[3]],
        },
    )

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [row],
            write_vintage=True,
            vintage_metadata_by_key={_key(): metadata},
            field_source_map_by_key={_key(): field_source_map},
        )
        latest_row = conn.execute(
            "SELECT ticker, period_end_date, revenue, run_id FROM rc_fundamental_quarterly"
        ).fetchone()
        pit_before = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-04-29T23:59:59Z",
            market="usa",
        )
        pit_after = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-04-30T00:00:00Z",
            market="usa",
        )
        provenance_rows = get_quarterly_field_provenance(conn, str(metadata["statement_vintage_id"]))

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 1
    assert latest_row == ("AAPL", "2026-03-31", 100.0, "SEC_NORM_RUN1")
    assert pit_before is None
    assert pit_after is not None
    assert pit_after["statement_vintage_id"] == metadata["statement_vintage_id"]
    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "sec_edgar"
    assert by_field["revenue"]["provenance_role"] == "PRIMARY_REPORTED"
    assert by_field["revenue"]["merge_action"] == "SEC_RETAINED"
    assert by_field["cash"]["source_provider"] == "sec_edgar"


def test_yahoo_bridge_metadata_contract_feeds_adapter_and_pit_reader(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_yahoo_bridge_integration.db"
    run_migration(db_path)
    row = _normalized_row(run_id="YAHOO_BRIDGE_RUN1")
    yahoo_row = _yahoo_quarterly_row()
    source_hash = build_yahoo_source_hash(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        yahoo_quarterly_row=yahoo_row,
        normalized_row=row,
        payload_hash="payload_hash_1",
    )
    metadata = build_yahoo_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=row,
        available_at_utc="2026-05-03T10:23:06+00:00",
        ingested_at_utc="2026-05-03T10:30:00+00:00",
        run_id="YAHOO_BRIDGE_RUN1",
        source_hash=source_hash,
        mode="yahoo_to_generic_bridge",
        payload_hash="payload_hash_1",
        provider_observed_at_utc="2026-05-03T10:23:06+00:00",
        provider_run_id="YRAW1",
        normalization_run_id="YQTR1",
    )
    field_source_map = build_yahoo_field_source_map(
        normalized_row=row,
        yahoo_fields={"revenue", "net_income", "cash", "total_debt"},
        default_role="PROVIDER_REPORTED",
    )

    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [row],
            write_vintage=True,
            vintage_metadata_by_key={_key(): metadata},
            field_source_map_by_key={_key(): field_source_map},
        )
        latest_row = conn.execute("SELECT ticker, revenue, run_id FROM rc_fundamental_quarterly").fetchone()
        pit_row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-03T10:23:06+00:00",
            market="usa",
        )
        provenance_rows = get_quarterly_field_provenance(conn, str(metadata["statement_vintage_id"]))

    assert latest_row == ("AAPL", 100.0, "YAHOO_BRIDGE_RUN1")
    assert pit_row is not None
    assert "yahoo_to_generic_bridge" in pit_row["statement_vintage_id"]
    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "yahoo"
    assert by_field["revenue"]["provenance_role"] == "PROVIDER_REPORTED"
    assert by_field["revenue"]["merge_action"] == "YAHOO_BRIDGED"


def test_mixed_sec_yahoo_fallback_metadata_contract_feeds_adapter(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_mixed_integration.db"
    run_migration(db_path)
    row = _normalized_row(free_cashflow=30.0, run_id="MIXED_RUN1")
    sec_facts = [
        _sec_fact("Revenues", 100.0),
        _sec_fact("NetIncomeLoss", 25.0),
        _sec_fact("CashAndCashEquivalentsAtCarryingValue", 80.0, statement_type="balance", start="NULL"),
    ]
    audit_rows = [
        _audit_row("free_cashflow", 30.0),
        _audit_row("total_debt", 20.0),
    ]
    source_hash = build_yahoo_source_hash(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        yahoo_quarterly_row=_yahoo_quarterly_row(),
        normalized_row=row,
        payload_hash="payload_hash_1",
        enrichment_audit_rows=audit_rows,
    )
    metadata = build_yahoo_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=row,
        available_at_utc="2026-05-03T10:30:00+00:00",
        ingested_at_utc="2026-05-03T10:31:00+00:00",
        run_id="MIXED_RUN1",
        source_hash=source_hash,
        mode="yahoo_fallback_enrichment",
        payload_hash="payload_hash_1",
        provider_observed_at_utc="2026-05-03T10:23:06+00:00",
        provider_run_id="YRAW1",
        normalization_run_id="SEC_NORM_RUN1",
    )
    sec_source_map = build_sec_field_source_map(
        normalized_row=row,
        field_to_contributing_facts={
            "revenue": [sec_facts[0]],
            "net_income": [sec_facts[1]],
            "cash": [sec_facts[2]],
        },
    )
    yahoo_source_map = build_yahoo_field_source_map(
        normalized_row=row,
        enrichment_audit_rows=audit_rows,
    )
    field_source_map = {**sec_source_map, **yahoo_source_map}

    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [row],
            write_vintage=True,
            vintage_metadata_by_key={_key(): metadata},
            field_source_map_by_key={_key(): field_source_map},
        )
        pit_row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-03T10:30:00+00:00",
            market="usa",
        )
        provenance_rows = get_quarterly_field_provenance(conn, str(metadata["statement_vintage_id"]))

    assert pit_row is not None
    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "sec_edgar"
    assert by_field["revenue"]["merge_action"] == "SEC_RETAINED"
    assert by_field["cash"]["source_provider"] == "sec_edgar"
    assert by_field["free_cashflow"]["source_provider"] == "yahoo"
    assert by_field["free_cashflow"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert by_field["total_debt"]["source_provider"] == "yahoo"
    assert by_field["total_debt"]["provenance_role"] == "FALLBACK_REPORTED"


def test_adapter_rejects_incomplete_metadata_from_contract_helpers(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_incomplete_metadata.db"
    run_migration(db_path)
    row = _normalized_row()
    metadata = build_sec_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=row,
        contributing_facts=[_sec_fact("Revenues", 100.0)],
        available_at_utc="2026-04-30T00:00:00Z",
        ingested_at_utc="2026-04-30T01:00:00Z",
        run_id="SEC_NORM_RUN1",
    )
    metadata["available_at_utc"] = ""

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:.*available_at_utc"):
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [row],
                write_vintage=True,
                vintage_metadata_by_key={_key(): metadata},
            )
        metadata["available_at_utc"] = "2026-04-30T00:00:00Z"
        metadata["statement_vintage_id"] = ""
        with pytest.raises(ValueError, match="REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:.*statement_vintage_id"):
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [row],
                write_vintage=True,
                vintage_metadata_by_key={_key(): metadata},
            )


def test_duplicate_statement_vintage_id_raises_integrity_error(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_duplicate_metadata.db"
    run_migration(db_path)
    row = _normalized_row()
    metadata = build_sec_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=row,
        contributing_facts=[_sec_fact("Revenues", 100.0)],
        available_at_utc="2026-04-30T00:00:00Z",
        ingested_at_utc="2026-04-30T01:00:00Z",
        run_id="SEC_NORM_RUN1",
    )

    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [row],
            write_vintage=True,
            vintage_metadata_by_key={_key(): metadata},
        )
        with pytest.raises(sqlite3.IntegrityError):
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [_normalized_row(revenue=101.0)],
                write_vintage=True,
                vintage_metadata_by_key={_key(): metadata},
            )


def test_default_adapter_behavior_remains_latest_only(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_vintage_default_latest_only.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(conn, [_normalized_row()])
        latest_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert result == {
        "latest_rows_written": 1,
        "vintage_rows_written": 0,
        "field_provenance_rows_written": 0,
    }
    assert latest_count == 1
    assert vintage_count == 0
    assert provenance_count == 0


def test_integration_does_not_import_provider_modules() -> None:
    import sys

    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules
    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules
    assert "yfinance" not in sys.modules


def _key(ticker: str = "AAPL", period_end_date: str = "2026-03-31") -> tuple[str, str]:
    return ticker.upper(), period_end_date


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
        "run_id": "LATEST_RUN1",
    }
    row.update(overrides)
    return row


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
