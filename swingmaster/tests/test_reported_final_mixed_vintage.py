from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_final_mixed_vintage import (
    build_final_mixed_source_hash,
    build_final_mixed_statement_vintage_id,
    build_final_mixed_vintage_metadata,
    merge_final_mixed_field_source_maps,
)
from swingmaster.fundamentals.reported_quarterly_dual_write import (
    write_normalized_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)


def test_source_hash_is_deterministic_for_same_inputs() -> None:
    first_hash = _source_hash()
    second_hash = _source_hash()

    assert first_hash == second_hash


def test_source_hash_is_stable_under_fallback_audit_row_order() -> None:
    audit_rows = [_audit_row("free_cashflow", 30.0), _audit_row("total_debt", 20.0)]

    first_hash = _source_hash(fallback_audit_rows=audit_rows)
    second_hash = _source_hash(fallback_audit_rows=list(reversed(audit_rows)))

    assert first_hash == second_hash


def test_source_hash_changes_when_normalized_financial_value_changes() -> None:
    first_hash = _source_hash()
    changed_hash = _source_hash(normalized_row=_normalized_row(revenue=101.0))

    assert first_hash != changed_hash


def test_source_hash_changes_when_sec_field_provenance_changes() -> None:
    first_hash = _source_hash()
    changed_sec_map = _sec_source_map(revenue={"source_hash": "sec_hash_revenue_changed"})
    changed_hash = _source_hash(sec_field_source_map=changed_sec_map)

    assert first_hash != changed_hash


def test_source_hash_changes_when_yahoo_fallback_audit_row_changes() -> None:
    first_hash = _source_hash()
    changed_hash = _source_hash(fallback_audit_rows=[_audit_row("free_cashflow", 31.0), _audit_row("total_debt", 20.0)])

    assert first_hash != changed_hash


def test_statement_vintage_id_is_deterministic_and_uses_mixed_prefix() -> None:
    source_hash = _source_hash()

    first_id = build_final_mixed_statement_vintage_id(
        market="USA",
        ticker=" aapl ",
        period_end_date="2026-03-31",
        source_hash=source_hash,
    )
    second_id = build_final_mixed_statement_vintage_id(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        source_hash=source_hash,
    )

    assert first_id == second_id
    assert first_id == f"mixed_sec_yahoo:usa:AAPL:2026-03-31:{source_hash[:16]}"


def test_metadata_builder_requires_available_at_utc() -> None:
    with pytest.raises(ValueError, match="FINAL_MIXED_VINTAGE_REQUIRED_FIELD_MISSING:available_at_utc"):
        _metadata(available_at_utc="")


def test_metadata_builder_requires_ingested_at_utc() -> None:
    with pytest.raises(ValueError, match="FINAL_MIXED_VINTAGE_REQUIRED_FIELD_MISSING:ingested_at_utc"):
        _metadata(ingested_at_utc=" ")


def test_metadata_builder_does_not_use_period_end_date_as_availability() -> None:
    with pytest.raises(ValueError, match="FINAL_MIXED_VINTAGE_METADATA_INVALID_AVAILABLE_AT_PERIOD_END_DATE"):
        _metadata(available_at_utc="2026-03-31")

    metadata = _metadata(available_at_utc="2026-05-03T10:30:00Z")
    assert metadata["available_at_utc"] == "2026-05-03T10:30:00Z"
    assert metadata["available_at_utc"] != "2026-03-31"


def test_field_merge_preserves_sec_retained_fields_as_sec() -> None:
    merged = _merged_source_map()

    assert merged["revenue"]["source_provider"] == "sec_edgar"
    assert merged["revenue"]["provenance_role"] == "PRIMARY_REPORTED"
    assert merged["revenue"]["merge_action"] == "SEC_RETAINED"


def test_field_merge_preserves_yahoo_filled_fields_as_yahoo_fallback() -> None:
    merged = _merged_source_map()

    assert merged["free_cashflow"]["source_provider"] == "yahoo"
    assert merged["free_cashflow"]["provenance_role"] == "FALLBACK_REPORTED"
    assert merged["free_cashflow"]["merge_action"] == "YAHOO_FILLED_MISSING"


def test_conflicting_sec_and_yahoo_field_provenance_raises_value_error() -> None:
    yahoo_map = _yahoo_source_map(revenue={"merge_action": "YAHOO_BRIDGED"})

    with pytest.raises(ValueError, match="FINAL_MIXED_VINTAGE_FIELD_SOURCE_CONFLICT:revenue"):
        merge_final_mixed_field_source_maps(
            normalized_row=_normalized_row(),
            sec_field_source_map=_sec_source_map(),
            yahoo_field_source_map=yahoo_map,
        )


def test_unmapped_non_null_field_becomes_unknown_not_yahoo() -> None:
    merged = _merged_source_map()

    assert merged["net_income"]["source_provider"] == "unknown"
    assert merged["net_income"]["provenance_role"] == "UNSPECIFIED"
    assert merged["net_income"]["merge_action"] == "SOURCE_NOT_PROVIDED"


def test_null_fields_do_not_create_provenance_map_entries() -> None:
    merged = merge_final_mixed_field_source_maps(
        normalized_row=_normalized_row(operating_income=None),
        sec_field_source_map=_sec_source_map(),
        yahoo_field_source_map=_yahoo_source_map(),
    )

    assert "operating_income" not in merged


def test_final_mixed_row_can_be_written_through_opt_in_adapter(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_final_mixed_write.db"
    run_migration(db_path)
    row = _normalized_row()
    metadata = _metadata()
    source_map = _merged_source_map()

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [row],
            write_vintage=True,
            vintage_metadata_by_key={_key(): metadata},
            field_source_map_by_key={_key(): source_map},
        )
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, source_provider, source_hash, revenue, free_cashflow, net_income
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()

    assert result == {
        "latest_rows_written": 1,
        "vintage_rows_written": 1,
        "field_provenance_rows_written": 5,
    }
    assert vintage_row == (
        metadata["statement_vintage_id"],
        "mixed_sec_yahoo",
        metadata["source_hash"],
        100.0,
        30.0,
        25.0,
    )


def test_pit_reader_returns_final_mixed_row_at_or_after_available_at(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_final_mixed_pit_after.db"
    run_migration(db_path)
    metadata = _write_final_mixed_fixture(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-03T10:30:00Z",
            market="usa",
        )

    assert row is not None
    assert row["statement_vintage_id"] == metadata["statement_vintage_id"]
    assert row["source_provider"] == "mixed_sec_yahoo"


def test_pit_reader_returns_none_before_available_at(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_final_mixed_pit_before.db"
    run_migration(db_path)
    _write_final_mixed_fixture(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-03T10:29:59Z",
            market="usa",
        )

    assert row is None


def test_provenance_rows_match_final_mixed_source_map(tmp_path: Path) -> None:
    db_path = tmp_path / "reported_final_mixed_provenance.db"
    run_migration(db_path)
    metadata = _write_final_mixed_fixture(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        provenance_rows = get_quarterly_field_provenance(conn, str(metadata["statement_vintage_id"]))

    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "sec_edgar"
    assert by_field["revenue"]["merge_action"] == "SEC_RETAINED"
    assert by_field["cash"]["source_provider"] == "sec_edgar"
    assert by_field["free_cashflow"]["source_provider"] == "yahoo"
    assert by_field["free_cashflow"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert by_field["total_debt"]["source_provider"] == "yahoo"
    assert by_field["net_income"]["source_provider"] == "unknown"
    assert by_field["net_income"]["merge_action"] == "SOURCE_NOT_PROVIDED"


def test_final_mixed_builder_does_not_import_provider_modules() -> None:
    import sys

    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules
    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules
    assert "yfinance" not in sys.modules


def _write_final_mixed_fixture(db_path: Path) -> dict[str, object]:
    row = _normalized_row()
    metadata = _metadata()
    source_map = _merged_source_map()
    with sqlite3.connect(str(db_path)) as conn:
        write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [row],
            write_vintage=True,
            vintage_metadata_by_key={_key(): metadata},
            field_source_map_by_key={_key(): source_map},
        )
    return metadata


def _source_hash(
    *,
    normalized_row: dict[str, object] | None = None,
    sec_field_source_map: dict[str, dict[str, object]] | None = None,
    yahoo_field_source_map: dict[str, dict[str, object]] | None = None,
    fallback_audit_rows: list[dict[str, object]] | None = None,
) -> str:
    return build_final_mixed_source_hash(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=normalized_row or _normalized_row(),
        sec_field_source_map=sec_field_source_map or _sec_source_map(),
        yahoo_field_source_map=yahoo_field_source_map or _yahoo_source_map(),
        fallback_audit_rows=fallback_audit_rows or [_audit_row("free_cashflow", 30.0), _audit_row("total_debt", 20.0)],
    )


def _metadata(**overrides: object) -> dict[str, object]:
    row = _normalized_row()
    metadata = build_final_mixed_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=row,
        source_hash=_source_hash(normalized_row=row),
        available_at_utc="2026-05-03T10:30:00Z",
        ingested_at_utc="2026-05-03T10:31:00Z",
        run_id="FINAL_MIXED_RUN1",
        normalization_run_id="SEC_NORM_RUN1",
    )
    metadata.update(overrides)
    if overrides:
        metadata = build_final_mixed_vintage_metadata(
            market=str(overrides.get("market", "usa")),
            ticker=str(overrides.get("ticker", "AAPL")),
            period_end_date=str(overrides.get("period_end_date", "2026-03-31")),
            normalized_row=row,
            source_hash=str(overrides.get("source_hash", _source_hash(normalized_row=row))),
            available_at_utc=str(overrides.get("available_at_utc", "2026-05-03T10:30:00Z")),
            ingested_at_utc=str(overrides.get("ingested_at_utc", "2026-05-03T10:31:00Z")),
            run_id=str(overrides.get("run_id", "FINAL_MIXED_RUN1")),
            normalization_run_id=overrides.get("normalization_run_id", "SEC_NORM_RUN1"),  # type: ignore[arg-type]
        )
    return metadata


def _merged_source_map() -> dict[str, dict[str, object]]:
    return merge_final_mixed_field_source_maps(
        normalized_row=_normalized_row(),
        sec_field_source_map=_sec_source_map(),
        yahoo_field_source_map=_yahoo_source_map(),
    )


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
        "free_cashflow": 30.0,
        "cash": 80.0,
        "total_debt": 20.0,
        "shares_outstanding": None,
        "currency": "USD",
        "run_id": "LATEST_RUN1",
    }
    row.update(overrides)
    return row


def _sec_source_map(**field_overrides: dict[str, object]) -> dict[str, dict[str, object]]:
    source_map = {
        "revenue": _sec_source("revenue"),
        "cash": _sec_source("cash"),
    }
    for field_name, overrides in field_overrides.items():
        source_map[field_name].update(overrides)
    return source_map


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


def _yahoo_source_map(**field_overrides: dict[str, object]) -> dict[str, dict[str, object]]:
    source_map = {
        "free_cashflow": _yahoo_source("free_cashflow", 30.0),
        "total_debt": _yahoo_source("total_debt", 20.0),
    }
    for field_name, overrides in field_overrides.items():
        source_map.setdefault(field_name, _yahoo_source(field_name, 100.0)).update(overrides)
    return source_map


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
