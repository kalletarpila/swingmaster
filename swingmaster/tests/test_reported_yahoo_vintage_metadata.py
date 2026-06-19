from __future__ import annotations

import pytest

from swingmaster.fundamentals.reported_yahoo_vintage_metadata import (
    YAHOO_REPORTED_AVAILABILITY_QUALITY,
    YAHOO_REPORTED_SOURCE_PROVIDER,
    build_yahoo_field_source_map,
    build_yahoo_source_hash,
    build_yahoo_statement_vintage_id,
    build_yahoo_vintage_metadata,
)


def test_source_hash_is_deterministic_for_same_yahoo_and_normalized_row() -> None:
    first = _source_hash()
    second = _source_hash()

    assert first == second


def test_source_hash_is_unchanged_by_enrichment_audit_row_order() -> None:
    first = _source_hash(enrichment_audit_rows=_audit_rows())
    second = _source_hash(enrichment_audit_rows=list(reversed(_audit_rows())))

    assert first == second


def test_source_hash_changes_when_payload_hash_changes() -> None:
    assert _source_hash(payload_hash="payload_hash_1") != _source_hash(payload_hash="payload_hash_2")


def test_source_hash_changes_when_normalized_financial_value_changes() -> None:
    assert _source_hash(normalized_row=_normalized_row(revenue=100.0)) != _source_hash(
        normalized_row=_normalized_row(revenue=101.0)
    )


def test_source_hash_changes_when_fallback_filled_field_changes() -> None:
    changed_audit_rows = _audit_rows()
    changed_audit_rows[0]["new_value"] = 31.0

    assert _source_hash(enrichment_audit_rows=_audit_rows()) != _source_hash(enrichment_audit_rows=changed_audit_rows)


def test_statement_vintage_id_is_deterministic_and_includes_identity_parts() -> None:
    source_hash = "abcdef1234567890fedcba0987654321"

    statement_vintage_id = build_yahoo_statement_vintage_id(
        market="USA",
        ticker=" aapl ",
        period_end_date="2026-03-31",
        source_hash=source_hash,
        mode="yahoo_fallback_enrichment",
    )

    assert statement_vintage_id == "yahoo:yahoo_fallback_enrichment:usa:AAPL:2026-03-31:abcdef1234567890"
    assert statement_vintage_id == build_yahoo_statement_vintage_id(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        source_hash=source_hash,
        mode="yahoo_fallback_enrichment",
    )


def test_metadata_builder_requires_available_at_utc() -> None:
    with pytest.raises(ValueError, match="REPORTED_YAHOO_VINTAGE_METADATA_REQUIRED_FIELD_MISSING:available_at_utc"):
        _metadata(available_at_utc="")


def test_metadata_builder_requires_ingested_at_utc() -> None:
    with pytest.raises(ValueError, match="REPORTED_YAHOO_VINTAGE_METADATA_REQUIRED_FIELD_MISSING:ingested_at_utc"):
        _metadata(ingested_at_utc="")


def test_metadata_builder_rejects_period_end_as_available_at() -> None:
    with pytest.raises(ValueError, match="REPORTED_YAHOO_VINTAGE_METADATA_INVALID_AVAILABLE_AT_PERIOD_END_DATE"):
        _metadata(available_at_utc="2026-03-31")


def test_metadata_includes_yahoo_source_provider_value() -> None:
    metadata = _metadata()

    assert metadata["source_provider"] == YAHOO_REPORTED_SOURCE_PROVIDER
    assert metadata["availability_quality"] == YAHOO_REPORTED_AVAILABILITY_QUALITY


def test_metadata_uses_payload_hash_and_source_hash() -> None:
    source_hash = _source_hash(payload_hash="payload_hash_1")
    metadata = _metadata(source_hash=source_hash, payload_hash="payload_hash_1")

    assert metadata["source_hash"] == source_hash
    assert metadata["payload_hash"] == "payload_hash_1"
    assert metadata["statement_vintage_id"].endswith(source_hash[:16])


def test_yahoo_only_field_source_map_marks_non_null_fields_with_yahoo_provenance() -> None:
    source_map = build_yahoo_field_source_map(
        normalized_row=_normalized_row(gross_profit=None),
        yahoo_fields={"revenue", "gross_profit", "cash"},
        default_role="PROVIDER_REPORTED",
    )

    assert source_map["revenue"]["source_provider"] == YAHOO_REPORTED_SOURCE_PROVIDER
    assert source_map["revenue"]["provenance_role"] == "PROVIDER_REPORTED"
    assert source_map["revenue"]["merge_action"] == "YAHOO_BRIDGED"
    assert source_map["cash"]["source_table"] == "rc_fundamental_yahoo_quarterly"
    assert "gross_profit" not in source_map


def test_fallback_source_map_marks_yahoo_filled_fields_only() -> None:
    source_map = build_yahoo_field_source_map(
        normalized_row=_normalized_row(revenue=100.0, free_cashflow=30.0, cash=80.0),
        enrichment_audit_rows=_audit_rows(),
    )

    assert "revenue" not in source_map
    assert source_map["free_cashflow"]["source_provider"] == YAHOO_REPORTED_SOURCE_PROVIDER
    assert source_map["free_cashflow"]["provenance_role"] == "FALLBACK_REPORTED"
    assert source_map["free_cashflow"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert source_map["free_cashflow"]["old_value"] is None
    assert source_map["free_cashflow"]["new_value"] == 30.0
    assert source_map["cash"]["merge_action"] == "YAHOO_FILLED_MISSING"


def test_null_normalized_fields_do_not_produce_provenance() -> None:
    source_map = build_yahoo_field_source_map(
        normalized_row=_normalized_row(free_cashflow=None),
        enrichment_audit_rows=_audit_rows(),
    )

    assert "free_cashflow" not in source_map


def test_ticker_normalization_matches_adapter_convention() -> None:
    metadata = build_yahoo_vintage_metadata(
        market="USA",
        ticker=" aapl ",
        period_end_date="2026-03-31",
        normalized_row=_normalized_row(ticker=" aapl "),
        available_at_utc="2026-05-03T10:23:06+00:00",
        ingested_at_utc="2026-05-03T10:30:00+00:00",
        run_id="YRUN1",
        source_hash=_source_hash(),
        mode="yahoo_to_generic_bridge",
    )

    assert metadata["statement_vintage_id"].startswith("yahoo:yahoo_to_generic_bridge:usa:AAPL:2026-03-31:")


def test_invalid_mode_raises() -> None:
    with pytest.raises(ValueError, match="REPORTED_YAHOO_VINTAGE_METADATA_INVALID_MODE:bad_mode"):
        build_yahoo_statement_vintage_id(
            market="usa",
            ticker="AAPL",
            period_end_date="2026-03-31",
            source_hash="hash",
            mode="bad_mode",
        )


def test_functions_make_no_provider_network_imports() -> None:
    import sys

    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules
    assert "yfinance" not in sys.modules


def _source_hash(
    *,
    normalized_row: dict[str, object] | None = None,
    payload_hash: str | None = "payload_hash_1",
    enrichment_audit_rows: list[dict[str, object]] | None = None,
) -> str:
    return build_yahoo_source_hash(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        yahoo_quarterly_row=_yahoo_quarterly_row(),
        normalized_row=normalized_row or _normalized_row(),
        payload_hash=payload_hash,
        enrichment_audit_rows=enrichment_audit_rows,
    )


def _metadata(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "market": "usa",
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "normalized_row": _normalized_row(),
        "available_at_utc": "2026-05-03T10:23:06+00:00",
        "ingested_at_utc": "2026-05-03T10:30:00+00:00",
        "run_id": "YRUN1",
        "source_hash": _source_hash(),
        "mode": "yahoo_to_generic_bridge",
        "payload_hash": "payload_hash_1",
        "source_document_id": None,
        "provider_observed_at_utc": "2026-05-03T10:23:06+00:00",
        "provider_run_id": "YRAW1",
        "normalization_run_id": "YQTR1",
    }
    values.update(overrides)
    return build_yahoo_vintage_metadata(**values)


def _normalized_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "gross_profit": 45.0,
        "operating_income": 30.0,
        "ebit": 30.0,
        "ebitda": None,
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


def _yahoo_quarterly_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "market": "usa",
        "symbol": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "free_cashflow": 30.0,
        "cash": 80.0,
        "shares_source": "ordinary_shares_number",
        "shares_quality": "OK",
        "source_run_id": "YRAW1",
        "run_id": "YQTR1",
        "created_at_utc": "2026-05-03T10:23:06+00:00",
    }
    row.update(overrides)
    return row


def _audit_rows() -> list[dict[str, object]]:
    return [
        {
            "ticker": "AAPL",
            "period_end_date": "2026-03-31",
            "field_name": "free_cashflow",
            "old_value": None,
            "new_value": 30.0,
            "primary_source": "sec_edgar",
            "fallback_source": "yahoo",
            "enrichment_status": "FILLED_FROM_YAHOO",
            "matched_yahoo_period_end_date": "2026-03-31",
            "match_method": "EXACT",
            "run_id": "ENRICH1",
            "created_at_utc": "2026-05-03T10:30:00+00:00",
        },
        {
            "ticker": "AAPL",
            "period_end_date": "2026-03-31",
            "field_name": "cash",
            "old_value": None,
            "new_value": 80.0,
            "primary_source": "sec_edgar",
            "fallback_source": "yahoo",
            "enrichment_status": "FILLED_FROM_YAHOO",
            "matched_yahoo_period_end_date": "2026-03-31",
            "match_method": "EXACT",
            "run_id": "ENRICH1",
            "created_at_utc": "2026-05-03T10:30:00+00:00",
        },
    ]
