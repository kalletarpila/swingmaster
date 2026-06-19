from __future__ import annotations

import pytest

from swingmaster.fundamentals.reported_sec_vintage_metadata import (
    SEC_REPORTED_AVAILABILITY_QUALITY,
    SEC_REPORTED_SOURCE_PROVIDER,
    build_sec_field_source_map,
    build_sec_source_hash,
    build_sec_statement_vintage_id,
    build_sec_vintage_metadata,
    extract_sec_filed_date,
)


def test_source_hash_is_deterministic_for_same_row_and_facts() -> None:
    first = build_sec_source_hash(
        ticker="aapl",
        period_end_date="2026-03-31",
        contributing_facts=_facts(),
        normalized_row=_normalized_row(),
    )
    second = build_sec_source_hash(
        ticker="AAPL",
        period_end_date="2026-03-31",
        contributing_facts=_facts(),
        normalized_row=_normalized_row(),
    )

    assert first == second


def test_source_hash_is_unchanged_by_contributing_fact_order() -> None:
    source_hash = build_sec_source_hash(
        ticker="AAPL",
        period_end_date="2026-03-31",
        contributing_facts=_facts(),
        normalized_row=_normalized_row(),
    )
    reordered_hash = build_sec_source_hash(
        ticker="AAPL",
        period_end_date="2026-03-31",
        contributing_facts=list(reversed(_facts())),
        normalized_row=_normalized_row(),
    )

    assert source_hash == reordered_hash


def test_source_hash_changes_when_contributing_fact_value_changes() -> None:
    facts = _facts()
    changed_facts = _facts()
    changed_facts[0]["field_value"] = 101.0

    assert build_sec_source_hash(
        ticker="AAPL",
        period_end_date="2026-03-31",
        contributing_facts=facts,
        normalized_row=_normalized_row(),
    ) != build_sec_source_hash(
        ticker="AAPL",
        period_end_date="2026-03-31",
        contributing_facts=changed_facts,
        normalized_row=_normalized_row(),
    )


def test_source_hash_changes_when_normalized_row_value_changes() -> None:
    changed_row = _normalized_row(revenue=101.0)

    assert build_sec_source_hash(
        ticker="AAPL",
        period_end_date="2026-03-31",
        contributing_facts=_facts(),
        normalized_row=_normalized_row(),
    ) != build_sec_source_hash(
        ticker="AAPL",
        period_end_date="2026-03-31",
        contributing_facts=_facts(),
        normalized_row=changed_row,
    )


def test_statement_vintage_id_is_deterministic_and_includes_identity_parts() -> None:
    source_hash = "abcdef1234567890fedcba0987654321"

    statement_vintage_id = build_sec_statement_vintage_id(
        market="USA",
        ticker=" aapl ",
        period_end_date="2026-03-31",
        source_hash=source_hash,
    )

    assert statement_vintage_id == "sec_edgar:usa:AAPL:2026-03-31:abcdef1234567890"
    assert statement_vintage_id == build_sec_statement_vintage_id(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        source_hash=source_hash,
    )


def test_metadata_builder_requires_available_at_utc() -> None:
    with pytest.raises(ValueError, match="REPORTED_SEC_VINTAGE_METADATA_REQUIRED_FIELD_MISSING:available_at_utc"):
        build_sec_vintage_metadata(
            market="usa",
            ticker="AAPL",
            period_end_date="2026-03-31",
            normalized_row=_normalized_row(),
            contributing_facts=_facts(),
            available_at_utc="",
            ingested_at_utc="2026-04-30T01:00:00Z",
            run_id="RUN1",
        )


def test_metadata_builder_requires_ingested_at_utc() -> None:
    with pytest.raises(ValueError, match="REPORTED_SEC_VINTAGE_METADATA_REQUIRED_FIELD_MISSING:ingested_at_utc"):
        build_sec_vintage_metadata(
            market="usa",
            ticker="AAPL",
            period_end_date="2026-03-31",
            normalized_row=_normalized_row(),
            contributing_facts=_facts(),
            available_at_utc="2026-04-30T00:00:00Z",
            ingested_at_utc="",
            run_id="RUN1",
        )


def test_metadata_builder_rejects_period_end_as_available_at() -> None:
    with pytest.raises(ValueError, match="REPORTED_SEC_VINTAGE_METADATA_INVALID_AVAILABLE_AT_PERIOD_END_DATE"):
        build_sec_vintage_metadata(
            market="usa",
            ticker="AAPL",
            period_end_date="2026-03-31",
            normalized_row=_normalized_row(),
            contributing_facts=_facts(),
            available_at_utc="2026-03-31",
            ingested_at_utc="2026-04-30T01:00:00Z",
            run_id="RUN1",
        )


def test_metadata_includes_sec_source_provider_value() -> None:
    metadata = _metadata()

    assert metadata["source_provider"] == SEC_REPORTED_SOURCE_PROVIDER
    assert metadata["availability_quality"] == SEC_REPORTED_AVAILABILITY_QUALITY


def test_filed_date_is_extracted_from_encoded_field_name() -> None:
    assert extract_sec_filed_date(_facts()) == "2026-04-29"


def test_filed_date_is_represented_as_date_only_metadata() -> None:
    metadata = _metadata()

    assert metadata["filed_at_utc"] == "2026-04-29"
    assert metadata["filed_at_utc_precision"] == "date_only"


def test_field_source_map_includes_non_null_fields_with_sec_provenance() -> None:
    source_map = build_sec_field_source_map(
        normalized_row=_normalized_row(),
        field_to_contributing_facts={
            "revenue": [_facts()[0]],
            "cash": [_facts()[1]],
        },
    )

    assert source_map["revenue"]["source_provider"] == SEC_REPORTED_SOURCE_PROVIDER
    assert source_map["revenue"]["provenance_role"] == "PRIMARY_REPORTED"
    assert source_map["revenue"]["merge_action"] == "SEC_RETAINED"
    assert source_map["revenue"]["source_table"] == "rc_fundamental_statement_raw"
    assert source_map["cash"]["source_provider"] == SEC_REPORTED_SOURCE_PROVIDER


def test_null_normalized_fields_do_not_produce_provenance() -> None:
    source_map = build_sec_field_source_map(
        normalized_row=_normalized_row(gross_profit=None),
        field_to_contributing_facts={
            "gross_profit": [_fact("GrossProfit", 45.0)],
        },
    )

    assert "gross_profit" not in source_map


def test_ticker_normalization_matches_adapter_convention() -> None:
    metadata = build_sec_vintage_metadata(
        market="USA",
        ticker=" aapl ",
        period_end_date="2026-03-31",
        normalized_row=_normalized_row(ticker=" aapl "),
        contributing_facts=_facts(),
        available_at_utc="2026-04-30T00:00:00Z",
        ingested_at_utc="2026-04-30T01:00:00Z",
        run_id="RUN1",
    )

    assert metadata["statement_vintage_id"].startswith("sec_edgar:usa:AAPL:2026-03-31:")


def test_explicit_sec_document_id_is_used_when_available() -> None:
    facts = _facts()
    facts[0]["accession"] = "0000320193-26-000001"

    metadata = build_sec_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=_normalized_row(),
        contributing_facts=facts,
        available_at_utc="2026-04-30T00:00:00Z",
        ingested_at_utc="2026-04-30T01:00:00Z",
        run_id="RUN1",
    )

    assert metadata["source_document_id"] == "0000320193-26-000001"


def test_functions_make_no_provider_network_imports() -> None:
    import sys

    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules
    assert "urllib.request" not in sys.modules


def _metadata() -> dict[str, object]:
    return build_sec_vintage_metadata(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=_normalized_row(),
        contributing_facts=_facts(),
        available_at_utc="2026-04-30T00:00:00Z",
        ingested_at_utc="2026-04-30T01:00:00Z",
        run_id="RUN1",
        normalization_run_id="NORM1",
    )


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


def _facts() -> list[dict[str, object]]:
    return [
        _fact("Revenues", 100.0),
        _fact("CashAndCashEquivalentsAtCarryingValue", 80.0, statement_type="balance", start="NULL"),
    ]


def _fact(
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
        "run_id": "SEC_RUN1",
    }
