from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from swingmaster.fundamentals.reported_quarterly_dual_write import (
    write_normalized_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_yahoo_vintage_metadata import (
    build_yahoo_field_source_map,
    build_yahoo_source_hash,
    build_yahoo_vintage_metadata,
)


def write_yahoo_quarterly_rows_with_optional_vintage(
    conn: sqlite3.Connection,
    *,
    normalized_rows: Sequence[Mapping[str, Any]],
    yahoo_quarterly_rows_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]] | None = None,
    payload_hash_by_key: Mapping[tuple[Any, ...], str | None] | None = None,
    write_vintage: bool = False,
    market: str = "usa",
    available_at_utc: str | None = None,
    ingested_at_utc: str | None = None,
    run_id: str | None = None,
    mode: str = "yahoo_to_generic_bridge",
    normalization_run_id: str | None = None,
) -> dict[str, int]:
    """Opt-in Yahoo bridge scaffold for latest/vintage writes."""
    rows = [dict(row) for row in normalized_rows]
    if not write_vintage:
        return write_normalized_quarterly_rows_with_optional_vintage(conn, rows)

    _require_vintage_inputs(available_at_utc=available_at_utc, ingested_at_utc=ingested_at_utc, run_id=run_id)
    if yahoo_quarterly_rows_by_key is None:
        raise ValueError("REPORTED_YAHOO_DUAL_WRITE_QUARTERLY_ROWS_REQUIRED")

    vintage_metadata_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    field_source_map_by_key: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for row in rows:
        key = _ticker_period_key(row)
        yahoo_quarterly_row = _resolve_required_mapping(
            key,
            yahoo_quarterly_rows_by_key,
            "REPORTED_YAHOO_DUAL_WRITE_QUARTERLY_ROW_MISSING",
        )
        payload_hash = _resolve_optional_value(key, payload_hash_by_key)
        source_hash = build_yahoo_source_hash(
            market=market,
            ticker=key[0],
            period_end_date=key[1],
            yahoo_quarterly_row=yahoo_quarterly_row,
            normalized_row=row,
            payload_hash=payload_hash,
        )
        vintage_metadata_by_key[key] = build_yahoo_vintage_metadata(
            market=market,
            ticker=key[0],
            period_end_date=key[1],
            normalized_row=row,
            available_at_utc=str(available_at_utc),
            ingested_at_utc=str(ingested_at_utc),
            run_id=str(run_id),
            source_hash=source_hash,
            mode=mode,
            payload_hash=payload_hash,
            provider_observed_at_utc=str(available_at_utc),
            provider_run_id=_optional_text(yahoo_quarterly_row.get("source_run_id")),
            normalization_run_id=_optional_text(normalization_run_id) or _optional_text(yahoo_quarterly_row.get("run_id")),
        )
        field_source_map_by_key[key] = build_yahoo_field_source_map(
            normalized_row=row,
            yahoo_fields=_non_null_financial_fields(row),
            default_role="PROVIDER_REPORTED",
        )

    return write_normalized_quarterly_rows_with_optional_vintage(
        conn,
        rows,
        write_vintage=True,
        vintage_metadata_by_key=vintage_metadata_by_key,
        field_source_map_by_key=field_source_map_by_key,
    )


def write_yahoo_fallback_enriched_rows_with_optional_vintage(
    conn: sqlite3.Connection,
    *,
    normalized_rows: Sequence[Mapping[str, Any]],
    enrichment_audit_rows_by_key: Mapping[tuple[Any, ...], Sequence[Mapping[str, Any]]] | None = None,
    yahoo_quarterly_rows_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]] | None = None,
    payload_hash_by_key: Mapping[tuple[Any, ...], str | None] | None = None,
    field_source_map_by_key: Mapping[tuple[Any, ...], Mapping[str, Mapping[str, Any]]] | None = None,
    write_vintage: bool = False,
    market: str = "usa",
    available_at_utc: str | None = None,
    ingested_at_utc: str | None = None,
    run_id: str | None = None,
    mode: str = "yahoo_fallback_enrichment",
    normalization_run_id: str | None = None,
) -> dict[str, int]:
    """Opt-in Yahoo fallback scaffold for latest/vintage writes."""
    rows = [dict(row) for row in normalized_rows]
    if not write_vintage:
        return write_normalized_quarterly_rows_with_optional_vintage(conn, rows)

    _require_vintage_inputs(available_at_utc=available_at_utc, ingested_at_utc=ingested_at_utc, run_id=run_id)
    if enrichment_audit_rows_by_key is None:
        raise ValueError("REPORTED_YAHOO_DUAL_WRITE_ENRICHMENT_AUDIT_ROWS_REQUIRED")

    vintage_metadata_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    merged_field_source_map_by_key: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for row in rows:
        key = _ticker_period_key(row)
        audit_rows = list(
            _resolve_required_sequence(
                key,
                enrichment_audit_rows_by_key,
                "REPORTED_YAHOO_DUAL_WRITE_ENRICHMENT_AUDIT_ROWS_MISSING",
            )
        )
        yahoo_quarterly_row = _resolve_optional_mapping(key, yahoo_quarterly_rows_by_key)
        payload_hash = _resolve_optional_value(key, payload_hash_by_key)
        source_hash = build_yahoo_source_hash(
            market=market,
            ticker=key[0],
            period_end_date=key[1],
            yahoo_quarterly_row=yahoo_quarterly_row,
            normalized_row=row,
            payload_hash=payload_hash,
            enrichment_audit_rows=audit_rows,
        )
        vintage_metadata_by_key[key] = build_yahoo_vintage_metadata(
            market=market,
            ticker=key[0],
            period_end_date=key[1],
            normalized_row=row,
            available_at_utc=str(available_at_utc),
            ingested_at_utc=str(ingested_at_utc),
            run_id=str(run_id),
            source_hash=source_hash,
            mode=mode,
            payload_hash=payload_hash,
            provider_observed_at_utc=str(available_at_utc),
            provider_run_id=_optional_text(yahoo_quarterly_row.get("source_run_id") if yahoo_quarterly_row else None),
            normalization_run_id=_optional_text(normalization_run_id)
            or _optional_text(yahoo_quarterly_row.get("run_id") if yahoo_quarterly_row else None),
        )
        yahoo_source_map = build_yahoo_field_source_map(
            normalized_row=row,
            enrichment_audit_rows=audit_rows,
        )
        explicit_source_map = dict(_resolve_optional_mapping(key, field_source_map_by_key) or {})
        merged_source_map = _merge_field_source_maps(explicit_source_map, yahoo_source_map)
        merged_field_source_map_by_key[key] = _fill_unknown_non_null_field_sources(row, merged_source_map)

    return write_normalized_quarterly_rows_with_optional_vintage(
        conn,
        rows,
        write_vintage=True,
        vintage_metadata_by_key=vintage_metadata_by_key,
        field_source_map_by_key=merged_field_source_map_by_key,
    )


def _merge_field_source_maps(
    base_map: Mapping[str, Mapping[str, Any]],
    fallback_map: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = {field_name: dict(source_info) for field_name, source_info in base_map.items()}
    for field_name, source_info in fallback_map.items():
        if field_name in merged and _source_conflicts(merged[field_name], source_info):
            raise ValueError(f"REPORTED_YAHOO_DUAL_WRITE_FIELD_SOURCE_CONFLICT:{field_name}")
        merged[field_name] = dict(source_info)
    return merged


def _source_conflicts(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return any(
        left.get(field_name) != right.get(field_name)
        for field_name in ("source_provider", "provenance_role", "merge_action")
    )


def _fill_unknown_non_null_field_sources(
    row: Mapping[str, Any],
    source_map: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    completed = {field_name: dict(source_info) for field_name, source_info in source_map.items()}
    for field_name in _non_null_financial_fields(row):
        completed.setdefault(
            field_name,
            {
                "source_provider": "unknown",
                "source_table": None,
                "source_row_ref": None,
                "source_hash": None,
                "provenance_role": "UNSPECIFIED",
                "merge_action": "SOURCE_NOT_PROVIDED",
            },
        )
    return completed


def _require_vintage_inputs(*, available_at_utc: str | None, ingested_at_utc: str | None, run_id: str | None) -> None:
    _require_text(available_at_utc, "available_at_utc")
    _require_text(ingested_at_utc, "ingested_at_utc")
    _require_text(run_id, "run_id")


def _resolve_required_mapping(
    key: tuple[str, str],
    rows_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]],
    error_code: str,
) -> Mapping[str, Any]:
    row = _resolve_optional_mapping(key, rows_by_key)
    if row is None:
        raise ValueError(f"{error_code}:{','.join(key)}")
    return row


def _resolve_required_sequence(
    key: tuple[str, str],
    rows_by_key: Mapping[tuple[Any, ...], Sequence[Mapping[str, Any]]],
    error_code: str,
) -> Sequence[Mapping[str, Any]]:
    rows = rows_by_key.get(key)
    if rows is None:
        rows = rows_by_key.get(("usa", *key))
    if not rows:
        raise ValueError(f"{error_code}:{','.join(key)}")
    return rows


def _resolve_optional_mapping(
    key: tuple[str, str],
    rows_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]] | None,
) -> Mapping[str, Any] | None:
    if rows_by_key is None:
        return None
    row = rows_by_key.get(key)
    if row is None:
        row = rows_by_key.get(("usa", *key))
    return row


def _resolve_optional_value(
    key: tuple[str, str],
    values_by_key: Mapping[tuple[Any, ...], str | None] | None,
) -> str | None:
    if values_by_key is None:
        return None
    value = values_by_key.get(key)
    if value is None:
        value = values_by_key.get(("usa", *key))
    return value


def _ticker_period_key(row: Mapping[str, Any]) -> tuple[str, str]:
    ticker = _normalize_ticker(row.get("ticker"))
    period_end_date = _require_text(row.get("period_end_date"), "period_end_date")
    if ticker is None:
        raise ValueError("REPORTED_YAHOO_DUAL_WRITE_REQUIRED_FIELD_MISSING:ticker")
    return ticker, period_end_date


def _non_null_financial_fields(row: Mapping[str, Any]) -> set[str]:
    financial_fields = (
        "revenue",
        "gross_profit",
        "operating_income",
        "ebit",
        "ebitda",
        "net_income",
        "operating_cashflow",
        "capex",
        "free_cashflow",
        "cash",
        "total_debt",
        "shares_outstanding",
    )
    return {field_name for field_name in financial_fields if row.get(field_name) is not None}


def _normalize_ticker(value: Any) -> str | None:
    if value is None:
        return None
    ticker = str(value).strip().upper()
    if not ticker:
        return None
    return ticker


def _optional_text(value: Any) -> str | None:
    if value is None or not str(value).strip():
        return None
    return str(value).strip()


def _require_text(value: Any, field_name: str) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"REPORTED_YAHOO_DUAL_WRITE_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()
