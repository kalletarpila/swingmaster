from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS


YAHOO_REPORTED_SOURCE_PROVIDER = "yahoo"
YAHOO_REPORTED_AVAILABILITY_QUALITY = "PROVIDER_OBSERVED"
YAHOO_METADATA_MODES = {
    "yahoo_quarterly_staging",
    "yahoo_to_generic_bridge",
    "yahoo_fallback_enrichment",
    "yahoo_missing_quarter_insert",
}


def build_yahoo_source_hash(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    yahoo_quarterly_row: Mapping[str, Any] | None = None,
    normalized_row: Mapping[str, Any] | None = None,
    payload_hash: str | None = None,
    enrichment_audit_rows: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    payload = {
        "market": _normalize_market(_require_text(market, "market")),
        "ticker": _require_ticker(ticker),
        "period_end_date": _require_text(period_end_date, "period_end_date"),
        "payload_hash": payload_hash,
        "normalized_row": _normalized_financial_payload(normalized_row or {}),
        "yahoo_quarterly_row": _canonical_mapping(yahoo_quarterly_row or {}),
        "enrichment_audit_rows": sorted(
            (_canonical_mapping(row) for row in (enrichment_audit_rows or ())),
            key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
        ),
    }
    return _hash_json(payload)


def build_yahoo_statement_vintage_id(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    source_hash: str,
    mode: str,
) -> str:
    normalized_mode = _require_mode(mode)
    _require_text(source_hash, "source_hash")
    return (
        f"yahoo:{normalized_mode}:{_normalize_market(_require_text(market, 'market'))}:"
        f"{_require_ticker(ticker)}:{_require_text(period_end_date, 'period_end_date')}:{source_hash[:16]}"
    )


def build_yahoo_vintage_metadata(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    normalized_row: Mapping[str, Any],
    available_at_utc: str,
    ingested_at_utc: str,
    run_id: str,
    source_hash: str,
    mode: str,
    payload_hash: str | None = None,
    source_document_id: str | None = None,
    provider_observed_at_utc: str | None = None,
    provider_run_id: str | None = None,
    normalization_run_id: str | None = None,
) -> dict[str, Any]:
    normalized_market = _normalize_market(_require_text(market, "market"))
    normalized_ticker = _require_ticker(ticker)
    period = _require_text(period_end_date, "period_end_date")
    available_at = _require_text(available_at_utc, "available_at_utc")
    ingested_at = _require_text(ingested_at_utc, "ingested_at_utc")
    _require_text(run_id, "run_id")
    _require_text(source_hash, "source_hash")
    normalized_mode = _require_mode(mode)
    if available_at == period:
        raise ValueError("REPORTED_YAHOO_VINTAGE_METADATA_INVALID_AVAILABLE_AT_PERIOD_END_DATE")

    return {
        "market": normalized_market,
        "statement_vintage_id": build_yahoo_statement_vintage_id(
            market=normalized_market,
            ticker=normalized_ticker,
            period_end_date=period,
            source_hash=source_hash,
            mode=normalized_mode,
        ),
        "source_provider": YAHOO_REPORTED_SOURCE_PROVIDER,
        "source_document_id": source_document_id,
        "source_hash": source_hash,
        "payload_hash": payload_hash,
        "revision_number": 1,
        "is_restated": 0,
        "supersedes_vintage_id": None,
        "availability_quality": YAHOO_REPORTED_AVAILABILITY_QUALITY,
        "filed_at_utc": None,
        "available_at_utc": available_at,
        "ingested_at_utc": ingested_at,
        "provider_observed_at_utc": provider_observed_at_utc,
        "run_id": run_id,
        "provider_run_id": provider_run_id,
        "normalization_run_id": normalization_run_id,
        "enrichment_run_id": run_id if normalized_mode == "yahoo_fallback_enrichment" else None,
        "created_at_utc": ingested_at,
        "updated_at_utc": None,
    }


def build_yahoo_field_source_map(
    *,
    normalized_row: Mapping[str, Any],
    yahoo_fields: set[str] | None = None,
    enrichment_audit_rows: Sequence[Mapping[str, Any]] | None = None,
    default_role: str = "FALLBACK_REPORTED",
) -> dict[str, dict[str, Any]]:
    source_map: dict[str, dict[str, Any]] = {}
    if yahoo_fields is not None:
        for field_name in sorted(yahoo_fields):
            if field_name not in REPORTED_FINANCIAL_FIELDS or normalized_row.get(field_name) is None:
                continue
            source_map[field_name] = {
                "source_provider": YAHOO_REPORTED_SOURCE_PROVIDER,
                "source_table": "rc_fundamental_yahoo_quarterly",
                "source_row_ref": _generic_source_row_ref(normalized_row),
                "source_hash": _hash_json({"field": field_name, "value": normalized_row.get(field_name)}),
                "provenance_role": default_role,
                "merge_action": "YAHOO_BRIDGED",
            }

    for audit_row in enrichment_audit_rows or ():
        if str(audit_row.get("fallback_source", "")).lower() != YAHOO_REPORTED_SOURCE_PROVIDER:
            continue
        if str(audit_row.get("enrichment_status", "")) != "FILLED_FROM_YAHOO":
            continue
        field_name = str(audit_row.get("field_name", ""))
        if field_name not in REPORTED_FINANCIAL_FIELDS or normalized_row.get(field_name) is None:
            continue
        source_map[field_name] = {
            "source_provider": YAHOO_REPORTED_SOURCE_PROVIDER,
            "source_table": "rc_fundamental_quarterly_enrichment_audit",
            "source_row_ref": _audit_source_row_ref(audit_row),
            "source_hash": _hash_json(_canonical_mapping(audit_row)),
            "provenance_role": "FALLBACK_REPORTED",
            "merge_action": "YAHOO_FILLED_MISSING",
            "old_value": audit_row.get("old_value"),
            "new_value": audit_row.get("new_value"),
            "available_at_utc": audit_row.get("created_at_utc"),
            "created_at_utc": audit_row.get("created_at_utc"),
            "run_id": audit_row.get("run_id"),
            "enrichment_run_id": audit_row.get("run_id"),
        }
    return source_map


def _normalized_financial_payload(normalized_row: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "ticker": _normalize_ticker(normalized_row.get("ticker")),
        "period_end_date": normalized_row.get("period_end_date"),
        "currency": normalized_row.get("currency"),
    }
    for field_name in REPORTED_FINANCIAL_FIELDS:
        payload[field_name] = normalized_row.get(field_name)
    return payload


def _canonical_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): row[key] for key in sorted(row, key=str)}


def _generic_source_row_ref(normalized_row: Mapping[str, Any]) -> str:
    return f"{_require_ticker(normalized_row.get('ticker'))}:{_require_text(normalized_row.get('period_end_date'), 'period_end_date')}"


def _audit_source_row_ref(audit_row: Mapping[str, Any]) -> str:
    return (
        f"{_require_ticker(audit_row.get('ticker'))}:"
        f"{_require_text(audit_row.get('period_end_date'), 'period_end_date')}:"
        f"{_require_text(audit_row.get('field_name'), 'field_name')}:"
        f"{audit_row.get('matched_yahoo_period_end_date') or 'NULL'}:"
        f"{audit_row.get('match_method') or 'NULL'}"
    )


def _hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _require_mode(value: Any) -> str:
    mode = _require_text(value, "mode")
    if mode not in YAHOO_METADATA_MODES:
        raise ValueError(f"REPORTED_YAHOO_VINTAGE_METADATA_INVALID_MODE:{mode}")
    return mode


def _require_ticker(value: Any) -> str:
    ticker = _normalize_ticker(value)
    if ticker is None:
        raise ValueError("REPORTED_YAHOO_VINTAGE_METADATA_REQUIRED_FIELD_MISSING:ticker")
    return ticker


def _normalize_ticker(value: Any) -> str | None:
    if value is None:
        return None
    ticker = str(value).strip().upper()
    if not ticker:
        return None
    return ticker


def _normalize_market(value: Any) -> str:
    return str(value).strip().lower()


def _require_text(value: Any, field_name: str) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"REPORTED_YAHOO_VINTAGE_METADATA_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()
