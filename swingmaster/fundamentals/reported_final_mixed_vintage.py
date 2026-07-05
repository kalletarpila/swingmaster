from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS


FINAL_MIXED_SOURCE_PROVIDER = "mixed_sec_yahoo"
FINAL_MIXED_AVAILABILITY_QUALITY = "PROVIDER_FILED_OR_OBSERVED"
UNKNOWN_FIELD_SOURCE = {
    "source_provider": "unknown",
    "source_table": None,
    "source_row_ref": None,
    "source_hash": None,
    "provenance_role": "UNSPECIFIED",
    "merge_action": "SOURCE_NOT_PROVIDED",
}
YAHOO_FALLBACK_FIELD_MERGE_ACTIONS = {"YAHOO_FILLED_MISSING"}


def build_final_mixed_source_hash(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    normalized_row: Mapping[str, Any],
    sec_field_source_map: Mapping[str, Mapping[str, Any]] | None = None,
    yahoo_field_source_map: Mapping[str, Mapping[str, Any]] | None = None,
    fallback_audit_rows: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    payload = {
        "market": _normalize_market(_require_text(market, "market")),
        "ticker": _require_ticker(ticker),
        "period_end_date": _require_text(period_end_date, "period_end_date"),
        "normalized_row": _normalized_financial_payload(normalized_row),
        "sec_field_source_map": _canonical_source_map(sec_field_source_map or {}),
        "yahoo_field_source_map": _canonical_source_map(yahoo_field_source_map or {}),
        "fallback_audit_rows": sorted(
            (_canonical_mapping(row) for row in (fallback_audit_rows or ())),
            key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
        ),
    }
    return _hash_json(payload)


def build_final_mixed_statement_vintage_id(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    source_hash: str,
) -> str:
    return (
        f"{FINAL_MIXED_SOURCE_PROVIDER}:{_normalize_market(_require_text(market, 'market'))}:"
        f"{_require_ticker(ticker)}:{_require_text(period_end_date, 'period_end_date')}:"
        f"{_require_text(source_hash, 'source_hash')[:16]}"
    )


def merge_final_mixed_field_source_maps(
    *,
    normalized_row: Mapping[str, Any],
    sec_field_source_map: Mapping[str, Mapping[str, Any]] | None = None,
    yahoo_field_source_map: Mapping[str, Mapping[str, Any]] | None = None,
    unknown_policy: str = "unknown_for_unmapped_non_null",
) -> dict[str, dict[str, Any]]:
    if unknown_policy != "unknown_for_unmapped_non_null":
        raise ValueError(f"FINAL_MIXED_VINTAGE_UNKNOWN_POLICY_UNSUPPORTED:{unknown_policy}")

    sec_map = _copy_source_map(sec_field_source_map or {})
    yahoo_map = _copy_source_map(yahoo_field_source_map or {})
    merged: dict[str, dict[str, Any]] = {}

    for field_name in REPORTED_FINANCIAL_FIELDS:
        field_value = normalized_row.get(field_name)
        if field_value is None:
            continue

        sec_source = sec_map.get(field_name)
        yahoo_source = yahoo_map.get(field_name)
        if sec_source is not None and yahoo_source is not None:
            if yahoo_source.get("merge_action") in YAHOO_FALLBACK_FIELD_MERGE_ACTIONS:
                merged[field_name] = dict(yahoo_source)
                continue
            raise ValueError(f"FINAL_MIXED_VINTAGE_FIELD_SOURCE_CONFLICT:{field_name}")
        if sec_source is not None:
            merged[field_name] = dict(sec_source)
            continue
        if yahoo_source is not None:
            merged[field_name] = dict(yahoo_source)
            continue
        merged[field_name] = dict(UNKNOWN_FIELD_SOURCE)

    return merged


def build_final_mixed_vintage_metadata(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    normalized_row: Mapping[str, Any],
    source_hash: str,
    available_at_utc: str,
    ingested_at_utc: str,
    run_id: str,
    normalization_run_id: str | None = None,
) -> dict[str, Any]:
    normalized_market = _normalize_market(_require_text(market, "market"))
    normalized_ticker = _require_ticker(ticker)
    period = _require_text(period_end_date, "period_end_date")
    available_at = _require_text(available_at_utc, "available_at_utc")
    ingested_at = _require_text(ingested_at_utc, "ingested_at_utc")
    _require_text(run_id, "run_id")
    normalized_source_hash = _require_text(source_hash, "source_hash")
    if available_at == period:
        raise ValueError("FINAL_MIXED_VINTAGE_METADATA_INVALID_AVAILABLE_AT_PERIOD_END_DATE")

    return {
        "market": normalized_market,
        "statement_vintage_id": build_final_mixed_statement_vintage_id(
            market=normalized_market,
            ticker=normalized_ticker,
            period_end_date=period,
            source_hash=normalized_source_hash,
        ),
        "source_provider": FINAL_MIXED_SOURCE_PROVIDER,
        "source_document_id": None,
        "source_hash": normalized_source_hash,
        "revision_number": 1,
        "is_restated": 0,
        "supersedes_vintage_id": None,
        "availability_quality": FINAL_MIXED_AVAILABILITY_QUALITY,
        "filed_at_utc": None,
        "available_at_utc": available_at,
        "ingested_at_utc": ingested_at,
        "provider_observed_at_utc": None,
        "run_id": run_id,
        "provider_run_id": None,
        "normalization_run_id": normalization_run_id,
        "enrichment_run_id": run_id,
        "created_at_utc": ingested_at,
        "updated_at_utc": None,
    }


def _normalized_financial_payload(normalized_row: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "ticker": _normalize_ticker(normalized_row.get("ticker")),
        "period_end_date": normalized_row.get("period_end_date"),
        "currency": normalized_row.get("currency"),
    }
    for field_name in REPORTED_FINANCIAL_FIELDS:
        payload[field_name] = normalized_row.get(field_name)
    return payload


def _copy_source_map(source_map: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(field_name): dict(source_info)
        for field_name, source_info in source_map.items()
        if field_name in REPORTED_FINANCIAL_FIELDS
    }


def _canonical_source_map(source_map: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        field_name: _canonical_mapping(source_map[field_name])
        for field_name in sorted(source_map)
        if field_name in REPORTED_FINANCIAL_FIELDS
    }


def _canonical_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): row[key] for key in sorted(row, key=str)}


def _hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _require_ticker(value: Any) -> str:
    ticker = _normalize_ticker(value)
    if ticker is None:
        raise ValueError("FINAL_MIXED_VINTAGE_REQUIRED_FIELD_MISSING:ticker")
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
        raise ValueError(f"FINAL_MIXED_VINTAGE_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()
