from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS
from swingmaster.fundamentals.sec_reconstruct_quarterly import parse_sec_field_name


SEC_REPORTED_SOURCE_PROVIDER = "sec_edgar"
SEC_REPORTED_AVAILABILITY_QUALITY = "PROVIDER_FILED_OR_OBSERVED"
SEC_FILED_DATE_PRECISION = "date_only"


def build_sec_statement_vintage_id(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    source_hash: str,
) -> str:
    _require_text(market, "market")
    normalized_ticker = _require_ticker(ticker)
    _require_text(period_end_date, "period_end_date")
    _require_text(source_hash, "source_hash")
    return f"sec_edgar:{_normalize_market(market)}:{normalized_ticker}:{period_end_date}:{source_hash[:16]}"


def build_sec_source_hash(
    *,
    ticker: str,
    period_end_date: str,
    contributing_facts: Sequence[Mapping[str, Any]],
    normalized_row: Mapping[str, Any],
) -> str:
    payload = {
        "ticker": _require_ticker(ticker),
        "period_end_date": _require_text(period_end_date, "period_end_date"),
        "normalized_row": _normalized_financial_payload(normalized_row),
        "contributing_facts": sorted(
            (_canonical_fact_payload(fact) for fact in contributing_facts),
            key=lambda fact: json.dumps(fact, sort_keys=True, separators=(",", ":")),
        ),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def build_sec_vintage_metadata(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    normalized_row: Mapping[str, Any],
    contributing_facts: Sequence[Mapping[str, Any]],
    available_at_utc: str,
    ingested_at_utc: str,
    run_id: str,
    normalization_run_id: str | None = None,
) -> dict[str, Any]:
    normalized_ticker = _require_ticker(ticker)
    normalized_market = _normalize_market(_require_text(market, "market"))
    period = _require_text(period_end_date, "period_end_date")
    available_at = _require_text(available_at_utc, "available_at_utc")
    ingested_at = _require_text(ingested_at_utc, "ingested_at_utc")
    _require_text(run_id, "run_id")
    if available_at == period:
        raise ValueError("REPORTED_SEC_VINTAGE_METADATA_INVALID_AVAILABLE_AT_PERIOD_END_DATE")

    source_hash = build_sec_source_hash(
        ticker=normalized_ticker,
        period_end_date=period,
        contributing_facts=contributing_facts,
        normalized_row=normalized_row,
    )
    filed_date = extract_sec_filed_date(contributing_facts)
    source_document_id = _source_document_id(normalized_ticker, period, source_hash, contributing_facts)
    return {
        "market": normalized_market,
        "statement_vintage_id": build_sec_statement_vintage_id(
            market=normalized_market,
            ticker=normalized_ticker,
            period_end_date=period,
            source_hash=source_hash,
        ),
        "source_provider": SEC_REPORTED_SOURCE_PROVIDER,
        "source_document_id": source_document_id,
        "source_hash": source_hash,
        "revision_number": 1,
        "is_restated": 0,
        "supersedes_vintage_id": None,
        "availability_quality": SEC_REPORTED_AVAILABILITY_QUALITY,
        "filed_at_utc": filed_date,
        "filed_at_utc_precision": SEC_FILED_DATE_PRECISION if filed_date is not None else None,
        "available_at_utc": available_at,
        "ingested_at_utc": ingested_at,
        "provider_observed_at_utc": None,
        "run_id": run_id,
        "provider_run_id": run_id,
        "normalization_run_id": normalization_run_id,
        "enrichment_run_id": None,
        "created_at_utc": ingested_at,
        "updated_at_utc": None,
    }


def build_sec_field_source_map(
    *,
    normalized_row: Mapping[str, Any],
    field_to_contributing_facts: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, dict[str, Any]]:
    source_map: dict[str, dict[str, Any]] = {}
    for field_name in REPORTED_FINANCIAL_FIELDS:
        field_value = normalized_row.get(field_name)
        facts = list(field_to_contributing_facts.get(field_name, ()))
        if field_value is None or not facts:
            continue
        fact_hash = _hash_json([_canonical_fact_payload(fact) for fact in facts])
        source_map[field_name] = {
            "source_provider": SEC_REPORTED_SOURCE_PROVIDER,
            "source_table": "rc_fundamental_statement_raw",
            "source_row_ref": _source_row_ref(facts),
            "source_document_id": _source_document_id(
                _require_ticker(normalized_row.get("ticker")),
                _require_text(normalized_row.get("period_end_date"), "period_end_date"),
                fact_hash,
                facts,
            ),
            "source_hash": fact_hash,
            "provenance_role": "PRIMARY_REPORTED",
            "merge_action": "SEC_RETAINED",
        }
    return source_map


def extract_sec_filed_date(contributing_facts: Sequence[Mapping[str, Any]]) -> str | None:
    filed_dates = sorted(
        {
            filed
            for fact in contributing_facts
            for filed in (_fact_metadata(fact).get("filed"),)
            if filed not in (None, "", "NULL")
        }
    )
    if not filed_dates:
        return None
    return filed_dates[-1]


def _canonical_fact_payload(fact: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _fact_metadata(fact)
    return {
        "ticker": _normalize_ticker(fact.get("ticker")),
        "statement_type": fact.get("statement_type"),
        "period_end_date": fact.get("period_end_date"),
        "field_name": fact.get("field_name", fact.get("encoded_field_name")),
        "field_value": fact.get("field_value"),
        "currency": fact.get("currency"),
        "source": fact.get("source"),
        "retrieved_at_utc": fact.get("retrieved_at_utc"),
        "run_id": fact.get("run_id"),
        "metadata": metadata,
    }


def _fact_metadata(fact: Mapping[str, Any]) -> dict[str, Any]:
    encoded_field_name = fact.get("encoded_field_name", fact.get("field_name"))
    if encoded_field_name is None:
        return {}
    parsed = parse_sec_field_name(str(encoded_field_name))
    if parsed is None:
        return {}
    return parsed


def _normalized_financial_payload(normalized_row: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "ticker": _normalize_ticker(normalized_row.get("ticker")),
        "period_end_date": normalized_row.get("period_end_date"),
        "currency": normalized_row.get("currency"),
    }
    for field_name in REPORTED_FINANCIAL_FIELDS:
        payload[field_name] = normalized_row.get(field_name)
    return payload


def _source_document_id(
    ticker: str,
    period_end_date: str,
    source_hash: str,
    contributing_facts: Sequence[Mapping[str, Any]],
) -> str:
    explicit_id = _first_explicit_source_document_id(contributing_facts)
    if explicit_id is not None:
        return explicit_id
    return f"sec_edgar:{ticker}:{period_end_date}:{source_hash[:16]}"


def _first_explicit_source_document_id(contributing_facts: Sequence[Mapping[str, Any]]) -> str | None:
    for fact in contributing_facts:
        for field_name in ("source_document_id", "accession", "accession_number", "adsh"):
            value = fact.get(field_name)
            if value is not None and str(value).strip():
                return str(value).strip()
    return None


def _source_row_ref(facts: Sequence[Mapping[str, Any]]) -> str:
    refs = sorted(
        str(fact.get("field_name", fact.get("encoded_field_name", _hash_json(_canonical_fact_payload(fact)))))
        for fact in facts
    )
    return "|".join(refs)


def _hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _require_ticker(value: Any) -> str:
    ticker = _normalize_ticker(value)
    if ticker is None:
        raise ValueError("REPORTED_SEC_VINTAGE_METADATA_REQUIRED_FIELD_MISSING:ticker")
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
        raise ValueError(f"REPORTED_SEC_VINTAGE_METADATA_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()
