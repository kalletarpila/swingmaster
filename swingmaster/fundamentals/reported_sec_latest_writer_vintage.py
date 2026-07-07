from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from swingmaster.fundamentals.build_quarterly import FIELD_MAPPINGS
from swingmaster.fundamentals.reported_quarterly_dual_write import (
    REPORTED_FINANCIAL_FIELDS,
    build_field_provenance_rows,
    build_quarterly_vintage_row_from_latest,
)
from swingmaster.fundamentals.reported_sec_vintage_metadata import (
    build_sec_vintage_metadata,
)
from swingmaster.fundamentals.sec_reconstruct_quarterly import DEBT_GROUPS


SEC_PROVIDER = "sec_edgar"
UNKNOWN_PROVIDER = "unknown"
UNKNOWN_PROVENANCE_ROLE = "UNKNOWN_RETAINED"
UNKNOWN_MERGE_ACTION = "SOURCE_NOT_PROVIDED"
SEC_PROVENANCE_ROLE = "PRIMARY_REPORTED"
SEC_MERGE_ACTION = "SEC_RETAINED"
DERIVED_TOTAL_DEBT_GROUPS = (
    ("LongTermDebtCurrent", "LongTermDebtNoncurrent", "ShortTermBorrowings"),
    *tuple(tuple(group) for group in DEBT_GROUPS),
)


def build_latest_writer_sec_vintage_candidate(
    *,
    latest_row: Mapping[str, Any],
    sec_raw_rows: Sequence[Mapping[str, Any]],
    market: str,
    available_at_utc: str,
    ingested_at_utc: str,
    vintage_run_id: str,
    source_run_id: str | None = None,
) -> dict[str, Any]:
    """Build a vintage candidate from latest-writer semantics without writes.

    The normalized values intentionally come from the existing latest row. SEC
    raw rows are used only as local source evidence for metadata/provenance.
    """
    normalized = _normalized_latest_row(latest_row, vintage_run_id)
    ticker = str(normalized["ticker"])
    period_end_date = str(normalized["period_end_date"])
    raw_fact_dicts = [_row_to_dict(row) for row in sec_raw_rows]
    exact_facts = [
        row
        for row in raw_fact_dicts
        if _normalize_ticker(row.get("ticker")) == ticker
        and str(row.get("period_end_date")) == period_end_date
        and str(row.get("source", SEC_PROVIDER)) == SEC_PROVIDER
    ]
    if not exact_facts:
        raise ValueError("REPORTED_SEC_LATEST_WRITER_VINTAGE_NO_SEC_RAW")

    field_to_facts = _build_latest_writer_field_facts(normalized, exact_facts)
    contributing_facts = _flatten_facts(field_to_facts)
    metadata = build_sec_vintage_metadata(
        market=market,
        ticker=ticker,
        period_end_date=period_end_date,
        normalized_row=normalized,
        contributing_facts=contributing_facts or exact_facts,
        available_at_utc=available_at_utc,
        ingested_at_utc=ingested_at_utc,
        run_id=vintage_run_id,
        normalization_run_id=vintage_run_id,
    )
    if source_run_id is not None:
        metadata["provider_run_id"] = source_run_id

    field_source_map = _build_field_source_map(
        normalized_row=normalized,
        field_to_facts=field_to_facts,
        metadata=metadata,
    )
    vintage_row = build_quarterly_vintage_row_from_latest(normalized, metadata)
    provenance_rows = build_field_provenance_rows(
        str(metadata["statement_vintage_id"]),
        vintage_row,
        str(metadata["source_provider"]),
        field_source_map=field_source_map,
        run_id=str(metadata["run_id"]),
    )
    unknown_fields = sorted(
        row["field_name"]
        for row in provenance_rows
        if row["source_provider"] == UNKNOWN_PROVIDER
    )
    sec_fields = sorted(
        row["field_name"]
        for row in provenance_rows
        if row["source_provider"] == SEC_PROVIDER
    )
    return {
        "normalized_row": normalized,
        "metadata": metadata,
        "field_source_map": field_source_map,
        "vintage_row": vintage_row,
        "provenance_rows": provenance_rows,
        "statement_vintage_id": metadata["statement_vintage_id"],
        "source_hash": metadata["source_hash"],
        "sec_raw_fact_count": len(exact_facts),
        "sec_provenance_fields": sec_fields,
        "unknown_provenance_fields": unknown_fields,
        "unknown_provenance_count": len(unknown_fields),
        "sec_provenance_count": len(sec_fields),
    }


def _normalized_latest_row(latest_row: Mapping[str, Any], run_id: str) -> dict[str, Any]:
    ticker = _normalize_ticker(latest_row.get("ticker"))
    period_end_date = _require_text(latest_row.get("period_end_date"), "period_end_date")
    normalized = {
        "ticker": ticker,
        "period_end_date": period_end_date,
        "run_id": run_id,
        "currency": latest_row.get("currency"),
    }
    for field_name in REPORTED_FINANCIAL_FIELDS:
        normalized[field_name] = latest_row.get(field_name)
    return normalized


def _build_latest_writer_field_facts(
    normalized_row: Mapping[str, Any],
    exact_facts: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    by_field: dict[str, list[dict[str, Any]]] = {}
    for field_name, (statement_type, candidate_names) in FIELD_MAPPINGS.items():
        if normalized_row.get(field_name) is None:
            continue
        facts = _matching_facts(exact_facts, statement_type, candidate_names, expected_value=normalized_row.get(field_name))
        if facts:
            by_field[field_name] = facts

    derived_total_debt = build_sec_component_provenance_for_derived_field(
        field_name="total_debt",
        latest_value=normalized_row.get("total_debt"),
        sec_facts=exact_facts,
    )
    if derived_total_debt is not None:
        by_field["total_debt"] = derived_total_debt["facts"]

    if normalized_row.get("free_cashflow") is not None:
        fcf_facts = [*by_field.get("operating_cashflow", []), *by_field.get("capex", [])]
        operating_cashflow = normalized_row.get("operating_cashflow")
        capex = normalized_row.get("capex")
        if fcf_facts and operating_cashflow is not None and capex is not None and _values_equal(
            float(operating_cashflow) + float(capex),
            normalized_row.get("free_cashflow"),
        ):
            by_field["free_cashflow"] = _dedupe_facts(fcf_facts)

    return by_field


def build_sec_component_provenance_for_derived_field(
    *,
    field_name: str,
    latest_value: Any,
    sec_facts: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    if field_name != "total_debt" or latest_value is None:
        return None
    exact_balance_facts = [
        dict(fact)
        for fact in sec_facts
        if str(fact.get("statement_type")) == "balance"
        and str(fact.get("source", SEC_PROVIDER)) == SEC_PROVIDER
        and fact.get("field_value") is not None
    ]
    if not exact_balance_facts:
        return None

    grouped = _debt_facts_by_group_key(exact_balance_facts)
    for group in DERIVED_TOTAL_DEBT_GROUPS:
        required = set(group)
        for group_key in sorted(grouped):
            facts_by_tag = grouped[group_key]
            if not required.issubset(facts_by_tag):
                continue
            selected = [_pick_deterministic_fact(facts_by_tag[tag]) for tag in group]
            if any(fact is None for fact in selected):
                continue
            selected_facts = [fact for fact in selected if fact is not None]
            component_sum = sum(float(fact["field_value"]) for fact in selected_facts)
            if _values_equal(component_sum, latest_value):
                return {
                    "field_name": field_name,
                    "facts": _dedupe_facts(selected_facts),
                    "component_tags": list(group),
                    "component_sum": component_sum,
                    "transform_action": "SEC_COMPONENT_SUM",
                }
    return None


def _matching_facts(
    facts: Sequence[Mapping[str, Any]],
    statement_type: str,
    candidate_names: Sequence[str],
    *,
    expected_value: Any,
) -> list[dict[str, Any]]:
    candidate_set = set(candidate_names)
    matches = []
    for fact in facts:
        if str(fact.get("statement_type")) != statement_type:
            continue
        field_name = str(fact.get("field_name"))
        base_field_name = field_name.split("|", 1)[0]
        if (field_name in candidate_set or base_field_name in candidate_set) and (
            expected_value is None or _values_equal(fact.get("field_value"), expected_value)
        ):
            matches.append(dict(fact))
    return _dedupe_facts(matches)


def _debt_facts_by_group_key(
    facts: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, str, str, str], dict[str, list[dict[str, Any]]]]:
    grouped: dict[tuple[str, str, str, str, str], dict[str, list[dict[str, Any]]]] = {}
    debt_tags = {tag for group in DERIVED_TOTAL_DEBT_GROUPS for tag in group}
    for fact in facts:
        parsed = _parse_sec_fact_name(str(fact.get("field_name")))
        if parsed is None:
            continue
        tag = parsed["tag"]
        if tag not in debt_tags:
            continue
        key = (
            _normalize_ticker(fact.get("ticker")),
            str(fact.get("period_end_date")),
            parsed.get("unit", ""),
            parsed.get("fy", ""),
            parsed.get("fp", ""),
        )
        grouped.setdefault(key, {}).setdefault(tag, []).append(dict(fact))
    return grouped


def _pick_deterministic_fact(facts: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not facts:
        return None
    return sorted(
        (dict(fact) for fact in facts),
        key=lambda fact: (
            _form_priority(_parse_sec_fact_name(str(fact.get("field_name"))) or {}),
            str(fact.get("retrieved_at_utc") or ""),
            str(fact.get("run_id") or ""),
            str(fact.get("field_name") or ""),
        ),
    )[-1]


def _form_priority(parsed: Mapping[str, Any]) -> int:
    return {"10-Q": 1, "10-K": 2}.get(str(parsed.get("form")), 0)


def _parse_sec_fact_name(field_name: str) -> dict[str, str] | None:
    parts = field_name.split("|")
    if not parts:
        return None
    metadata = {"tag": parts[0]}
    for part in parts[1:]:
        if "=" not in part:
            return None
        key, value = part.split("=", 1)
        metadata[key] = value
    return metadata


def _values_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def _build_field_source_map(
    *,
    normalized_row: Mapping[str, Any],
    field_to_facts: Mapping[str, Sequence[Mapping[str, Any]]],
    metadata: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    source_map: dict[str, dict[str, Any]] = {}
    for field_name in REPORTED_FINANCIAL_FIELDS:
        if normalized_row.get(field_name) is None:
            continue
        facts = list(field_to_facts.get(field_name, ()))
        if facts:
            source_map[field_name] = {
                "source_provider": SEC_PROVIDER,
                "source_table": "rc_fundamental_statement_raw",
                "source_row_ref": "|".join(sorted(str(fact.get("field_name")) for fact in facts)),
                "source_document_id": metadata.get("source_document_id"),
                "source_hash": _hash_json([_fact_payload(fact) for fact in facts]),
                "provenance_role": SEC_PROVENANCE_ROLE,
                "merge_action": SEC_MERGE_ACTION,
            }
        else:
            source_map[field_name] = {
                "source_provider": UNKNOWN_PROVIDER,
                "source_table": None,
                "source_row_ref": None,
                "source_document_id": None,
                "source_hash": None,
                "provenance_role": UNKNOWN_PROVENANCE_ROLE,
                "merge_action": UNKNOWN_MERGE_ACTION,
            }
    return source_map


def _flatten_facts(
    field_to_facts: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[Mapping[str, Any]]:
    facts = []
    seen = set()
    for field_name in sorted(field_to_facts):
        for fact in field_to_facts[field_name]:
            key = json.dumps(_fact_payload(fact), sort_keys=True, separators=(",", ":"))
            if key in seen:
                continue
            seen.add(key)
            facts.append(fact)
    return facts


def _dedupe_facts(facts: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    deduped = []
    seen = set()
    for fact in facts:
        payload = _fact_payload(fact)
        key = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(fact))
    return sorted(deduped, key=lambda fact: str(fact.get("field_name")))


def _fact_payload(fact: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ticker": _normalize_ticker(fact.get("ticker")),
        "statement_type": fact.get("statement_type"),
        "period_end_date": fact.get("period_end_date"),
        "field_name": fact.get("field_name"),
        "field_value": fact.get("field_value"),
        "currency": fact.get("currency"),
        "source": fact.get("source"),
        "retrieved_at_utc": fact.get("retrieved_at_utc"),
        "run_id": fact.get("run_id"),
    }


def _hash_json(value: Any) -> str:
    import hashlib

    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()} if hasattr(row, "keys") else dict(row)


def _normalize_ticker(value: Any) -> str:
    text = _require_text(value, "ticker")
    return text.upper()


def _require_text(value: Any, field_name: str) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"REPORTED_SEC_LATEST_WRITER_VINTAGE_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()
