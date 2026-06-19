from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from swingmaster.fundamentals.reported_quarterly_dual_write import (
    write_normalized_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_sec_vintage_metadata import (
    build_sec_field_source_map,
    build_sec_vintage_metadata,
)


def write_sec_reconstructed_quarterly_rows_with_optional_vintage(
    conn: sqlite3.Connection,
    *,
    normalized_rows: Sequence[Mapping[str, Any]],
    contributing_facts_by_key: Mapping[tuple[Any, ...], Mapping[str, Sequence[Mapping[str, Any]]]] | None = None,
    write_vintage: bool = False,
    market: str = "usa",
    available_at_utc: str | None = None,
    ingested_at_utc: str | None = None,
    run_id: str | None = None,
    normalization_run_id: str | None = None,
    sec_metadata_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]] | None = None,
) -> dict[str, int]:
    """Opt-in SEC scaffold for writing normalized rows to latest/vintage tables.

    The default path remains latest-only. Vintage writes require explicit
    availability metadata and caller-provided SEC contributing fact fixtures.
    """
    rows = [dict(row) for row in normalized_rows]
    if not write_vintage:
        return write_normalized_quarterly_rows_with_optional_vintage(conn, rows)

    _require_text(available_at_utc, "available_at_utc")
    _require_text(ingested_at_utc, "ingested_at_utc")
    _require_text(run_id, "run_id")
    if contributing_facts_by_key is None:
        raise ValueError("REPORTED_SEC_DUAL_WRITE_CONTRIBUTING_FACTS_REQUIRED")

    vintage_metadata_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    field_source_map_by_key: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for row in rows:
        key = _ticker_period_key(row)
        field_to_facts = _resolve_contributing_facts(row, contributing_facts_by_key)
        contributing_facts = _flatten_contributing_facts(field_to_facts)
        metadata = build_sec_vintage_metadata(
            market=market,
            ticker=str(row["ticker"]),
            period_end_date=str(row["period_end_date"]),
            normalized_row=row,
            contributing_facts=contributing_facts,
            available_at_utc=str(available_at_utc),
            ingested_at_utc=str(ingested_at_utc),
            run_id=str(run_id),
            normalization_run_id=normalization_run_id,
        )
        if sec_metadata_by_key is not None:
            metadata.update(sec_metadata_by_key.get(key, {}))
        vintage_metadata_by_key[key] = metadata
        field_source_map_by_key[key] = build_sec_field_source_map(
            normalized_row=row,
            field_to_contributing_facts=field_to_facts,
        )

    return write_normalized_quarterly_rows_with_optional_vintage(
        conn,
        rows,
        write_vintage=True,
        vintage_metadata_by_key=vintage_metadata_by_key,
        field_source_map_by_key=field_source_map_by_key,
    )


def _resolve_contributing_facts(
    row: Mapping[str, Any],
    contributing_facts_by_key: Mapping[tuple[Any, ...], Mapping[str, Sequence[Mapping[str, Any]]]],
) -> Mapping[str, Sequence[Mapping[str, Any]]]:
    key = _ticker_period_key(row)
    field_to_facts = contributing_facts_by_key.get(key)
    if field_to_facts is None:
        raise ValueError("REPORTED_SEC_DUAL_WRITE_CONTRIBUTING_FACTS_MISSING:" + ",".join(key))
    if not field_to_facts:
        raise ValueError("REPORTED_SEC_DUAL_WRITE_CONTRIBUTING_FACTS_MISSING:" + ",".join(key))
    return field_to_facts


def _flatten_contributing_facts(
    field_to_facts: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[Mapping[str, Any]]:
    facts: list[Mapping[str, Any]] = []
    for field_name in sorted(field_to_facts):
        facts.extend(field_to_facts[field_name])
    if not facts:
        raise ValueError("REPORTED_SEC_DUAL_WRITE_CONTRIBUTING_FACTS_EMPTY")
    return facts


def _ticker_period_key(row: Mapping[str, Any]) -> tuple[str, str]:
    ticker = _normalize_ticker(row.get("ticker"))
    period_end_date = _require_text(row.get("period_end_date"), "period_end_date")
    if ticker is None:
        raise ValueError("REPORTED_SEC_DUAL_WRITE_REQUIRED_FIELD_MISSING:ticker")
    return ticker, period_end_date


def _normalize_ticker(value: Any) -> str | None:
    if value is None:
        return None
    ticker = str(value).strip().upper()
    if not ticker:
        return None
    return ticker


def _require_text(value: Any, field_name: str) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"REPORTED_SEC_DUAL_WRITE_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()
