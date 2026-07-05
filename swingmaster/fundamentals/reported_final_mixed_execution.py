from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from swingmaster.fundamentals.reported_final_mixed_vintage import (
    build_final_mixed_source_hash,
    build_final_mixed_vintage_metadata,
    merge_final_mixed_field_source_maps,
)
from swingmaster.fundamentals.reported_quarterly_dual_write import (
    write_normalized_quarterly_rows_with_optional_vintage,
)


def execute_final_mixed_vintage_write(
    conn: sqlite3.Connection,
    *,
    normalized_row: Mapping[str, Any],
    market: str,
    available_at_utc: str,
    ingested_at_utc: str,
    run_id: str,
    sec_field_source_map: Mapping[str, Mapping[str, Any]] | None = None,
    yahoo_field_source_map: Mapping[str, Mapping[str, Any]] | None = None,
    fallback_audit_rows: Sequence[Mapping[str, Any]] | None = None,
    normalization_run_id: str | None = None,
) -> dict[str, Any]:
    """Write one final mixed vintage using an existing SQLite connection."""
    row = dict(normalized_row)
    ticker = _require_text(row.get("ticker"), "ticker").upper()
    period_end_date = _require_text(row.get("period_end_date"), "period_end_date")
    source_hash = build_final_mixed_source_hash(
        market=market,
        ticker=ticker,
        period_end_date=period_end_date,
        normalized_row=row,
        sec_field_source_map=sec_field_source_map,
        yahoo_field_source_map=yahoo_field_source_map,
        fallback_audit_rows=fallback_audit_rows,
    )
    metadata = build_final_mixed_vintage_metadata(
        market=market,
        ticker=ticker,
        period_end_date=period_end_date,
        normalized_row=row,
        source_hash=source_hash,
        available_at_utc=available_at_utc,
        ingested_at_utc=ingested_at_utc,
        run_id=run_id,
        normalization_run_id=normalization_run_id,
    )
    field_source_map = merge_final_mixed_field_source_maps(
        normalized_row=row,
        sec_field_source_map=sec_field_source_map,
        yahoo_field_source_map=yahoo_field_source_map,
    )
    key = (ticker, period_end_date)
    result = write_normalized_quarterly_rows_with_optional_vintage(
        conn,
        [row],
        write_vintage=True,
        vintage_metadata_by_key={key: metadata},
        field_source_map_by_key={key: field_source_map},
    )
    return build_final_mixed_execution_summary(
        statement_vintage_id=str(metadata["statement_vintage_id"]),
        source_hash=source_hash,
        vintage_rows_inserted=result["vintage_rows_written"],
        provenance_rows_inserted=result["field_provenance_rows_written"],
        provenance_field_count=len(field_source_map),
    )


def build_final_mixed_execution_summary(
    *,
    statement_vintage_id: str | None,
    source_hash: str | None,
    vintage_rows_inserted: int = 0,
    provenance_rows_inserted: int = 0,
    provenance_field_count: int = 0,
    skipped_noop: int = 0,
    already_known: int = 0,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "final_mixed_written": vintage_rows_inserted > 0 and error is None,
        "statement_vintage_id": statement_vintage_id,
        "source_hash": source_hash,
        "vintage_rows_inserted": vintage_rows_inserted,
        "provenance_rows_inserted": provenance_rows_inserted,
        "provenance_field_count": provenance_field_count,
        "skipped_noop": skipped_noop,
        "already_known": already_known,
        "error": error,
    }


def _require_text(value: Any, field_name: str) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"FINAL_MIXED_EXECUTION_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()
