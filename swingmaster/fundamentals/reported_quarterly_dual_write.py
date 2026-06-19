from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any

from swingmaster.fundamentals.build_quarterly import insert_quarterly_rows
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)


LATEST_QUARTERLY_FIELDS = (
    "ticker",
    "period_end_date",
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
    "currency",
    "run_id",
)
REPORTED_FINANCIAL_FIELDS = (
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
VINTAGE_METADATA_FIELDS = (
    "market",
    "statement_vintage_id",
    "source_provider",
    "source_document_id",
    "source_hash",
    "revision_number",
    "is_restated",
    "supersedes_vintage_id",
    "availability_quality",
    "filed_at_utc",
    "available_at_utc",
    "ingested_at_utc",
    "provider_observed_at_utc",
    "run_id",
    "provider_run_id",
    "normalization_run_id",
    "enrichment_run_id",
    "created_at_utc",
    "updated_at_utc",
)
REQUIRED_VINTAGE_METADATA_FIELDS = (
    "market",
    "statement_vintage_id",
    "source_provider",
    "available_at_utc",
    "ingested_at_utc",
    "created_at_utc",
    "run_id",
)


def build_quarterly_vintage_row_from_latest(
    row: Mapping[str, Any],
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    _require_fields(row, ("ticker", "period_end_date"))
    _require_fields(metadata, REQUIRED_VINTAGE_METADATA_FIELDS)
    normalized_ticker = _normalize_ticker(row.get("ticker"))
    if normalized_ticker is None:
        raise ValueError("REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:ticker")

    vintage_row = {
        "ticker": normalized_ticker,
        "period_end_date": row.get("period_end_date"),
        "revenue": row.get("revenue"),
        "gross_profit": row.get("gross_profit"),
        "operating_income": row.get("operating_income"),
        "ebit": row.get("ebit"),
        "ebitda": row.get("ebitda"),
        "net_income": row.get("net_income"),
        "operating_cashflow": row.get("operating_cashflow"),
        "capex": row.get("capex"),
        "free_cashflow": row.get("free_cashflow"),
        "cash": row.get("cash"),
        "total_debt": row.get("total_debt"),
        "shares_outstanding": row.get("shares_outstanding"),
        "currency": row.get("currency"),
    }
    for field_name in VINTAGE_METADATA_FIELDS:
        vintage_row[field_name] = metadata.get(field_name)
    return vintage_row


def build_field_provenance_rows(
    statement_vintage_id: str,
    row: Mapping[str, Any],
    source_provider: str,
    field_source_map: Mapping[str, Any] | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    _require_text(statement_vintage_id, "statement_vintage_id")
    _require_text(source_provider, "source_provider")
    _require_fields(row, ("ticker", "period_end_date", "market", "created_at_utc"))
    normalized_ticker = _normalize_ticker(row.get("ticker"))
    if normalized_ticker is None:
        raise ValueError("REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:ticker")

    provenance_rows: list[dict[str, Any]] = []
    for field_name in REPORTED_FINANCIAL_FIELDS:
        field_value = row.get(field_name)
        if field_value is None:
            continue
        source_info = _field_source_info(field_source_map, field_name, source_provider)
        provenance_rows.append(
            {
                "ticker": normalized_ticker,
                "market": row.get("market"),
                "period_end_date": row.get("period_end_date"),
                "statement_vintage_id": statement_vintage_id,
                "field_name": field_name,
                "field_value": field_value,
                "source_provider": source_info["source_provider"],
                "source_table": source_info.get("source_table"),
                "source_row_ref": source_info.get("source_row_ref"),
                "source_document_id": source_info.get("source_document_id", row.get("source_document_id")),
                "source_hash": source_info.get("source_hash", row.get("source_hash")),
                "provenance_role": source_info.get("provenance_role", "PRIMARY"),
                "merge_action": source_info.get("merge_action", "RETAINED_PRIMARY"),
                "old_value": source_info.get("old_value"),
                "new_value": source_info.get("new_value", field_value),
                "available_at_utc": source_info.get("available_at_utc", row.get("available_at_utc")),
                "created_at_utc": source_info.get("created_at_utc", row.get("created_at_utc")),
                "run_id": source_info.get("run_id", run_id if run_id is not None else row.get("run_id")),
                "enrichment_run_id": source_info.get("enrichment_run_id", row.get("enrichment_run_id")),
            }
        )
    return provenance_rows


def write_quarterly_latest_and_vintage(
    conn: sqlite3.Connection,
    row: Mapping[str, Any],
    metadata: Mapping[str, Any],
    field_source_map: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    vintage_row = build_quarterly_vintage_row_from_latest(row, metadata)
    latest_row = _build_latest_row(row, metadata)
    provenance_rows = build_field_provenance_rows(
        str(vintage_row["statement_vintage_id"]),
        vintage_row,
        str(vintage_row["source_provider"]),
        field_source_map=field_source_map,
        run_id=str(vintage_row["run_id"]),
    )

    latest_rows_written = insert_quarterly_rows(conn, [latest_row])
    vintage_rows_written = insert_quarterly_vintage_row(conn, vintage_row)
    provenance_rows_written = insert_quarterly_field_provenance_rows(conn, provenance_rows)
    return {
        "latest_rows_written": latest_rows_written,
        "vintage_rows_written": vintage_rows_written,
        "field_provenance_rows_written": provenance_rows_written,
    }


def _build_latest_row(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> dict[str, Any]:
    _require_fields(row, ("ticker", "period_end_date"))
    _require_fields(metadata, ("run_id",))
    normalized_ticker = _normalize_ticker(row.get("ticker"))
    if normalized_ticker is None:
        raise ValueError("REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:ticker")
    latest_row = {field_name: row.get(field_name) for field_name in LATEST_QUARTERLY_FIELDS}
    latest_row["ticker"] = normalized_ticker
    latest_row["run_id"] = metadata.get("run_id")
    return latest_row


def _field_source_info(
    field_source_map: Mapping[str, Any] | None,
    field_name: str,
    default_source_provider: str,
) -> dict[str, Any]:
    if field_source_map is None or field_name not in field_source_map:
        return {"source_provider": default_source_provider}
    field_info = field_source_map[field_name]
    if isinstance(field_info, str):
        return {"source_provider": field_info}
    if isinstance(field_info, Mapping):
        source_info = dict(field_info)
        source_info.setdefault("source_provider", default_source_provider)
        return source_info
    raise ValueError(f"REPORTED_QUARTERLY_DUAL_WRITE_INVALID_FIELD_SOURCE:{field_name}")


def _require_fields(row: Mapping[str, Any], field_names: tuple[str, ...]) -> None:
    missing = [field_name for field_name in field_names if _is_missing(row.get(field_name))]
    if missing:
        raise ValueError("REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:" + ",".join(missing))


def _require_text(value: Any, field_name: str) -> None:
    if _is_missing(value):
        raise ValueError(f"REPORTED_QUARTERLY_DUAL_WRITE_REQUIRED_FIELDS_MISSING:{field_name}")


def _normalize_ticker(value: Any) -> str | None:
    if value is None:
        return None
    ticker = str(value).strip().upper()
    if not ticker:
        return None
    return ticker


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False
