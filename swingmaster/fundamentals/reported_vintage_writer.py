from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from typing import Any


VINTAGE_COLUMNS = (
    "ticker",
    "market",
    "period_end_date",
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
    "created_at_utc",
    "updated_at_utc",
)
FIELD_PROVENANCE_COLUMNS = (
    "ticker",
    "market",
    "period_end_date",
    "statement_vintage_id",
    "field_name",
    "field_value",
    "source_provider",
    "source_table",
    "source_row_ref",
    "source_document_id",
    "source_hash",
    "provenance_role",
    "merge_action",
    "old_value",
    "new_value",
    "available_at_utc",
    "created_at_utc",
    "run_id",
    "enrichment_run_id",
)
VINTAGE_DEFAULTS: dict[str, Any] = {
    "revision_number": 1,
    "is_restated": 0,
    "availability_quality": "ESTIMATED",
}


def insert_quarterly_vintage_row(conn: sqlite3.Connection, row: Mapping[str, Any]) -> int:
    normalized_row = _normalize_vintage_row(row)
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_vintage (
            ticker,
            market,
            period_end_date,
            statement_vintage_id,
            source_provider,
            source_document_id,
            source_hash,
            revision_number,
            is_restated,
            supersedes_vintage_id,
            availability_quality,
            filed_at_utc,
            available_at_utc,
            ingested_at_utc,
            provider_observed_at_utc,
            run_id,
            provider_run_id,
            normalization_run_id,
            enrichment_run_id,
            revenue,
            gross_profit,
            operating_income,
            ebit,
            ebitda,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            currency,
            created_at_utc,
            updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        tuple(normalized_row[column_name] for column_name in VINTAGE_COLUMNS),
    )
    return 1


def insert_quarterly_field_provenance_rows(
    conn: sqlite3.Connection,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    normalized_rows = [_normalize_field_provenance_row(row) for row in rows]
    conn.executemany(
        """
        INSERT INTO rc_fundamental_quarterly_field_provenance (
            ticker,
            market,
            period_end_date,
            statement_vintage_id,
            field_name,
            field_value,
            source_provider,
            source_table,
            source_row_ref,
            source_document_id,
            source_hash,
            provenance_role,
            merge_action,
            old_value,
            new_value,
            available_at_utc,
            created_at_utc,
            run_id,
            enrichment_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [tuple(normalized_row[column_name] for column_name in FIELD_PROVENANCE_COLUMNS) for normalized_row in normalized_rows],
    )
    return len(normalized_rows)


def _normalize_vintage_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized_row = {column_name: row.get(column_name) for column_name in VINTAGE_COLUMNS}
    normalized_row.update({column_name: row.get(column_name, default) for column_name, default in VINTAGE_DEFAULTS.items()})
    normalized_row["ticker"] = _normalize_ticker(row.get("ticker"))
    _require_fields(
        normalized_row,
        (
            "ticker",
            "period_end_date",
            "statement_vintage_id",
            "source_provider",
            "available_at_utc",
            "ingested_at_utc",
            "created_at_utc",
        ),
    )
    return normalized_row


def _normalize_field_provenance_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized_row = {column_name: row.get(column_name) for column_name in FIELD_PROVENANCE_COLUMNS}
    normalized_row["ticker"] = _normalize_ticker(row.get("ticker"))
    _require_fields(
        normalized_row,
        (
            "ticker",
            "period_end_date",
            "statement_vintage_id",
            "field_name",
            "source_provider",
            "provenance_role",
            "merge_action",
            "created_at_utc",
        ),
    )
    return normalized_row


def _normalize_ticker(value: Any) -> str | None:
    if value is None:
        return None
    ticker = str(value).strip().upper()
    if not ticker:
        return None
    return ticker


def _require_fields(row: Mapping[str, Any], field_names: tuple[str, ...]) -> None:
    missing = [field_name for field_name in field_names if _is_missing(row.get(field_name))]
    if missing:
        raise ValueError("REPORTED_VINTAGE_REQUIRED_FIELDS_MISSING:" + ",".join(missing))


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False
