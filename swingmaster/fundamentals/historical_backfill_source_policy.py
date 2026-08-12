from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


SEC_PROVIDER = "sec_edgar"
YAHOO_PROVIDER = "yahoo"

SEC_SUPPORTED_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)
YAHOO_SUPPORTED_FIELDS = (
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
BACKFILL_FIELDS = (
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
CORE_COMPLETENESS_FIELDS = (
    "revenue",
    "ebit",
    "free_cashflow",
    "operating_cashflow",
    "capex",
    "cash",
    "total_debt",
    "shares_outstanding",
)


@dataclass(frozen=True)
class FieldPolicy:
    field_name: str
    sec_supported: bool
    yahoo_supported: bool
    precedence: tuple[str, ...]
    notes: str


FIELD_POLICIES: dict[str, FieldPolicy] = {
    field_name: FieldPolicy(
        field_name=field_name,
        sec_supported=field_name in SEC_SUPPORTED_FIELDS,
        yahoo_supported=field_name in YAHOO_SUPPORTED_FIELDS,
        precedence=tuple(
            provider
            for provider, supported in (
                (SEC_PROVIDER, field_name in SEC_SUPPORTED_FIELDS),
                (YAHOO_PROVIDER, field_name in YAHOO_SUPPORTED_FIELDS),
            )
            if supported
        ),
        notes=(
            "Yahoo direct EBIT only; SEC OperatingIncomeLoss is not EBIT"
            if field_name == "ebit"
            else "Yahoo direct EBITDA only; no validated SEC EBITDA"
            if field_name == "ebitda"
            else "SEC retains precedence; Yahoo fills NULL"
        ),
    )
    for field_name in BACKFILL_FIELDS
}


def sec_supported_fields() -> tuple[str, ...]:
    return SEC_SUPPORTED_FIELDS


def yahoo_supported_fields() -> tuple[str, ...]:
    return YAHOO_SUPPORTED_FIELDS


def supported_fields_for_provider(provider: str) -> tuple[str, ...]:
    normalized = provider.strip().lower()
    if normalized == SEC_PROVIDER:
        return SEC_SUPPORTED_FIELDS
    if normalized == YAHOO_PROVIDER:
        return YAHOO_SUPPORTED_FIELDS
    raise ValueError(f"HISTORICAL_BACKFILL_PROVIDER_UNSUPPORTED:{provider}")


def merge_sec_yahoo_fields(
    *,
    existing_row: Mapping[str, Any] | None = None,
    sec_row: Mapping[str, Any] | None = None,
    yahoo_row: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    existing_row = existing_row or {}
    sec_row = sec_row or {}
    yahoo_row = yahoo_row or {}
    for field_name in BACKFILL_FIELDS:
        value = existing_row.get(field_name)
        if field_name in SEC_SUPPORTED_FIELDS and sec_row.get(field_name) is not None:
            value = sec_row.get(field_name)
        if value is None and field_name in YAHOO_SUPPORTED_FIELDS and yahoo_row.get(field_name) is not None:
            value = yahoo_row.get(field_name)
        merged[field_name] = value
    return merged


def non_null_supported_fields(row: Mapping[str, Any] | None, provider: str) -> tuple[str, ...]:
    if row is None:
        return ()
    return tuple(field for field in supported_fields_for_provider(provider) if row.get(field) is not None)


def missing_core_fields(row: Mapping[str, Any]) -> tuple[str, ...]:
    missing = []
    for field in CORE_COMPLETENESS_FIELDS:
        if field == "free_cashflow" and (row.get("free_cashflow") is not None or (row.get("operating_cashflow") is not None and row.get("capex") is not None)):
            continue
        if field in {"operating_cashflow", "capex"} and row.get("free_cashflow") is not None:
            continue
        if row.get(field) is None:
            missing.append(field)
    return tuple(missing)
