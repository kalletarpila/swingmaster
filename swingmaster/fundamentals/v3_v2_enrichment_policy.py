from __future__ import annotations

from typing import Any

from swingmaster.fundamentals.v3_canonical_migration import CANONICAL_FIELD_NAMES


V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = False
V2_ENRICHABLE_FIELDS = CANONICAL_FIELD_NAMES


def decide_v2_value_action(
    *,
    field_name: str,
    existing_v3_value: Any,
    v2_value: Any,
    same_quarter_confirmed: bool,
    value_equivalent: bool = False,
) -> str:
    if field_name not in V2_ENRICHABLE_FIELDS:
        raise ValueError(f"V3_V2_UNKNOWN_CANONICAL_FIELD:{field_name}")
    if not same_quarter_confirmed:
        return "BLOCK_IDENTITY_NOT_CONFIRMED"
    if v2_value is None:
        return "NOOP_V2_NULL"
    if existing_v3_value is None:
        return "FILL_NULL"
    if value_equivalent or existing_v3_value == v2_value:
        return "CONFIRM_ONLY"
    return "CONFLICT_NO_OVERWRITE"


def decide_v2_publish_date_action(
    *,
    existing_publish_date: str | None,
    v2_publish_date: str | None,
    same_quarter_confirmed: bool,
) -> str:
    if not same_quarter_confirmed:
        return "BLOCK_IDENTITY_NOT_CONFIRMED"
    if v2_publish_date is None:
        return "NOOP_V2_NULL"
    if existing_publish_date is None:
        return "FILL_NULL"
    if existing_publish_date == v2_publish_date:
        return "CONFIRM_ONLY"
    return "CONFLICT_NO_OVERWRITE"
