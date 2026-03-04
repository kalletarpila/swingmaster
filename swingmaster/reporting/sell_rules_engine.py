from __future__ import annotations

# Expected open_positions rows use: section, ticker, buy_date, holding_trading_days,
# plus either last_close_return or price inputs (last_close/close and buy_price).
# Price and holding-period calculations are produced elsewhere; this module only
# loads, validates, and applies deterministic sell rules.

import json
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple


ROOT = Path("/home/kalle/projects/swingmaster")
SELL_RULES_DIR = ROOT / "daily_reports" / "sell_rules"
ALLOWED_TRIGGERS = {"OPEN_POSITION"}
ALLOWED_CONDITION_KEYS = {
    "holding_trading_days_gte",
    "holding_trading_days_eq",
    "last_close_return_gte",
    "last_close_return_lte",
    "close_to_buy_return_gte",
    "close_to_buy_return_lte",
}
CONDITION_FIELD_MAP = {
    "holding_trading_days_gte": "holding_trading_days",
    "holding_trading_days_eq": "holding_trading_days",
    "last_close_return_gte": "last_close_return",
    "last_close_return_lte": "last_close_return",
    "close_to_buy_return_gte": "close_to_buy_return",
    "close_to_buy_return_lte": "close_to_buy_return",
}
CONDITION_TYPE_MAP = {
    "holding_trading_days_gte": int,
    "holding_trading_days_eq": int,
    "last_close_return_gte": (int, float),
    "last_close_return_lte": (int, float),
    "close_to_buy_return_gte": (int, float),
    "close_to_buy_return_lte": (int, float),
}


def _compare_condition(row_value: Any, op: str, threshold: Any) -> bool:
    if row_value is None:
        return False
    if op == "gte":
        return row_value >= threshold
    if op == "lte":
        return row_value <= threshold
    if op == "eq":
        return row_value == threshold
    raise ValueError(f"Unsupported condition operator: {op}")


def _resolve_condition_value(row: Dict[str, Any], condition_key: str) -> Tuple[bool, Any]:
    field_name = CONDITION_FIELD_MAP[condition_key]
    if field_name == "holding_trading_days":
        if field_name not in row:
            return False, None
        return True, row.get(field_name)

    if field_name in {"last_close_return", "close_to_buy_return"}:
        if "last_close_return" in row:
            return True, row.get("last_close_return")

        close_field_name = "last_close" if "last_close" in row else "close" if "close" in row else None
        if close_field_name is None or "buy_price" not in row:
            return False, None

        close_value = row.get(close_field_name)
        buy_price = row.get("buy_price")
        if close_value is None or buy_price in {None, 0}:
            return True, None
        return True, (float(close_value) / float(buy_price)) - 1.0

    raise ValueError(f"Unsupported condition field mapping: {field_name}")


def validate_sell_rules_config(config: Dict[str, Any], requested_market: str) -> Dict[str, Any]:
    top_level_keys = set(config.keys())
    if top_level_keys != {"market", "version", "rules"}:
        raise ValueError("Invalid sell-rules config: top-level keys must be exactly market, version, rules")
    if not isinstance(config["market"], str):
        raise ValueError("Invalid sell-rules config: market must be a string")
    if config["version"] != 1 or not isinstance(config["version"], int):
        raise ValueError("Invalid sell-rules config: version must be integer 1")
    if config["market"] != requested_market:
        raise ValueError(
            f"Invalid sell-rules config: market {config['market']} does not match requested market {requested_market}"
        )
    if not isinstance(config["rules"], list):
        raise ValueError("Invalid sell-rules config: rules must be a list")

    for rule in config["rules"]:
        if not isinstance(rule, dict):
            raise ValueError("Invalid sell-rules config: each rule must be an object")
        rule_keys = set(rule.keys())
        if rule_keys != {"rule_hit", "trigger", "conditions"}:
            raise ValueError("Invalid sell-rules config: each rule must have exactly rule_hit, trigger, conditions")
        if not isinstance(rule["rule_hit"], str) or not rule["rule_hit"]:
            raise ValueError("Invalid sell-rules config: rule_hit must be a non-empty string")
        if not isinstance(rule["trigger"], str):
            raise ValueError("Invalid sell-rules config: trigger must be a string")
        if rule["trigger"] not in ALLOWED_TRIGGERS:
            raise ValueError(f"Invalid sell-rules config: unknown trigger {rule['trigger']}")
        if not isinstance(rule["conditions"], dict):
            raise ValueError("Invalid sell-rules config: conditions must be an object")

        for condition_key, condition_value in rule["conditions"].items():
            if condition_key not in ALLOWED_CONDITION_KEYS:
                raise ValueError(f"Invalid sell-rules config: unknown condition key {condition_key}")
            expected_type = CONDITION_TYPE_MAP[condition_key]
            if isinstance(condition_value, bool) or not isinstance(condition_value, expected_type):
                raise ValueError(
                    f"Invalid sell-rules config: condition {condition_key} must be "
                    f"{'an integer' if expected_type is int else 'a number'}"
                )

    return config


def load_sell_rules_config(market: str) -> Dict[str, Any]:
    path = SELL_RULES_DIR / f"{market.lower()}.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing sell-rules file: {path}")
    config = json.loads(path.read_text(encoding="utf-8"))
    return validate_sell_rules_config(config, market)


def apply_sell_rules(
    open_positions: Sequence[Dict[str, Any]],
    config: Dict[str, Any],
    sell_section_name: str = "SELLS",
) -> Tuple[List[Dict[str, Any]], int]:
    out: List[Dict[str, Any]] = []
    missing_field_count = 0

    for rule in config["rules"]:
        trigger = rule["trigger"]
        matched_rows: List[Dict[str, Any]] = []

        for row in open_positions:
            if row.get("section") != trigger:
                continue
            if row.get("ticker") in {None, "", "(none)"}:
                continue

            ok = True
            for condition_key, condition_value in rule["conditions"].items():
                value_available, row_value = _resolve_condition_value(row, condition_key)
                if not value_available:
                    missing_field_count += 1
                    ok = False
                    break

                if condition_key.endswith("_gte"):
                    if not _compare_condition(row_value, "gte", condition_value):
                        ok = False
                        break
                elif condition_key.endswith("_lte"):
                    if not _compare_condition(row_value, "lte", condition_value):
                        ok = False
                        break
                elif condition_key.endswith("_eq"):
                    if not _compare_condition(row_value, "eq", condition_value):
                        ok = False
                        break
                else:
                    raise ValueError(f"Unsupported condition key: {condition_key}")

            if ok:
                sell_row = dict(row)
                sell_row["section"] = sell_section_name
                sell_row["rule_hit"] = rule["rule_hit"]
                matched_rows.append(sell_row)

        matched_rows.sort(key=lambda row: (str(row.get("ticker") or ""), str(row.get("buy_date") or "")))
        out.extend(matched_rows)

    return out, missing_field_count
