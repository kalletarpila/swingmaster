from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class EwScoreModelConfig:
    rule_id: str
    description: str
    beta0: float
    beta1: float
    trained_on_market: str
    cohort: str
    n_used: int
    base_rate: float
    auc: float
    label_definition: str
    progressive_prefix_scoring: bool
    level3_score_threshold: float | None


def _models_dir() -> Path:
    return Path(__file__).resolve().parent / "models"


def _require(payload: dict[str, Any], key: str, expected_type: type) -> Any:
    if key not in payload:
        raise ValueError(f"Missing required field '{key}' in model config")
    value = payload[key]
    if expected_type is float:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"Field '{key}' must be float")
        return float(value)
    if expected_type is int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"Field '{key}' must be int")
        return value
    if not isinstance(value, expected_type):
        raise ValueError(f"Field '{key}' must be {expected_type.__name__}")
    return value


def load_model_config(rule_id: str) -> EwScoreModelConfig:
    model_path = _models_dir() / f"{rule_id}.json"
    if not model_path.exists():
        raise ValueError(f"Unknown EW score model rule_id: {rule_id}")

    payload = json.loads(model_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Model config must be a JSON object")

    loaded_rule_id = _require(payload, "rule_id", str)
    if loaded_rule_id != rule_id:
        raise ValueError(
            f"rule_id mismatch: requested '{rule_id}', config has '{loaded_rule_id}'"
        )

    level3_score_threshold: float | None = None
    if "level3_score_threshold" in payload:
        level3_score_threshold = _require(payload, "level3_score_threshold", float)
        if level3_score_threshold < 0.0 or level3_score_threshold > 1.0:
            raise ValueError("Field 'level3_score_threshold' must be within [0, 1]")

    return EwScoreModelConfig(
        rule_id=loaded_rule_id,
        description=_require(payload, "description", str),
        beta0=_require(payload, "beta0", float),
        beta1=_require(payload, "beta1", float),
        trained_on_market=_require(payload, "trained_on_market", str),
        cohort=_require(payload, "cohort", str),
        n_used=_require(payload, "n_used", int),
        base_rate=_require(payload, "base_rate", float),
        auc=_require(payload, "auc", float),
        label_definition=_require(payload, "label_definition", str),
        progressive_prefix_scoring=_require(payload, "progressive_prefix_scoring", bool),
        level3_score_threshold=level3_score_threshold,
    )
