from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from swingmaster.ew_score.training.template_schema_v1 import (
    FEATURE_TYPE_PREFIX_RETURN_PCT,
    MATURITY_MODE_DAY_N_READY,
    MODEL_TYPE_LOGISTIC_1D,
    SPLIT_TYPE_TIME_BY_ENTRY_DATE,
    THRESHOLD_METHOD_TARGET_SELECTION_RATE_TRAIN,
    THRESHOLD_METHOD_TRAIN_PERCENTILE,
    CohortConfigV1,
    EwScoreTrainingTemplateV1,
    FeatureConfigV1,
    FeatureMaturityConfigV1,
    LabelConfigV1,
    Level3ThresholdConfigV1,
    ModelConfigV1,
    SplitConfigV1,
    ThresholdConfigV1,
)

_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_QUOTED_STRING_RE = re.compile(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"")
_SQL_KEYWORDS = {
    "ABS",
    "AVG",
    "AND",
    "AS",
    "ASC",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "COALESCE",
    "COUNT",
    "DATE",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "FROM",
    "GLOB",
    "GROUP",
    "HAVING",
    "IN",
    "IS",
    "ISNULL",
    "JOIN",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "ROUND",
    "SELECT",
    "MIN",
    "MAX",
    "SUM",
    "THEN",
    "WHEN",
    "WHERE",
}


class TemplateUnavailableError(ValueError):
    pass


class TemplateValidationError(ValueError):
    pass


def _require_object(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise TemplateValidationError(f"Missing or invalid object field '{key}'")
    return value


def _require_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TemplateValidationError(f"Missing or invalid string field '{key}'")
    return value


def _require_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TemplateValidationError(f"Missing or invalid int field '{key}'")
    return value


def _require_float(payload: Mapping[str, Any], key: str) -> float:
    value = payload.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TemplateValidationError(f"Missing or invalid float field '{key}'")
    return float(value)


def _require_string_list(payload: Mapping[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise TemplateValidationError(f"Missing or invalid list field '{key}'")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise TemplateValidationError(f"Field '{key}' must contain non-empty strings")
        out.append(item)
    return out


def _require_enum(field: str, actual: str, expected: str) -> None:
    if actual != expected:
        raise TemplateValidationError(
            f"Unknown enum value for '{field}': '{actual}' (expected '{expected}')"
        )


def _validate_label_sql_expr(sql_expr: str) -> None:
    normalized = re.sub(r"\s+", " ", sql_expr.strip()).upper()
    if not normalized.startswith("CASE "):
        raise TemplateValidationError("label.sql_expr must be a CASE expression")
    if not normalized.endswith("END"):
        raise TemplateValidationError("label.sql_expr must terminate with END")
    if re.search(r"\bTHEN\s+[01]\b", normalized) is None:
        raise TemplateValidationError("label.sql_expr must include THEN 0/1")
    if re.search(r"\bELSE\s+[01]\b", normalized) is None:
        raise TemplateValidationError("label.sql_expr must include ELSE 0/1")


def extract_sql_identifiers(sql_expr: str) -> set[str]:
    scrubbed = _QUOTED_STRING_RE.sub(" ", sql_expr)
    identifiers: set[str] = set()
    for token in _IDENTIFIER_RE.findall(scrubbed):
        if token.upper() in _SQL_KEYWORDS:
            continue
        identifiers.add(token)
    return identifiers


def required_columns_for_template(template: EwScoreTrainingTemplateV1) -> set[str]:
    required = set(template.cohort.require_columns)
    required.update(extract_sql_identifiers(template.label.sql_expr))
    return required


def validate_template_payload(payload: Mapping[str, Any]) -> EwScoreTrainingTemplateV1:
    rule_id = _require_str(payload, "rule_id")
    description = _require_str(payload, "description")
    trained_on_market = _require_str(payload, "trained_on_market")

    cohort_obj = _require_object(payload, "cohort")
    cohort_name = _require_str(cohort_obj, "name")
    pipeline_version_filter = _require_str(cohort_obj, "pipeline_version_filter")
    require_columns = _require_string_list(cohort_obj, "require_columns")
    if "where_sql" in cohort_obj and not isinstance(cohort_obj["where_sql"], str):
        raise TemplateValidationError("cohort.where_sql must be string when provided")
    where_sql = cohort_obj.get("where_sql", "1=1")
    if not where_sql.strip():
        raise TemplateValidationError("cohort.where_sql must be non-empty when provided")

    label_obj = _require_object(payload, "label")
    label_name = _require_str(label_obj, "name")
    label_definition = _require_str(label_obj, "definition")
    label_sql_expr = _require_str(label_obj, "sql_expr")
    _validate_label_sql_expr(label_sql_expr)

    feature_obj = _require_object(payload, "feature")
    feature_name = _require_str(feature_obj, "name")
    feature_type = _require_str(feature_obj, "type")
    _require_enum("feature.type", feature_type, FEATURE_TYPE_PREFIX_RETURN_PCT)

    maturity_obj = _require_object(feature_obj, "maturity")
    maturity_mode = _require_str(maturity_obj, "mode")
    _require_enum("feature.maturity.mode", maturity_mode, MATURITY_MODE_DAY_N_READY)
    maturity_n = _require_int(maturity_obj, "n")
    if maturity_n < 0:
        raise TemplateValidationError("feature.maturity.n must be >= 0")

    model_obj = _require_object(payload, "model")
    model_type = _require_str(model_obj, "type")
    _require_enum("model.type", model_type, MODEL_TYPE_LOGISTIC_1D)
    model_x = _require_str(model_obj, "x")
    if model_x != feature_name:
        raise TemplateValidationError("model.x must equal feature.name")

    split_obj = _require_object(payload, "split")
    split_type = _require_str(split_obj, "type")
    _require_enum("split.type", split_type, SPLIT_TYPE_TIME_BY_ENTRY_DATE)
    train_frac = _require_float(split_obj, "train_frac")
    if train_frac <= 0.0 or train_frac > 1.0:
        raise TemplateValidationError("split.train_frac must be within (0, 1]")

    threshold_obj = _require_object(payload, "threshold")
    level3_obj = _require_object(threshold_obj, "level3")
    method = _require_str(level3_obj, "method")
    if method not in (
        THRESHOLD_METHOD_TRAIN_PERCENTILE,
        THRESHOLD_METHOD_TARGET_SELECTION_RATE_TRAIN,
    ):
        raise TemplateValidationError(f"Unknown enum value for 'threshold.level3.method': '{method}'")

    percentile: int | None = None
    target_rate: float | None = None
    if method == THRESHOLD_METHOD_TRAIN_PERCENTILE:
        percentile = _require_int(level3_obj, "percentile")
        if percentile < 0 or percentile > 100:
            raise TemplateValidationError("threshold.level3.percentile must be within [0, 100]")
    if method == THRESHOLD_METHOD_TARGET_SELECTION_RATE_TRAIN:
        target_rate = _require_float(level3_obj, "target_rate")
        if target_rate <= 0.0 or target_rate >= 1.0:
            raise TemplateValidationError("threshold.level3.target_rate must be within (0, 1)")

    return EwScoreTrainingTemplateV1(
        rule_id=rule_id,
        description=description,
        trained_on_market=trained_on_market,
        cohort=CohortConfigV1(
            name=cohort_name,
            pipeline_version_filter=pipeline_version_filter,
            require_columns=require_columns,
            where_sql=where_sql,
        ),
        label=LabelConfigV1(
            name=label_name,
            definition=label_definition,
            sql_expr=label_sql_expr,
        ),
        feature=FeatureConfigV1(
            name=feature_name,
            type=feature_type,
            maturity=FeatureMaturityConfigV1(mode=maturity_mode, n=maturity_n),
        ),
        model=ModelConfigV1(type=model_type, x=model_x),
        split=SplitConfigV1(type=split_type, train_frac=train_frac),
        threshold=ThresholdConfigV1(
            level3=Level3ThresholdConfigV1(
                method=method,
                percentile=percentile,
                target_rate=target_rate,
            )
        ),
    )


def load_and_validate_template(path: str | Path) -> EwScoreTrainingTemplateV1:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise TemplateUnavailableError(str(exc)) from exc
    except json.JSONDecodeError as exc:
        raise TemplateValidationError(f"Template must be valid JSON: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise TemplateValidationError("Template must be a JSON object")
    return validate_template_payload(payload)
