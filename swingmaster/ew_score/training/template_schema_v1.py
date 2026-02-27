from __future__ import annotations

from dataclasses import dataclass

FEATURE_TYPE_PREFIX_RETURN_PCT = "PREFIX_RETURN_PCT"
MATURITY_MODE_DAY_N_READY = "DAY_N_READY"
MODEL_TYPE_LOGISTIC_1D = "LOGISTIC_1D"
SPLIT_TYPE_TIME_BY_ENTRY_DATE = "TIME_BY_ENTRY_DATE"
THRESHOLD_METHOD_TRAIN_PERCENTILE = "TRAIN_PERCENTILE"
THRESHOLD_METHOD_TARGET_SELECTION_RATE_TRAIN = "TARGET_SELECTION_RATE_TRAIN"


@dataclass(frozen=True)
class CohortConfigV1:
    name: str
    pipeline_version_filter: str
    require_columns: list[str]
    where_sql: str


@dataclass(frozen=True)
class LabelConfigV1:
    name: str
    definition: str
    sql_expr: str


@dataclass(frozen=True)
class FeatureMaturityConfigV1:
    mode: str
    n: int


@dataclass(frozen=True)
class FeatureConfigV1:
    name: str
    type: str
    maturity: FeatureMaturityConfigV1


@dataclass(frozen=True)
class ModelConfigV1:
    type: str
    x: str


@dataclass(frozen=True)
class SplitConfigV1:
    type: str
    train_frac: float


@dataclass(frozen=True)
class Level3ThresholdConfigV1:
    method: str
    percentile: int | None
    target_rate: float | None


@dataclass(frozen=True)
class ThresholdConfigV1:
    level3: Level3ThresholdConfigV1


@dataclass(frozen=True)
class EwScoreTrainingTemplateV1:
    rule_id: str
    description: str
    trained_on_market: str
    cohort: CohortConfigV1
    label: LabelConfigV1
    feature: FeatureConfigV1
    model: ModelConfigV1
    split: SplitConfigV1
    threshold: ThresholdConfigV1
