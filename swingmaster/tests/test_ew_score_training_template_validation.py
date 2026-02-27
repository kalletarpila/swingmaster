from __future__ import annotations

from pathlib import Path

from swingmaster.ew_score.training.template_schema_v1 import (
    FEATURE_TYPE_PREFIX_RETURN_PCT,
    MATURITY_MODE_DAY_N_READY,
    MODEL_TYPE_LOGISTIC_1D,
    SPLIT_TYPE_TIME_BY_ENTRY_DATE,
    THRESHOLD_METHOD_TRAIN_PERCENTILE,
)
from swingmaster.ew_score.training.validate_template import load_and_validate_template


def test_ew_score_training_template_validation_fin() -> None:
    template_path = (
        Path(__file__).resolve().parents[1]
        / "ew_score"
        / "training"
        / "templates"
        / "EW_SCORE_ROLLING_V1_FIN.template.json"
    )

    template = load_and_validate_template(template_path)

    assert template.rule_id == "EW_SCORE_ROLLING_V1_FIN"
    assert template.trained_on_market == "FIN"
    assert template.feature.type == FEATURE_TYPE_PREFIX_RETURN_PCT
    assert template.feature.maturity.mode == MATURITY_MODE_DAY_N_READY
    assert template.feature.maturity.n == 3
    assert template.model.type == MODEL_TYPE_LOGISTIC_1D
    assert template.model.x == "r_prefix_pct"
    assert template.split.type == SPLIT_TYPE_TIME_BY_ENTRY_DATE
    assert template.split.train_frac == 0.8
    assert template.threshold.level3.method == THRESHOLD_METHOD_TRAIN_PERCENTILE
    assert template.threshold.level3.percentile == 70
