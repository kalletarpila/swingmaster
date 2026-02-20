from __future__ import annotations

import pytest

from swingmaster.ew_score.model_config import load_model_config


def test_load_rolling_fin_model_config_values() -> None:
    cfg = load_model_config("EW_SCORE_ROLLING_V1_FIN")

    assert cfg.rule_id == "EW_SCORE_ROLLING_V1_FIN"
    assert cfg.beta0 == pytest.approx(-0.732656617080, abs=1e-12)
    assert cfg.beta1 == pytest.approx(0.172656235053, abs=1e-12)
    assert cfg.level3_score_threshold == pytest.approx(0.47, abs=1e-12)
