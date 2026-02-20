from __future__ import annotations

import pytest

from swingmaster.ew_score.model_config import load_model_config


def test_load_ew_score_day3_v1_fin_config() -> None:
    cfg = load_model_config("EW_SCORE_DAY3_V1_FIN")

    assert cfg.beta0 == pytest.approx(-0.732656617080, abs=1e-12)
    assert cfg.beta1 == pytest.approx(0.172656235053, abs=1e-12)
    assert cfg.progressive_prefix_scoring is True
    assert cfg.auc == pytest.approx(0.614434348935, abs=1e-12)
    assert cfg.base_rate == pytest.approx(0.356410256410, abs=1e-12)
    assert cfg.n_used == 390
    assert cfg.level3_score_threshold == pytest.approx(0.47, abs=1e-12)
