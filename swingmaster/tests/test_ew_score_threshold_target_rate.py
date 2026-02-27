from __future__ import annotations

import numpy as np
import pytest

from swingmaster.ew_score.training.fit_logistic_1d import (
    calculate_level3_threshold,
    selection_rate_at_threshold,
)
from swingmaster.ew_score.training.template_schema_v1 import (
    THRESHOLD_METHOD_TARGET_SELECTION_RATE_TRAIN,
)


def test_target_selection_rate_threshold_calculation() -> None:
    scores = np.linspace(0.01, 1.0, 100)
    target_rate = 0.10

    threshold = calculate_level3_threshold(
        scores_train=scores,
        method=THRESHOLD_METHOD_TARGET_SELECTION_RATE_TRAIN,
        target_rate=target_rate,
    )
    expected = float(np.quantile(scores, 0.90))
    selection_rate = selection_rate_at_threshold(scores, threshold)

    assert threshold == pytest.approx(expected, abs=1e-12)
    assert selection_rate == pytest.approx(0.10, abs=0.02)
