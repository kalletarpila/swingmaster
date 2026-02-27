from __future__ import annotations

import math

import numpy as np

from swingmaster.ew_score.training.fit_logistic_1d import fit_logistic_1d


def test_fit_logistic_1d_returns_finite_values() -> None:
    x_train = np.array([-2.0, -1.0, -0.5, 0.2, 0.8, 1.5, 2.0], dtype=float)
    y_train = np.array([0, 0, 0, 0, 1, 1, 1], dtype=int)
    x_test = np.array([-1.5, -0.2, 0.5, 1.8], dtype=float)
    y_test = np.array([0, 0, 1, 1], dtype=int)

    result = fit_logistic_1d(
        x_train=x_train,
        y_train=y_train,
        x_test=x_test,
        y_test=y_test,
        threshold_percentile=70,
    )

    assert math.isfinite(result.beta0)
    assert math.isfinite(result.beta1)
    assert 0.0 <= result.auc_train <= 1.0
    assert 0.0 <= result.auc_test <= 1.0
