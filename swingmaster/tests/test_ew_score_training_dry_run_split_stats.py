from __future__ import annotations

import pytest

from swingmaster.ew_score.training.dry_run import _compute_time_split_stats


def test_compute_time_split_stats_deterministic_floor() -> None:
    labels = [1, 0, 1, 0, 1]

    n_train, n_test, base_rate_train, base_rate_test = _compute_time_split_stats(
        labels=labels,
        train_frac=0.8,
    )

    assert n_train == 4
    assert n_test == 1
    assert base_rate_train == pytest.approx(0.5, abs=1e-12)
    assert base_rate_test == pytest.approx(1.0, abs=1e-12)
