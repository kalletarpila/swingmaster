from __future__ import annotations

import math
import tempfile
from pathlib import Path

from swingmaster.ew_score.training.compare_prev_version import (
    compute_compare_metrics,
    load_prev_rule,
)


def test_compare_prev_version_metrics() -> None:
    x_values = [-2.0, -1.0, 0.0, 1.0, 2.0, 3.0]
    y_values = [0, 0, 0, 0, 1, 1]
    split_flags = ["train", "train", "train", "test", "test", "test"]

    with tempfile.TemporaryDirectory() as tmp:
        models_dir = Path(tmp)
        prev_path = models_dir / "EW_SCORE_ROLLING_FIN_V1.json"
        prev_path.write_text(
            '{"beta0": -1.0, "beta1": 0.8, "level3_score_threshold": 0.7}',
            encoding="utf-8",
        )

        prev_rule = load_prev_rule(models_dir=models_dir, new_rule_id="EW_SCORE_ROLLING_FIN_V2")
        assert prev_rule is not None
        assert prev_rule["prev_rule_id"] == "EW_SCORE_ROLLING_FIN_V1"

        metrics = compute_compare_metrics(
            x_values=x_values,
            y_values=y_values,
            split_flags=split_flags,
            new_rule={"beta0": -0.2, "beta1": 1.0, "level3_score_threshold": 0.5},
            prev_rule=prev_rule,
            base_rate_test=2.0 / 3.0,
        )
        assert metrics["prev_available"] == 1
        assert math.isfinite(float(metrics["auc_test_prev"]))
        assert math.isfinite(float(metrics["auc_test_new"]))
        assert math.isfinite(float(metrics["delta_auc_test"]))

        no_prev = load_prev_rule(models_dir=models_dir, new_rule_id="EW_SCORE_ROLLING_FIN_V1")
        assert no_prev is None
