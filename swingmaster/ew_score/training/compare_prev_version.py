from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import numpy as np


_RULE_ID_RE = re.compile(r"^EW_SCORE_ROLLING_([A-Za-z0-9]+)_V([0-9]+)$")


class PrevRuleInvalidError(ValueError):
    pass


def parse_rule_id_version(rule_id: str) -> tuple[str, int]:
    match = _RULE_ID_RE.match(rule_id)
    if match is None:
        raise ValueError("Invalid canonical rule_id format")
    return match.group(1), int(match.group(2))


def load_prev_rule(models_dir: Path, new_rule_id: str) -> dict[str, Any] | None:
    market, version = parse_rule_id_version(new_rule_id)
    if version <= 1:
        return None
    prev_version = version - 1
    prev_rule_id = f"EW_SCORE_ROLLING_{market}_V{prev_version}"
    prev_path = models_dir / f"{prev_rule_id}.json"
    if not prev_path.exists():
        return None

    payload = json.loads(prev_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise PrevRuleInvalidError("Previous rule JSON must be an object")
    for key in ("beta0", "beta1", "level3_score_threshold"):
        if key not in payload:
            raise PrevRuleInvalidError(f"Missing key in previous rule: {key}")
    return {
        "prev_rule_id": prev_rule_id,
        "prev_rule_path": prev_path,
        "payload": payload,
    }


def compute_probs(beta0: float, beta1: float, x_values: list[float]) -> list[float]:
    x = np.asarray(x_values, dtype=float)
    z = beta0 + beta1 * x
    clipped = np.clip(z, -500.0, 500.0)
    probs = 1.0 / (1.0 + np.exp(-clipped))
    return probs.tolist()


def _auc_pairwise(y_true: np.ndarray, y_score: np.ndarray) -> float:
    positives = y_score[y_true == 1]
    negatives = y_score[y_true == 0]
    total = int(positives.size) * int(negatives.size)
    if total == 0:
        return 0.5
    wins = 0.0
    for pos in positives:
        for neg in negatives:
            if pos > neg:
                wins += 1.0
            elif pos == neg:
                wins += 0.5
    return float(wins / float(total))


def _auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    return _auc_pairwise(y_true, y_score)


def _selection_metrics(y_test: np.ndarray, p_test: np.ndarray, threshold: float, base_rate_test: float) -> tuple[float, float, float]:
    selected = p_test >= threshold
    selection_rate = float(np.mean(selected)) if selected.size else 0.0
    selected_count = int(np.sum(selected))
    if selected_count == 0:
        win_rate_selected = float("nan")
        uplift_selected = float("nan")
    else:
        win_rate_selected = float(np.mean(y_test[selected]))
        uplift_selected = float(win_rate_selected - base_rate_test)
    return selection_rate, win_rate_selected, uplift_selected


def compute_compare_metrics(
    x_values: list[float],
    y_values: list[int],
    split_flags: list[str],
    new_rule: dict[str, float],
    prev_rule: dict[str, Any] | None,
    base_rate_test: float,
) -> dict[str, float | int | str]:
    x = np.asarray(x_values, dtype=float)
    y = np.asarray(y_values, dtype=int)
    split = np.asarray(split_flags)

    mask_train = split == "train"
    mask_test = split == "test"
    x_train = x[mask_train]
    y_train = y[mask_train]
    x_test = x[mask_test]
    y_test = y[mask_test]

    p_train_new = np.asarray(
        compute_probs(float(new_rule["beta0"]), float(new_rule["beta1"]), x_train.tolist()),
        dtype=float,
    )
    p_test_new = np.asarray(
        compute_probs(float(new_rule["beta0"]), float(new_rule["beta1"]), x_test.tolist()),
        dtype=float,
    )
    threshold_new = float(new_rule["level3_score_threshold"])
    selection_rate_new, win_rate_selected_new, uplift_new = _selection_metrics(
        y_test=y_test,
        p_test=p_test_new,
        threshold=threshold_new,
        base_rate_test=base_rate_test,
    )
    auc_train_new = _auc(y_train, p_train_new) if y_train.size else 0.5
    auc_test_new = _auc(y_test, p_test_new) if y_test.size else 0.5

    out: dict[str, float | int | str] = {
        "prev_available": 0,
        "auc_train_new": auc_train_new,
        "auc_test_new": auc_test_new,
        "threshold_new": threshold_new,
        "selection_rate_test_new": selection_rate_new,
        "win_rate_selected_test_new": win_rate_selected_new,
        "uplift_selected_test_new": uplift_new,
    }

    if prev_rule is None:
        return out

    payload = prev_rule["payload"]
    p_train_prev = np.asarray(
        compute_probs(float(payload["beta0"]), float(payload["beta1"]), x_train.tolist()),
        dtype=float,
    )
    p_test_prev = np.asarray(
        compute_probs(float(payload["beta0"]), float(payload["beta1"]), x_test.tolist()),
        dtype=float,
    )
    threshold_prev = float(payload["level3_score_threshold"])
    selection_rate_prev, win_rate_selected_prev, uplift_prev = _selection_metrics(
        y_test=y_test,
        p_test=p_test_prev,
        threshold=threshold_prev,
        base_rate_test=base_rate_test,
    )
    auc_train_prev = _auc(y_train, p_train_prev) if y_train.size else 0.5
    auc_test_prev = _auc(y_test, p_test_prev) if y_test.size else 0.5

    out.update(
        {
            "prev_available": 1,
            "prev_rule_id": str(prev_rule["prev_rule_id"]),
            "prev_rule_path": str(Path(prev_rule["prev_rule_path"]).resolve()),
            "auc_train_prev": auc_train_prev,
            "auc_test_prev": auc_test_prev,
            "delta_auc_test": float(auc_test_new - auc_test_prev),
            "threshold_prev": threshold_prev,
            "delta_threshold": float(threshold_new - threshold_prev),
            "selection_rate_test_prev": selection_rate_prev,
            "delta_selection_rate_test": float(selection_rate_new - selection_rate_prev),
            "win_rate_selected_test_prev": win_rate_selected_prev,
            "delta_win_rate_selected_test": float(win_rate_selected_new - win_rate_selected_prev),
            "uplift_selected_test_prev": uplift_prev,
            "delta_uplift_selected_test": float(uplift_new - uplift_prev),
        }
    )
    return out
