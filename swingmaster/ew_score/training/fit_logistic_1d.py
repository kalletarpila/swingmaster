from __future__ import annotations

from dataclasses import dataclass
import warnings

import numpy as np

from swingmaster.ew_score.training.template_schema_v1 import (
    THRESHOLD_METHOD_TARGET_SELECTION_RATE_TRAIN,
    THRESHOLD_METHOD_TRAIN_PERCENTILE,
)


@dataclass(frozen=True)
class Logistic1DFitResult:
    beta0: float
    beta1: float
    auc_train: float
    auc_test: float
    level3_score_threshold: float
    scores_train: np.ndarray
    scores_test: np.ndarray
    backend: str
    regularization: str


def calculate_level3_threshold(
    scores_train: np.ndarray,
    method: str,
    percentile: int | None = None,
    target_rate: float | None = None,
) -> float:
    if method == THRESHOLD_METHOD_TARGET_SELECTION_RATE_TRAIN:
        if target_rate is None:
            raise ValueError("target_rate is required for TARGET_SELECTION_RATE_TRAIN")
        return float(np.quantile(scores_train, 1.0 - target_rate))
    if method == THRESHOLD_METHOD_TRAIN_PERCENTILE:
        if percentile is None:
            raise ValueError("percentile is required for TRAIN_PERCENTILE")
        return float(np.quantile(scores_train, percentile / 100.0))
    raise ValueError(f"Unsupported threshold method: {method}")


def selection_rate_at_threshold(scores: np.ndarray, threshold: float) -> float:
    if scores.size == 0:
        return 0.0
    return float(np.mean(scores >= threshold))


def _sigmoid(z: np.ndarray) -> np.ndarray:
    clipped = np.clip(z, -500.0, 500.0)
    return 1.0 / (1.0 + np.exp(-clipped))


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
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                module=r"joblib\._multiprocessing_helpers",
            )
            from sklearn.metrics import roc_auc_score  # type: ignore

        return float(roc_auc_score(y_true, y_score))
    except Exception:
        return _auc_pairwise(y_true, y_score)


def _fit_logistic_numpy_newton(
    x: np.ndarray,
    y: np.ndarray,
    max_iter: int = 100,
    tol: float = 1e-10,
) -> tuple[float, float]:
    x_col = x.reshape(-1, 1)
    X = np.hstack([np.ones((x_col.shape[0], 1)), x_col])
    beta = np.zeros(2, dtype=float)
    ridge = 1e-8

    for _ in range(max_iter):
        z = X @ beta
        p = _sigmoid(z)
        w = p * (1.0 - p)
        hessian = (X.T * w) @ X + ridge * np.eye(2)
        grad = X.T @ (y - p)
        step = np.linalg.solve(hessian, grad)
        beta_next = beta + step
        if float(np.max(np.abs(beta_next - beta))) < tol:
            beta = beta_next
            break
        beta = beta_next
    return float(beta[0]), float(beta[1])


def fit_logistic_1d(
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_test: np.ndarray,
    y_test: np.ndarray,
    threshold_percentile: int | None = None,
    threshold_method: str = THRESHOLD_METHOD_TRAIN_PERCENTILE,
    threshold_target_rate: float | None = None,
) -> Logistic1DFitResult:
    x_train_arr = np.asarray(x_train, dtype=float)
    y_train_arr = np.asarray(y_train, dtype=int)
    x_test_arr = np.asarray(x_test, dtype=float)
    y_test_arr = np.asarray(y_test, dtype=int)

    if x_train_arr.size == 0:
        raise ValueError("x_train must not be empty")
    if y_train_arr.size != x_train_arr.size:
        raise ValueError("x_train/y_train size mismatch")
    if y_test_arr.size != x_test_arr.size:
        raise ValueError("x_test/y_test size mismatch")

    beta0: float
    beta1: float
    backend: str
    regularization: str

    try:
        import statsmodels.api as sm  # type: ignore

        X_train = sm.add_constant(x_train_arr, has_constant="add")
        fitted = sm.Logit(y_train_arr, X_train).fit(disp=0, method="lbfgs", maxiter=1000)
        beta0 = float(fitted.params[0])
        beta1 = float(fitted.params[1])
        backend = "statsmodels_logit"
        regularization = "none"
    except Exception:
        try:
            from sklearn.linear_model import LogisticRegression  # type: ignore

            try:
                clf = LogisticRegression(
                    penalty=None,
                    solver="lbfgs",
                    max_iter=1000,
                )
                regularization = "none"
            except Exception:
                clf = LogisticRegression(
                    penalty="l2",
                    C=1e12,
                    solver="lbfgs",
                    max_iter=1000,
                )
                regularization = "l2_C=1e12"
            clf.fit(x_train_arr.reshape(-1, 1), y_train_arr)
            beta0 = float(clf.intercept_[0])
            beta1 = float(clf.coef_[0][0])
            backend = "sklearn_logistic_regression"
        except Exception:
            beta0, beta1 = _fit_logistic_numpy_newton(x_train_arr, y_train_arr)
            backend = "numpy_newton"
            regularization = "ridge_1e-8"

    scores_train = _sigmoid(beta0 + beta1 * x_train_arr)
    scores_test = _sigmoid(beta0 + beta1 * x_test_arr) if x_test_arr.size else np.array([])
    auc_train = _auc(y_train_arr, scores_train)
    auc_test = _auc(y_test_arr, scores_test) if y_test_arr.size else 0.5
    level3_score_threshold = calculate_level3_threshold(
        scores_train=scores_train,
        method=threshold_method,
        percentile=threshold_percentile,
        target_rate=threshold_target_rate,
    )

    return Logistic1DFitResult(
        beta0=beta0,
        beta1=beta1,
        auc_train=auc_train,
        auc_test=auc_test,
        level3_score_threshold=level3_score_threshold,
        scores_train=scores_train,
        scores_test=scores_test,
        backend=backend,
        regularization=regularization,
    )
