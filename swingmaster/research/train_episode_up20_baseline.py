from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


DEFAULT_DB = "/tmp/swingmaster_usa_episode_rank_test.db"
DEFAULT_TABLE = "rc_episode_model_baseline_up20_60d_close"
DEFAULT_SCORES_TABLE = "rc_episode_model_baseline_up20_60d_close_scores_lr"

BASELINE_FEATURE_COLUMNS = [
    "ew_score_fastpass",
    "ew_level_fastpass",
    "exit_state_pass",
    "exit_state_no_trade",
    "buy_true",
    "ew_confirm_above_5",
    "ew_confirm_confirmed",
    "badge_penny_stock",
    "badge_low_volume",
    "badge_bull_div_last_20_days",
    "badge_det_slow_structural",
    "badge_det_slow_soft",
    "badge_det_trend_structural",
    "badge_det_trend_soft",
]

BASELINE_NO_FASTPASS_FEATURE_COLUMNS = [
    col for col in BASELINE_FEATURE_COLUMNS if col not in {"ew_score_fastpass", "ew_level_fastpass"}
]

EPISODE_FEATURE_COLUMNS = BASELINE_FEATURE_COLUMNS + [
    "close_at_entry",
    "close_at_ew_start",
    "close_at_ew_exit",
    "days_entry_to_ew_trading",
    "days_in_entry_window_trading",
    "pipe_min_sma3",
    "pipe_max_sma3",
    "pre40_min_sma5",
    "pre40_max_sma5",
    "r_entry_to_ew_start_pct",
    "r_ew_start_to_exit_pct",
    "pipe_range_pct",
    "pre40_range_pct",
]

FULL_FEATURE_COLUMNS = EPISODE_FEATURE_COLUMNS + [
    "stock_close_exit_day",
    "stock_volume_exit_day",
    "stock_r_5d_pct",
    "stock_r_10d_pct",
    "stock_r_20d_pct",
    "stock_r_60d_pct",
    "stock_vs_ma20_pct",
    "stock_vs_ma50_pct",
    "stock_vs_ma200_pct",
    "stock_volume_vs_20d",
    "gspc_r_5d_pct",
    "gspc_r_20d_pct",
    "gspc_vs_ma200_pct",
    "ndx_r_5d_pct",
    "ndx_r_20d_pct",
    "ndx_vs_ma200_pct",
    "dji_r_5d_pct",
    "dji_r_20d_pct",
    "dji_vs_ma200_pct",
    "djt_r_5d_pct",
    "djt_r_20d_pct",
    "djt_vs_ma200_pct",
]

FULL_NO_DOW_FEATURE_COLUMNS = [
    col
    for col in FULL_FEATURE_COLUMNS
    if col not in {
        "dji_r_5d_pct",
        "dji_r_20d_pct",
        "dji_vs_ma200_pct",
        "djt_r_5d_pct",
        "djt_r_20d_pct",
        "djt_vs_ma200_pct",
    }
]

FULL_NO_FASTPASS_FEATURE_COLUMNS = [
    col for col in FULL_FEATURE_COLUMNS if col not in {"ew_score_fastpass", "ew_level_fastpass"}
]

FULL_NO_DOW_NO_FASTPASS_FEATURE_COLUMNS = [
    col for col in FULL_NO_DOW_FEATURE_COLUMNS if col not in {"ew_score_fastpass", "ew_level_fastpass"}
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train baseline logistic model for episode up20% in next 60 trading days."
    )
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Input feature table")
    parser.add_argument(
        "--feature-set",
        default="baseline",
        choices=[
            "baseline",
            "baseline_no_fastpass",
            "episode",
            "full",
            "full_no_fastpass",
            "full_no_dow",
            "full_no_dow_no_fastpass",
        ],
        help="Predefined feature set to train with",
    )
    parser.add_argument(
        "--scores-table",
        default=DEFAULT_SCORES_TABLE,
        help="Output table for predicted scores",
    )
    parser.add_argument(
        "--score-all-table",
        default=None,
        help="Optional table to score all rows from another SQLite table using the trained model",
    )
    parser.add_argument(
        "--score-all-output-table",
        default=None,
        help="Optional output table name for --score-all-table predictions",
    )
    parser.add_argument(
        "--model-type",
        default="logreg",
        choices=["logreg", "hgb", "catboost"],
        help="Model family to train",
    )
    parser.add_argument("--top-k", type=int, nargs="*", default=[50, 100, 200])
    return parser.parse_args()


def _resolve_feature_columns(feature_set: str) -> list[str]:
    if feature_set == "baseline":
        return BASELINE_FEATURE_COLUMNS
    if feature_set == "baseline_no_fastpass":
        return BASELINE_NO_FASTPASS_FEATURE_COLUMNS
    if feature_set == "episode":
        return EPISODE_FEATURE_COLUMNS
    if feature_set == "full":
        return FULL_FEATURE_COLUMNS
    if feature_set == "full_no_fastpass":
        return FULL_NO_FASTPASS_FEATURE_COLUMNS
    if feature_set == "full_no_dow":
        return FULL_NO_DOW_FEATURE_COLUMNS
    if feature_set == "full_no_dow_no_fastpass":
        return FULL_NO_DOW_NO_FASTPASS_FEATURE_COLUMNS
    raise ValueError(f"unknown feature_set: {feature_set}")


def _load_dataset(conn: sqlite3.Connection, table_name: str, feature_columns: Sequence[str]) -> pd.DataFrame:
    selected_columns = [
        "episode_id",
        "ticker",
        "entry_window_exit_date",
        "split_bucket",
        "label_up20_60d_close",
        *feature_columns,
    ]
    sql = f"""
        SELECT {", ".join(selected_columns)}
        FROM {table_name}
    """
    cur = conn.execute(sql)
    rows = cur.fetchall()
    df = pd.DataFrame.from_records(rows, columns=selected_columns)
    for col in ["label_up20_60d_close", *feature_columns]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values(["entry_window_exit_date", "ticker"], kind="stable").reset_index(drop=True)


def _precision_at_k(scores: Sequence[float], labels: Sequence[int], k: int) -> float:
    if k <= 0 or len(scores) == 0:
        return 0.0
    pairs = sorted(zip(scores, labels), key=lambda row: row[0], reverse=True)
    top = pairs[: min(k, len(pairs))]
    if not top:
        return 0.0
    return sum(int(label) for _, label in top) / len(top)


def _evaluate_split(
    name: str,
    frame: pd.DataFrame,
    scores: Sequence[float],
    top_k_values: Iterable[int],
) -> None:
    labels = frame["label_up20_60d_close"].astype(int).tolist()
    base_rate = sum(labels) / len(labels)
    roc_auc = roc_auc_score(labels, scores)
    ap = average_precision_score(labels, scores)
    print(
        f"SPLIT name={name} rows={len(frame)} positives={sum(labels)} "
        f"base_rate={base_rate:.4f} roc_auc={roc_auc:.4f} avg_precision={ap:.4f}"
    )
    for k in top_k_values:
        p_at_k = _precision_at_k(scores, labels, k)
        lift = (p_at_k / base_rate) if base_rate > 0 else 0.0
        print(f"TOPK split={name} k={min(k, len(frame))} precision={p_at_k:.4f} lift={lift:.4f}")


def _fit_model(train_df: pd.DataFrame, feature_columns: Sequence[str], model_type: str):
    if model_type == "catboost":
        model = CatBoostClassifier(
            iterations=500,
            depth=5,
            learning_rate=0.05,
            loss_function="Logloss",
            eval_metric="PRAUC",
            random_seed=42,
            verbose=False,
        )
        return model.fit(train_df[list(feature_columns)], train_df["label_up20_60d_close"].astype(int))
    if model_type == "hgb":
        model = HistGradientBoostingClassifier(
            learning_rate=0.05,
            max_depth=3,
            max_iter=300,
            min_samples_leaf=20,
            random_state=42,
        )
        return model.fit(train_df[list(feature_columns)], train_df["label_up20_60d_close"].astype(int))

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
                        ("scaler", StandardScaler()),
                    ]
                ),
                list(feature_columns),
            ),
        ]
    )
    model = LogisticRegression(max_iter=2000, solver="lbfgs")
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    ).fit(train_df[list(feature_columns)], train_df["label_up20_60d_close"].astype(int))


def _report_coefficients(model, feature_columns: Sequence[str], model_type: str) -> None:
    if model_type != "logreg":
        return
    lr = model.named_steps["model"]
    coefs = list(zip(feature_columns, lr.coef_[0]))
    coefs_sorted = sorted(coefs, key=lambda row: abs(row[1]), reverse=True)
    print("SECTION coefficients")
    for name, value in coefs_sorted:
        print(f"COEF feature={name} value={value:.6f}")


def _report_importances(model, feature_columns: Sequence[str], model_type: str) -> None:
    if model_type == "catboost":
        pairs = list(zip(feature_columns, model.get_feature_importance()))
        pairs_sorted = sorted(pairs, key=lambda row: row[1], reverse=True)
        print("SECTION feature_importances")
        for name, value in pairs_sorted:
            print(f"IMPORTANCE feature={name} value={value:.6f}")
        return
    if model_type != "hgb":
        return
    if not hasattr(model, "feature_importances_"):
        return
    pairs = list(zip(feature_columns, model.feature_importances_))
    pairs_sorted = sorted(pairs, key=lambda row: row[1], reverse=True)
    print("SECTION feature_importances")
    for name, value in pairs_sorted:
        print(f"IMPORTANCE feature={name} value={value:.6f}")


def _write_scores(
    conn: sqlite3.Connection,
    scores_table: str,
    all_df: pd.DataFrame,
    all_scores: Sequence[float],
) -> None:
    out = all_df[["episode_id", "ticker", "entry_window_exit_date", "split_bucket", "label_up20_60d_close"]].copy()
    out["score_lr_up20_60d_close"] = list(all_scores)
    conn.execute(f"DROP TABLE IF EXISTS {scores_table}")
    out.to_sql(scores_table, conn, index=False)
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{scores_table}_split ON {scores_table}(split_bucket)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{scores_table}_score ON {scores_table}(score_lr_up20_60d_close)"
    )
    conn.commit()


def _write_score_all(
    conn: sqlite3.Connection,
    input_table: str,
    output_table: str,
    feature_columns: Sequence[str],
    model,
) -> None:
    try:
        df = _load_dataset(conn, input_table, feature_columns)
    except Exception:
        selected_columns = [
            "episode_id",
            "ticker",
            "entry_window_exit_date",
            "split_bucket",
            "label_up20_60d_close",
            *feature_columns,
        ]
        sql = f"SELECT {', '.join(selected_columns)} FROM {input_table}"
        rows = conn.execute(sql).fetchall()
        df = pd.DataFrame.from_records(rows, columns=selected_columns)
        for col in ["label_up20_60d_close", *feature_columns]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.sort_values(["entry_window_exit_date", "ticker"], kind="stable").reset_index(drop=True)
    scores = model.predict_proba(df[list(feature_columns)])[:, 1]
    out = df[["episode_id", "ticker", "entry_window_exit_date", "split_bucket", "label_up20_60d_close"]].copy()
    out["score_pred"] = list(scores)
    conn.execute(f"DROP TABLE IF EXISTS {output_table}")
    out.to_sql(output_table, conn, index=False)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{output_table}_score ON {output_table}(score_pred)")
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{output_table}_split ON {output_table}(split_bucket)"
    )
    conn.commit()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"db not found: {db_path}")

    with sqlite3.connect(str(db_path)) as conn:
        feature_columns = _resolve_feature_columns(args.feature_set)
        df = _load_dataset(conn, args.table, feature_columns)
        train_df = df[df["split_bucket"] == "train"].copy()
        validation_df = df[df["split_bucket"] == "validation"].copy()
        test_df = df[df["split_bucket"] == "test"].copy()

        model = _fit_model(train_df, feature_columns, args.model_type)

        train_scores = model.predict_proba(train_df[list(feature_columns)])[:, 1]
        validation_scores = model.predict_proba(validation_df[list(feature_columns)])[:, 1]
        test_scores = model.predict_proba(test_df[list(feature_columns)])[:, 1]

        print(f"REPORT db={db_path}")
        print(f"REPORT table={args.table}")
        print(f"REPORT feature_set={args.feature_set}")
        print(f"REPORT model_type={args.model_type}")
        print(f"REPORT features={','.join(feature_columns)}")
        _evaluate_split("train", train_df, train_scores, args.top_k)
        _evaluate_split("validation", validation_df, validation_scores, args.top_k)
        _evaluate_split("test", test_df, test_scores, args.top_k)
        _report_coefficients(model, feature_columns, args.model_type)
        _report_importances(model, feature_columns, args.model_type)

        all_scores = model.predict_proba(df[list(feature_columns)])[:, 1]
        _write_scores(conn, args.scores_table, df, all_scores)
        print(f"REPORT scores_table={args.scores_table}")
        if args.score_all_table and args.score_all_output_table:
            _write_score_all(
                conn,
                args.score_all_table,
                args.score_all_output_table,
                feature_columns,
                model,
            )
            print(f"REPORT score_all_output_table={args.score_all_output_table}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
