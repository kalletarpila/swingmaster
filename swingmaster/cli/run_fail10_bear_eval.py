from __future__ import annotations

import argparse
import os
from pathlib import Path

os.environ.setdefault("SQLITE_TMPDIR", "/tmp")
import sqlite3

from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.regime.production import DEFAULT_REGIME_VERSION
from swingmaster.research.fail10_bear_evaluation import evaluate_fail10_bear_models


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate existing FAIL10 BEAR model artifacts (CatBoost vs HGB)."
    )
    parser.add_argument("--rc-db", required=True, help="RC SQLite DB path")
    parser.add_argument(
        "--regime-version",
        default=DEFAULT_REGIME_VERSION,
        help="Regime version used from rc_episode_regime",
    )
    parser.add_argument("--model-dir", required=True, help="Directory containing trained model artifacts")
    parser.add_argument("--out-dir", default=None, help="Optional output directory for evaluation JSON")
    parser.add_argument("--computed-at", default=None, help="Optional ISO8601 timestamp")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(str(Path(args.rc_db)))
    try:
        apply_migrations(conn)
        summary = evaluate_fail10_bear_models(
            conn,
            model_dir=Path(args.model_dir),
            out_dir=Path(args.out_dir) if args.out_dir else None,
            regime_version=args.regime_version,
            computed_at=args.computed_at,
        )
    except (sqlite3.Error, RuntimeError, OSError, ValueError) as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    finally:
        conn.close()

    _summary(status="OK")
    _summary(rc_db=args.rc_db)
    _summary(regime_version=args.regime_version)
    _summary(model_dir=str(Path(args.model_dir).resolve()))
    _summary(output_path=summary.output_path)
    _summary(n_valid=summary.n_valid)
    _summary(n_test=summary.n_test)
    _summary(pos_rate_valid=summary.pos_rate_valid)
    _summary(pos_rate_test=summary.pos_rate_test)
    _summary(auc_valid_catboost=summary.auc_valid_catboost)
    _summary(auc_test_catboost=summary.auc_test_catboost)
    _summary(precision_at_0_60_test_catboost=summary.precision_at_0_60_test_catboost)
    _summary(lift_at_0_60_test_catboost=summary.lift_at_0_60_test_catboost)
    _summary(top_10pct_fail10_rate_test_catboost=summary.top_10pct_fail10_rate_test_catboost)
    _summary(avg_growth_top_10pct_test_catboost=summary.avg_growth_top_10pct_test_catboost)
    _summary(gt_50_rate_top_10pct_test_catboost=summary.gt_50_rate_top_10pct_test_catboost)
    _summary(auc_valid_hgb=summary.auc_valid_hgb)
    _summary(auc_test_hgb=summary.auc_test_hgb)
    _summary(precision_at_0_60_test_hgb=summary.precision_at_0_60_test_hgb)
    _summary(lift_at_0_60_test_hgb=summary.lift_at_0_60_test_hgb)
    _summary(top_10pct_fail10_rate_test_hgb=summary.top_10pct_fail10_rate_test_hgb)
    _summary(avg_growth_top_10pct_test_hgb=summary.avg_growth_top_10pct_test_hgb)
    _summary(gt_50_rate_top_10pct_test_hgb=summary.gt_50_rate_top_10pct_test_hgb)
    _summary(previous_selected_production_candidate=summary.previous_selected_production_candidate)
    _summary(evaluation_recommended_candidate=summary.evaluation_recommended_candidate)


if __name__ == "__main__":
    main()
