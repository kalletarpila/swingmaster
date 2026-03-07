from __future__ import annotations

import argparse
import os
from pathlib import Path

os.environ.setdefault("SQLITE_TMPDIR", "/tmp")
import sqlite3

from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.regime.production import DEFAULT_REGIME_VERSION
from swingmaster.research.up20_sideways_training import train_and_compare_up20_sideways


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train and compare UP20 SIDEWAYS candidates (CatBoost vs HGB)."
    )
    parser.add_argument("--rc-db", required=True, help="RC SQLite DB path")
    parser.add_argument(
        "--regime-version",
        default=DEFAULT_REGIME_VERSION,
        help="Regime version used from rc_episode_regime",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for artifacts and metadata",
    )
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
        metrics = train_and_compare_up20_sideways(
            conn,
            out_dir=Path(args.out_dir),
            regime_version=args.regime_version,
            computed_at=args.computed_at,
        )
    except sqlite3.Error as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    except RuntimeError as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    finally:
        conn.close()

    _summary(status="OK")
    _summary(rc_db=args.rc_db)
    _summary(regime_version=args.regime_version)
    _summary(out_dir=str(Path(args.out_dir).resolve()))
    _summary(n_train=metrics.n_train)
    _summary(n_valid=metrics.n_valid)
    _summary(n_test=metrics.n_test)
    _summary(pos_rate_train=metrics.pos_rate_train)
    _summary(pos_rate_valid=metrics.pos_rate_valid)
    _summary(pos_rate_test=metrics.pos_rate_test)
    _summary(feature_count=metrics.feature_count)
    _summary(auc_valid_catboost=metrics.auc_valid_catboost)
    _summary(auc_test_catboost=metrics.auc_test_catboost)
    _summary(auc_valid_hgb=metrics.auc_valid_hgb)
    _summary(auc_test_hgb=metrics.auc_test_hgb)
    _summary(selected_production_candidate=metrics.selected_production_candidate)


if __name__ == "__main__":
    main()
