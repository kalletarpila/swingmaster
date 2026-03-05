from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.dual_score.production import (
    DEFAULT_MODEL_VERSION,
    DEFAULT_OSAKEDATA_DB,
    DEFAULT_TRAIN_YEAR_FROM,
    DEFAULT_TRAIN_YEAR_TO,
    compute_and_store_dual_scores_production,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute dual production scores inside RC DB and materialize source/current tables."
    )
    parser.add_argument("--rc-db", required=True, help="RC SQLite database path")
    parser.add_argument(
        "--osakedata-db",
        default=DEFAULT_OSAKEDATA_DB,
        help="Osakedata SQLite path for full_no_dow market features",
    )
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace-all", "insert-missing"],
        default="upsert",
        help="Write mode for dual source/current tables",
    )
    parser.add_argument(
        "--model-version",
        default=DEFAULT_MODEL_VERSION,
        help="Model version label for auditability",
    )
    parser.add_argument("--computed-at", default=None, help="Optional ISO8601 timestamp")
    parser.add_argument("--train-year-from", type=int, default=DEFAULT_TRAIN_YEAR_FROM)
    parser.add_argument("--train-year-to", type=int, default=DEFAULT_TRAIN_YEAR_TO)
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(str(Path(args.rc_db)))
    try:
        summary = compute_and_store_dual_scores_production(
            conn,
            osakedata_db_path=args.osakedata_db,
            mode=args.mode,
            model_version=args.model_version,
            computed_at=args.computed_at,
            train_year_from=args.train_year_from,
            train_year_to=args.train_year_to,
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
    _summary(osakedata_db=args.osakedata_db)
    _summary(mode=args.mode)
    _summary(model_version=summary.model_version)
    _summary(train_year_from=args.train_year_from)
    _summary(train_year_to=args.train_year_to)
    _summary(rows_scored=summary.rows_scored)
    _summary(rows_with_labels=summary.rows_with_labels)
    _summary(rows_train_full=summary.rows_train_full)
    _summary(rows_train_pass_only=summary.rows_train_pass_only)
    _summary(rows_train_fail10=summary.rows_train_fail10)
    _summary(rows_changed_up20_source=summary.rows_changed_up20_source)
    _summary(rows_changed_fail10_source=summary.rows_changed_fail10_source)
    _summary(rows_changed_dual_current=summary.rows_changed_dual_current)


if __name__ == "__main__":
    main()
