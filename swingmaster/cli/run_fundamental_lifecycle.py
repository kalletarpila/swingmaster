from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.lifecycle import (
    FUND_LIFECYCLE_RULE_V1,
    LIFECYCLE_CLASSES,
    run_lifecycle_classification,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply rule-based lifecycle classification to fundamentals TTM rows")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", default=None, help="Optional ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Classify without updating lifecycle_class")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    with sqlite3.connect(str(db_path)) as conn:
        rows_classified, class_counts = run_lifecycle_classification(
            conn=conn,
            ticker=args.ticker,
            dry_run=args.dry_run,
        )

    _summary(rule_id=FUND_LIFECYCLE_RULE_V1)
    _summary(ticker=args.ticker if args.ticker is not None else "ALL")
    _summary(rows_classified=rows_classified)
    for lifecycle_class in LIFECYCLE_CLASSES:
        _summary(**{f"class_{lifecycle_class}": class_counts[lifecycle_class]})
    _summary(db_path=str(db_path))
    _summary(run_id=args.run_id)
    _summary(status="dry-run" if args.dry_run else "ok")


if __name__ == "__main__":
    main()
