from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster import SWINGMASTER_MACRO_DB_PATH
from swingmaster.infra.sqlite.migrator import apply_macro_migrations
from swingmaster.macro.scorecard import compute_and_store_risk_appetite_scorecard


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute and store daily risk appetite scorecard")
    parser.add_argument(
        "--db-path",
        required=True,
        help=f"Macro SQLite database path (expected: {SWINGMASTER_MACRO_DB_PATH})",
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace-all", "insert-missing"],
        default="upsert",
        help="Write mode for risk appetite score table",
    )
    parser.add_argument("--computed-at", default=None, help="Optional ISO8601 timestamp")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(str(Path(args.db_path)))
    try:
        apply_macro_migrations(conn)
        summary = compute_and_store_risk_appetite_scorecard(
            conn,
            date_from=args.start_date,
            date_to=args.end_date,
            mode=args.mode,
            computed_at=args.computed_at,
        )
    except sqlite3.Error as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    except RuntimeError as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    except ValueError as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    finally:
        conn.close()

    _summary(status=summary.summary_status)
    _summary(date_from=summary.date_from)
    _summary(date_to=summary.date_to)
    _summary(mode=summary.mode)
    _summary(normalized_rows_scanned=summary.normalized_rows_scanned)
    _summary(score_rows_inserted=summary.score_rows_inserted)
    _summary(score_rows_updated=summary.score_rows_updated)
    _summary(score_rows_deleted=summary.score_rows_deleted)
    _summary(score_rows_skipped=summary.score_rows_skipped)
    _summary(valid_rows_published=summary.valid_rows_published)
    _summary(missing_component_rows=summary.missing_component_rows)
    _summary(summary_status=summary.summary_status)


if __name__ == "__main__":
    main()
