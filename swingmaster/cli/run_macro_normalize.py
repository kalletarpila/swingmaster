from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster import SWINGMASTER_MACRO_DB_PATH
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.macro.normalize import normalize_macro_sources


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize macro raw observations into daily aligned source table")
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
        help="Write mode for normalized macro table",
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
        apply_migrations(conn)
        summary = normalize_macro_sources(
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
    _summary(raw_rows_scanned=summary.raw_rows_scanned)
    _summary(normalized_rows_inserted=summary.normalized_rows_inserted)
    _summary(normalized_rows_updated=summary.normalized_rows_updated)
    _summary(normalized_rows_deleted=summary.normalized_rows_deleted)
    _summary(normalized_rows_skipped=summary.normalized_rows_skipped)
    _summary(distinct_sources_normalized=summary.distinct_sources_normalized)
    _summary(summary_status=summary.summary_status)


if __name__ == "__main__":
    main()
