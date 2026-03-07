from __future__ import annotations

import argparse
import os
from pathlib import Path

os.environ.setdefault("SQLITE_TMPDIR", "/tmp")
import sqlite3

from swingmaster.episode_exit_features.production import (
    DEFAULT_OSAKEDATA_DB,
    compute_and_store_episode_exit_features,
)
from swingmaster.infra.sqlite.migrator import apply_migrations


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute and store V1 episode-exit features as-of entry_window_exit_date."
    )
    parser.add_argument("--rc-db", required=True, help="RC SQLite database path")
    parser.add_argument(
        "--osakedata-db",
        default=DEFAULT_OSAKEDATA_DB,
        help="Osakedata SQLite database path",
    )
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace-all", "insert-missing"],
        default="upsert",
        help="Write mode for rc_episode_exit_features",
    )
    parser.add_argument("--date-from", default=None, help="Optional entry_window_exit_date lower bound YYYY-MM-DD")
    parser.add_argument("--date-to", default=None, help="Optional entry_window_exit_date upper bound YYYY-MM-DD")
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
        summary = compute_and_store_episode_exit_features(
            conn,
            osakedata_db_path=args.osakedata_db,
            mode=args.mode,
            date_from=args.date_from,
            date_to=args.date_to,
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
    _summary(osakedata_db=args.osakedata_db)
    _summary(mode=args.mode)
    _summary(date_from=args.date_from)
    _summary(date_to=args.date_to)
    _summary(episodes_scanned=summary.episodes_scanned)
    _summary(inserted=summary.inserted)
    _summary(updated=summary.updated)
    _summary(skipped=summary.skipped)
    _summary(rows_written=summary.rows_written)


if __name__ == "__main__":
    main()
