from __future__ import annotations

import argparse
import os
from pathlib import Path

# Ensure SQLite temporary files are created in a writable location.
os.environ.setdefault("SQLITE_TMPDIR", "/tmp")
import sqlite3

from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.regime.production import (
    DEFAULT_CRASH_CONFIRM_DAYS,
    DEFAULT_OSAKEDATA_DB,
    DEFAULT_REGIME_VERSION,
    compute_and_store_market_regimes,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute and store market regime daily + episode mappings into RC DB."
    )
    parser.add_argument("--rc-db", required=True, help="RC SQLite database path")
    parser.add_argument(
        "--osakedata-db",
        default=DEFAULT_OSAKEDATA_DB,
        help="Osakedata SQLite path (must contain ^GSPC and ^NDX)",
    )
    parser.add_argument("--market", default="usa", help="Market code stored in regime tables")
    parser.add_argument(
        "--regime-version",
        default=DEFAULT_REGIME_VERSION,
        help="Version tag for reproducibility",
    )
    parser.add_argument(
        "--crash-confirm-days",
        type=int,
        default=DEFAULT_CRASH_CONFIRM_DAYS,
        help="Consecutive crash-candidate days required for CRASH_ALERT",
    )
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace-all", "insert-missing"],
        default="upsert",
        help="Write mode for regime tables",
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
        summary = compute_and_store_market_regimes(
            conn,
            osakedata_db_path=args.osakedata_db,
            market=args.market,
            regime_version=args.regime_version,
            crash_confirm_days=args.crash_confirm_days,
            mode=args.mode,
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
    _summary(market=summary.market)
    _summary(regime_version=summary.regime_version)
    _summary(crash_confirm_days=summary.crash_confirm_days)
    _summary(mode=args.mode)
    _summary(rows_daily_source=summary.rows_daily_source)
    _summary(rows_daily_changed=summary.rows_daily_changed)
    _summary(rows_episode_source=summary.rows_episode_source)
    _summary(rows_episode_changed=summary.rows_episode_changed)


if __name__ == "__main__":
    main()
