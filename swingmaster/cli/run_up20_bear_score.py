from __future__ import annotations

import argparse
import os
from pathlib import Path

os.environ.setdefault("SQLITE_TMPDIR", "/tmp")
import sqlite3

from swingmaster.episode_exit_features.production import DEFAULT_OSAKEDATA_DB
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.regime.production import DEFAULT_REGIME_VERSION
from swingmaster.research.up20_bear_scoring import compute_and_store_up20_bear_scores


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score selected UP20_BEAR production candidate for BEAR episodes."
    )
    parser.add_argument("--rc-db", required=True, help="RC SQLite DB path")
    parser.add_argument(
        "--regime-version",
        default=DEFAULT_REGIME_VERSION,
        help="Regime version used from rc_episode_regime",
    )
    parser.add_argument("--model-dir", required=True, help="Directory containing UP20_BEAR artifacts")
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace-all", "insert-missing"],
        default="upsert",
        help="Write mode for rc_episode_model_score",
    )
    parser.add_argument("--date-from", default=None, help="Optional entry_window_exit_date lower bound YYYY-MM-DD")
    parser.add_argument("--date-to", default=None, help="Optional entry_window_exit_date upper bound YYYY-MM-DD")
    parser.add_argument(
        "--osakedata-db",
        default=DEFAULT_OSAKEDATA_DB,
        help="Osakedata SQLite DB path used for building missing rc_episode_exit_features rows",
    )
    parser.add_argument("--scored-at", default=None, help="Optional ISO8601 timestamp")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(str(Path(args.rc_db)))
    try:
        apply_migrations(conn)
        result = compute_and_store_up20_bear_scores(
            conn,
            model_dir=Path(args.model_dir),
            regime_version=args.regime_version,
            mode=args.mode,
            date_from=args.date_from,
            date_to=args.date_to,
            osakedata_db_path=args.osakedata_db,
            scored_at=args.scored_at,
        )
    except (sqlite3.Error, RuntimeError, OSError, ValueError, FileNotFoundError) as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    finally:
        conn.close()

    _summary(status="OK")
    _summary(rc_db=args.rc_db)
    _summary(regime_version=args.regime_version)
    _summary(model_dir=str(Path(args.model_dir).resolve()))
    _summary(mode=args.mode)
    _summary(date_from=args.date_from)
    _summary(date_to=args.date_to)
    _summary(episodes_scanned=result.episodes_scanned)
    _summary(episodes_eligible=result.episodes_eligible)
    _summary(scores_inserted=result.scores_inserted)
    _summary(scores_updated=result.scores_updated)
    _summary(scores_skipped=result.scores_skipped)
    _summary(model_id=result.model_id)
    _summary(regime_used=result.regime_used)
    _summary(feature_count=result.feature_count)


if __name__ == "__main__":
    main()
