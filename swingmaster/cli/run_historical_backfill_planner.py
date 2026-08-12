from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from swingmaster.fundamentals.historical_backfill_planner import (
    DEFAULT_YAHOO_RECENT_TARGETS,
    build_historical_backfill_plan,
    open_readonly_db,
    plan_content_hash,
    write_planner_artifacts,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a read-only generic historical quarterly backfill plan")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", default="usa", help="Market code")
    parser.add_argument("--ticker", default=None, help="Optional single ticker")
    parser.add_argument("--tickers", default=None, help="Optional comma-separated ticker list")
    parser.add_argument("--yahoo-recent-targets", type=int, default=DEFAULT_YAHOO_RECENT_TARGETS)
    parser.add_argument("--artifact-dir", required=True, help="Output directory under temp")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_tickers(ticker: str | None, tickers: str | None) -> list[str] | None:
    if ticker and tickers:
        raise RuntimeError("HISTORICAL_BACKFILL_PLANNER_TICKER_ARGS_MUTUALLY_EXCLUSIVE")
    if ticker:
        return [ticker.strip().upper()]
    if tickers:
        return [item.strip().upper() for item in tickers.split(",") if item.strip()]
    return None


def run_historical_backfill_planner(
    *,
    db_path: Path,
    market: str,
    artifact_dir: Path,
    yahoo_recent_targets: int,
    tickers: list[str] | None = None,
) -> dict[str, object]:
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    with open_readonly_db(db_path) as conn:
        plan = build_historical_backfill_plan(
            conn,
            market=market.lower(),
            yahoo_recent_targets=yahoo_recent_targets,
            tickers=tickers,
        )
    content_hash = plan_content_hash(plan)
    metadata = {
        "db_path": str(db_path.expanduser().resolve()),
        "market": market.lower(),
        "planner_started_at_utc": started_at,
        "planner_finished_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "yahoo_recent_targets": yahoo_recent_targets,
        "tickers": tickers or [],
        "provider_calls": {"yahoo": 0, "sec": 0},
        "production_writes": 0,
        "content_hash": content_hash,
    }
    paths = write_planner_artifacts(plan, artifact_dir, metadata)
    return {
        "summary": plan["summary"],
        "metadata": metadata,
        "artifact_paths": paths,
        "content_hash": content_hash,
    }


def main() -> None:
    args = parse_args()
    result = run_historical_backfill_planner(
        db_path=Path(args.db),
        market=str(args.market),
        artifact_dir=Path(args.artifact_dir),
        yahoo_recent_targets=int(args.yahoo_recent_targets),
        tickers=resolve_tickers(args.ticker, args.tickers),
    )
    summary = result["summary"]
    print(json.dumps(summary, sort_keys=True))
    _summary(content_hash=result["content_hash"])
    _summary(artifact_dir=str(Path(args.artifact_dir)))


if __name__ == "__main__":
    main()
