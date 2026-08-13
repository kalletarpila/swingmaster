from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals_v2.simfin_api_shares import (
    acquire_simfin_api_shares,
    apply_simfin_api_shares,
    write_dry_run_artifacts,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Acquire/apply SimFin API point-in-time shares for rc_fundamentals_v2")
    sub = parser.add_subparsers(dest="command", required=True)

    dry = sub.add_parser("dry-run", help="Build read-only shares candidate and request plan")
    dry.add_argument("--v2-db", default="/home/kalle/projects/swingmaster/rc_fundamentals_v2.db")
    dry.add_argument("--legacy-db", default="/home/kalle/projects/swingmaster/fundamentals_usa.db")
    dry.add_argument("--simfin-dir", default="/home/kalle/projects/swingmaster/simfin")
    dry.add_argument("--artifact-dir", required=True)
    dry.add_argument("--market", default="usa")

    acquire = sub.add_parser("acquire", help="Network acquire SimFin API shares into raw cache")
    acquire.add_argument("--v2-db", required=True)
    acquire.add_argument("--tickers", required=True, help="Comma-separated explicit ticker list")
    acquire.add_argument("--run-id", required=True)
    acquire.add_argument("--market", default="usa")
    acquire.add_argument("--max-tickers", type=int)
    acquire.add_argument("--min-start-interval-seconds", type=float, default=2.1)
    acquire.add_argument("--force-refresh", action="store_true")
    acquire.add_argument("--dry-run", action="store_true")

    apply = sub.add_parser("apply", help="Offline apply cached SimFin API shares into V2 canonical quarters")
    apply.add_argument("--v2-db", required=True)
    apply.add_argument("--tickers", required=True, help="Comma-separated explicit ticker list")
    apply.add_argument("--run-id", required=True)
    apply.add_argument("--market", default="usa")
    apply.add_argument("--max-age-days", type=int, default=120)
    apply.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "dry-run":
        result = write_dry_run_artifacts(
            v2_db=Path(args.v2_db),
            artifact_dir=Path(args.artifact_dir),
            market=args.market,
            legacy_db=Path(args.legacy_db),
            simfin_dir=Path(args.simfin_dir),
        )
    elif args.command == "acquire":
        result = acquire_simfin_api_shares(
            db_path=Path(args.v2_db),
            tickers=args.tickers.split(","),
            run_id=args.run_id,
            market=args.market,
            dry_run=args.dry_run,
            force_refresh=args.force_refresh,
            max_tickers=args.max_tickers,
            min_interval_seconds=args.min_start_interval_seconds,
        )
    else:
        result = apply_simfin_api_shares(
            db_path=Path(args.v2_db),
            tickers=args.tickers.split(","),
            run_id=args.run_id,
            market=args.market,
            max_age_days=args.max_age_days,
            dry_run=args.dry_run,
        )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
