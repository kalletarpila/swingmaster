from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals_v2.simfin_api_statements import (
    acquire_simfin_api_statements,
    apply_simfin_api_statements,
    build_candidate_inventory,
    write_dry_run_artifacts,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Acquire/apply SimFin API quarterly statements for rc_fundamentals_v2")
    sub = parser.add_subparsers(dest="command", required=True)

    dry = sub.add_parser("dry-run", help="Build read-only candidate and request plan")
    dry.add_argument("--legacy-db", default="/home/kalle/projects/swingmaster/fundamentals_usa.db")
    dry.add_argument("--v2-db", default="/home/kalle/projects/swingmaster/rc_fundamentals_v2.db")
    dry.add_argument("--simfin-dir", default="/home/kalle/projects/swingmaster/simfin")
    dry.add_argument("--artifact-dir", required=True)
    dry.add_argument("--market", default="usa")

    acquire = sub.add_parser("acquire", help="Network acquire SimFin API statements into raw cache")
    acquire.add_argument("--v2-db", required=True)
    acquire.add_argument("--tickers", required=True, help="Comma-separated explicit ticker list")
    acquire.add_argument("--run-id", required=True)
    acquire.add_argument("--market", default="usa")
    acquire.add_argument("--max-tickers", type=int)
    acquire.add_argument("--min-start-interval-seconds", type=float, default=2.1)
    acquire.add_argument("--rate-limit-retry-delay-seconds", type=float, default=120.0)
    acquire.add_argument("--force-refresh", action="store_true")
    acquire.add_argument("--dry-run", action="store_true")

    apply = sub.add_parser("apply", help="Offline apply cached SimFin API statements into V2 canonical tables")
    apply.add_argument("--v2-db", required=True)
    apply.add_argument("--tickers", required=True, help="Comma-separated explicit ticker list")
    apply.add_argument("--run-id", required=True)
    apply.add_argument("--market", default="usa")
    apply.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "dry-run":
        inventory = build_candidate_inventory(
            legacy_db=Path(args.legacy_db),
            v2_db=Path(args.v2_db),
            simfin_dir=Path(args.simfin_dir),
            market=args.market,
        )
        result = write_dry_run_artifacts(inventory, Path(args.artifact_dir))
    elif args.command == "acquire":
        result = acquire_simfin_api_statements(
            db_path=Path(args.v2_db),
            tickers=args.tickers.split(","),
            run_id=args.run_id,
            market=args.market,
            dry_run=args.dry_run,
            force_refresh=args.force_refresh,
            max_tickers=args.max_tickers,
            min_interval_seconds=args.min_start_interval_seconds,
            rate_limit_retry_delay_seconds=args.rate_limit_retry_delay_seconds,
        )
    else:
        result = apply_simfin_api_statements(
            db_path=Path(args.v2_db),
            tickers=args.tickers.split(","),
            run_id=args.run_id,
            market=args.market,
            dry_run=args.dry_run,
        )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
