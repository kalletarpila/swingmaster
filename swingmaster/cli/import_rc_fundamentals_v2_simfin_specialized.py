from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals_v2.simfin_specialized import import_simfin_specialized


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import local SimFin BANK/INSURANCE quarterly data into rc_fundamentals_v2")
    parser.add_argument("--db", required=True)
    parser.add_argument("--simfin-dir", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--legacy-db", default="/home/kalle/projects/swingmaster/fundamentals_usa.db")
    parser.add_argument("--market", default="usa")
    parser.add_argument("--profile", action="append", choices=["bank", "insurance"], help="May be supplied more than once. Defaults to both.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    profiles = tuple(profile.upper() for profile in args.profile) if args.profile else ("BANK", "INSURANCE")
    result = import_simfin_specialized(
        db_path=Path(args.db),
        simfin_dir=Path(args.simfin_dir),
        artifact_dir=Path(args.artifact_dir),
        profiles=profiles,
        legacy_db=Path(args.legacy_db) if args.legacy_db else None,
        market=str(args.market).lower(),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
