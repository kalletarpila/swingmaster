from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals_v2.simfin_seed import build_rc_fundamentals_v2_from_simfin


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build clean rc_fundamentals_v2.db from local SimFin quarterly files")
    parser.add_argument("--simfin-dir", required=True)
    parser.add_argument("--output-db", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--market", default="usa")
    parser.add_argument("--legacy-db", default="/home/kalle/projects/swingmaster/fundamentals_usa.db")
    parser.add_argument("--rebuild", action="store_true", help="Allow replacing an existing output DB")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_rc_fundamentals_v2_from_simfin(
        simfin_dir=Path(args.simfin_dir),
        output_db=Path(args.output_db),
        artifact_dir=Path(args.artifact_dir),
        market=str(args.market).lower(),
        rebuild=bool(args.rebuild),
        legacy_db=Path(args.legacy_db) if args.legacy_db else None,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
