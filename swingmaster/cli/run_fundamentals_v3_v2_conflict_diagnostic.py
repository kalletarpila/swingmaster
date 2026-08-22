from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from swingmaster.fundamentals.v3_core_gap_diagnostic import run_v2_conflict_root_cause_diagnostic


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only Fundamentals V3/V2 conflict root-cause diagnostic.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--v2-db", type=Path, default=Path("rc_fundamentals_v2.db"))
    parser.add_argument("--legacy-db", type=Path, default=Path("fundamentals_usa.db"))
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path("temp/fundamentals_v3_phase3b_v2_conflict_diagnostic")
        / datetime.now().strftime("%Y%m%dT%H%M%S_PHASE3B_V2_CONFLICT_DIAGNOSTIC"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_v2_conflict_root_cause_diagnostic(
        v3_db=args.v3_db,
        v2_db=args.v2_db,
        legacy_db=args.legacy_db,
        artifact_root=args.artifact_root,
    )
    print(json.dumps({"artifact_root": str(args.artifact_root), **summary}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
