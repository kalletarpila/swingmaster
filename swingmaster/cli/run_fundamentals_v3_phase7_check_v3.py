from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase7_check_v3 import audit, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the read-only Fundamentals V3 Phase 7 production audit.")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--artifact-root", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase7_check_v3" / utc_stamp()
    )
    summary = audit(Path(args.v3_db), Path(args.rawcandle_db), artifact_root)
    print(f"classification={summary['classification']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()

