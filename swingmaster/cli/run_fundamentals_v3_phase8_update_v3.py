from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8_update_v3 import Phase8Paths, run_phase8_diagnosis, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 8 diagnosis and bounded repair gate.")
    parser.add_argument("--phase7-root", default="temp/fundamentals_v3_phase7_check_v3/20260825T_PHASE7_CHECK_V3")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8_update_v3" / utc_stamp()
    )
    summary = run_phase8_diagnosis(
        Phase8Paths(
            phase7_root=Path(args.phase7_root),
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={artifact_root}")
    print(f"manual_requests={summary['manual_requests']}")


if __name__ == "__main__":
    main()

