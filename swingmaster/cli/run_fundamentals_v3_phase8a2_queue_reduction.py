from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a2_queue_reduction import Phase8A2Paths, run_phase8a2, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reduce Fundamentals V3 Phase 8 manual evidence queue.")
    parser.add_argument("--phase8-root", default="temp/fundamentals_v3_phase8_update_v3/20260825T_PHASE8_UPDATE_V3")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a2_manual_queue_reduction" / utc_stamp()
    )
    summary = run_phase8a2(
        Phase8A2Paths(
            phase8_root=Path(args.phase8_root),
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"next_action={summary['next_action']}")
    print(f"queue_a_units={summary['queue_a_units']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()

