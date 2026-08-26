from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a8_r1_resolution import Phase8A8Paths, run_phase8a8, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve Phase 8A8 residual R1 cases before downstream rebuild.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--a7-artifact-root", default="temp/fundamentals_v3_phase8a7_canonical_closure/20260825T183549Z")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a8_r1_resolution" / utc_stamp()
    )
    summary = run_phase8a8(
        Phase8A8Paths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            a7_artifact_root=Path(args.a7_artifact_root),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"four_case_rows_applied={summary['four_case_rows_applied']}")
    print(f"final_r1={summary['final_r1']}")
    print(f"final_r2={summary['final_r2']}")
    print(f"final_r3={summary['final_r3']}")
    print(f"next_action={summary['next_action']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()
