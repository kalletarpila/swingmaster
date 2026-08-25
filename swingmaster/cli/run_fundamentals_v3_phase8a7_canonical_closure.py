from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a7_canonical_closure import Phase8A7Paths, run_phase8a7, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8A7 canonical repair closure before downstream rebuild.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--a6-artifact-root", default="temp/fundamentals_v3_phase8a6_safe_apply/20260825T181951Z")
    parser.add_argument("--semantic-verified-csv", default="temp/phase8_semantic_manual_check_verified.csv")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a7_canonical_closure" / utc_stamp()
    )
    summary = run_phase8a7(
        Phase8A7Paths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            a6_artifact_root=Path(args.a6_artifact_root),
            semantic_verified_csv=Path(args.semantic_verified_csv),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"five_revenue_repairs_applied={summary['five_revenue_repairs_applied']}")
    print(f"remove_from_v3_count={summary['remove_from_v3_count']}")
    print(f"r1={summary['r1']}")
    print(f"r2={summary['r2']}")
    print(f"r3={summary['r3']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()
