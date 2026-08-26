from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10a_sequence_collision_analysis import (
    Phase8A10APaths,
    run_phase8a10a,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze retained Phase 8A9 sequence-collision R1 cases read-only.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--a9-root", default="temp/fundamentals_v3_phase8a9_period_end_apply/20260826T052001Z")
    parser.add_argument("--verified-csv", default="temp/phase8_period_end_R1_verified.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10a_sequence_collision_analysis" / utc_stamp()
    )
    summary = run_phase8a10a(
        Phase8A10APaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
            a9_root=Path(args.a9_root),
            verified_csv=Path(args.verified_csv),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"frozen_r1={summary['frozen_r1']}")
    print(f"unique_tickers={summary['unique_tickers']}")
    print(f"collision_rows={summary['collision_rows']}")
    print(f"sequence_conflict_rows={summary['sequence_conflict_rows']}")
    print(f"production_ready_rows={summary['production_ready_rows']}")
    print(f"production_writes={summary['production_writes']}")
    print(f"rawcandle_writes={summary['rawcandle_writes']}")
    print(f"next_action={summary['next_action']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()
