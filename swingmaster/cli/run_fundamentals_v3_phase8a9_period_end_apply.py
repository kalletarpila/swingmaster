from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a9_period_end_apply import Phase8A9Paths, run_phase8a9, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply verified Phase 8A9 R1 period-end repairs.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--verified-csv", default="temp/phase8_period_end_R1_verified.csv")
    parser.add_argument(
        "--a8-external-queue-csv",
        default="temp/fundamentals_v3_phase8a8_r1_resolution/20260826T045013Z/external_research_queue_R1.csv",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a9_period_end_apply" / utc_stamp()
    )
    summary = run_phase8a9(
        Phase8A9Paths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            verified_csv=Path(args.verified_csv),
            a8_external_queue_csv=Path(args.a8_external_queue_csv),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"verified_rows={summary['verified_rows']}")
    print(f"frozen_repairs={summary['frozen_repairs']}")
    print(f"rows_applied={summary['rows_applied']}")
    print(f"retained_r1={summary['retained_r1']}")
    print(f"next_action={summary['next_action']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()
