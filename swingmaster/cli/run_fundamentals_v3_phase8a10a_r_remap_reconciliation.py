from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10a_r_remap_reconciliation import (
    Phase8A10ARPaths,
    run_phase8a10a_r,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile external Phase 8A10A structural remaps against current V3.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--official-timeline-csv", default="temp/phase8_structural_R1_official_fiscal_timelines.csv")
    parser.add_argument("--case-resolution-csv", default="temp/phase8_structural_R1_case_resolution.csv")
    parser.add_argument("--segment-remap-csv", default="temp/phase8_structural_R1_segment_remap.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10a_r_remap_reconciliation" / utc_stamp()
    )
    summary = run_phase8a10a_r(
        Phase8A10ARPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
            official_timeline_csv=Path(args.official_timeline_csv),
            case_resolution_csv=Path(args.case_resolution_csv),
            segment_remap_csv=Path(args.segment_remap_csv),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"external_ready_yes={summary['external']['external_ready_yes']}")
    print(f"external_ready_no={summary['external']['external_ready_no']}")
    print(f"frozen_apply_groups={summary['frozen_apply_groups']}")
    print(f"frozen_apply_rows={summary['frozen_apply_rows']}")
    print(f"atomic_operations={summary['atomic_operations']}")
    print(f"production_writes={summary['production_writes']}")
    print(f"rawcandle_writes={summary['rawcandle_writes']}")
    print(f"next_action={summary['next_action']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()
