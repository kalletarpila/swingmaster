from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import (
    Phase8A10DRPaths,
    run_phase8a10d_r_segment_reconciliation,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only global P1 segment reconciliation rehearsal.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--case-resolution-csv", default="temp/phase8_global_P1_verified_case_resolution.csv")
    parser.add_argument("--official-timeline-csv", default="temp/phase8_global_P1_official_fiscal_timelines.csv")
    parser.add_argument("--transformation-plan-csv", default="temp/phase8_global_P1_transformation_plan.csv")
    parser.add_argument("--fundamental-repairs-csv", default="temp/phase8_global_P1_fundamental_value_repairs.csv")
    parser.add_argument("--full-a10b-root", default="temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--publish-apply-root", default="temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10d_r_segment_reconciliation" / utc_stamp()
    )
    summary = run_phase8a10d_r_segment_reconciliation(
        Phase8A10DRPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            case_resolution_csv=Path(args.case_resolution_csv),
            official_timeline_csv=Path(args.official_timeline_csv),
            transformation_plan_csv=Path(args.transformation_plan_csv),
            fundamental_repairs_csv=Path(args.fundamental_repairs_csv),
            full_a10b_root=Path(args.full_a10b_root),
            rawcandle_db=Path(args.rawcandle_db),
            publish_apply_root=Path(args.publish_apply_root),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"P1_before={summary['exact_a10b_post_audit']['P1_before']}")
    print(f"P1_after={summary['exact_a10b_post_audit']['P1_after']}")
    print(f"remaining_original_P1={','.join(summary['exact_a10b_post_audit']['remaining_original_P1'])}")
    print(f"new_P1_introduced={','.join(summary['exact_a10b_post_audit']['new_P1_introduced'])}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
