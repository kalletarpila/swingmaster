from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10f_current_downstream import Phase8A10FPaths, run_phase8a10f, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only Phase 8A10F current-downstream reconciliation.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--case-resolution-csv", default="temp/phase8_current_downstream_verified_case_resolution.csv")
    parser.add_argument("--official-timeline-csv", default="temp/phase8_current_downstream_official_fiscal_timelines.csv")
    parser.add_argument("--transformation-plan-csv", default="temp/phase8_current_downstream_transformation_plan.csv")
    parser.add_argument("--a10c-root", default="temp/fundamentals_v3_phase8a10c_local_review/20260826T165000Z")
    parser.add_argument("--a10b-root", default="temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    parser.add_argument("--a10d-root", default="temp/fundamentals_v3_phase8a10d_r_segment_reconciliation/20260826T171500Z")
    parser.add_argument("--a10e-root", default="temp/fundamentals_v3_phase8a10e_one_year_period_shift/20260826T174000Z")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10f_current_downstream_reconcile" / utc_stamp()
    )
    summary = run_phase8a10f(
        Phase8A10FPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            case_resolution_csv=Path(args.case_resolution_csv),
            official_timeline_csv=Path(args.official_timeline_csv),
            transformation_plan_csv=Path(args.transformation_plan_csv),
            a10c_root=Path(args.a10c_root),
            a10b_root=Path(args.a10b_root),
            a10d_root=Path(args.a10d_root),
            a10e_root=Path(args.a10e_root),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"case_rows={summary['input']['case_rows']}")
    print(f"externally_ready={summary['input']['production_ready']['YES']}")
    print(f"externally_blocked={summary['input']['production_ready']['NO']}")
    print(f"locally_ready_ops={summary['external_ready_21']['locally_ready']}")
    print(f"frozen_operations={summary['frozen_repair']['repair_operations']}")
    print(f"blockers={summary['current_downstream_after_rehearsal']['still_blocked']}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
