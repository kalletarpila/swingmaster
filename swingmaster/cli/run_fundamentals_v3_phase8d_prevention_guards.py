from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8d_prevention_guards import Phase8DPaths, run_phase8d
from swingmaster.fundamentals.v3_fiscal_calendar import utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8D fiscal-calendar prevention guard proving.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--phase8c-artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8c_fiscal_calendar_metadata/20260827T_PHASE8C"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase8d_prevention_guards") / utc_stamp()
    summary = run_phase8d(Phase8DPaths(artifact_root=artifact_root, v3_db=args.v3_db, phase8c_artifact_root=args.phase8c_artifact_root))
    print(f"classification={summary['classification']}")
    print(f"companies_before={summary['baseline_before']['companies']}")
    print(f"companies_after={summary['baseline_after']['companies']}")
    print(f"canonical_rows_after={summary['baseline_after']['canonical_quarter_rows']}")
    print(f"guard_activation={summary['dry_run']['guard_activation_status']}")
    print(f"known_replay={summary['known_phase8_replay']['decision_counts']}")
    print(f"material_false_blocks={summary['false_positive']['material_false_block_count']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
