from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6b_score_lifecycle_calibration_design import run_phase6b_design, utc_stamp


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6B score/lifecycle calibration design.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6b_score_lifecycle_calibration_design") / utc_stamp()
    summary = run_phase6b_design(v3_db=args.v3_db, artifact_root=artifact_root)
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"score_components_found={summary['score_components_found']}")
    print(f"sparse_scale_components={summary['sparse_scale_components']}")
    print(f"lifecycle_states_found={summary['lifecycle_states_found']}")
    print(f"production_writes={summary['production_writes']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
