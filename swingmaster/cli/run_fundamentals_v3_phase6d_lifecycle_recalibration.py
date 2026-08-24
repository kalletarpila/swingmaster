from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6d_lifecycle_recalibration import run_phase6d_lifecycle_recalibration, utc_stamp


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6D lifecycle recalibration.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6d_lifecycle_recalibration") / utc_stamp()
    summary = run_phase6d_lifecycle_recalibration(v3_db=args.v3_db, artifact_root=artifact_root)
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"calibration_observations={summary['calibration_observations']}")
    print(f"calibration_companies={summary['calibration_companies']}")
    print(f"lifecycle_ready_observations={summary['lifecycle_ready_observations']}")
    print(f"fingerprint={summary['fingerprint']}")
    print(f"production_writes={summary['production_writes']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
