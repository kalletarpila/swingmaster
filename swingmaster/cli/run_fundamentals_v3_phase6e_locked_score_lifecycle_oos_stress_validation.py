from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6e_locked_score_lifecycle_oos_stress_validation import (
    run_phase6e_validation,
    utc_stamp,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6E locked score/lifecycle OOS and stress validation.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--score-artifact-root", type=Path, default=None)
    parser.add_argument("--lifecycle-artifact-root", type=Path, default=None)
    parser.add_argument("--no-durable-docs", action="store_true")
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6e_oos_stress_validation") / utc_stamp()
    summary = run_phase6e_validation(
        v3_db=args.v3_db,
        artifact_root=artifact_root,
        score_artifact_root=args.score_artifact_root,
        lifecycle_artifact_root=args.lifecycle_artifact_root,
        write_durable_docs=not args.no_durable_docs,
    )
    print(summary["classification"])
    print(f"artifact_root={summary.get('artifact_root')}")
    print(f"score_fingerprint_actual={summary.get('score_fingerprint_actual')}")
    print(f"lifecycle_fingerprint_actual={summary.get('lifecycle_fingerprint_actual')}")
    print(f"oos_2026_observations={summary.get('oos_2026_observations')}")
    print(f"oos_2026_score_ready={summary.get('oos_2026_score_ready')}")
    print(f"stress_2020_observations={summary.get('stress_2020_observations')}")
    print(f"stress_2020_score_ready={summary.get('stress_2020_score_ready')}")
    print(f"production_writes={summary.get('production_writes')}")
    print(f"next={summary.get('recommended_next_step')}")


if __name__ == "__main__":
    main()
