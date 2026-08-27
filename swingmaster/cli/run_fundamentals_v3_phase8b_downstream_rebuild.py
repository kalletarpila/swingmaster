from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8b_downstream_rebuild import Phase8BPaths, run_phase8b_downstream_rebuild, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8B downstream rebuild with known deferred canonical defects.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--rawcandle-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--publish-apply-root", type=Path, default=Path("temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z"))
    parser.add_argument("--score-artifact-root", type=Path, default=None)
    parser.add_argument("--lifecycle-artifact-root", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase8b_downstream_rebuild") / utc_stamp()
    summary = run_phase8b_downstream_rebuild(
        Phase8BPaths(
            artifact_root=artifact_root,
            v3_db=args.v3_db,
            rawcandle_db=args.rawcandle_db,
            publish_apply_root=args.publish_apply_root,
            score_artifact_root=args.score_artifact_root,
            lifecycle_artifact_root=args.lifecycle_artifact_root,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"companies={summary['canonical_baseline']['companies']}")
    print(f"global_P1_before={summary['known_defects']['global_P1_before']}")
    print(f"ttm_rows_after={summary['ttm']['rows_after']}")
    print(f"score_rows_after={summary['score']['rows_after']}")
    print(f"lifecycle_rows_after={summary['lifecycle']['rows_after']}")
    print(f"valuation_rows_after={summary['valuation']['rows_after']}")
    print(f"canonical_fingerprint_identical={summary['integrity']['fingerprint_identical']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
