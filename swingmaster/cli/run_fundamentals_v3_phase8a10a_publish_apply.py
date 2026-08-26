from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10a_publish_apply import (
    Phase8A10APublishApplyPaths,
    run_phase8a10a_publish_apply,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply verified Phase 8A10A publish-date residual repairs.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--verified-csv", default=None)
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10a_publish_apply" / utc_stamp()
    )
    summary = run_phase8a10a_publish_apply(
        Phase8A10APublishApplyPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            verified_csv=Path(args.verified_csv) if args.verified_csv else None,
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"verified_rows={summary.get('verified_input', {}).get('rows')}")
    print(f"rows_updated={summary.get('apply', {}).get('rows_updated')}")
    print(f"changed_publish_cells={summary.get('apply', {}).get('changed_publish_cells')}")
    print(f"publish_residual_after={summary.get('residual', {}).get('publish_residual_after')}")
    print(f"remaining_structural_r1={summary.get('special_cases', {}).get('remaining')}")
    print(f"downstream_writes={summary.get('downstream_writes')}")
    print(f"rawcandle_writes={summary.get('rawcandle', {}).get('writes')}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary.get('next_action')}")


if __name__ == "__main__":
    main()
