from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10a_apply_structural_repairs import (
    Phase8A10AApplyPaths,
    run_phase8a10a_apply,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply the frozen Phase 8A10A structural quarter-sequence repair set.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--a10ar-root", default="temp/fundamentals_v3_phase8a10a_r_remap_reconciliation/20260826T071127Z")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10a_apply" / utc_stamp()
    )
    summary = run_phase8a10a_apply(
        Phase8A10AApplyPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
            a10ar_root=Path(args.a10ar_root),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"frozen_groups={summary['frozen_groups']}")
    print(f"frozen_rows={summary['frozen_rows']}")
    print(f"atomic_operations_executed={summary['production_apply']['atomic_operations_executed']}")
    print(f"groups_committed={summary['production_apply']['groups_committed']}")
    print(f"groups_rolled_back={summary['production_apply']['groups_rolled_back']}")
    print(f"structural_r1_after={summary['residual_r1']['structural_r1_after']}")
    print(f"downstream_writes={summary['downstream_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
