from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10a_special_resolution import (
    Phase8A10ASpecialPaths,
    run_phase8a10a_special,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve Phase 8A10A FNGR/IMMR/RCAT structural special cases read-only.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--a10ar-root", default="temp/fundamentals_v3_phase8a10a_r_remap_reconciliation/20260826T071127Z")
    parser.add_argument("--apply-root", default="temp/fundamentals_v3_phase8a10a_apply/20260826T091635Z")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10a_special_resolution" / utc_stamp()
    )
    summary = run_phase8a10a_special(
        Phase8A10ASpecialPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
            a10ar_root=Path(args.a10ar_root),
            apply_root=Path(args.apply_root),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"structural_r1_start={summary['structural_r1_start']}")
    print(f"fngr_ready={summary['fngr']['production_ready']}")
    print(f"immr_ready={summary['immr']['production_ready']}")
    print(f"rcat_ready={summary['rcat']['production_ready']}")
    print(f"frozen_operations={summary['frozen_special_apply']['operations']}")
    print(f"production_writes={summary['production_writes']}")
    print(f"rawcandle_writes={summary['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
