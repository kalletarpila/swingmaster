from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10a_special_final_reconcile import (
    Phase8A10ASpecialFinalReconcilePaths,
    run_phase8a10a_special_final_reconcile,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only reconcile IMMR/RCAT special final evidence.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--temp-root", default="temp")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10a_special_final_reconcile" / utc_stamp()
    )
    summary = run_phase8a10a_special_final_reconcile(
        Phase8A10ASpecialFinalReconcilePaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
            temp_root=Path(args.temp_root),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"immr_ready={summary['immr']['production_ready']}")
    print(f"rcat_ready={summary['rcat']['production_ready']}")
    print(f"blockers={summary['blockers']['count']}")
    print(f"frozen_operations={summary['frozen_apply']['atomic_operations']}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
