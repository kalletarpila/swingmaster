from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10a_special_remove import (
    Phase8A10ASpecialRemovePaths,
    run_phase8a10a_special_remove,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove IMMR/RCAT from V3 with bounded transactional guards.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10a_special_remove" / utc_stamp()
    )
    summary = run_phase8a10a_special_remove(
        Phase8A10ASpecialRemovePaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"companies_deleted={summary['apply']['companies_deleted']}")
    print(f"canonical_rows_deleted={summary['apply']['canonical_rows_deleted']}")
    print(f"structural_r1_before={summary['residual']['structural_r1_before']}")
    print(f"structural_r1_after={summary['residual']['structural_r1_after']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
