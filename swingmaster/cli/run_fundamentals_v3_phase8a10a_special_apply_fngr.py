from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10a_special_apply_fngr import (
    Phase8A10AFngrApplyPaths,
    run_phase8a10a_special_fngr_apply,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply the bounded FNGR Phase 8A10A special repair.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--special-root", default="temp/fundamentals_v3_phase8a10a_special_resolution/20260826T093155Z")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10a_special_apply" / utc_stamp()
    )
    summary = run_phase8a10a_special_fngr_apply(
        Phase8A10AFngrApplyPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
            special_root=Path(args.special_root),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"quarter_id={summary['quarter_id']}")
    print(f"transaction_status={summary['transaction_status']}")
    print(f"changed_cells={summary['changed_cells']}")
    print(f"structural_r1_after={summary['structural_r1_after']}")
    print(f"remaining_tickers={summary['remaining_tickers']}")
    print(f"downstream_writes={summary['downstream_writes']}")
    print(f"rawcandle_writes={summary['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
