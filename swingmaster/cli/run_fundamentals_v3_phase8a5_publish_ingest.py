from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a5_publish_ingest import Phase8A5Paths, run_phase8a5, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest verified Phase 8 publish-date evidence.")
    parser.add_argument("--verified-csv", default="temp/fundamentals_v3_phase8_publish_date_manual_check_verified.csv")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a5_verified_publish_ingest" / utc_stamp()
    )
    summary = run_phase8a5(
        Phase8A5Paths(
            verified_csv=Path(args.verified_csv),
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"next_action={summary['next_action']}")
    print(f"frozen_publish_repair_rows={summary['frozen_publish_repair_rows']}")
    print(f"unresolved_rows={summary['unresolved_rows']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()

