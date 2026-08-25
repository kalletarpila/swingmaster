from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a6_safe_apply import Phase8A6Paths, run_phase8a6, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply safe verified Phase 8A6 canonical repairs.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument(
        "--a5-artifact-root",
        default="temp/fundamentals_v3_phase8a5_verified_publish_ingest/20260825T_PHASE8A5_VERIFIED_PUBLISH_INGEST",
    )
    parser.add_argument("--semantic-verified-csv", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a6_safe_apply" / utc_stamp()
    )
    summary = run_phase8a6(
        Phase8A6Paths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            a5_artifact_root=Path(args.a5_artifact_root),
            semantic_verified_csv=Path(args.semantic_verified_csv) if args.semantic_verified_csv else None,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"publish_repairs_applied={summary['publish_repairs_applied']}")
    print(f"period_end_repairs_applied={summary['period_end_repairs_applied']}")
    print(f"semantic_repairs_applied={summary['semantic_repairs_applied']}")
    print(f"write_guard_failures={summary['write_guard_failures']}")
    print(f"derived_data_status={summary['derived_data_status']}")
    print(f"artifact_root={artifact_root}")


if __name__ == "__main__":
    main()
