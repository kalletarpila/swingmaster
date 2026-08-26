from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10b_p2p3_reprioritization import (
    Phase8A10BP2P3Paths,
    run_phase8a10b_p2p3_reprioritization,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only Phase 8A10B P2/P3 recent reprioritization.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--source-audit-root", default="temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10b_p2p3_reprioritization" / utc_stamp()
    )
    summary = run_phase8a10b_p2p3_reprioritization(
        Phase8A10BP2P3Paths(
            artifact_root=artifact_root,
            source_audit_root=Path(args.source_audit_root),
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"P2A_rows={summary['p2_reprioritization']['P2A_rows']}")
    print(f"P2B_rows={summary['p2_reprioritization']['P2B_rows']}")
    print(f"P2C_rows={summary['p2_reprioritization']['P2C_rows']}")
    print(f"P3_ESCALATED_rows={summary['p3_reprioritization']['P3_ESCALATED_rows']}")
    print(f"current_critical_rows={summary['current_critical']['total_queue_rows']}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
