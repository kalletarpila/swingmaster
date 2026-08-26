from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10c_local_review import Phase8A10CPaths, run_phase8a10c_local_review, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only Phase 8A10C local evidence review.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--p2p3-root", default="temp/fundamentals_v3_phase8a10b_p2p3_reprioritization/20260826T162000Z")
    parser.add_argument("--full-audit-root", default="temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10c_local_review" / utc_stamp()
    )
    summary = run_phase8a10c_local_review(
        Phase8A10CPaths(
            artifact_root=artifact_root,
            p2p3_root=Path(args.p2p3_root),
            full_audit_root=Path(args.full_audit_root),
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"local_evidence_rows={summary['starting_state']['local_evidence_rows']}")
    print(f"external_queue_rows={summary['final_external_queue']['queue_rows']}")
    print(f"unique_tickers={summary['final_external_queue']['unique_tickers']}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
