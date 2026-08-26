from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import (
    Phase8A10BPaths,
    run_phase8a10b_full_sequence_audit,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only full V3 fiscal sequence/date audit.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--publish-apply-root", default="temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10b_full_sequence_audit" / utc_stamp()
    )
    summary = run_phase8a10b_full_sequence_audit(
        Phase8A10BPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            rawcandle_db=Path(args.rawcandle_db),
            publish_apply_root=Path(args.publish_apply_root),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"companies={summary['baseline']['companies']}")
    print(f"quarters={summary['fiscal_sequence']['quarters_audited']}")
    print(f"publish_R1={summary['publish_residual_reconciliation']['current_true_publish_R1']}")
    print(f"publish_R2={summary['publish_residual_reconciliation']['current_true_publish_R2']}")
    print(f"P1_rows={summary['severity']['P1_rows']}")
    print(f"P1_companies={summary['severity']['P1_companies']}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
