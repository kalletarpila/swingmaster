from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_closure import run_canonical_migration_closure


DEFAULT_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_6_canonical_migration_closure/20260823T_PHASE3C_6_CANONICAL_MIGRATION_CLOSURE")


def main() -> None:
    args = parse_args()
    summary = run_canonical_migration_closure(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=Path(args.artifact_root),
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Close Fundamentals V3 Phase 3 canonical migration")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "baseline": {
            "companies": summary["baseline"]["company_total"],
            "active": summary["baseline"]["active"],
            "inactive": summary["baseline"]["inactive"],
            "canonical_q": summary["baseline"]["coverage"]["canonical_q_total"],
            "core_ready": summary["baseline"]["coverage"]["core_ready_q"],
            "core_not_ready": summary["baseline"]["coverage"]["core_not_ready_q"],
            "publish_known": summary["baseline"]["coverage"]["publish_date_known"],
            "publish_null": summary["baseline"]["coverage"]["publish_date_null"],
        },
        "residual_reclassification": summary["residual_reclassification"],
        "closure_gate": summary["closure_gate"],
        "phase4_handoff": summary["phase4_handoff"],
        "fingerprint": summary["fingerprint"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
