from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_residual_reconciliation import run_residual_reconciliation


DEFAULT_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_5_residual_reconciliation/20260823T_PHASE3C_5_RESIDUAL_RECONCILIATION")


def main() -> None:
    args = parse_args()
    summary = run_residual_reconciliation(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=Path(args.artifact_root),
        migration_run_id=args.migration_run_id,
        now_utc=args.now_utc,
        apply_production=not args.no_apply,
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 3C-5 residual reconciliation")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--migration-run-id", default="V3_PHASE3C5_RESIDUAL_RECONCILIATION_20260823T000000Z")
    parser.add_argument("--now-utc", default="2026-08-23T00:00:00Z")
    parser.add_argument("--no-apply", action="store_true")
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "migration_run_id": summary["migration_run_id"],
        "raw_issue_inventory": summary["raw_issue_inventory"],
        "consolidation": summary["consolidation"],
        "issue_scope": summary["issue_scope"],
        "v2_historical_residual": summary["v2_historical_residual"],
        "field_conflicts": summary["field_conflicts"],
        "metadata": summary["metadata"],
        "canonical_corrections": summary["canonical_corrections"],
        "remaining_canonical_issues": summary["remaining_canonical_issues"],
        "phase4_handoff": summary["phase4_handoff"],
        "pre": {
            "canonical_q": summary["pre_baseline"]["coverage"]["canonical_q_total"],
            "core_ready": summary["pre_baseline"]["coverage"]["core_ready_q"],
            "core_not_ready": summary["pre_baseline"]["coverage"]["core_not_ready_q"],
            "publish_null": summary["pre_baseline"]["coverage"]["publish_date_null"],
        },
        "post": {
            "canonical_q": summary["post_baseline"]["coverage"]["canonical_q_total"],
            "core_ready": summary["post_baseline"]["coverage"]["core_ready_q"],
            "core_not_ready": summary["post_baseline"]["coverage"]["core_not_ready_q"],
            "publish_null": summary["post_baseline"]["coverage"]["publish_date_null"],
        },
        "idempotency": summary["idempotency"],
        "integrity": summary["integrity"],
        "post_gate": summary["post_gate"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
