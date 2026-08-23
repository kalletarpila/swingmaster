from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_v2_mapping_review import run_v2_mapping_review


DEFAULT_PHASE3C4_ROOT = Path("temp/fundamentals_v3_phase3c_4_v2_residual_history/20260823T_PHASE3C_4_V2_RESIDUAL_HISTORY")
DEFAULT_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_4b_v2_mapping_review/20260823T_PHASE3C_4B_V2_MAPPING_REVIEW")


def main() -> None:
    args = _parse_args()
    summary = run_v2_mapping_review(
        v3_db=Path(args.v3_db),
        v2_db=Path(args.v2_db),
        legacy_db=Path(args.legacy_db),
        phase3c4_root=Path(args.phase3c4_root),
        artifact_root=Path(args.artifact_root),
        migration_run_id=args.migration_run_id,
        now_utc=args.now_utc,
    )
    print(json.dumps(_compact(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review and repair residual Phase 3C-4B V2 historical mappings")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--phase3c4-root", default=str(DEFAULT_PHASE3C4_ROOT))
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--migration-run-id", default="V3_PHASE3C4B_V2_HISTORICAL_MAPPING_REVIEW_2026-08-23T000000Z")
    parser.add_argument("--now-utc", default="2026-08-23T00:00:00Z")
    return parser.parse_args()


def _compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "migration_run_id": summary["migration_run_id"],
        "review_population": summary["review_population"],
        "terminal_classification": summary["terminal_classification"],
        "ready_new_q_after_review": summary["ready_new_q_after_review"],
        "ready_existing_q_null_fill": summary["ready_existing_q_null_fill"],
        "production_rows": summary["production_apply"]["rows"],
        "field_contributions": summary["production_apply"]["field_contributions"],
        "pre": {
            "canonical_q": summary["pre_baseline"]["coverage"]["canonical_q_total"],
            "core_ready": summary["pre_baseline"]["coverage"]["core_ready_q"],
            "core_not_ready": summary["pre_baseline"]["coverage"]["core_not_ready_q"],
            "publish_null": summary["pre_baseline"]["coverage"]["publish_date_null"],
            "field_missing": summary["pre_baseline"]["coverage"]["field_missing"],
        },
        "post": {
            "canonical_q": summary["post_baseline"]["coverage"]["canonical_q_total"],
            "core_ready": summary["post_baseline"]["coverage"]["core_ready_q"],
            "core_not_ready": summary["post_baseline"]["coverage"]["core_not_ready_q"],
            "publish_null": summary["post_baseline"]["coverage"]["publish_date_null"],
            "field_missing": summary["post_baseline"]["coverage"]["field_missing"],
        },
        "phase3c5_handoff_rows": summary["phase3c5_handoff_rows"],
        "no_overwrite": summary["no_overwrite"],
        "idempotency": summary["idempotency"],
        "integrity": summary["integrity"],
        "post_gate": summary["post_gate"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
