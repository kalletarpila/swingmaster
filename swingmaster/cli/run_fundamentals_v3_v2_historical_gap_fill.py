from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_v2_historical_gap_fill import run_v2_historical_gap_fill


DEFAULT_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_4_v2_residual_history/20260823T_PHASE3C_4_V2_RESIDUAL_HISTORY")


def main() -> None:
    args = _parse_args()
    summary = run_v2_historical_gap_fill(
        v3_db=Path(args.v3_db),
        v2_db=Path(args.v2_db),
        legacy_db=Path(args.legacy_db),
        artifact_root=Path(args.artifact_root),
        migration_run_id=args.migration_run_id,
        now_utc=args.now_utc,
    )
    print(json.dumps(_compact(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply Phase 3C-4 V2 residual historical gap fill")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--migration-run-id", default="V3_PHASE3C4_V2_RESIDUAL_HISTORICAL_GAP_FILL_2026-08-23T000000Z")
    parser.add_argument("--now-utc", default="2026-08-23T00:00:00Z")
    return parser.parse_args()


def _compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "migration_run_id": summary["migration_run_id"],
        "candidate_population": summary["candidate_population"],
        "new_q_gate_calibration": summary["new_q_gate_calibration"],
        "production_rows": summary["production_apply"]["rows"],
        "field_contribution": summary["field_contribution"],
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
        "phase3c4b_review_rows": summary["phase3c4b_review_rows"],
        "phase3c4b_value_estimate": summary["phase3c4b_value_estimate"],
        "no_overwrite": summary["no_overwrite"],
        "idempotency": summary["idempotency"],
        "integrity": summary["integrity"],
        "post_gate": summary["post_gate"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
