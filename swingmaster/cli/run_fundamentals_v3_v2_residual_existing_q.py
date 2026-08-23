from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_v2_residual_existing_q import run_v2_residual_existing_q


DEFAULT_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_3_v2_residual_existing_q/20260823T_PHASE3C_3_V2_RESIDUAL_EXISTING_Q")


def main() -> None:
    args = _parse_args()
    summary = run_v2_residual_existing_q(
        v3_db=Path(args.v3_db),
        v2_db=Path(args.v2_db),
        legacy_db=Path(args.legacy_db),
        artifact_root=Path(args.artifact_root),
        migration_run_id=args.migration_run_id,
        now_utc=args.now_utc,
    )
    print(json.dumps(_compact_summary(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply Phase 3C-3 V2 residual existing-Q enrichment to Fundamentals V3")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--migration-run-id", default="V3_PHASE3C3_V2_RESIDUAL_EXISTING_Q_2026-08-23T000000Z")
    parser.add_argument("--now-utc", default="2026-08-23T00:00:00Z")
    return parser.parse_args()


def _compact_summary(summary: dict) -> dict:
    pre = summary["pre_baseline"]
    post = summary["post_baseline"]
    return {
        "classification": summary["classification"],
        "migration_run_id": summary["migration_run_id"],
        "pre": {
            "companies": pre["company_total"],
            "active": pre["active"],
            "inactive": pre["inactive"],
            "canonical_q": pre["coverage"]["canonical_q_total"],
            "core_ready": pre["coverage"]["core_ready_q"],
            "core_not_ready": pre["coverage"]["core_not_ready_q"],
            "publish_null": pre["coverage"]["publish_date_null"],
            "field_missing": pre["coverage"]["field_missing"],
            "core_gap_profile": pre["core_gap_profile"],
        },
        "post": {
            "companies": post["company_total"],
            "active": post["active"],
            "inactive": post["inactive"],
            "canonical_q": post["coverage"]["canonical_q_total"],
            "core_ready": post["coverage"]["core_ready_q"],
            "core_not_ready": post["coverage"]["core_not_ready_q"],
            "publish_null": post["coverage"]["publish_date_null"],
            "field_missing": post["coverage"]["field_missing"],
            "core_gap_profile": post["core_gap_profile"],
        },
        "identity": summary["identity"],
        "year_overlap": summary["year_overlap"],
        "safe_null_fills_planned": summary["safe_null_fills_planned"],
        "safe_publish_fills_planned": summary["safe_publish_fills_planned"],
        "v2_only_historical_q_candidates": summary["v2_only_historical_q_candidates"],
        "production_rows": summary["production_apply"]["rows"],
        "production_metadata": summary["production_apply"]["metadata"],
        "no_overwrite": summary["no_overwrite"],
        "idempotency": summary["idempotency"],
        "integrity": summary["integrity"],
        "post_gate": summary["post_gate"],
    }


if __name__ == "__main__":
    main()
