from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_universe_refinement import run_universe_refinement


DEFAULT_BOOTSTRAP_RUN_ID = "V3_YAHOO_FULL_BOOTSTRAP_20260821T140717Z"
DEFAULT_BOOTSTRAP_ROOT = Path("temp/fundamentals_v3_phase2d_production") / DEFAULT_BOOTSTRAP_RUN_ID
DEFAULT_POST_A_ROOT = Path("temp/fundamentals_v3_phase2d_post_metadata_recovery/20260821T_POST_A_METADATA_RECOVERY")
DEFAULT_POST_A2_ROOT = Path("temp/fundamentals_v3_phase2d_post_fiscal_anchor_resolution/20260821T_POST_A2_FISCAL_ANCHOR_RESOLUTION")
DEFAULT_POST_A3_ROOT = Path("temp/fundamentals_v3_phase2d_post_official_fiscal_recovery/20260822T_A3_OFFICIAL_FISCAL_RECOVERY")
DEFAULT_ACTIVITY_ROOT = Path("temp/fundamentals_v3_phase2d_post_activity_triage/20260822T_POST_B_PRICE_ACTIVITY_TRIAGE")
DEFAULT_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3b_universe_refinement/20260822T_PHASE3B_UNIVERSE_REFINEMENT")


def main() -> None:
    args = _parse_args()
    summary = run_universe_refinement(
        production_v3_db=Path(args.production_v3_db),
        v2_db=Path(args.v2_db),
        legacy_db=Path(args.legacy_db),
        osakedata_db=Path(args.osakedata_db),
        artifact_root=Path(args.artifact_root),
        company_baseline_csv=Path(args.company_baseline_csv),
        raw_cache_db=Path(args.raw_cache_db),
        bootstrap_root=Path(args.bootstrap_root),
        post_a_root=Path(args.post_a_root),
        post_a2_root=Path(args.post_a2_root),
        post_a3_root=Path(args.post_a3_root),
        bootstrap_run_id=args.bootstrap_run_id,
        migration_run_id=args.migration_run_id,
        now_utc=args.now_utc,
    )
    print(json.dumps(_compact_summary(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refine Fundamentals V3 universe by excluding ordinary-model-incompatible financial companies")
    parser.add_argument("--production-v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--osakedata-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--company-baseline-csv", default=str(DEFAULT_ACTIVITY_ROOT / "phase3_company_active_baseline.csv"))
    parser.add_argument("--raw-cache-db", default=str(DEFAULT_BOOTSTRAP_ROOT / "yahoo_raw_cache.db"))
    parser.add_argument("--bootstrap-run-id", default=DEFAULT_BOOTSTRAP_RUN_ID)
    parser.add_argument("--migration-run-id", default="V3_PHASE3B_U_REFINED_YAHOO_SEED_20260822T000000Z")
    parser.add_argument("--now-utc", default="2026-08-22T00:00:00Z")
    parser.add_argument("--bootstrap-root", default=str(DEFAULT_BOOTSTRAP_ROOT))
    parser.add_argument("--post-a-root", default=str(DEFAULT_POST_A_ROOT))
    parser.add_argument("--post-a2-root", default=str(DEFAULT_POST_A2_ROOT))
    parser.add_argument("--post-a3-root", default=str(DEFAULT_POST_A3_ROOT))
    return parser.parse_args()


def _compact_summary(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "provider_calls": summary["provider_calls"],
        "v2_canonical_contribution": summary["v2_canonical_contribution"],
        "legacy_canonical_contribution": summary["legacy_canonical_contribution"],
        "before": {
            "company_total": summary["before"]["company_total"],
            "active": summary["before"]["active"],
            "inactive": summary["before"]["inactive"],
            "canonical_q_total": summary["before"]["coverage"]["canonical_q_total"],
            "core_ready_q": summary["before"]["coverage"]["core_ready_q"],
            "core_not_ready_q": summary["before"]["coverage"]["core_not_ready_q"],
        },
        "after": {
            "company_total": summary["after"]["company_total"],
            "active": summary["after"]["active"],
            "inactive": summary["after"]["inactive"],
            "canonical_q_total": summary["after"]["coverage"]["canonical_q_total"],
            "core_ready_q": summary["after"]["coverage"]["core_ready_q"],
            "core_not_ready_q": summary["after"]["coverage"]["core_not_ready_q"],
            "publish_date_known": summary["after"]["coverage"]["publish_date_known"],
            "publish_date_null": summary["after"]["coverage"]["publish_date_null"],
        },
        "excluded": {
            "companies": summary["impact"]["companies_to_remove"],
            "canonical_qs": summary["impact"]["canonical_qs_removed"],
            "core_ready_q": summary["impact"]["core_ready_q_removed"],
            "core_not_ready_q": summary["impact"]["core_not_ready_q_removed"],
        },
        "class_counts": summary["plan"]["class_counts"],
        "retained_parity": summary["retained_parity"],
        "special_case_parity_passed": summary["special_case_parity"]["passed"],
        "production_integrity": summary["production_integrity"],
        "idempotency": summary["idempotency"],
    }


if __name__ == "__main__":
    main()
