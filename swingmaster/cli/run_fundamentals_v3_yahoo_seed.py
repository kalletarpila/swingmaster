from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_yahoo_canonical_seed import run_yahoo_seed


DEFAULT_BOOTSTRAP_RUN_ID = "V3_YAHOO_FULL_BOOTSTRAP_20260821T140717Z"
DEFAULT_BOOTSTRAP_ROOT = Path("temp/fundamentals_v3_phase2d_production") / DEFAULT_BOOTSTRAP_RUN_ID
DEFAULT_POST_A_ROOT = Path("temp/fundamentals_v3_phase2d_post_metadata_recovery/20260821T_POST_A_METADATA_RECOVERY")
DEFAULT_POST_A2_ROOT = Path("temp/fundamentals_v3_phase2d_post_fiscal_anchor_resolution/20260821T_POST_A2_FISCAL_ANCHOR_RESOLUTION")
DEFAULT_POST_A3_ROOT = Path("temp/fundamentals_v3_phase2d_post_official_fiscal_recovery/20260822T_A3_OFFICIAL_FISCAL_RECOVERY")
DEFAULT_ACTIVITY_ROOT = Path("temp/fundamentals_v3_phase2d_post_activity_triage/20260822T_POST_B_PRICE_ACTIVITY_TRIAGE")


def main() -> None:
    args = _parse_args()
    summary = run_yahoo_seed(
        target_db=Path(args.target_db),
        artifact_root=Path(args.artifact_root),
        company_baseline_csv=Path(args.company_baseline_csv),
        raw_cache_db=Path(args.raw_cache_db),
        bootstrap_root=Path(args.bootstrap_root),
        post_a_root=Path(args.post_a_root),
        post_a2_root=Path(args.post_a2_root),
        post_a3_root=Path(args.post_a3_root),
        bootstrap_run_id=args.bootstrap_run_id,
        migration_run_id=args.migration_run_id,
    )
    print(json.dumps(_compact_summary(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply Phase 3B Yahoo-only canonical V3 seed")
    parser.add_argument("--target-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--migration-run-id", required=True)
    parser.add_argument("--bootstrap-run-id", default=DEFAULT_BOOTSTRAP_RUN_ID)
    parser.add_argument("--bootstrap-root", default=str(DEFAULT_BOOTSTRAP_ROOT))
    parser.add_argument("--post-a-root", default=str(DEFAULT_POST_A_ROOT))
    parser.add_argument("--post-a2-root", default=str(DEFAULT_POST_A2_ROOT))
    parser.add_argument("--post-a3-root", default=str(DEFAULT_POST_A3_ROOT))
    parser.add_argument("--company-baseline-csv", default=str(DEFAULT_ACTIVITY_ROOT / "phase3_company_active_baseline.csv"))
    parser.add_argument("--raw-cache-db", default=str(DEFAULT_BOOTSTRAP_ROOT / "yahoo_raw_cache.db"))
    return parser.parse_args()


def _compact_summary(summary: dict) -> dict:
    return {
        "company_summary": summary["company_summary"],
        "candidate_accounting": summary["candidate_accounting"],
        "row_counts": summary["row_counts"],
        "canonical_q_distribution": summary["canonical_q_distribution"],
        "coverage": {
            "canonical_q_total": summary["coverage"]["canonical_q_total"],
            "publish_date_known": summary["coverage"]["publish_date_known"],
            "publish_date_null": summary["coverage"]["publish_date_null"],
            "core_ready_q": summary["coverage"]["core_ready_q"],
            "core_not_ready_q": summary["coverage"]["core_not_ready_q"],
        },
        "issue_summary": summary["issue_summary"],
        "special_cases": summary["special_cases"],
        "integrity": summary["integrity"],
        "idempotency": {
            "row_counts_unchanged": summary["idempotency"]["row_counts_unchanged"],
            "second_run_q_creations": summary["idempotency"]["second_run_q_creations"],
            "inappropriate_field_fills": summary["idempotency"]["inappropriate_field_fills"],
            "duplicate_issue_count": summary["idempotency"]["duplicate_issue_count"],
        },
        "backup_path": summary["backup_path"],
    }


if __name__ == "__main__":
    main()
