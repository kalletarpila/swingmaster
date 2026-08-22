from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_v2_enrichment import run_v2_enrichment


DEFAULT_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_v2_enrichment/20260822T_PHASE3C_V2_ENRICHMENT")


def main() -> None:
    args = _parse_args()
    summary = run_v2_enrichment(
        v3_db=Path(args.v3_db),
        v2_db=Path(args.v2_db),
        legacy_db=Path(args.legacy_db),
        artifact_root=Path(args.artifact_root),
        migration_run_id=args.migration_run_id,
        now_utc=args.now_utc,
    )
    print(json.dumps(_compact_summary(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply Phase 3C V2 NULL-fill enrichment to refined Fundamentals V3")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--migration-run-id", default="V3_PHASE3C_V2_ENRICHMENT_20260822T000000Z")
    parser.add_argument("--now-utc", default="2026-08-22T00:00:00Z")
    return parser.parse_args()


def _compact_summary(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "migration_run_id": summary["migration_run_id"],
        "pre": {
            "companies": summary["pre_baseline"]["company_total"],
            "active": summary["pre_baseline"]["active"],
            "inactive": summary["pre_baseline"]["inactive"],
            "canonical_q": summary["pre_baseline"]["coverage"]["canonical_q_total"],
            "core_ready": summary["pre_baseline"]["coverage"]["core_ready_q"],
            "core_not_ready": summary["pre_baseline"]["coverage"]["core_not_ready_q"],
            "publish_known": summary["pre_baseline"]["coverage"]["publish_date_known"],
            "publish_null": summary["pre_baseline"]["coverage"]["publish_date_null"],
        },
        "post": {
            "companies": summary["post_baseline"]["company_total"],
            "active": summary["post_baseline"]["active"],
            "inactive": summary["post_baseline"]["inactive"],
            "canonical_q": summary["post_baseline"]["coverage"]["canonical_q_total"],
            "core_ready": summary["post_baseline"]["coverage"]["core_ready_q"],
            "core_not_ready": summary["post_baseline"]["coverage"]["core_not_ready_q"],
            "publish_known": summary["post_baseline"]["coverage"]["publish_date_known"],
            "publish_null": summary["post_baseline"]["coverage"]["publish_date_null"],
        },
        "identity": summary["identity"],
        "safe_null_fills_planned": summary["safe_null_fills_planned"],
        "safe_publish_fills_planned": summary["safe_publish_fills_planned"],
        "production_rows": summary["production_apply"]["rows"],
        "no_overwrite": summary["no_overwrite"],
        "idempotency": summary["idempotency"],
        "integrity": summary["integrity"],
        "post_gate": summary["post_gate"],
        "provider_calls": summary["provider_calls"],
        "legacy_canonical_contribution": summary["legacy_canonical_contribution"],
    }


if __name__ == "__main__":
    main()
