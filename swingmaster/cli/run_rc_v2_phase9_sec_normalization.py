from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals_v2.phase9_sec_normalization import run_phase9_sec_normalization


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 9 offline SEC normalization audit and scoped residual backfill for rc_fundamentals_v2")
    parser.add_argument("--v2-db", required=True, help="Path to rc_fundamentals_v2.db")
    parser.add_argument("--legacy-db", required=True, help="Path to fundamentals_usa.db")
    parser.add_argument("--artifact-dir", required=True, help="Directory for Phase 9 audit artifacts")
    parser.add_argument("--run-id", required=True, help="Deterministic Phase 9 run id")
    parser.add_argument("--market", default="usa")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; no writes")
    parser.add_argument("--apply", action="store_true", help="Apply approved NULL-only Phase 9 residual fills")
    parser.add_argument("--create-backup", action="store_true", help="Copy and integrity-check V2 DB before apply")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.dry_run and args.apply:
        raise SystemExit("PHASE9_DRY_RUN_AND_APPLY_MUTUALLY_EXCLUSIVE")
    if not args.dry_run and not args.apply:
        raise SystemExit("PHASE9_REQUIRES_DRY_RUN_OR_APPLY")
    summary = run_phase9_sec_normalization(
        v2_db=Path(args.v2_db).expanduser().resolve(),
        legacy_db=Path(args.legacy_db).expanduser().resolve(),
        artifact_dir=Path(args.artifact_dir).expanduser().resolve(),
        run_id=args.run_id,
        dry_run=args.dry_run,
        apply=args.apply,
        market=args.market,
        create_backup=args.create_backup,
    )
    for key in ("mode", "provider_calls", "eligible_revenue_rows", "final_phase9_classification"):
        print(f"SUMMARY {key}={summary[key]}")
    for key, value in summary["apply_actions"].items():
        print(f"SUMMARY apply_actions.{key}={value}")


if __name__ == "__main__":
    main()
