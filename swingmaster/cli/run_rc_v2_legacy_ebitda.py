from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals_v2.legacy_ebitda import run_legacy_ebitda_import


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline legacy Yahoo EBITDA fallback importer for rc_fundamentals_v2")
    parser.add_argument("--v2-db", required=True)
    parser.add_argument("--legacy-db", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--market", default="usa")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--create-backup", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.dry_run and args.apply:
        raise SystemExit("LEGACY_EBITDA_DRY_RUN_AND_APPLY_MUTUALLY_EXCLUSIVE")
    if not args.dry_run and not args.apply:
        raise SystemExit("LEGACY_EBITDA_REQUIRES_DRY_RUN_OR_APPLY")
    summary = run_legacy_ebitda_import(
        v2_db=Path(args.v2_db).expanduser().resolve(),
        legacy_db=Path(args.legacy_db).expanduser().resolve(),
        artifact_dir=Path(args.artifact_dir).expanduser().resolve(),
        run_id=args.run_id,
        dry_run=args.dry_run,
        apply=args.apply,
        market=args.market,
        create_backup=args.create_backup,
    )
    for key in ("mode", "provider_calls", "final_phase3_classification"):
        print(f"SUMMARY {key}={summary[key]}")
    for key, value in summary["decision"].items():
        print(f"SUMMARY decision.{key}={value}")


if __name__ == "__main__":
    main()
