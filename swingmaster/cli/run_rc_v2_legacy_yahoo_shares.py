from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals_v2.legacy_yahoo_shares import run_legacy_yahoo_shares_import


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline legacy Yahoo Ordinary Shares Number fallback importer for rc_fundamentals_v2")
    parser.add_argument("--v2-db", required=True, help="Path to rc_fundamentals_v2.db")
    parser.add_argument("--legacy-db", required=True, help="Path to legacy fundamentals_usa.db")
    parser.add_argument("--artifact-dir", required=True, help="Directory for audit artifacts")
    parser.add_argument("--run-id", required=True, help="Deterministic import run id")
    parser.add_argument("--market", default="usa")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; no writes")
    parser.add_argument("--apply", action="store_true", help="Apply NULL-only shares_outstanding fills")
    parser.add_argument("--create-backup", action="store_true", help="Copy and integrity-check V2 DB before apply")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.dry_run and args.apply:
        raise SystemExit("LEGACY_YAHOO_SHARES_DRY_RUN_AND_APPLY_MUTUALLY_EXCLUSIVE")
    if not args.dry_run and not args.apply:
        raise SystemExit("LEGACY_YAHOO_SHARES_REQUIRES_DRY_RUN_OR_APPLY")
    summary = run_legacy_yahoo_shares_import(
        v2_db=Path(args.v2_db).expanduser().resolve(),
        legacy_db=Path(args.legacy_db).expanduser().resolve(),
        artifact_dir=Path(args.artifact_dir).expanduser().resolve(),
        run_id=args.run_id,
        dry_run=args.dry_run,
        apply=args.apply,
        market=args.market,
        create_backup=args.create_backup,
    )
    for key in (
        "status",
        "mode",
        "eligible_rows",
        "dry_run_fills",
        "shares_fills",
        "conflicts",
        "bad_provenance",
        "provider_calls",
    ):
        print(f"SUMMARY {key}={summary[key]}")


if __name__ == "__main__":
    main()
