from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_legacy_deep_history import PHASE3C_2_ARTIFACT_ROOT, run_legacy_deep_history_extension


def main() -> None:
    args = _parse_args()
    summary = run_legacy_deep_history_extension(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=Path(args.artifact_root),
        apply_production=not args.dry_run_only,
    )
    print(json.dumps(_compact(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply Phase 3C-2 Legacy deep-history import to V3")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root", default=str(PHASE3C_2_ARTIFACT_ROOT))
    parser.add_argument("--dry-run-only", action="store_true")
    return parser.parse_args()


def _compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "run_id": summary["run_id"],
        "ready_reconciliation": summary["ready_reconciliation"],
        "dry_gate": summary["dry_gate"],
        "production_rows": summary["production"].get("rows", {}),
        "post_counts": summary["post"]["counts"],
        "post_core": summary["post"]["core"],
        "idempotency": summary["idempotency"],
        "backup": summary["backup"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
