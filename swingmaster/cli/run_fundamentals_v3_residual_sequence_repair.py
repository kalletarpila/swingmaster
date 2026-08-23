from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_residual_sequence_repair import run_residual_sequence_review, utc_stamp


def main() -> None:
    args = parse_args()
    root = Path(args.artifact_root) if args.artifact_root else Path("temp/fundamentals_v3_phase3c_6b2_residual_sequence_review") / f"{utc_stamp()}_PHASE3C_6B2_RESIDUAL_SEQUENCE_REVIEW"
    summary = run_residual_sequence_review(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=root,
        apply_production=args.apply,
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve V3 Phase 3C-6B-2 residual sequence exceptions")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "run_id": summary.get("run_id"),
        "plan": summary.get("plan"),
        "dry_gate": summary.get("dry", {}).get("gate"),
        "production": summary.get("production", {}).get("rows"),
        "post": summary.get("post"),
        "artifact_root": summary.get("artifact_root"),
    }


if __name__ == "__main__":
    main()
