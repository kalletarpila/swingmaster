from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import run_phase4b_missing_field_recovery, utc_stamp


def main() -> None:
    args = parse_args()
    root = Path(args.artifact_root) if args.artifact_root else Path("temp/fundamentals_v3_phase4b_missing_field_recovery") / f"{utc_stamp()}_PHASE4B_MISSING_FIELD_RECOVERY"
    summary = run_phase4b_missing_field_recovery(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=root,
        phase4a_root=Path(args.phase4a_root),
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4B missing-field recovery")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--phase4a-root", default="temp/fundamentals_v3_phase4a_historical_completeness_audit/20260823T_PHASE4A_AUDIT")
    parser.add_argument("--artifact-root")
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "universe_cleanup": summary.get("universe_cleanup"),
        "before": summary.get("before"),
        "after": summary.get("after"),
        "zero_q": summary.get("zero_q"),
        "formula": summary.get("formula"),
        "second_pass": summary.get("second_pass"),
        "phase4c": summary.get("phase4c"),
        "integrity": summary.get("integrity"),
        "idempotency": summary.get("idempotency"),
        "artifact_root": summary.get("artifact_root"),
        "recommended_next_step": summary.get("recommended_next_step"),
    }


if __name__ == "__main__":
    main()
