from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_phase4a_completeness_audit import DEFAULT_RAWCANDLE_DB, run_phase4a_historical_completeness_audit, utc_stamp


def main() -> None:
    args = parse_args()
    root = Path(args.artifact_root) if args.artifact_root else Path("temp/fundamentals_v3_phase4a_historical_completeness_audit") / f"{utc_stamp()}_PHASE4A_HISTORICAL_COMPLETENESS_AUDIT"
    summary = run_phase4a_historical_completeness_audit(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        rawcandle_db=Path(args.rawcandle_db),
        artifact_root=root,
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4A historical completeness audit")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--rawcandle-db", default=str(DEFAULT_RAWCANDLE_DB))
    parser.add_argument("--artifact-root")
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "baseline": {
            "companies": summary["baseline"]["company_total"],
            "active": summary["baseline"]["active"],
            "inactive": summary["baseline"]["inactive"],
            "canonical_q": summary["baseline"]["coverage"]["canonical_q_total"],
            "core_ready": summary["baseline"]["coverage"]["core_ready_q"],
            "core_not_ready": summary["baseline"]["coverage"]["core_not_ready_q"],
            "publish_known": summary["baseline"]["coverage"]["publish_date_known"],
            "publish_null": summary["baseline"]["coverage"]["publish_date_null"],
        },
        "zero_q": summary["zero_q"],
        "recoverability": summary["recoverability"],
        "core_ready_uplift": summary["core_ready_uplift"],
        "phase4c": summary["phase4c"],
        "integrity": summary["integrity"],
        "gate": summary["gate"],
        "artifact_root": summary["artifact_root"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
