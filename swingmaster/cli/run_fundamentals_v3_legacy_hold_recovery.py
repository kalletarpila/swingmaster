from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_legacy_hold_recovery import PHASE3C_1D_ARTIFACT_ROOT, run_legacy_hold_recovery


def main() -> None:
    args = _parse_args()
    summary = run_legacy_hold_recovery(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=Path(args.artifact_root),
    )
    print(json.dumps(_compact(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover Legacy 2018+ HOLD rows using SEC Q4 structure")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root", default=str(PHASE3C_1D_ARTIFACT_ROOT))
    return parser.parse_args()


def _compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "baseline_passed": summary["baseline_reconciliation"]["passed"],
        "source_shape": {k: v for k, v in summary["source_shape"].items() if k != "period_type_statement_counts"},
        "q4_presence": summary["q4_presence"],
        "q4_accuracy": summary["q4_accuracy"],
        "transition_reanalysis": summary["transition_reanalysis"],
        "v2_calibration": summary["v2_calibration"],
        "v2_help": summary["v2_help"],
        "legacy_only_recovery": summary["legacy_only_recovery"],
        "final_classification": summary["final_classification"],
        "phase3c2_ready_rows": summary["phase3c2_ready_rows"],
        "phase3c2_hold_rows": summary["phase3c2_hold_rows"],
        "phase3c2_expected_contribution": summary["phase3c2_expected_contribution"],
        "phase3c2_sequence_violations": summary["phase3c2_sequence_violations"],
        "phase3c2b_repair_opportunity": summary["phase3c2b_repair_opportunity"],
        "read_only_proof": summary["read_only_proof"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
