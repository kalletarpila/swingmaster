from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_legacy_backward_validation import PHASE3C_1C_ARTIFACT_ROOT, run_legacy_breakpoint_diagnostic


DEFAULT_ARTIFACT_ROOT = PHASE3C_1C_ARTIFACT_ROOT


def main() -> None:
    args = _parse_args()
    summary = run_legacy_breakpoint_diagnostic(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=Path(args.artifact_root),
    )
    print(json.dumps(_compact(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose Legacy backward-chain breakpoints from the 2018 V3 floor")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    return parser.parse_args()


def _compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "baseline_passed": summary["baseline_reconciliation"]["passed"],
        "historical_floor": summary["historical_floor"],
        "population": summary["population"],
        "anchor_accounting": summary["anchor_accounting"],
        "company_status_accounting": summary["company_status_accounting"],
        "mapping_risk": summary["mapping_risk"],
        "adjacent_summary": summary["adjacent_summary"],
        "breakpoint_summary": summary["breakpoint_summary"],
        "legacy_2018plus_classification": summary["legacy_2018plus_classification"],
        "phase3c2_ready_rows": summary["phase3c2_ready_rows"],
        "phase3c2_hold_rows": summary["phase3c2_hold_rows"],
        "phase3c2_expected_contribution": summary["phase3c2_expected_contribution"],
        "read_only_proof": summary["read_only_proof"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
