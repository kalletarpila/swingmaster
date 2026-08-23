from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_sec_q4_field_semantics import PHASE3C_1E_ARTIFACT_ROOT, run_sec_q4_field_semantics_validation


def main() -> None:
    args = _parse_args()
    summary = run_sec_q4_field_semantics_validation(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=Path(args.artifact_root),
        phase3c1d_root=Path(args.phase3c1d_root),
    )
    print(json.dumps(_compact(summary), indent=2, sort_keys=True))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate SEC Q4 field semantics and Phase 3C-2 source policy")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root", default=str(PHASE3C_1E_ARTIFACT_ROOT))
    parser.add_argument("--phase3c1d-root", default="temp/fundamentals_v3_phase3c_1d_legacy_hold_recovery/20260822T_PHASE3C_1D_LEGACY_HOLD_RECOVERY")
    return parser.parse_args()


def _compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "known_q4_calibration_population": summary["known_q4_calibration_population"],
        "expected_q4_coverage": summary["expected_q4_coverage"],
        "expected_core_readiness": summary["expected_core_readiness"],
        "read_only_proof": summary["read_only_proof"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
