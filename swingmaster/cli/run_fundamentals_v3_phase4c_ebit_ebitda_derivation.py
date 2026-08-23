from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c_ebit_ebitda_derivation import run_phase4c_ebit_ebitda_research, utc_stamp


def main() -> None:
    args = parse_args()
    root = Path(args.artifact_root) if args.artifact_root else Path("temp/fundamentals_v3_phase4c_ebit_ebitda_derivation") / f"{utc_stamp()}_PHASE4C_EBIT_EBITDA_DERIVATION"
    summary = run_phase4c_ebit_ebitda_research(
        v3_db=Path(args.v3_db),
        v2_db=Path(args.v2_db),
        legacy_db=Path(args.legacy_db),
        artifact_root=root,
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4C EBIT/EBITDA derivation research")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--artifact-root")
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "baseline": summary["baseline"],
        "canonical_ebit": {
            "ebit_equals_operating_income": summary["canonical_ebit"]["ebit_equals_operating_income"],
            "alternative_rules": summary["canonical_ebit"]["alternative_rules"],
        },
        "canonical_ebitda": {
            "ebit_plus_da": summary["canonical_ebitda"]["ebit_plus_da"],
            "operating_income_plus_da": summary["canonical_ebitda"]["operating_income_plus_da"],
        },
        "candidate_classification": summary["candidate_classification"],
        "production_apply": summary["production_apply"],
        "expected_impact": summary["expected_impact"],
        "integrity": summary["integrity"],
        "artifact_root": summary["artifact_root"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
