from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c2_company_formula_discovery import run_phase4c2_company_formula_discovery, utc_stamp


def main() -> None:
    args = parse_args()
    root = Path(args.artifact_root) if args.artifact_root else Path("temp/fundamentals_v3_phase4c_2_company_formula_discovery") / f"{utc_stamp()}_PHASE4C2_COMPANY_FORMULA_DISCOVERY"
    summary = run_phase4c2_company_formula_discovery(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=root,
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4C-2 company-specific formula discovery")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root")
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "baseline": summary["baseline"],
        "component_inventory": summary["component_inventory"],
        "ebit": summary["ebit"],
        "ebitda": summary["ebitda"],
        "metadata": summary["metadata"],
        "recovery_potential": summary["recovery_potential"],
        "safety": summary["safety"],
        "integrity": summary["integrity"],
        "artifact_root": summary["artifact_root"],
        "recommended_next_step": summary["recommended_next_step"],
    }


if __name__ == "__main__":
    main()
