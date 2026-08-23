from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c2b0_sec_source_layer_audit import run_phase4c2b0_sec_source_layer_audit


def main() -> None:
    args = parse_args()
    summary = run_phase4c2b0_sec_source_layer_audit(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        simfin_dir=Path(args.simfin_dir),
        phase4c2_root=Path(args.phase4c2_root),
        artifact_root=Path(args.artifact_root),
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4C-2B0 SEC source-layer audit")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--simfin-dir", default="simfin")
    parser.add_argument("--phase4c2-root", default="temp/fundamentals_v3_phase4c_2_company_formula_discovery/20260823T_PHASE4C2_COMPANY_FORMULA_DISCOVERY")
    parser.add_argument("--artifact-root", default="temp/fundamentals_v3_phase4c_2b0_sec_source_layer_audit/20260823T_PHASE4C2B0_SEC_SOURCE_LAYER_AUDIT")
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "prior_claim_classification": summary["prior_claim_classification"],
        "phase4c2_actual_source_statement": summary["phase4c2_actual_source_statement"],
        "baseline": summary["baseline"],
        "local_sec_architecture": summary["local_sec_architecture"],
        "component_coverage": summary["component_coverage"],
        "missing_ebit_population": summary["missing_ebit_population"],
        "missing_ebitda_population": summary["missing_ebitda_population"],
        "simfin_comparison": summary["simfin_comparison"],
        "recommendation": summary["recommendation"],
        "safety": summary["safety"],
        "integrity": summary["integrity"],
        "artifact_root": summary["artifact_root"],
    }


if __name__ == "__main__":
    main()
