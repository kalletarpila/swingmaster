from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c2f_gate_refinement import run_phase4c2f_gate_refinement


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4C-2F formula gate refinement.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--component-db", type=Path, default=Path("temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db"))
    parser.add_argument("--simfin-dir", type=Path, default=Path("simfin"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase4c_2f_gate_refinement/20260824T_PHASE4C2F_GATE_REFINEMENT"))
    args = parser.parse_args()
    summary = run_phase4c2f_gate_refinement(v3_db=args.v3_db, component_db=args.component_db, simfin_dir=args.simfin_dir, artifact_root=args.artifact_root)
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"ebit_missing={summary['baseline']['ebit_missing']}")
    print(f"ebitda_missing={summary['baseline']['ebitda_missing']}")
    print(f"current_core_ready={summary['baseline']['core_ready']}")
    print(f"final_refined_ebit={summary['recovery_comparison']['final_refined']['ebit']}")
    print(f"final_refined_ebitda={summary['recovery_comparison']['final_refined']['ebitda']}")
    print(f"final_refined_core_uplift={summary['recovery_comparison']['final_refined']['core_uplift']}")
    print(f"backtest_ebit_gt_5pct={summary['backtest']['ebit_gt_5pct']}")
    print(f"backtest_ebitda_gt_5pct={summary['backtest']['ebitda_gt_5pct']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
