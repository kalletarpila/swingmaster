from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c3c_bounded_refinement import run_phase4c3c_bounded_refinement


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4C-3C bounded rejection-logic refinement.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--component-db", type=Path, default=Path("temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase4c_3c_bounded_rejection_refinement/20260824T_PHASE4C3C_BOUNDED_REJECTION_REFINEMENT"))
    args = parser.parse_args()
    summary = run_phase4c3c_bounded_refinement(v3_db=args.v3_db, component_db=args.component_db, artifact_root=args.artifact_root)
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"bounded_rows={summary['bounded_population']['total_rows']}")
    print(f"multiple_interest_rows={summary['bounded_population']['multiple_interest_rows']}")
    print(f"da_conflict_rows={summary['bounded_population']['da_conflict_rows']}")
    print(f"additional_ebit_fills={summary['recovery_impact']['additional_ebit_fills']}")
    print(f"additional_ebitda_fills={summary['recovery_impact']['additional_ebitda_fills']}")
    print(f"additional_core_ready_uplift={summary['recovery_impact']['additional_core_ready_uplift']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
