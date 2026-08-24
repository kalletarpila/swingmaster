from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c3b_rejected_case_review import run_phase4c3b_rejected_case_review


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4C-3B rejected EBIT/EBITDA case review.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--component-db", type=Path, default=Path("temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase4c_3b_rejected_case_review/20260824T_PHASE4C3B_REJECTED_CASE_REVIEW"))
    parser.add_argument("--sample-size", type=int, default=15)
    args = parser.parse_args()
    summary = run_phase4c3b_rejected_case_review(v3_db=args.v3_db, component_db=args.component_db, artifact_root=args.artifact_root, sample_size=args.sample_size)
    print(summary["outcome"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"ebit_missing={summary['baseline']['ebit_missing']}")
    print(f"ebitda_missing={summary['baseline']['ebitda_missing']}")
    print(f"selected_cases={summary['selection']['selected_cases']}")
    print(f"ebit_cases={summary['selection']['ebit_cases']}")
    print(f"ebitda_cases={summary['selection']['ebitda_cases']}")
    print(f"q4_cases={summary['selection']['q4_cases']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
