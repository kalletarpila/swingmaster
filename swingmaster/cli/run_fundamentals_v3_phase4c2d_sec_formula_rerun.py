from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c2d_sec_formula_rerun import run_phase4c2d_sec_formula_rerun


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4C-2D SEC company formula discovery.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--component-db", type=Path, default=Path("temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db"))
    parser.add_argument("--simfin-dir", type=Path, default=Path("simfin"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase4c_2d_sec_formula_rerun/20260824T_PHASE4C2D_SEC_FORMULA_RERUN"))
    args = parser.parse_args()
    summary = run_phase4c2d_sec_formula_rerun(v3_db=args.v3_db, component_db=args.component_db, simfin_dir=args.simfin_dir, artifact_root=args.artifact_root)
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"known_ebit_targets={summary['baseline']['known_ebit_targets']}")
    print(f"known_ebitda_targets={summary['baseline']['known_ebitda_targets']}")
    print(f"strong_ebit_q1q3={summary['ebit_q1q3']['strong_q1q3']}")
    print(f"strong_ebitda_q1q3={summary['ebitda_q1q3']['strong_q1q3']}")
    print(f"strong_ebit_q4={summary['q4_fingerprints']['strong_ebit_q4']}")
    print(f"strong_ebitda_q4={summary['q4_fingerprints']['strong_ebitda_q4']}")
    print(f"auto_strong_ebit_fills={summary['recovery']['total_auto_strong_ebit_fills']}")
    print(f"auto_strong_ebitda_fills={summary['recovery']['total_auto_strong_ebitda_fills']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
