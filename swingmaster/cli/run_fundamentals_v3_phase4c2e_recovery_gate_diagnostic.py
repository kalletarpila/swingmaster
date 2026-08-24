from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c2e_recovery_gate_diagnostic import run_phase4c2e_recovery_gate_diagnostic


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4C-2E EBIT/EBITDA recovery gate diagnostic.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--component-db", type=Path, default=Path("temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db"))
    parser.add_argument("--simfin-dir", type=Path, default=Path("simfin"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase4c_2e_recovery_gate_diagnostic/20260824T_PHASE4C2E_RECOVERY_GATE_DIAGNOSTIC"))
    parser.add_argument("--baseline-artifact-root", type=Path, default=None)
    args = parser.parse_args()
    summary = run_phase4c2e_recovery_gate_diagnostic(
        v3_db=args.v3_db,
        component_db=args.component_db,
        simfin_dir=args.simfin_dir,
        artifact_root=args.artifact_root,
        baseline_artifact_root=args.baseline_artifact_root,
    )
    reproduction = summary["reproduction"]
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"ebit_missing={reproduction['ebit_missing']}")
    print(f"ebitda_missing={reproduction['ebitda_missing']}")
    print(f"quarterization_ready_ebit={reproduction['quarterization_ready_ebit_current_definition']}")
    print(f"canonical_ebit_plus_da={reproduction['canonical_ebit_plus_da_current_definition']}")
    print(f"derivable_ebit_plus_da={reproduction['derivable_ebit_plus_da_current_definition']}")
    print(f"auto_strong_ebit={reproduction['current_auto_strong_ebit']}")
    print(f"auto_strong_ebitda={reproduction['current_auto_strong_ebitda']}")
    print(f"current_444_result={summary['required_conclusions']['current_444_result']}")
    print(f"zero_ebit_result={summary['required_conclusions']['zero_ebit_fills']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
