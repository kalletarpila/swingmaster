from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c3_production_apply import run_phase4c3_production_apply


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply approved Fundamentals V3 Phase 4C-3 EBIT/EBITDA recovery plan.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--component-db", type=Path, default=Path("temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db"))
    parser.add_argument("--plan-path", type=Path, default=Path("temp/fundamentals_v3_phase4c_2f_gate_refinement/20260824T_PHASE4C2F_GATE_REFINEMENT/phase4c3_ebit_ebitda_production_apply_plan.csv"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase4c_3_production_apply/20260824T_PHASE4C3_PRODUCTION_APPLY"))
    parser.add_argument("--dry-only", action="store_true")
    args = parser.parse_args()
    summary = run_phase4c3_production_apply(
        v3_db=args.v3_db,
        component_db=args.component_db,
        plan_path=args.plan_path,
        artifact_root=args.artifact_root,
        apply_production=not args.dry_only,
    )
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"run_id={summary['run_id']}")
    print(f"plan_hash={summary['plan']['hash']}")
    print(f"planned_rows={summary['plan']['rows']}")
    print(f"ebit_applied={summary['production']['ebit_writes']}")
    print(f"ebitda_applied={summary['production']['ebitda_writes']}")
    print(f"q4_applied={summary['production']['q4_writes']}")
    print(f"core_ready={summary['final_baseline']['core_ready']}")
    print(f"core_uplift={summary['final_baseline']['core_ready_uplift']}")
    print(f"second_run_ebit={summary['idempotency']['second_run_ebit_writes']}")
    print(f"second_run_ebitda={summary['idempotency']['second_run_ebitda_writes']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
