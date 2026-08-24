from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase5_ttm_engine import run_phase5_ttm_engine


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 5 EBIT-first TTM rebuild.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase5_ttm_engine/20260824T_PHASE5_TTM_ENGINE"))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    summary = run_phase5_ttm_engine(v3_db=args.v3_db, artifact_root=args.artifact_root, apply=not args.dry_run, run_id=args.run_id)
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"run_id={summary.get('run_id', '')}")
    if "dry_rebuild" in summary:
        print(f"total_endpoints={summary['dry_rebuild']['total_endpoints']}")
        print(f"core_ttm_ebit_ready={summary['dry_rebuild']['core_ttm_ebit_ready']}")
        print(f"core_ttm_ebitda_ready={summary['dry_rebuild']['core_ttm_ebitda_ready']}")
        print(f"ttm_pit_ready={summary['dry_rebuild']['ttm_pit_ready']}")
    if "production" in summary:
        print(f"v3_ttm_rows={summary['production'].get('v3_ttm_rows', 0)}")
        print(f"idempotent_second_run_changes={summary['production'].get('idempotent_second_run_changes', '')}")
    print(f"next={summary.get('recommended_next_step', '')}")


if __name__ == "__main__":
    main()
