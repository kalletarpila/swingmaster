from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase4d_historical_completeness_closure import run_phase4d_historical_completeness_closure


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 4D historical completeness closure.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--component-db", type=Path, default=Path("temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase4d_historical_completeness_closure/20260824T_PHASE4D_HISTORICAL_COMPLETENESS_CLOSURE"))
    args = parser.parse_args()
    summary = run_phase4d_historical_completeness_closure(v3_db=args.v3_db, component_db=args.component_db, artifact_root=args.artifact_root)
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"companies={summary['baseline']['companies']}")
    print(f"canonical_q={summary['baseline']['canonical_q']}")
    print(f"core_ready={summary['baseline']['core_ready']}")
    print(f"core_not_ready={summary['baseline']['core_not_ready']}")
    print(f"publish_known={summary['baseline']['publish_known']}")
    print(f"publish_null={summary['baseline']['publish_null']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
