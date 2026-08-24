from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6_score_valuation_design import run_phase6_design, utc_stamp


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6 score and valuation design audit.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6_score_valuation_design") / utc_stamp()
    summary = run_phase6_design(v3_db=args.v3_db, osakedata_db=args.osakedata_db, artifact_root=artifact_root)
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"ev_ebit_computable={summary['coverage']['ev_ebit_computable']}")
    print(f"ev_ebit_meaningful={summary['coverage']['ev_ebit_meaningful']}")
    print(f"ev_ebitda_computable={summary['coverage']['ev_ebitda_computable']}")
    print(f"ev_ebitda_meaningful={summary['coverage']['ev_ebitda_meaningful']}")
    print(f"primary_score_ready={summary['coverage']['primary_score_ready']}")
    print(f"production_writes={summary['production_writes']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
