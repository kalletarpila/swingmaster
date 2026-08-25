from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6i_production_rebuild import run_phase6i_production_rebuild, utc_stamp


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6I production rebuild and proving.")
    parser.add_argument("--v3-db", type=Path, default=Path("/home/kalle/projects/swingmaster/rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--score-artifact-root", type=Path, default=None)
    parser.add_argument("--lifecycle-artifact-root", type=Path, default=None)
    parser.add_argument("--no-durable-docs", action="store_true")
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6i_production_rebuild") / utc_stamp()
    print(f"production_db={args.v3_db.resolve()}")
    print("operation=Phase 6I controlled derived-data production apply")
    print("scope=v3_valuation,v3_score,v3_lifecycle schema and rows only")
    summary = run_phase6i_production_rebuild(
        v3_db=args.v3_db,
        osakedata_db=args.osakedata_db,
        artifact_root=artifact_root,
        score_artifact_root=args.score_artifact_root,
        lifecycle_artifact_root=args.lifecycle_artifact_root,
        write_durable_docs=not args.no_durable_docs,
    )
    print(summary["classification"])
    print(f"artifact_root={summary.get('artifact_root')}")
    print(f"production_writes_run1={{'valuation': {summary.get('run1', {}).get('valuation', {}).get('apply')}, 'score': {summary.get('run1', {}).get('score', {}).get('apply')}, 'lifecycle': {summary.get('run1', {}).get('lifecycle', {}).get('apply')}}}")
    print(f"idempotency_pass={summary.get('idempotency_pass')}")
    print(f"source_drift={summary.get('source_drift')}")
    print(f"next={summary.get('recommended_next_step')}")


if __name__ == "__main__":
    main()
