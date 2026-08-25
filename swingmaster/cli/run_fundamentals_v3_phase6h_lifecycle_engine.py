from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6h_lifecycle_engine import run_phase6h_lifecycle_engine, utc_stamp


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6H lifecycle engine dry-run.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--lifecycle-artifact-root", type=Path, default=None)
    parser.add_argument("--no-durable-docs", action="store_true")
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6h_lifecycle_engine") / utc_stamp()
    summary = run_phase6h_lifecycle_engine(
        v3_db=args.v3_db,
        artifact_root=artifact_root,
        lifecycle_artifact_root=args.lifecycle_artifact_root,
        write_durable_docs=not args.no_durable_docs,
    )
    print(summary["classification"])
    print(f"artifact_root={summary.get('artifact_root')}")
    print(f"model_version={summary.get('model_version')}")
    print(f"actual_fingerprint={summary.get('actual_fingerprint')}")
    print(f"states={summary.get('states')}")
    print(f"ebit_primary={summary.get('ebit_primary')}")
    print(f"requires_ebitda={summary.get('requires_ebitda')}")
    print(f"uses_score={summary.get('uses_score')}")
    print(f"uses_valuation={summary.get('uses_valuation')}")
    dry = summary.get("dry_summary") or {}
    print(f"endpoints={dry.get('endpoints')}")
    print(f"lifecycle_ready={dry.get('lifecycle_ready')}")
    print(f"not_ready={dry.get('not_ready')}")
    print(f"production_writes={summary.get('production_writes')}")
    print(f"next={summary.get('recommended_next_step')}")


if __name__ == "__main__":
    main()
