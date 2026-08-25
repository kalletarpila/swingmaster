from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6g_legacy2_score_engine import run_phase6g_score_engine, utc_stamp


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6G Legacy 2.0 score engine dry-run.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--score-artifact-root", type=Path, default=None)
    parser.add_argument("--no-durable-docs", action="store_true")
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6g_legacy2_score_engine") / utc_stamp()
    summary = run_phase6g_score_engine(
        v3_db=args.v3_db,
        artifact_root=artifact_root,
        score_artifact_root=args.score_artifact_root,
        write_durable_docs=not args.no_durable_docs,
    )
    print(summary["classification"])
    print(f"artifact_root={summary.get('artifact_root')}")
    print(f"model_version={summary.get('model_version')}")
    print(f"score_fingerprint_actual={summary.get('score_fingerprint_actual')}")
    print(f"total_max={summary.get('total_max')}")
    print(f"market_price_inputs={summary.get('market_price_inputs')}")
    dry = summary.get("dry_summary") or {}
    print(f"endpoints={dry.get('endpoints')}")
    print(f"applicable={dry.get('applicable')}")
    print(f"score_ready={dry.get('score_ready')}")
    print(f"not_ready={dry.get('not_ready')}")
    print(f"not_applicable={dry.get('not_applicable')}")
    print(f"production_writes={summary.get('production_writes')}")
    print(f"next={summary.get('recommended_next_step')}")


if __name__ == "__main__":
    main()
