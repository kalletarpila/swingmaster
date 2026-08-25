from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6cr_score_architecture_reconciliation import (
    run_phase6cr_reconciliation,
    utc_stamp,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6C-R score architecture reconciliation.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--no-durable-docs", action="store_true")
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6cr_score_architecture_reconciliation") / utc_stamp()
    summary = run_phase6cr_reconciliation(
        v3_db=args.v3_db,
        artifact_root=artifact_root,
        write_durable_docs=not args.no_durable_docs,
    )
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"development_observations={summary['development_observations']}")
    print(f"validation_2024_observations={summary['validation_2024_observations']}")
    print(f"oos_2025_observations={summary['oos_2025_observations']}")
    print(f"score_ready_development={summary['score_ready_development']}")
    print(f"score_ready_2024={summary['score_ready_2024']}")
    print(f"score_ready_2025={summary['score_ready_2025']}")
    print(f"fingerprint={summary['fingerprint']}")
    print(f"production_writes={summary['production_writes']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
