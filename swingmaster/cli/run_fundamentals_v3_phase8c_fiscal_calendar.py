from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_fiscal_calendar import Phase8CPaths, run_phase8c, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8C V3 fiscal-calendar metadata import and read-only validation.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--input-csv", type=Path, default=Path("temp/v3_active_tickers_all.csv"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--phase8b-artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8b_downstream_rebuild/20260827T_PHASE8B"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase8c_fiscal_calendar_metadata") / utc_stamp()
    summary = run_phase8c(
        Phase8CPaths(
            artifact_root=artifact_root,
            input_csv=args.input_csv,
            v3_db=args.v3_db,
            phase8b_artifact_root=args.phase8b_artifact_root,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"companies_before={summary['baseline_before']['companies']}")
    print(f"companies_after={summary['baseline_after']['companies']}")
    print(f"active_before={summary['baseline_before']['active_companies']}")
    print(f"active_after={summary['baseline_after']['active_companies']}")
    print(f"canonical_rows_after={summary['baseline_after']['canonical_quarter_rows']}")
    print(f"ttm_rows_after={summary['baseline_after']['ttm_rows']}")
    print(f"score_rows_after={summary['baseline_after']['score_rows']}")
    print(f"lifecycle_rows_after={summary['baseline_after']['lifecycle_rows']}")
    print(f"valuation_rows_after={summary['baseline_after']['valuation_rows']}")
    print(f"profile_rows_inserted={summary['metadata_writes']['profile_rows_inserted']}")
    print(f"anchor_rows_inserted={summary['metadata_writes']['anchor_rows_inserted']}")
    print(f"idempotent_rerun={summary['metadata_writes']['idempotent_rerun']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
