from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10e_one_year_period_shift import (
    Phase8A10EPaths,
    run_phase8a10e,
    utc_stamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run read-only Phase 8A10E one-year period_end shift analysis.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--official-timeline-csv", default="temp/phase8_global_P1_official_fiscal_timelines.csv")
    parser.add_argument("--a10b-root", default="temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    parser.add_argument("--a10d-root", default="temp/fundamentals_v3_phase8a10d_r_segment_reconciliation/20260826T171500Z")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--publish-apply-root", default="temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10e_one_year_period_shift" / utc_stamp()
    )
    summary = run_phase8a10e(
        Phase8A10EPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            official_timeline_csv=Path(args.official_timeline_csv),
            a10b_root=Path(args.a10b_root),
            a10d_root=Path(args.a10d_root),
            rawcandle_db=Path(args.rawcandle_db),
            publish_apply_root=Path(args.publish_apply_root),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"ticker_count={summary['starting_state']['ticker_count']}")
    print(f"nine_ticker_P1_before={summary['a10b_reaudit']['nine_ticker_P1_before']}")
    print(f"nine_ticker_P1_after={summary['a10b_reaudit']['nine_ticker_P1_after']}")
    print(f"global_P1_before={summary['a10b_reaudit']['global_P1_before']}")
    print(f"global_P1_after={summary['a10b_reaudit']['global_P1_after']}")
    print(f"repair_rows={summary['frozen_repair']['repair_rows']}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
