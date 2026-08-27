from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10e_r3_latest8q_reconstruction import Phase8A10ER3Paths, run_phase8a10e_r3, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8A10E-R3 latest-8Q clean segment reconstruction.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--financial-timeline-csv", default="temp/swingmaster_v3_official_fiscal_quarter_timeline_with_financials_2026-08-26.csv")
    parser.add_argument("--a10b-root", default="temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    parser.add_argument("--publish-apply-root", default="temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = Path(args.artifact_root) if args.artifact_root else Path("temp") / "fundamentals_v3_phase8a10e_r3_latest8q_reconstruction" / utc_stamp()
    summary = run_phase8a10e_r3(
        Phase8A10ER3Paths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            financial_timeline_csv=Path(args.financial_timeline_csv),
            a10b_root=Path(args.a10b_root),
            publish_apply_root=Path(args.publish_apply_root),
            rawcandle_db=Path(args.rawcandle_db),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"official_rows={summary['input']['rows']}")
    print(f"tickers={summary['input']['ticker_count']}")
    print(f"targets_total={summary['reconstruction']['targets_total']}")
    print(f"reused_current_rows={summary['reconstruction']['reused_current_rows']}")
    print(f"reused_with_metadata_repair={summary['reconstruction']['reused_with_metadata_repair']}")
    print(f"reused_with_identity_repair={summary['reconstruction']['reused_with_identity_repair']}")
    print(f"source_insufficient_targets={summary['reconstruction']['source_insufficient_targets']}")
    print(f"ready_ticker_groups={summary['frozen_replacement']['ready_ticker_groups']}")
    print(f"blocked_tickers={','.join(summary['frozen_replacement']['blocked_tickers'])}")
    print(f"nine_ticker_P1_before={summary['a10b']['nine_ticker_P1_before']}")
    print(f"nine_ticker_P1_after={summary['a10b']['nine_ticker_P1_after']}")
    print(f"global_P1_before={summary['a10b']['global_P1_before']}")
    print(f"global_P1_after={summary['a10b']['global_P1_after']}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()

