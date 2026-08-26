from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase8a10e_r_latest8q_mapping import Phase8A10ERPaths, run_phase8a10e_r, utc_stamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8A10E-R latest-8Q official mapping rehearsal.")
    parser.add_argument("--artifact-root", default=None)
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--official-latest8q-csv", default="temp/swingmaster_v3_official_fiscal_quarter_timeline_2026-08-26.csv")
    parser.add_argument("--supplemental-fingerprint-csv", default="temp/phase8_global_P1_official_fiscal_timelines.csv")
    parser.add_argument("--a10b-root", default="temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    parser.add_argument("--rawcandle-db", default="/home/kalle/projects/rawcandle/data/osakedata.db")
    parser.add_argument("--publish-apply-root", default="temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_root = (
        Path(args.artifact_root)
        if args.artifact_root
        else Path("temp") / "fundamentals_v3_phase8a10e_r_latest8q_mapping" / utc_stamp()
    )
    summary = run_phase8a10e_r(
        Phase8A10ERPaths(
            artifact_root=artifact_root,
            v3_db=Path(args.v3_db),
            official_latest8q_csv=Path(args.official_latest8q_csv),
            supplemental_fingerprint_csv=Path(args.supplemental_fingerprint_csv),
            a10b_root=Path(args.a10b_root),
            rawcandle_db=Path(args.rawcandle_db),
            publish_apply_root=Path(args.publish_apply_root),
        )
    )
    print(f"classification={summary['classification']}")
    print(f"csv_rows={summary['external_input']['rows']}")
    print(f"tickers={summary['external_input']['ticker_count']}")
    print(f"nine_ticker_P1_before={summary['a10b_reaudit']['nine_ticker_P1_before']}")
    print(f"nine_ticker_P1_after={summary['a10b_reaudit']['nine_ticker_P1_after']}")
    print(f"global_P1_before={summary['a10b_reaudit']['global_P1_before']}")
    print(f"global_P1_after={summary['a10b_reaudit']['global_P1_after']}")
    print(f"production_ready_groups={summary['frozen_repair']['production_ready_ticker_groups']}")
    print(f"blocked_groups={summary['frozen_repair']['blocked_ticker_groups']}")
    print(f"production_writes={summary['safety']['production_writes']}")
    print(f"rawcandle_writes={summary['safety']['rawcandle_writes']}")
    print(f"artifact_root={artifact_root}")
    print(f"next_action={summary['next_action']}")


if __name__ == "__main__":
    main()
