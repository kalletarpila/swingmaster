from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.sec_edgar import SEC_USER_AGENT
from swingmaster.fundamentals.v3_sec_component_layer import (
    ARTIFACT_ROOT_DEFAULT,
    COMPONENT_DB_DEFAULT,
    RAW_CACHE_DEFAULT,
    acquire_sec_components,
    close_sec_component_phase,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Acquire and validate SEC component facts for Fundamentals V3.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--component-db", type=Path, default=COMPONENT_DB_DEFAULT)
    parser.add_argument("--raw-cache-dir", type=Path, default=RAW_CACHE_DEFAULT)
    parser.add_argument("--artifact-root", type=Path, default=ARTIFACT_ROOT_DEFAULT)
    parser.add_argument("--user-agent", default=SEC_USER_AGENT)
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--tickers", default="", help="Comma-separated ticker list.")
    parser.add_argument("--calibration", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--analysis-only", action="store_true")
    parser.add_argument("--rate-limit-seconds", type=float, default=0.11)
    args = parser.parse_args()

    tickers = list(args.ticker)
    if args.tickers:
        tickers.extend(t.strip().upper() for t in args.tickers.split(",") if t.strip())
    if args.analysis_only:
        summary = close_sec_component_phase(
            v3_db=args.v3_db,
            component_db=args.component_db,
            raw_cache_dir=args.raw_cache_dir,
            artifact_root=args.artifact_root,
            user_agent=args.user_agent,
        )
    else:
        summary = acquire_sec_components(
            v3_db=args.v3_db,
            component_db=args.component_db,
            raw_cache_dir=args.raw_cache_dir,
            artifact_root=args.artifact_root,
            user_agent=args.user_agent,
            tickers=tickers or None,
            calibration=args.calibration,
            limit=args.limit,
            plan_only=args.plan_only,
            rate_limit_seconds=args.rate_limit_seconds,
        )
    print(summary["classification"])
    print(f"component_db={summary['component_db']}")
    print(f"raw_cache={summary['architecture']['raw_cache_path']}")
    print(f"facts={summary['storage']['normalized_fact_count']}")
    print(f"fetch_ok={summary['universe']['fetch_ok']}")
    print(f"failed={summary['universe']['failed']}")
    print(f"pretax_companies={summary['pretax']['companies_with_pretax']}")
    print(f"interest_companies={summary['interest']['companies_with_any_interest']}")
    print(f"da_companies={summary['da']['companies_with_any_da']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
