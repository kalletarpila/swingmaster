from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase6f_valuation_engine import run_phase6f_valuation_engine, utc_stamp


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Phase 6F valuation engine dry-run and contract validation.")
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--no-durable-docs", action="store_true")
    args = parser.parse_args()
    artifact_root = args.artifact_root or Path("temp/fundamentals_v3_phase6f_valuation_engine") / utc_stamp()
    summary = run_phase6f_valuation_engine(
        v3_db=args.v3_db,
        osakedata_db=args.osakedata_db,
        artifact_root=artifact_root,
        write_durable_docs=not args.no_durable_docs,
    )
    dry = summary["dry_summary"]
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"model_version={summary['model_version']}")
    print(f"existing_valuation_rows={summary['existing_valuation_rows']}")
    print(f"migration_required={summary['migration_required']}")
    print(f"ttm_endpoints={dry['ttm_endpoints']}")
    print(f"endpoints_with_publish_date={dry['endpoints_with_publish_date']}")
    print(f"valuation_dates_resolved={dry['valuation_dates_resolved']}")
    print(f"target_prices_available={dry['target_prices_available']}")
    print(f"calculable_snapshots={dry['calculable_snapshots']}")
    print(f"ev_ebit_valid={dry['ev_ebit_valid']}")
    print(f"fcf_yield_valid={dry['fcf_yield_valid']}")
    print(f"ev_sales_valid={dry['ev_sales_valid']}")
    print(f"ev_ebitda_valid={dry['ev_ebitda_valid']}")
    print(f"pe_valid={dry['pe_valid']}")
    print(f"missing_publish_date={dry['missing_publish_date']}")
    print(f"missing_target_price={dry['missing_target_price']}")
    print(f"production_writes={summary['production_writes']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
