from __future__ import annotations

import argparse
from pathlib import Path

from swingmaster.fundamentals.v3_phase4c2b_simfin_validation import run_phase4c2b_simfin_validation


def main() -> None:
    parser = argparse.ArgumentParser(description="Run read-only Fundamentals V3 Phase 4C-2B SimFin validation.")
    parser.add_argument("--v3-db", type=Path, default=Path("fundamentals_usa.db"))
    parser.add_argument("--simfin-dir", type=Path, default=Path("simfin"))
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=Path("temp/fundamentals_v3_phase4c_2b_simfin_validation/20260823T_PHASE4C2B_SIMFIN_VALIDATION"),
    )
    args = parser.parse_args()

    summary = run_phase4c2b_simfin_validation(v3_db=args.v3_db, simfin_dir=args.simfin_dir, artifact_root=args.artifact_root)
    multifield_fills = sum(row["safe_simfin_fills"] for row in summary["dry_recovery"]["multifield"].values())
    print(summary["classification"])
    print(f"artifact_root={summary['artifact_root']}")
    print(f"canonical_financial_writes={summary['safety']['canonical_financial_writes']}")
    print(f"simfin_multifield_safe_dry_fills={multifield_fills}")
    print(f"strong_ebit_fingerprints={summary['ebit']['strong']}")
    print(f"strong_ebitda_fingerprints={summary['ebitda']['strong']}")
    print(f"next={summary['recommended_next_step']}")


if __name__ == "__main__":
    main()
