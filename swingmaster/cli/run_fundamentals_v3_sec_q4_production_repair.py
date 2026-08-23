from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.v3_sec_q4_production_repair import ARTIFACT_ROOT, run_sec_q4_production_repair, utc_stamp


def main() -> None:
    args = parse_args()
    artifact_root = Path(args.artifact_root) if args.artifact_root else ARTIFACT_ROOT / f"{utc_stamp()}_PHASE3C_6B1_SEC_Q4_REPAIR"
    summary = run_sec_q4_production_repair(
        v3_db=Path(args.v3_db),
        legacy_db=Path(args.legacy_db),
        v2_db=Path(args.v2_db),
        artifact_root=artifact_root,
        apply_production=args.apply,
    )
    print(json.dumps(compact(summary), indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair Fundamentals V3 SEC-Q4 canonical sequence defects")
    parser.add_argument("--v3-db", default="rc_fundamentals_v3.db")
    parser.add_argument("--legacy-db", default="fundamentals_usa.db")
    parser.add_argument("--v2-db", default="rc_fundamentals_v2.db")
    parser.add_argument("--artifact-root")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def compact(summary: dict) -> dict:
    return {
        "classification": summary["classification"],
        "run_id": summary["run_id"],
        "plan": summary.get("plan", {}),
        "dry_gate": summary.get("dry", {}).get("gate", {}),
        "production_rows": summary.get("production", {}).get("rows", {}),
        "post": summary.get("post", {}),
        "artifact_root": summary.get("artifact_root"),
    }


if __name__ == "__main__":
    main()
