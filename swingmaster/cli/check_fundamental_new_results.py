from __future__ import annotations

import argparse
import json
from pathlib import Path

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.result_check import (
    DEFAULT_OHLCV_DB_PATH,
    DEFAULT_OHLCV_STALE_DAYS,
    run_manual_result_check,
    validate_temp_path,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual USA fundamentals new-results check")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ohlcv-db", default=str(DEFAULT_OHLCV_DB_PATH))
    parser.add_argument("--decision-date", required=True)
    parser.add_argument("--ohlcv-stale-days", type=int, default=DEFAULT_OHLCV_STALE_DAYS)
    parser.add_argument("--event-watch-days-after", type=int, default=5)
    parser.add_argument("--output-root")
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--tickers-file")
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    tickers = list(args.ticker or [])
    if args.tickers_file:
        path = validate_temp_path(Path(args.tickers_file), must_exist=True)
        tickers.extend(line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    result = run_manual_result_check(
        fundamentals_db=Path(args.fundamentals_db),
        ohlcv_db=Path(args.ohlcv_db),
        decision_date=args.decision_date,
        ohlcv_stale_days=args.ohlcv_stale_days,
        event_watch_days_after=args.event_watch_days_after,
        output_root=Path(args.output_root) if args.output_root else None,
        tickers=tickers or None,
    )
    summary = dict(result["summary"])
    summary.update(
        {
            "check_status": result["check_status"],
            "candidate_count": result["plan"]["candidate_count"],
            "candidate_hash": result["plan"]["candidate_hash"],
            "plan_json": result["artifact_paths"]["plan_json"],
            "candidates_csv": result["artifact_paths"]["candidates_csv"],
            "manual_review_csv": result["artifact_paths"]["manual_review_csv"],
            "output_root": str(Path(result["artifact_paths"]["plan_json"]).parent),
        }
    )
    payload = {
        "check_status": result["check_status"],
        "summary": summary,
        "artifact_paths": result["artifact_paths"],
        "stages": result["stages"],
    }
    if args.json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for key, value in payload["summary"].items():
            print(f"SUMMARY {key}={value}")
        for key, value in payload["artifact_paths"].items():
            print(f"ARTIFACT {key}={value}")
    return 0 if result["check_status"] == "SUCCESS" else 2 if result["check_status"] == "PARTIAL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
