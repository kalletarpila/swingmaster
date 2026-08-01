from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.historical_percentile import (
    DEFAULT_METRIC,
    DEFAULT_OSAKEDATA_DB,
    MATERIAL_DIFFERENCE_THRESHOLD,
    asdict_result,
    audit_current_vs_historical_percentiles,
    default_output_root,
    default_recent_percentile_dates,
    write_csv_atomic,
    write_json_atomic,
)
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit current versus effective-date-safe historical fundamental percentiles")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--osakedata-db", default=str(DEFAULT_OSAKEDATA_DB))
    parser.add_argument("--market", default="usa")
    parser.add_argument("--date", action="append", default=[])
    parser.add_argument("--date-limit", type=int, default=3)
    parser.add_argument("--sample-size", type=int, default=250)
    parser.add_argument("--metric", default=DEFAULT_METRIC)
    parser.add_argument("--material-threshold", type=float, default=MATERIAL_DIFFERENCE_THRESHOLD)
    parser.add_argument("--output-root")
    parser.add_argument("--summary-json")
    parser.add_argument("--output-csv")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_root = validate_temp_path(Path(args.output_root)) if args.output_root else default_output_root()
    output_root.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(Path(args.fundamentals_db).expanduser().resolve())) as fundamentals_conn, sqlite3.connect(
        str(Path(args.osakedata_db).expanduser().resolve())
    ) as osakedata_conn:
        dates = sorted(set(args.date), reverse=True) if args.date else default_recent_percentile_dates(fundamentals_conn, args.date_limit)
        summary, detail_rows = audit_current_vs_historical_percentiles(
            fundamentals_conn,
            osakedata_conn,
            dates=dates,
            market=args.market,
            metric=args.metric,
            sample_size=args.sample_size,
            material_threshold=args.material_threshold,
        )
    payload = {"dates": dates, "summary": asdict_result(summary)}
    summary_path = validate_temp_path(Path(args.summary_json)) if args.summary_json else output_root / "summary.json"
    csv_path = validate_temp_path(Path(args.output_csv)) if args.output_csv else output_root / "audit_rows.csv"
    write_json_atomic(summary_path, payload)
    write_csv_atomic(csv_path, detail_rows)
    if args.json:
        print(json.dumps({**payload, "artifact_paths": {"summary_json": str(summary_path), "output_csv": str(csv_path)}}, indent=2, sort_keys=True))
    else:
        for key, value in payload["summary"].items():
            print(f"{key}: {value}")
        print("artifact_paths:")
        print(f"  summary_json: {summary_path}")
        print(f"  output_csv: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
