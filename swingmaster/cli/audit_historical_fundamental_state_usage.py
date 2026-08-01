from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.historical_state_integration_audit import (
    DEFAULT_PRICE_DB,
    DEFAULT_STATE_DB,
    asdict_audit,
    audit_state_fundamental_usage,
    default_output_root,
    resolve_dates,
    resolve_tickers,
    state_dependency_map,
    write_csv_atomic,
    write_json_atomic,
)
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit state usage of historical fundamental context")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--state-db", default=str(DEFAULT_STATE_DB))
    parser.add_argument("--price-db", default=str(DEFAULT_PRICE_DB))
    parser.add_argument("--market", default="usa")
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--tickers-file")
    parser.add_argument("--date")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--first-n", type=int)
    parser.add_argument("--sample-size", type=int)
    parser.add_argument("--random-seed", type=int, default=17)
    parser.add_argument("--include-percentiles", action="store_true")
    parser.add_argument("--include-valuation", action="store_true")
    parser.add_argument("--output-json")
    parser.add_argument("--output-csv")
    parser.add_argument("--output-summary-json")
    parser.add_argument("--progress-log")
    parser.add_argument("--output-root")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_root = validate_temp_path(Path(args.output_root)) if args.output_root else default_output_root()
    output_root.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(Path(args.fundamentals_db).expanduser().resolve())) as fundamentals_conn, sqlite3.connect(
        str(Path(args.state_db).expanduser().resolve())
    ) as state_conn, sqlite3.connect(str(Path(args.price_db).expanduser().resolve())) as price_conn:
        dates = resolve_dates(state_conn, date=args.date, date_from=args.date_from, date_to=args.date_to)
        tickers = resolve_tickers(
            state_conn,
            tickers=args.ticker,
            tickers_file=Path(args.tickers_file) if args.tickers_file else None,
            dates=dates,
            first_n=args.first_n,
            sample_size=args.sample_size,
            random_seed=args.random_seed,
        )
        summary, rows = audit_state_fundamental_usage(
            fundamentals_conn,
            state_conn,
            price_conn,
            tickers=tickers,
            dates=dates,
            market=args.market,
            include_percentiles=args.include_percentiles,
            include_valuation=args.include_valuation,
        )
    output_json = validate_temp_path(Path(args.output_json)) if args.output_json else output_root / "audit.json"
    output_csv = validate_temp_path(Path(args.output_csv)) if args.output_csv else output_root / "audit_rows.csv"
    summary_json = validate_temp_path(Path(args.output_summary_json)) if args.output_summary_json else output_root / "summary.json"
    progress_log = validate_temp_path(Path(args.progress_log)) if args.progress_log else output_root / "progress.log"
    payload = {
        "summary": asdict_audit(summary),
        "dependency_map": [asdict_audit(row) for row in state_dependency_map()],
        "rows": rows,
    }
    write_json_atomic(output_json, payload)
    write_json_atomic(summary_json, payload["summary"])
    write_csv_atomic(output_csv, rows)
    progress_log.parent.mkdir(parents=True, exist_ok=True)
    progress_log.write_text(f"rows={len(rows)}\nstatus=ok\n", encoding="utf-8")
    artifacts = {
        "output_json": str(output_json),
        "output_csv": str(output_csv),
        "output_summary_json": str(summary_json),
        "progress_log": str(progress_log),
    }
    if args.json:
        print(json.dumps({"summary": payload["summary"], "artifact_paths": artifacts}, indent=2, sort_keys=True))
    else:
        for key, value in payload["summary"].items():
            print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
