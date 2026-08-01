from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.historical_snapshot import DEFAULT_PRICE_DB
from swingmaster.fundamentals.historical_state_integration_audit import DEFAULT_STATE_DB
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path
from swingmaster.research.historical_fundamental_context import (
    asdict_context,
    audit_historical_state_fundamental_context,
    build_historical_state_rows,
    default_output_root,
    enrich_historical_state_rows,
    resolve_dates,
    resolve_tickers,
    write_csv_atomic,
    write_json_atomic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect historical SwingMaster state rows")
    parser.add_argument("--state-db", default=str(DEFAULT_STATE_DB))
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
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
    parser.add_argument("--include-historical-fundamentals", action="store_true")
    parser.add_argument("--no-percentile", action="store_true")
    parser.add_argument("--no-valuation", action="store_true")
    parser.add_argument("--audit-output-root")
    parser.add_argument("--output-json")
    parser.add_argument("--output-csv")
    parser.add_argument("--output-summary-json")
    parser.add_argument("--progress-log")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    state_path = Path(args.state_db).expanduser().resolve()
    include_percentile = not args.no_percentile
    include_valuation = not args.no_valuation
    with _readonly_connect(state_path) as state_conn:
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
        rows = build_historical_state_rows(state_conn, tickers=tickers, dates=dates)
        summary = None
        if args.include_historical_fundamentals:
            with _readonly_connect(Path(args.fundamentals_db).expanduser().resolve()) as fundamentals_conn, _readonly_connect(
                Path(args.price_db).expanduser().resolve()
            ) as price_conn:
                rows, _timings, _cache = enrich_historical_state_rows(
                    rows,
                    fundamentals_conn,
                    price_conn,
                    market=args.market,
                    include_percentile=include_percentile,
                    include_valuation=include_valuation,
                )
                if args.audit_output_root:
                    summary, rows = audit_historical_state_fundamental_context(
                        state_conn,
                        fundamentals_conn,
                        price_conn,
                        tickers=tickers,
                        dates=dates,
                        market=args.market,
                        include_percentile=include_percentile,
                        include_valuation=include_valuation,
                    )
    payload: dict[str, object] = {"rows": rows}
    if summary is not None:
        payload["summary"] = asdict_context(summary)
        _write_audit_artifacts(args, payload, rows)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for row in rows:
            print(f"{row['ticker']} {row['date']} state={row['state']} reasons={','.join(row['reason_codes'])}")
            if "fundamental_context" in row:
                context = row["fundamental_context"]
                print(
                    "  fundamental_context_status="
                    f"{context['fundamental_context_status']} source_ttm={context['fundamental_source_ttm_as_of_date']}"
                )
    return 0


def _write_audit_artifacts(args: argparse.Namespace, payload: dict[str, object], rows: list[dict[str, object]]) -> None:
    output_root = validate_temp_path(Path(args.audit_output_root)) if args.audit_output_root else default_output_root()
    output_root.mkdir(parents=True, exist_ok=True)
    output_json = validate_temp_path(Path(args.output_json)) if args.output_json else output_root / "audit.json"
    output_csv = validate_temp_path(Path(args.output_csv)) if args.output_csv else output_root / "audit_rows.csv"
    summary_json = validate_temp_path(Path(args.output_summary_json)) if args.output_summary_json else output_root / "summary.json"
    progress_log = validate_temp_path(Path(args.progress_log)) if args.progress_log else output_root / "progress.log"
    write_json_atomic(output_json, payload)
    write_json_atomic(summary_json, payload["summary"])
    write_csv_atomic(output_csv, rows)
    progress_log.parent.mkdir(parents=True, exist_ok=True)
    progress_log.write_text(f"rows={len(rows)}\nstatus=ok\n", encoding="utf-8")


def _readonly_connect(path: Path) -> sqlite3.Connection:
    resolved = path.resolve()
    conn = sqlite3.connect(f"file:{resolved.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


if __name__ == "__main__":
    raise SystemExit(main())
