from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.quarter_refresh_decision import (
    DEFAULT_OHLCV_DB_PATH,
    DEFAULT_OHLCV_STALE_DAYS,
    PRIORITY_ORDER,
    build_quarter_refresh_decisions,
    open_readonly_db,
    summarize_quarter_refresh_decisions,
    temp_root,
    utc_timestamp,
    validate_temp_path,
    write_csv_atomic,
    write_decision_artifacts,
    write_json_atomic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only USA quarterly fundamentals refresh decision audit")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ohlcv-db", default=str(DEFAULT_OHLCV_DB_PATH))
    parser.add_argument("--market", default="usa")
    parser.add_argument("--decision-date", required=True)
    parser.add_argument("--ohlcv-stale-days", type=int, default=DEFAULT_OHLCV_STALE_DAYS)
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--tickers-file")
    parser.add_argument("--decision", action="append", default=[])
    parser.add_argument("--min-priority", choices=sorted(PRIORITY_ORDER), default=None)
    parser.add_argument("--output-root")
    parser.add_argument("--output-csv")
    parser.add_argument("--summary-json")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_root = _output_root(args.output_root)
    tickers = _selected_tickers(args)

    with open_readonly_db(Path(args.fundamentals_db)) as conn, open_readonly_db(Path(args.ohlcv_db)) as ohlcv_conn:
        rows = build_quarter_refresh_decisions(
            conn,
            ohlcv_conn=ohlcv_conn,
            tickers=tickers,
            market=args.market,
            decision_date=args.decision_date,
            ohlcv_stale_days=args.ohlcv_stale_days,
        )

    rows = _filter_rows(rows, decisions=set(args.decision or []), min_priority=args.min_priority)
    summary = summarize_quarter_refresh_decisions(rows)
    artifact_paths = write_decision_artifacts(rows, output_root)

    if args.output_csv:
        path = validate_temp_path(Path(args.output_csv))
        write_csv_atomic(path, [asdict(row) for row in rows])
        artifact_paths["output_csv"] = str(path)
    if args.summary_json:
        path = validate_temp_path(Path(args.summary_json))
        write_json_atomic(path, summary)
        artifact_paths["summary_json_override"] = str(path)

    payload = {"summary": summary, "artifact_paths": artifact_paths}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_summary(payload)
    return 0


def _output_root(value: str | None) -> Path:
    root = validate_temp_path(Path(value)) if value else temp_root() / "quarter_refresh_decision_audit" / utc_timestamp()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _selected_tickers(args: argparse.Namespace) -> list[str] | None:
    tickers = list(args.ticker or [])
    if args.tickers_file:
        tickers.extend(_read_tickers_file(Path(args.tickers_file)))
    return tickers or None


def _read_tickers_file(path: Path) -> list[str]:
    resolved = validate_temp_path(path, must_exist=True)
    return [line.strip() for line in resolved.read_text(encoding="utf-8").splitlines() if line.strip()]


def _filter_rows(rows: list[Any], *, decisions: set[str], min_priority: str | None) -> list[Any]:
    filtered = rows
    if decisions:
        filtered = [row for row in filtered if row.decision in decisions]
    if min_priority:
        threshold = PRIORITY_ORDER[min_priority]
        filtered = [row for row in filtered if row.decision_priority_rank <= threshold]
    return filtered


def _print_summary(payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    print(f"audit_version: {summary['audit_version']}")
    print(f"total_tickers: {summary['total_tickers']}")
    print(f"eligible_for_future_auto_fetch_count: {summary['eligible_for_future_auto_fetch_count']}")
    print(f"manual_review_count: {summary['manual_review_count']}")
    print(f"no_action_count: {summary['no_action_count']}")
    print("decision_counts:")
    for key, value in summary["decision_counts"].items():
        print(f"  {key}: {value}")
    print("priority_counts:")
    for key, value in summary["priority_counts"].items():
        print(f"  {key}: {value}")
    print("market_data_activity:")
    for key in (
        "active_fetch_count",
        "stale_or_inactive_count",
        "no_ohlcv_count",
        "ohlcv_age_0_7_days",
        "ohlcv_age_8_14_days",
        "ohlcv_age_15_30_days",
        "ohlcv_age_over_30_days",
        "inactive_but_calendar_upcoming_count",
        "inactive_but_due_today_count",
        "inactive_with_fetch_candidate_count_before_suppression",
    ):
        print(f"  {key}: {summary[key]}")
    print("artifact_paths:")
    for key, value in sorted(payload["artifact_paths"].items()):
        print(f"  {key}: {value}")


if __name__ == "__main__":
    raise SystemExit(main())
