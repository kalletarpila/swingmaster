from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.earnings_events import (
    DEFAULT_SAFETY_MARGIN_DAYS,
    assess_earnings_coverage,
    dataclass_to_dict,
    default_fundamentals_usa_db_path,
    fetch_yahoo_earnings_events,
    open_readonly_db,
    plan_earnings_history_range,
    plan_yahoo_earnings_limit,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect Yahoo earnings-date source coverage for one ticker")
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--start-date", default=None, help="Diagnostic lower-bound override, YYYY-MM-DD")
    parser.add_argument("--safety-margin-days", type=int, default=DEFAULT_SAFETY_MARGIN_DAYS)
    parser.add_argument("--limit", type=int, default=None, help="Diagnostic yfinance limit override")
    parser.add_argument("--include-future", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def build_summary(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    with open_readonly_db(db_path) as conn:
        range_plan = plan_earnings_history_range(
            conn,
            args.ticker,
            safety_margin_days=args.safety_margin_days,
            manual_start_date=args.start_date,
        )
    if range_plan.status == "NO_FUNDAMENTALS_HISTORY" and args.start_date is None:
        coverage = assess_earnings_coverage((), range_plan=range_plan)
        summary = {
            "ticker": args.ticker,
            "normalized_ticker": range_plan.ticker,
            "database_path": str(db_path),
            "range_plan": asdict(range_plan),
            "limit_plan": None,
            "source": None,
            "coverage": asdict(coverage),
            "records": [],
        }
        return summary, 0

    if range_plan.fetch_lower_bound is None:
        raise RuntimeError("FETCH_LOWER_BOUND_NOT_AVAILABLE")
    limit_plan = plan_yahoo_earnings_limit(range_plan.fetch_lower_bound, manual_limit=args.limit)
    result = fetch_yahoo_earnings_events(
        ticker=args.ticker,
        range_plan=range_plan,
        limit_plan=limit_plan,
        include_future=args.include_future,
    )
    session_counts = Counter(record.announcement_session for record in result.records)
    summary = {
        "ticker": args.ticker,
        "normalized_ticker": result.normalized_ticker,
        "database_path": str(db_path),
        "range_plan": asdict(range_plan),
        "limit_plan": asdict(limit_plan),
        "source": {
            "source_observed_at_utc": result.source_observed_at_utc,
            "requested_limit": result.requested_limit,
            "raw_yahoo_row_count": result.diagnostics.raw_row_count,
            "completed_qualifying_count": result.diagnostics.completed_qualifying_count,
            "future_unreported_count": result.diagnostics.unreported_count,
            "rows_filtered_before_lower_bound": result.diagnostics.rows_before_lower_bound,
            "duplicate_count": result.diagnostics.duplicate_count,
            "invalid_count": result.diagnostics.invalid_count,
            "actual_columns": list(result.diagnostics.actual_columns),
            "session_counts": dict(sorted(session_counts.items())),
            "status": result.status,
            "error_message": result.error_message,
        },
        "coverage": asdict(result.coverage),
        "records": [asdict(record) for record in result.records],
    }
    exit_code = 1 if result.status in {"SOURCE_FAILED", "PARSE_FAILED"} else 0
    return summary, exit_code


def print_text(summary: dict[str, Any]) -> None:
    print(f"ticker: {summary['ticker']}")
    print(f"normalized_ticker: {summary['normalized_ticker']}")
    print(f"database_path: {summary['database_path']}")
    range_plan = summary["range_plan"]
    for key in (
        "source_table",
        "source_period_end_column",
        "qualifying_fundamentals_row_count",
        "oldest_required_period_end_date",
        "safety_margin_days",
        "fetch_lower_bound",
        "range_overridden",
        "manual_start_date",
        "status",
    ):
        print(f"range_{key}: {range_plan.get(key)}")
    limit_plan = summary.get("limit_plan")
    if limit_plan is not None:
        for key in (
            "estimated_quarters",
            "buffer_events",
            "requested_limit",
            "uncapped_requested_limit",
            "cap",
            "capped",
            "manual_limit",
            "limit_overridden",
            "cap_source",
        ):
            print(f"limit_{key}: {limit_plan.get(key)}")
    source = summary.get("source")
    if source is not None:
        for key, value in source.items():
            print(f"source_{key}: {value}")
    coverage = summary["coverage"]
    for key, value in coverage.items():
        print(f"coverage_{key}: {value}")
    print("records:")
    for record in summary["records"]:
        print(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary, exit_code = build_summary(args)
    except Exception as exc:
        if args.json_output:
            print(
                json.dumps(
                    {"status": "ERROR", "error_type": type(exc).__name__, "error_message": str(exc)},
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                )
            )
        else:
            print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if args.json_output:
        print(json.dumps(dataclass_to_dict(summary), sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        print_text(summary)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
