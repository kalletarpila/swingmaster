from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker
from swingmaster.fundamentals.earnings_event_matching import (
    DEFAULT_MAX_REPORTING_DELAY_DAYS,
    audit_universe,
    temp_root,
    validate_temp_path,
    write_audit_artifacts,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only full-universe earnings-event matching audit")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ticker", action="append", default=None)
    parser.add_argument("--tickers-file", default=None)
    parser.add_argument("--first-n", type=int, default=None)
    parser.add_argument("--max-delay-days", type=int, default=DEFAULT_MAX_REPORTING_DELAY_DAYS)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--output-csv", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        db_path = Path(args.fundamentals_db)
        tickers = _load_selected_tickers(args)
        if args.first_n is not None:
            if args.first_n < 0:
                raise ValueError("FIRST_N_MUST_BE_NON_NEGATIVE")
            tickers = tickers[: args.first_n] if tickers is not None else None
        payload = audit_universe(db_path, tickers=tickers, max_delay_days=args.max_delay_days)
        output_json = validate_temp_path(Path(args.output_json)) if args.output_json else _default_output_path("json")
        output_csv = validate_temp_path(Path(args.output_csv)) if args.output_csv else _default_output_path("csv")
        write_audit_artifacts(payload, output_json, output_csv)
        print(
            json.dumps(
                {
                    "output_json": str(output_json),
                    "output_csv": str(output_csv),
                    "aggregate": payload["aggregate"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def _load_selected_tickers(args: argparse.Namespace) -> list[str] | None:
    selected: list[str] = []
    if args.ticker:
        selected.extend(normalize_ticker(ticker) for ticker in args.ticker)
    if args.tickers_file:
        path = validate_temp_path(Path(args.tickers_file), must_exist=True)
        selected.extend(normalize_ticker(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if not selected:
        return None
    return sorted(dict.fromkeys(selected))


def _default_output_path(suffix: str) -> Path:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")
    return temp_root() / "earnings_event_matching_audit" / timestamp / f"audit.{suffix}"


if __name__ == "__main__":
    raise SystemExit(main())
