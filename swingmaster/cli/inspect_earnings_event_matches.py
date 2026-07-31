from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, open_readonly_db
from swingmaster.fundamentals.earnings_event_matching import (
    DEFAULT_MAX_REPORTING_DELAY_DAYS,
    inspect_ticker,
    validate_temp_path,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only earnings-event to quarterly-period inspection")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--max-delay-days", type=int, default=DEFAULT_MAX_REPORTING_DELAY_DAYS)
    parser.add_argument("--output-json", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        db_path = Path(args.fundamentals_db)
        with open_readonly_db(db_path) as conn:
            payload = inspect_ticker(conn, args.ticker, max_delay_days=args.max_delay_days)
        text = json.dumps(payload, indent=2, sort_keys=True)
        if args.output_json:
            output_path = validate_temp_path(Path(args.output_json))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = output_path.with_name(output_path.name + ".tmp")
            tmp.write_text(text + "\n", encoding="utf-8")
            tmp.replace(output_path)
        print(text)
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
