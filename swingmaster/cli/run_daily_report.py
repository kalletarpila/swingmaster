from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from swingmaster.cli.daily_report import (
    MARKET_CONFIGS,
    apply_buy_rules,
    build_report_rows_json_mode,
    fetch_report_raw_rows,
    infer_market_from_db_path,
    load_buy_rules_config,
    write_outputs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate daily report using JSON buy-rule filtering")
    parser.add_argument("positionals", nargs="*", help="Legacy syntax: [AS_OF_DATE] MARKET [MARKET...]")
    parser.add_argument("--date", default=None, help="As-of date (YYYY-MM-DD)")
    parser.add_argument("--market", action="append", dest="markets", default=[], help="Market code (FIN|SE|USA)")
    parser.add_argument("--rc-db", default=None, help="Optional RC SQLite database path")
    parser.add_argument("--out-dir", default=None, help="Output directory (default: daily_reports)")
    return parser.parse_args()


def _is_date(value: str) -> bool:
    parts = value.split("-")
    return len(parts) == 3 and all(part.isdigit() for part in parts) and len(parts[0]) == 4


def _normalize_market_tokens(values: List[str]) -> List[str]:
    out: List[str] = []
    for value in values:
        for token in value.replace(",", " ").split():
            out.append(token.lower())
    return out


def _resolve_markets(args: argparse.Namespace) -> tuple[str, List[str]]:
    as_of_date = args.date
    positional_markets: List[str] = []
    if args.positionals:
        remaining = list(args.positionals)
        if as_of_date is None and remaining and _is_date(remaining[0]):
            as_of_date = remaining.pop(0)
        positional_markets = _normalize_market_tokens(remaining)

    if as_of_date is None:
        raise SystemExit("Missing date. Use --date YYYY-MM-DD or legacy positional date.")

    explicit_markets = _normalize_market_tokens(args.markets)
    markets = explicit_markets or positional_markets
    if not markets:
        if args.rc_db:
            markets = [infer_market_from_db_path(args.rc_db)]
        else:
            raise SystemExit("Missing market. Use --market or legacy positional market.")

    return as_of_date, markets


def main() -> None:
    args = parse_args()
    as_of_date, markets = _resolve_markets(args)
    out_dir = Path(args.out_dir or "/home/kalle/projects/swingmaster/daily_reports")

    for market in markets:
        if market not in MARKET_CONFIGS:
            raise SystemExit(f"Unknown market: {market}")
        config = MARKET_CONFIGS[market]
        db_path = Path(args.rc_db) if args.rc_db and len(markets) == 1 else config.db_path

        all_rows, missing_fastpass_table = fetch_report_raw_rows(db_path, config, as_of_date)
        if missing_fastpass_table:
            print("WARNING FASTPASS_TABLE_MISSING")

        try:
            rules_config = load_buy_rules_config(config.rules_market)
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(2)

        base_rows = [row for row in all_rows if not str(row.get("section", "")).startswith("BUYS")]
        json_buy_rows, _ = apply_buy_rules(base_rows, rules_config, buy_section_name="BUYS")
        final_rows = build_report_rows_json_mode(
            all_rows=all_rows,
            buy_rows=json_buy_rows,
            market_label=config.display_market,
            as_of_date=as_of_date,
        )

        txt_out = out_dir / f"{config.output_prefix}_daily_report_{as_of_date}.txt"
        csv_out = out_dir / f"{config.output_prefix}_daily_report_{as_of_date}.csv"
        write_outputs(final_rows, txt_out, csv_out)
        print(f"{config.display_market} TXT: {txt_out}")
        print(f"{config.display_market} CSV: {csv_out}")


if __name__ == "__main__":
    main()
