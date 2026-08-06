from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker
from swingmaster.fundamentals.quarter_completeness import (
    DEFAULT_MARKET,
    audit_quarter_completeness,
    utc_timestamp,
    validate_temp_path,
    write_audit_artifacts,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only audit of quarterly fundamentals completeness")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--market", default=DEFAULT_MARKET)
    parser.add_argument("--ticker", action="append", default=None)
    parser.add_argument("--tickers-file", default=None)
    parser.add_argument("--period-from", default=None)
    parser.add_argument("--period-to", default=None)
    parser.add_argument("--first-n", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--output-json", default=None, help="Alias for all_quarters.csv compatibility is not used; JSON full payload path")
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--output-summary-json", default=None)
    parser.add_argument("--progress-log", default=None)
    parser.add_argument("--checkpoint-json", default=None)
    parser.add_argument("--resume-from-json", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    _validate_args(args)
    tickers = _load_tickers(args)
    output_paths = _resolve_output_paths(args)
    payload = audit_quarter_completeness(
        Path(args.fundamentals_db).expanduser().resolve(),
        market=str(args.market).lower(),
        tickers=tickers,
        first_n=args.first_n,
        sample_size=args.sample_size,
        random_seed=args.random_seed,
        period_from=args.period_from,
        period_to=args.period_to,
    )
    if args.output_json:
        _write_json(validate_temp_path(Path(args.output_json)), payload)
    if args.progress_log:
        progress_path = validate_temp_path(Path(args.progress_log))
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        progress_path.write_text(
            f"completed rows={payload['summary']['total_quarter_rows']} tickers={payload['summary']['distinct_tickers']}\n",
            encoding="utf-8",
        )
    write_audit_artifacts(payload, output_paths)
    if args.json_output:
        print(json.dumps(payload["summary"], sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        _print_summary(payload["summary"], output_paths)
    return 0


def _validate_args(args: argparse.Namespace) -> None:
    if args.first_n is not None and args.first_n < 0:
        raise ValueError("FIRST_N_MUST_BE_NON_NEGATIVE")
    if args.sample_size is not None and args.sample_size < 0:
        raise ValueError("SAMPLE_SIZE_MUST_BE_NON_NEGATIVE")
    if args.resume_from_json:
        validate_temp_path(Path(args.resume_from_json), must_exist=True)
    for attr in ("output_json", "output_csv", "output_summary_json", "progress_log", "checkpoint_json", "tickers_file"):
        value = getattr(args, attr, None)
        if value and attr != "tickers_file":
            validate_temp_path(Path(value))
    if args.tickers_file:
        validate_temp_path(Path(args.tickers_file), must_exist=True)


def _load_tickers(args: argparse.Namespace) -> list[str] | None:
    tickers = [normalize_ticker(ticker) for ticker in (args.ticker or [])]
    if args.tickers_file:
        path = validate_temp_path(Path(args.tickers_file), must_exist=True)
        tickers.extend(normalize_ticker(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return sorted(dict.fromkeys(tickers)) if tickers else None


def _resolve_output_paths(args: argparse.Namespace) -> dict[str, Path]:
    root = Path(args.output_root) if args.output_root else Path("temp") / "fundamental_quarter_completeness_audit" / utc_timestamp()
    root = validate_temp_path(root)
    output_csv = Path(args.output_csv) if args.output_csv else root / "all_quarters.csv"
    summary_json = Path(args.output_summary_json) if args.output_summary_json else root / "summary.json"
    checkpoint_json = Path(args.checkpoint_json) if args.checkpoint_json else root / "checkpoint.json"
    return {
        "output_csv": validate_temp_path(output_csv),
        "summary_json": validate_temp_path(summary_json),
        "checkpoint_json": validate_temp_path(checkpoint_json),
        "ticker_csv": validate_temp_path(root / "ticker_summary.csv"),
        "field_csv": validate_temp_path(root / "field_completeness.csv"),
        "retry_csv": validate_temp_path(root / "retry_candidates.csv"),
        "latest_csv": validate_temp_path(root / "latest_quarter_issues.csv"),
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")


def _print_summary(summary: dict[str, Any], paths: dict[str, Path]) -> None:
    for key in (
        "total_quarter_rows",
        "distinct_tickers",
        "quarter_basic_complete_count",
        "quarter_basic_incomplete_count",
        "ttm_input_complete_count",
        "score_history_complete_count",
        "valuation_input_ready_count",
        "retry_yahoo_count",
        "retry_sec_count",
        "retry_both_count",
        "manual_review_count",
        "not_retryable_count",
        "database_content_unchanged",
    ):
        print(f"SUMMARY {key}={summary.get(key)}")
    for name, path in sorted(paths.items()):
        print(f"ARTIFACT {name}={path}")


if __name__ == "__main__":
    raise SystemExit(main())
