from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.effective_date_audit import (
    audit_effective_date_usage,
    default_output_root,
    validate_temp_path,
    write_artifacts,
    write_csv_atomic,
    write_json_atomic,
)
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only audit of fundamentals effective-date usage")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--tickers-file")
    parser.add_argument("--as-of-date")
    parser.add_argument("--first-n", type=int)
    parser.add_argument("--sample-size", type=int)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--output-root")
    parser.add_argument("--output-json")
    parser.add_argument("--output-csv")
    parser.add_argument("--output-summary-json")
    parser.add_argument("--progress-log")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_root = validate_temp_path(Path(args.output_root)) if args.output_root else default_output_root()
    output_root.mkdir(parents=True, exist_ok=True)
    tickers = _selected_tickers(args)
    payload = audit_effective_date_usage(
        Path(args.fundamentals_db),
        tickers=tickers,
        as_of_date=args.as_of_date,
        first_n=args.first_n,
        sample_size=args.sample_size,
        random_seed=args.random_seed,
    )
    payload["runtime_artifact_root"] = str(output_root)
    artifact_paths = write_artifacts(payload, output_root)
    if args.output_json:
        path = validate_temp_path(Path(args.output_json))
        write_json_atomic(path, payload)
        artifact_paths["output_json"] = str(path)
    if args.output_csv:
        path = validate_temp_path(Path(args.output_csv))
        write_csv_atomic(path, payload["comparisons"])
        artifact_paths["output_csv"] = str(path)
    if args.output_summary_json:
        path = validate_temp_path(Path(args.output_summary_json))
        write_json_atomic(path, _summary_payload(payload))
        artifact_paths["output_summary_json"] = str(path)
    progress_path = validate_temp_path(Path(args.progress_log)) if args.progress_log else output_root / "progress.log"
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(_progress_text(payload, artifact_paths), encoding="utf-8")
    artifact_paths["progress_log"] = str(progress_path)

    if args.json:
        print(json.dumps({"summary": _summary_payload(payload), "artifact_paths": artifact_paths}, indent=2, sort_keys=True))
    else:
        _print_summary(payload, artifact_paths)
    return 0


def _selected_tickers(args: argparse.Namespace) -> list[str] | None:
    tickers = list(args.ticker or [])
    if args.tickers_file:
        path = validate_temp_path(Path(args.tickers_file), must_exist=True)
        tickers.extend(line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return tickers or None


def _summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "audit_version": payload["audit_version"],
        "database_path": payload["database_path"],
        "as_of_date": payload["as_of_date"],
        "database_content_unchanged": payload["database_content_unchanged"],
        "universe_impact": payload["universe_impact"],
        "missing_match_policy_recommendation": payload["missing_match_policy_recommendation"],
        "architecture_recommendation": payload["architecture_recommendation"],
    }


def _progress_text(payload: dict[str, Any], artifact_paths: dict[str, str]) -> str:
    impact = payload["universe_impact"]
    return "\n".join(
        [
            f"audit_version={payload['audit_version']}",
            f"database_path={payload['database_path']}",
            f"as_of_date={payload['as_of_date']}",
            f"total_tickers={impact['total_tickers']}",
            f"total_matched_quarters={impact['total_matched_quarters']}",
            f"affected_ticker_count={impact['affected_ticker_count']}",
            f"database_content_unchanged={payload['database_content_unchanged']}",
            f"artifact_paths={json.dumps(artifact_paths, sort_keys=True)}",
            "",
        ]
    )


def _print_summary(payload: dict[str, Any], artifact_paths: dict[str, str]) -> None:
    impact = payload["universe_impact"]
    print(f"audit_version: {payload['audit_version']}")
    print(f"database_path: {payload['database_path']}")
    print(f"as_of_date: {payload['as_of_date']}")
    print(f"total_tickers: {impact['total_tickers']}")
    print(f"total_matched_quarters: {impact['total_matched_quarters']}")
    print(f"quarters_with_positive_reporting_delay: {impact['quarters_with_positive_reporting_delay']}")
    print(f"median_reporting_delay_days: {impact['median_reporting_delay_days']}")
    print(f"p95_reporting_delay_days: {impact['p95_reporting_delay_days']}")
    print(f"affected_ticker_count: {impact['affected_ticker_count']}")
    print(f"database_content_unchanged: {payload['database_content_unchanged']}")
    print(f"artifact_root: {payload['runtime_artifact_root']}")
    print("historical_output_rows_potentially_affected:")
    for key, value in impact["historical_output_rows_potentially_affected"].items():
        print(f"  {key}: {value}")
    print("artifact_paths:")
    for key, value in sorted(artifact_paths.items()):
        print(f"  {key}: {value}")


if __name__ == "__main__":
    raise SystemExit(main())
