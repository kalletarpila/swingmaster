from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path
from swingmaster.fundamentals.ticker_cleanup_audit import (
    audit_ticker_cleanup,
    temp_root,
    utc_timestamp,
    validate_temp_path,
    write_audit_artifacts,
    write_json_atomic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only fundamentals ticker cleanup audit")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--tickers-file")
    parser.add_argument("--first-n", type=int)
    parser.add_argument("--sample-size", type=int)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--output-root")
    parser.add_argument("--output-json")
    parser.add_argument("--output-csv")
    parser.add_argument("--output-summary-json")
    parser.add_argument("--progress-log")
    parser.add_argument("--checkpoint-json")
    parser.add_argument("--resume-from-json")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_root = _output_root(args.output_root)
    tickers = _selected_tickers(args)

    payload = audit_ticker_cleanup(
        Path(args.fundamentals_db),
        tickers=tickers,
        first_n=args.first_n,
        sample_size=args.sample_size,
        random_seed=args.random_seed,
    )
    payload["runtime_artifact_root"] = str(output_root)
    payload["resume_source"] = args.resume_from_json

    artifact_paths = write_audit_artifacts(payload, output_root)
    if args.output_json:
        artifact_paths["output_json"] = str(validate_temp_path(Path(args.output_json)))
        write_json_atomic(Path(args.output_json), payload)
    if args.output_summary_json:
        artifact_paths["output_summary_json"] = str(validate_temp_path(Path(args.output_summary_json)))
        write_json_atomic(Path(args.output_summary_json), payload["summary"])
    if args.output_csv:
        from swingmaster.fundamentals.ticker_cleanup_audit import write_csv_atomic

        artifact_paths["output_csv"] = str(validate_temp_path(Path(args.output_csv)))
        write_csv_atomic(Path(args.output_csv), payload["all_tickers"])
    if args.checkpoint_json:
        artifact_paths["checkpoint_json"] = str(validate_temp_path(Path(args.checkpoint_json)))
        write_json_atomic(Path(args.checkpoint_json), _checkpoint_payload(payload))
    else:
        default_checkpoint = output_root / "checkpoint.json"
        artifact_paths["checkpoint_json"] = str(default_checkpoint)
        write_json_atomic(default_checkpoint, _checkpoint_payload(payload))
    if args.progress_log:
        progress_path = validate_temp_path(Path(args.progress_log))
    else:
        progress_path = output_root / "progress.log"
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(_progress_text(payload, artifact_paths), encoding="utf-8")
    artifact_paths["progress_log"] = str(progress_path)

    if args.json:
        print(json.dumps({"summary": payload["summary"], "artifact_paths": artifact_paths}, indent=2, sort_keys=True))
    else:
        _print_summary(payload, artifact_paths)
    return 0


def _output_root(value: str | None) -> Path:
    if value:
        root = validate_temp_path(Path(value))
    else:
        root = temp_root() / "fundamentals_ticker_cleanup_audit" / utc_timestamp()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _selected_tickers(args: argparse.Namespace) -> list[str] | None:
    tickers = list(args.ticker or [])
    if args.tickers_file:
        tickers.extend(_read_tickers_file(Path(args.tickers_file)))
    if args.resume_from_json:
        checkpoint = _read_checkpoint(Path(args.resume_from_json))
        tickers.extend(str(ticker) for ticker in checkpoint.get("selected_tickers", []))
    return tickers or None


def _read_tickers_file(path: Path) -> list[str]:
    resolved = validate_temp_path(path, must_exist=True)
    return [line.strip() for line in resolved.read_text(encoding="utf-8").splitlines() if line.strip()]


def _read_checkpoint(path: Path) -> dict[str, Any]:
    resolved = validate_temp_path(path, must_exist=True)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "selected_tickers" in payload:
        return payload
    if isinstance(payload, dict) and "all_tickers" in payload:
        return {"selected_tickers": [row["ticker"] for row in payload["all_tickers"]]}
    raise ValueError("CHECKPOINT_MISSING_SELECTED_TICKERS")


def _checkpoint_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "audit_version": payload["audit_version"],
        "database_path": payload["database_path"],
        "database_content_unchanged": payload["database_content_unchanged"],
        "selected_tickers": [row["ticker"] for row in payload["all_tickers"]],
        "summary": payload["summary"],
    }


def _progress_text(payload: dict[str, Any], artifact_paths: dict[str, str]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            f"audit_version={payload['audit_version']}",
            f"database_path={payload['database_path']}",
            f"total_distinct_tickers={summary['total_distinct_tickers']}",
            f"safe_remove_candidates={summary['safe_remove_candidates']}",
            f"manual_review_candidates={summary['manual_review_candidates']}",
            f"database_content_unchanged={payload['database_content_unchanged']}",
            f"artifact_paths={json.dumps(artifact_paths, sort_keys=True)}",
            "",
        ]
    )


def _print_summary(payload: dict[str, Any], artifact_paths: dict[str, str]) -> None:
    summary = payload["summary"]
    print(f"audit_version: {payload['audit_version']}")
    print(f"database_path: {payload['database_path']}")
    print(f"total_distinct_tickers: {summary['total_distinct_tickers']}")
    print(f"safe_remove_candidates: {summary['safe_remove_candidates']}")
    print(f"manual_review_candidates: {summary['manual_review_candidates']}")
    print(f"delisted_historical_companies_kept: {summary['delisted_historical_companies_kept']}")
    print(f"database_content_unchanged: {payload['database_content_unchanged']}")
    print(f"artifact_root: {payload['runtime_artifact_root']}")
    print("category_counts:")
    for key, value in summary["category_counts"].items():
        print(f"  {key}: {value}")
    print("artifact_paths:")
    for key, value in sorted(artifact_paths.items()):
        print(f"  {key}: {value}")


if __name__ == "__main__":
    raise SystemExit(main())
