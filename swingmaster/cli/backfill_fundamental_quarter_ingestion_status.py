from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker
from swingmaster.fundamentals.quarter_completeness import (
    DEFAULT_MARKET,
    audit_quarter_completeness,
    upsert_quarter_ingestion_status,
    utc_timestamp,
    validate_temp_path,
    write_audit_artifacts,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill canonical quarterly fundamentals readiness statuses")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--market", default=DEFAULT_MARKET)
    parser.add_argument("--ticker", action="append", default=None)
    parser.add_argument("--tickers-file", default=None)
    parser.add_argument("--period-from", default=None)
    parser.add_argument("--period-to", default=None)
    parser.add_argument("--first-n", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.apply == args.dry_run:
        raise ValueError("CHOOSE_EXACTLY_ONE_OF_APPLY_OR_DRY_RUN")
    if args.first_n is not None and args.first_n < 0:
        raise ValueError("FIRST_N_MUST_BE_NON_NEGATIVE")
    if args.sample_size is not None and args.sample_size < 0:
        raise ValueError("SAMPLE_SIZE_MUST_BE_NON_NEGATIVE")

    db_path = Path(args.fundamentals_db).expanduser().resolve()
    tickers = _load_tickers(args)
    output_root = validate_temp_path(
        Path(args.output_root) if args.output_root else Path("temp") / "fundamental_quarter_ingestion_status" / utc_timestamp()
    )
    output_paths = _output_paths(output_root)
    run_id = str(args.run_id or f"QUARTER_INGESTION_STATUS_BACKFILL_{utc_timestamp()}")

    if args.apply:
        run_migration(db_path)

    payload = audit_quarter_completeness(
        db_path,
        market=str(args.market).lower(),
        tickers=tickers,
        first_n=args.first_n,
        sample_size=args.sample_size,
        random_seed=args.random_seed,
        period_from=args.period_from,
        period_to=args.period_to,
    )
    rows_written = 0
    if args.apply:
        with sqlite3.connect(str(db_path)) as conn:
            rows_written = upsert_quarter_ingestion_status(
                conn,
                _assessment_rows_from_payload(payload),
                run_id=run_id,
            )
            conn.commit()

    payload["summary"].update(
        {
            "mode": "apply" if args.apply else "dry-run",
            "run_id": run_id,
            "status_rows_written": rows_written,
        }
    )
    write_audit_artifacts(payload, output_paths)
    if args.json_output:
        print(json.dumps(payload["summary"], sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        for key in (
            "mode",
            "total_quarter_rows",
            "quarter_basic_complete_count",
            "ttm_input_complete_count",
            "score_history_complete_count",
            "status_rows_written",
            "database_content_unchanged",
        ):
            print(f"SUMMARY {key}={payload['summary'].get(key)}")
        for name, path in sorted(output_paths.items()):
            print(f"ARTIFACT {name}={path}")
    return 0


def _assessment_rows_from_payload(payload: dict[str, Any]) -> list[Any]:
    from swingmaster.fundamentals.quarter_completeness import QuarterAssessment

    list_fields = {
        "missing_core_fields",
        "missing_ttm_fields",
        "missing_score_fields",
        "missing_valuation_fields",
        "data_quality_warnings",
    }
    rows = []
    for row in payload["all_quarters"]:
        normalized = dict(row)
        for field in list_fields:
            value = normalized.get(field)
            if isinstance(value, str):
                normalized[field] = json.loads(value)
        rows.append(QuarterAssessment(**normalized))
    return rows


def _load_tickers(args: argparse.Namespace) -> list[str] | None:
    tickers = [normalize_ticker(ticker) for ticker in (args.ticker or [])]
    if args.tickers_file:
        path = validate_temp_path(Path(args.tickers_file), must_exist=True)
        tickers.extend(normalize_ticker(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return sorted(dict.fromkeys(tickers)) if tickers else None


def _output_paths(root: Path) -> dict[str, Path]:
    return {
        "output_csv": validate_temp_path(root / "all_quarters.csv"),
        "summary_json": validate_temp_path(root / "summary.json"),
        "checkpoint_json": validate_temp_path(root / "checkpoint.json"),
        "ticker_csv": validate_temp_path(root / "ticker_summary.csv"),
        "field_csv": validate_temp_path(root / "field_completeness.csv"),
        "retry_csv": validate_temp_path(root / "retry_candidates.csv"),
        "latest_csv": validate_temp_path(root / "latest_quarter_issues.csv"),
    }


if __name__ == "__main__":
    raise SystemExit(main())
