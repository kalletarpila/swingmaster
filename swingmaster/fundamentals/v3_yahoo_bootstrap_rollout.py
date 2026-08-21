from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sqlite3
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_repositories import V3RawCacheRepository
from swingmaster.fundamentals.v3_yahoo_bootstrap import (
    ApprovedV3Company,
    YahooMetadataEnricher,
    replay_v3_yahoo_bootstrap_from_raw_cache,
    run_v3_yahoo_bootstrap_adapter,
    select_approved_v3_yahoo_companies,
)


ROLLOUT_ARTIFACT_SCHEMA_VERSION = 1
DEFAULT_ROLLOUT_DELAY_SECONDS = 0.5
RETRYABLE_TICKER_STATUSES = {"SOURCE_ERROR"}
COMPLETED_TICKER_STATUSES = {"CANDIDATE_READY", "METADATA_REJECTED", "EMPTY", "SOURCE_ERROR", "RAW_CACHE_MISS"}
RETRYABLE_RESUME_STATUSES = {"RUNNING", "SOURCE_ERROR"}
PER_TICKER_FIELDS = (
    "work_key",
    "ticker",
    "provider_symbol",
    "status",
    "attempt_count",
    "retry_eligible",
    "raw_ok",
    "raw_empty",
    "raw_error",
    "normalized_rows",
    "migration_candidates",
    "metadata_rejections",
    "raw_cache_misses",
    "candidate_keys",
    "error_message",
    "elapsed_seconds",
)


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def temp_root() -> Path:
    return repository_root() / "temp"


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_temp_path(path: Path, *, must_exist: bool = False) -> Path:
    resolved = path.expanduser().resolve()
    root = temp_root().resolve()
    if must_exist and not resolved.exists():
        raise ValueError(f"PATH_DOES_NOT_EXIST:{resolved}")
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"V3_YAHOO_ROLLOUT_PATH_OUTSIDE_TEMP:{resolved}") from exc
    return resolved


def select_rollout_companies(
    v3_conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str | None = None,
    tickers: str | None = None,
    tickers_file: Path | None = None,
    limit: int | None = None,
    allow_full_universe: bool = False,
) -> list[ApprovedV3Company]:
    if tickers_file is not None and (ticker or tickers):
        raise ValueError("V3_YAHOO_ROLLOUT_TICKER_INPUTS_MUTUALLY_EXCLUSIVE")
    file_tickers = None
    if tickers_file is not None:
        path = validate_temp_path(tickers_file, must_exist=True)
        file_tickers = ",".join(line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if limit is not None and limit < 0:
        raise ValueError(f"V3_YAHOO_ROLLOUT_INVALID_LIMIT:{limit}")
    if not allow_full_universe and ticker is None and tickers is None and file_tickers is None and limit is None:
        raise ValueError("V3_YAHOO_ROLLOUT_REQUIRES_BOUND")
    return select_approved_v3_yahoo_companies(
        v3_conn,
        market=market,
        ticker=ticker,
        tickers=tickers or file_tickers,
        limit=limit,
    )


def company_work_key(company: ApprovedV3Company) -> str:
    return f"{company.market}|{company.ticker}|YAHOO|{company.provider_symbol}"


def build_rollout_plan(companies: list[ApprovedV3Company]) -> dict[str, Any]:
    work_keys = [company_work_key(company) for company in companies]
    return {
        "company_count": len(companies),
        "work_keys": work_keys,
        "duplicate_work_key_count": len(work_keys) - len(set(work_keys)),
        "plan_hash": hashlib.sha256("\n".join(work_keys).encode("utf-8")).hexdigest(),
        "first_10_work_keys": work_keys[:10],
        "last_10_work_keys": work_keys[-10:],
    }


def disk_preflight(*, required_bytes: int, paths: list[Path], multiplier: float = 3.0) -> dict[str, Any]:
    if required_bytes < 0:
        raise ValueError(f"V3_YAHOO_ROLLOUT_INVALID_REQUIRED_BYTES:{required_bytes}")
    estimated_required_bytes = int(required_bytes * multiplier)
    checks = []
    for root_text in sorted({str(path.expanduser().resolve().parent) for path in paths}):
        root = Path(root_text)
        root.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(root)
        checks.append(
            {
                "path": str(root),
                "free_bytes": usage.free,
                "estimated_required_bytes": estimated_required_bytes,
                "ok": usage.free >= estimated_required_bytes,
            }
        )
    return {
        "required_bytes": required_bytes,
        "multiplier": multiplier,
        "estimated_required_bytes": estimated_required_bytes,
        "checks": checks,
        "ok": all(check["ok"] for check in checks),
    }


def run_v3_yahoo_bootstrap_rollout(
    *,
    companies: list[ApprovedV3Company],
    raw_cache_repo: V3RawCacheRepository,
    metadata_enricher: YahooMetadataEnricher,
    run_id: str,
    checkpoint_json: Path,
    summary_json: Path,
    candidates_jsonl: Path,
    rejections_jsonl: Path,
    progress_log: Path | None = None,
    client: Any | None = None,
    delay_seconds: float = DEFAULT_ROLLOUT_DELAY_SECONDS,
    dry_run: bool = False,
    replay_raw_cache: bool = False,
    resume_from_json: Path | None = None,
    retry_failed_on_resume: bool = False,
    max_consecutive_source_errors: int = 25,
    max_tickers_this_run: int | None = None,
    sleep_fn: Any = time.sleep,
) -> tuple[dict[str, Any], int]:
    if delay_seconds < 0:
        raise ValueError(f"V3_YAHOO_ROLLOUT_INVALID_DELAY:{delay_seconds}")
    if dry_run and replay_raw_cache:
        raise ValueError("V3_YAHOO_ROLLOUT_DRY_RUN_AND_REPLAY_MUTUALLY_EXCLUSIVE")
    if max_consecutive_source_errors < 1:
        raise ValueError(f"V3_YAHOO_ROLLOUT_INVALID_MAX_CONSECUTIVE_SOURCE_ERRORS:{max_consecutive_source_errors}")
    if max_tickers_this_run is not None and max_tickers_this_run < 0:
        raise ValueError(f"V3_YAHOO_ROLLOUT_INVALID_MAX_TICKERS_THIS_RUN:{max_tickers_this_run}")
    checkpoint_path = validate_temp_path(checkpoint_json)
    summary_path = validate_temp_path(summary_json)
    candidates_path = validate_temp_path(candidates_jsonl)
    rejections_path = validate_temp_path(rejections_jsonl)
    progress_path = validate_temp_path(progress_log) if progress_log is not None else None
    started_at = utc_now_text()
    results_by_ticker = _load_resume_results(
        resume_from_json,
        expected_run_id=run_id,
        selected_tickers=[company.ticker for company in companies],
        retry_failed_on_resume=retry_failed_on_resume,
    )
    all_candidates, all_rejections = _restore_outputs_from_rows(results_by_ticker)
    exit_code = 0
    consecutive_source_errors = 0
    processed_this_run = 0

    if dry_run:
        for company in companies:
            results_by_ticker[company.ticker] = _planned_row(company)
        payload = _build_payload(run_id, companies, results_by_ticker, started_at, complete=True)
        _atomic_write_json(checkpoint_path, payload)
        _atomic_write_json(summary_path, payload["summary"])
        _atomic_write_csv(checkpoint_path.with_suffix(".csv"), payload["per_ticker_results"])
        _atomic_write_jsonl(candidates_path, [])
        _atomic_write_jsonl(rejections_path, [])
        return payload, exit_code

    for index, company in enumerate(companies):
        previous = results_by_ticker.get(company.ticker)
        if previous is not None and _should_skip_previous(previous, retry_failed_on_resume=retry_failed_on_resume):
            _write_all_artifacts(
                checkpoint_path,
                summary_path,
                candidates_path,
                rejections_path,
                progress_path,
                run_id,
                companies,
                results_by_ticker,
                all_candidates,
                all_rejections,
                started_at,
                last_ticker=company.ticker,
                resumed=True,
            )
            continue
        if max_tickers_this_run is not None and processed_this_run >= max_tickers_this_run:
            break
        if index > 0 and delay_seconds > 0 and not replay_raw_cache:
            sleep_fn(delay_seconds)
        attempt_count = int((previous or {}).get("attempt_count") or 0) + 1
        results_by_ticker[company.ticker] = _running_row(company, attempt_count=attempt_count)
        _write_all_artifacts(
            checkpoint_path,
            summary_path,
            candidates_path,
            rejections_path,
            progress_path,
            run_id,
            companies,
            results_by_ticker,
            all_candidates,
            all_rejections,
            started_at,
            last_ticker=company.ticker,
            resumed=False,
        )
        row, candidates, rejections = _process_company(
            company,
            raw_cache_repo=raw_cache_repo,
            metadata_enricher=metadata_enricher,
            run_id=run_id,
            client=client,
            dry_run=False,
            replay_raw_cache=replay_raw_cache,
            attempt_count=attempt_count,
        )
        results_by_ticker[company.ticker] = row
        processed_this_run += 1
        if row["status"] == "SOURCE_ERROR":
            consecutive_source_errors += 1
        else:
            consecutive_source_errors = 0
        for candidate in candidates:
            all_candidates[str(candidate["candidate_key"])] = candidate
        all_rejections.extend(rejections)
        _write_all_artifacts(
            checkpoint_path,
            summary_path,
            candidates_path,
            rejections_path,
            progress_path,
            run_id,
            companies,
            results_by_ticker,
            all_candidates,
            all_rejections,
            started_at,
            last_ticker=company.ticker,
            resumed=False,
        )
        if consecutive_source_errors >= max_consecutive_source_errors:
            exit_code = 1
            break
    complete = exit_code == 0 and len(results_by_ticker) == len(companies)
    payload = _build_payload(run_id, companies, results_by_ticker, started_at, complete=complete)
    _atomic_write_json(checkpoint_path, payload)
    _atomic_write_json(summary_path, payload["summary"])
    _atomic_write_jsonl(candidates_path, [all_candidates[key] for key in sorted(all_candidates)])
    _atomic_write_jsonl(rejections_path, sorted(all_rejections, key=lambda row: (str(row["ticker"]), str(row.get("period_end_date") or ""))))
    return payload, exit_code


def _planned_row(company: ApprovedV3Company) -> dict[str, Any]:
    return {
        "work_key": company_work_key(company),
        "ticker": company.ticker,
        "provider_symbol": company.provider_symbol,
        "status": "PLANNED",
        "attempt_count": 0,
        "retry_eligible": False,
        "raw_ok": 0,
        "raw_empty": 0,
        "raw_error": 0,
        "normalized_rows": 0,
        "migration_candidates": 0,
        "metadata_rejections": 0,
        "raw_cache_misses": 0,
        "candidate_keys": [],
        "candidate_records": [],
        "rejection_records": [],
        "error_message": "",
        "elapsed_seconds": 0.0,
    }


def _running_row(company: ApprovedV3Company, *, attempt_count: int) -> dict[str, Any]:
    row = _planned_row(company)
    row.update({"status": "RUNNING", "attempt_count": attempt_count, "retry_eligible": True})
    return row


def _process_company(
    company: ApprovedV3Company,
    *,
    raw_cache_repo: V3RawCacheRepository,
    metadata_enricher: YahooMetadataEnricher,
    run_id: str,
    client: Any | None,
    dry_run: bool,
    replay_raw_cache: bool,
    attempt_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    started = time.perf_counter()
    if replay_raw_cache:
        summary = replay_v3_yahoo_bootstrap_from_raw_cache(
            companies=[company],
            raw_cache_repo=raw_cache_repo,
            metadata_enricher=metadata_enricher,
            fetch_run_id=run_id,
        )
    else:
        summary = run_v3_yahoo_bootstrap_adapter(
            companies=[company],
            raw_cache_repo=raw_cache_repo,
            metadata_enricher=metadata_enricher,
            fetch_run_id=run_id,
            client=client,
            delay_seconds=0,
            dry_run=dry_run,
        )
    candidates = [asdict(candidate) for candidate in summary["candidates"]]
    rejections = [asdict(rejection) for rejection in summary["rejections"]]
    status = _ticker_status(summary)
    row = {
        "work_key": company_work_key(company),
        "ticker": company.ticker,
        "provider_symbol": company.provider_symbol,
        "status": status,
        "attempt_count": attempt_count,
        "retry_eligible": status in RETRYABLE_TICKER_STATUSES,
        "raw_ok": int(summary.get("raw_ok") or 0),
        "raw_empty": int(summary.get("raw_empty") or 0),
        "raw_error": int(summary.get("raw_error") or 0),
        "normalized_rows": int(summary.get("normalized_rows") or 0),
        "migration_candidates": int(summary.get("migration_candidates") or 0),
        "metadata_rejections": int(summary.get("metadata_rejections") or 0),
        "raw_cache_misses": int(summary.get("raw_cache_misses") or 0),
        "candidate_keys": [str(candidate["candidate_key"]) for candidate in candidates],
        "candidate_records": candidates,
        "rejection_records": rejections,
        "error_message": _first_error(rejections),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    return row, candidates, rejections


def _ticker_status(summary: dict[str, Any]) -> str:
    if int(summary.get("raw_cache_misses") or 0):
        return "RAW_CACHE_MISS"
    if int(summary.get("raw_error") or 0):
        return "SOURCE_ERROR"
    if int(summary.get("raw_empty") or 0):
        return "EMPTY"
    if int(summary.get("migration_candidates") or 0):
        return "CANDIDATE_READY"
    if int(summary.get("metadata_rejections") or 0):
        return "METADATA_REJECTED"
    return "NO_ROWS"


def _first_error(rejections: list[dict[str, Any]]) -> str:
    if not rejections:
        return ""
    first = rejections[0]
    return str(first.get("reason") or "")


def _load_resume_results(
    resume_from_json: Path | None,
    *,
    expected_run_id: str,
    selected_tickers: list[str],
    retry_failed_on_resume: bool,
) -> dict[str, dict[str, Any]]:
    if resume_from_json is None:
        return {}
    payload = json.loads(validate_temp_path(resume_from_json, must_exist=True).read_text(encoding="utf-8"))
    if payload.get("artifact_schema_version") != ROLLOUT_ARTIFACT_SCHEMA_VERSION:
        raise ValueError("V3_YAHOO_ROLLOUT_INCOMPATIBLE_CHECKPOINT_SCHEMA")
    if payload.get("run_id") != expected_run_id:
        raise ValueError("V3_YAHOO_ROLLOUT_INCOMPATIBLE_RUN_ID")
    if payload.get("selected_tickers") != selected_tickers:
        raise ValueError("V3_YAHOO_ROLLOUT_INCOMPATIBLE_SELECTED_TICKERS")
    rows = {
        str(row["ticker"]): dict(row)
        for row in payload.get("per_ticker_results", [])
        if isinstance(row, dict) and "ticker" in row
    }
    if retry_failed_on_resume:
        return rows
    return rows


def _should_skip_previous(row: dict[str, Any], *, retry_failed_on_resume: bool) -> bool:
    status = str(row.get("status") or "")
    if status == "RUNNING":
        return False
    if retry_failed_on_resume and status in RETRYABLE_RESUME_STATUSES:
        return False
    return status in COMPLETED_TICKER_STATUSES


def _restore_outputs_from_rows(rows_by_ticker: dict[str, dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    candidates: dict[str, dict[str, Any]] = {}
    rejections: list[dict[str, Any]] = []
    for row in rows_by_ticker.values():
        for candidate in row.get("candidate_records") or []:
            candidates[str(candidate["candidate_key"])] = dict(candidate)
        for rejection in row.get("rejection_records") or []:
            rejections.append(dict(rejection))
    return candidates, rejections


def _write_all_artifacts(
    checkpoint_path: Path,
    summary_path: Path,
    candidates_path: Path,
    rejections_path: Path,
    progress_path: Path | None,
    run_id: str,
    companies: list[ApprovedV3Company],
    results_by_ticker: dict[str, dict[str, Any]],
    candidates_by_key: dict[str, dict[str, Any]],
    rejections: list[dict[str, Any]],
    started_at: str,
    *,
    last_ticker: str,
    resumed: bool,
) -> None:
    payload = _build_payload(run_id, companies, results_by_ticker, started_at, complete=False)
    _atomic_write_json(checkpoint_path, payload)
    _atomic_write_json(summary_path, payload["summary"])
    _atomic_write_csv(checkpoint_path.with_suffix(".csv"), payload["per_ticker_results"])
    _atomic_write_jsonl(candidates_path, [candidates_by_key[key] for key in sorted(candidates_by_key)])
    _atomic_write_jsonl(rejections_path, sorted(rejections, key=lambda row: (str(row["ticker"]), str(row.get("period_end_date") or ""))))
    if progress_path is not None:
        progress_path.parent.mkdir(parents=True, exist_ok=True)
        with progress_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"ticker": last_ticker, "resumed": resumed, "processed": len(results_by_ticker)}, sort_keys=True) + "\n")


def _build_payload(
    run_id: str,
    companies: list[ApprovedV3Company],
    results_by_ticker: dict[str, dict[str, Any]],
    started_at: str,
    *,
    complete: bool,
) -> dict[str, Any]:
    per_ticker = [results_by_ticker[company.ticker] for company in companies if company.ticker in results_by_ticker]
    summary = _summarize(companies, per_ticker)
    return {
        "artifact_schema_version": ROLLOUT_ARTIFACT_SCHEMA_VERSION,
        "run_id": run_id,
        "started_at_utc": started_at,
        "updated_at_utc": utc_now_text(),
        "complete": complete,
        "selected_tickers": [company.ticker for company in companies],
        "plan": build_rollout_plan(companies),
        "selected_company_count": len(companies),
        "per_ticker_results": per_ticker,
        "summary": summary,
    }


def _summarize(companies: list[ApprovedV3Company], rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "")
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "selected_company_count": len(companies),
        "processed_company_count": len(rows),
        "remaining_company_count": max(len(companies) - len(rows), 0),
        "status_counts": dict(sorted(status_counts.items())),
        "raw_ok": sum(int(row.get("raw_ok") or 0) for row in rows),
        "raw_empty": sum(int(row.get("raw_empty") or 0) for row in rows),
        "raw_error": sum(int(row.get("raw_error") or 0) for row in rows),
        "normalized_rows": sum(int(row.get("normalized_rows") or 0) for row in rows),
        "migration_candidates": sum(int(row.get("migration_candidates") or 0) for row in rows),
        "metadata_rejections": sum(int(row.get("metadata_rejections") or 0) for row in rows),
        "raw_cache_misses": sum(int(row.get("raw_cache_misses") or 0) for row in rows),
        "retry_eligible_count": sum(1 for row in rows if bool(row.get("retry_eligible"))),
    }


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _atomic_write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
    tmp.replace(path)


def _atomic_write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PER_TICKER_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: json.dumps(row[field], sort_keys=True) if isinstance(row.get(field), (list, dict)) else row.get(field, "") for field in PER_TICKER_FIELDS})
    tmp.replace(path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resumable Fundamentals V3 Yahoo bootstrap rollout")
    parser.add_argument("--v3-db", required=True)
    parser.add_argument("--raw-cache-db", required=True)
    parser.add_argument("--v2-db", default=None)
    parser.add_argument("--legacy-db", default=None)
    parser.add_argument("--market", default="usa")
    parser.add_argument("--ticker", default=None)
    parser.add_argument("--tickers", default=None)
    parser.add_argument("--tickers-file", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--allow-full-universe", action="store_true")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--checkpoint-json", required=True)
    parser.add_argument("--summary-json", required=True)
    parser.add_argument("--candidates-jsonl", required=True)
    parser.add_argument("--rejections-jsonl", required=True)
    parser.add_argument("--progress-log", default=None)
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_ROLLOUT_DELAY_SECONDS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replay-raw-cache", action="store_true")
    parser.add_argument("--resume-from-json", default=None)
    parser.add_argument("--retry-failed-on-resume", action="store_true")
    parser.add_argument("--max-consecutive-source-errors", type=int, default=25)
    parser.add_argument("--max-tickers-this-run", type=int, default=None)
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def run_from_args(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    with sqlite3.connect(str(Path(args.v3_db))) as v3_conn:
        companies = select_rollout_companies(
            v3_conn,
            market=args.market,
            ticker=args.ticker,
            tickers=args.tickers,
            tickers_file=Path(args.tickers_file) if args.tickers_file else None,
            limit=args.limit,
            allow_full_universe=args.allow_full_universe,
        )
    v2_conn = sqlite3.connect(str(Path(args.v2_db))) if args.v2_db else None
    legacy_conn = sqlite3.connect(str(Path(args.legacy_db))) if args.legacy_db else None
    try:
        return run_v3_yahoo_bootstrap_rollout(
            companies=companies,
            raw_cache_repo=V3RawCacheRepository(Path(args.raw_cache_db)),
            metadata_enricher=YahooMetadataEnricher(v2_conn=v2_conn, legacy_conn=legacy_conn),
            run_id=args.run_id,
            checkpoint_json=Path(args.checkpoint_json),
            summary_json=Path(args.summary_json),
            candidates_jsonl=Path(args.candidates_jsonl),
            rejections_jsonl=Path(args.rejections_jsonl),
            progress_log=Path(args.progress_log) if args.progress_log else None,
            delay_seconds=args.delay_seconds,
            dry_run=args.dry_run,
            replay_raw_cache=args.replay_raw_cache,
            resume_from_json=Path(args.resume_from_json) if args.resume_from_json else None,
            retry_failed_on_resume=args.retry_failed_on_resume,
            max_consecutive_source_errors=args.max_consecutive_source_errors,
            max_tickers_this_run=args.max_tickers_this_run,
        )
    finally:
        if v2_conn is not None:
            v2_conn.close()
        if legacy_conn is not None:
            legacy_conn.close()


def main() -> None:
    payload, exit_code = run_from_args(parse_args())
    if payload:
        print(f"SUMMARY run_id={payload['run_id']}")
        for key, value in payload["summary"].items():
            print(f"SUMMARY {key}={json.dumps(value, sort_keys=True) if isinstance(value, dict) else value}")
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
