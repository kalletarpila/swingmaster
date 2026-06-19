from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote


STATUS_DRY_RUN_READY = "DRY_RUN_READY"
STATUS_DRY_RUN_PARTIAL_POLICY_REQUIRED = "DRY_RUN_PARTIAL_POLICY_REQUIRED"
STATUS_BLOCKED_MISSING_SCHEMA = "BLOCKED_MISSING_SCHEMA"
STATUS_NO_SOURCE_ROWS = "NO_SOURCE_ROWS"
STATUS_UNKNOWN = "UNKNOWN"
POLICY_REQUIRED = "policy_required"
POLICY_LIVE_SAFE_LEGACY_BASELINE = "live_safe_legacy_baseline"
POLICY_RESEARCH_ESTIMATED_LEGACY = "research_estimated_legacy"
POLICY_EXTERNALLY_VERIFIED_RELEASE_DATE = "externally_verified_release_date"
AVAILABILITY_POLICY_CHOICES = (
    POLICY_REQUIRED,
    POLICY_LIVE_SAFE_LEGACY_BASELINE,
    POLICY_RESEARCH_ESTIMATED_LEGACY,
    POLICY_EXTERNALLY_VERIFIED_RELEASE_DATE,
)
VERIFIED_AVAILABILITY_REQUIRED_FIELDS = (
    "market",
    "ticker",
    "period_end_date",
    "available_at_utc",
    "source_provider",
    "source_document_id",
    "source_hash",
    "verified_at_utc",
)
FINANCIAL_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)
LATEST_COLUMNS = (
    "ticker",
    "period_end_date",
    *FINANCIAL_FIELDS,
    "currency",
    "run_id",
)
REQUIRED_TABLES = (
    "rc_fundamental_quarterly",
    "rc_fundamental_quarterly_vintage",
    "rc_fundamental_quarterly_field_provenance",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run legacy reported vintage backfill planner")
    parser.add_argument("--fundamentals-db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", required=True, help="Market code, e.g. usa")
    parser.add_argument("--as-of-date", required=True, help="Latest quarterly period cutoff in YYYY-MM-DD format")
    parser.add_argument("--tickers", default=None, help="Comma-separated ticker list")
    parser.add_argument("--max-rows", type=int, default=None, help="Maximum latest rows to inspect")
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format")
    parser.add_argument("--include-sample-rows", type=int, default=0, help="Number of candidate previews to include")
    parser.add_argument("--fail-if-blocked", action="store_true", help="Exit nonzero on blocked status")
    parser.add_argument(
        "--legacy-availability-policy",
        choices=AVAILABILITY_POLICY_CHOICES,
        default=POLICY_REQUIRED,
        help="Policy for legacy candidate available_at_utc handling",
    )
    parser.add_argument(
        "--legacy-available-at-utc",
        default=None,
        help="UTC timestamp required by live_safe_legacy_baseline, e.g. 2026-06-19T00:00:00Z",
    )
    parser.add_argument(
        "--legacy-availability-lag-days",
        type=int,
        default=None,
        help="Positive day lag required by research_estimated_legacy",
    )
    parser.add_argument(
        "--verified-availability-file",
        default=None,
        help="Local CSV or JSONL file required by externally_verified_release_date",
    )
    return parser.parse_args()


def open_readonly_db(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"SQLite database not found: {resolved}")
    uri = f"file:{quote(str(resolved))}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    return conn


def normalize_market(market: str) -> str:
    normalized = market.strip().lower()
    if not normalized:
        raise ValueError("market must be non-empty")
    return normalized


def normalize_ticker(ticker: Any) -> str | None:
    if ticker is None:
        return None
    normalized = str(ticker).strip().upper()
    if not normalized:
        return None
    return normalized


def parse_tickers_arg(raw_tickers: str | None) -> list[str] | None:
    if raw_tickers is None:
        return None
    tickers: list[str] = []
    seen = set()
    for part in raw_tickers.replace("\n", ",").split(","):
        ticker = normalize_ticker(part)
        if ticker is None or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def build_availability_policy_context(
    policy: str = POLICY_REQUIRED,
    legacy_available_at_utc: str | None = None,
    legacy_availability_lag_days: int | None = None,
    verified_availability_file: Path | None = None,
) -> dict[str, Any]:
    if policy not in AVAILABILITY_POLICY_CHOICES:
        raise ValueError(f"unknown legacy availability policy: {policy}")
    if policy == POLICY_REQUIRED:
        return _policy_context(
            policy=policy,
            available_at_is_synthetic=False,
            available_at_is_externally_verified=False,
            availability_quality="LEGACY_ESTIMATED",
            legacy_available_at_utc=None,
            legacy_availability_lag_days=None,
            verified_availability_file=None,
            verified_availability_row_count=0,
            verified_availability_by_key={},
            production_backtest_warning=(
                "No legacy available_at_utc is synthesized; candidates require an explicit availability policy."
            ),
        )
    if policy == POLICY_LIVE_SAFE_LEGACY_BASELINE:
        if _is_missing(legacy_available_at_utc):
            raise ValueError("--legacy-available-at-utc is required for live_safe_legacy_baseline")
        normalized_available_at = normalize_utc_timestamp(str(legacy_available_at_utc), "--legacy-available-at-utc")
        return _policy_context(
            policy=policy,
            available_at_is_synthetic=True,
            available_at_is_externally_verified=False,
            availability_quality="LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL",
            legacy_available_at_utc=normalized_available_at,
            legacy_availability_lag_days=None,
            verified_availability_file=None,
            verified_availability_row_count=0,
            verified_availability_by_key={},
            production_backtest_warning=(
                "Live-safe baseline makes legacy rows available only from the explicit backfill timestamp; "
                "historical backtests before that timestamp will not see these legacy vintages."
            ),
        )
    if policy == POLICY_RESEARCH_ESTIMATED_LEGACY:
        if legacy_availability_lag_days is None:
            raise ValueError("--legacy-availability-lag-days is required for research_estimated_legacy")
        if legacy_availability_lag_days < 1:
            raise ValueError("--legacy-availability-lag-days must be >= 1")
        return _policy_context(
            policy=policy,
            available_at_is_synthetic=True,
            available_at_is_externally_verified=False,
            availability_quality="LEGACY_ESTIMATED",
            legacy_available_at_utc=None,
            legacy_availability_lag_days=legacy_availability_lag_days,
            verified_availability_file=None,
            verified_availability_row_count=0,
            verified_availability_by_key={},
            production_backtest_warning=(
                "Research-estimated availability is not audit-grade and must not be used as production ESS "
                "evidence without explicit later approval."
            ),
        )
    if verified_availability_file is None:
        raise ValueError("--verified-availability-file is required for externally_verified_release_date")
    verified_by_key = load_verified_availability_file(verified_availability_file)
    return _policy_context(
        policy=policy,
        available_at_is_synthetic=False,
        available_at_is_externally_verified=True,
        availability_quality="EXTERNALLY_VERIFIED",
        legacy_available_at_utc=None,
        legacy_availability_lag_days=None,
        verified_availability_file=str(verified_availability_file.expanduser().resolve()),
        verified_availability_row_count=len(verified_by_key),
        verified_availability_by_key=verified_by_key,
        production_backtest_warning=(
            "Externally verified mode uses only the provided local file in this phase; no provider data was fetched."
        ),
    )


def load_verified_availability_file(file_path: Path) -> dict[tuple[str, str, str], dict[str, Any]]:
    resolved = file_path.expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"verified availability file not found: {resolved}")
    suffix = resolved.suffix.lower()
    if suffix == ".csv":
        rows = _read_verified_availability_csv(resolved)
    elif suffix in {".jsonl", ".ndjson"}:
        rows = _read_verified_availability_jsonl(resolved)
    else:
        raise ValueError("--verified-availability-file must be .csv, .jsonl, or .ndjson")
    verified_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row_number, row in enumerate(rows, start=1):
        normalized = normalize_verified_availability_row(row, row_number)
        key = (normalized["market"], normalized["ticker"], normalized["period_end_date"])
        if key in verified_by_key:
            raise ValueError(
                "duplicate verified availability row for "
                f"market={key[0]} ticker={key[1]} period_end_date={key[2]}"
            )
        verified_by_key[key] = normalized
    return verified_by_key


def normalize_verified_availability_row(row: dict[str, Any], row_number: int) -> dict[str, Any]:
    missing_fields = [field_name for field_name in VERIFIED_AVAILABILITY_REQUIRED_FIELDS if _is_missing(row.get(field_name))]
    if missing_fields:
        raise ValueError(f"verified availability row {row_number} missing required fields: {','.join(missing_fields)}")
    market = normalize_market(str(row["market"]))
    ticker = normalize_ticker(row["ticker"])
    if ticker is None:
        raise ValueError(f"verified availability row {row_number} has empty ticker")
    period_end_date = str(row["period_end_date"]).strip()
    validate_date(period_end_date, f"verified availability row {row_number} period_end_date")
    available_at_utc = normalize_utc_timestamp(str(row["available_at_utc"]), f"verified availability row {row_number} available_at_utc")
    verified_at_utc = normalize_utc_timestamp(str(row["verified_at_utc"]), f"verified availability row {row_number} verified_at_utc")
    normalized: dict[str, Any] = {key: row.get(key) for key in row}
    normalized.update(
        {
            "market": market,
            "ticker": ticker,
            "period_end_date": period_end_date,
            "available_at_utc": available_at_utc,
            "verified_at_utc": verified_at_utc,
            "source_provider": str(row["source_provider"]).strip(),
            "source_document_id": str(row["source_document_id"]).strip(),
            "source_hash": str(row["source_hash"]).strip(),
        }
    )
    return normalized


def validate_date(value: str, field_name: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def normalize_utc_timestamp(value: str, field_name: str) -> str:
    candidate = value.strip()
    if not candidate.endswith("Z"):
        raise ValueError(f"{field_name} must use UTC Z format, e.g. 2026-06-19T00:00:00Z")
    try:
        datetime.strptime(candidate, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise ValueError(f"{field_name} must use UTC Z format, e.g. 2026-06-19T00:00:00Z") from exc
    return candidate


def _read_verified_availability_csv(file_path: Path) -> list[dict[str, Any]]:
    with file_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_verified_availability_jsonl(file_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with file_path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                raise ValueError(f"verified availability JSONL line {line_number} must be an object")
            rows.append(parsed)
    return rows


def _policy_context(**kwargs: Any) -> dict[str, Any]:
    return kwargs


def list_existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()
    return {str(row[0]) for row in rows}


def table_columns(conn: sqlite3.Connection, table_name: str, existing_tables: set[str] | None = None) -> set[str]:
    if existing_tables is not None and table_name not in existing_tables:
        return set()
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def load_latest_quarterly_rows(
    conn: sqlite3.Connection,
    tickers: list[str] | None,
    max_rows: int | None,
    as_of_date: str,
) -> list[dict[str, Any]]:
    if max_rows is not None and max_rows < 1:
        raise ValueError("--max-rows must be >= 1")
    params: list[Any] = [as_of_date]
    ticker_sql = ""
    if tickers is not None:
        if not tickers:
            return []
        placeholders = ",".join("?" for _ in tickers)
        ticker_sql = f"AND ticker IN ({placeholders})"
        params.extend(tickers)
    limit_sql = ""
    if max_rows is not None:
        limit_sql = "LIMIT ?"
        params.append(max_rows)
    rows = conn.execute(
        f"""
        SELECT {", ".join(LATEST_COLUMNS)}
        FROM rc_fundamental_quarterly
        WHERE period_end_date <= ?
        {ticker_sql}
        ORDER BY ticker ASC, period_end_date ASC
        {limit_sql}
        """,
        tuple(params),
    ).fetchall()
    return [dict(zip(LATEST_COLUMNS, row, strict=True)) for row in rows]


def build_legacy_source_hash(row: dict[str, Any], market: str) -> str:
    ticker = normalize_ticker(row.get("ticker"))
    period_end_date = row.get("period_end_date")
    if ticker is None or _is_missing(period_end_date):
        raise ValueError("SOURCE_HASH_INPUT_MISSING")
    payload = {
        "market": market,
        "ticker": ticker,
        "period_end_date": str(period_end_date),
        "currency": row.get("currency"),
    }
    for field_name in FINANCIAL_FIELDS:
        payload[field_name] = row.get(field_name)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_legacy_statement_vintage_id(row: dict[str, Any], market: str, source_hash: str) -> str:
    ticker = normalize_ticker(row.get("ticker"))
    period_end_date = row.get("period_end_date")
    if ticker is None or _is_missing(period_end_date) or not source_hash:
        raise ValueError("STATEMENT_VINTAGE_ID_INPUT_MISSING")
    return f"legacy:{market}:{ticker}:{period_end_date}:{source_hash[:16]}"


def detect_existing_vintage(
    conn: sqlite3.Connection,
    market: str,
    ticker: str,
    period_end_date: str,
    statement_vintage_id: str,
) -> dict[str, bool]:
    same_period = conn.execute(
        """
        SELECT 1
        FROM rc_fundamental_quarterly_vintage
        WHERE market = ?
          AND ticker = ?
          AND period_end_date = ?
        LIMIT 1
        """,
        (market, ticker, period_end_date),
    ).fetchone()
    same_id = conn.execute(
        """
        SELECT 1
        FROM rc_fundamental_quarterly_vintage
        WHERE statement_vintage_id = ?
        LIMIT 1
        """,
        (statement_vintage_id,),
    ).fetchone()
    return {"same_period": same_period is not None, "same_id": same_id is not None}


def build_candidate_vintage_preview(
    conn: sqlite3.Connection,
    row: dict[str, Any],
    market: str,
    dry_run_id: str,
    availability_policy: dict[str, Any],
) -> dict[str, Any]:
    ticker = normalize_ticker(row.get("ticker"))
    if ticker is None:
        return _skipped_candidate(row, market, "MISSING_TICKER")
    period_end_date = row.get("period_end_date")
    if _is_missing(period_end_date):
        return _skipped_candidate(row, market, "MISSING_PERIOD_END_DATE", ticker=ticker)
    try:
        source_hash = build_legacy_source_hash(row, market)
        statement_vintage_id = build_legacy_statement_vintage_id(row, market, source_hash)
    except ValueError as exc:
        return _skipped_candidate(row, market, str(exc), ticker=ticker)
    existing = detect_existing_vintage(conn, market, ticker, str(period_end_date), statement_vintage_id)
    if existing["same_period"]:
        return _skipped_candidate(row, market, "ALREADY_HAS_VINTAGE", ticker=ticker)
    if existing["same_id"]:
        return _skipped_candidate(row, market, "DUPLICATE_STATEMENT_VINTAGE_ID", ticker=ticker)
    provenance_count = count_non_null_financial_fields(row)
    policy_result = resolve_candidate_availability(row, market, availability_policy)
    return {
        "status": "PLANNED",
        "ticker": ticker,
        "market": market,
        "period_end_date": str(period_end_date),
        "statement_vintage_id": statement_vintage_id,
        "source_hash": source_hash,
        "availability_quality": policy_result["availability_quality"],
        "available_at_utc": policy_result["available_at_utc"],
        "ingested_at_utc": None,
        "dry_run_id": dry_run_id,
        "planned_field_provenance_count": provenance_count,
        "requires_policy_decision": policy_result["requires_policy_decision"],
        "availability_policy": availability_policy["policy"],
        "verification_source": policy_result["verification_source"],
        "skip_reason": None,
        "warnings": policy_result["warnings"],
    }


def resolve_candidate_availability(
    row: dict[str, Any],
    market: str,
    availability_policy: dict[str, Any],
) -> dict[str, Any]:
    policy = availability_policy["policy"]
    if policy == POLICY_REQUIRED:
        return {
            "available_at_utc": None,
            "availability_quality": "LEGACY_ESTIMATED",
            "requires_policy_decision": True,
            "verification_source": None,
            "warnings": [
                "AVAILABLE_AT_REQUIRES_POLICY_DECISION",
                "LEGACY_PLACEHOLDER_METADATA_ONLY",
            ],
        }
    if policy == POLICY_LIVE_SAFE_LEGACY_BASELINE:
        return {
            "available_at_utc": availability_policy["legacy_available_at_utc"],
            "availability_quality": "LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL",
            "requires_policy_decision": False,
            "verification_source": None,
            "warnings": [
                "LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL",
                "HISTORICAL_BACKTESTS_BEFORE_BACKFILL_TIMESTAMP_WILL_NOT_SEE_THESE_VINTAGES",
            ],
        }
    if policy == POLICY_RESEARCH_ESTIMATED_LEGACY:
        available_at_utc = estimate_available_at_utc_from_period_end(
            str(row["period_end_date"]),
            int(availability_policy["legacy_availability_lag_days"]),
        )
        return {
            "available_at_utc": available_at_utc,
            "availability_quality": "LEGACY_ESTIMATED",
            "requires_policy_decision": False,
            "verification_source": None,
            "warnings": [
                "RESEARCH_ESTIMATED_AVAILABILITY_NOT_AUDIT_GRADE",
                "DO_NOT_USE_AS_PRODUCTION_ESS_EVIDENCE_WITHOUT_LATER_APPROVAL",
            ],
        }
    key = (market, normalize_ticker(row.get("ticker")) or "", str(row["period_end_date"]))
    verified_row = availability_policy["verified_availability_by_key"].get(key)
    if verified_row is None:
        return {
            "available_at_utc": None,
            "availability_quality": "EXTERNALLY_VERIFIED",
            "requires_policy_decision": True,
            "verification_source": None,
            "warnings": [
                "REQUIRES_VERIFIED_AVAILABILITY",
                "NO_PROVIDER_DATA_FETCHED",
            ],
        }
    return {
        "available_at_utc": verified_row["available_at_utc"],
        "availability_quality": "EXTERNALLY_VERIFIED",
        "requires_policy_decision": False,
        "verification_source": {
            "source_provider": verified_row["source_provider"],
            "source_document_id": verified_row["source_document_id"],
            "source_hash": verified_row["source_hash"],
            "verified_at_utc": verified_row["verified_at_utc"],
            "filed_at_utc": verified_row.get("filed_at_utc"),
            "source_url_or_ref": verified_row.get("source_url_or_ref"),
            "source_confidence": verified_row.get("source_confidence"),
            "notes": verified_row.get("notes"),
        },
        "warnings": [
            "EXTERNALLY_VERIFIED_FROM_LOCAL_FILE",
            "NO_PROVIDER_DATA_FETCHED",
        ],
    }


def estimate_available_at_utc_from_period_end(period_end_date: str, lag_days: int) -> str:
    validate_date(period_end_date, "period_end_date")
    available_date = datetime.strptime(period_end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=lag_days)
    return available_date.strftime("%Y-%m-%dT00:00:00Z")


def build_candidate_provenance_preview(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "statement_vintage_id": candidate["statement_vintage_id"],
        "source_provider": "UNKNOWN_LEGACY",
        "provenance_role": "LEGACY_BASELINE",
        "merge_action": "LEGACY_BACKFILL_BASELINE",
        "created_by_run_id": candidate["dry_run_id"],
        "planned_rows": candidate["planned_field_provenance_count"],
    }


def count_non_null_financial_fields(row: dict[str, Any]) -> int:
    return sum(1 for field_name in FINANCIAL_FIELDS if row.get(field_name) is not None)


def run_dry_run(
    fundamentals_db_path: Path,
    market: str,
    as_of_date: str,
    tickers: list[str] | None = None,
    max_rows: int | None = None,
    include_sample_rows: int = 0,
    legacy_availability_policy: str = POLICY_REQUIRED,
    legacy_available_at_utc: str | None = None,
    legacy_availability_lag_days: int | None = None,
    verified_availability_file: Path | None = None,
) -> dict[str, Any]:
    market = normalize_market(market)
    if include_sample_rows < 0:
        raise ValueError("--include-sample-rows must be >= 0")
    availability_policy = build_availability_policy_context(
        policy=legacy_availability_policy,
        legacy_available_at_utc=legacy_available_at_utc,
        legacy_availability_lag_days=legacy_availability_lag_days,
        verified_availability_file=verified_availability_file,
    )
    normalized_tickers = [ticker for ticker in (normalize_ticker(ticker) for ticker in tickers or []) if ticker is not None]
    dry_run_id = f"dry-run:legacy-reported-vintage:{market}:{as_of_date}"
    with open_readonly_db(fundamentals_db_path) as conn:
        existing_tables = list_existing_tables(conn)
        missing_tables = [table_name for table_name in REQUIRED_TABLES if table_name not in existing_tables]
        if missing_tables:
            report = _empty_report(fundamentals_db_path, market, as_of_date, dry_run_id, availability_policy)
            report["summary"]["overall_status"] = STATUS_BLOCKED_MISSING_SCHEMA
            report["summary"]["blocked_rows"] = len(missing_tables)
            report["blocked_reasons"] = [f"MISSING_TABLE:{table_name}" for table_name in missing_tables]
            return report
        latest_rows = load_latest_quarterly_rows(
            conn,
            normalized_tickers if tickers is not None else None,
            max_rows,
            as_of_date,
        )
        planned: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for row in latest_rows:
            candidate = build_candidate_vintage_preview(conn, row, market, dry_run_id, availability_policy)
            if candidate["status"] == "PLANNED":
                planned.append(candidate)
            else:
                skipped.append(candidate)

    return build_report(
        fundamentals_db_path,
        market,
        as_of_date,
        dry_run_id,
        latest_rows,
        planned,
        skipped,
        include_sample_rows,
        availability_policy,
    )


def build_report(
    fundamentals_db_path: Path,
    market: str,
    as_of_date: str,
    dry_run_id: str,
    latest_rows: list[dict[str, Any]],
    planned: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    include_sample_rows: int,
    availability_policy: dict[str, Any],
) -> dict[str, Any]:
    planned_provenance_rows = sum(candidate["planned_field_provenance_count"] for candidate in planned)
    requires_policy_decision_rows = sum(1 for candidate in planned if candidate["requires_policy_decision"])
    already_has_vintage_rows = sum(1 for candidate in skipped if candidate["skip_reason"] == "ALREADY_HAS_VINTAGE")
    blocked_rows = sum(1 for candidate in skipped if candidate["skip_reason"] not in {"ALREADY_HAS_VINTAGE"})
    if not latest_rows:
        overall_status = STATUS_NO_SOURCE_ROWS
    elif requires_policy_decision_rows > 0:
        overall_status = STATUS_DRY_RUN_PARTIAL_POLICY_REQUIRED
    else:
        overall_status = STATUS_DRY_RUN_READY
    return {
        "summary": {
            "fundamentals_db": str(fundamentals_db_path.expanduser().resolve()),
            "market": market,
            "as_of_date": as_of_date,
            "dry_run_id": dry_run_id,
            "total_latest_rows": len(latest_rows),
            "candidate_rows": len(planned),
            "planned_vintage_rows": len(planned),
            "planned_provenance_rows": planned_provenance_rows,
            "already_has_vintage_rows": already_has_vintage_rows,
            "skipped_rows": len(skipped),
            "blocked_rows": blocked_rows,
            "requires_policy_decision_rows": requires_policy_decision_rows,
            "warning_count": sum(len(candidate["warnings"]) for candidate in planned + skipped),
            "overall_status": overall_status,
        },
        "policy": build_policy(availability_policy),
        "candidate_samples": planned[:include_sample_rows],
        "skipped_reasons": _count_reasons(skipped),
        "blocked_reasons": _count_reasons([candidate for candidate in skipped if candidate["skip_reason"] != "ALREADY_HAS_VINTAGE"]),
    }


def build_policy(availability_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    if availability_policy is None:
        availability_policy = build_availability_policy_context()
    return {
        "selected_policy": availability_policy["policy"],
        "available_at_is_synthetic": availability_policy["available_at_is_synthetic"],
        "available_at_is_externally_verified": availability_policy["available_at_is_externally_verified"],
        "legacy_available_at_utc": availability_policy["legacy_available_at_utc"],
        "legacy_availability_lag_days": availability_policy["legacy_availability_lag_days"],
        "verified_availability_file": availability_policy["verified_availability_file"],
        "verified_availability_row_count": availability_policy["verified_availability_row_count"],
        "availability_quality": availability_policy["availability_quality"],
        "production_backtest_warning": availability_policy["production_backtest_warning"],
        "statement_vintage_id_format": "legacy:{market}:{ticker}:{period_end_date}:{source_hash_prefix_16}",
        "source_hash_algorithm": "sha256",
        "source_hash_fields": ["market", "ticker", "period_end_date", *FINANCIAL_FIELDS, "currency"],
        "metadata_placeholder_policy": {
            "source_provider": "UNKNOWN_LEGACY",
            "source_document_id": None,
            "filed_at_utc": None,
            "provider_observed_at_utc": None,
            "provider_run_id": None,
            "normalization_run_id": "latest row run_id if later applied",
            "revision_number": 1,
            "is_restated": 0,
            "supersedes_vintage_id": None,
            "availability_quality": "LEGACY_ESTIMATED",
        },
        "availability_policy": {
            "selected_policy": availability_policy["policy"],
            "available_at_utc": availability_policy["legacy_available_at_utc"],
            "status": "REQUIRES_POLICY_DECISION" if availability_policy["policy"] == POLICY_REQUIRED else "EXPLICIT_POLICY_SELECTED",
            "note": "Dry-run does not pretend period_end_date is true availability.",
        },
        "provenance_policy": {
            "source_provider": "UNKNOWN_LEGACY",
            "provenance_role": "LEGACY_BASELINE",
            "merge_action": "LEGACY_BACKFILL_BASELINE",
            "null_financial_fields": "no provenance row planned",
        },
    }


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def render_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "summary",
        f"fundamentals_db: {summary['fundamentals_db']}",
        f"market: {summary['market']}",
        f"as_of_date: {summary['as_of_date']}",
        f"dry_run_id: {summary['dry_run_id']}",
        f"total_latest_rows: {summary['total_latest_rows']}",
        f"candidate_rows: {summary['candidate_rows']}",
        f"planned_vintage_rows: {summary['planned_vintage_rows']}",
        f"planned_provenance_rows: {summary['planned_provenance_rows']}",
        f"already_has_vintage_rows: {summary['already_has_vintage_rows']}",
        f"skipped_rows: {summary['skipped_rows']}",
        f"blocked_rows: {summary['blocked_rows']}",
        f"requires_policy_decision_rows: {summary['requires_policy_decision_rows']}",
        f"warning_count: {summary['warning_count']}",
        f"overall_status: {summary['overall_status']}",
        "",
        "policy",
        f"selected_policy: {report['policy']['selected_policy']}",
        f"available_at_is_synthetic: {report['policy']['available_at_is_synthetic']}",
        f"available_at_is_externally_verified: {report['policy']['available_at_is_externally_verified']}",
        f"legacy_available_at_utc: {_text(report['policy']['legacy_available_at_utc'])}",
        f"legacy_availability_lag_days: {_text(report['policy']['legacy_availability_lag_days'])}",
        f"verified_availability_file: {_text(report['policy']['verified_availability_file'])}",
        f"verified_availability_row_count: {report['policy']['verified_availability_row_count']}",
        f"availability_quality: {report['policy']['availability_quality']}",
        f"production_backtest_warning: {report['policy']['production_backtest_warning']}",
        f"statement_vintage_id_format: {report['policy']['statement_vintage_id_format']}",
        f"source_hash_fields: {','.join(report['policy']['source_hash_fields'])}",
        f"availability_policy: {report['policy']['availability_policy']['status']}",
        "",
        "candidate_samples",
        "ticker;market;period_end_date;statement_vintage_id;source_hash;availability_policy;availability_quality;available_at_utc;planned_field_provenance_count;warnings",
    ]
    for candidate in report["candidate_samples"]:
        lines.append(
            ";".join(
                [
                    _text(candidate["ticker"]),
                    _text(candidate["market"]),
                    _text(candidate["period_end_date"]),
                    _text(candidate["statement_vintage_id"]),
                    _text(candidate["source_hash"]),
                    _text(candidate["availability_policy"]),
                    _text(candidate["availability_quality"]),
                    _text(candidate["available_at_utc"]),
                    _text(candidate["planned_field_provenance_count"]),
                    ",".join(candidate["warnings"]),
                ]
            )
        )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    try:
        report = run_dry_run(
            fundamentals_db_path=Path(args.fundamentals_db),
            market=args.market,
            as_of_date=args.as_of_date,
            tickers=parse_tickers_arg(args.tickers),
            max_rows=args.max_rows,
            include_sample_rows=args.include_sample_rows,
            legacy_availability_policy=args.legacy_availability_policy,
            legacy_available_at_utc=args.legacy_available_at_utc,
            legacy_availability_lag_days=args.legacy_availability_lag_days,
            verified_availability_file=Path(args.verified_availability_file) if args.verified_availability_file else None,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    print(render_json(report) if args.format == "json" else render_text(report))
    if args.fail_if_blocked and report["summary"]["overall_status"] == STATUS_BLOCKED_MISSING_SCHEMA:
        raise SystemExit(1)


def _empty_report(
    fundamentals_db_path: Path,
    market: str,
    as_of_date: str,
    dry_run_id: str,
    availability_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "summary": {
            "fundamentals_db": str(fundamentals_db_path.expanduser().resolve()),
            "market": market,
            "as_of_date": as_of_date,
            "dry_run_id": dry_run_id,
            "total_latest_rows": 0,
            "candidate_rows": 0,
            "planned_vintage_rows": 0,
            "planned_provenance_rows": 0,
            "already_has_vintage_rows": 0,
            "skipped_rows": 0,
            "blocked_rows": 0,
            "requires_policy_decision_rows": 0,
            "warning_count": 0,
            "overall_status": STATUS_UNKNOWN,
        },
        "policy": build_policy(availability_policy),
        "candidate_samples": [],
        "skipped_reasons": {},
        "blocked_reasons": {},
    }


def _skipped_candidate(
    row: dict[str, Any],
    market: str,
    reason: str,
    ticker: str | None = None,
) -> dict[str, Any]:
    return {
        "status": "SKIPPED",
        "ticker": ticker if ticker is not None else normalize_ticker(row.get("ticker")),
        "market": market,
        "period_end_date": row.get("period_end_date"),
        "statement_vintage_id": None,
        "source_hash": None,
        "availability_quality": None,
        "available_at_utc": None,
        "availability_policy": None,
        "verification_source": None,
        "planned_field_provenance_count": 0,
        "requires_policy_decision": False,
        "skip_reason": reason,
        "warnings": [reason],
    }


def _count_reasons(candidates: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        reason = str(candidate["skip_reason"])
        counts[reason] = counts.get(reason, 0) + 1
    return counts


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _text(value: Any) -> str:
    return "" if value is None else str(value)


if __name__ == "__main__":
    main()
