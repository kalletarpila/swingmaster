from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote


STATUS_DRY_RUN_READY = "DRY_RUN_READY"
STATUS_DRY_RUN_PARTIAL_POLICY_REQUIRED = "DRY_RUN_PARTIAL_POLICY_REQUIRED"
STATUS_BLOCKED_MISSING_SCHEMA = "BLOCKED_MISSING_SCHEMA"
STATUS_NO_SOURCE_ROWS = "NO_SOURCE_ROWS"
STATUS_UNKNOWN = "UNKNOWN"
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
    return {
        "status": "PLANNED",
        "ticker": ticker,
        "market": market,
        "period_end_date": str(period_end_date),
        "statement_vintage_id": statement_vintage_id,
        "source_hash": source_hash,
        "availability_quality": "LEGACY_ESTIMATED",
        "available_at_utc": None,
        "ingested_at_utc": None,
        "dry_run_id": dry_run_id,
        "planned_field_provenance_count": provenance_count,
        "requires_policy_decision": True,
        "skip_reason": None,
        "warnings": [
            "AVAILABLE_AT_REQUIRES_POLICY_DECISION",
            "LEGACY_PLACEHOLDER_METADATA_ONLY",
        ],
    }


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
) -> dict[str, Any]:
    market = normalize_market(market)
    if include_sample_rows < 0:
        raise ValueError("--include-sample-rows must be >= 0")
    normalized_tickers = [ticker for ticker in (normalize_ticker(ticker) for ticker in tickers or []) if ticker is not None]
    dry_run_id = f"dry-run:legacy-reported-vintage:{market}:{as_of_date}"
    with open_readonly_db(fundamentals_db_path) as conn:
        existing_tables = list_existing_tables(conn)
        missing_tables = [table_name for table_name in REQUIRED_TABLES if table_name not in existing_tables]
        if missing_tables:
            report = _empty_report(fundamentals_db_path, market, as_of_date, dry_run_id)
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
            candidate = build_candidate_vintage_preview(conn, row, market, dry_run_id)
            if candidate["status"] == "PLANNED":
                planned.append(candidate)
            else:
                skipped.append(candidate)

    return build_report(fundamentals_db_path, market, as_of_date, dry_run_id, latest_rows, planned, skipped, include_sample_rows)


def build_report(
    fundamentals_db_path: Path,
    market: str,
    as_of_date: str,
    dry_run_id: str,
    latest_rows: list[dict[str, Any]],
    planned: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    include_sample_rows: int,
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
        "policy": build_policy(),
        "candidate_samples": planned[:include_sample_rows],
        "skipped_reasons": _count_reasons(skipped),
        "blocked_reasons": _count_reasons([candidate for candidate in skipped if candidate["skip_reason"] != "ALREADY_HAS_VINTAGE"]),
    }


def build_policy() -> dict[str, Any]:
    return {
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
            "available_at_utc": None,
            "status": "REQUIRES_POLICY_DECISION",
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
        f"statement_vintage_id_format: {report['policy']['statement_vintage_id_format']}",
        f"source_hash_fields: {','.join(report['policy']['source_hash_fields'])}",
        "availability_policy: REQUIRES_POLICY_DECISION",
        "",
        "candidate_samples",
        "ticker;market;period_end_date;statement_vintage_id;source_hash;availability_quality;available_at_utc;planned_field_provenance_count;warnings",
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
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    print(render_json(report) if args.format == "json" else render_text(report))
    if args.fail_if_blocked and report["summary"]["overall_status"] == STATUS_BLOCKED_MISSING_SCHEMA:
        raise SystemExit(1)


def _empty_report(fundamentals_db_path: Path, market: str, as_of_date: str, dry_run_id: str) -> dict[str, Any]:
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
        "policy": build_policy(),
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
