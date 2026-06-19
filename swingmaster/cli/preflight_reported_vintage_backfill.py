from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote


STATUS_OK_READY_FOR_BACKFILL_DESIGN = "OK_READY_FOR_BACKFILL_DESIGN"
STATUS_PARTIAL_METADATA_REQUIRED = "PARTIAL_METADATA_REQUIRED"
STATUS_BLOCKED_MISSING_SCHEMA = "BLOCKED_MISSING_SCHEMA"
STATUS_NO_SOURCE_ROWS = "NO_SOURCE_ROWS"
LATEST_COLUMNS = (
    "ticker",
    "period_end_date",
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
    "currency",
    "run_id",
)
REQUIRED_VINTAGE_METADATA_FIELDS = (
    "market",
    "ticker",
    "period_end_date",
    "statement_vintage_id",
    "source_provider",
    "source_document_id",
    "source_hash",
    "filed_at_utc",
    "available_at_utc",
    "ingested_at_utc",
    "provider_observed_at_utc",
    "run_id",
    "provider_run_id",
    "normalization_run_id",
    "revision_number",
    "is_restated",
    "supersedes_vintage_id",
    "availability_quality",
)
UNSAFE_METADATA_FIELDS = (
    "statement_vintage_id",
    "source_provider",
    "source_document_id",
    "source_hash",
    "filed_at_utc",
    "available_at_utc",
    "ingested_at_utc",
    "provider_observed_at_utc",
    "provider_run_id",
    "normalization_run_id",
    "supersedes_vintage_id",
    "availability_quality",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only reported vintage backfill preflight")
    parser.add_argument("--fundamentals-db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", required=True, help="Market code, e.g. usa or omxh")
    parser.add_argument("--as-of-date", required=True, help="Latest quarterly period cutoff in YYYY-MM-DD format")
    parser.add_argument("--tickers", default=None, help="Comma-separated ticker list")
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format")
    parser.add_argument("--max-tickers", type=int, default=None, help="Maximum tickers to inspect")
    parser.add_argument("--fail-if-blocked", action="store_true", help="Exit nonzero on blocked status")
    return parser.parse_args()


def normalize_market(market: str) -> str:
    normalized = market.strip().lower()
    if not normalized:
        raise ValueError("market must be non-empty")
    return normalized


def normalize_ticker(ticker: str) -> str:
    normalized = ticker.strip().upper()
    if not normalized:
        raise ValueError("ticker must be non-empty")
    return normalized


def parse_tickers_arg(raw_tickers: str | None) -> list[str] | None:
    if raw_tickers is None:
        return None
    tickers: list[str] = []
    seen = set()
    for part in raw_tickers.replace("\n", ",").split(","):
        if not part.strip():
            continue
        ticker = normalize_ticker(part)
        if ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def open_readonly_db(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"SQLite database not found: {resolved}")
    uri = f"file:{quote(str(resolved))}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.execute("PRAGMA query_only=ON")
    return conn


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
    existing_tables: set[str],
    tickers: list[str] | None,
    max_tickers: int | None,
    as_of_date: str,
) -> list[dict[str, Any]]:
    if max_tickers is not None and max_tickers < 1:
        raise ValueError("--max-tickers must be >= 1")
    if "rc_fundamental_quarterly" not in existing_tables:
        return []
    params: list[Any] = [as_of_date]
    ticker_filter = ""
    selected_tickers = tickers
    if selected_tickers is None:
        selected_tickers = _load_source_tickers(conn, as_of_date, max_tickers)
    elif max_tickers is not None:
        selected_tickers = selected_tickers[:max_tickers]
    if selected_tickers is not None:
        if not selected_tickers:
            return []
        placeholders = ",".join("?" for _ in selected_tickers)
        ticker_filter = f"AND ticker IN ({placeholders})"
        params.extend(selected_tickers)
    rows = conn.execute(
        f"""
        SELECT {", ".join(LATEST_COLUMNS)}
        FROM rc_fundamental_quarterly
        WHERE period_end_date <= ?
        {ticker_filter}
        ORDER BY ticker ASC, period_end_date ASC
        """,
        tuple(params),
    ).fetchall()
    return [dict(zip(LATEST_COLUMNS, row, strict=True)) for row in rows]


def count_existing_vintage_rows(conn: sqlite3.Connection, existing_tables: set[str], market: str) -> int:
    if "rc_fundamental_quarterly_vintage" not in existing_tables:
        return 0
    columns = table_columns(conn, "rc_fundamental_quarterly_vintage", existing_tables)
    if "market" in columns:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarterly_vintage
            WHERE market = ?
            """,
            (market,),
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()
    return int(row[0]) if row is not None else 0


def find_matching_vintage(
    conn: sqlite3.Connection,
    existing_tables: set[str],
    ticker: str,
    period_end_date: str,
    market: str,
) -> bool:
    if "rc_fundamental_quarterly_vintage" not in existing_tables:
        return False
    columns = table_columns(conn, "rc_fundamental_quarterly_vintage", existing_tables)
    if "market" in columns:
        row = conn.execute(
            """
            SELECT 1
            FROM rc_fundamental_quarterly_vintage
            WHERE ticker = ?
              AND period_end_date = ?
              AND market = ?
            LIMIT 1
            """,
            (ticker, period_end_date, market),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT 1
            FROM rc_fundamental_quarterly_vintage
            WHERE ticker = ?
              AND period_end_date = ?
            LIMIT 1
            """,
            (ticker, period_end_date),
        ).fetchone()
    return row is not None


def inspect_backfill_candidate(
    conn: sqlite3.Connection,
    existing_tables: set[str],
    market: str,
    latest_row: dict[str, Any],
) -> dict[str, Any]:
    ticker = normalize_ticker(str(latest_row["ticker"]))
    period_end_date = str(latest_row["period_end_date"])
    matching_vintage_exists = find_matching_vintage(conn, existing_tables, ticker, period_end_date, market)
    missing_required_metadata = list(UNSAFE_METADATA_FIELDS)
    return {
        "ticker": ticker,
        "market": market,
        "period_end_date": period_end_date,
        "latest_row_present": True,
        "matching_vintage_exists": matching_vintage_exists,
        "inferred_statement_vintage_id": _synthetic_statement_vintage_id(market, ticker, period_end_date),
        "eligible_for_backfill": not matching_vintage_exists,
        "metadata_status": "INCOMPLETE_LEGACY_METADATA",
        "missing_required_metadata": missing_required_metadata,
        "proposed_availability_quality": "LEGACY_ESTIMATED",
        "proposed_source_provider": "UNKNOWN_LEGACY",
        "warnings": [
            "AVAILABLE_AT_NOT_RECONSTRUCTABLE_FROM_LATEST",
            "SOURCE_PROVIDER_NOT_PROVEN_FROM_LATEST",
        ],
    }


def inspect_metadata_gaps(existing_tables: set[str]) -> list[dict[str, Any]]:
    latest_exists = "rc_fundamental_quarterly" in existing_tables
    gaps = []
    inferable_fields = {"market", "ticker", "period_end_date", "run_id", "revision_number", "is_restated"}
    proposed_fields = {
        "statement_vintage_id",
        "source_provider",
        "source_hash",
        "availability_quality",
        "supersedes_vintage_id",
    }
    for field_name in REQUIRED_VINTAGE_METADATA_FIELDS:
        can_infer = latest_exists and field_name in inferable_fields
        proposed_only = latest_exists and field_name in proposed_fields
        gaps.append(
            {
                "field": field_name,
                "can_infer_safely": can_infer,
                "proposed_only": proposed_only and not can_infer,
                "status": "AVAILABLE" if can_infer else "UNAVAILABLE",
                "note": _metadata_gap_note(field_name),
            }
        )
    return gaps


def build_candidate_backfill_policy() -> dict[str, str]:
    return {
        "statement_vintage_id": "Use deterministic synthetic legacy id based on market, ticker, period, and baseline revision.",
        "source_provider": "Use UNKNOWN_LEGACY unless source provenance can be proven separately.",
        "available_at_utc": "Cannot be accurately reconstructed from rc_fundamental_quarterly; use only a conservative placeholder if explicitly approved later.",
        "availability_quality": "Use LEGACY_ESTIMATED or UNKNOWN for synthetic baselines.",
        "is_restated": "Use 0 for synthetic baseline only if accepted as unknown-not-restated, not as provider truth.",
        "revision_number": "Use 1 for synthetic legacy baseline.",
        "source_hash": "Future backfill can hash current latest row values; this preflight does not compute or store it.",
    }


def build_summary(
    fundamentals_db_path: Path,
    market: str,
    as_of_date: str,
    latest_rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    existing_vintage_row_count: int,
    missing_vintage_table: bool,
    missing_provenance_table: bool,
) -> dict[str, Any]:
    blocked_reasons = []
    if missing_vintage_table:
        blocked_reasons.append("MISSING_TABLE:rc_fundamental_quarterly_vintage")
    if missing_provenance_table:
        blocked_reasons.append("MISSING_TABLE:rc_fundamental_quarterly_field_provenance")
    warning_count = sum(len(candidate["warnings"]) for candidate in candidates)
    already_backfilled_rows = sum(1 for candidate in candidates if candidate["matching_vintage_exists"])
    eligible_latest_rows = sum(1 for candidate in candidates if candidate["eligible_for_backfill"])
    if missing_vintage_table or missing_provenance_table:
        overall_status = STATUS_BLOCKED_MISSING_SCHEMA
    elif not latest_rows:
        overall_status = STATUS_NO_SOURCE_ROWS
    elif any(candidate["missing_required_metadata"] for candidate in candidates):
        overall_status = STATUS_PARTIAL_METADATA_REQUIRED
    else:
        overall_status = STATUS_OK_READY_FOR_BACKFILL_DESIGN
    return {
        "fundamentals_db": str(fundamentals_db_path.expanduser().resolve()),
        "market": market,
        "as_of_date": as_of_date,
        "ticker_count_checked": len({candidate["ticker"] for candidate in candidates}),
        "latest_quarterly_row_count": len(latest_rows),
        "existing_vintage_row_count": existing_vintage_row_count,
        "eligible_latest_rows": eligible_latest_rows,
        "already_backfilled_rows": already_backfilled_rows,
        "missing_vintage_table": missing_vintage_table,
        "missing_provenance_table": missing_provenance_table,
        "blocked_reason_count": len(blocked_reasons),
        "blocked_reasons": blocked_reasons,
        "warning_count": warning_count,
        "overall_status": overall_status,
    }


def run_preflight(
    fundamentals_db_path: Path,
    market: str,
    as_of_date: str,
    tickers: list[str] | None = None,
    max_tickers: int | None = None,
) -> dict[str, Any]:
    market = normalize_market(market)
    normalized_tickers = [normalize_ticker(ticker) for ticker in tickers] if tickers is not None else None
    with open_readonly_db(fundamentals_db_path) as conn:
        existing_tables = list_existing_tables(conn)
        latest_rows = load_latest_quarterly_rows(conn, existing_tables, normalized_tickers, max_tickers, as_of_date)
        existing_vintage_row_count = count_existing_vintage_rows(conn, existing_tables, market)
        candidates = [
            inspect_backfill_candidate(conn, existing_tables, market, latest_row)
            for latest_row in latest_rows
        ]
        missing_vintage_table = "rc_fundamental_quarterly_vintage" not in existing_tables
        missing_provenance_table = "rc_fundamental_quarterly_field_provenance" not in existing_tables
        metadata_gaps = inspect_metadata_gaps(existing_tables)

    return {
        "summary": build_summary(
            fundamentals_db_path,
            market,
            as_of_date,
            latest_rows,
            candidates,
            existing_vintage_row_count,
            missing_vintage_table,
            missing_provenance_table,
        ),
        "candidates": candidates,
        "metadata_gaps": metadata_gaps,
        "candidate_backfill_policy": build_candidate_backfill_policy(),
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
        f"ticker_count_checked: {summary['ticker_count_checked']}",
        f"latest_quarterly_row_count: {summary['latest_quarterly_row_count']}",
        f"existing_vintage_row_count: {summary['existing_vintage_row_count']}",
        f"eligible_latest_rows: {summary['eligible_latest_rows']}",
        f"already_backfilled_rows: {summary['already_backfilled_rows']}",
        f"missing_vintage_table: {summary['missing_vintage_table']}",
        f"missing_provenance_table: {summary['missing_provenance_table']}",
        f"blocked_reason_count: {summary['blocked_reason_count']}",
        f"warning_count: {summary['warning_count']}",
        f"overall_status: {summary['overall_status']}",
        "",
        "candidates",
        "ticker;market;period_end_date;latest_row_present;matching_vintage_exists;"
        "inferred_statement_vintage_id;eligible_for_backfill;metadata_status;"
        "missing_required_metadata;proposed_availability_quality;proposed_source_provider;warnings",
    ]
    for candidate in report["candidates"]:
        lines.append(
            ";".join(
                [
                    _text_value(candidate["ticker"]),
                    _text_value(candidate["market"]),
                    _text_value(candidate["period_end_date"]),
                    _text_value(candidate["latest_row_present"]),
                    _text_value(candidate["matching_vintage_exists"]),
                    _text_value(candidate["inferred_statement_vintage_id"]),
                    _text_value(candidate["eligible_for_backfill"]),
                    _text_value(candidate["metadata_status"]),
                    ",".join(candidate["missing_required_metadata"]),
                    _text_value(candidate["proposed_availability_quality"]),
                    _text_value(candidate["proposed_source_provider"]),
                    ",".join(candidate["warnings"]),
                ]
            )
        )
    lines.extend(["", "metadata_gaps", "field;status;can_infer_safely;proposed_only;note"])
    for gap in report["metadata_gaps"]:
        lines.append(
            ";".join(
                [
                    _text_value(gap["field"]),
                    _text_value(gap["status"]),
                    _text_value(gap["can_infer_safely"]),
                    _text_value(gap["proposed_only"]),
                    _text_value(gap["note"]),
                ]
            )
        )
    lines.extend(["", "candidate_backfill_policy"])
    for key, value in report["candidate_backfill_policy"].items():
        lines.append(f"{key}: {value}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    try:
        report = run_preflight(
            fundamentals_db_path=Path(args.fundamentals_db),
            market=args.market,
            as_of_date=args.as_of_date,
            tickers=parse_tickers_arg(args.tickers),
            max_tickers=args.max_tickers,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    if args.format == "json":
        print(render_json(report))
    else:
        print(render_text(report))
    if args.fail_if_blocked and report["summary"]["overall_status"] == STATUS_BLOCKED_MISSING_SCHEMA:
        raise SystemExit(1)


def _load_source_tickers(conn: sqlite3.Connection, as_of_date: str, max_tickers: int | None) -> list[str]:
    sql = """
        SELECT DISTINCT ticker
        FROM rc_fundamental_quarterly
        WHERE period_end_date <= ?
          AND ticker IS NOT NULL
        ORDER BY ticker ASC
    """
    if max_tickers is not None:
        sql += "\nLIMIT ?"
        rows = conn.execute(sql, (as_of_date, max_tickers)).fetchall()
    else:
        rows = conn.execute(sql, (as_of_date,)).fetchall()
    return [normalize_ticker(str(row[0])) for row in rows]


def _synthetic_statement_vintage_id(market: str, ticker: str, period_end_date: str) -> str:
    safe_period = period_end_date.replace("-", "")
    return f"LEGACY_{market.upper()}_{ticker}_{safe_period}_V1"


def _metadata_gap_note(field_name: str) -> str:
    notes = {
        "available_at_utc": "Not present in rc_fundamental_quarterly; cannot be reconstructed accurately.",
        "filed_at_utc": "Not present in rc_fundamental_quarterly.",
        "statement_vintage_id": "Can be proposed synthetically, but no original vintage id exists.",
        "source_provider": "Not present in rc_fundamental_quarterly.",
        "source_hash": "Can be proposed from row values later; not computed by this preflight.",
        "ingested_at_utc": "Not present in rc_fundamental_quarterly.",
        "provider_observed_at_utc": "Not present in rc_fundamental_quarterly.",
    }
    return notes.get(field_name, "")


def _text_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


if __name__ == "__main__":
    main()
