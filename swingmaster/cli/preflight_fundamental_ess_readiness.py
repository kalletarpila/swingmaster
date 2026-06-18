from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote


STATUS_OK = "OK"
STATUS_PARTIAL = "PARTIAL"
STATUS_MISSING = "MISSING"
STATUS_STALE = "STALE"
STATUS_UNKNOWN = "UNKNOWN"
STATUS_NOT_APPLICABLE = "NOT_APPLICABLE"
READINESS_STATUSES = (
    STATUS_OK,
    STATUS_PARTIAL,
    STATUS_MISSING,
    STATUS_STALE,
    STATUS_UNKNOWN,
    STATUS_NOT_APPLICABLE,
)
CORE_TABLES = (
    "rc_fundamental_quarterly",
    "rc_fundamental_ttm",
    "rc_fundamental_valuation",
    "rc_fundamental_score_percentile",
    "rc_fundamental_quarter_state",
)
OPTIONAL_TABLES = (
    "rc_fundamental_reporting_frequency_classification",
    "rc_fundamental_missing_period_recovery_check",
)
EVENT_TABLE_CANDIDATES = (
    "rc_fundamental_event",
    "rc_fundamental_events",
    "rc_fundamental_earnings_calendar",
    "rc_fundamental_events_and_expectations",
)
SCHEMA_GAP_CHECKS = (
    ("market in rc_fundamental_quarterly", "rc_fundamental_quarterly", "market"),
    ("market in rc_fundamental_ttm", "rc_fundamental_ttm", "market"),
    ("available_at_utc", "rc_fundamental_quarterly", "available_at_utc"),
    ("filed_at_utc", "rc_fundamental_quarterly", "filed_at_utc"),
    ("statement_vintage_id", "rc_fundamental_quarterly", "statement_vintage_id"),
    ("source_hash", "rc_fundamental_statement_raw", "source_hash"),
    ("input_vintage_hash", "rc_fundamental_ttm", "input_vintage_hash"),
    ("config_hash", "rc_fundamental_ttm", "config_hash"),
    ("universe_version", "rc_fundamental_score_percentile", "universe_version"),
    ("universe_hash", "rc_fundamental_score_percentile", "universe_hash"),
    ("price_match_status", "rc_fundamental_valuation", "price_match_status"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only ESS readiness preflight for fundamentals tables")
    parser.add_argument("--fundamentals-db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", required=True, help="Market code, e.g. usa or omxh")
    parser.add_argument("--as-of-date", required=True, help="Readiness as-of date in YYYY-MM-DD format")
    parser.add_argument("--osakedata-db", default=None, help="Optional osakedata SQLite database path")
    parser.add_argument("--tickers", default=None, help="Comma-separated ticker list")
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format")
    parser.add_argument("--fail-if-not-ok", action="store_true", help="Exit nonzero if overall status is not OK")
    parser.add_argument("--max-tickers", type=int, default=None, help="Maximum tickers to inspect after deterministic ordering")
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
    tickers = []
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


def connect_read_only(db_path: Path) -> sqlite3.Connection:
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


def load_tickers_to_check(
    conn: sqlite3.Connection,
    existing_tables: set[str],
    explicit_tickers: list[str] | None,
    max_tickers: int | None,
) -> list[str]:
    if max_tickers is not None and max_tickers < 1:
        raise ValueError("--max-tickers must be >= 1")
    if explicit_tickers is not None:
        tickers = explicit_tickers
    else:
        ticker_set: set[str] = set()
        for table_name in (
            "rc_fundamental_quarterly",
            "rc_fundamental_ttm",
            "rc_fundamental_valuation",
            "rc_fundamental_score_percentile",
            "rc_fundamental_quarter_state",
        ):
            if table_name not in existing_tables:
                continue
            rows = conn.execute(f"SELECT DISTINCT ticker FROM {table_name} WHERE ticker IS NOT NULL").fetchall()
            ticker_set.update(normalize_ticker(str(row[0])) for row in rows)
        tickers = sorted(ticker_set)
    if max_tickers is not None:
        return tickers[:max_tickers]
    return tickers


def inspect_ticker_readiness(
    conn: sqlite3.Connection,
    existing_tables: set[str],
    market: str,
    ticker: str,
    as_of_date: str,
) -> dict[str, Any]:
    ticker = normalize_ticker(ticker)
    warnings: list[str] = []
    identity_status = STATUS_OK
    if ticker.endswith(".HE") and market != "omxh":
        identity_status = STATUS_UNKNOWN
        warnings.append("HE_SUFFIX_MARKET_MISMATCH")

    reported = _inspect_reported(conn, existing_tables, ticker, as_of_date)
    derived = _inspect_latest_date(conn, existing_tables, "rc_fundamental_ttm", "as_of_date", ticker, as_of_date)
    valuation = _inspect_latest_date(conn, existing_tables, "rc_fundamental_valuation", "as_of_date", ticker, as_of_date)
    rank = _inspect_latest_date(
        conn,
        existing_tables,
        "rc_fundamental_score_percentile",
        "target_date",
        ticker,
        as_of_date,
    )
    quarter_state_status, quarter_state_warning = _inspect_quarter_state(conn, existing_tables, market, ticker)
    if quarter_state_warning is not None:
        warnings.append(quarter_state_warning)

    missing_period_status = _inspect_optional_latest(
        conn,
        existing_tables,
        "rc_fundamental_missing_period_recovery_check",
        "classification_as_of_date",
        market,
        ticker,
        as_of_date,
    )
    reporting_frequency_status = _inspect_optional_latest(
        conn,
        existing_tables,
        "rc_fundamental_reporting_frequency_classification",
        "as_of_date",
        market,
        ticker,
        as_of_date,
    )
    event_status = STATUS_NOT_APPLICABLE
    if any(table_name in existing_tables for table_name in EVENT_TABLE_CANDIDATES):
        event_status = STATUS_MISSING

    overall = _resolve_overall_status(
        reported["status"],
        derived["status"],
        valuation["status"],
        rank["status"],
        identity_status,
    )
    reasons = _build_reasons(
        reported["status"],
        derived["status"],
        valuation["status"],
        rank["status"],
        identity_status,
        warnings,
    )
    return {
        "ticker": ticker,
        "market": market,
        "identity_status": identity_status,
        "reported_status": reported["status"],
        "reported_latest_period_end": reported["latest_period_end"],
        "reported_quarter_count": reported["quarter_count"],
        "derived_status": derived["status"],
        "latest_ttm_as_of_date": derived["latest_date"],
        "valuation_status": valuation["status"],
        "valuation_as_of_date": valuation["latest_date"],
        "rank_status": rank["status"],
        "rank_target_date": rank["latest_date"],
        "quarter_state_status": quarter_state_status,
        "missing_period_status": missing_period_status,
        "reporting_frequency_status": reporting_frequency_status,
        "event_status": event_status,
        "overall_ess_readiness": overall,
        "reasons": reasons,
        "warnings": warnings,
    }


def _inspect_reported(
    conn: sqlite3.Connection,
    existing_tables: set[str],
    ticker: str,
    as_of_date: str,
) -> dict[str, Any]:
    if "rc_fundamental_quarterly" not in existing_tables:
        return {"status": STATUS_UNKNOWN, "latest_period_end": None, "quarter_count": 0}
    row = conn.execute(
        """
        SELECT COUNT(*), MAX(period_end_date)
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
          AND period_end_date <= ?
        """,
        (ticker, as_of_date),
    ).fetchone()
    quarter_count = int(row[0]) if row is not None and row[0] is not None else 0
    latest_period_end = str(row[1]) if row is not None and row[1] is not None else None
    return {
        "status": STATUS_OK if quarter_count > 0 else STATUS_MISSING,
        "latest_period_end": latest_period_end,
        "quarter_count": quarter_count,
    }


def _inspect_latest_date(
    conn: sqlite3.Connection,
    existing_tables: set[str],
    table_name: str,
    date_column: str,
    ticker: str,
    as_of_date: str,
) -> dict[str, Any]:
    if table_name not in existing_tables:
        return {"status": STATUS_UNKNOWN, "latest_date": None}
    row = conn.execute(
        f"""
        SELECT MAX({date_column})
        FROM {table_name}
        WHERE ticker = ?
          AND {date_column} <= ?
        """,
        (ticker, as_of_date),
    ).fetchone()
    latest_date = str(row[0]) if row is not None and row[0] is not None else None
    return {"status": STATUS_OK if latest_date is not None else STATUS_MISSING, "latest_date": latest_date}


def _inspect_quarter_state(
    conn: sqlite3.Connection,
    existing_tables: set[str],
    market: str,
    ticker: str,
) -> tuple[str, str | None]:
    if "rc_fundamental_quarter_state" not in existing_tables:
        return STATUS_UNKNOWN, None
    row = conn.execute(
        """
        SELECT market
        FROM rc_fundamental_quarter_state
        WHERE ticker = ?
        """,
        (ticker,),
    ).fetchone()
    if row is None:
        return STATUS_MISSING, None
    row_market = str(row[0]).lower() if row[0] is not None else ""
    if row_market != market:
        return STATUS_PARTIAL, f"QUARTER_STATE_MARKET_MISMATCH:{row_market or 'NULL'}"
    return STATUS_OK, None


def _inspect_optional_latest(
    conn: sqlite3.Connection,
    existing_tables: set[str],
    table_name: str,
    date_column: str,
    market: str,
    ticker: str,
    as_of_date: str,
) -> str:
    if table_name not in existing_tables:
        return STATUS_NOT_APPLICABLE
    row = conn.execute(
        f"""
        SELECT 1
        FROM {table_name}
        WHERE ticker = ?
          AND market = ?
          AND {date_column} <= ?
        LIMIT 1
        """,
        (ticker, market, as_of_date),
    ).fetchone()
    return STATUS_OK if row is not None else STATUS_MISSING


def _resolve_overall_status(
    reported_status: str,
    derived_status: str,
    valuation_status: str,
    rank_status: str,
    identity_status: str,
) -> str:
    if STATUS_UNKNOWN in {reported_status, derived_status, valuation_status, rank_status, identity_status}:
        return STATUS_UNKNOWN
    if reported_status == STATUS_MISSING:
        return STATUS_MISSING
    if (
        reported_status == STATUS_OK
        and derived_status == STATUS_OK
        and valuation_status == STATUS_OK
        and rank_status == STATUS_OK
        and identity_status == STATUS_OK
    ):
        return STATUS_OK
    return STATUS_PARTIAL


def _build_reasons(
    reported_status: str,
    derived_status: str,
    valuation_status: str,
    rank_status: str,
    identity_status: str,
    warnings: list[str],
) -> list[str]:
    reasons = []
    if identity_status != STATUS_OK:
        reasons.append(f"identity={identity_status}")
    if reported_status != STATUS_OK:
        reasons.append(f"reported={reported_status}")
    if derived_status != STATUS_OK:
        reasons.append(f"derived={derived_status}")
    if valuation_status != STATUS_OK:
        reasons.append(f"valuation={valuation_status}")
    if rank_status != STATUS_OK:
        reasons.append(f"rank={rank_status}")
    reasons.extend(warnings)
    return reasons


def inspect_schema_gaps(conn: sqlite3.Connection, existing_tables: set[str]) -> list[dict[str, Any]]:
    gaps = []
    for concept, table_name, column_name in SCHEMA_GAP_CHECKS:
        columns = table_columns(conn, table_name, existing_tables)
        gaps.append(
            {
                "concept": concept,
                "table": table_name,
                "column": column_name,
                "supported": table_name in existing_tables and column_name in columns,
                "status": STATUS_OK if table_name in existing_tables and column_name in columns else STATUS_MISSING,
            }
        )
    event_supported = any(table_name in existing_tables for table_name in EVENT_TABLE_CANDIDATES)
    gaps.append(
        {
            "concept": "event table",
            "table": "|".join(EVENT_TABLE_CANDIDATES),
            "column": None,
            "supported": event_supported,
            "status": STATUS_OK if event_supported else STATUS_MISSING,
        }
    )
    return gaps


def build_summary(market: str, as_of_date: str, ticker_rows: list[dict[str, Any]], missing_tables: list[str]) -> dict[str, Any]:
    counts = {status: 0 for status in READINESS_STATUSES}
    warning_count = 0
    for row in ticker_rows:
        counts[str(row["overall_ess_readiness"])] += 1
        warning_count += len(row["warnings"])
    overall_status = STATUS_OK
    if not ticker_rows:
        overall_status = STATUS_UNKNOWN
    elif counts[STATUS_UNKNOWN] > 0:
        overall_status = STATUS_UNKNOWN
    elif counts[STATUS_MISSING] > 0:
        overall_status = STATUS_MISSING
    elif counts[STATUS_PARTIAL] > 0 or counts[STATUS_STALE] > 0:
        overall_status = STATUS_PARTIAL
    return {
        "market": market,
        "as_of_date": as_of_date,
        "ticker_count_checked": len(ticker_rows),
        "overall_status": overall_status,
        "counts_by_readiness": counts,
        "missing_tables": missing_tables,
        "warning_count": warning_count,
    }


def run_preflight(
    fundamentals_db_path: Path,
    market: str,
    as_of_date: str,
    tickers: list[str] | None = None,
    max_tickers: int | None = None,
    osakedata_db_path: Path | None = None,
) -> dict[str, Any]:
    market = normalize_market(market)
    with connect_read_only(fundamentals_db_path) as conn:
        existing_tables = list_existing_tables(conn)
        missing_tables = sorted(table_name for table_name in CORE_TABLES if table_name not in existing_tables)
        tickers_to_check = load_tickers_to_check(conn, existing_tables, tickers, max_tickers)
        ticker_rows = [
            inspect_ticker_readiness(conn, existing_tables, market, ticker, as_of_date)
            for ticker in tickers_to_check
        ]
        schema_gaps = inspect_schema_gaps(conn, existing_tables)

    osakedata_status = STATUS_NOT_APPLICABLE
    osakedata_error = None
    if osakedata_db_path is not None:
        try:
            with connect_read_only(osakedata_db_path) as osakedata_conn:
                osakedata_tables = list_existing_tables(osakedata_conn)
                osakedata_status = STATUS_OK if osakedata_tables else STATUS_UNKNOWN
        except Exception as exc:
            osakedata_status = STATUS_UNKNOWN
            osakedata_error = str(exc)

    summary = build_summary(market, as_of_date, ticker_rows, missing_tables)
    return {
        "summary": summary,
        "tickers": ticker_rows,
        "schema_gaps": schema_gaps,
        "osakedata": {
            "status": osakedata_status,
            "path": str(osakedata_db_path) if osakedata_db_path is not None else None,
            "error": osakedata_error,
        },
    }


def render_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "summary",
        f"market: {summary['market']}",
        f"as_of_date: {summary['as_of_date']}",
        f"ticker_count_checked: {summary['ticker_count_checked']}",
        f"overall_status: {summary['overall_status']}",
        f"warning_count: {summary['warning_count']}",
        f"missing_tables: {', '.join(summary['missing_tables']) if summary['missing_tables'] else ''}",
        "counts_by_readiness:",
    ]
    for status in READINESS_STATUSES:
        lines.append(f"  {status}: {summary['counts_by_readiness'][status]}")

    lines.extend(
        [
            "",
            "tickers",
            "ticker;market;identity_status;reported_status;reported_latest_period_end;reported_quarter_count;"
            "derived_status;latest_ttm_as_of_date;valuation_status;valuation_as_of_date;rank_status;"
            "rank_target_date;quarter_state_status;missing_period_status;reporting_frequency_status;"
            "event_status;overall_ess_readiness;reasons;warnings",
        ]
    )
    for row in report["tickers"]:
        lines.append(
            ";".join(
                [
                    _text_value(row["ticker"]),
                    _text_value(row["market"]),
                    _text_value(row["identity_status"]),
                    _text_value(row["reported_status"]),
                    _text_value(row["reported_latest_period_end"]),
                    _text_value(row["reported_quarter_count"]),
                    _text_value(row["derived_status"]),
                    _text_value(row["latest_ttm_as_of_date"]),
                    _text_value(row["valuation_status"]),
                    _text_value(row["valuation_as_of_date"]),
                    _text_value(row["rank_status"]),
                    _text_value(row["rank_target_date"]),
                    _text_value(row["quarter_state_status"]),
                    _text_value(row["missing_period_status"]),
                    _text_value(row["reporting_frequency_status"]),
                    _text_value(row["event_status"]),
                    _text_value(row["overall_ess_readiness"]),
                    "|".join(row["reasons"]),
                    "|".join(row["warnings"]),
                ]
            )
        )

    lines.extend(["", "schema_gaps", "concept;table;column;supported;status"])
    for gap in report["schema_gaps"]:
        lines.append(
            ";".join(
                [
                    _text_value(gap["concept"]),
                    _text_value(gap["table"]),
                    _text_value(gap["column"]),
                    "true" if gap["supported"] else "false",
                    _text_value(gap["status"]),
                ]
            )
        )
    return "\n".join(lines)


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def _text_value(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def main() -> None:
    args = parse_args()
    try:
        report = run_preflight(
            fundamentals_db_path=Path(args.fundamentals_db),
            market=args.market,
            as_of_date=args.as_of_date,
            tickers=parse_tickers_arg(args.tickers),
            max_tickers=args.max_tickers,
            osakedata_db_path=Path(args.osakedata_db) if args.osakedata_db else None,
        )
    except Exception as exc:
        print(f"ERROR {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    if args.format == "json":
        print(render_json(report))
    else:
        print(render_text(report))

    if args.fail_if_not_ok and report["summary"]["overall_status"] != STATUS_OK:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
