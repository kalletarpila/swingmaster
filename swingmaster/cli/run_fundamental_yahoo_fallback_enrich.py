from __future__ import annotations

import argparse
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_MARKET = "usa"
ALLOWED_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)
QUARTERLY_INSERT_FIELDS = (
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
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich missing SEC quarterly fields from Yahoo quarterly fallback")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", default=DEFAULT_MARKET, help="Market code")
    parser.add_argument("--ticker", default=None, help="Optional single ticker override")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Validate only without writing updates or audit rows")
    parser.add_argument(
        "--replace-audit-for-run",
        action="store_true",
        help="Delete existing enrichment audit rows for the selected run_id before writing new audit rows",
    )
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def resolve_created_at_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_tickers(conn: sqlite3.Connection, market: str, ticker: str | None) -> list[str]:
    if ticker is not None:
        return [ticker.upper()]
    if market == "usa":
        rows = conn.execute(
            """
            SELECT DISTINCT ticker
            FROM rc_fundamental_quarterly
            WHERE ticker NOT LIKE '%.HE'
            ORDER BY ticker
            """
        ).fetchall()
        return [str(row[0]).upper() for row in rows]
    rows = conn.execute(
        """
        SELECT DISTINCT ticker
        FROM rc_fundamental_quarterly
        WHERE ticker LIKE '%.HE'
        ORDER BY ticker
        """
    ).fetchall()
    return [str(row[0]).upper() for row in rows]


def load_quarterly_rows(conn: sqlite3.Connection, ticker: str) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT ticker, period_end_date, {", ".join(ALLOWED_FIELDS)}
            FROM rc_fundamental_quarterly
            WHERE ticker = ?
            ORDER BY period_end_date ASC
            """,
            (ticker.upper(),),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return rows


def load_yahoo_rows(conn: sqlite3.Connection, market: str, ticker: str) -> dict[str, sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT symbol, period_end_date, {", ".join(ALLOWED_FIELDS)}
            FROM rc_fundamental_yahoo_quarterly
            WHERE market = ?
              AND symbol = ?
            ORDER BY period_end_date ASC
            """,
            (market, ticker.upper()),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return {str(row["period_end_date"]): row for row in rows}


def load_yahoo_quarterly_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(rc_fundamental_yahoo_quarterly)").fetchall()
    return {str(row[1]) for row in rows}


def _calendar_quarter(value: date) -> int:
    return ((value.month - 1) // 3) + 1


def quarterly_satisfies_detected(conn: sqlite3.Connection, ticker: str, detected_source_period_end_date: str) -> bool:
    detected_date = date.fromisoformat(detected_source_period_end_date)
    detected_quarter = _calendar_quarter(detected_date)
    rows = conn.execute(
        """
        SELECT period_end_date
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
        ORDER BY period_end_date ASC
        """,
        (ticker.upper(),),
    ).fetchall()
    for row in rows:
        period_end_date = row[0]
        if period_end_date is None:
            continue
        quarter_date = date.fromisoformat(str(period_end_date))
        if quarter_date.year != detected_date.year:
            continue
        if _calendar_quarter(quarter_date) != detected_quarter:
            continue
        if abs((quarter_date - detected_date).days) <= 7:
            return True
    return False


def resolve_yahoo_match(
    yahoo_rows_by_period: dict[str, sqlite3.Row],
    period_end_date: str,
) -> tuple[sqlite3.Row | None, str | None]:
    exact_match = yahoo_rows_by_period.get(period_end_date)
    if exact_match is not None:
        return exact_match, "EXACT"

    sec_date = date.fromisoformat(period_end_date)
    sec_quarter = _calendar_quarter(sec_date)
    candidates: list[tuple[int, date, sqlite3.Row]] = []
    for yahoo_period_end_date, yahoo_row in yahoo_rows_by_period.items():
        yahoo_date = date.fromisoformat(yahoo_period_end_date)
        if yahoo_date.year != sec_date.year:
            continue
        if _calendar_quarter(yahoo_date) != sec_quarter:
            continue
        abs_diff_days = abs((yahoo_date - sec_date).days)
        if abs_diff_days > 7:
            continue
        candidates.append((abs_diff_days, yahoo_date, yahoo_row))
    if not candidates:
        return None, None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2], "SAME_QUARTER_DATE_TOLERANCE"


def build_field_updates(
    quarterly_row: sqlite3.Row,
    yahoo_row: sqlite3.Row,
    match_method: str,
    run_id: str,
    created_at_utc: str,
) -> tuple[dict[str, float], list[dict[str, Any]]]:
    updates: dict[str, float] = {}
    audit_rows: list[dict[str, Any]] = []
    for field_name in ALLOWED_FIELDS:
        if quarterly_row[field_name] is not None:
            continue
        if yahoo_row[field_name] is None:
            continue
        new_value = float(yahoo_row[field_name])
        updates[field_name] = new_value
        audit_rows.append(
            {
                "ticker": str(quarterly_row["ticker"]).upper(),
                "period_end_date": str(quarterly_row["period_end_date"]),
                "field_name": field_name,
                "old_value": None,
                "new_value": new_value,
                "primary_source": "sec_edgar",
                "fallback_source": "yahoo",
                "enrichment_status": "FILLED_FROM_YAHOO",
                "matched_yahoo_period_end_date": str(yahoo_row["period_end_date"]),
                "match_method": match_method,
                "run_id": run_id,
                "created_at_utc": created_at_utc,
            }
        )
    return updates, audit_rows


def insert_missing_quarterly_row_from_yahoo(
    conn: sqlite3.Connection,
    market: str,
    ticker: str,
    detected_source_period_end_date: str | None,
    run_id: str,
) -> int:
    if detected_source_period_end_date is None:
        return 0
    if quarterly_satisfies_detected(conn, ticker, detected_source_period_end_date):
        return 0

    yahoo_rows_by_period = load_yahoo_rows(conn, market, ticker)
    matched_yahoo_row, _match_method = resolve_yahoo_match(yahoo_rows_by_period, detected_source_period_end_date)
    if matched_yahoo_row is None:
        return 0

    matched_period_end_date = str(matched_yahoo_row["period_end_date"])
    existing_row = conn.execute(
        """
        SELECT 1
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
          AND period_end_date = ?
        """,
        (ticker.upper(), matched_period_end_date),
    ).fetchone()
    if existing_row is not None:
        return 0

    yahoo_columns = load_yahoo_quarterly_columns(conn)
    insert_columns = ["ticker", "period_end_date", *QUARTERLY_INSERT_FIELDS, "run_id"]
    placeholders = ", ".join("?" for _ in insert_columns)
    values: list[object] = [ticker.upper(), matched_period_end_date]
    for field_name in QUARTERLY_INSERT_FIELDS:
        if field_name in yahoo_columns and field_name in matched_yahoo_row.keys():
            values.append(matched_yahoo_row[field_name])
        else:
            values.append(None)
    values.append(run_id)
    conn.execute(
        f"""
        INSERT INTO rc_fundamental_quarterly ({", ".join(insert_columns)})
        VALUES ({placeholders})
        """,
        values,
    )
    return 1


def replace_audit_rows_for_run(conn: sqlite3.Connection, run_id: str) -> int:
    cursor = conn.execute(
        """
        DELETE FROM rc_fundamental_quarterly_enrichment_audit
        WHERE run_id = ?
        """,
        (run_id,),
    )
    return int(cursor.rowcount)


def update_quarterly_row(
    conn: sqlite3.Connection,
    ticker: str,
    period_end_date: str,
    updates: dict[str, float],
) -> int:
    if not updates:
        return 0
    assignments = ", ".join(f"{field_name} = ?" for field_name in updates)
    values: list[object] = [updates[field_name] for field_name in updates]
    values.extend([ticker.upper(), period_end_date])
    cursor = conn.execute(
        f"""
        UPDATE rc_fundamental_quarterly
        SET {assignments}
        WHERE ticker = ?
          AND period_end_date = ?
        """,
        values,
    )
    return int(cursor.rowcount)


def insert_audit_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT INTO rc_fundamental_quarterly_enrichment_audit (
            ticker,
            period_end_date,
            field_name,
            old_value,
            new_value,
            primary_source,
            fallback_source,
            enrichment_status,
            matched_yahoo_period_end_date,
            match_method,
            run_id,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["period_end_date"],
                row["field_name"],
                row["old_value"],
                row["new_value"],
                row["primary_source"],
                row["fallback_source"],
                row["enrichment_status"],
                row["matched_yahoo_period_end_date"],
                row["match_method"],
                row["run_id"],
                row["created_at_utc"],
            )
            for row in rows
        ],
    )
    return len(rows)


def run_yahoo_fallback_enrich(
    db_path: Path,
    market: str,
    ticker: str | None,
    run_id: str,
    dry_run: bool,
    replace_audit_for_run: bool,
    detected_source_period_end_date: str | None = None,
) -> dict[str, Any]:
    created_at_utc = resolve_created_at_utc()
    filled_per_field = {field_name: 0 for field_name in ALLOWED_FIELDS}
    tickers_processed = 0
    quarterly_rows_scanned = 0
    yahoo_rows_matched = 0
    fields_checked = 0
    fields_filled = 0
    rows_updated = 0
    no_match_count = 0
    exact_matches = 0
    quarter_aligned_matches = 0
    rows_inserted = 0
    pending_updates: list[tuple[str, str, dict[str, float]]] = []
    pending_audit_rows: list[dict[str, Any]] = []

    with sqlite3.connect(str(db_path)) as conn:
        tickers = load_tickers(conn, market, ticker)
        tickers_processed = len(tickers)
        for current_ticker in tickers:
            if detected_source_period_end_date is not None and not dry_run:
                rows_inserted += insert_missing_quarterly_row_from_yahoo(
                    conn=conn,
                    market=market,
                    ticker=current_ticker,
                    detected_source_period_end_date=detected_source_period_end_date,
                    run_id=run_id,
                )
            quarterly_rows = load_quarterly_rows(conn, current_ticker)
            yahoo_rows_by_period = load_yahoo_rows(conn, market, current_ticker)
            for quarterly_row in quarterly_rows:
                quarterly_rows_scanned += 1
                fields_checked += len(ALLOWED_FIELDS)
                period_end_date = str(quarterly_row["period_end_date"])
                yahoo_row, match_method = resolve_yahoo_match(yahoo_rows_by_period, period_end_date)
                if yahoo_row is None:
                    no_match_count += 1
                    continue
                yahoo_rows_matched += 1
                if match_method == "EXACT":
                    exact_matches += 1
                elif match_method == "SAME_QUARTER_DATE_TOLERANCE":
                    quarter_aligned_matches += 1
                updates, audit_rows = build_field_updates(quarterly_row, yahoo_row, str(match_method), run_id, created_at_utc)
                if not updates:
                    continue
                rows_updated += 1
                fields_filled += len(audit_rows)
                for audit_row in audit_rows:
                    filled_per_field[str(audit_row["field_name"])] += 1
                pending_updates.append((current_ticker, period_end_date, updates))
                pending_audit_rows.extend(audit_rows)

        if not dry_run:
            if replace_audit_for_run:
                replace_audit_rows_for_run(conn, run_id)
            for current_ticker, period_end_date, updates in pending_updates:
                update_quarterly_row(conn, current_ticker, period_end_date, updates)
            insert_audit_rows(conn, pending_audit_rows)
            conn.commit()

    summary: dict[str, Any] = {
        "market": market,
        "tickers_processed": tickers_processed,
        "quarterly_rows_scanned": quarterly_rows_scanned,
        "yahoo_rows_matched": yahoo_rows_matched,
        "fields_checked": fields_checked,
        "fields_filled": fields_filled,
        "rows_updated": rows_updated,
        "rows_inserted": rows_inserted,
        "no_match_count": no_match_count,
        "exact_matches": exact_matches,
        "quarter_aligned_matches": quarter_aligned_matches,
        "dry_run": "true" if dry_run else "false",
        "run_id": run_id,
    }
    for field_name in ALLOWED_FIELDS:
        summary[f"filled_{field_name}"] = filled_per_field[field_name]
    return summary


def main() -> None:
    args = parse_args()
    summary = run_yahoo_fallback_enrich(
        db_path=resolve_db_path(args.db),
        market=args.market,
        ticker=args.ticker,
        run_id=args.run_id,
        dry_run=args.dry_run,
        replace_audit_for_run=args.replace_audit_for_run,
    )
    _summary(market=summary["market"])
    _summary(tickers_processed=summary["tickers_processed"])
    _summary(quarterly_rows_scanned=summary["quarterly_rows_scanned"])
    _summary(yahoo_rows_matched=summary["yahoo_rows_matched"])
    _summary(fields_checked=summary["fields_checked"])
    _summary(fields_filled=summary["fields_filled"])
    _summary(rows_updated=summary["rows_updated"])
    _summary(rows_inserted=summary["rows_inserted"])
    _summary(no_match_count=summary["no_match_count"])
    _summary(exact_matches=summary["exact_matches"])
    _summary(quarter_aligned_matches=summary["quarter_aligned_matches"])
    _summary(dry_run=summary["dry_run"])
    _summary(run_id=summary["run_id"])
    for field_name in ALLOWED_FIELDS:
        _summary(**{f"filled_{field_name}": summary[f"filled_{field_name}"]})


if __name__ == "__main__":
    main()
