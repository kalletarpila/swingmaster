from __future__ import annotations

import argparse
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.reported_quarterly_dual_write import (
    REPORTED_FINANCIAL_FIELDS,
    write_normalized_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_yahoo_dual_write_adapter import (
    write_yahoo_fallback_enriched_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_yahoo_vintage_metadata import (
    build_yahoo_source_hash,
    build_yahoo_vintage_metadata,
)


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
    parser.add_argument("--write-vintage", action="store_true", help="Opt in to latest/vintage/provenance writes")
    parser.add_argument("--vintage-market", help="Vintage market; required with --write-vintage")
    parser.add_argument("--vintage-available-at-utc", help="PIT availability timestamp; required with --write-vintage")
    parser.add_argument("--vintage-ingested-at-utc", help="Ingestion timestamp; required with --write-vintage")
    parser.add_argument("--vintage-run-id", help="Vintage write run id; required with --write-vintage")
    parser.add_argument(
        "--vintage-normalization-run-id",
        help="Optional normalization run id for vintage metadata",
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
            SELECT
                market,
                symbol,
                period_end_date,
                {", ".join(ALLOWED_FIELDS)},
                source_run_id,
                run_id,
                created_at_utc
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


def load_quarterly_row_for_vintage(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> dict[str, Any]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT
                ticker,
                period_end_date,
                revenue,
                gross_profit,
                operating_income,
                ebit,
                ebitda,
                net_income,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                shares_outstanding,
                currency,
                run_id
            FROM rc_fundamental_quarterly
            WHERE ticker = ?
              AND period_end_date = ?
            """,
            (ticker.upper(), period_end_date),
        ).fetchone()
    finally:
        conn.row_factory = previous_row_factory
    if row is None:
        raise RuntimeError(f"FUNDAMENTAL_QUARTERLY_ROW_NOT_FOUND:{ticker.upper()},{period_end_date}")
    return dict(row)


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
    return 1 if insert_missing_quarterly_row_from_yahoo_with_metadata(
        conn=conn,
        market=market,
        ticker=ticker,
        detected_source_period_end_date=detected_source_period_end_date,
        run_id=run_id,
    ) else 0


def insert_missing_quarterly_row_from_yahoo_with_metadata(
    conn: sqlite3.Connection,
    market: str,
    ticker: str,
    detected_source_period_end_date: str | None,
    run_id: str,
) -> dict[str, Any] | None:
    if detected_source_period_end_date is None:
        return None
    if quarterly_satisfies_detected(conn, ticker, detected_source_period_end_date):
        return None

    yahoo_rows_by_period = load_yahoo_rows(conn, market, ticker)
    matched_yahoo_row, match_method = resolve_yahoo_match(yahoo_rows_by_period, detected_source_period_end_date)
    if matched_yahoo_row is None:
        return None

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
        return None

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
    return {
        "ticker": ticker.upper(),
        "period_end_date": matched_period_end_date,
        "detected_source_period_end_date": detected_source_period_end_date,
        "match_method": match_method,
        "yahoo_row": dict(matched_yahoo_row),
    }


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
    *,
    write_vintage: bool = False,
    vintage_market: str | None = None,
    vintage_available_at_utc: str | None = None,
    vintage_ingested_at_utc: str | None = None,
    vintage_run_id: str | None = None,
    vintage_normalization_run_id: str | None = None,
) -> dict[str, Any]:
    if write_vintage:
        _validate_vintage_args(
            vintage_market=vintage_market,
            vintage_available_at_utc=vintage_available_at_utc,
            vintage_ingested_at_utc=vintage_ingested_at_utc,
            vintage_run_id=vintage_run_id,
        )

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
    pending_missing_inserts: list[dict[str, Any]] = []

    with sqlite3.connect(str(db_path)) as conn:
        tickers = load_tickers(conn, market, ticker)
        tickers_processed = len(tickers)
        for current_ticker in tickers:
            if detected_source_period_end_date is not None and not dry_run:
                missing_insert = insert_missing_quarterly_row_from_yahoo_with_metadata(
                    conn=conn,
                    market=market,
                    ticker=current_ticker,
                    detected_source_period_end_date=detected_source_period_end_date,
                    run_id=run_id,
                )
                if missing_insert is not None:
                    rows_inserted += 1
                    pending_missing_inserts.append(missing_insert)
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
            if write_vintage:
                _write_fallback_vintages(
                    conn=conn,
                    market=str(vintage_market),
                    available_at_utc=str(vintage_available_at_utc),
                    ingested_at_utc=str(vintage_ingested_at_utc),
                    run_id=str(vintage_run_id),
                    normalization_run_id=vintage_normalization_run_id,
                    audit_rows=pending_audit_rows,
                )
                _write_missing_quarter_vintages(
                    conn=conn,
                    market=str(vintage_market),
                    available_at_utc=str(vintage_available_at_utc),
                    ingested_at_utc=str(vintage_ingested_at_utc),
                    run_id=str(vintage_run_id),
                    normalization_run_id=vintage_normalization_run_id,
                    missing_inserts=pending_missing_inserts,
                )
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


def _validate_vintage_args(
    *,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
) -> None:
    required_values = {
        "vintage_market": vintage_market,
        "vintage_available_at_utc": vintage_available_at_utc,
        "vintage_ingested_at_utc": vintage_ingested_at_utc,
        "vintage_run_id": vintage_run_id,
    }
    missing = [name for name, value in required_values.items() if value is None or not str(value).strip()]
    if missing:
        raise ValueError("YAHOO_FALLBACK_ENRICH_CLI_VINTAGE_REQUIRED_FIELDS_MISSING:" + ",".join(missing))


def _write_fallback_vintages(
    *,
    conn: sqlite3.Connection,
    market: str,
    available_at_utc: str,
    ingested_at_utc: str,
    run_id: str,
    normalization_run_id: str | None,
    audit_rows: list[dict[str, Any]],
) -> None:
    audit_rows_by_key = _audit_rows_by_key(audit_rows)
    if not audit_rows_by_key:
        return

    normalized_rows = [
        _with_optional_normalization_run_id(
            load_quarterly_row_for_vintage(conn, ticker, period_end_date),
            normalization_run_id,
        )
        for ticker, period_end_date in sorted(audit_rows_by_key)
    ]
    yahoo_rows_by_key = _matched_yahoo_rows_by_key(conn, market, audit_rows_by_key)
    write_yahoo_fallback_enriched_rows_with_optional_vintage(
        conn,
        normalized_rows=normalized_rows,
        enrichment_audit_rows_by_key=audit_rows_by_key,
        yahoo_quarterly_rows_by_key=yahoo_rows_by_key,
        write_vintage=True,
        market=market,
        available_at_utc=available_at_utc,
        ingested_at_utc=ingested_at_utc,
        run_id=run_id,
        mode="yahoo_fallback_enrichment",
        normalization_run_id=normalization_run_id,
    )


def _write_missing_quarter_vintages(
    *,
    conn: sqlite3.Connection,
    market: str,
    available_at_utc: str,
    ingested_at_utc: str,
    run_id: str,
    normalization_run_id: str | None,
    missing_inserts: list[dict[str, Any]],
) -> None:
    if not missing_inserts:
        return

    normalized_rows: list[dict[str, Any]] = []
    metadata_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    field_source_map_by_key: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
    for inserted in missing_inserts:
        ticker = str(inserted["ticker"]).upper()
        period_end_date = str(inserted["period_end_date"])
        key = (ticker, period_end_date)
        normalized_row = _with_optional_normalization_run_id(
            load_quarterly_row_for_vintage(conn, ticker, period_end_date),
            normalization_run_id,
        )
        yahoo_row = dict(inserted["yahoo_row"])
        yahoo_row["detected_source_period_end_date"] = inserted.get("detected_source_period_end_date")
        yahoo_row["match_method"] = inserted.get("match_method")
        source_hash = build_yahoo_source_hash(
            market=market,
            ticker=ticker,
            period_end_date=period_end_date,
            yahoo_quarterly_row=yahoo_row,
            normalized_row=normalized_row,
        )
        metadata_by_key[key] = build_yahoo_vintage_metadata(
            market=market,
            ticker=ticker,
            period_end_date=period_end_date,
            normalized_row=normalized_row,
            available_at_utc=available_at_utc,
            ingested_at_utc=ingested_at_utc,
            run_id=run_id,
            source_hash=source_hash,
            mode="yahoo_missing_quarter_insert",
            provider_observed_at_utc=available_at_utc,
            provider_run_id=_optional_text(yahoo_row.get("source_run_id")),
            normalization_run_id=normalization_run_id or _optional_text(yahoo_row.get("run_id")),
        )
        field_source_map_by_key[key] = _missing_quarter_field_source_map(normalized_row, source_hash)
        normalized_rows.append(normalized_row)

    write_normalized_quarterly_rows_with_optional_vintage(
        conn,
        normalized_rows,
        write_vintage=True,
        vintage_metadata_by_key=metadata_by_key,
        field_source_map_by_key=field_source_map_by_key,
    )


def _audit_rows_by_key(audit_rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in audit_rows:
        if row.get("enrichment_status") != "FILLED_FROM_YAHOO":
            continue
        key = (str(row["ticker"]).upper(), str(row["period_end_date"]))
        grouped.setdefault(key, []).append(dict(row))
    return grouped


def _matched_yahoo_rows_by_key(
    conn: sqlite3.Connection,
    market: str,
    audit_rows_by_key: dict[tuple[str, str], list[dict[str, Any]]],
) -> dict[tuple[str, str], dict[str, Any]]:
    output: dict[tuple[str, str], dict[str, Any]] = {}
    yahoo_rows_cache: dict[str, dict[str, sqlite3.Row]] = {}
    for key, audit_rows in audit_rows_by_key.items():
        ticker, _period_end_date = key
        yahoo_rows = yahoo_rows_cache.setdefault(ticker, load_yahoo_rows(conn, market, ticker))
        matched_period = str(audit_rows[0]["matched_yahoo_period_end_date"])
        yahoo_row = yahoo_rows.get(matched_period)
        if yahoo_row is None:
            raise ValueError(f"YAHOO_FALLBACK_ENRICH_CLI_VINTAGE_YAHOO_ROW_MISSING:{ticker},{matched_period}")
        output[key] = dict(yahoo_row)
    return output


def _missing_quarter_field_source_map(
    normalized_row: dict[str, Any],
    source_hash: str,
) -> dict[str, dict[str, Any]]:
    source_map: dict[str, dict[str, Any]] = {}
    source_row_ref = f"{normalized_row['ticker']}:{normalized_row['period_end_date']}"
    for field_name in REPORTED_FINANCIAL_FIELDS:
        if normalized_row.get(field_name) is None:
            continue
        source_map[field_name] = {
            "source_provider": "yahoo",
            "source_table": "rc_fundamental_yahoo_quarterly",
            "source_row_ref": source_row_ref,
            "source_hash": source_hash,
            "provenance_role": "PROVIDER_REPORTED",
            "merge_action": "YAHOO_INSERTED_MISSING_QUARTER",
        }
    return source_map


def _with_optional_normalization_run_id(row: dict[str, Any], normalization_run_id: str | None) -> dict[str, Any]:
    if normalization_run_id is not None and str(normalization_run_id).strip():
        row["run_id"] = str(normalization_run_id).strip()
    return row


def _optional_text(value: Any) -> str | None:
    if value is None or not str(value).strip():
        return None
    return str(value).strip()


def main() -> None:
    args = parse_args()
    write_vintage = bool(getattr(args, "write_vintage", False))
    summary = run_yahoo_fallback_enrich(
        db_path=resolve_db_path(args.db),
        market=args.market,
        ticker=args.ticker,
        run_id=args.run_id,
        dry_run=args.dry_run,
        replace_audit_for_run=args.replace_audit_for_run,
        write_vintage=write_vintage,
        vintage_market=getattr(args, "vintage_market", None),
        vintage_available_at_utc=getattr(args, "vintage_available_at_utc", None),
        vintage_ingested_at_utc=getattr(args, "vintage_ingested_at_utc", None),
        vintage_run_id=getattr(args, "vintage_run_id", None),
        vintage_normalization_run_id=getattr(args, "vintage_normalization_run_id", None),
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
    if write_vintage:
        _summary(vintage_write="enabled")


if __name__ == "__main__":
    main()
