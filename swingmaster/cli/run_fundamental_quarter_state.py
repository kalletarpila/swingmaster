from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage fundamentals quarter state per ticker")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--ticker", default=None, help="Optional single ticker")
    parser.add_argument("--market", default=None, help="Optional market override for new state rows")
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--sync-from-quarterly", action="store_true", help="Sync latest DB quarter dates from rc_fundamental_quarterly")
    mode_group.add_argument("--mark-detected-period", default=None, help="Set detected source period and new-quarter flag for ticker")
    mode_group.add_argument("--acknowledge-ingested", action="store_true", help="Clear new-quarter flag after DB quarter ingest")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def infer_market(ticker: str) -> str:
    return "omxh" if ticker.upper().endswith(".HE") else "usa"


def infer_primary_source(market: str) -> str:
    return "yahoo" if market == "omxh" else "sec_edgar"


def load_latest_quarter_rows(conn: sqlite3.Connection, ticker: str | None) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        if ticker is not None:
            rows = conn.execute(
                """
                SELECT ticker, MAX(period_end_date) AS latest_db_period_end_date
                FROM rc_fundamental_quarterly
                WHERE ticker = ?
                GROUP BY ticker
                ORDER BY ticker
                """,
                (ticker.upper(),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT ticker, MAX(period_end_date) AS latest_db_period_end_date
                FROM rc_fundamental_quarterly
                GROUP BY ticker
                ORDER BY ticker
                """
            ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return rows


def upsert_state_from_quarterly(conn: sqlite3.Connection, rows: list[sqlite3.Row], run_id: str, updated_at_utc: str) -> int:
    rows_updated = 0
    for row in rows:
        ticker = str(row["ticker"]).upper()
        market = infer_market(ticker)
        primary_source = infer_primary_source(market)
        latest_db_period_end_date = str(row["latest_db_period_end_date"]) if row["latest_db_period_end_date"] is not None else None
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_state (
                ticker,
                market,
                primary_source,
                latest_db_period_end_date,
                detected_source_period_end_date,
                new_quarter_available,
                last_checked_at_utc,
                last_updated_at_utc,
                last_detection_run_id,
                last_ingest_run_id
            ) VALUES (?, ?, ?, ?, NULL, 0, NULL, ?, NULL, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                market = excluded.market,
                primary_source = excluded.primary_source,
                latest_db_period_end_date = excluded.latest_db_period_end_date,
                last_updated_at_utc = excluded.last_updated_at_utc,
                last_ingest_run_id = excluded.last_ingest_run_id
            """,
            (
                ticker,
                market,
                primary_source,
                latest_db_period_end_date,
                updated_at_utc,
                run_id,
            ),
        )
        rows_updated += 1
    return rows_updated


def mark_detected_period(
    conn: sqlite3.Connection,
    ticker: str,
    market: str | None,
    detected_period_end_date: str,
    run_id: str,
    updated_at_utc: str,
) -> int:
    normalized_ticker = ticker.upper()
    resolved_market = market.strip().lower() if market is not None else infer_market(normalized_ticker)
    primary_source = infer_primary_source(resolved_market)
    row = conn.execute(
        """
        SELECT latest_db_period_end_date
        FROM rc_fundamental_quarter_state
        WHERE ticker = ?
        """,
        (normalized_ticker,),
    ).fetchone()
    latest_db_period_end_date = str(row[0]) if row is not None and row[0] is not None else None
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarter_state (
            ticker,
            market,
            primary_source,
            latest_db_period_end_date,
            detected_source_period_end_date,
            new_quarter_available,
            last_checked_at_utc,
            last_updated_at_utc,
            last_detection_run_id,
            last_ingest_run_id
        ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, NULL)
        ON CONFLICT(ticker) DO UPDATE SET
            market = excluded.market,
            primary_source = excluded.primary_source,
            detected_source_period_end_date = excluded.detected_source_period_end_date,
            new_quarter_available = 1,
            last_checked_at_utc = excluded.last_checked_at_utc,
            last_updated_at_utc = excluded.last_updated_at_utc,
            last_detection_run_id = excluded.last_detection_run_id
        """,
        (
            normalized_ticker,
            resolved_market,
            primary_source,
            latest_db_period_end_date,
            detected_period_end_date,
            updated_at_utc,
            updated_at_utc,
            run_id,
        ),
    )
    return 1


def acknowledge_ingested(conn: sqlite3.Connection, rows: list[sqlite3.Row], run_id: str, updated_at_utc: str) -> int:
    rows_updated = 0
    for row in rows:
        ticker = str(row["ticker"]).upper()
        latest_db_period_end_date = str(row["latest_db_period_end_date"]) if row["latest_db_period_end_date"] is not None else None
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_state (
                ticker,
                market,
                primary_source,
                latest_db_period_end_date,
                detected_source_period_end_date,
                new_quarter_available,
                last_checked_at_utc,
                last_updated_at_utc,
                last_detection_run_id,
                last_ingest_run_id
            ) VALUES (?, ?, ?, ?, NULL, 0, NULL, ?, NULL, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                market = excluded.market,
                primary_source = excluded.primary_source,
                latest_db_period_end_date = excluded.latest_db_period_end_date,
                detected_source_period_end_date = NULL,
                new_quarter_available = 0,
                last_updated_at_utc = excluded.last_updated_at_utc,
                last_ingest_run_id = excluded.last_ingest_run_id
            """,
            (
                ticker,
                infer_market(ticker),
                infer_primary_source(infer_market(ticker)),
                latest_db_period_end_date,
                updated_at_utc,
                run_id,
            ),
        )
        rows_updated += 1
    return rows_updated


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    updated_at_utc = now_utc()
    with sqlite3.connect(str(db_path)) as conn:
        if args.sync_from_quarterly:
            rows = load_latest_quarter_rows(conn, args.ticker)
            rows_updated = upsert_state_from_quarterly(conn, rows, args.run_id, updated_at_utc)
            conn.commit()
            _summary(mode="sync_from_quarterly")
            _summary(ticker=args.ticker.upper() if args.ticker is not None else "ALL")
            _summary(rows_updated=rows_updated)
            _summary(run_id=args.run_id)
            return

        if args.mark_detected_period is not None:
            if args.ticker is None:
                raise SystemExit("FUNDAMENTAL_QUARTER_STATE_TICKER_REQUIRED_FOR_MARK_DETECTED")
            rows_updated = mark_detected_period(
                conn,
                ticker=args.ticker,
                market=args.market,
                detected_period_end_date=args.mark_detected_period,
                run_id=args.run_id,
                updated_at_utc=updated_at_utc,
            )
            conn.commit()
            _summary(mode="mark_detected_period")
            _summary(ticker=args.ticker.upper())
            _summary(detected_source_period_end_date=args.mark_detected_period)
            _summary(rows_updated=rows_updated)
            _summary(run_id=args.run_id)
            return

        if args.acknowledge_ingested:
            rows = load_latest_quarter_rows(conn, args.ticker)
            rows_updated = acknowledge_ingested(conn, rows, args.run_id, updated_at_utc)
            conn.commit()
            _summary(mode="acknowledge_ingested")
            _summary(ticker=args.ticker.upper() if args.ticker is not None else "ALL")
            _summary(rows_updated=rows_updated)
            _summary(run_id=args.run_id)
            return

        raise SystemExit("FUNDAMENTAL_QUARTER_STATE_MODE_REQUIRED")


if __name__ == "__main__":
    main()
