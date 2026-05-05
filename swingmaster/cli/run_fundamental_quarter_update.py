from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_quarter_state import acknowledge_ingested, load_latest_quarter_rows
from swingmaster.cli.run_fundamental_quarterly_to_ttm import run_quarterly_to_ttm
from swingmaster.cli.run_fundamental_yahoo_fallback_enrich import run_yahoo_fallback_enrich
from swingmaster.fundamentals.lifecycle import run_lifecycle_classification
from swingmaster.fundamentals.score import run_fundamental_scoring


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process flagged quarter-state tickers through score")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--run-id", required=True, help="Deterministic base run identifier")
    parser.add_argument("--market", default=None, help="Optional market filter")
    parser.add_argument("--ticker", default=None, help="Optional single ticker filter")
    parser.add_argument("--limit", type=int, default=None, help="Optional ticker limit after deterministic ordering")
    parser.add_argument("--dry-run", action="store_true", help="Read-only preview without running write paths")
    parser.add_argument("--skip-ack", action="store_true", help="Run steps but do not acknowledge quarter state")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def derive_child_run_ids(base_run_id: str) -> dict[str, str]:
    return {
        "ttm": f"{base_run_id}__TTM",
        "lifecycle": f"{base_run_id}__LIFECYCLE",
        "score": f"{base_run_id}__SCORE",
        "ack": f"{base_run_id}__ACK",
        "enrich": f"{base_run_id}__ENRICH",
    }


def load_eligible_rows(
    db_path: Path,
    market: str | None,
    ticker: str | None,
    limit: int | None,
) -> list[sqlite3.Row]:
    with sqlite3.connect(str(db_path)) as conn:
        previous_row_factory = conn.row_factory
        conn.row_factory = sqlite3.Row
        try:
            sql = """
                SELECT
                    ticker,
                    market,
                    latest_db_period_end_date,
                    detected_source_period_end_date,
                    new_quarter_available
                FROM rc_fundamental_quarter_state
                WHERE new_quarter_available = 1
            """
            params: list[object] = []
            if market is not None:
                sql += " AND market = ?"
                params.append(market.strip().lower())
            if ticker is not None:
                sql += " AND ticker = ?"
                params.append(ticker.strip().upper())
            sql += " ORDER BY ticker ASC"
            if limit is not None:
                sql += " LIMIT ?"
                params.append(limit)
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.row_factory = previous_row_factory
    return rows


def run_lifecycle_step(db_path: Path, ticker: str, dry_run: bool) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        rows_classified, _class_counts = run_lifecycle_classification(
            conn=conn,
            ticker=ticker.upper(),
            dry_run=dry_run,
        )
    return 0 if dry_run else rows_classified


def run_score_step(db_path: Path, ticker: str, dry_run: bool) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        rows_scored, _min_score, _max_score, _avg_score = run_fundamental_scoring(
            conn=conn,
            ticker=ticker.upper(),
            dry_run=dry_run,
        )
    return 0 if dry_run else rows_scored


def latest_quarter_meets_detected(conn: sqlite3.Connection, ticker: str, detected_source_period_end_date: str) -> bool:
    row = conn.execute(
        """
        SELECT MAX(period_end_date)
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
        """,
        (ticker.upper(),),
    ).fetchone()
    if row is None or row[0] is None:
        return False
    return str(row[0]) >= detected_source_period_end_date


def acknowledge_ticker(db_path: Path, ticker: str, run_id: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        rows = load_latest_quarter_rows(conn, ticker.upper())
        rows_updated = acknowledge_ingested(
            conn,
            rows,
            run_id=run_id,
            updated_at_utc=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        )
        conn.commit()
    return rows_updated


def process_ticker(
    db_path: Path,
    row: sqlite3.Row,
    child_run_ids: dict[str, str],
    skip_ack: bool,
) -> dict[str, int]:
    ticker = str(row["ticker"]).upper()
    market = str(row["market"]).lower()
    detected_source_period_end_date = row["detected_source_period_end_date"]
    if detected_source_period_end_date is None:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_DETECTED_DATE_MISSING:{ticker}")

    print(f"TICKER {ticker} market={market} detected_period={detected_source_period_end_date}")

    if market == "usa":
        run_yahoo_fallback_enrich(
            db_path=db_path,
            market=market,
            ticker=ticker,
            run_id=child_run_ids["enrich"],
            dry_run=False,
            replace_audit_for_run=False,
        )

    ttm_summary = run_quarterly_to_ttm(
        db_path=db_path,
        ticker=ticker,
        run_id=child_run_ids["ttm"],
        dry_run=False,
        replace_ticker=True,
    )
    print("STEP ttm=OK")

    lifecycle_rows_written = run_lifecycle_step(
        db_path=db_path,
        ticker=ticker,
        dry_run=False,
    )
    print("STEP lifecycle=OK")

    score_rows_written = run_score_step(
        db_path=db_path,
        ticker=ticker,
        dry_run=False,
    )
    print("STEP score=OK")

    ack_rows_written = 0
    if skip_ack:
        print("STEP ack=SKIPPED")
    else:
        with sqlite3.connect(str(db_path)) as conn:
            if not latest_quarter_meets_detected(conn, ticker, str(detected_source_period_end_date)):
                raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_ACK_PERIOD_MISMATCH:{ticker}")
        ack_rows_written = acknowledge_ticker(
            db_path=db_path,
            ticker=ticker,
            run_id=child_run_ids["ack"],
        )
        print("STEP ack=OK")

    return {
        "ttm_rows_written": int(ttm_summary["rows_written"]),
        "lifecycle_rows_written": lifecycle_rows_written,
        "score_rows_written": score_rows_written,
        "ack_rows_written": ack_rows_written,
    }


def run_fundamental_quarter_update(
    db_path: Path,
    run_id: str,
    market: str | None,
    ticker: str | None,
    limit: int | None,
    dry_run: bool,
    skip_ack: bool,
) -> dict[str, object]:
    rows = load_eligible_rows(db_path, market, ticker, limit)
    market_label = market.strip().lower() if market is not None else "ALL"
    if dry_run:
        for row in rows:
            print(
                f"TICKER {str(row['ticker']).upper()} market={str(row['market']).lower()} "
                f"detected_period={row['detected_source_period_end_date']}"
            )
        summary = {
            "tickers_total": len(rows),
            "tickers_processed": 0,
            "market": market_label,
            "dry_run": 1,
            "skip_ack": 1 if skip_ack else 0,
            "run_id": run_id,
        }
        _summary(**summary)
        return summary

    child_run_ids = derive_child_run_ids(run_id)
    tickers_processed = 0
    for row in rows:
        current_ticker = str(row["ticker"]).upper()
        try:
            process_ticker(
                db_path=db_path,
                row=row,
                child_run_ids=child_run_ids,
                skip_ack=skip_ack,
            )
        except Exception as exc:
            step_name = "unknown"
            message = str(exc)
            if "DETECTED_DATE_MISSING" in message:
                step_name = "state"
            elif "ACK_PERIOD_MISMATCH" in message:
                step_name = "ack"
            elif "FUNDAMENTAL_TTM" in message:
                step_name = "ttm"
            elif "LIFECYCLE" in message:
                step_name = "lifecycle"
            elif "SCORE" in message:
                step_name = "score"
            elif "YAHOO" in message or "ENRICH" in message:
                step_name = "enrichment"
            print(f"TICKER {current_ticker}=FAILED")
            print(f"ERROR ticker={current_ticker} step={step_name}")
            raise
        tickers_processed += 1

    summary = {
        "tickers_total": len(rows),
        "tickers_processed": tickers_processed,
        "market": market_label,
        "dry_run": 0,
        "skip_ack": 1 if skip_ack else 0,
        "run_id": run_id,
    }
    _summary(**summary)
    return summary


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    try:
        run_fundamental_quarter_update(
            db_path=db_path,
            run_id=args.run_id,
            market=args.market,
            ticker=args.ticker,
            limit=args.limit,
            dry_run=args.dry_run,
            skip_ack=args.skip_ack,
        )
    except Exception as exc:
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
