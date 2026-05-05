from __future__ import annotations

import argparse
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_bootstrap_sec_raw import SEC_USER_AGENT, run_sec_raw_bootstrap
from swingmaster.fundamentals.build_quarterly import build_and_insert_quarterly_rows
from swingmaster.cli.run_fundamental_quarter_state import acknowledge_ingested, load_latest_quarter_rows
from swingmaster.cli.run_fundamental_quarterly_to_ttm import run_quarterly_to_ttm
from swingmaster.cli.run_fundamental_yahoo_audit import run_yahoo_audit
from swingmaster.cli.run_fundamental_yahoo_fallback_enrich import run_yahoo_fallback_enrich
from swingmaster.cli.run_fundamental_yahoo_quarterly_write import run_yahoo_quarterly_write
from swingmaster.cli.run_fundamental_yahoo_to_quarterly import run_yahoo_to_quarterly
from swingmaster.fundamentals.lifecycle import run_lifecycle_classification
from swingmaster.fundamentals.score import run_fundamental_scoring

DEFAULT_EXCHANGE = "HE"


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
        "raw": f"{base_run_id}__RAW",
        "yqtr": f"{base_run_id}__YQTR",
        "qbridge": f"{base_run_id}__QBRIDGE",
        "sec_raw": f"{base_run_id}__SEC_RAW",
        "quarterly": f"{base_run_id}__QUARTERLY",
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


def _calendar_quarter(value: date) -> int:
    return ((value.month - 1) // 3) + 1


def usa_quarter_satisfies_detected(conn: sqlite3.Connection, ticker: str, detected_source_period_end_date: str) -> bool:
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


def run_sec_quarterly_build_step(db_path: Path, ticker: str, run_id: str, dry_run: bool) -> tuple[int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        return build_and_insert_quarterly_rows(
            conn=conn,
            ticker=ticker.upper(),
            run_id=run_id,
            dry_run=dry_run,
        )


def run_quarterly_refresh(
    db_path: Path,
    ticker: str,
    market: str,
    child_run_ids: dict[str, str],
) -> dict[str, Any]:
    if market == "usa":
        retrieved_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with sqlite3.connect(str(db_path)) as conn:
            state_row = conn.execute(
                """
                SELECT detected_source_period_end_date
                FROM rc_fundamental_quarter_state
                WHERE ticker = ?
                """,
                (ticker.upper(),),
            ).fetchone()
            detected_source_period_end_date = str(state_row[0]) if state_row is not None and state_row[0] is not None else None
            if detected_source_period_end_date is None:
                raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_DETECTED_DATE_MISSING:{ticker}")
            sec_refresh_required = not usa_quarter_satisfies_detected(conn, ticker, detected_source_period_end_date)

        sec_refresh_summary: dict[str, Any] | None = None
        if sec_refresh_required:
            cik, rows = run_sec_raw_bootstrap(
                db_path=db_path,
                ticker=ticker,
                run_id=child_run_ids["sec_raw"],
                retrieved_at_utc=retrieved_at_utc,
                user_agent=SEC_USER_AGENT,
                dry_run=False,
            )
            periods_detected, rows_written = run_sec_quarterly_build_step(
                db_path=db_path,
                ticker=ticker,
                run_id=child_run_ids["quarterly"],
                dry_run=False,
            )
            sec_refresh_summary = {
                "cik": cik,
                "rows": rows,
                "periods_detected": periods_detected,
                "rows_written": rows_written,
            }
            with sqlite3.connect(str(db_path)) as conn:
                if not usa_quarter_satisfies_detected(conn, ticker, detected_source_period_end_date):
                    raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_SEC_REFRESH_MISSING_DETECTED:{ticker}")

        enrich_summary = run_yahoo_fallback_enrich(
            db_path=db_path,
            market=market,
            ticker=ticker,
            run_id=child_run_ids["enrich"],
            dry_run=False,
            replace_audit_for_run=False,
        )
        return {
            "mode": "enrich",
            "sec_refresh_required": sec_refresh_required,
            "sec_refresh_summary": sec_refresh_summary,
            "summary": enrich_summary,
        }

    raw_summary = run_yahoo_audit(
        db_path=db_path,
        market=market,
        exchange=DEFAULT_EXCHANGE,
        symbols_arg=ticker,
        limit=None,
        run_id=child_run_ids["raw"],
        dry_run=False,
    )
    if int(raw_summary["ok_count"]) <= 0:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_RAW_NOT_USABLE:{ticker}")

    yahoo_quarterly_summary = run_yahoo_quarterly_write(
        db_path=db_path,
        market=market,
        symbol=ticker,
        run_id=child_run_ids["yqtr"],
        dry_run=False,
        replace_symbol=True,
    )
    quarterly_bridge_summary = run_yahoo_to_quarterly(
        db_path=db_path,
        market=market,
        symbol=ticker,
        run_id=child_run_ids["qbridge"],
        dry_run=False,
        replace_symbol=True,
    )
    return {
        "mode": "yahoo_refresh",
        "raw_summary": raw_summary,
        "yahoo_quarterly_summary": yahoo_quarterly_summary,
        "quarterly_bridge_summary": quarterly_bridge_summary,
    }


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
    quarterly_refresh_summary = run_quarterly_refresh(
        db_path=db_path,
        ticker=ticker,
        market=market,
        child_run_ids=child_run_ids,
    )
    print("STEP quarterly_refresh=OK")

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
            detected_period_text = str(detected_source_period_end_date)
            if market == "usa":
                ack_allowed = usa_quarter_satisfies_detected(conn, ticker, detected_period_text)
            else:
                ack_allowed = latest_quarter_meets_detected(conn, ticker, detected_period_text)
            if not ack_allowed:
                raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_ACK_PERIOD_MISMATCH:{ticker}")
        ack_rows_written = acknowledge_ticker(
            db_path=db_path,
            ticker=ticker,
            run_id=child_run_ids["ack"],
        )
        print("STEP ack=OK")

    return {
        "quarterly_refresh_mode": 1 if quarterly_refresh_summary else 0,
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
            elif "RAW_NOT_USABLE" in message:
                step_name = "quarterly_refresh"
            elif "SEC_REFRESH" in message:
                step_name = "quarterly_refresh"
            elif "FUNDAMENTAL_TTM" in message:
                step_name = "ttm"
            elif "LIFECYCLE" in message:
                step_name = "lifecycle"
            elif "SCORE" in message:
                step_name = "score"
            elif "YAHOO" in message or "ENRICH" in message:
                step_name = "quarterly_refresh"
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
