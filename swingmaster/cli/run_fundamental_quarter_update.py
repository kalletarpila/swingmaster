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
from swingmaster.cli.run_fundamental_valuation import run_fundamental_valuation
from swingmaster.cli.run_fundamental_yahoo_audit import run_yahoo_audit
from swingmaster.cli.run_fundamental_yahoo_fallback_enrich import run_yahoo_fallback_enrich
from swingmaster.cli.run_fundamental_yahoo_quarterly_write import run_yahoo_quarterly_write
from swingmaster.cli.run_fundamental_yahoo_to_quarterly import run_yahoo_to_quarterly
from swingmaster.fundamentals.lifecycle import run_lifecycle_classification
from swingmaster.fundamentals.score import run_fundamental_scoring

DEFAULT_EXCHANGE = "HE"
VINTAGE_MODE_VALIDATION_ONLY = "validation_only"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process flagged quarter-state tickers through score")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--run-id", required=True, help="Deterministic base run identifier")
    parser.add_argument("--market", default=None, help="Optional market filter")
    parser.add_argument("--ticker", default=None, help="Optional single ticker filter")
    parser.add_argument("--limit", type=int, default=None, help="Optional ticker limit after deterministic ordering")
    parser.add_argument(
        "--osakedata-db",
        default=None,
        help="OHLCV SQLite database path used for final USA valuation step (required when market is usa or omitted)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Read-only preview without running write paths")
    parser.add_argument("--skip-ack", action="store_true", help="Run steps but do not acknowledge quarter state")
    parser.add_argument(
        "--write-vintage",
        action="store_true",
        help="Validate future reported vintage metadata without executing vintage writes",
    )
    parser.add_argument("--vintage-market", default=None, help="Required market for future vintage writes")
    parser.add_argument(
        "--vintage-available-at-utc",
        default=None,
        help="Required explicit PIT availability timestamp for future vintage writes",
    )
    parser.add_argument(
        "--vintage-ingested-at-utc",
        default=None,
        help="Required explicit ingestion timestamp for future vintage writes",
    )
    parser.add_argument("--vintage-run-id", default=None, help="Required explicit run id for future vintage writes")
    parser.add_argument(
        "--vintage-normalization-run-id",
        default=None,
        help="Optional normalization run id for future vintage writes",
    )
    parser.add_argument(
        "--vintage-mode",
        default=None,
        choices=[VINTAGE_MODE_VALIDATION_ONLY],
        help="Phase 4I6 supports validation_only only; no vintage writes are executed",
    )
    return parser.parse_args(argv)


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def _validate_vintage_timestamp(value: str, field_name: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_INVALID_TIMESTAMP:{field_name}") from exc


def validate_vintage_options(
    *,
    write_vintage: bool,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
    vintage_mode: str | None,
) -> dict[str, object]:
    if not write_vintage:
        return {}
    if vintage_market is None or vintage_market.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MARKET_REQUIRED")
    if vintage_available_at_utc is None or vintage_available_at_utc.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_AVAILABLE_AT_UTC_REQUIRED")
    if vintage_ingested_at_utc is None or vintage_ingested_at_utc.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_INGESTED_AT_UTC_REQUIRED")
    if vintage_run_id is None or vintage_run_id.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_RUN_ID_REQUIRED")
    if vintage_mode is None or vintage_mode.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MODE_REQUIRED")
    if vintage_mode != VINTAGE_MODE_VALIDATION_ONLY:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MODE_UNSUPPORTED:{vintage_mode}")
    _validate_vintage_timestamp(vintage_available_at_utc, "vintage_available_at_utc")
    _validate_vintage_timestamp(vintage_ingested_at_utc, "vintage_ingested_at_utc")
    return {
        "vintage_requested": True,
        "vintage_mode": VINTAGE_MODE_VALIDATION_ONLY,
        "vintage_execution_enabled": False,
        "vintage_validation_status": "OK",
        "vintage_rows_inserted": 0,
        "vintage_provenance_rows_inserted": 0,
        "vintage_rows_skipped_noop": 0,
        "vintage_rows_failed": 0,
        "vintage_error_summary": None,
    }


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def resolve_optional_db_path(db_arg: str | None) -> Path | None:
    if db_arg is None:
        return None
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
        "valuation": f"{base_run_id}__VALUATION",
        "ack": f"{base_run_id}__ACK",
        "enrich": f"{base_run_id}__ENRICH",
    }


def resolve_latest_close_as_of_date(osakedata_db_path: Path, market: str) -> str:
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        row = conn.execute(
            """
            SELECT MAX(pvm)
            FROM osakedata
            WHERE market = ?
              AND close IS NOT NULL
            """,
            (market,),
        ).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_VALUATION_AS_OF_DATE_NOT_FOUND:{market}")
    return str(row[0])


def should_run_usa_valuation(market: str | None) -> bool:
    if market is None:
        return True
    return market.strip().lower() == "usa"


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


def latest_quarter_period_end_date(conn: sqlite3.Connection, ticker: str) -> str | None:
    row = conn.execute(
        """
        SELECT MAX(period_end_date)
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
        """,
        (ticker.upper(),),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def _build_sec_missing_detected_message(ticker: str, detected_source_period_end_date: str, latest_quarter: str | None) -> str:
    return (
        "FUNDAMENTAL_QUARTER_UPDATE_SEC_REFRESH_MISSING_DETECTED:"
        f"{ticker}:expected_detected_period={detected_source_period_end_date}:"
        f"latest_quarter_after_sec_refresh={latest_quarter or 'NONE'}"
    )


def _build_enrich_missing_detected_message(
    ticker: str,
    detected_source_period_end_date: str,
    latest_quarter: str | None,
) -> str:
    return (
        "FUNDAMENTAL_QUARTER_UPDATE_ENRICH_MISSING_DETECTED:"
        f"{ticker}:expected_detected_period={detected_source_period_end_date}:"
        f"latest_quarter_after_enrich={latest_quarter or 'NONE'}"
    )


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
                    latest_quarter = latest_quarter_period_end_date(conn, ticker)
                    print(
                        f"WARN ticker={ticker.upper()} step=quarterly_refresh_sec "
                        f"message={_build_sec_missing_detected_message(ticker, detected_source_period_end_date, latest_quarter)}"
                    )

        enrich_summary = run_yahoo_fallback_enrich(
            db_path=db_path,
            market=market,
            ticker=ticker,
            run_id=child_run_ids["enrich"],
            dry_run=False,
            replace_audit_for_run=False,
            detected_source_period_end_date=detected_source_period_end_date,
        )
        with sqlite3.connect(str(db_path)) as conn:
            if not usa_quarter_satisfies_detected(conn, ticker, detected_source_period_end_date):
                latest_quarter = latest_quarter_period_end_date(conn, ticker)
                raise RuntimeError(
                    _build_enrich_missing_detected_message(ticker, detected_source_period_end_date, latest_quarter)
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
    osakedata_db_path: Path | None,
    run_id: str,
    market: str | None,
    ticker: str | None,
    limit: int | None,
    dry_run: bool,
    skip_ack: bool,
    write_vintage: bool = False,
    vintage_market: str | None = None,
    vintage_available_at_utc: str | None = None,
    vintage_ingested_at_utc: str | None = None,
    vintage_run_id: str | None = None,
    vintage_normalization_run_id: str | None = None,
    vintage_mode: str | None = None,
) -> dict[str, object]:
    vintage_summary = validate_vintage_options(
        write_vintage=write_vintage,
        vintage_market=vintage_market,
        vintage_available_at_utc=vintage_available_at_utc,
        vintage_ingested_at_utc=vintage_ingested_at_utc,
        vintage_run_id=vintage_run_id,
        vintage_mode=vintage_mode,
    )
    _ = vintage_normalization_run_id
    rows = load_eligible_rows(db_path, market, ticker, limit)
    market_label = market.strip().lower() if market is not None else "ALL"
    strict_single_ticker_mode = ticker is not None
    if dry_run:
        for row in rows:
            print(
                f"TICKER {str(row['ticker']).upper()} market={str(row['market']).lower()} "
                f"detected_period={row['detected_source_period_end_date']}"
            )
        summary = {
            "tickers_total": len(rows),
            "tickers_processed": 0,
            "tickers_succeeded": 0,
            "tickers_failed": 0,
            "market": market_label,
            "dry_run": 1,
            "skip_ack": 1 if skip_ack else 0,
            "run_id": run_id,
        }
        summary.update(vintage_summary)
        _summary(**summary)
        return summary

    child_run_ids = derive_child_run_ids(run_id)
    tickers_processed = 0
    tickers_succeeded = 0
    tickers_failed = 0
    for row in rows:
        current_ticker = str(row["ticker"]).upper()
        tickers_processed += 1
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
            print(f"ERROR ticker={current_ticker} step={step_name} message={message}")
            tickers_failed += 1
            if strict_single_ticker_mode:
                raise
            continue
        tickers_succeeded += 1

    valuation_as_of_date = ""
    valuation_rows_written = 0
    if should_run_usa_valuation(market):
        if osakedata_db_path is None:
            raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_OSAKEDATA_DB_REQUIRED_FOR_USA_VALUATION")
        valuation_as_of_date = resolve_latest_close_as_of_date(osakedata_db_path, market="usa")
        valuation_summary = run_fundamental_valuation(
            db_path=db_path,
            osakedata_db_path=osakedata_db_path,
            market="usa",
            as_of_date=valuation_as_of_date,
            ticker=None,
            run_id=child_run_ids["valuation"],
            dry_run=False,
            replace=True,
        )
        valuation_rows_written = int(valuation_summary["rows_written"])
        print(f"STEP valuation=OK as_of_date={valuation_as_of_date} rows_written={valuation_rows_written}")

    summary = {
        "tickers_total": len(rows),
        "tickers_processed": tickers_processed,
        "tickers_succeeded": tickers_succeeded,
        "tickers_failed": tickers_failed,
        "market": market_label,
        "dry_run": 0,
        "skip_ack": 1 if skip_ack else 0,
        "valuation_as_of_date": valuation_as_of_date,
        "valuation_rows_written": valuation_rows_written,
        "run_id": run_id,
    }
    summary.update(vintage_summary)
    _summary(**summary)
    if not strict_single_ticker_mode and tickers_failed > 0:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed={tickers_failed}")
    return summary


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    osakedata_db_path = resolve_optional_db_path(args.osakedata_db)
    try:
        run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=osakedata_db_path,
            run_id=args.run_id,
            market=args.market,
            ticker=args.ticker,
            limit=args.limit,
            dry_run=args.dry_run,
            skip_ack=args.skip_ack,
            write_vintage=args.write_vintage,
            vintage_market=args.vintage_market,
            vintage_available_at_utc=args.vintage_available_at_utc,
            vintage_ingested_at_utc=args.vintage_ingested_at_utc,
            vintage_run_id=args.vintage_run_id,
            vintage_normalization_run_id=args.vintage_normalization_run_id,
            vintage_mode=args.vintage_mode,
        )
    except Exception as exc:
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
