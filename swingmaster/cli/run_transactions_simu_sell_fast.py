from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from swingmaster.reporting.sell_rules_engine import apply_sell_rules, load_sell_rules_config


ENGINE_VERSION = "SIMU_SELL_FAST_V1"
SELL_RULES_DIR = ROOT / "daily_reports" / "sell_rules"
MAX_EVAL_ROWS = 2_000_000


def parse_boolish(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError("invalid boolean value")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run SELL simulation from open positions")
    parser.add_argument("--rc-db", required=True, help="RC SQLite database path")
    parser.add_argument("--osakedata-db", required=True, help="osakedata SQLite database path")
    parser.add_argument("--market", required=True, help="Market code (FIN|SE|USA)")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--sell-ruleset", default=None, help="Optional sell rules market override")
    parser.add_argument("--mode", choices=["append", "replace-run"], default="append")
    parser.add_argument(
        "--dry-run",
        nargs="?",
        const="true",
        default="true",
        type=parse_boolish,
        help="Do not write to rc_transactions_simu (default: true)",
    )
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id")
    parser.add_argument("--created-at", default=None, help="Optional ISO8601 created_at timestamp")
    return parser.parse_args()


def summary(**items: Any) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def exit_with_message(message: str) -> None:
    print(f"message={message}")
    raise SystemExit(2)


def exit_with_error_summary(message: str) -> None:
    summary(status="ERROR")
    print(f"message={message}")
    raise SystemExit(2)


def normalize_market(value: str) -> str:
    market = value.upper()
    if market not in {"FIN", "SE", "USA"}:
        raise ValueError("INVALID_MARKET")
    return market


def validate_date_range(start_date: str, end_date: str) -> Tuple[date, date]:
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError as exc:
        raise ValueError("INVALID_DATE_RANGE") from exc
    if start > end:
        raise ValueError("INVALID_DATE_RANGE")
    return start, end


def resolve_created_at(value: str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return value


def sell_ruleset_identity_for_market(market: str) -> str:
    return str((SELL_RULES_DIR / f"{market.lower()}.json").resolve())


def compute_run_id(
    market: str,
    start_date: str,
    end_date: str,
    sell_ruleset_identity: str,
    rc_db: str,
    osakedata_db: str,
) -> Tuple[str, str]:
    payload = {
        "end_date": end_date,
        "engine_version": ENGINE_VERSION,
        "market": market,
        "osakedata_db": osakedata_db,
        "rc_db": rc_db,
        "sell_ruleset_identity": sell_ruleset_identity,
        "start_date": start_date,
    }
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    shortsha = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()[:10]
    run_id = f"{ENGINE_VERSION}_{market}_{start_date.replace('-', '')}_{end_date.replace('-', '')}_{shortsha}"
    return run_id, canonical_json


def build_sell_reason(run_id: str, rule_hit: str) -> str:
    return f"{ENGINE_VERSION}_RUNID={run_id}|{rule_hit}"


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def fetch_open_positions(conn: sqlite3.Connection, end_date: str) -> List[Dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
          id,
          ticker,
          market,
          buy_date,
          buy_price,
          buy_qty,
          run_id AS buy_run_id
        FROM rc_transactions_simu
        WHERE sell_date IS NULL
          AND buy_date <= ?
        ORDER BY buy_date ASC, ticker ASC, id ASC
        """,
        (end_date,),
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def fetch_price_rows(
    conn: sqlite3.Connection,
    market: str,
    tickers: Sequence[str],
    min_needed_date: str,
    end_date: str,
) -> List[Tuple[str, str, float]]:
    if not tickers:
        return []

    placeholders = ",".join("?" for _ in tickers)
    sql = f"""
    SELECT osake, pvm, close
    FROM osakedata
    WHERE market = ?
      AND osake IN ({placeholders})
      AND pvm >= ?
      AND pvm <= ?
    ORDER BY osake ASC, pvm ASC
    """
    params: List[Any] = [market.lower()]
    params.extend(tickers)
    params.extend([min_needed_date, end_date])
    cursor = conn.execute(sql, params)
    return [(str(row[0]), str(row[1]), float(row[2])) for row in cursor.fetchall()]


def build_price_maps(
    price_rows: Sequence[Tuple[str, str, float]],
) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, int]], Dict[Tuple[str, str], float]]:
    dates_by_ticker: Dict[str, List[str]] = {}
    idx_by_ticker_date: Dict[str, Dict[str, int]] = {}
    close_by_ticker_date: Dict[Tuple[str, str], float] = {}

    for ticker, trading_date, close in price_rows:
        ticker_dates = dates_by_ticker.setdefault(ticker, [])
        idx_by_date = idx_by_ticker_date.setdefault(ticker, {})
        ticker_dates.append(trading_date)
        idx_by_date[trading_date] = len(idx_by_date)
        close_by_ticker_date[(ticker, trading_date)] = close

    return dates_by_ticker, idx_by_ticker_date, close_by_ticker_date


def evaluate_sell_rows(
    open_positions: Sequence[Dict[str, Any]],
    start_date: str,
    end_date: str,
    dates_by_ticker: Dict[str, List[str]],
    idx_by_ticker_date: Dict[str, Dict[str, int]],
    close_by_ticker_date: Dict[Tuple[str, str], float],
    rules_config: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], int, int, int, int]:
    sell_rows: List[Dict[str, Any]] = []
    eval_rows_total = 0
    priced_on_end_date = 0
    holding_days_available = 0
    missing_field_count = 0

    for position in open_positions:
        ticker = str(position["ticker"])
        buy_date = str(position["buy_date"])
        buy_price = position.get("buy_price")
        ticker_dates = dates_by_ticker.get(ticker, [])
        idx_by_date = idx_by_ticker_date.get(ticker, {})
        end_date_close = close_by_ticker_date.get((ticker, end_date))

        if end_date_close is not None:
            priced_on_end_date += 1
        if buy_date in idx_by_date and end_date in idx_by_date:
            holding_days_available += 1

        if not ticker_dates:
            continue

        first_eval_date = start_date if start_date >= buy_date else buy_date
        start_idx = bisect_left(ticker_dates, first_eval_date)
        end_idx = bisect_right(ticker_dates, end_date)

        for eval_date in ticker_dates[start_idx:end_idx]:
            eval_rows_total += 1
            if eval_rows_total > MAX_EVAL_ROWS:
                raise ValueError("TOO_MANY_EVAL_ROWS")

            holding_trading_days = None
            if buy_date in idx_by_date and eval_date in idx_by_date:
                holding_trading_days = idx_by_date[eval_date] - idx_by_date[buy_date]

            sell_price = close_by_ticker_date.get((ticker, eval_date))
            last_close_return = None
            if sell_price is not None and buy_price not in {None, 0}:
                last_close_return = (float(sell_price) / float(buy_price)) - 1.0

            row = {
                "id": position["id"],
                "section": "OPEN_POSITION",
                "ticker": ticker,
                "buy_date": buy_date,
                "buy_price": buy_price,
                "buy_qty": position.get("buy_qty"),
                "holding_trading_days": holding_trading_days,
                "last_close_return": last_close_return,
                "eval_date": eval_date,
                "sell_price": sell_price,
            }
            matched_rows, row_missing_field_count = apply_sell_rules(
                [row],
                rules_config,
                sell_section_name="SELLS",
            )
            missing_field_count += row_missing_field_count
            if matched_rows:
                sell_rows.append(matched_rows[0])
                break

    return sell_rows, eval_rows_total, priced_on_end_date, holding_days_available, missing_field_count


def revert_previous_run_sells(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute(
        """
        UPDATE rc_transactions_simu
        SET
          sell_date = NULL,
          sell_price = NULL,
          sell_qty = NULL,
          sell_reason = NULL,
          holding_trading_days = NULL
        WHERE sell_reason LIKE ?
        """,
        (f"{ENGINE_VERSION}_RUNID={run_id}%",),
    )


def apply_sell_updates(
    conn: sqlite3.Connection,
    sell_rows: Sequence[Dict[str, Any]],
    run_id: str,
) -> Tuple[int, int, int]:
    sells_updated = 0
    sells_skipped_already_closed = 0
    sells_skipped_missing_price = 0

    for sell_row in sorted(sell_rows, key=lambda row: (str(row.get("eval_date") or ""), str(row.get("ticker") or ""), int(row["id"]))):
        if sell_row.get("sell_price") is None:
            sells_skipped_missing_price += 1
            continue

        cursor = conn.execute(
            """
            UPDATE rc_transactions_simu
            SET
              sell_date = ?,
              sell_price = ?,
              sell_qty = ?,
              sell_reason = ?,
              holding_trading_days = ?
            WHERE id = ?
              AND sell_date IS NULL
            """,
            (
                sell_row["eval_date"],
                float(sell_row["sell_price"]),
                sell_row.get("buy_qty"),
                build_sell_reason(run_id, str(sell_row["rule_hit"])),
                sell_row.get("holding_trading_days"),
                sell_row["id"],
            ),
        )
        if cursor.rowcount == 1:
            sells_updated += 1
        else:
            sells_skipped_already_closed += 1

    return sells_updated, sells_skipped_already_closed, sells_skipped_missing_price


def main() -> None:
    args = parse_args()

    try:
        validate_date_range(args.start_date, args.end_date)
    except ValueError:
        exit_with_message("INVALID_DATE_RANGE")

    try:
        market = normalize_market(args.market)
        sell_ruleset_market = normalize_market(args.sell_ruleset or args.market)
    except ValueError as exc:
        exit_with_message(str(exc))

    _created_at = resolve_created_at(args.created_at)
    _requested_dry_run = args.dry_run

    del _created_at
    del _requested_dry_run

    rc_db_path = str(Path(args.rc_db).resolve())
    osakedata_db_path = str(Path(args.osakedata_db).resolve())
    sell_ruleset_identity = sell_ruleset_identity_for_market(sell_ruleset_market)
    dry_run = bool(args.dry_run)

    if args.run_id:
        run_id = args.run_id
        canonical_json = json.dumps(
            {
                "end_date": args.end_date,
                "engine_version": ENGINE_VERSION,
                "market": market,
                "osakedata_db": osakedata_db_path,
                "rc_db": rc_db_path,
                "sell_ruleset_identity": sell_ruleset_identity,
                "start_date": args.start_date,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    else:
        run_id, canonical_json = compute_run_id(
            market=market,
            start_date=args.start_date,
            end_date=args.end_date,
            sell_ruleset_identity=sell_ruleset_identity,
            rc_db=rc_db_path,
            osakedata_db=osakedata_db_path,
        )

    try:
        rules_config = load_sell_rules_config(sell_ruleset_market)
    except Exception:
        exit_with_message("SELL_RULESET_LOAD_FAILED")

    rc_conn = sqlite3.connect(rc_db_path)
    transaction_started = False
    try:
        if not table_exists(rc_conn, "rc_transactions_simu"):
            exit_with_message("RC_TRANSACTIONS_SIMU_MISSING")

        if not dry_run and args.mode == "replace-run":
            try:
                rc_conn.execute("BEGIN")
                transaction_started = True
                revert_previous_run_sells(rc_conn, run_id)
            except sqlite3.Error:
                rc_conn.rollback()
                exit_with_error_summary("DB_UPDATE_FAILED")

        open_positions = fetch_open_positions(rc_conn, args.end_date)

        tickers = sorted({str(row["ticker"]) for row in open_positions if row.get("ticker") not in {None, "", "(none)"}})
        min_needed_date = min([args.start_date, *[str(row["buy_date"]) for row in open_positions]], default=args.start_date)

        with sqlite3.connect(osakedata_db_path) as os_conn:
            if not table_exists(os_conn, "osakedata"):
                if transaction_started:
                    rc_conn.rollback()
                    transaction_started = False
                exit_with_message("OSAKEDATA_TABLE_MISSING")
            price_rows = fetch_price_rows(
                conn=os_conn,
                market=market,
                tickers=tickers,
                min_needed_date=min_needed_date,
                end_date=args.end_date,
            )

        dates_by_ticker, idx_by_ticker_date, close_by_ticker_date = build_price_maps(price_rows)
        try:
            sell_rows, eval_rows_total, priced_on_end_date, holding_days_available, missing_field_count = evaluate_sell_rows(
                open_positions=open_positions,
                start_date=args.start_date,
                end_date=args.end_date,
                dates_by_ticker=dates_by_ticker,
                idx_by_ticker_date=idx_by_ticker_date,
                close_by_ticker_date=close_by_ticker_date,
                rules_config=rules_config,
            )
        except ValueError as exc:
            if transaction_started:
                rc_conn.rollback()
                transaction_started = False
            if str(exc) == "TOO_MANY_EVAL_ROWS":
                exit_with_error_summary("TOO_MANY_EVAL_ROWS")
            raise

        sells_matched = len(sell_rows)
        earliest_sell_date = min((str(row["eval_date"]) for row in sell_rows), default="")
        latest_sell_date = max((str(row["eval_date"]) for row in sell_rows), default="")
        sells_updated = 0
        sells_skipped_already_closed = 0
        sells_skipped_missing_price = 0

        if not dry_run:
            try:
                if args.mode == "append":
                    rc_conn.execute("BEGIN")
                    transaction_started = True
                sells_updated, sells_skipped_already_closed, sells_skipped_missing_price = apply_sell_updates(
                    conn=rc_conn,
                    sell_rows=sell_rows,
                    run_id=run_id,
                )
                rc_conn.commit()
                transaction_started = False
            except sqlite3.Error:
                rc_conn.rollback()
                transaction_started = False
                exit_with_error_summary("DB_UPDATE_FAILED")
        else:
            sells_skipped_missing_price = sum(1 for row in sell_rows if row.get("sell_price") is None)

        summary(
            status="OK",
            rc_db=rc_db_path,
            osakedata_db=osakedata_db_path,
            market=market,
            start_date=args.start_date,
            end_date=args.end_date,
            run_id=run_id,
            engine_version=ENGINE_VERSION,
            sell_ruleset_identity=sell_ruleset_identity,
            open_positions_total=len(open_positions),
            priced_on_end_date=priced_on_end_date,
            holding_days_available=holding_days_available,
            eval_rows_total=eval_rows_total,
            sells_matched=sells_matched,
            earliest_sell_date=earliest_sell_date,
            latest_sell_date=latest_sell_date,
            mode=args.mode,
            sell_rows_total=len(sell_rows),
            sells_updated=sells_updated,
            sells_skipped_already_closed=sells_skipped_already_closed,
            sells_skipped_missing_price=sells_skipped_missing_price,
            missing_field_count=missing_field_count,
            dry_run=1 if dry_run else 0,
        )
        print(canonical_json)
    finally:
        rc_conn.close()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        print(f"message={exc}")
        sys.exit(1)
