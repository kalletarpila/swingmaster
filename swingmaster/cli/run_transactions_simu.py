from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from swingmaster.cli.daily_report import (
    MARKET_CONFIGS,
    apply_buy_rules,
    build_buy_rows_for_date,
    fetch_report_raw_rows,
    load_buy_rules_config,
    validate_buy_rules_config,
)


ENGINE_VERSION = "SIMU_TX_V1"
TRIGGER_FIELD_CANDIDATES = ["trigger", "trigger_key", "trigger_type", "buy_trigger", "event_type"]
DATE_FIELD_CANDIDATES = ["buy_date", "date", "as_of_date"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate rc_transactions_simu with BUY transactions only")
    parser.add_argument("--rc-db", required=True, help="RC SQLite database path")
    parser.add_argument("--market", required=True, help="Market code (FIN|SE|USA)")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--mode", choices=["append", "replace-run"], default="append")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--created-at", default=None, help="Optional ISO8601 created_at timestamp")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id")
    return parser.parse_args()


def summary(**items: Any) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def normalize_market(market: str) -> str:
    upper = market.upper()
    if upper not in {"FIN", "SE", "USA"}:
        raise ValueError(f"Unsupported market: {market}")
    return upper


def market_config_key(market: str) -> str:
    return market.lower()


def resolve_ruleset_identity(market: str) -> str:
    root = Path(__file__).resolve().parents[2]
    return str((root / "daily_reports" / "buy_rules" / f"{market.lower()}.json").resolve())


def compute_run_id(market: str, start_date: str, end_date: str, ruleset_identity: str) -> Tuple[str, str]:
    payload = {
        "end_date": end_date,
        "engine_version": ENGINE_VERSION,
        "market": market,
        "ruleset_identity": ruleset_identity,
        "start_date": start_date,
    }
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    shortsha = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()[:10]
    run_id = f"{ENGINE_VERSION}_{market}_{start_date.replace('-', '')}_{end_date.replace('-', '')}_{shortsha}"
    return run_id, canonical_json


def validate_date_range(start_date: str, end_date: str) -> None:
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError as exc:
        raise ValueError("INVALID_DATE_RANGE") from exc
    if start > end:
        raise ValueError("INVALID_DATE_RANGE")


def fetch_trading_dates_in_range(conn: sqlite3.Connection, start_date: str, end_date: str) -> List[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT date
        FROM rc_state_daily
        WHERE date >= ? AND date <= ?
        ORDER BY date ASC
        """,
        (start_date, end_date),
    ).fetchall()
    return [str(row[0]) for row in rows]


def load_base_rows_for_range(
    rc_db: Path,
    config: Any,
    rules_config: Dict[str, Any],
    start_date: str,
    end_date: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool, int, int]:
    base_rows: List[Dict[str, Any]] = []
    buy_rows: List[Dict[str, Any]] = []
    missing_fastpass_table = False
    days_with_rows = 0
    conn = sqlite3.connect(str(rc_db))
    try:
        trading_dates = fetch_trading_dates_in_range(conn, start_date, end_date)
    finally:
        conn.close()
    for as_of_date in trading_dates:
        rows, missing_fastpass = fetch_report_raw_rows(rc_db, config, as_of_date)
        missing_fastpass_table = missing_fastpass_table or missing_fastpass
        daily_base_rows = [row for row in rows if not str(row.get("section", "")).startswith("BUYS")]
        daily_buy_rows, missing_fastpass = build_buy_rows_for_date(rc_db, config, as_of_date)
        missing_fastpass_table = missing_fastpass_table or missing_fastpass
        if daily_base_rows or daily_buy_rows:
            days_with_rows += 1
        base_rows.extend(daily_base_rows)
        daily_buy_rows.sort(key=lambda row: str(row.get("ticker") or ""))
        buy_rows.extend(daily_buy_rows)
    return base_rows, buy_rows, missing_fastpass_table, len(trading_dates), days_with_rows, trading_dates


def detect_trigger_field(base_rows: Sequence[Dict[str, Any]]) -> str | None:
    if not base_rows:
        return None
    first_row = base_rows[0]
    for key in TRIGGER_FIELD_CANDIDATES:
        if key in first_row:
            return key
    return None


def detect_date_field(base_rows: Sequence[Dict[str, Any]]) -> str | None:
    if not base_rows:
        return None
    first_row = base_rows[0]
    for key in DATE_FIELD_CANDIDATES:
        if key in first_row:
            return key
    return None


def check_transactions_table_exists(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='rc_transactions_simu' LIMIT 1"
    ).fetchone()
    return row is not None


def resolve_buy_price(conn: sqlite3.Connection, buy_row: Dict[str, Any]) -> float | None:
    ticker = buy_row.get("ticker")
    entry_window_date = buy_row.get("entry_window_date")
    buy_date = buy_row.get("event_date") or buy_row.get("as_of_date")
    if not ticker or not entry_window_date or not buy_date:
        return None

    row = conn.execute(
        """
        SELECT entry_window_date, entry_window_exit_date, close_at_ew_start, close_at_ew_exit
        FROM rc_pipeline_episode
        WHERE ticker = ? AND entry_window_date = ?
        ORDER BY rowid DESC
        LIMIT 1
        """,
        (ticker, entry_window_date),
    ).fetchone()
    if row is None:
        return None

    episode_entry_date, episode_exit_date, close_at_ew_start, close_at_ew_exit = row
    if buy_date == episode_entry_date and close_at_ew_start is not None:
        return float(close_at_ew_start)
    if buy_date == episode_exit_date and close_at_ew_exit is not None:
        return float(close_at_ew_exit)
    if buy_row.get("to_state") == "PASS" and close_at_ew_exit is not None:
        return float(close_at_ew_exit)
    if close_at_ew_start is not None:
        return float(close_at_ew_start)
    return None


def build_transactions(
    conn: sqlite3.Connection,
    buy_rows: Sequence[Dict[str, Any]],
    market: str,
    run_id: str,
    created_at: str,
) -> List[Dict[str, Any]]:
    transactions: List[Dict[str, Any]] = []
    for buy_row in buy_rows:
        buy_date = str(buy_row.get("event_date") or buy_row.get("as_of_date") or "")
        buy_price = resolve_buy_price(conn, buy_row)
        if not buy_date or buy_price is None:
            continue
        transactions.append(
            {
                "ticker": str(buy_row["ticker"]),
                "market": market,
                "buy_date": buy_date,
                "buy_price": buy_price,
                "buy_qty": 1,
                "buy_rule_hit": buy_row.get("rule_hit"),
                "sell_date": None,
                "sell_price": None,
                "sell_qty": None,
                "sell_reason": None,
                "holding_trading_days": None,
                "run_id": run_id,
                "created_at": created_at,
            }
        )
    return transactions


def simulate_insert_counts(
    conn: sqlite3.Connection,
    transactions: Sequence[Dict[str, Any]],
    run_id: str,
    mode: str,
) -> Tuple[int, int]:
    existing_keys = set()
    if mode == "append":
        rows = conn.execute(
            "SELECT ticker, buy_date, run_id FROM rc_transactions_simu WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        existing_keys = {(str(t), str(d), str(r)) for t, d, r in rows}

    seen_keys = set(existing_keys)
    inserted = 0
    ignored = 0
    for tx in transactions:
        key = (tx["ticker"], tx["buy_date"], tx["run_id"])
        if key in seen_keys:
            ignored += 1
            continue
        seen_keys.add(key)
        inserted += 1
    return inserted, ignored


def write_transactions(
    conn: sqlite3.Connection,
    transactions: Sequence[Dict[str, Any]],
    run_id: str,
    mode: str,
) -> Tuple[int, int]:
    if mode == "replace-run":
        conn.execute("DELETE FROM rc_transactions_simu WHERE run_id = ?", (run_id,))

    inserted = 0
    ignored = 0
    for tx in transactions:
        before = conn.total_changes
        conn.execute(
            """
            INSERT OR IGNORE INTO rc_transactions_simu (
              ticker, market, buy_date, buy_price, buy_qty, buy_rule_hit,
              sell_date, sell_price, sell_qty, sell_reason, holding_trading_days,
              run_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tx["ticker"],
                tx["market"],
                tx["buy_date"],
                tx["buy_price"],
                tx["buy_qty"],
                tx["buy_rule_hit"],
                tx["sell_date"],
                tx["sell_price"],
                tx["sell_qty"],
                tx["sell_reason"],
                tx["holding_trading_days"],
                tx["run_id"],
                tx["created_at"],
            ),
        )
        if conn.total_changes > before:
            inserted += 1
        else:
            ignored += 1
    conn.commit()
    return inserted, ignored


def main() -> None:
    args = parse_args()

    try:
        market = normalize_market(args.market)
        validate_date_range(args.start_date, args.end_date)
    except ValueError as exc:
        summary(status="ERROR", message=str(exc))
        raise SystemExit(2)

    rc_db = Path(args.rc_db)
    created_at = args.created_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    config = MARKET_CONFIGS[market_config_key(market)]

    try:
        rules_config = load_buy_rules_config(market)
        validate_buy_rules_config(rules_config, market)
    except Exception:
        summary(status="ERROR", message="BUY_RULESET_LOAD_FAILED")
        raise SystemExit(2)

    ruleset_identity = resolve_ruleset_identity(market)
    run_id, canonical_json = (
        (args.run_id, None)
        if args.run_id
        else compute_run_id(market, args.start_date, args.end_date, ruleset_identity)
    )

    try:
        base_rows, buy_rows, missing_fastpass_table, days_total, days_with_rows, trading_dates = load_base_rows_for_range(
            rc_db, config, rules_config, args.start_date, args.end_date
        )
    except Exception:
        summary(status="ERROR", message="BASE_ROWS_LOAD_FAILED")
        raise SystemExit(2)

    if not trading_dates:
        summary(status="ERROR", message="NO_TRADING_DATES_IN_RANGE")
        raise SystemExit(2)

    if missing_fastpass_table:
        print("WARNING FASTPASS_TABLE_MISSING")

    summary(base_rows_total=len(base_rows))
    summary(days_with_rows=days_with_rows)
    summary(days_total=days_total)
    summary(trading_dates_min=trading_dates[0] if trading_dates else "")
    summary(trading_dates_max=trading_dates[-1] if trading_dates else "")
    trigger_field = detect_trigger_field(base_rows)
    summary(base_rows_has_trigger_field=1 if trigger_field is not None else 0)
    if trigger_field is not None:
        summary(base_rows_trigger_NEW_EW=sum(1 for row in base_rows if row.get(trigger_field) == "NEW_EW"))
        summary(base_rows_trigger_NEW_PASS=sum(1 for row in base_rows if row.get(trigger_field) == "NEW_PASS"))
        summary(
            base_rows_trigger_EW_SNAPSHOT=sum(1 for row in base_rows if row.get(trigger_field) == "EW_SNAPSHOT")
        )

    date_field = detect_date_field(base_rows)
    if date_field is not None:
        date_values = sorted(str(row.get(date_field)) for row in base_rows if row.get(date_field) is not None)
        if date_values:
            summary(base_rows_date_min=date_values[0])
            summary(base_rows_date_max=date_values[-1])

    summary(buy_rows_total=len(buy_rows))

    conn = sqlite3.connect(str(rc_db))
    try:
        if not check_transactions_table_exists(conn):
            summary(status="ERROR", message="RC_TRANSACTIONS_SIMU_MISSING")
            raise SystemExit(2)

        transactions = build_transactions(conn, buy_rows, market, run_id, created_at)
        buys_total = len(transactions)

        if args.dry_run:
            buys_inserted, buys_ignored = simulate_insert_counts(conn, transactions, run_id, args.mode)
            summary(status="OK")
            summary(rc_db=rc_db)
            summary(market=market)
            summary(start_date=args.start_date)
            summary(end_date=args.end_date)
            summary(run_id=run_id)
            summary(buys_total=buys_total)
            summary(buys_inserted=buys_inserted)
            summary(buys_ignored=buys_ignored)
            summary(mode=args.mode)
            summary(dry_run=1)
            print(canonical_json if canonical_json is not None else "")
            return

        buys_inserted, buys_ignored = write_transactions(conn, transactions, run_id, args.mode)
    finally:
        conn.close()

    summary(status="OK")
    summary(rc_db=rc_db)
    summary(market=market)
    summary(start_date=args.start_date)
    summary(end_date=args.end_date)
    summary(run_id=run_id)
    summary(buys_total=buys_total)
    summary(buys_inserted=buys_inserted)
    summary(buys_ignored=buys_ignored)
    summary(mode=args.mode)
    summary(dry_run=0)


if __name__ == "__main__":
    main()
