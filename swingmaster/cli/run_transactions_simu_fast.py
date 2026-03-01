from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from swingmaster.cli.daily_report import apply_buy_rules, load_buy_rules_config


ENGINE_VERSION = "SIMU_TX_FAST_V1"
ROOT = Path(__file__).resolve().parents[2]
BUY_RULES_DIR = ROOT / "daily_reports" / "buy_rules"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast BUY simulation from NEW_EW and NEW_PASS transitions")
    parser.add_argument("--rc-db", required=True, help="RC SQLite database path")
    parser.add_argument("--market", required=True, help="Market code (FIN|SE|USA)")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--mode", choices=["append", "replace-run"], default="append")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to rc_transactions_simu")
    parser.add_argument("--run-id", default=None, help="Optional explicit run_id")
    parser.add_argument("--created-at", default=None, help="Optional ISO8601 created_at timestamp")
    return parser.parse_args()


def summary(**items: Any) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def normalize_market(value: str) -> str:
    market = value.upper()
    if market not in {"FIN", "SE", "USA"}:
        raise ValueError("INVALID_MARKET")
    return market


def validate_date_range(start_date: str, end_date: str) -> None:
    try:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
    except ValueError as exc:
        raise ValueError("INVALID_DATE_RANGE") from exc
    if start > end:
        raise ValueError("INVALID_DATE_RANGE")


def ruleset_identity_for_market(market: str) -> str:
    return str((BUY_RULES_DIR / f"{market.lower()}.json").resolve())


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


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    if not table_exists(conn, table_name):
        return False
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row[1]) == column_name for row in rows)


def inspect_schema(conn: sqlite3.Connection) -> Dict[str, bool]:
    has_state_daily = table_exists(conn, "rc_state_daily")
    has_ew_score_daily = table_exists(conn, "rc_ew_score_daily")
    has_pipeline_episode = table_exists(conn, "rc_pipeline_episode")
    return {
        "has_state_daily": has_state_daily,
        "has_ew_score_daily": has_ew_score_daily,
        "has_pipeline_episode": has_pipeline_episode,
        "has_days_in_current_episode": column_exists(conn, "rc_state_daily", "days_in_current_episode"),
        "has_days_in_stabilizing_before_ew": column_exists(conn, "rc_state_daily", "days_in_stabilizing_before_ew"),
        "has_close_at_ew_start": column_exists(conn, "rc_pipeline_episode", "close_at_ew_start"),
        "has_close_at_ew_exit": column_exists(conn, "rc_pipeline_episode", "close_at_ew_exit"),
        "has_entry_window_exit_date": column_exists(conn, "rc_pipeline_episode", "entry_window_exit_date"),
        "has_entry_window_exit_state": column_exists(conn, "rc_pipeline_episode", "entry_window_exit_state"),
    }


def build_shared_sql_parts(schema: Dict[str, bool]) -> Dict[str, str]:
    return {
        "days_current_expr": (
            "sd.days_in_current_episode AS days_in_current_episode"
            if schema["has_state_daily"] and schema["has_days_in_current_episode"]
            else "NULL AS days_in_current_episode"
        ),
        "days_stab_expr": (
            "sd.days_in_stabilizing_before_ew AS days_in_stabilizing_before_ew"
            if schema["has_state_daily"] and schema["has_days_in_stabilizing_before_ew"]
            else "NULL AS days_in_stabilizing_before_ew"
        ),
        "state_daily_join": (
            "LEFT JOIN rc_state_daily sd ON sd.ticker = t.ticker AND sd.date = t.date"
            if schema["has_state_daily"]
            else ""
        ),
        "fastpass_score_expr": (
            "es.ew_score_fastpass AS ew_score_fastpass"
            if schema["has_ew_score_daily"]
            else "NULL AS ew_score_fastpass"
        ),
        "fastpass_level_expr": (
            "es.ew_level_fastpass AS ew_level_fastpass"
            if schema["has_ew_score_daily"]
            else "NULL AS ew_level_fastpass"
        ),
        "rolling_level_expr": (
            "es.ew_level_rolling AS ew_level_rolling"
            if schema["has_ew_score_daily"]
            else "NULL AS ew_level_rolling"
        ),
    }


def fetch_new_ew_candidates(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    schema: Dict[str, bool],
) -> List[Dict[str, Any]]:
    parts = build_shared_sql_parts(schema)
    buy_price_expr = (
        "ep.close_at_ew_start AS buy_price"
        if schema["has_pipeline_episode"] and schema["has_close_at_ew_start"]
        else "NULL AS buy_price"
    )
    ew_score_join = (
        "LEFT JOIN rc_ew_score_daily es ON es.ticker = t.ticker AND es.date = t.date"
        if schema["has_ew_score_daily"]
        else ""
    )
    episode_join = (
        """
        LEFT JOIN rc_pipeline_episode ep
          ON ep.rowid = (
            SELECT MAX(ep2.rowid)
            FROM rc_pipeline_episode ep2
            WHERE ep2.ticker = t.ticker
              AND ep2.entry_window_date = t.date
          )
        """
        if schema["has_pipeline_episode"]
        else ""
    )

    sql = f"""
    SELECT
      'NEW_EW' AS section,
      t.ticker AS ticker,
      t.date AS event_date,
      COALESCE(ep.entry_window_date, t.date) AS entry_window_date,
      ep.downtrend_entry_date AS downtrend_entry_date,
      t.from_state AS from_state,
      t.to_state AS to_state,
      {parts['fastpass_score_expr']},
      {parts['fastpass_level_expr']},
      NULL AS ew_score_rolling,
      {parts['rolling_level_expr']},
      {parts['days_current_expr']},
      {parts['days_stab_expr']},
      {buy_price_expr}
    FROM rc_transition t
    {ew_score_join}
    {episode_join}
    {parts['state_daily_join']}
    WHERE t.to_state = 'ENTRY_WINDOW'
      AND t.date >= ?
      AND t.date <= ?
    ORDER BY t.date ASC, t.ticker ASC
    """
    cursor = conn.execute(sql, (start_date, end_date))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def fetch_new_pass_candidates(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    schema: Dict[str, bool],
) -> List[Dict[str, Any]]:
    parts = build_shared_sql_parts(schema)
    buy_price_expr = (
        "ep.close_at_ew_exit AS buy_price"
        if schema["has_pipeline_episode"] and schema["has_close_at_ew_exit"]
        else "NULL AS buy_price"
    )
    if schema["has_ew_score_daily"]:
        fastpass_score_expr = "fp.ew_score_fastpass AS ew_score_fastpass"
        fastpass_level_expr = "fp.ew_level_fastpass AS ew_level_fastpass"
        rolling_score_expr = "roll.ew_score_rolling AS ew_score_rolling"
        rolling_level_expr = "roll.ew_level_rolling AS ew_level_rolling"
        fastpass_join = "LEFT JOIN rc_ew_score_daily fp ON fp.ticker = t.ticker AND fp.date = ep.entry_window_date"
        rolling_join = """
        LEFT JOIN rc_ew_score_daily roll
          ON roll.rowid = (
            SELECT r2.rowid
            FROM rc_ew_score_daily r2
            WHERE r2.ticker = t.ticker
              AND r2.date >= ep.entry_window_date
              AND r2.date < t.date
            ORDER BY r2.date DESC, r2.rowid DESC
            LIMIT 1
          )
        """
    else:
        fastpass_score_expr = "NULL AS ew_score_fastpass"
        fastpass_level_expr = "NULL AS ew_level_fastpass"
        rolling_score_expr = "NULL AS ew_score_rolling"
        rolling_level_expr = "NULL AS ew_level_rolling"
        fastpass_join = ""
        rolling_join = ""

    if schema["has_entry_window_exit_date"]:
        episode_join = """
        LEFT JOIN rc_pipeline_episode ep
          ON ep.rowid = (
            SELECT MAX(ep2.rowid)
            FROM rc_pipeline_episode ep2
            WHERE ep2.ticker = t.ticker
              AND ep2.entry_window_exit_date = t.date
          )
        """
    elif schema["has_entry_window_exit_state"]:
        episode_join = """
        LEFT JOIN rc_pipeline_episode ep
          ON ep.rowid = (
            SELECT MAX(ep2.rowid)
            FROM rc_pipeline_episode ep2
            WHERE ep2.ticker = t.ticker
              AND ep2.entry_window_exit_state = 'PASS'
              AND ep2.entry_window_date <= t.date
          )
        """
    else:
        episode_join = ""

    sql = f"""
    SELECT
      'NEW_PASS' AS section,
      t.ticker AS ticker,
      t.date AS event_date,
      ep.entry_window_date AS entry_window_date,
      ep.downtrend_entry_date AS downtrend_entry_date,
      t.from_state AS from_state,
      t.to_state AS to_state,
      {fastpass_score_expr},
      {fastpass_level_expr},
      {rolling_score_expr},
      {rolling_level_expr},
      {parts['days_current_expr']},
      {parts['days_stab_expr']},
      {buy_price_expr}
    FROM rc_transition t
    {episode_join}
    {fastpass_join}
    {rolling_join}
    {parts['state_daily_join']}
    WHERE t.to_state = 'PASS'
      AND t.date >= ?
      AND t.date <= ?
    ORDER BY t.date ASC, t.ticker ASC
    """
    cursor = conn.execute(sql, (start_date, end_date))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def fetch_trading_day_map(
    conn: sqlite3.Connection,
    candidate_rows: List[Dict[str, Any]],
    has_state_daily: bool,
) -> Tuple[Dict[str, Dict[str, int]], int]:
    if not has_state_daily:
        return {}, 0

    tickers = sorted({str(row["ticker"]) for row in candidate_rows if row.get("ticker")})
    all_dates = sorted(
        {
            str(date_value)
            for row in candidate_rows
            for date_value in (
                row.get("downtrend_entry_date"),
                row.get("entry_window_date"),
                row.get("event_date"),
            )
            if date_value
        }
    )
    if not tickers or not all_dates:
        return {}, 0

    placeholders = ",".join("?" for _ in tickers)
    sql = f"""
    SELECT ticker, date
    FROM rc_state_daily
    WHERE ticker IN ({placeholders})
      AND date >= ?
      AND date <= ?
    ORDER BY ticker ASC, date ASC
    """
    params = [*tickers, all_dates[0], all_dates[-1]]
    rows = conn.execute(sql, params).fetchall()

    date_to_idx: Dict[str, Dict[str, int]] = {}
    current_ticker = None
    current_idx = 0
    for ticker, trading_date in rows:
        ticker_str = str(ticker)
        date_str = str(trading_date)
        if ticker_str != current_ticker:
            current_ticker = ticker_str
            current_idx = 0
            date_to_idx[ticker_str] = {}
        date_to_idx[ticker_str][date_str] = current_idx
        current_idx += 1
    return date_to_idx, len(rows)


def fill_trading_day_fields(
    candidate_rows: List[Dict[str, Any]],
    date_to_idx: Dict[str, Dict[str, int]],
) -> Tuple[int, int]:
    filled_current = 0
    filled_stabilizing = 0
    for row in candidate_rows:
        ticker = row.get("ticker")
        ticker_dates = date_to_idx.get(str(ticker), {})
        downtrend_entry_date = row.get("downtrend_entry_date")
        entry_window_date = row.get("entry_window_date")
        event_date = row.get("event_date")

        if row.get("days_in_current_episode") is None:
            if downtrend_entry_date and event_date:
                idx_downtrend = ticker_dates.get(str(downtrend_entry_date))
                idx_event = ticker_dates.get(str(event_date))
                if idx_downtrend is not None and idx_event is not None:
                    row["days_in_current_episode"] = max(0, idx_event - idx_downtrend)
                    filled_current += 1

        if row.get("days_in_stabilizing_before_ew") is None:
            if downtrend_entry_date and entry_window_date:
                idx_downtrend = ticker_dates.get(str(downtrend_entry_date))
                idx_entry = ticker_dates.get(str(entry_window_date))
                if idx_downtrend is not None and idx_entry is not None:
                    row["days_in_stabilizing_before_ew"] = max(0, idx_entry - idx_downtrend)
                    filled_stabilizing += 1

    return filled_current, filled_stabilizing


def build_transactions(
    buy_rows: List[Dict[str, Any]],
    market: str,
    run_id: str,
    created_at: str,
) -> Tuple[List[Tuple[Any, ...]], int]:
    transactions: List[Tuple[Any, ...]] = []
    skipped_null_price = 0
    for row in buy_rows:
        buy_price = row.get("buy_price")
        if buy_price is None:
            skipped_null_price += 1
            continue
        transactions.append(
            (
                row["ticker"],
                market,
                row["event_date"],
                buy_price,
                1,
                row.get("rule_hit"),
                None,
                None,
                None,
                None,
                None,
                run_id,
                created_at,
            )
        )
    return transactions, skipped_null_price


def insert_transactions(
    conn: sqlite3.Connection,
    transactions: List[Tuple[Any, ...]],
    run_id: str,
    mode: str,
) -> Tuple[int, int]:
    if mode == "replace-run":
        conn.execute("DELETE FROM rc_transactions_simu WHERE run_id = ?", (run_id,))

    inserted = 0
    ignored = 0
    sql = """
    INSERT OR IGNORE INTO rc_transactions_simu (
      ticker, market, buy_date, buy_price, buy_qty, buy_rule_hit,
      sell_date, sell_price, sell_qty, sell_reason, holding_trading_days,
      run_id, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for tx in transactions:
        before = conn.total_changes
        conn.execute(sql, tx)
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

    ruleset_identity = ruleset_identity_for_market(market)
    run_id, canonical_json = (
        (args.run_id, None)
        if args.run_id
        else compute_run_id(market, args.start_date, args.end_date, ruleset_identity)
    )
    created_at = args.created_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    try:
        rules_config = load_buy_rules_config(market)
    except Exception:
        summary(status="ERROR", message="BUY_RULESET_LOAD_FAILED")
        raise SystemExit(2)

    conn = sqlite3.connect(str(Path(args.rc_db)))
    try:
        if not table_exists(conn, "rc_transition"):
            summary(status="ERROR", message="RC_TRANSITION_MISSING")
            raise SystemExit(2)
        if not table_exists(conn, "rc_pipeline_episode"):
            summary(status="ERROR", message="RC_PIPELINE_EPISODE_MISSING")
            raise SystemExit(2)

        schema = inspect_schema(conn)
        if not schema["has_state_daily"]:
            print("WARNING RC_STATE_DAILY_MISSING")
        if not schema["has_ew_score_daily"]:
            print("WARNING FASTPASS_TABLE_MISSING")

        new_ew_candidates = fetch_new_ew_candidates(conn, args.start_date, args.end_date, schema)
        new_pass_candidates = fetch_new_pass_candidates(conn, args.start_date, args.end_date, schema)
        candidate_rows = sorted(
            new_ew_candidates + new_pass_candidates,
            key=lambda row: (
                str(row.get("event_date") or ""),
                str(row.get("ticker") or ""),
                str(row.get("section") or ""),
            ),
        )

        date_to_idx, trading_days_map_rows = fetch_trading_day_map(conn, candidate_rows, schema["has_state_daily"])
        days_filled_current_episode, days_filled_stabilizing_before_ew = fill_trading_day_fields(
            candidate_rows, date_to_idx
        )

        buy_rows, missing_field_count = apply_buy_rules(candidate_rows, rules_config, buy_section_name="BUYS")
        buy_rows.sort(
            key=lambda row: (
                str(row.get("event_date") or ""),
                str(row.get("ticker") or ""),
                str(row.get("rule_hit") or ""),
            )
        )
        transactions, skipped_null_price = build_transactions(buy_rows, market, run_id, created_at)

        if not table_exists(conn, "rc_transactions_simu"):
            summary(status="ERROR", message="RC_TRANSACTIONS_SIMU_MISSING")
            raise SystemExit(2)

        if args.dry_run:
            buys_inserted = 0
            buys_ignored = 0
        else:
            buys_inserted, buys_ignored = insert_transactions(conn, transactions, run_id, args.mode)
    except sqlite3.Error as exc:
        summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    finally:
        conn.close()

    summary(status="OK")
    summary(rc_db=args.rc_db)
    summary(market=market)
    summary(start_date=args.start_date)
    summary(end_date=args.end_date)
    summary(run_id=run_id)
    summary(engine_version=ENGINE_VERSION)
    summary(ruleset_identity=ruleset_identity)
    summary(new_ew_candidates=len(new_ew_candidates))
    summary(new_pass_candidates=len(new_pass_candidates))
    summary(candidate_rows_total=len(candidate_rows))
    summary(buy_rows_total=len(buy_rows))
    summary(missing_field_count=missing_field_count)
    summary(days_filled_current_episode=days_filled_current_episode)
    summary(days_filled_stabilizing_before_ew=days_filled_stabilizing_before_ew)
    summary(trading_days_map_tickers=len(date_to_idx))
    summary(trading_days_map_rows=trading_days_map_rows)
    summary(
        null_days_in_current_episode=sum(1 for row in candidate_rows if row.get("days_in_current_episode") is None)
    )
    summary(
        null_days_in_stabilizing_before_ew=sum(
            1 for row in candidate_rows if row.get("days_in_stabilizing_before_ew") is None
        )
    )
    summary(skipped_null_price=skipped_null_price)
    summary(buys_inserted=buys_inserted)
    summary(buys_ignored=buys_ignored)
    summary(mode=args.mode)
    summary(dry_run=1 if args.dry_run else 0)
    print(canonical_json if canonical_json is not None else "")


if __name__ == "__main__":
    main()

# Smoke example:
# PYTHONPATH=. python3 swingmaster/cli/run_transactions_simu_fast.py \
#   --rc-db /home/kalle/projects/swingmaster/swingmaster_rc_usa_2024_2025.db \
#   --market USA \
#   --start-date 2026-02-15 \
#   --end-date 2026-02-26 \
#   --mode replace-run
