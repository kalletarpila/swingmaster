from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.providers.yahoo import YahooFinanceClient


DEFAULT_MARKET = "omxh"
DEFAULT_EXCHANGE = "HE"
DEFAULT_SYMBOLS = "NOKIA.HE"
DEFAULT_PROVIDER = "yahoo"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic Yahoo Finance Finland fundamentals raw audit")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--market", default=DEFAULT_MARKET, help="Market code")
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE, help="Exchange code for summary output")
    parser.add_argument("--symbols", default=None, help="Comma-separated Yahoo Finance symbols")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit after deterministic sorting")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and classify only without writing rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def normalize_symbols(symbols_arg: str | None) -> list[str]:
    if symbols_arg is None or not symbols_arg.strip():
        symbols = [DEFAULT_SYMBOLS]
    else:
        symbols = [symbol.strip() for symbol in symbols_arg.split(",") if symbol.strip()]
    return sorted(symbols)


def canonical_json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def compute_payload_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json_dumps(payload).encode("utf-8")).hexdigest()


def statement_payload_has_data(statement_payload: dict[str, Any]) -> bool:
    data = statement_payload.get("data")
    if not isinstance(data, list):
        return False
    return any(any(cell is not None for cell in row) for row in data if isinstance(row, list))


def payload_has_usable_statement_data(payload: dict[str, Any]) -> bool:
    for key in ("quarterly_income_stmt", "quarterly_balance_sheet", "quarterly_cashflow"):
        statement_payload = payload.get(key)
        if isinstance(statement_payload, dict) and statement_payload_has_data(statement_payload):
            return True
    return False


def build_audit_row(
    market: str,
    symbol: str,
    payload: dict[str, Any],
    status: str,
    error_message: str | None,
    loaded_at_utc: str,
    run_id: str,
) -> dict[str, Any]:
    combined_payload = {
        "info": payload.get("info", {}),
        "fast_info": payload.get("fast_info", {}),
        "quarterly_income_stmt": payload.get("quarterly_income_stmt", {"index": [], "columns": [], "data": []}),
        "quarterly_balance_sheet": payload.get("quarterly_balance_sheet", {"index": [], "columns": [], "data": []}),
        "quarterly_cashflow": payload.get("quarterly_cashflow", {"index": [], "columns": [], "data": []}),
    }
    return {
        "market": market,
        "provider": DEFAULT_PROVIDER,
        "symbol": symbol,
        "info_json": canonical_json_dumps(combined_payload["info"]),
        "fast_info_json": canonical_json_dumps(combined_payload["fast_info"]),
        "quarterly_income_stmt_json": canonical_json_dumps(combined_payload["quarterly_income_stmt"]),
        "quarterly_balance_sheet_json": canonical_json_dumps(combined_payload["quarterly_balance_sheet"]),
        "quarterly_cashflow_json": canonical_json_dumps(combined_payload["quarterly_cashflow"]),
        "payload_hash": compute_payload_hash(combined_payload),
        "status": status,
        "error_message": error_message,
        "loaded_at_utc": loaded_at_utc,
        "run_id": run_id,
    }


def insert_audit_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    conn.executemany(
        """
        INSERT INTO rc_fundamental_yahoo_raw (
            market,
            provider,
            symbol,
            info_json,
            fast_info_json,
            quarterly_income_stmt_json,
            quarterly_balance_sheet_json,
            quarterly_cashflow_json,
            payload_hash,
            status,
            error_message,
            loaded_at_utc,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["market"],
                row["provider"],
                row["symbol"],
                row["info_json"],
                row["fast_info_json"],
                row["quarterly_income_stmt_json"],
                row["quarterly_balance_sheet_json"],
                row["quarterly_cashflow_json"],
                row["payload_hash"],
                row["status"],
                row["error_message"],
                row["loaded_at_utc"],
                row["run_id"],
            )
            for row in rows
        ],
    )
    return len(rows)


def run_yahoo_audit(
    db_path: Path,
    market: str,
    exchange: str,
    symbols_arg: str | None,
    limit: int | None,
    run_id: str,
    dry_run: bool,
) -> dict[str, Any]:
    client = YahooFinanceClient()
    symbols = normalize_symbols(symbols_arg)
    symbols_total = len(symbols)
    if limit is not None:
        symbols = symbols[:limit]
    loaded_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows: list[dict[str, Any]] = []
    ok_count = 0
    empty_count = 0
    error_count = 0

    for symbol in symbols:
        try:
            payload = client.get_raw_payload(symbol)
            status = "OK" if payload_has_usable_statement_data(payload) else "EMPTY"
            if status == "OK":
                ok_count += 1
            else:
                empty_count += 1
            rows.append(
                build_audit_row(
                    market=market,
                    symbol=symbol,
                    payload=payload,
                    status=status,
                    error_message=None,
                    loaded_at_utc=loaded_at_utc,
                    run_id=run_id,
                )
            )
        except Exception as exc:
            error_count += 1
            rows.append(
                build_audit_row(
                    market=market,
                    symbol=symbol,
                    payload={},
                    status="ERROR",
                    error_message=str(exc),
                    loaded_at_utc=loaded_at_utc,
                    run_id=run_id,
                )
            )

    rows_written = 0
    if not dry_run:
        with sqlite3.connect(str(db_path)) as conn:
            rows_written = insert_audit_rows(conn, rows)
            conn.commit()

    return {
        "market": market,
        "exchange": exchange,
        "symbols_total": symbols_total,
        "symbols_processed": len(symbols),
        "ok_count": ok_count,
        "empty_count": empty_count,
        "error_count": error_count,
        "rows_written": rows_written,
        "dry_run": "true" if dry_run else "false",
        "run_id": run_id,
    }


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    summary = run_yahoo_audit(
        db_path=db_path,
        market=args.market,
        exchange=args.exchange,
        symbols_arg=args.symbols,
        limit=args.limit,
        run_id=args.run_id,
        dry_run=args.dry_run,
    )
    _summary(market=summary["market"])
    _summary(exchange=summary["exchange"])
    _summary(symbols_total=summary["symbols_total"])
    _summary(symbols_processed=summary["symbols_processed"])
    _summary(ok_count=summary["ok_count"])
    _summary(empty_count=summary["empty_count"])
    _summary(error_count=summary["error_count"])
    _summary(rows_written=summary["rows_written"])
    _summary(dry_run=summary["dry_run"])
    _summary(run_id=summary["run_id"])


if __name__ == "__main__":
    main()
