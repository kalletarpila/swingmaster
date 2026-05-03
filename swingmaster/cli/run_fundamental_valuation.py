from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_MARKET = "omxh"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute deterministic Fundamental Valuation V1 using EV/EBIT")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--osakedata-db", required=True, help="OHLCV SQLite database path")
    parser.add_argument("--market", default=DEFAULT_MARKET, help="Market code for OHLCV close lookup and ticker universe")
    parser.add_argument("--as-of-date", required=True, help="Target date in YYYY-MM-DD format")
    parser.add_argument("--ticker", default=None, help="Optional single ticker override")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Compute without writing rows")
    parser.add_argument("--replace", action="store_true", help="Delete matching stored valuation rows before writing")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def resolve_created_at_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_market_ticker_universe(osakedata_conn: sqlite3.Connection, market: str) -> list[str]:
    rows = osakedata_conn.execute(
        """
        SELECT DISTINCT osake
        FROM osakedata
        WHERE market = ?
        ORDER BY osake
        """,
        (market,),
    ).fetchall()
    return [str(row[0]).upper() for row in rows]


def load_ttm_rows(
    fundamentals_conn: sqlite3.Connection,
    as_of_date: str,
    ticker: str | None,
    market_universe: list[str] | None,
) -> list[sqlite3.Row]:
    previous_row_factory = fundamentals_conn.row_factory
    fundamentals_conn.row_factory = sqlite3.Row
    try:
        if ticker is not None:
            rows = fundamentals_conn.execute(
                """
                SELECT ticker, as_of_date, latest_period_end_date, ebit_ttm, fundamental_score_lifecycle
                FROM rc_fundamental_ttm
                WHERE ticker = ?
                  AND as_of_date = ?
                ORDER BY ticker ASC
                """,
                (ticker.upper(), as_of_date),
            ).fetchall()
        else:
            if not market_universe:
                return []
            placeholders = ", ".join("?" for _ in market_universe)
            rows = fundamentals_conn.execute(
                f"""
                SELECT ticker, as_of_date, latest_period_end_date, ebit_ttm, fundamental_score_lifecycle
                FROM rc_fundamental_ttm
                WHERE as_of_date = ?
                  AND ticker IN ({placeholders})
                ORDER BY ticker ASC
                """,
                [as_of_date, *market_universe],
            ).fetchall()
    finally:
        fundamentals_conn.row_factory = previous_row_factory
    return rows


def load_quarterly_ev_inputs(
    fundamentals_conn: sqlite3.Connection,
    ticker: str,
    period_end_date: str,
) -> sqlite3.Row | None:
    previous_row_factory = fundamentals_conn.row_factory
    fundamentals_conn.row_factory = sqlite3.Row
    try:
        return fundamentals_conn.execute(
            """
            SELECT ticker, period_end_date, cash, total_debt, shares_outstanding
            FROM rc_fundamental_quarterly
            WHERE ticker = ?
              AND period_end_date = ?
            """,
            (ticker.upper(), period_end_date),
        ).fetchone()
    finally:
        fundamentals_conn.row_factory = previous_row_factory


def load_latest_close_price(
    osakedata_conn: sqlite3.Connection,
    ticker: str,
    market: str,
    as_of_date: str,
) -> float | None:
    row = osakedata_conn.execute(
        """
        SELECT close
        FROM osakedata
        WHERE osake = ?
          AND market = ?
          AND pvm <= ?
        ORDER BY pvm DESC
        LIMIT 1
        """,
        (ticker.upper(), market, as_of_date),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def build_valuation_row(
    ttm_row: sqlite3.Row,
    quarterly_row: sqlite3.Row | None,
    close_price: float | None,
    run_id: str,
    created_at_utc: str,
) -> dict[str, Any]:
    shares_outstanding = _coerce_optional_float(quarterly_row["shares_outstanding"]) if quarterly_row is not None else None
    cash = _coerce_optional_float(quarterly_row["cash"]) if quarterly_row is not None else None
    total_debt = _coerce_optional_float(quarterly_row["total_debt"]) if quarterly_row is not None else None
    ebit_ttm = _coerce_optional_float(ttm_row["ebit_ttm"])
    fundamental_score_lifecycle = _coerce_optional_float(ttm_row["fundamental_score_lifecycle"])

    market_cap: float | None = None
    if close_price is not None and shares_outstanding is not None and shares_outstanding > 0:
        market_cap = close_price * shares_outstanding

    enterprise_value: float | None = None
    if market_cap is not None and total_debt is not None and cash is not None:
        enterprise_value = market_cap + total_debt - cash

    valuation_ev_ebit: float | None = None
    if enterprise_value is not None and ebit_ttm is not None and ebit_ttm > 0:
        valuation_ev_ebit = enterprise_value / ebit_ttm

    if close_price is None:
        valuation_status = "MISSING_PRICE"
        valuation_bucket = "INVALID"
    elif shares_outstanding is None or shares_outstanding <= 0:
        valuation_status = "MISSING_SHARES"
        valuation_bucket = "INVALID"
    elif ebit_ttm is None or ebit_ttm <= 0:
        valuation_status = "INVALID_EBIT"
        valuation_bucket = "INVALID"
    elif total_debt is None or cash is None:
        valuation_status = "MISSING_EV_INPUT"
        valuation_bucket = "INVALID"
    else:
        valuation_status = "OK"
        expensive_threshold = 25.0 if fundamental_score_lifecycle is not None and fundamental_score_lifecycle >= 60 else 18.0
        if valuation_ev_ebit is None:
            valuation_bucket = "INVALID"
            valuation_status = "INVALID_EBIT"
        elif valuation_ev_ebit < 10:
            valuation_bucket = "CHEAP"
        elif valuation_ev_ebit < expensive_threshold:
            valuation_bucket = "FAIR"
        else:
            valuation_bucket = "EXPENSIVE"

    return {
        "ticker": str(ttm_row["ticker"]).upper(),
        "as_of_date": str(ttm_row["as_of_date"]),
        "valuation_ev_ebit": valuation_ev_ebit,
        "valuation_bucket": valuation_bucket,
        "valuation_status": valuation_status,
        "market_cap": market_cap,
        "enterprise_value": enterprise_value,
        "close_price": close_price,
        "shares_outstanding": shares_outstanding,
        "cash": cash,
        "total_debt": total_debt,
        "ebit_ttm": ebit_ttm,
        "fundamental_score_lifecycle": fundamental_score_lifecycle,
        "run_id": run_id,
        "created_at_utc": created_at_utc,
    }


def delete_existing_rows(
    conn: sqlite3.Connection,
    as_of_date: str,
    ticker: str | None,
) -> int:
    if ticker is not None:
        cursor = conn.execute(
            """
            DELETE FROM rc_fundamental_valuation
            WHERE ticker = ?
              AND as_of_date = ?
            """,
            (ticker.upper(), as_of_date),
        )
        return int(cursor.rowcount)
    cursor = conn.execute(
        """
        DELETE FROM rc_fundamental_valuation
        WHERE as_of_date = ?
        """,
        (as_of_date,),
    )
    return int(cursor.rowcount)


def insert_valuation_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    conn.executemany(
        """
        INSERT OR REPLACE INTO rc_fundamental_valuation (
            ticker,
            as_of_date,
            valuation_ev_ebit,
            valuation_bucket,
            valuation_status,
            market_cap,
            enterprise_value,
            close_price,
            shares_outstanding,
            cash,
            total_debt,
            ebit_ttm,
            fundamental_score_lifecycle,
            run_id,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["as_of_date"],
                row["valuation_ev_ebit"],
                row["valuation_bucket"],
                row["valuation_status"],
                row["market_cap"],
                row["enterprise_value"],
                row["close_price"],
                row["shares_outstanding"],
                row["cash"],
                row["total_debt"],
                row["ebit_ttm"],
                row["fundamental_score_lifecycle"],
                row["run_id"],
                row["created_at_utc"],
            )
            for row in rows
        ],
    )
    return len(rows)


def run_fundamental_valuation(
    db_path: Path,
    osakedata_db_path: Path,
    market: str,
    as_of_date: str,
    ticker: str | None,
    run_id: str,
    dry_run: bool,
    replace: bool,
) -> dict[str, Any]:
    created_at_utc = resolve_created_at_utc()
    with sqlite3.connect(str(osakedata_db_path)) as osakedata_conn:
        market_universe = None if ticker is not None else load_market_ticker_universe(osakedata_conn, market)
        with sqlite3.connect(str(db_path)) as fundamentals_conn:
            ttm_rows = load_ttm_rows(fundamentals_conn, as_of_date, ticker, market_universe)
            valuation_rows = []
            for ttm_row in ttm_rows:
                quarterly_row = load_quarterly_ev_inputs(
                    fundamentals_conn,
                    str(ttm_row["ticker"]),
                    str(ttm_row["latest_period_end_date"]),
                )
                close_price = load_latest_close_price(osakedata_conn, str(ttm_row["ticker"]), market, as_of_date)
                valuation_rows.append(
                    build_valuation_row(
                        ttm_row=ttm_row,
                        quarterly_row=quarterly_row,
                        close_price=close_price,
                        run_id=run_id,
                        created_at_utc=created_at_utc,
                    )
                )

    rows_written = 0
    if not dry_run:
        with sqlite3.connect(str(db_path)) as conn:
            if replace:
                delete_existing_rows(conn, as_of_date, ticker)
            rows_written = insert_valuation_rows(conn, valuation_rows)
            conn.commit()

    ok_count = sum(1 for row in valuation_rows if row["valuation_status"] == "OK")
    invalid_count = len(valuation_rows) - ok_count
    cheap_count = sum(1 for row in valuation_rows if row["valuation_bucket"] == "CHEAP")
    fair_count = sum(1 for row in valuation_rows if row["valuation_bucket"] == "FAIR")
    expensive_count = sum(1 for row in valuation_rows if row["valuation_bucket"] == "EXPENSIVE")

    return {
        "market": market,
        "as_of_date": as_of_date,
        "tickers_processed": len(ttm_rows),
        "rows_written": rows_written,
        "ok_count": ok_count,
        "invalid_count": invalid_count,
        "cheap_count": cheap_count,
        "fair_count": fair_count,
        "expensive_count": expensive_count,
        "dry_run": "true" if dry_run else "false",
        "replace": "true" if replace else "false",
        "run_id": run_id,
    }


def main() -> None:
    args = parse_args()
    summary = run_fundamental_valuation(
        db_path=resolve_db_path(args.db),
        osakedata_db_path=resolve_db_path(args.osakedata_db),
        market=args.market,
        as_of_date=args.as_of_date,
        ticker=args.ticker,
        run_id=args.run_id,
        dry_run=args.dry_run,
        replace=args.replace,
    )
    _summary(market=summary["market"])
    _summary(as_of_date=summary["as_of_date"])
    _summary(tickers_processed=summary["tickers_processed"])
    _summary(rows_written=summary["rows_written"])
    _summary(ok_count=summary["ok_count"])
    _summary(invalid_count=summary["invalid_count"])
    _summary(cheap_count=summary["cheap_count"])
    _summary(fair_count=summary["fair_count"])
    _summary(expensive_count=summary["expensive_count"])
    _summary(dry_run=summary["dry_run"])
    _summary(replace=summary["replace"])
    _summary(run_id=summary["run_id"])


if __name__ == "__main__":
    main()
