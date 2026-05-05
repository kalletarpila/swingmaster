from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Populate usa_close_change.db from osakedata.db. "
            "Tickers and prices from osakedata (market=usa), "
            "sector/industry from ticker_meta."
        )
    )
    parser.add_argument("--osakedata", required=True, help="Path to osakedata.db")
    parser.add_argument("--db", required=True, help="Path to target usa_close_change.db")
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="replace: clear existing rows first, append: keep existing rows",
    )
    parser.add_argument("--batch-size", type=int, default=50000, help="Rows per INSERT batch")
    return parser.parse_args()


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS instruments (
            ticker   TEXT PRIMARY KEY,
            sector   TEXT,
            industry TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_change_daily (
            ticker       TEXT NOT NULL,
            trade_date   TEXT NOT NULL,
            close_change REAL NOT NULL,
            PRIMARY KEY (ticker, trade_date),
            FOREIGN KEY (ticker) REFERENCES instruments(ticker)
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_price_change_daily_date
        ON price_change_daily(trade_date);
        """
    )


def main() -> None:
    args = parse_args()
    osakedata_path = Path(args.osakedata)
    db_path = Path(args.db)

    if not osakedata_path.exists():
        raise SystemExit(f"ERROR: osakedata not found: {osakedata_path}")

    src = sqlite3.connect(f"file:{osakedata_path}?mode=ro", uri=True)
    src.execute("PRAGMA query_only=ON;")

    dst = sqlite3.connect(str(db_path))
    dst.execute("PRAGMA foreign_keys=ON;")

    try:
        _apply_pragmas(dst)
        _ensure_schema(dst)

        if args.mode == "replace":
            dst.execute("DELETE FROM price_change_daily;")
            dst.execute("DELETE FROM instruments;")
            dst.commit()

        # --- instruments ---
        # Haetaan kaikki usa-tickerit ticker_meta-taulusta.
        # sector/industry voivat olla NULL.
        instruments_rows = src.execute(
            """
            SELECT ticker, sector, industry
            FROM ticker_meta
            WHERE market = 'usa'
            ORDER BY ticker
            """
        ).fetchall()

        if not instruments_rows:
            raise SystemExit("ERROR: no usa tickers found in ticker_meta")

        dst.executemany(
            """
            INSERT INTO instruments(ticker, sector, industry) VALUES(?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                sector   = excluded.sector,
                industry = excluded.industry
            """,
            instruments_rows,
        )
        dst.commit()
        n_instruments = len(instruments_rows)

        # --- price_change_daily ---
        # Lasketaan close_change LAG-ikkunafunktiolla suoraan SQL:ssä.
        # close_change = ((close / LAG(close)) - 1) * 100
        # Ensimmäiselle päivälle per ticker LAG palauttaa NULL -> jätetään pois.
        # Rajataan vain tickereihin joilla market='usa' osakedata-taulussa.
        print("Fetching price changes (this may take a moment)...")
        cursor = src.execute(
            """
            SELECT
                o.osake AS ticker,
                o.pvm   AS trade_date,
                ((o.close / LAG(o.close) OVER (PARTITION BY o.osake ORDER BY o.pvm)) - 1.0) * 100.0
                    AS close_change
            FROM osakedata o
            INNER JOIN ticker_meta tm ON tm.ticker = o.osake AND tm.market = 'usa'
            WHERE o.market = 'usa'
              AND o.close IS NOT NULL
              AND o.close > 0
            ORDER BY o.osake, o.pvm
            """
        )

        insert_sql = (
            "INSERT OR REPLACE INTO price_change_daily(ticker, trade_date, close_change) "
            "VALUES(?, ?, ?)"
        )

        batch: list[tuple[str, str, float]] = []
        value_count = 0

        for row in cursor:
            ticker, trade_date, close_change = row
            if close_change is None:
                # Ensimmäinen päivä per ticker – ei edellistä päivää
                continue
            batch.append((ticker, trade_date, close_change))
            if len(batch) >= args.batch_size:
                dst.executemany(insert_sql, batch)
                dst.commit()
                value_count += len(batch)
                batch.clear()

        if batch:
            dst.executemany(insert_sql, batch)
            dst.commit()
            value_count += len(batch)

        # --- Validointi ---
        n_daily = dst.execute("SELECT COUNT(*) FROM price_change_daily").fetchone()[0]
        min_date, max_date = dst.execute(
            "SELECT MIN(trade_date), MAX(trade_date) FROM price_change_daily"
        ).fetchone()
        n_tickers_with_data = dst.execute(
            "SELECT COUNT(DISTINCT ticker) FROM price_change_daily"
        ).fetchone()[0]

        print(f"SUMMARY status=OK")
        print(f"SUMMARY osakedata={osakedata_path}")
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY instruments_written={n_instruments}")
        print(f"SUMMARY daily_rows_written={n_daily}")
        print(f"SUMMARY tickers_with_price_data={n_tickers_with_data}")
        print(f"SUMMARY min_date={min_date}")
        print(f"SUMMARY max_date={max_date}")

    except Exception:
        dst.rollback()
        raise
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    main()
