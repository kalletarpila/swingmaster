from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import wide USA close-change CSV into normalized SQLite tables"
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Input CSV path (expected delimiter ';' and decimal comma values)",
    )
    parser.add_argument("--db", required=True, help="Output SQLite database path")
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="replace: clear previous rows first, append: keep existing rows",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Rows per bulk insert batch",
    )
    return parser.parse_args()


def _batched(rows: list[tuple[str, str, float]], batch_size: int) -> Iterable[list[tuple[str, str, float]]]:
    for i in range(0, len(rows), batch_size):
        yield rows[i : i + batch_size]


def _to_float_or_none(raw: str) -> float | None:
    value = raw.strip()
    if not value:
        return None
    return float(value.replace(",", "."))


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS instruments (
            ticker TEXT PRIMARY KEY,
            sector TEXT,
            industry TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_change_daily (
            ticker TEXT NOT NULL,
            trade_date TEXT NOT NULL,
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


def _clear_existing(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM price_change_daily;")
    conn.execute("DELETE FROM instruments;")


def _detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t,")
        return dialect.delimiter
    except csv.Error:
        return ";"


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv)
    db_path = Path(args.db)

    if not csv_path.exists():
        raise SystemExit(f"ERROR: csv file not found: {csv_path}")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON;")

    try:
        _apply_pragmas(conn)
        _create_schema(conn)
        if args.mode == "replace":
            _clear_existing(conn)

        with csv_path.open("r", encoding="utf-8", newline="") as f:
            sample = f.read(20000)
            f.seek(0)
            delimiter = _detect_delimiter(sample)
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader, None)
            if not header or len(header) < 4:
                raise SystemExit("ERROR: invalid CSV header")

            fixed = header[:3]
            if [c.strip().lower() for c in fixed] != ["ticker", "sector", "industry"]:
                raise SystemExit("ERROR: CSV must start with columns: ticker;sector;industry")

            date_columns = header[3:]
            insert_instruments_sql = (
                "INSERT INTO instruments(ticker, sector, industry) VALUES(?, ?, ?) "
                "ON CONFLICT(ticker) DO UPDATE SET "
                "sector=excluded.sector, industry=excluded.industry"
            )
            insert_values_sql = (
                "INSERT OR REPLACE INTO price_change_daily(ticker, trade_date, close_change) "
                "VALUES(?, ?, ?)"
            )

            row_count = 0
            value_count = 0
            values_buffer: list[tuple[str, str, float]] = []

            for row in reader:
                if len(row) != len(header):
                    raise SystemExit(
                        f"ERROR: malformed row {row_count + 2}: expected {len(header)} columns, got {len(row)}"
                    )

                ticker = row[0].strip()
                sector = row[1].strip() or None
                industry = row[2].strip() or None
                if not ticker:
                    raise SystemExit(f"ERROR: missing ticker at CSV row {row_count + 2}")

                conn.execute(insert_instruments_sql, (ticker, sector, industry))

                for idx, raw_value in enumerate(row[3:]):
                    parsed = _to_float_or_none(raw_value)
                    if parsed is None:
                        continue
                    values_buffer.append((ticker, date_columns[idx], parsed))

                if len(values_buffer) >= args.batch_size:
                    conn.executemany(insert_values_sql, values_buffer)
                    value_count += len(values_buffer)
                    values_buffer.clear()

                row_count += 1

            if values_buffer:
                for batch in _batched(values_buffer, args.batch_size):
                    conn.executemany(insert_values_sql, batch)
                    value_count += len(batch)

        conn.commit()

        n_tickers = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        n_values = conn.execute("SELECT COUNT(*) FROM price_change_daily").fetchone()[0]
        min_date, max_date = conn.execute(
            "SELECT MIN(trade_date), MAX(trade_date) FROM price_change_daily"
        ).fetchone()

        print(f"SUMMARY status=OK")
        print(f"SUMMARY csv={csv_path}")
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY rows_read={row_count}")
        print(f"SUMMARY values_written={value_count}")
        print(f"SUMMARY instruments={n_tickers}")
        print(f"SUMMARY daily_rows={n_values}")
        print(f"SUMMARY min_date={min_date}")
        print(f"SUMMARY max_date={max_date}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
