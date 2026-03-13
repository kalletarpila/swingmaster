from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path

from swingmaster import SWINGMASTER_MACRO_DB_PATH
from swingmaster.infra.sqlite.migrator import apply_macro_migrations
from swingmaster.macro.raw_ingest import ingest_macro_raw


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw macro source observations into macro SQLite")
    parser.add_argument(
        "--db-path",
        required=True,
        help=f"Macro SQLite database path (expected: {SWINGMASTER_MACRO_DB_PATH})",
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace-all", "insert-missing"],
        default="upsert",
        help="Write mode for raw macro table",
    )
    parser.add_argument("--computed-at", default=None, help="Optional ISO8601 timestamp")
    parser.add_argument(
        "--fred-api-key",
        default=None,
        help="Optional FRED API key (or set environment variable FRED_API_KEY)",
    )
    parser.add_argument(
        "--cboe-csv-url",
        default=None,
        help="Optional CBOE CSV URL override (or set environment variable CBOE_PCR_CSV_URL)",
    )
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def main() -> None:
    args = parse_args()
    fred_api_key = args.fred_api_key or os.getenv("FRED_API_KEY")
    if not fred_api_key:
        raise RuntimeError("FRED_API_KEY_MISSING")
    conn = sqlite3.connect(str(Path(args.db_path)))
    try:
        apply_macro_migrations(conn)
        summary = ingest_macro_raw(
            conn,
            date_from=args.start_date,
            date_to=args.end_date,
            mode=args.mode,
            computed_at=args.computed_at,
            fred_api_key=fred_api_key,
            cboe_csv_url=args.cboe_csv_url,
        )
    except sqlite3.Error as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    except RuntimeError as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    except ValueError as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    finally:
        conn.close()

    _summary(status=summary.summary_status)
    _summary(sources_requested=summary.sources_requested)
    _summary(date_from=summary.date_from)
    _summary(date_to=summary.date_to)
    _summary(mode=summary.mode)
    _summary(rows_inserted=summary.rows_inserted)
    _summary(rows_updated=summary.rows_updated)
    _summary(rows_deleted=summary.rows_deleted)
    _summary(rows_skipped=summary.rows_skipped)
    _summary(distinct_sources_loaded=summary.distinct_sources_loaded)
    _summary(run_id=summary.run_id)
    _summary(summary_status=summary.summary_status)


if __name__ == "__main__":
    main()
