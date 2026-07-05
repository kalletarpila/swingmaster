from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.build_quarterly import build_quarterly_rows
from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS
from swingmaster.fundamentals.reported_sec_dual_write_adapter import (
    write_sec_reconstructed_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.sec_reconstruct_quarterly import (
    insert_reconstructed_quarterly_rows,
    load_sec_fact_rows,
    reconstruct_quarterly_rows,
)
from swingmaster.fundamentals.sec_reconstruction_provenance import (
    build_sec_contributing_facts_by_reconstructed_rows,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct SEC quarterly raw rows from stored sec_fact rows")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic run identifier")
    parser.add_argument("--retrieved-at-utc", required=True, help="Deterministic retrieved timestamp")
    parser.add_argument("--dry-run", action="store_true", help="Reconstruct without writing rows")
    parser.add_argument("--write-vintage", action="store_true", help="Opt in to latest/vintage/provenance writes")
    parser.add_argument("--vintage-market", help="Vintage market; required with --write-vintage")
    parser.add_argument("--vintage-available-at-utc", help="PIT availability timestamp; required with --write-vintage")
    parser.add_argument("--vintage-ingested-at-utc", help="Ingestion timestamp; required with --write-vintage")
    parser.add_argument("--vintage-run-id", help="Vintage write run id; required with --write-vintage")
    parser.add_argument(
        "--vintage-normalization-run-id",
        help="Optional normalization run id for vintage metadata and latest-compatible rows",
    )
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def run_sec_reconstruct_quarterly(
    db_path: Path,
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
    dry_run: bool,
    *,
    write_vintage: bool = False,
    vintage_market: str | None = None,
    vintage_available_at_utc: str | None = None,
    vintage_ingested_at_utc: str | None = None,
    vintage_run_id: str | None = None,
    vintage_normalization_run_id: str | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    normalized_ticker = ticker.upper()
    if write_vintage:
        _validate_vintage_args(
            vintage_market=vintage_market,
            vintage_available_at_utc=vintage_available_at_utc,
            vintage_ingested_at_utc=vintage_ingested_at_utc,
            vintage_run_id=vintage_run_id,
        )

    with sqlite3.connect(str(db_path)) as conn:
        sec_fact_rows = load_sec_fact_rows(conn, normalized_ticker)
        reconstructed_rows = reconstruct_quarterly_rows(sec_fact_rows, normalized_ticker, run_id, retrieved_at_utc)
        if not dry_run:
            insert_reconstructed_quarterly_rows(conn, reconstructed_rows)
            if write_vintage:
                normalization_run_id = vintage_normalization_run_id or run_id
                normalized_rows = build_quarterly_rows(reconstructed_rows, normalization_run_id)
                contributing_facts_by_key = build_sec_contributing_facts_by_reconstructed_rows(
                    reconstructed_rows=reconstructed_rows,
                    raw_fact_rows=sec_fact_rows,
                )
                _require_vintage_provenance_for_non_null_fields(
                    normalized_rows=normalized_rows,
                    contributing_facts_by_key=contributing_facts_by_key,
                )
                write_sec_reconstructed_quarterly_rows_with_optional_vintage(
                    conn,
                    normalized_rows=normalized_rows,
                    contributing_facts_by_key=contributing_facts_by_key,
                    write_vintage=True,
                    market=str(vintage_market),
                    available_at_utc=str(vintage_available_at_utc),
                    ingested_at_utc=str(vintage_ingested_at_utc),
                    run_id=str(vintage_run_id),
                    normalization_run_id=normalization_run_id,
                )
            conn.commit()
    return len(sec_fact_rows), reconstructed_rows


def _validate_vintage_args(
    *,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
) -> None:
    required_values = {
        "vintage_market": vintage_market,
        "vintage_available_at_utc": vintage_available_at_utc,
        "vintage_ingested_at_utc": vintage_ingested_at_utc,
        "vintage_run_id": vintage_run_id,
    }
    missing = [name for name, value in required_values.items() if value is None or not str(value).strip()]
    if missing:
        raise ValueError("SEC_RECONSTRUCT_CLI_VINTAGE_REQUIRED_FIELDS_MISSING:" + ",".join(missing))


def _require_vintage_provenance_for_non_null_fields(
    *,
    normalized_rows: list[dict[str, Any]],
    contributing_facts_by_key: dict[tuple[str, str], dict[str, list[dict[str, Any]]]],
) -> None:
    for row in normalized_rows:
        ticker = str(row["ticker"]).upper()
        period_end_date = str(row["period_end_date"])
        field_map = contributing_facts_by_key.get((ticker, period_end_date), {})
        for field_name in REPORTED_FINANCIAL_FIELDS:
            if row.get(field_name) is None:
                continue
            if not field_map.get(field_name):
                raise ValueError(
                    "SEC_RECONSTRUCT_CLI_VINTAGE_PROVENANCE_MISSING:"
                    f"{ticker},{period_end_date},{field_name}"
                )


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    write_vintage = bool(getattr(args, "write_vintage", False))
    sec_fact_rows_read, reconstructed_rows = run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker=args.ticker,
        run_id=args.run_id,
        retrieved_at_utc=args.retrieved_at_utc,
        dry_run=args.dry_run,
        write_vintage=write_vintage,
        vintage_market=getattr(args, "vintage_market", None),
        vintage_available_at_utc=getattr(args, "vintage_available_at_utc", None),
        vintage_ingested_at_utc=getattr(args, "vintage_ingested_at_utc", None),
        vintage_run_id=getattr(args, "vintage_run_id", None),
        vintage_normalization_run_id=getattr(args, "vintage_normalization_run_id", None),
    )
    periods = sorted({row["period_end_date"] for row in reconstructed_rows})
    _summary(ticker=args.ticker.upper())
    _summary(sec_fact_rows_read=sec_fact_rows_read)
    _summary(quarterly_rows_reconstructed=len(reconstructed_rows))
    _summary(output_periods=len(periods))
    _summary(income_rows=sum(1 for row in reconstructed_rows if row["statement_type"] == "income"))
    _summary(cashflow_rows=sum(1 for row in reconstructed_rows if row["statement_type"] == "cashflow"))
    _summary(balance_rows=sum(1 for row in reconstructed_rows if row["statement_type"] == "balance"))
    _summary(first_period=periods[0] if periods else "NULL")
    _summary(last_period=periods[-1] if periods else "NULL")
    _summary(db_path=str(db_path))
    _summary(run_id=args.run_id)
    _summary(source="sec_edgar")
    _summary(period_type="quarterly")
    if write_vintage:
        _summary(vintage_write="enabled")
    _summary(status="dry-run" if args.dry_run else "ok")


if __name__ == "__main__":
    main()
