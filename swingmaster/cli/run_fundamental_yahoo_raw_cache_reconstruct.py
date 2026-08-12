from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_yahoo_quarterly_prototype import build_normalized_rows, should_persist_row
from swingmaster.cli.run_fundamental_yahoo_quarterly_write import build_persist_rows, insert_rows


SUPPORTED_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
    "shares_source",
    "shares_quality",
)

RECONSTRUCT_REASON_OK = "OK"
RECONSTRUCT_REASON_NO_MAPPED_VALUE_AT_TARGET_PERIOD = "NO_MAPPED_STATEMENT_VALUE_AT_TARGET_PERIOD"
RECONSTRUCT_REASON_TARGET_PERIOD_NOT_FOUND = "TARGET_PERIOD_NOT_FOUND"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct one Yahoo quarterly cache row from persisted Yahoo raw")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--market", required=True, help="Market code")
    parser.add_argument("--symbol", required=True, help="Ticker symbol")
    parser.add_argument("--period-end-date", required=True, help="Historical Yahoo source period to reconstruct")
    parser.add_argument("--run-id", required=True, help="Deterministic reconstruction run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Preview only without writing rc_fundamental_yahoo_quarterly")
    parser.add_argument("--artifact-dir", help="Optional directory for raw_lineage.csv and dry_run_preview.json")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def resolve_artifact_dir(artifact_dir_arg: str | None) -> Path | None:
    if artifact_dir_arg is None or not artifact_dir_arg.strip():
        return None
    path = Path(artifact_dir_arg).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_yahoo_raw_rows(conn: sqlite3.Connection, market: str, symbol: str) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM rc_fundamental_yahoo_raw
            WHERE market = ?
              AND symbol = ?
              AND provider = 'yahoo'
              AND status = 'OK'
            ORDER BY loaded_at_utc DESC, id DESC
            """,
            (market.lower(), symbol.upper()),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return rows


def _row_for_period(raw_row: sqlite3.Row, period_end_date: str) -> dict[str, Any] | None:
    rows = build_normalized_rows(raw_row)
    for row in rows:
        if str(row["period_end_date"]) == period_end_date and should_persist_row(row):
            return row
    return None


def _analyze_row_for_period(raw_row: sqlite3.Row, period_end_date: str) -> dict[str, Any]:
    rows = build_normalized_rows(raw_row)
    for row in rows:
        if str(row["period_end_date"]) == period_end_date:
            return {
                "row": row,
                "period_marker_present": True,
                "persistable": should_persist_row(row),
                "reason": RECONSTRUCT_REASON_OK
                if should_persist_row(row)
                else RECONSTRUCT_REASON_NO_MAPPED_VALUE_AT_TARGET_PERIOD,
            }
    return {
        "row": None,
        "period_marker_present": False,
        "persistable": False,
        "reason": RECONSTRUCT_REASON_TARGET_PERIOD_NOT_FOUND,
    }


def build_raw_lineage(raw_rows: list[sqlite3.Row], period_end_date: str) -> list[dict[str, Any]]:
    lineage: list[dict[str, Any]] = []
    for raw_row in raw_rows:
        parse_status = "PARSED"
        target_row: dict[str, Any] | None = None
        period_marker_present = False
        persistable = False
        reason = ""
        error_message = ""
        try:
            analysis = _analyze_row_for_period(raw_row, period_end_date)
            target_row = analysis["row"]
            period_marker_present = bool(analysis["period_marker_present"])
            persistable = bool(analysis["persistable"])
            reason = str(analysis["reason"])
        except Exception as exc:
            parse_status = "PARSE_FAILED"
            error_message = str(exc)
        output: dict[str, Any] = {
            "raw_id": raw_row["id"],
            "market": raw_row["market"],
            "symbol": raw_row["symbol"],
            "run_id": raw_row["run_id"],
            "loaded_at_utc": raw_row["loaded_at_utc"],
            "target_period_end_date": period_end_date,
            "period_present": int(period_marker_present),
            "persistable": int(persistable),
            "reason": reason,
            "parse_status": parse_status,
            "error_message": error_message,
        }
        for field_name in SUPPORTED_FIELDS:
            output[field_name] = "" if target_row is None or target_row.get(field_name) is None else target_row[field_name]
        lineage.append(output)
    return lineage


def select_historical_snapshot(raw_rows: list[sqlite3.Row], period_end_date: str) -> tuple[sqlite3.Row, dict[str, Any]]:
    period_marker_present = False
    for raw_row in raw_rows:
        analysis = _analyze_row_for_period(raw_row, period_end_date)
        if analysis["period_marker_present"]:
            period_marker_present = True
        if analysis["persistable"]:
            return raw_row, analysis["row"]
    if period_marker_present:
        raise RuntimeError(f"YAHOO_RAW_CACHE_RECONSTRUCT_{RECONSTRUCT_REASON_NO_MAPPED_VALUE_AT_TARGET_PERIOD}:{period_end_date}")
    raise RuntimeError(f"YAHOO_RAW_CACHE_RECONSTRUCT_TARGET_PERIOD_NOT_FOUND:{period_end_date}")


def _lineage_fieldnames() -> list[str]:
    return [
        "raw_id",
        "market",
        "symbol",
        "run_id",
        "loaded_at_utc",
        "target_period_end_date",
        "period_present",
        "persistable",
        "reason",
        "parse_status",
        "error_message",
        *SUPPORTED_FIELDS,
    ]


def write_raw_lineage(path: Path, lineage: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_lineage_fieldnames())
        writer.writeheader()
        writer.writerows(lineage)


def write_preview(path: Path, preview: dict[str, Any]) -> None:
    path.write_text(json.dumps(preview, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def run_yahoo_raw_cache_reconstruct(
    *,
    db_path: Path,
    market: str,
    symbol: str,
    period_end_date: str,
    run_id: str,
    dry_run: bool,
    artifact_dir: Path | None = None,
) -> dict[str, Any]:
    normalized_market = market.lower()
    normalized_symbol = symbol.upper()
    created_at_utc = _utc_now()
    with sqlite3.connect(str(db_path)) as conn:
        raw_rows = load_yahoo_raw_rows(conn, normalized_market, normalized_symbol)
        lineage = build_raw_lineage(raw_rows, period_end_date)
        try:
            selected_raw_row, normalized_row = select_historical_snapshot(raw_rows, period_end_date)
        except RuntimeError as exc:
            reason = str(exc).split(":", 1)[0].removeprefix("YAHOO_RAW_CACHE_RECONSTRUCT_")
            preview = {
                "market": normalized_market,
                "symbol": normalized_symbol,
                "period_end_date": period_end_date,
                "selected_raw_id": None,
                "selected_source_run_id": None,
                "selected_loaded_at_utc": None,
                "selection_rule": "NEWEST_PARSEABLE_RAW_SNAPSHOT_CONTAINING_PERIOD",
                "dry_run": dry_run,
                "rows_written": 0,
                "row": None,
                "status": "NOT_RECONSTRUCTABLE",
                "reason": reason,
                "lineage": lineage,
            }
            if dry_run:
                if artifact_dir is not None:
                    write_raw_lineage(artifact_dir / "raw_lineage.csv", lineage)
                    write_preview(artifact_dir / "dry_run_preview.json", preview)
                return preview
            raise
        persist_rows = build_persist_rows(
            market=normalized_market,
            symbol=normalized_symbol,
            source_run_id=str(selected_raw_row["run_id"]),
            run_id=run_id,
            created_at_utc=created_at_utc,
            normalized_rows=[normalized_row],
        )
        if len(persist_rows) != 1:
            raise RuntimeError(f"YAHOO_RAW_CACHE_RECONSTRUCT_TARGET_ROW_NOT_PERSISTABLE:{normalized_symbol},{period_end_date}")
        rows_written = 0
        if not dry_run:
            rows_written = insert_rows(conn, persist_rows)
            conn.commit()

    preview = {
        "market": normalized_market,
        "symbol": normalized_symbol,
        "period_end_date": period_end_date,
        "selected_raw_id": int(selected_raw_row["id"]),
        "selected_source_run_id": str(selected_raw_row["run_id"]),
        "selected_loaded_at_utc": str(selected_raw_row["loaded_at_utc"]),
        "selection_rule": "NEWEST_PARSEABLE_RAW_SNAPSHOT_CONTAINING_PERIOD",
        "dry_run": dry_run,
        "rows_written": rows_written,
        "row": persist_rows[0],
        "status": "RECONSTRUCTED",
        "reason": RECONSTRUCT_REASON_OK,
    }
    if artifact_dir is not None:
        write_raw_lineage(artifact_dir / "raw_lineage.csv", lineage)
        write_preview(artifact_dir / "dry_run_preview.json", preview)
    return preview


def main() -> None:
    args = parse_args()
    result = run_yahoo_raw_cache_reconstruct(
        db_path=resolve_db_path(args.db),
        market=args.market,
        symbol=args.symbol,
        period_end_date=args.period_end_date,
        run_id=args.run_id,
        dry_run=bool(args.dry_run),
        artifact_dir=resolve_artifact_dir(args.artifact_dir),
    )
    _summary(market=result["market"])
    _summary(symbol=result["symbol"])
    _summary(period_end_date=result["period_end_date"])
    _summary(selected_source_run_id=result["selected_source_run_id"])
    _summary(selected_loaded_at_utc=result["selected_loaded_at_utc"])
    _summary(rows_written=result["rows_written"])
    _summary(dry_run="true" if result["dry_run"] else "false")
    _summary(run_id=args.run_id)


if __name__ == "__main__":
    main()
