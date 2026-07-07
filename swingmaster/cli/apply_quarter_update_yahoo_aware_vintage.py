from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from swingmaster.cli.run_fundamental_quarter_update import (
    _build_yahoo_aware_execution_candidates,
    _latest_rows_for_run_ids,
    _sec_provenance_by_key,
    _yahoo_audit_rows_by_key,
    _yahoo_rows_by_key,
    build_quarter_update_vintage_post_run_guard_summary,
    execute_quarter_update_yahoo_aware_vintage_plan,
)

APPROVAL_TOKEN = "USER_APPROVES_YAHOO_AWARE_VINTAGE_APPLY"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply planned quarter_update Yahoo-aware vintage corrections")
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--vintage-run-id", required=True)
    parser.add_argument("--available-at-utc", required=True)
    parser.add_argument("--ingested-at-utc", required=True)
    parser.add_argument("--approval-token", default="")
    return parser.parse_args(argv)


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def _base_run_id_from_quarterly(source_run_id: str) -> str:
    if source_run_id.endswith("__QUARTERLY"):
        return source_run_id.removesuffix("__QUARTERLY")
    return source_run_id


def _sec_latest_writer_vintage_run_id(source_run_id: str) -> str:
    return f"{_base_run_id_from_quarterly(source_run_id)}__SEC_LATEST_WRITER_VINTAGE"


def _enrich_run_id(source_run_id: str) -> str:
    return f"{_base_run_id_from_quarterly(source_run_id)}__ENRICH"


def run_apply_quarter_update_yahoo_aware_vintage(
    *,
    fundamentals_db: Path,
    market: str,
    source_run_id: str,
    vintage_run_id: str,
    available_at_utc: str,
    ingested_at_utc: str,
    approval_token: str,
) -> dict[str, object]:
    db_path = fundamentals_db.expanduser().resolve()
    normalized_market = market.strip().lower()
    normalized_source_run_id = source_run_id.strip()
    normalized_vintage_run_id = vintage_run_id.strip()
    enrich_run_id = _enrich_run_id(normalized_source_run_id)
    sec_vintage_run_id = _sec_latest_writer_vintage_run_id(normalized_source_run_id)

    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        plan = build_quarter_update_vintage_post_run_guard_summary(
            conn,
            market=normalized_market,
            source_run_id=normalized_source_run_id,
            vintage_run_id=sec_vintage_run_id,
            enrich_run_id=enrich_run_id,
            enrich_summary=None,
            available_at_utc=available_at_utc,
            ingested_at_utc=ingested_at_utc,
        )
        latest_rows = [dict(row) for row in _latest_rows_for_run_ids(conn, [normalized_source_run_id, enrich_run_id])]
        final_mixed_candidates, yahoo_candidates = _build_yahoo_aware_execution_candidates(
            plan=plan,
            latest_rows=latest_rows,
            sec_provenance_by_key=_sec_provenance_by_key(conn, sec_vintage_run_id),
            yahoo_audit_rows_by_key=_yahoo_audit_rows_by_key(conn, enrich_run_id),
            yahoo_rows_by_key=_yahoo_rows_by_key(conn, normalized_market),
            vintage_run_id=normalized_vintage_run_id,
        )
        if approval_token != APPROVAL_TOKEN:
            execution = {
                "vintage_yahoo_aware_execution_status": "APPROVAL_REQUIRED",
                "vintage_yahoo_aware_final_mixed_rows_written": 0,
                "vintage_yahoo_aware_yahoo_vintage_rows_written": 0,
                "vintage_yahoo_aware_provenance_rows_written": 0,
                "vintage_yahoo_aware_rows_blocked": 0,
                "vintage_yahoo_aware_rows_skipped": 0,
                "vintage_yahoo_aware_error": "APPROVAL_TOKEN_REQUIRED",
            }
        else:
            execution = execute_quarter_update_yahoo_aware_vintage_plan(
                conn,
                plan=plan,
                final_mixed_candidates_by_key=final_mixed_candidates,
                yahoo_vintage_candidates_by_key=yahoo_candidates,
                market=normalized_market,
                available_at_utc=available_at_utc,
                ingested_at_utc=ingested_at_utc,
                vintage_run_id=normalized_vintage_run_id,
            )
            conn.commit()
    return {
        "market": normalized_market,
        "source_run_id": normalized_source_run_id,
        "vintage_run_id": normalized_vintage_run_id,
        **plan,
        **execution,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = run_apply_quarter_update_yahoo_aware_vintage(
            fundamentals_db=Path(args.fundamentals_db),
            market=args.market,
            source_run_id=args.source_run_id,
            vintage_run_id=args.vintage_run_id,
            available_at_utc=args.available_at_utc,
            ingested_at_utc=args.ingested_at_utc,
            approval_token=args.approval_token,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    _summary(**summary)
    status = str(summary.get("vintage_yahoo_aware_execution_status") or "")
    return 0 if status in {"EXECUTION_COMPLETED", "NO_ACTION_REQUIRED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
