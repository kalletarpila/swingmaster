from __future__ import annotations

import argparse
import json
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
    parser.add_argument("--expected-final-mixed-count", type=int, default=None)
    parser.add_argument("--expected-yahoo-vintage-count", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--format", choices=("text", "json"), default="text")
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


def _as_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _has_unknown_provenance(value: object) -> bool:
    text = str(value or "").strip()
    return bool(text) and text.lower() not in {"0", "none", "[]", "{}"}


def _recovery_status_from_plan(plan: dict[str, object]) -> str:
    planning_status = str(plan.get("vintage_yahoo_aware_planning_status") or "")
    planned_final = _as_int(plan.get("vintage_planned_final_mixed_rows"))
    planned_yahoo = _as_int(plan.get("vintage_planned_yahoo_vintage_rows"))
    planned_provenance = _as_int(plan.get("vintage_planned_yahoo_aware_provenance_rows"))
    blocked_rows = _as_int(plan.get("vintage_yahoo_aware_blocked_rows"))
    if planning_status == "NO_ACTION_REQUIRED" and planned_final == 0 and planned_yahoo == 0:
        return "YAHOO_AWARE_RECOVERY_NOOP"
    if planning_status not in {"FINAL_MIXED_PLAN_READY", "YAHOO_VINTAGE_PLAN_READY"}:
        return "YAHOO_AWARE_RECOVERY_BLOCKED"
    if blocked_rows > 0 or _has_unknown_provenance(plan.get("vintage_yahoo_aware_unknown_provenance_fields")):
        return "YAHOO_AWARE_RECOVERY_BLOCKED"
    if planned_final + planned_yahoo <= 0 or planned_provenance <= 0:
        return "YAHOO_AWARE_RECOVERY_BLOCKED"
    return "YAHOO_AWARE_RECOVERY_READY"


def _expected_count_error(
    *,
    plan: dict[str, object],
    expected_final_mixed_count: int | None,
    expected_yahoo_vintage_count: int | None,
) -> str:
    planned_final = _as_int(plan.get("vintage_planned_final_mixed_rows"))
    planned_yahoo = _as_int(plan.get("vintage_planned_yahoo_vintage_rows"))
    if expected_final_mixed_count is not None and planned_final != expected_final_mixed_count:
        return f"EXPECTED_FINAL_MIXED_COUNT_MISMATCH:expected={expected_final_mixed_count}:actual={planned_final}"
    if expected_yahoo_vintage_count is not None and planned_yahoo != expected_yahoo_vintage_count:
        return f"EXPECTED_YAHOO_VINTAGE_COUNT_MISMATCH:expected={expected_yahoo_vintage_count}:actual={planned_yahoo}"
    return ""


def run_apply_quarter_update_yahoo_aware_vintage(
    *,
    fundamentals_db: Path,
    market: str,
    source_run_id: str,
    vintage_run_id: str,
    available_at_utc: str,
    ingested_at_utc: str,
    approval_token: str,
    dry_run: bool = False,
    expected_final_mixed_count: int | None = None,
    expected_yahoo_vintage_count: int | None = None,
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
        recovery_status = _recovery_status_from_plan(plan)
        expected_error = _expected_count_error(
            plan=plan,
            expected_final_mixed_count=expected_final_mixed_count,
            expected_yahoo_vintage_count=expected_yahoo_vintage_count,
        )
        if dry_run:
            execution = {
                "vintage_yahoo_aware_execution_status": "DRY_RUN_READY"
                if recovery_status == "YAHOO_AWARE_RECOVERY_READY"
                else "DRY_RUN_BLOCKED",
                "vintage_yahoo_aware_final_mixed_rows_written": 0,
                "vintage_yahoo_aware_yahoo_vintage_rows_written": 0,
                "vintage_yahoo_aware_provenance_rows_written": 0,
                "vintage_yahoo_aware_rows_blocked": plan.get("vintage_yahoo_aware_blocked_rows", 0),
                "vintage_yahoo_aware_rows_skipped": 0,
                "vintage_yahoo_aware_error": ""
                if recovery_status == "YAHOO_AWARE_RECOVERY_READY"
                else str(plan.get("vintage_yahoo_aware_block_reason") or recovery_status),
            }
        elif expected_error:
            execution = {
                "vintage_yahoo_aware_execution_status": "EXECUTION_BLOCKED",
                "vintage_yahoo_aware_final_mixed_rows_written": 0,
                "vintage_yahoo_aware_yahoo_vintage_rows_written": 0,
                "vintage_yahoo_aware_provenance_rows_written": 0,
                "vintage_yahoo_aware_rows_blocked": 0,
                "vintage_yahoo_aware_rows_skipped": 0,
                "vintage_yahoo_aware_error": expected_error,
            }
        elif approval_token != APPROVAL_TOKEN:
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
        "overall_status": recovery_status,
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
            dry_run=args.dry_run,
            expected_final_mixed_count=args.expected_final_mixed_count,
            expected_yahoo_vintage_count=args.expected_yahoo_vintage_count,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(json.dumps({"summary": summary}, indent=2, sort_keys=True))
    else:
        _summary(**summary)
    status = str(summary.get("vintage_yahoo_aware_execution_status") or "")
    if args.dry_run:
        return 0 if status == "DRY_RUN_READY" else 1
    return 0 if status in {"EXECUTION_COMPLETED", "NO_ACTION_REQUIRED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
