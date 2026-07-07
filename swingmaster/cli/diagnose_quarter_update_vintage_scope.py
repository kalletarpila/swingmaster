from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_quarter_update import (
    build_quarter_update_vintage_post_run_guard_summary,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose quarter_update Yahoo-aware vintage planner scope read-only")
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--enrich-run-id", required=True)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--sample-limit", type=int, default=20)
    return parser.parse_args(argv)


def _connect_read_only(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.expanduser().resolve()
    conn = sqlite3.connect(f"file:{resolved.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _base_run_id(source_run_id: str) -> str:
    normalized = source_run_id.strip()
    if normalized.endswith("__QUARTERLY__QUARTERLY"):
        return normalized.removesuffix("__QUARTERLY__QUARTERLY")
    if normalized.endswith("__QUARTERLY"):
        return normalized.removesuffix("__QUARTERLY")
    return normalized


def _vintage_run_id(source_run_id: str) -> str:
    return f"{_base_run_id(source_run_id)}__SEC_LATEST_WRITER_VINTAGE"


def _count(conn: sqlite3.Connection, sql: str, params: tuple[object, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0)


def _resolve_source_run_id(conn: sqlite3.Connection, source_run_id: str) -> tuple[str, str]:
    requested = source_run_id.strip()
    if _count(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly WHERE run_id = ?", (requested,)) > 0:
        return requested, "requested"
    fallback = f"{requested}__QUARTERLY"
    if _count(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly WHERE run_id = ?", (fallback,)) > 0:
        return fallback, "appended_quarterly_suffix"
    return requested, "requested_no_rows"


def _overall_parity(conn: sqlite3.Connection, market: str) -> dict[str, int]:
    normalized_market = market.strip().lower()
    return {
        "latest_count": _count(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly"),
        "vintage_count": _count(
            conn,
            "SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage WHERE market = ?",
            (normalized_market,),
        ),
        "provenance_count": _count(
            conn,
            "SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance WHERE market = ?",
            (normalized_market,),
        ),
        "latest_without_vintage_count": _count(
            conn,
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarterly AS latest
            LEFT JOIN rc_fundamental_quarterly_vintage AS vintage
              ON vintage.market = ?
             AND vintage.ticker = latest.ticker
             AND vintage.period_end_date = latest.period_end_date
            WHERE vintage.ticker IS NULL
            """,
            (normalized_market,),
        ),
        "vintage_without_latest_count": _count(
            conn,
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarterly_vintage AS vintage
            LEFT JOIN rc_fundamental_quarterly AS latest
              ON latest.ticker = vintage.ticker
             AND latest.period_end_date = vintage.period_end_date
            WHERE vintage.market = ?
              AND latest.ticker IS NULL
            """,
            (normalized_market,),
        ),
        "duplicate_statement_vintage_id_count": _count(
            conn,
            """
            SELECT COUNT(*)
            FROM (
                SELECT statement_vintage_id
                FROM rc_fundamental_quarterly_vintage
                WHERE market = ?
                GROUP BY statement_vintage_id
                HAVING COUNT(*) > 1
            )
            """,
            (normalized_market,),
        ),
    }


def _bounded_csv(value: object, sample_limit: int) -> tuple[int, str]:
    text = str(value or "").strip()
    if not text:
        return 0, ""
    parts = [part for part in text.split(",") if part]
    return len(parts), ",".join(parts[:sample_limit])


def _status(parity: dict[str, int], guard: dict[str, object]) -> str:
    if (
        parity["latest_without_vintage_count"] > 0
        or parity["vintage_without_latest_count"] > 0
        or parity["duplicate_statement_vintage_id_count"] > 0
    ):
        return "PARITY_DRIFT"
    mismatch_count = int(guard.get("vintage_post_run_value_mismatch_count") or 0)
    if mismatch_count == 0:
        return "NO_MISMATCH"
    scope_count = int(guard.get("vintage_yahoo_aware_planner_scope_count") or 0)
    blocked_rows = int(guard.get("vintage_yahoo_aware_blocked_rows") or 0)
    completion_status = str(guard.get("vintage_completion_status") or "")
    unexplained = int(guard.get("vintage_post_run_unexplained_mismatch_count") or 0)
    if completion_status == "BLOCKED_POST_RUN_DRIFT" and unexplained > 0 and scope_count == 0 and blocked_rows < 100:
        return "SCOPE_FIX_VERIFIED_BLOCKED_NARROW"
    if scope_count > 100 or blocked_rows >= 100:
        return "SCOPE_FIX_FAILED_STILL_BROAD"
    return "UNKNOWN"


def _exit_code(summary: dict[str, Any]) -> int:
    status = str(summary.get("overall_diagnostic_status") or "")
    if status == "NO_MISMATCH":
        return 0
    if status == "SCOPE_FIX_VERIFIED_BLOCKED_NARROW":
        return 0
    return 1


def run_diagnostic(
    *,
    fundamentals_db: Path,
    market: str,
    source_run_id: str,
    enrich_run_id: str,
    sample_limit: int,
) -> dict[str, Any]:
    normalized_market = market.strip().lower()
    with _connect_read_only(fundamentals_db) as conn:
        resolved_source_run_id, source_run_id_resolution = _resolve_source_run_id(conn, source_run_id)
        vintage_run_id = _vintage_run_id(resolved_source_run_id)
        parity = _overall_parity(conn, normalized_market)
        guard = build_quarter_update_vintage_post_run_guard_summary(
            conn,
            market=normalized_market,
            source_run_id=resolved_source_run_id,
            vintage_run_id=vintage_run_id,
            enrich_run_id=enrich_run_id.strip(),
            enrich_summary=None,
            available_at_utc="2026-07-07T00:00:00Z",
            ingested_at_utc="2026-07-07T00:00:00Z",
        )
    unknown_count, unknown_sample = _bounded_csv(guard.get("vintage_yahoo_aware_unknown_provenance_fields"), sample_limit)
    summary: dict[str, Any] = {
        "market": normalized_market,
        "source_run_id_requested": source_run_id.strip(),
        "source_run_id_used": resolved_source_run_id,
        "source_run_id_resolution": source_run_id_resolution,
        "enrich_run_id": enrich_run_id.strip(),
        "vintage_run_id_used": vintage_run_id,
        **parity,
        "value_mismatch_count": guard.get("vintage_post_run_value_mismatch_count"),
        "yahoo_explained_mismatch_count": guard.get("vintage_post_run_yahoo_explained_mismatch_count"),
        "unexplained_mismatch_count": guard.get("vintage_post_run_unexplained_mismatch_count"),
        "value_mismatch_sample": guard.get("vintage_post_run_value_mismatch_sample"),
        "unexplained_mismatch_sample": guard.get("vintage_post_run_unexplained_mismatch_sample"),
        "completion_status": guard.get("vintage_completion_status"),
        "completion_reason": guard.get("vintage_completion_reason"),
        "next_action": guard.get("vintage_next_required_action"),
        "planner_status": guard.get("vintage_yahoo_aware_planning_status"),
        "planner_scope_count": guard.get("vintage_yahoo_aware_planner_scope_count"),
        "planner_scope_source": guard.get("vintage_yahoo_aware_planner_scope_source"),
        "planner_blocked_rows": guard.get("vintage_yahoo_aware_blocked_rows"),
        "unknown_provenance_field_sample_count": unknown_count,
        "unknown_provenance_field_sample": unknown_sample,
        "yahoo_aware_execution_status": "NOT_REQUESTED",
    }
    summary["overall_diagnostic_status"] = _status(parity, guard)
    return summary


def _print_text(summary: dict[str, Any]) -> None:
    for key, value in summary.items():
        print(f"SUMMARY {key}={value}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = run_diagnostic(
            fundamentals_db=Path(args.fundamentals_db),
            market=args.market,
            source_run_id=args.source_run_id,
            enrich_run_id=args.enrich_run_id,
            sample_limit=args.sample_limit,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(json.dumps({"summary": summary}, indent=2, sort_keys=True))
    else:
        _print_text(summary)
    return _exit_code(summary)


if __name__ == "__main__":
    raise SystemExit(main())
