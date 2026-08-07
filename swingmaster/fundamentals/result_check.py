from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from argparse import Namespace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.cli import apply_yahoo_earnings_events, refresh_yahoo_earnings_calendar, rebuild_earnings_event_matches
from swingmaster.fundamentals.earnings_events import DEFAULT_SAFETY_MARGIN_DAYS, normalize_ticker, repository_root
from swingmaster.fundamentals.earnings_event_matching import DEFAULT_MAX_REPORTING_DELAY_DAYS
from swingmaster.fundamentals.quarter_refresh_decision import (
    DECISION_FETCH_NEW_QUARTER,
    DECISION_RETRY_FETCH_FAILED,
    DECISION_RETRY_PARTIAL_QUARTER,
    DEFAULT_OHLCV_DB_PATH,
    DEFAULT_OHLCV_STALE_DAYS,
    PRIORITY_P4_REVIEW,
    build_quarter_refresh_decisions,
    open_readonly_db,
    summarize_quarter_refresh_decisions,
    validate_temp_path,
)


PLAN_VERSION = "fundamental_result_check_plan_v1"
CHECK_STATUS_SUCCESS = "SUCCESS"
CHECK_STATUS_PARTIAL = "PARTIAL"
CHECK_STATUS_FAILED = "FAILED"
EXECUTABLE_DECISIONS = {
    DECISION_FETCH_NEW_QUARTER,
    DECISION_RETRY_PARTIAL_QUARTER,
    DECISION_RETRY_FETCH_FAILED,
}


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def temp_root() -> Path:
    return repository_root() / "temp"


def default_output_root() -> Path:
    return temp_root() / "fundamental_result_check" / utc_timestamp()


def run_manual_result_check(
    *,
    fundamentals_db: Path,
    ohlcv_db: Path = DEFAULT_OHLCV_DB_PATH,
    decision_date: str | date,
    ohlcv_stale_days: int = DEFAULT_OHLCV_STALE_DAYS,
    event_watch_days_after: int = 5,
    output_root: Path | None = None,
    tickers: list[str] | None = None,
) -> dict[str, Any]:
    root = validate_temp_path(output_root or default_output_root())
    root.mkdir(parents=True, exist_ok=True)
    normalized_tickers = sorted(dict.fromkeys(normalize_ticker(ticker) for ticker in (tickers or []))) or None
    parsed_decision_date = _parse_date(decision_date)
    created_at_utc = utc_now_text()
    stages: list[dict[str, Any]] = []

    try:
        initial_rows = _decision_rows(
            fundamentals_db=fundamentals_db,
            ohlcv_db=ohlcv_db,
            decision_date=parsed_decision_date,
            ohlcv_stale_days=ohlcv_stale_days,
            tickers=normalized_tickers,
        )
        active_tickers = [row.ticker for row in initial_rows if row.fundamental_fetch_enabled == 1]
        stages.append(_stage("ohlcv_activity", CHECK_STATUS_SUCCESS, active_tickers=len(active_tickers), total_tickers=len(initial_rows)))
    except Exception as exc:
        return _write_failed_plan(
            root=root,
            fundamentals_db=fundamentals_db,
            ohlcv_db=ohlcv_db,
            decision_date=parsed_decision_date,
            ohlcv_stale_days=ohlcv_stale_days,
            created_at_utc=created_at_utc,
            stages=[_stage("ohlcv_activity", CHECK_STATUS_FAILED, error=str(exc))],
        )

    calendar_summary = _run_calendar_refresh(root, fundamentals_db, active_tickers)
    stages.append(calendar_summary["stage"])
    if calendar_summary["stage"]["status"] == CHECK_STATUS_FAILED:
        return _write_failed_plan(
            root=root,
            fundamentals_db=fundamentals_db,
            ohlcv_db=ohlcv_db,
            decision_date=parsed_decision_date,
            ohlcv_stale_days=ohlcv_stale_days,
            created_at_utc=created_at_utc,
            stages=stages,
            calendar_refresh_summary=calendar_summary,
        )

    event_candidates = select_completed_event_refresh_candidates(
        fundamentals_db=fundamentals_db,
        tickers=active_tickers,
        decision_date=parsed_decision_date,
        event_watch_days_after=event_watch_days_after,
    )
    stages.append(_stage("completed_event_candidate_selection", CHECK_STATUS_SUCCESS, selected_tickers=len(event_candidates)))

    event_summary = _run_completed_event_refresh(root, fundamentals_db, event_candidates)
    stages.append(event_summary["stage"])
    match_summary = _run_match_rebuild(root, fundamentals_db, enabled=bool(event_candidates) and event_summary["stage"]["status"] == CHECK_STATUS_SUCCESS)
    stages.append(match_summary["stage"])

    final_rows = _decision_rows(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date=parsed_decision_date,
        ohlcv_stale_days=ohlcv_stale_days,
        tickers=normalized_tickers,
    )
    stages.append(_stage("quarter_refresh_decisions", CHECK_STATUS_SUCCESS, total_tickers=len(final_rows)))

    check_status = _overall_status(stages)
    if check_status == CHECK_STATUS_PARTIAL:
        executable_rows: list[dict[str, Any]] = []
    else:
        executable_rows = [_plan_row(row) for row in final_rows if _row_is_executable(row)]
    manual_review_rows = [_plan_row(row) for row in final_rows if row.decision_priority == PRIORITY_P4_REVIEW]
    plan = _build_plan(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date=parsed_decision_date,
        ohlcv_stale_days=ohlcv_stale_days,
        created_at_utc=created_at_utc,
        check_status=check_status,
        candidate_rows=executable_rows,
        stages=stages,
    )
    summary_payload = {
        **summarize_quarter_refresh_decisions(final_rows),
        "check_status": check_status,
        "created_at_utc": created_at_utc,
        "candidate_count": len(executable_rows),
        "candidate_hash": plan["candidate_hash"],
        "completed_event_refresh_candidate_count": len(event_candidates),
        "output_root": str(root),
    }
    paths = _write_artifacts(
        root=root,
        plan=plan,
        candidate_rows=executable_rows,
        manual_review_rows=manual_review_rows,
        summary=summary_payload,
        calendar_refresh_summary=calendar_summary,
        completed_event_refresh_summary=event_summary,
    )
    return {"check_status": check_status, "plan": plan, "artifact_paths": paths, "stages": stages, "summary": summary_payload}


def select_completed_event_refresh_candidates(
    *,
    fundamentals_db: Path,
    tickers: list[str],
    decision_date: date,
    event_watch_days_after: int,
) -> list[str]:
    if not tickers:
        return []
    selected = set(tickers)
    earliest = decision_date - timedelta(days=max(event_watch_days_after, 0))
    with sqlite3.connect(f"file:{fundamentals_db.resolve().as_posix()}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            """
            SELECT ticker, calendar_status, estimated_announcement_date
            FROM rc_earnings_calendar
            WHERE market = 'usa'
            """
        ).fetchall()
    output: list[str] = []
    for ticker, status, estimated_date in rows:
        normalized = normalize_ticker(str(ticker))
        if normalized not in selected:
            continue
        if status == "DUE_TODAY":
            output.append(normalized)
            continue
        if status != "DATE_PASSED_EVENT_NOT_FOUND" or estimated_date is None:
            continue
        parsed = _parse_date(str(estimated_date))
        if earliest <= parsed <= decision_date:
            output.append(normalized)
    return sorted(dict.fromkeys(output))


def candidate_hash(candidate_rows: list[Mapping[str, Any]]) -> str:
    material = [
        {
            "market": row.get("market"),
            "ticker": row.get("ticker"),
            "decision": row.get("decision"),
            "target_period_end_date": row.get("target_period_end_date"),
            "planned_action": row.get("planned_action"),
        }
        for row in sorted(candidate_rows, key=lambda item: (str(item.get("ticker")), str(item.get("target_period_end_date"))))
    ]
    payload = json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_candidate_hash(plan: Mapping[str, Any]) -> bool:
    return str(plan.get("candidate_hash")) == candidate_hash([dict(row) for row in plan.get("candidates", [])])


def _decision_rows(
    *,
    fundamentals_db: Path,
    ohlcv_db: Path,
    decision_date: date,
    ohlcv_stale_days: int,
    tickers: list[str] | None,
) -> list[Any]:
    with open_readonly_db(fundamentals_db) as conn, open_readonly_db(ohlcv_db) as ohlcv_conn:
        return build_quarter_refresh_decisions(
            conn,
            ohlcv_conn=ohlcv_conn,
            tickers=tickers,
            market="usa",
            decision_date=decision_date,
            ohlcv_stale_days=ohlcv_stale_days,
        )


def _run_calendar_refresh(root: Path, fundamentals_db: Path, active_tickers: list[str]) -> dict[str, Any]:
    output_root = root / "calendar_refresh"
    tickers_file = output_root / "active_tickers.txt"
    output_root.mkdir(parents=True, exist_ok=True)
    tickers_file.write_text("\n".join(active_tickers) + ("\n" if active_tickers else ""), encoding="utf-8")
    summary_json = output_root / "calendar_refresh_summary.json"
    if not active_tickers:
        payload = {"selected_tickers": 0, "status": CHECK_STATUS_SUCCESS}
        _write_json(summary_json, payload)
        return {"stage": _stage("calendar_refresh", CHECK_STATUS_SUCCESS, selected_tickers=0), "summary": payload}
    exit_code = refresh_yahoo_earnings_calendar.main(
        [
            "--fundamentals-db",
            str(fundamentals_db),
            "--tickers-file",
            str(tickers_file),
            "--output-root",
            str(output_root),
            "--summary-json",
            str(summary_json),
            "--apply",
        ]
    )
    payload = _read_json(summary_json)
    status = CHECK_STATUS_SUCCESS if exit_code == 0 else CHECK_STATUS_FAILED
    return {"stage": _stage("calendar_refresh", status, selected_tickers=len(active_tickers), exit_code=exit_code), "summary": payload}


def _run_completed_event_refresh(root: Path, fundamentals_db: Path, tickers: list[str]) -> dict[str, Any]:
    output_root = root / "completed_event_refresh"
    backup_root = output_root / "backups"
    output_root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    failures = 0
    for ticker in tickers:
        args = Namespace(
            ticker=ticker,
            fundamentals_db=str(fundamentals_db),
            start_date=None,
            safety_margin_days=DEFAULT_SAFETY_MARGIN_DAYS,
            limit=None,
            include_future=False,
            dry_run=False,
            apply=True,
            json_output=True,
            backup=str(backup_root),
        )
        try:
            summary, exit_code = apply_yahoo_earnings_events.build_apply_summary(args)
        except Exception as exc:
            summary = {
                "status": CHECK_STATUS_FAILED,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            }
            exit_code = 1
        rows.append({"ticker": ticker, "exit_code": exit_code, "summary": summary})
        if exit_code != 0:
            failures += 1
    payload = {"selected_tickers": len(tickers), "failed_tickers": failures, "results": rows}
    _write_json(output_root / "completed_event_refresh_summary.json", payload)
    status = CHECK_STATUS_SUCCESS if failures == 0 else CHECK_STATUS_PARTIAL
    return {"stage": _stage("completed_event_refresh", status, selected_tickers=len(tickers), failed_tickers=failures), "summary": payload}


def _run_match_rebuild(root: Path, fundamentals_db: Path, *, enabled: bool) -> dict[str, Any]:
    output_root = root / "event_match_rebuild"
    output_root.mkdir(parents=True, exist_ok=True)
    summary_json = output_root / "event_match_rebuild_summary.json"
    if not enabled:
        payload = {"skipped": True, "reason": "NO_COMPLETED_EVENT_REFRESH_CHANGES_OR_PARTIAL"}
        _write_json(summary_json, payload)
        return {"stage": _stage("event_match_rebuild", CHECK_STATUS_SUCCESS, skipped=True), "summary": payload}
    args = Namespace(
        fundamentals_db=str(fundamentals_db),
        max_delay_days=DEFAULT_MAX_REPORTING_DELAY_DAYS,
        include_low_confidence=True,
        exclude_low_confidence=False,
        dry_run=False,
        apply=True,
        backup=str(output_root / "backups" / f"{fundamentals_db.name}.pre_event_match_rebuild.bak"),
        checkpoint_json=str(output_root / "checkpoint.json"),
        summary_json=str(summary_json),
        output_csv=str(output_root / "matches.csv"),
        json_output=True,
    )
    payload = rebuild_earnings_event_matches.run_cli(args)
    return {"stage": _stage("event_match_rebuild", CHECK_STATUS_SUCCESS, applied=True), "summary": payload}


def _row_is_executable(row: Any) -> bool:
    return (
        row.decision in EXECUTABLE_DECISIONS
        and row.fundamental_fetch_enabled == 1
        and row.target_period_end_date is not None
        and row.matched_latest_event_period_end_date is not None
    )


def _plan_row(row: Any) -> dict[str, Any]:
    current_quarter_exists = 1 if row.quarter_basic_complete is not None else 0
    resolution = "MATCHED_EARNINGS_EVENT" if row.matched_latest_event_period_end_date else "AMBIGUOUS_OR_UNRESOLVED"
    eligible = _row_is_executable(row)
    return {
        "market": row.market,
        "ticker": row.ticker,
        "decision": row.decision,
        "priority": row.decision_priority,
        "fundamental_fetch_enabled": row.fundamental_fetch_enabled,
        "calendar_status": row.calendar_status,
        "estimated_announcement_date": row.estimated_announcement_date,
        "estimated_session": row.estimated_session,
        "completed_earnings_event_id": row.latest_completed_earnings_event_id,
        "completed_event_date": row.latest_completed_earnings_event_date,
        "target_period_end_date": row.target_period_end_date,
        "target_period_resolution_status": resolution,
        "current_quarter_exists": current_quarter_exists,
        "quarter_basic_complete": row.quarter_basic_complete,
        "ttm_input_complete": row.ttm_input_complete,
        "score_history_complete": row.score_history_complete,
        "missing_basic_fields": row.missing_basic_fields,
        "ingestion_status": row.ingestion_status,
        "last_fetch_status": row.last_fetch_status,
        "planned_action": row.planned_action if eligible else "MANUAL_REVIEW",
        "reason": row.decision_reason,
        "eligible_for_execution": 1 if eligible else 0,
    }


def _build_plan(
    *,
    fundamentals_db: Path,
    ohlcv_db: Path,
    decision_date: date,
    ohlcv_stale_days: int,
    created_at_utc: str,
    check_status: str,
    candidate_rows: list[dict[str, Any]],
    stages: list[dict[str, Any]],
) -> dict[str, Any]:
    digest = candidate_hash(candidate_rows)
    return {
        "plan_version": PLAN_VERSION,
        "created_at_utc": created_at_utc,
        "decision_date": decision_date.isoformat(),
        "fundamentals_db": str(fundamentals_db.resolve()),
        "ohlcv_db": str(ohlcv_db.resolve()),
        "ohlcv_stale_days": ohlcv_stale_days,
        "candidate_count": len(candidate_rows),
        "candidate_hash": digest,
        "check_status": check_status,
        "stages": stages,
        "candidates": candidate_rows,
    }


def _write_failed_plan(
    *,
    root: Path,
    fundamentals_db: Path,
    ohlcv_db: Path,
    decision_date: date,
    ohlcv_stale_days: int,
    created_at_utc: str,
    stages: list[dict[str, Any]],
    calendar_refresh_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = _build_plan(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date=decision_date,
        ohlcv_stale_days=ohlcv_stale_days,
        created_at_utc=created_at_utc,
        check_status=CHECK_STATUS_FAILED,
        candidate_rows=[],
        stages=stages,
    )
    summary_payload = {
        "check_status": CHECK_STATUS_FAILED,
        "created_at_utc": created_at_utc,
        "candidate_count": 0,
        "candidate_hash": plan["candidate_hash"],
        "output_root": str(root),
    }
    paths = _write_artifacts(
        root=root,
        plan=plan,
        candidate_rows=[],
        manual_review_rows=[],
        summary=summary_payload,
        calendar_refresh_summary=calendar_refresh_summary or {"summary": {}},
        completed_event_refresh_summary={"summary": {}},
    )
    return {"check_status": CHECK_STATUS_FAILED, "plan": plan, "artifact_paths": paths, "stages": stages, "summary": summary_payload}


def _write_artifacts(
    *,
    root: Path,
    plan: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
    manual_review_rows: list[dict[str, Any]],
    summary: dict[str, Any],
    calendar_refresh_summary: dict[str, Any],
    completed_event_refresh_summary: dict[str, Any],
) -> dict[str, str]:
    paths = {
        "plan_json": root / "plan.json",
        "candidates_csv": root / "candidates.csv",
        "manual_review_csv": root / "manual_review.csv",
        "summary_json": root / "summary.json",
        "calendar_refresh_summary_json": root / "calendar_refresh_summary.json",
        "completed_event_refresh_summary_json": root / "completed_event_refresh_summary.json",
    }
    _write_json(paths["plan_json"], plan)
    _write_csv(paths["candidates_csv"], candidate_rows)
    _write_csv(paths["manual_review_csv"], manual_review_rows)
    _write_json(paths["summary_json"], summary)
    _write_json(paths["calendar_refresh_summary_json"], calendar_refresh_summary.get("summary", calendar_refresh_summary))
    _write_json(paths["completed_event_refresh_summary_json"], completed_event_refresh_summary.get("summary", completed_event_refresh_summary))
    return {key: str(path) for key, path in paths.items()}


def _stage(name: str, status: str, **details: Any) -> dict[str, Any]:
    return {"stage": name, "status": status, **details}


def _overall_status(stages: list[Mapping[str, Any]]) -> str:
    statuses = {str(stage.get("status")) for stage in stages}
    if CHECK_STATUS_FAILED in statuses:
        return CHECK_STATUS_FAILED
    if CHECK_STATUS_PARTIAL in statuses:
        return CHECK_STATUS_PARTIAL
    return CHECK_STATUS_SUCCESS


def _write_json(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(validate_temp_path(path, must_exist=True).read_text(encoding="utf-8"))


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = _fieldnames(rows)
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(resolved)


def _fieldnames(rows: list[Mapping[str, Any]]) -> list[str]:
    if not rows:
        return ["ticker", "decision", "target_period_end_date", "eligible_for_execution"]
    output = list(rows[0].keys())
    for row in rows[1:]:
        for key in row:
            if key not in output:
                output.append(key)
    return output


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
