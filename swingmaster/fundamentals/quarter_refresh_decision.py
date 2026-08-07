from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker, repository_root


AUDIT_VERSION = "quarter_refresh_decision_v2"
DEFAULT_MARKET = "usa"
DEFAULT_OHLCV_DB_PATH = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
DEFAULT_OHLCV_STALE_DAYS = 14

MARKET_DATA_ACTIVE = "ACTIVE"
MARKET_DATA_STALE_OR_INACTIVE = "STALE_OR_INACTIVE"

DECISION_NO_ACTION_INACTIVE_SECURITY = "NO_ACTION_INACTIVE_SECURITY"
DECISION_NO_ACTION_UPCOMING = "NO_ACTION_UPCOMING"
DECISION_NO_ACTION_COMPLETE = "NO_ACTION_COMPLETE"
DECISION_WATCH_DUE_TODAY = "WATCH_DUE_TODAY"
DECISION_WATCH_POST_EVENT_GRACE = "WATCH_POST_EVENT_GRACE"
DECISION_FETCH_NEW_QUARTER = "FETCH_NEW_QUARTER"
DECISION_RETRY_PARTIAL_QUARTER = "RETRY_PARTIAL_QUARTER"
DECISION_RETRY_FETCH_FAILED = "RETRY_FETCH_FAILED"
DECISION_REVIEW_DATE_PASSED_NO_EVENT = "REVIEW_DATE_PASSED_NO_EVENT"
DECISION_REVIEW_NO_CALENDAR_ESTIMATE = "REVIEW_NO_CALENDAR_ESTIMATE"
DECISION_REVIEW_AMBIGUOUS_PERIOD = "REVIEW_AMBIGUOUS_PERIOD"

PRIORITY_P1_FETCH_NOW = "P1_FETCH_NOW"
PRIORITY_P2_RETRY = "P2_RETRY"
PRIORITY_P3_WATCH = "P3_WATCH"
PRIORITY_P4_REVIEW = "P4_REVIEW"
PRIORITY_P5_NO_ACTION = "P5_NO_ACTION"

PRIORITY_ORDER = {
    PRIORITY_P1_FETCH_NOW: 1,
    PRIORITY_P2_RETRY: 2,
    PRIORITY_P3_WATCH: 3,
    PRIORITY_P4_REVIEW: 4,
    PRIORITY_P5_NO_ACTION: 5,
}


@dataclass(frozen=True)
class QuarterRefreshDecisionRow:
    market: str
    ticker: str
    decision_date: str
    ohlcv_stale_days: int
    latest_ohlcv_date: str | None
    ohlcv_age_days: int | None
    market_data_activity_status: str
    fundamental_fetch_enabled: int
    last_assessed_at_utc: str
    calendar_status: str | None
    estimated_announcement_at: str | None
    estimated_announcement_date: str | None
    estimated_session: str | None
    latest_completed_earnings_event_date: str | None
    latest_completed_earnings_event_id: int | None
    latest_db_period_end_date: str | None
    detected_source_period_end_date: str | None
    matched_latest_event_period_end_date: str | None
    target_period_end_date: str | None
    quarter_basic_complete: int | None
    ttm_input_complete: int | None
    score_history_complete: int | None
    ingestion_status: str | None
    last_fetch_status: str | None
    missing_basic_fields: str | None
    decision_before_activity_suppression: str
    decision: str
    decision_priority: str
    decision_priority_rank: int
    decision_reason: str
    eligible_for_future_auto_fetch: int
    inactive_with_fetch_candidate_before_suppression: int
    planned_action: str


def open_readonly_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def temp_root() -> Path:
    return repository_root() / "temp"


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def validate_temp_path(path: Path, *, must_exist: bool = False) -> Path:
    resolved = path.expanduser().resolve()
    root = temp_root().resolve()
    if must_exist and not resolved.exists():
        raise ValueError(f"PATH_DOES_NOT_EXIST:{resolved}")
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"RUNTIME_PATH_OUTSIDE_TEMP:{resolved}") from exc
    return resolved


def classify_quarter_refresh_decision(
    *,
    market: str,
    ticker: str,
    decision_date: str | date,
    ohlcv_stale_days: int,
    latest_ohlcv_date: str | None,
    ohlcv_age_days: int | None,
    market_data_activity_status: str,
    fundamental_fetch_enabled: int,
    last_assessed_at_utc: str,
    calendar: Mapping[str, Any] | None,
    latest_event: Mapping[str, Any] | None,
    latest_db_period_end_date: str | None,
    detected_source_period_end_date: str | None,
    matched_latest_event_period_end_date: str | None,
    matched_quarter_status: Mapping[str, Any] | None,
) -> QuarterRefreshDecisionRow:
    calendar_status = _text(calendar, "calendar_status")
    estimated_announcement_at = _text(calendar, "estimated_announcement_at")
    estimated_announcement_date = _text(calendar, "estimated_announcement_date")
    estimated_session = _text(calendar, "estimated_session")
    latest_event_id = _int(latest_event, "id")
    latest_event_date = _text(latest_event, "announcement_date")
    target_period = matched_latest_event_period_end_date or detected_source_period_end_date
    event_confirms_calendar = _event_confirms_calendar(
        calendar_status=calendar_status,
        estimated_announcement_date=estimated_announcement_date,
        latest_event_date=latest_event_date,
    )
    decision_event_id = latest_event_id if event_confirms_calendar else None
    decision_target_period = target_period if event_confirms_calendar or calendar_status is None else None
    decision_quarter_status = matched_quarter_status if decision_target_period else None

    pre_suppression = _classify_fetch_decision_without_security(
        calendar_status=calendar_status,
        latest_event_id=decision_event_id,
        target_period_end_date=decision_target_period,
        quarter_status=decision_quarter_status,
    )

    if int(fundamental_fetch_enabled) == 0:
        return _row(
            market=market,
            ticker=ticker,
            decision_date=str(decision_date),
            ohlcv_stale_days=ohlcv_stale_days,
            latest_ohlcv_date=latest_ohlcv_date,
            ohlcv_age_days=ohlcv_age_days,
            market_data_activity_status=market_data_activity_status,
            fundamental_fetch_enabled=fundamental_fetch_enabled,
            last_assessed_at_utc=last_assessed_at_utc,
            calendar=calendar,
            latest_event=latest_event,
            latest_db_period_end_date=latest_db_period_end_date,
            detected_source_period_end_date=detected_source_period_end_date,
            matched_latest_event_period_end_date=matched_latest_event_period_end_date,
            target_period_end_date=target_period,
            quarter_status=matched_quarter_status,
            decision=DECISION_NO_ACTION_INACTIVE_SECURITY,
            reason="Latest OHLCV data is missing or stale, so live fundamentals fetching is disabled.",
            decision_before_activity_suppression=pre_suppression[0],
            eligible=0,
            inactive_with_fetch_candidate_before_suppression=1 if _is_auto_fetch_candidate(pre_suppression) else 0,
        )

    decision, reason = pre_suppression
    return _row(
        market=market,
        ticker=ticker,
        decision_date=str(decision_date),
        ohlcv_stale_days=ohlcv_stale_days,
        latest_ohlcv_date=latest_ohlcv_date,
        ohlcv_age_days=ohlcv_age_days,
        market_data_activity_status=market_data_activity_status,
        fundamental_fetch_enabled=fundamental_fetch_enabled,
        last_assessed_at_utc=last_assessed_at_utc,
        calendar=calendar,
        latest_event=latest_event,
        latest_db_period_end_date=latest_db_period_end_date,
        detected_source_period_end_date=detected_source_period_end_date,
        matched_latest_event_period_end_date=matched_latest_event_period_end_date,
        target_period_end_date=target_period,
        quarter_status=matched_quarter_status,
        decision=decision,
        reason=reason,
        decision_before_activity_suppression=decision,
        eligible=1 if _is_auto_fetch_candidate((decision, reason)) else 0,
        inactive_with_fetch_candidate_before_suppression=0,
    )


def build_quarter_refresh_decisions(
    conn: sqlite3.Connection,
    *,
    ohlcv_conn: sqlite3.Connection | None = None,
    tickers: list[str] | None = None,
    market: str = DEFAULT_MARKET,
    decision_date: str | date | None = None,
    ohlcv_stale_days: int = DEFAULT_OHLCV_STALE_DAYS,
    assessed_at_utc: str | None = None,
) -> list[QuarterRefreshDecisionRow]:
    parsed_decision_date = _parse_date(decision_date) if decision_date is not None else datetime.now(timezone.utc).date()
    assessed = assessed_at_utc or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    selected = _select_tickers(conn, tickers=tickers, market=market)
    activity = load_market_data_activity(
        ohlcv_conn,
        tickers=selected,
        market=market,
        decision_date=parsed_decision_date,
        ohlcv_stale_days=ohlcv_stale_days,
        assessed_at_utc=assessed,
    )
    calendars = _calendar_by_ticker(conn, selected, market)
    latest_events = _latest_completed_event_by_ticker(conn, selected, market)
    latest_quarters = _latest_quarter_by_ticker(conn, selected)
    quarter_states = _quarter_state_by_ticker(conn, selected, market)
    matches = _match_by_event_id(conn, [row["id"] for row in latest_events.values()], market)
    quarter_statuses = _quarter_status_by_key(conn, selected, market)

    rows: list[QuarterRefreshDecisionRow] = []
    for ticker in selected:
        activity_row = activity[ticker]
        latest_event = latest_events.get(ticker)
        match = matches.get(int(latest_event["id"])) if latest_event is not None else None
        matched_period = str(match["period_end_date"]) if match is not None else None
        state = quarter_states.get(ticker)
        detected_period = _text(state, "detected_source_period_end_date")
        target_period = matched_period or detected_period
        rows.append(
            classify_quarter_refresh_decision(
                market=market,
                ticker=ticker,
                decision_date=str(parsed_decision_date),
                ohlcv_stale_days=ohlcv_stale_days,
                latest_ohlcv_date=activity_row["latest_ohlcv_date"],
                ohlcv_age_days=activity_row["ohlcv_age_days"],
                market_data_activity_status=activity_row["market_data_activity_status"],
                fundamental_fetch_enabled=activity_row["fundamental_fetch_enabled"],
                last_assessed_at_utc=activity_row["last_assessed_at_utc"],
                calendar=calendars.get(ticker),
                latest_event=latest_event,
                latest_db_period_end_date=latest_quarters.get(ticker),
                detected_source_period_end_date=detected_period,
                matched_latest_event_period_end_date=matched_period,
                matched_quarter_status=quarter_statuses.get((ticker, target_period)) if target_period else None,
            )
        )
    return rows


def summarize_quarter_refresh_decisions(rows: list[QuarterRefreshDecisionRow]) -> dict[str, Any]:
    decision_counts = _counts(row.decision for row in rows)
    priority_counts = _counts(row.decision_priority for row in rows)
    calendar_counts = _counts(row.calendar_status or "NO_CALENDAR_ROW" for row in rows)
    return {
        "audit_version": AUDIT_VERSION,
        "total_tickers": len(rows),
        "decision_date": _single_value(row.decision_date for row in rows),
        "ohlcv_stale_days": _single_value(row.ohlcv_stale_days for row in rows),
        "decision_counts": decision_counts,
        "priority_counts": priority_counts,
        "calendar_status_counts": calendar_counts,
        "active_fetch_count": sum(row.fundamental_fetch_enabled for row in rows),
        "stale_or_inactive_count": sum(1 for row in rows if row.market_data_activity_status == MARKET_DATA_STALE_OR_INACTIVE),
        "no_ohlcv_count": sum(1 for row in rows if row.latest_ohlcv_date is None),
        "ohlcv_age_0_7_days": sum(1 for row in rows if row.ohlcv_age_days is not None and 0 <= row.ohlcv_age_days <= 7),
        "ohlcv_age_8_14_days": sum(1 for row in rows if row.ohlcv_age_days is not None and 8 <= row.ohlcv_age_days <= 14),
        "ohlcv_age_15_30_days": sum(1 for row in rows if row.ohlcv_age_days is not None and 15 <= row.ohlcv_age_days <= 30),
        "ohlcv_age_over_30_days": sum(1 for row in rows if row.ohlcv_age_days is not None and row.ohlcv_age_days > 30),
        "inactive_but_calendar_upcoming_count": sum(
            1 for row in rows if row.fundamental_fetch_enabled == 0 and row.calendar_status == "UPCOMING"
        ),
        "inactive_but_due_today_count": sum(
            1 for row in rows if row.fundamental_fetch_enabled == 0 and row.calendar_status == "DUE_TODAY"
        ),
        "inactive_with_fetch_candidate_count_before_suppression": sum(
            row.inactive_with_fetch_candidate_before_suppression for row in rows
        ),
        "suppressed_review_no_calendar_estimate_tickers": [
            row.ticker
            for row in rows
            if row.decision == DECISION_NO_ACTION_INACTIVE_SECURITY
            and row.decision_before_activity_suppression == DECISION_REVIEW_NO_CALENDAR_ESTIMATE
        ],
        "suppressed_review_date_passed_no_event_tickers": [
            row.ticker
            for row in rows
            if row.decision == DECISION_NO_ACTION_INACTIVE_SECURITY
            and row.decision_before_activity_suppression == DECISION_REVIEW_DATE_PASSED_NO_EVENT
        ],
        "eligible_for_future_auto_fetch_count": sum(row.eligible_for_future_auto_fetch for row in rows),
        "manual_review_count": sum(1 for row in rows if row.decision_priority == PRIORITY_P4_REVIEW),
        "no_action_count": sum(1 for row in rows if row.decision_priority == PRIORITY_P5_NO_ACTION),
        "no_action_upcoming": decision_counts.get(DECISION_NO_ACTION_UPCOMING, 0),
        "no_action_complete": decision_counts.get(DECISION_NO_ACTION_COMPLETE, 0),
        "no_action_inactive_security": decision_counts.get(DECISION_NO_ACTION_INACTIVE_SECURITY, 0),
        "watch_due_today": decision_counts.get(DECISION_WATCH_DUE_TODAY, 0),
        "watch_post_event_grace": decision_counts.get(DECISION_WATCH_POST_EVENT_GRACE, 0),
        "fetch_new_quarter": decision_counts.get(DECISION_FETCH_NEW_QUARTER, 0),
        "retry_partial_quarter": decision_counts.get(DECISION_RETRY_PARTIAL_QUARTER, 0),
        "retry_fetch_failed": decision_counts.get(DECISION_RETRY_FETCH_FAILED, 0),
        "review_date_passed_no_event": decision_counts.get(DECISION_REVIEW_DATE_PASSED_NO_EVENT, 0),
        "review_no_calendar_estimate": decision_counts.get(DECISION_REVIEW_NO_CALENDAR_ESTIMATE, 0),
        "review_ambiguous_period": decision_counts.get(DECISION_REVIEW_AMBIGUOUS_PERIOD, 0),
        "session_counts": _counts(row.estimated_session or "UNKNOWN" for row in rows),
        "due_today_tickers": [row.ticker for row in rows if row.calendar_status == "DUE_TODAY"],
        "date_passed_event_not_found_tickers": [
            {
                "ticker": row.ticker,
                "estimated_announcement_date": row.estimated_announcement_date,
                "latest_completed_earnings_event_date": row.latest_completed_earnings_event_date,
                "latest_db_period_end_date": row.latest_db_period_end_date,
                "decision": row.decision,
            }
            for row in rows
            if row.calendar_status == "DATE_PASSED_EVENT_NOT_FOUND"
        ],
    }


def load_market_data_activity(
    ohlcv_conn: sqlite3.Connection | None,
    *,
    tickers: list[str],
    market: str,
    decision_date: date,
    ohlcv_stale_days: int,
    assessed_at_utc: str,
) -> dict[str, dict[str, Any]]:
    latest_dates = _latest_ohlcv_dates(ohlcv_conn, tickers=tickers, market=market)
    return {
        ticker: classify_market_data_activity(
            market=market,
            ticker=ticker,
            latest_ohlcv_date=latest_dates.get(ticker),
            decision_date=decision_date,
            ohlcv_stale_days=ohlcv_stale_days,
            assessed_at_utc=assessed_at_utc,
        )
        for ticker in tickers
    }


def classify_market_data_activity(
    *,
    market: str,
    ticker: str,
    latest_ohlcv_date: str | None,
    decision_date: date,
    ohlcv_stale_days: int,
    assessed_at_utc: str,
) -> dict[str, Any]:
    age_days = None
    if latest_ohlcv_date:
        age_days = (decision_date - _parse_date(latest_ohlcv_date)).days
    active = age_days is not None and age_days <= ohlcv_stale_days
    return {
        "market": market,
        "ticker": ticker,
        "latest_ohlcv_date": latest_ohlcv_date,
        "ohlcv_age_days": age_days,
        "market_data_activity_status": MARKET_DATA_ACTIVE if active else MARKET_DATA_STALE_OR_INACTIVE,
        "fundamental_fetch_enabled": 1 if active else 0,
        "last_assessed_at_utc": assessed_at_utc,
    }


def write_decision_artifacts(rows: list[QuarterRefreshDecisionRow], root: Path) -> dict[str, str]:
    resolved = validate_temp_path(root)
    resolved.mkdir(parents=True, exist_ok=True)
    summary = summarize_quarter_refresh_decisions(rows)
    paths = {
        "summary_json": resolved / "quarter_refresh_decision_summary.json",
        "decisions_csv": resolved / "quarter_refresh_decisions.csv",
        "actionable_csv": resolved / "quarter_refresh_actionable.csv",
        "review_csv": resolved / "quarter_refresh_review.csv",
    }
    write_json_atomic(paths["summary_json"], summary)
    write_csv_atomic(paths["decisions_csv"], [asdict(row) for row in rows])
    write_csv_atomic(
        paths["actionable_csv"],
        [asdict(row) for row in rows if row.decision_priority in {PRIORITY_P1_FETCH_NOW, PRIORITY_P2_RETRY}],
    )
    write_csv_atomic(
        paths["review_csv"],
        [asdict(row) for row in rows if row.decision_priority == PRIORITY_P4_REVIEW],
    )
    return {key: str(value) for key, value in paths.items()}


def write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def write_csv_atomic(path: Path, rows: list[Mapping[str, Any]]) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(QuarterRefreshDecisionRow.__dataclass_fields__)
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(resolved)


def _classify_fetch_decision_without_security(
    *,
    calendar_status: str | None,
    latest_event_id: int | None,
    target_period_end_date: str | None,
    quarter_status: Mapping[str, Any] | None,
) -> tuple[str, str]:
    if calendar_status == "UPCOMING":
        return DECISION_NO_ACTION_UPCOMING, "Next expected earnings event is still upcoming."
    if calendar_status == "DATE_PASSED_EVENT_NOT_FOUND" and latest_event_id is None:
        return DECISION_REVIEW_DATE_PASSED_NO_EVENT, "Calendar estimate passed but no completed event confirms publication."
    if calendar_status == "NO_CURRENT_ESTIMATE":
        return DECISION_REVIEW_NO_CALENDAR_ESTIMATE, "No current Yahoo next-event estimate is available."
    if calendar_status == "DUE_TODAY" and latest_event_id is None:
        return DECISION_WATCH_DUE_TODAY, "Calendar event is due today but publication is not confirmed."
    if latest_event_id is None:
        return DECISION_REVIEW_NO_CALENDAR_ESTIMATE, "No completed event confirms a current quarter publication."
    if target_period_end_date is None:
        return DECISION_REVIEW_AMBIGUOUS_PERIOD, "Completed event exists but fiscal period cannot be resolved safely."
    if quarter_status is None:
        return DECISION_FETCH_NEW_QUARTER, "Completed event confirms publication and target quarter is missing."
    if _fetch_failed(quarter_status):
        return DECISION_RETRY_FETCH_FAILED, "Target quarter has a recorded fetch failure."
    if int(_value(quarter_status, "quarter_basic_complete") or 0) == 1:
        return DECISION_NO_ACTION_COMPLETE, "Target quarter exists and quarter_basic_complete is true."
    return DECISION_RETRY_PARTIAL_QUARTER, "Target quarter exists but quarter_basic_complete is false."


def _row(
    *,
    market: str,
    ticker: str,
    decision_date: str,
    ohlcv_stale_days: int,
    latest_ohlcv_date: str | None,
    ohlcv_age_days: int | None,
    market_data_activity_status: str,
    fundamental_fetch_enabled: int,
    last_assessed_at_utc: str,
    calendar: Mapping[str, Any] | None,
    latest_event: Mapping[str, Any] | None,
    latest_db_period_end_date: str | None,
    detected_source_period_end_date: str | None,
    matched_latest_event_period_end_date: str | None,
    target_period_end_date: str | None,
    quarter_status: Mapping[str, Any] | None,
    decision: str,
    reason: str,
    decision_before_activity_suppression: str,
    eligible: int,
    inactive_with_fetch_candidate_before_suppression: int,
) -> QuarterRefreshDecisionRow:
    priority = priority_for_decision(decision)
    return QuarterRefreshDecisionRow(
        market=market,
        ticker=ticker,
        decision_date=decision_date,
        ohlcv_stale_days=ohlcv_stale_days,
        latest_ohlcv_date=latest_ohlcv_date,
        ohlcv_age_days=ohlcv_age_days,
        market_data_activity_status=market_data_activity_status,
        fundamental_fetch_enabled=int(fundamental_fetch_enabled),
        last_assessed_at_utc=last_assessed_at_utc,
        calendar_status=_text(calendar, "calendar_status"),
        estimated_announcement_at=_text(calendar, "estimated_announcement_at"),
        estimated_announcement_date=_text(calendar, "estimated_announcement_date"),
        estimated_session=_text(calendar, "estimated_session"),
        latest_completed_earnings_event_date=_text(latest_event, "announcement_date"),
        latest_completed_earnings_event_id=_int(latest_event, "id"),
        latest_db_period_end_date=latest_db_period_end_date,
        detected_source_period_end_date=detected_source_period_end_date,
        matched_latest_event_period_end_date=matched_latest_event_period_end_date,
        target_period_end_date=target_period_end_date,
        quarter_basic_complete=_int(quarter_status, "quarter_basic_complete"),
        ttm_input_complete=_int(quarter_status, "ttm_input_complete"),
        score_history_complete=_int(quarter_status, "score_history_complete"),
        ingestion_status=_text(quarter_status, "ingestion_status"),
        last_fetch_status=_text(quarter_status, "last_fetch_status"),
        missing_basic_fields=_text(quarter_status, "missing_basic_fields"),
        decision_before_activity_suppression=decision_before_activity_suppression,
        decision=decision,
        decision_priority=priority,
        decision_priority_rank=PRIORITY_ORDER[priority],
        decision_reason=reason,
        eligible_for_future_auto_fetch=int(eligible),
        inactive_with_fetch_candidate_before_suppression=int(inactive_with_fetch_candidate_before_suppression),
        planned_action=planned_action_for_decision(decision),
    )


def priority_for_decision(decision: str) -> str:
    if decision == DECISION_FETCH_NEW_QUARTER:
        return PRIORITY_P1_FETCH_NOW
    if decision in {DECISION_RETRY_FETCH_FAILED, DECISION_RETRY_PARTIAL_QUARTER}:
        return PRIORITY_P2_RETRY
    if decision in {DECISION_WATCH_DUE_TODAY, DECISION_WATCH_POST_EVENT_GRACE}:
        return PRIORITY_P3_WATCH
    if decision in {
        DECISION_REVIEW_DATE_PASSED_NO_EVENT,
        DECISION_REVIEW_NO_CALENDAR_ESTIMATE,
        DECISION_REVIEW_AMBIGUOUS_PERIOD,
    }:
        return PRIORITY_P4_REVIEW
    return PRIORITY_P5_NO_ACTION


def planned_action_for_decision(decision: str) -> str:
    return {
        DECISION_FETCH_NEW_QUARTER: "PLAN_FETCH_QUARTERLY_FUNDAMENTALS",
        DECISION_RETRY_PARTIAL_QUARTER: "PLAN_RETRY_QUARTERLY_FUNDAMENTALS",
        DECISION_RETRY_FETCH_FAILED: "PLAN_RETRY_AFTER_BACKOFF",
        DECISION_WATCH_DUE_TODAY: "WATCH_FOR_COMPLETED_EVENT",
        DECISION_WATCH_POST_EVENT_GRACE: "WATCH_GRACE_WINDOW",
        DECISION_REVIEW_DATE_PASSED_NO_EVENT: "MANUAL_REVIEW_CALENDAR_OR_EVENT",
        DECISION_REVIEW_NO_CALENDAR_ESTIMATE: "MANUAL_REVIEW_SOURCE_COVERAGE",
        DECISION_REVIEW_AMBIGUOUS_PERIOD: "MANUAL_REVIEW_PERIOD_MAPPING",
        DECISION_NO_ACTION_INACTIVE_SECURITY: "NO_PROVIDER_TRAFFIC",
        DECISION_NO_ACTION_COMPLETE: "NO_PROVIDER_TRAFFIC",
        DECISION_NO_ACTION_UPCOMING: "NO_PROVIDER_TRAFFIC",
    }.get(decision, "NO_PROVIDER_TRAFFIC")


def _is_auto_fetch_candidate(decision_pair: tuple[str, str]) -> bool:
    return decision_pair[0] in {DECISION_FETCH_NEW_QUARTER, DECISION_RETRY_FETCH_FAILED, DECISION_RETRY_PARTIAL_QUARTER}


def _event_confirms_calendar(
    *,
    calendar_status: str | None,
    estimated_announcement_date: str | None,
    latest_event_date: str | None,
) -> bool:
    if latest_event_date is None:
        return False
    if calendar_status in {"UPCOMING", "NO_CURRENT_ESTIMATE"}:
        return False
    if estimated_announcement_date is None:
        return calendar_status is None
    return latest_event_date >= estimated_announcement_date


def _fetch_failed(row: Mapping[str, Any]) -> bool:
    values = {str(_value(row, "ingestion_status") or "").upper(), str(_value(row, "last_fetch_status") or "").upper()}
    return "FETCH_FAILED" in values or any(value.endswith("FETCH_FAILED") for value in values)


def _select_tickers(conn: sqlite3.Connection, *, tickers: list[str] | None, market: str) -> list[str]:
    if tickers:
        return sorted(dict.fromkeys(normalize_ticker(ticker) for ticker in tickers))
    rows = conn.execute(
        """
        SELECT ticker FROM rc_earnings_calendar WHERE market = ?
        UNION
        SELECT ticker FROM rc_fundamental_quarter_state WHERE market = ?
        UNION
        SELECT ticker FROM rc_fundamental_quarterly
        ORDER BY ticker
        """,
        (market, market),
    ).fetchall()
    return [normalize_ticker(str(row[0])) for row in rows]


def _latest_ohlcv_dates(
    conn: sqlite3.Connection | None,
    *,
    tickers: list[str],
    market: str,
) -> dict[str, str]:
    if conn is None or not tickers:
        return {}
    selected = set(tickers)
    result: dict[str, str] = {}
    for row in conn.execute(
        """
        SELECT osake, MAX(pvm) AS latest_ohlcv_date
        FROM osakedata
        WHERE market = ?
        GROUP BY osake
        """,
        (market,),
    ):
        ticker = normalize_ticker(str(row["osake"]))
        if ticker in selected and row["latest_ohlcv_date"]:
            result[ticker] = str(row["latest_ohlcv_date"])
    return result


def _calendar_by_ticker(conn: sqlite3.Connection, tickers: list[str], market: str) -> dict[str, sqlite3.Row]:
    selected = set(tickers)
    return {
        normalize_ticker(str(row["ticker"])): row
        for row in conn.execute(
            """
            SELECT *
            FROM rc_earnings_calendar
            WHERE market = ?
            """,
            (market,),
        )
        if normalize_ticker(str(row["ticker"])) in selected
    }


def _latest_completed_event_by_ticker(conn: sqlite3.Connection, tickers: list[str], market: str) -> dict[str, sqlite3.Row]:
    selected = set(tickers)
    result: dict[str, sqlite3.Row] = {}
    for row in conn.execute(
        """
        SELECT *
        FROM rc_earnings_event
        WHERE market = ?
          AND is_reported = 1
          AND reported_eps IS NOT NULL
        ORDER BY ticker, announcement_date DESC, id DESC
        """,
        (market,),
    ):
        ticker = normalize_ticker(str(row["ticker"]))
        if ticker in selected and ticker not in result:
            result[ticker] = row
    return result


def _latest_quarter_by_ticker(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, str | None]:
    selected = set(tickers)
    result = {ticker: None for ticker in tickers}
    for row in conn.execute(
        """
        SELECT ticker, MAX(period_end_date) AS latest_period
        FROM rc_fundamental_quarterly
        GROUP BY ticker
        """
    ):
        ticker = normalize_ticker(str(row["ticker"]))
        if ticker in selected:
            result[ticker] = str(row["latest_period"]) if row["latest_period"] else None
    return result


def _quarter_state_by_ticker(conn: sqlite3.Connection, tickers: list[str], market: str) -> dict[str, sqlite3.Row]:
    selected = set(tickers)
    return {
        normalize_ticker(str(row["ticker"])): row
        for row in conn.execute("SELECT * FROM rc_fundamental_quarter_state WHERE market = ?", (market,))
        if normalize_ticker(str(row["ticker"])) in selected
    }


def _match_by_event_id(conn: sqlite3.Connection, event_ids: Iterable[int], market: str) -> dict[int, sqlite3.Row]:
    ids = [int(item) for item in event_ids if item is not None]
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    return {
        int(row["earnings_event_id"]): row
        for row in conn.execute(
            f"""
            SELECT *
            FROM rc_fundamental_quarter_earnings_match
            WHERE market = ? AND earnings_event_id IN ({placeholders})
            ORDER BY matching_confidence DESC, id DESC
            """,
            (market, *ids),
        )
    }


def _quarter_status_by_key(conn: sqlite3.Connection, tickers: list[str], market: str) -> dict[tuple[str, str], sqlite3.Row]:
    selected = set(tickers)
    result: dict[tuple[str, str], sqlite3.Row] = {}
    for row in conn.execute(
        """
        SELECT *
        FROM rc_fundamental_quarter_ingestion_status
        WHERE market = ?
        """,
        (market,),
    ):
        ticker = normalize_ticker(str(row["ticker"]))
        if ticker in selected:
            result[(ticker, str(row["period_end_date"]))] = row
    return result


def _counts(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _single_value(values: Iterable[Any]) -> Any:
    unique = sorted({value for value in values if value is not None})
    if not unique:
        return None
    if len(unique) == 1:
        return unique[0]
    return unique


def _text(row: Mapping[str, Any] | None, key: str) -> str | None:
    if row is None:
        return None
    value = _value(row, key)
    return str(value) if value is not None else None


def _int(row: Mapping[str, Any] | None, key: str) -> int | None:
    if row is None:
        return None
    value = _value(row, key)
    return int(value) if value is not None else None


def _value(row: Mapping[str, Any], key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return None


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value[:10])
