from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker


SOURCE_YAHOO = "YAHOO_FINANCE"
CALENDAR_STATUSES = {
    "UPCOMING",
    "DUE_TODAY",
    "DATE_PASSED_EVENT_NOT_FOUND",
    "COMPLETED_EVENT_FOUND",
    "DATE_CHANGED",
    "NO_CURRENT_ESTIMATE",
}


@dataclass(frozen=True)
class EarningsCalendarEstimate:
    ticker: str
    estimated_announcement_at: str | None
    estimated_announcement_date: str | None
    estimated_session: str
    source: str = SOURCE_YAHOO


def upsert_earnings_calendar(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    estimate: EarningsCalendarEstimate | None,
    observed_at_utc: str,
    today_new_york: str,
) -> str:
    normalized = normalize_ticker(ticker)
    existing = _load_existing(conn, market, normalized, SOURCE_YAHOO)
    completed_event_id = _completed_event_id(conn, market, normalized)
    status = _calendar_status(
        estimate=estimate,
        completed_event_id=completed_event_id,
        today_new_york=today_new_york,
    )
    previous_estimate = existing["estimated_announcement_at"] if existing else None
    new_estimate = estimate.estimated_announcement_at if estimate else None
    changed = bool(existing and previous_estimate and new_estimate and previous_estimate != new_estimate)
    date_change_count = int(existing["date_change_count"] if existing else 0) + (1 if changed else 0)
    first_observed = str(existing["first_observed_at_utc"] if existing else observed_at_utc)
    created_at = str(existing["created_at_utc"] if existing else observed_at_utc)
    conn.execute(
        """
        INSERT INTO rc_earnings_calendar (
            market,
            ticker,
            estimated_announcement_at,
            estimated_announcement_date,
            estimated_session,
            calendar_status,
            source,
            source_observed_at_utc,
            first_observed_at_utc,
            last_observed_at_utc,
            previous_estimated_announcement_at,
            date_change_count,
            completed_earnings_event_id,
            created_at_utc,
            updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market, ticker, source) DO UPDATE SET
            estimated_announcement_at = excluded.estimated_announcement_at,
            estimated_announcement_date = excluded.estimated_announcement_date,
            estimated_session = excluded.estimated_session,
            calendar_status = excluded.calendar_status,
            source_observed_at_utc = excluded.source_observed_at_utc,
            last_observed_at_utc = excluded.last_observed_at_utc,
            previous_estimated_announcement_at = excluded.previous_estimated_announcement_at,
            date_change_count = excluded.date_change_count,
            completed_earnings_event_id = excluded.completed_earnings_event_id,
            updated_at_utc = excluded.updated_at_utc
        """,
        (
            market,
            normalized,
            new_estimate,
            estimate.estimated_announcement_date if estimate else None,
            estimate.estimated_session if estimate else "UNKNOWN",
            status,
            SOURCE_YAHOO,
            observed_at_utc,
            first_observed,
            observed_at_utc,
            previous_estimate if changed else (existing["previous_estimated_announcement_at"] if existing else None),
            date_change_count,
            completed_event_id,
            created_at,
            observed_at_utc,
        ),
    )
    return status


def select_future_yahoo_estimate(rows: list[Mapping[str, Any]], *, today_new_york: str, ticker: str) -> EarningsCalendarEstimate | None:
    candidates: list[tuple[str, str | None, str]] = []
    for row in rows:
        if _is_completed_yahoo_row(row):
            continue
        estimated_at = _row_text(row, "Earnings Date") or _row_text(row, "earnings_date") or _row_text(row, "date")
        estimated_date = _date_part(estimated_at)
        if estimated_date is None or estimated_date < today_new_york:
            continue
        candidates.append((estimated_date, estimated_at, _session_from_text(estimated_at)))
    if not candidates:
        return None
    estimated_date, estimated_at, session = sorted(candidates, key=lambda item: (item[0], item[1] or ""))[0]
    return EarningsCalendarEstimate(
        ticker=normalize_ticker(ticker),
        estimated_announcement_at=estimated_at,
        estimated_announcement_date=estimated_date,
        estimated_session=session,
    )


def new_york_today_from_utc(now_utc: datetime | None = None) -> str:
    try:
        from zoneinfo import ZoneInfo
    except ImportError:  # pragma: no cover
        return (now_utc or datetime.now(timezone.utc)).date().isoformat()
    current = now_utc or datetime.now(timezone.utc)
    return current.astimezone(ZoneInfo("America/New_York")).date().isoformat()


def _calendar_status(
    *,
    estimate: EarningsCalendarEstimate | None,
    completed_event_id: int | None,
    today_new_york: str,
) -> str:
    if completed_event_id is not None:
        return "COMPLETED_EVENT_FOUND"
    if estimate is None or estimate.estimated_announcement_date is None:
        return "NO_CURRENT_ESTIMATE"
    if estimate.estimated_announcement_date == today_new_york:
        return "DUE_TODAY"
    if estimate.estimated_announcement_date < today_new_york:
        return "DATE_PASSED_EVENT_NOT_FOUND"
    return "UPCOMING"


def _completed_event_id(conn: sqlite3.Connection, market: str, ticker: str) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM rc_earnings_event
        WHERE market = ?
          AND ticker = ?
          AND is_reported = 1
          AND reported_eps IS NOT NULL
        ORDER BY announcement_date DESC, id DESC
        LIMIT 1
        """,
        (market, ticker),
    ).fetchone()
    return int(row[0]) if row else None


def _load_existing(conn: sqlite3.Connection, market: str, ticker: str, source: str) -> sqlite3.Row | None:
    previous = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            """
            SELECT *
            FROM rc_earnings_calendar
            WHERE market = ? AND ticker = ? AND source = ?
            """,
            (market, ticker, source),
        ).fetchone()
    finally:
        conn.row_factory = previous


def _is_completed_yahoo_row(row: Mapping[str, Any]) -> bool:
    reported = _row_value(row, "Reported EPS")
    return reported is not None and str(reported).strip() not in {"", "nan", "NaN"}


def _row_text(row: Mapping[str, Any], key: str) -> str | None:
    value = _row_value(row, key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_value(row: Mapping[str, Any], key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return None


def _date_part(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except ValueError:
        return None


def _session_from_text(value: str | None) -> str:
    text = (value or "").lower()
    if "amc" in text or "after" in text:
        return "AFTER_MARKET"
    if "bmo" in text or "before" in text:
        return "BEFORE_MARKET"
    return "UNKNOWN"
