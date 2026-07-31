from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker, open_readonly_db, repository_root
from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS


DEFAULT_MAX_REPORTING_DELAY_DAYS = 140
HIGH_CONFIDENCE_MAX_DELAY_DAYS = 70
MEDIUM_CONFIDENCE_MAX_DELAY_DAYS = 100
TTM_REQUIRED_FIELDS = ("revenue", "gross_profit", "ebit", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
SCORING_REQUIRED_FIELDS = TTM_REQUIRED_FIELDS
VALUATION_REQUIRED_FIELDS = ("revenue", "ebit", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
SNAPSHOT_REQUIRED_FIELDS = ("revenue", "ebit", "free_cashflow", "cash", "total_debt")
CONSUMER_REQUIRED_FIELDS = {
    "ttm": TTM_REQUIRED_FIELDS,
    "score": SCORING_REQUIRED_FIELDS,
    "valuation": VALUATION_REQUIRED_FIELDS,
    "snapshot": SNAPSHOT_REQUIRED_FIELDS,
}


@dataclass(frozen=True)
class QuarterPeriod:
    ticker: str
    period_end_date: str
    non_null_field_count: int


@dataclass(frozen=True)
class EarningsAnnouncement:
    ticker: str
    announcement_at: str
    announcement_date: str
    announcement_session: str
    reported_eps: float | None
    estimated_eps: float | None
    source_observed_at_utc: str


@dataclass(frozen=True)
class PeriodEventMatch:
    ticker: str
    period_end_date: str
    announcement_at: str | None
    announcement_date: str | None
    announcement_session: str | None
    announcement_effective_trading_date: str | None
    reporting_delay_days: int | None
    match_status: str
    match_confidence: str
    ambiguity_reason: str | None = None


@dataclass(frozen=True)
class FieldAvailability:
    ticker: str
    period_end_date: str
    field_name: str
    latest_value: float | None
    first_available_at_utc: str | None
    latest_available_at_utc: str | None
    first_source_provider: str | None
    latest_source_provider: str | None
    availability_status: str
    source_count: int
    vintage_count: int


def temp_root() -> Path:
    return repository_root() / "temp"


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


def load_quarter_periods(conn: sqlite3.Connection, ticker: str) -> list[QuarterPeriod]:
    normalized = normalize_ticker(ticker)
    fields_expr = " + ".join(f"CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END" for field in REPORTED_FINANCIAL_FIELDS)
    rows = conn.execute(
        f"""
        SELECT ticker, period_end_date, ({fields_expr}) AS non_null_field_count
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
          AND period_end_date IS NOT NULL
          AND date(period_end_date) IS NOT NULL
        ORDER BY date(period_end_date), period_end_date
        """,
        (normalized,),
    ).fetchall()
    return [
        QuarterPeriod(
            ticker=normalize_ticker(str(row["ticker"])),
            period_end_date=str(row["period_end_date"]),
            non_null_field_count=int(row["non_null_field_count"] or 0),
        )
        for row in rows
    ]


def load_earnings_announcements(conn: sqlite3.Connection, ticker: str) -> list[EarningsAnnouncement]:
    normalized = normalize_ticker(ticker)
    rows = conn.execute(
        """
        SELECT ticker, announcement_at, announcement_date, announcement_session,
               reported_eps, estimated_eps, source_observed_at_utc
        FROM rc_earnings_event
        WHERE ticker = ?
          AND is_reported = 1
          AND date(announcement_date) IS NOT NULL
        ORDER BY date(announcement_date), announcement_at
        """,
        (normalized,),
    ).fetchall()
    return [
        EarningsAnnouncement(
            ticker=normalize_ticker(str(row["ticker"])),
            announcement_at=str(row["announcement_at"]),
            announcement_date=str(row["announcement_date"]),
            announcement_session=str(row["announcement_session"]),
            reported_eps=row["reported_eps"],
            estimated_eps=row["estimated_eps"],
            source_observed_at_utc=str(row["source_observed_at_utc"]),
        )
        for row in rows
    ]


def match_periods_to_events(
    periods: list[QuarterPeriod],
    events: list[EarningsAnnouncement],
    *,
    max_delay_days: int = DEFAULT_MAX_REPORTING_DELAY_DAYS,
) -> list[PeriodEventMatch]:
    if max_delay_days <= 0:
        raise ValueError("MAX_DELAY_DAYS_MUST_BE_POSITIVE")
    event_index = 0
    matches: list[PeriodEventMatch] = []
    for period_index, period in enumerate(periods):
        period_end = _parse_date(period.period_end_date)
        if period_end is None:
            matches.append(_unmatched(period, "INVALID_PERIOD", "invalid period_end_date"))
            continue
        while event_index < len(events) and _event_date(events[event_index]) is not None and _event_date(events[event_index]) <= period_end:
            event_index += 1
        if event_index >= len(events):
            matches.append(_unmatched(period, "UNMATCHED_NO_EVENT", None))
            continue
        event = events[event_index]
        event_date = _event_date(event)
        if event_date is None:
            matches.append(_unmatched(period, "INVALID_PERIOD", "invalid announcement_date"))
            event_index += 1
            continue
        delay = (event_date - period_end).days
        if delay < 0:
            matches.append(_unmatched(period, "AMBIGUOUS_SEQUENCE_CONFLICT", "event precedes period end"))
            continue
        if delay > max_delay_days:
            matches.append(_unmatched(period, "UNMATCHED_OUTSIDE_WINDOW", f"delay {delay} > {max_delay_days}"))
            continue
        next_period_end = _parse_date(periods[period_index + 1].period_end_date) if period_index + 1 < len(periods) else None
        if next_period_end is not None and event_date > next_period_end:
            matches.append(_unmatched(period, "AMBIGUOUS_SEQUENCE_CONFLICT", "candidate event occurs after next period end"))
            continue
        next_event_index = event_index + 1
        if next_period_end is not None and next_event_index < len(events):
            next_event_date = _event_date(events[next_event_index])
            if next_event_date is not None and period_end < next_event_date <= next_period_end:
                matches.append(_unmatched(period, "AMBIGUOUS_MULTIPLE_EVENTS", "multiple events between period end and next period end"))
                continue
        confidence = _confidence_for_delay(delay, max_delay_days)
        matches.append(
            PeriodEventMatch(
                ticker=period.ticker,
                period_end_date=period.period_end_date,
                announcement_at=event.announcement_at,
                announcement_date=event.announcement_date,
                announcement_session=event.announcement_session,
                announcement_effective_trading_date=announcement_effective_trading_date(
                    event.announcement_date,
                    event.announcement_session,
                ),
                reporting_delay_days=delay,
                match_status=f"MATCHED_{confidence}_CONFIDENCE",
                match_confidence=confidence,
            )
        )
        event_index += 1
    return matches


def load_field_availability(conn: sqlite3.Connection, ticker: str) -> dict[tuple[str, str], FieldAvailability]:
    normalized = normalize_ticker(ticker)
    latest_rows = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
        """,
        (normalized,),
    ).fetchall()
    latest_by_period = {str(row["period_end_date"]): row for row in latest_rows}
    provenance_rows = conn.execute(
        """
        SELECT ticker, period_end_date, field_name, field_value, source_provider,
               available_at_utc, statement_vintage_id, created_at_utc
        FROM rc_fundamental_quarterly_field_provenance
        WHERE ticker = ?
        ORDER BY date(period_end_date), field_name, available_at_utc, created_at_utc, statement_vintage_id
        """,
        (normalized,),
    ).fetchall()
    grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in provenance_rows:
        grouped.setdefault((str(row["period_end_date"]), str(row["field_name"])), []).append(row)

    availability: dict[tuple[str, str], FieldAvailability] = {}
    periods = sorted(latest_by_period)
    for period_end_date in periods:
        latest = latest_by_period[period_end_date]
        for field_name in REPORTED_FINANCIAL_FIELDS:
            rows = grouped.get((period_end_date, field_name), [])
            values = [row for row in rows if row["field_value"] is not None]
            latest_value = latest[field_name]
            availability[(period_end_date, field_name)] = _field_availability_from_rows(
                normalized,
                period_end_date,
                field_name,
                latest_value,
                values,
            )
    return availability


def inspect_ticker(
    conn: sqlite3.Connection,
    ticker: str,
    *,
    max_delay_days: int = DEFAULT_MAX_REPORTING_DELAY_DAYS,
) -> dict[str, Any]:
    normalized = normalize_ticker(ticker)
    periods = load_quarter_periods(conn, normalized)
    events = load_earnings_announcements(conn, normalized)
    matches = match_periods_to_events(periods, events, max_delay_days=max_delay_days)
    availability = load_field_availability(conn, normalized)
    row_readiness = [
        _row_readiness(period, availability)
        for period in periods
    ]
    return {
        "ticker": normalized,
        "quarterly_period_count": len(periods),
        "earnings_event_count": len(events),
        "matches": [asdict(match) for match in matches],
        "availability_summary": summarize_availability(availability.values()),
        "field_availability": [asdict(item) for item in sorted(availability.values(), key=lambda item: (item.period_end_date, item.field_name))],
        "row_readiness": row_readiness,
    }


def summarize_ticker(conn: sqlite3.Connection, ticker: str, *, max_delay_days: int = DEFAULT_MAX_REPORTING_DELAY_DAYS) -> dict[str, Any]:
    inspected = inspect_ticker(conn, ticker, max_delay_days=max_delay_days)
    matches = inspected["matches"]
    availability = inspected["field_availability"]
    readiness = inspected["row_readiness"]
    periods_with_multiple_sources = {
        item["period_end_date"]
        for item in availability
        if item["source_count"] > 1
    }
    yahoo_after_sec = _periods_with_source_order(conn, ticker, "sec_edgar", "yahoo")
    sec_after_yahoo = _periods_with_source_order(conn, ticker, "yahoo", "sec_edgar")
    return {
        "ticker": inspected["ticker"],
        "quarterly_period_count": inspected["quarterly_period_count"],
        "earnings_event_count": inspected["earnings_event_count"],
        "matched_count": sum(1 for match in matches if str(match["match_status"]).startswith("MATCHED_")),
        "high_confidence_count": sum(1 for match in matches if match["match_confidence"] == "HIGH"),
        "medium_confidence_count": sum(1 for match in matches if match["match_confidence"] == "MEDIUM"),
        "low_confidence_count": sum(1 for match in matches if match["match_confidence"] == "LOW"),
        "unmatched_count": sum(1 for match in matches if str(match["match_status"]).startswith("UNMATCHED_")),
        "ambiguous_count": sum(1 for match in matches if str(match["match_status"]).startswith("AMBIGUOUS_")),
        "field_availability_exact_count": sum(1 for item in availability if item["availability_status"] == "FIELD_AVAILABILITY_EXACT"),
        "field_availability_source_observed_count": sum(1 for item in availability if item["availability_status"] == "FIELD_AVAILABILITY_SOURCE_OBSERVED"),
        "field_availability_filing_bound_count": sum(1 for item in availability if item["availability_status"] == "FIELD_AVAILABILITY_FILING_BOUND"),
        "field_availability_inferred_count": sum(1 for item in availability if item["availability_status"] == "FIELD_AVAILABILITY_INFERRED"),
        "field_availability_unknown_count": sum(1 for item in availability if item["availability_status"] in {"FIELD_AVAILABILITY_UNKNOWN", "HISTORICAL_TIMING_NOT_RECONSTRUCTABLE"}),
        "periods_with_partial_initial_data": sum(1 for item in readiness if item["row_state"] == "PARTIAL_FUNDAMENTALS_AVAILABLE"),
        "periods_later_supplemented": sum(1 for item in readiness if item["later_supplemented"]),
        "periods_with_multiple_sources": len(periods_with_multiple_sources),
        "periods_with_sec_after_yahoo": len(sec_after_yahoo),
        "periods_with_yahoo_after_sec": len(yahoo_after_sec),
        "historical_timing_reconstructable": all(
            item["availability_status"] not in {"FIELD_AVAILABILITY_UNKNOWN", "HISTORICAL_TIMING_NOT_RECONSTRUCTABLE"}
            for item in availability
            if item["latest_value"] is not None
        ),
    }


def audit_universe(
    db_path: Path,
    *,
    tickers: list[str] | None = None,
    max_delay_days: int = DEFAULT_MAX_REPORTING_DELAY_DAYS,
) -> dict[str, Any]:
    with open_readonly_db(db_path) as conn:
        selected = tickers or load_universe(conn)
        rows = _summarize_universe_fast(conn, selected, max_delay_days=max_delay_days)
    return {
        "artifact_schema_version": 1,
        "database_path": str(db_path.resolve()),
        "max_delay_days": max_delay_days,
        "ticker_count": len(rows),
        "aggregate": aggregate_audit_rows(rows),
        "per_ticker": rows,
    }


def load_universe(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT UPPER(ticker) AS ticker
        FROM rc_fundamental_quarterly
        WHERE ticker IS NOT NULL
        ORDER BY UPPER(ticker)
        """
    ).fetchall()
    return [str(row["ticker"]) for row in rows]


def _summarize_universe_fast(
    conn: sqlite3.Connection,
    tickers: list[str],
    *,
    max_delay_days: int,
) -> list[dict[str, Any]]:
    quarterly_counts = _count_by_ticker(conn, "rc_fundamental_quarterly")
    event_counts = _count_events_by_ticker(conn)
    availability_counts = _availability_counts_by_ticker(conn)
    period_source_counts = _period_source_counts_by_ticker(conn)
    source_order_counts = _source_order_counts_by_ticker(conn)
    later_supplemented = _later_supplemented_periods_by_ticker(conn)
    partial_initial = _partial_initial_periods_by_ticker(conn)
    periods_by_ticker = _load_all_quarter_periods(conn)
    events_by_ticker = _load_all_earnings_announcements(conn)
    rows: list[dict[str, Any]] = []
    for ticker in tickers:
        periods = periods_by_ticker.get(ticker, [])
        events = events_by_ticker.get(ticker, [])
        matches = match_periods_to_events(periods, events, max_delay_days=max_delay_days)
        availability = availability_counts.get(ticker, {})
        rows.append(
            {
                "ticker": ticker,
                "quarterly_period_count": quarterly_counts.get(ticker, 0),
                "earnings_event_count": event_counts.get(ticker, 0),
                "matched_count": sum(1 for match in matches if match.match_status.startswith("MATCHED_")),
                "high_confidence_count": sum(1 for match in matches if match.match_confidence == "HIGH"),
                "medium_confidence_count": sum(1 for match in matches if match.match_confidence == "MEDIUM"),
                "low_confidence_count": sum(1 for match in matches if match.match_confidence == "LOW"),
                "unmatched_count": sum(1 for match in matches if match.match_status.startswith("UNMATCHED_")),
                "ambiguous_count": sum(1 for match in matches if match.match_status.startswith("AMBIGUOUS_")),
                "field_availability_exact_count": availability.get("FIELD_AVAILABILITY_EXACT", 0),
                "field_availability_source_observed_count": availability.get("FIELD_AVAILABILITY_SOURCE_OBSERVED", 0),
                "field_availability_filing_bound_count": availability.get("FIELD_AVAILABILITY_FILING_BOUND", 0),
                "field_availability_inferred_count": availability.get("FIELD_AVAILABILITY_INFERRED", 0),
                "field_availability_unknown_count": availability.get("FIELD_AVAILABILITY_UNKNOWN", 0)
                + availability.get("HISTORICAL_TIMING_NOT_RECONSTRUCTABLE", 0),
                "periods_with_partial_initial_data": partial_initial.get(ticker, 0),
                "periods_later_supplemented": later_supplemented.get(ticker, 0),
                "periods_with_multiple_sources": period_source_counts.get(ticker, 0),
                "periods_with_sec_after_yahoo": source_order_counts.get(ticker, {}).get("sec_after_yahoo", 0),
                "periods_with_yahoo_after_sec": source_order_counts.get(ticker, {}).get("yahoo_after_sec", 0),
                "historical_timing_reconstructable": availability.get("HISTORICAL_TIMING_NOT_RECONSTRUCTABLE", 0) == 0
                and availability.get("FIELD_AVAILABILITY_UNKNOWN", 0) == 0,
            }
        )
    return rows


def _load_all_quarter_periods(conn: sqlite3.Connection) -> dict[str, list[QuarterPeriod]]:
    fields_expr = " + ".join(f"CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END" for field in REPORTED_FINANCIAL_FIELDS)
    rows = conn.execute(
        f"""
        SELECT ticker, period_end_date, ({fields_expr}) AS non_null_field_count
        FROM rc_fundamental_quarterly
        WHERE ticker IS NOT NULL
          AND period_end_date IS NOT NULL
          AND date(period_end_date) IS NOT NULL
        ORDER BY ticker, date(period_end_date), period_end_date
        """
    ).fetchall()
    grouped: dict[str, list[QuarterPeriod]] = {}
    for row in rows:
        ticker = str(row["ticker"])
        grouped.setdefault(ticker, []).append(
            QuarterPeriod(
                ticker=ticker,
                period_end_date=str(row["period_end_date"]),
                non_null_field_count=int(row["non_null_field_count"] or 0),
            )
        )
    return grouped


def _load_all_earnings_announcements(conn: sqlite3.Connection) -> dict[str, list[EarningsAnnouncement]]:
    rows = conn.execute(
        """
        SELECT ticker, announcement_at, announcement_date, announcement_session,
               reported_eps, estimated_eps, source_observed_at_utc
        FROM rc_earnings_event
        WHERE is_reported = 1
          AND date(announcement_date) IS NOT NULL
        ORDER BY ticker, date(announcement_date), announcement_at
        """
    ).fetchall()
    grouped: dict[str, list[EarningsAnnouncement]] = {}
    for row in rows:
        ticker = str(row["ticker"])
        grouped.setdefault(ticker, []).append(
            EarningsAnnouncement(
                ticker=ticker,
                announcement_at=str(row["announcement_at"]),
                announcement_date=str(row["announcement_date"]),
                announcement_session=str(row["announcement_session"]),
                reported_eps=row["reported_eps"],
                estimated_eps=row["estimated_eps"],
                source_observed_at_utc=str(row["source_observed_at_utc"]),
            )
        )
    return grouped


def _count_by_ticker(conn: sqlite3.Connection, table_name: str) -> dict[str, int]:
    rows = conn.execute(
        f"""
        SELECT ticker, COUNT(*) AS row_count
        FROM {table_name}
        GROUP BY ticker
        """
    ).fetchall()
    return {str(row["ticker"]): int(row["row_count"] or 0) for row in rows}


def _count_events_by_ticker(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT ticker, COUNT(*) AS row_count
        FROM rc_earnings_event
        WHERE is_reported = 1
        GROUP BY ticker
        """
    ).fetchall()
    return {str(row["ticker"]): int(row["row_count"] or 0) for row in rows}


def _availability_counts_by_ticker(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    rows = conn.execute(
        """
        SELECT ticker,
               CASE
                   WHEN source_provider = 'UNKNOWN_LEGACY' THEN 'HISTORICAL_TIMING_NOT_RECONSTRUCTABLE'
                   WHEN source_provider = 'sec_edgar' THEN 'FIELD_AVAILABILITY_FILING_BOUND'
                   WHEN source_provider = 'yahoo' THEN 'FIELD_AVAILABILITY_SOURCE_OBSERVED'
                   WHEN available_at_utc IS NOT NULL THEN 'FIELD_AVAILABILITY_INFERRED'
                   ELSE 'FIELD_AVAILABILITY_UNKNOWN'
               END AS availability_status,
               COUNT(*) AS row_count
        FROM rc_fundamental_quarterly_field_provenance
        WHERE field_value IS NOT NULL
        GROUP BY ticker, availability_status
        """
    ).fetchall()
    grouped: dict[str, dict[str, int]] = {}
    for row in rows:
        grouped.setdefault(str(row["ticker"]), {})[str(row["availability_status"])] = int(row["row_count"] or 0)
    return grouped


def _period_source_counts_by_ticker(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT ticker, COUNT(*) AS period_count
        FROM (
            SELECT ticker, period_end_date
            FROM rc_fundamental_quarterly_field_provenance
            WHERE field_value IS NOT NULL
            GROUP BY ticker, period_end_date
            HAVING COUNT(DISTINCT source_provider) > 1
        )
        GROUP BY ticker
        """
    ).fetchall()
    return {str(row["ticker"]): int(row["period_count"] or 0) for row in rows}


def _source_order_counts_by_ticker(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    rows = conn.execute(
        """
        WITH per_source AS (
            SELECT ticker, period_end_date, source_provider, MIN(available_at_utc) AS first_available
            FROM rc_fundamental_quarterly_field_provenance
            WHERE source_provider IN ('yahoo', 'sec_edgar')
              AND available_at_utc IS NOT NULL
            GROUP BY ticker, period_end_date, source_provider
        ),
        paired AS (
            SELECT y.ticker,
                   y.period_end_date,
                   y.first_available AS yahoo_available,
                   s.first_available AS sec_available
            FROM per_source y
            JOIN per_source s
              ON s.ticker = y.ticker
             AND s.period_end_date = y.period_end_date
            WHERE y.source_provider = 'yahoo'
              AND s.source_provider = 'sec_edgar'
        )
        SELECT ticker,
               SUM(CASE WHEN sec_available > yahoo_available THEN 1 ELSE 0 END) AS sec_after_yahoo,
               SUM(CASE WHEN yahoo_available > sec_available THEN 1 ELSE 0 END) AS yahoo_after_sec
        FROM paired
        GROUP BY ticker
        """
    ).fetchall()
    return {
        str(row["ticker"]): {
            "sec_after_yahoo": int(row["sec_after_yahoo"] or 0),
            "yahoo_after_sec": int(row["yahoo_after_sec"] or 0),
        }
        for row in rows
    }


def _later_supplemented_periods_by_ticker(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT ticker, COUNT(*) AS period_count
        FROM (
            SELECT ticker, period_end_date
            FROM rc_fundamental_quarterly_vintage
            GROUP BY ticker, period_end_date
            HAVING COUNT(DISTINCT statement_vintage_id) > 1
                OR COUNT(DISTINCT source_provider) > 1
        )
        GROUP BY ticker
        """
    ).fetchall()
    return {str(row["ticker"]): int(row["period_count"] or 0) for row in rows}


def _partial_initial_periods_by_ticker(conn: sqlite3.Connection) -> dict[str, int]:
    field_checks = " + ".join(f"CASE WHEN v.{field} IS NOT NULL THEN 1 ELSE 0 END" for field in REPORTED_FINANCIAL_FIELDS)
    rows = conn.execute(
        f"""
        WITH first_vintage AS (
            SELECT ticker, period_end_date, MIN(available_at_utc || '|' || statement_vintage_id) AS first_key
            FROM rc_fundamental_quarterly_vintage
            GROUP BY ticker, period_end_date
        )
        SELECT v.ticker, COUNT(*) AS period_count
        FROM rc_fundamental_quarterly_vintage v
        JOIN first_vintage f
          ON f.ticker = v.ticker
         AND f.period_end_date = v.period_end_date
         AND f.first_key = v.available_at_utc || '|' || v.statement_vintage_id
        WHERE ({field_checks}) > 0
          AND ({field_checks}) < ?
        GROUP BY v.ticker
        """,
        (len(REPORTED_FINANCIAL_FIELDS),),
    ).fetchall()
    return {str(row["ticker"]): int(row["period_count"] or 0) for row in rows}


def aggregate_audit_rows(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    count_fields = [
        "quarterly_period_count",
        "earnings_event_count",
        "matched_count",
        "high_confidence_count",
        "medium_confidence_count",
        "low_confidence_count",
        "unmatched_count",
        "ambiguous_count",
        "field_availability_exact_count",
        "field_availability_source_observed_count",
        "field_availability_filing_bound_count",
        "field_availability_inferred_count",
        "field_availability_unknown_count",
        "periods_with_partial_initial_data",
        "periods_later_supplemented",
        "periods_with_multiple_sources",
        "periods_with_sec_after_yahoo",
        "periods_with_yahoo_after_sec",
    ]
    aggregate = {field: sum(int(row.get(field) or 0) for row in rows) for field in count_fields}
    aggregate["ticker_count"] = len(rows)
    aggregate["timing_reconstructable_ticker_count"] = sum(1 for row in rows if row.get("historical_timing_reconstructable"))
    matched = int(aggregate["matched_count"])
    period_count = int(aggregate["quarterly_period_count"])
    aggregate["match_rate"] = matched / period_count if period_count else None
    for field in ("high_confidence_count", "medium_confidence_count", "low_confidence_count"):
        aggregate[field.replace("_count", "_pct_of_periods")] = int(aggregate[field]) / period_count if period_count else None
    return aggregate


def write_audit_artifacts(payload: Mapping[str, Any], output_json: Path, output_csv: Path) -> None:
    json_path = validate_temp_path(output_json)
    csv_path = validate_temp_path(output_csv)
    _write_json_atomic(json_path, payload)
    rows = list(payload["per_ticker"])
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = csv_path.with_name(csv_path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else ["ticker"])
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(csv_path)


def summarize_availability(items: Any) -> dict[str, int]:
    summary: dict[str, int] = {}
    for item in items:
        summary[item.availability_status] = summary.get(item.availability_status, 0) + 1
    return dict(sorted(summary.items()))


def announcement_effective_trading_date(announcement_date: str | None, announcement_session: str | None) -> str | None:
    parsed = _parse_date(announcement_date)
    if parsed is None:
        return None
    session = str(announcement_session or "").upper()
    if session in {"BEFORE_MARKET", "DURING_MARKET"}:
        return parsed.isoformat()
    if session == "AFTER_MARKET":
        return _next_weekday(parsed).isoformat()
    return None


def field_effective_trading_date(available_at_utc: str | None) -> str | None:
    if available_at_utc is None:
        return None
    parsed = _parse_datetime(available_at_utc)
    if parsed is None:
        return None
    return parsed.date().isoformat()


def _field_availability_from_rows(
    ticker: str,
    period_end_date: str,
    field_name: str,
    latest_value: float | None,
    rows: list[sqlite3.Row],
) -> FieldAvailability:
    if latest_value is None:
        return FieldAvailability(ticker, period_end_date, field_name, None, None, None, None, None, "FIELD_AVAILABILITY_UNKNOWN", 0, 0)
    if not rows:
        return FieldAvailability(ticker, period_end_date, field_name, latest_value, None, None, None, None, "FIELD_AVAILABILITY_UNKNOWN", 0, 0)
    ordered = sorted(rows, key=lambda row: (str(row["available_at_utc"] or ""), str(row["created_at_utc"] or ""), str(row["statement_vintage_id"])))
    first = ordered[0]
    latest = ordered[-1]
    source_count = len({str(row["source_provider"]) for row in ordered})
    vintage_count = len({str(row["statement_vintage_id"]) for row in ordered})
    status = _availability_status(ordered)
    return FieldAvailability(
        ticker=ticker,
        period_end_date=period_end_date,
        field_name=field_name,
        latest_value=latest_value,
        first_available_at_utc=first["available_at_utc"],
        latest_available_at_utc=latest["available_at_utc"],
        first_source_provider=str(first["source_provider"]),
        latest_source_provider=str(latest["source_provider"]),
        availability_status=status,
        source_count=source_count,
        vintage_count=vintage_count,
    )


def _availability_status(rows: list[sqlite3.Row]) -> str:
    sources = {str(row["source_provider"]) for row in rows}
    if sources == {"UNKNOWN_LEGACY"}:
        return "HISTORICAL_TIMING_NOT_RECONSTRUCTABLE"
    if any(str(row["source_provider"]) == "sec_edgar" for row in rows):
        return "FIELD_AVAILABILITY_FILING_BOUND"
    if any(str(row["source_provider"]) == "yahoo" for row in rows):
        return "FIELD_AVAILABILITY_SOURCE_OBSERVED"
    if any(row["available_at_utc"] for row in rows):
        return "FIELD_AVAILABILITY_INFERRED"
    return "FIELD_AVAILABILITY_UNKNOWN"


def _row_readiness(period: QuarterPeriod, availability: Mapping[tuple[str, str], FieldAvailability]) -> dict[str, Any]:
    field_items = [availability[(period.period_end_date, field)] for field in REPORTED_FINANCIAL_FIELDS]
    known_values = [item for item in field_items if item.latest_value is not None]
    sources = {item.latest_source_provider for item in known_values if item.latest_source_provider}
    later_supplemented = any(item.vintage_count > 1 or item.source_count > 1 for item in known_values)
    consumer_available_at = {
        consumer: _consumer_available_at(period.period_end_date, fields, availability)
        for consumer, fields in CONSUMER_REQUIRED_FIELDS.items()
    }
    if not known_values:
        row_state = "ANNOUNCED_NO_FUNDAMENTALS"
    elif consumer_available_at["score"] is not None:
        row_state = "MINIMUM_SCORING_FIELDS_AVAILABLE"
    elif len(known_values) >= len(REPORTED_FINANCIAL_FIELDS) - 2:
        row_state = "MATERIALLY_COMPLETE"
    else:
        row_state = "PARTIAL_FUNDAMENTALS_AVAILABLE"
    if "sec_edgar" in sources:
        row_state = "SEC_CONFIRMED" if len(sources) == 1 else "LATER_SUPPLEMENTED"
    return {
        "ticker": period.ticker,
        "period_end_date": period.period_end_date,
        "row_state": row_state,
        "known_field_count": len(known_values),
        "source_count": len(sources),
        "later_supplemented": later_supplemented,
        "consumer_available_at": consumer_available_at,
    }


def _consumer_available_at(
    period_end_date: str,
    fields: tuple[str, ...],
    availability: Mapping[tuple[str, str], FieldAvailability],
) -> str | None:
    dates: list[str] = []
    for field_name in fields:
        item = availability.get((period_end_date, field_name))
        if item is None or item.latest_value is None or item.first_available_at_utc is None:
            return None
        if item.availability_status in {"FIELD_AVAILABILITY_UNKNOWN", "HISTORICAL_TIMING_NOT_RECONSTRUCTABLE"}:
            return None
        dates.append(item.first_available_at_utc)
    return max(dates) if dates else None


def _periods_with_source_order(conn: sqlite3.Connection, ticker: str, first_source: str, later_source: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT period_end_date, source_provider, MIN(available_at_utc) AS first_available
        FROM rc_fundamental_quarterly_field_provenance
        WHERE ticker = ?
          AND source_provider IN (?, ?)
          AND available_at_utc IS NOT NULL
        GROUP BY period_end_date, source_provider
        """,
        (normalize_ticker(ticker), first_source, later_source),
    ).fetchall()
    by_period: dict[str, dict[str, str]] = {}
    for row in rows:
        by_period.setdefault(str(row["period_end_date"]), {})[str(row["source_provider"])] = str(row["first_available"])
    return {
        period
        for period, sources in by_period.items()
        if first_source in sources and later_source in sources and sources[first_source] < sources[later_source]
    }


def _unmatched(period: QuarterPeriod, status: str, reason: str | None) -> PeriodEventMatch:
    return PeriodEventMatch(
        ticker=period.ticker,
        period_end_date=period.period_end_date,
        announcement_at=None,
        announcement_date=None,
        announcement_session=None,
        announcement_effective_trading_date=None,
        reporting_delay_days=None,
        match_status=status,
        match_confidence="NONE",
        ambiguity_reason=reason,
    )


def _confidence_for_delay(delay: int, max_delay_days: int) -> str:
    if delay <= HIGH_CONFIDENCE_MAX_DELAY_DAYS:
        return "HIGH"
    if delay <= min(MEDIUM_CONFIDENCE_MAX_DELAY_DAYS, max_delay_days):
        return "MEDIUM"
    return "LOW"


def _event_date(event: EarningsAnnouncement) -> date | None:
    return _parse_date(event.announcement_date)


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed_date = _parse_date(text)
        if parsed_date is None:
            return None
        return datetime.combine(parsed_date, datetime.min.time())


def _next_weekday(value: date) -> date:
    current = value + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)
