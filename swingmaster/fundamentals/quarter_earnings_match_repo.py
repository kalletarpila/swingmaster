from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from swingmaster.fundamentals.earnings_event_matching import (
    DEFAULT_MAX_REPORTING_DELAY_DAYS,
    HIGH_CONFIDENCE_MAX_DELAY_DAYS,
    MEDIUM_CONFIDENCE_MAX_DELAY_DAYS,
)
from swingmaster.fundamentals.earnings_events import normalize_ticker, repository_root


MATCH_TABLE = "rc_fundamental_quarter_earnings_match"
AVAILABILITY_POLICY = "EARNINGS_EFFECTIVE_DATE_ASSUMED"
MATCHER_VERSION = "earnings_event_quarter_match_v1"
MATCHING_METHOD = "SEQUENTIAL_NEXT_REPORTED_EVENT_V1"
MATCHED_STATUSES = {
    "MATCHED_HIGH_CONFIDENCE",
    "MATCHED_MEDIUM_CONFIDENCE",
    "MATCHED_LOW_CONFIDENCE",
}
MATERIAL_COLUMNS = (
    "market",
    "ticker",
    "period_end_date",
    "earnings_event_id",
    "announcement_at",
    "announcement_date",
    "announcement_session",
    "effective_trading_date",
    "effective_date_status",
    "reporting_delay_days",
    "matching_status",
    "matching_confidence",
    "matching_method",
    "candidate_count",
    "availability_policy",
    "matcher_version",
)


@dataclass(frozen=True)
class PersistedQuarterEarningsMatch:
    market: str
    ticker: str
    period_end_date: str
    earnings_event_id: int
    announcement_at: str
    announcement_date: str
    announcement_session: str
    effective_trading_date: str | None
    effective_date_status: str
    reporting_delay_days: int
    matching_status: str
    matching_confidence: str
    matching_method: str
    candidate_count: int
    availability_policy: str
    matcher_version: str


@dataclass(frozen=True)
class MatchOutcome:
    market: str
    ticker: str
    period_end_date: str
    earnings_event_id: int | None
    announcement_at: str | None
    announcement_date: str | None
    announcement_session: str | None
    effective_trading_date: str | None
    effective_date_status: str
    reporting_delay_days: int | None
    matching_status: str
    matching_confidence: str
    matching_method: str | None
    candidate_count: int
    ambiguity_reason: str | None = None


@dataclass(frozen=True)
class QuarterPeriod:
    market: str
    ticker: str
    period_end_date: str


@dataclass(frozen=True)
class EarningsEvent:
    id: int
    market: str
    ticker: str
    announcement_at: str
    announcement_date: str
    announcement_session: str


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def resolve_effective_trading_date(
    announcement_date: str | None,
    announcement_session: str | None,
) -> tuple[str | None, str]:
    parsed = _parse_date(announcement_date)
    if parsed is None:
        return None, "NO_TRADING_CALENDAR_DATE"
    session = str(announcement_session or "").upper()
    if session == "UNKNOWN":
        return None, "UNKNOWN_SESSION"
    if session in {"BEFORE_MARKET", "DURING_MARKET"}:
        if is_usa_trading_day(parsed):
            return parsed.isoformat(), "RESOLVED_SAME_TRADING_DAY"
        return None, "NO_TRADING_CALENDAR_DATE"
    if session == "AFTER_MARKET":
        return next_usa_trading_day(parsed).isoformat(), "RESOLVED_NEXT_TRADING_DAY"
    return None, "UNKNOWN_SESSION"


def is_usa_trading_day(value: date) -> bool:
    return value.weekday() < 5 and value not in usa_market_holidays(value.year)


def next_usa_trading_day(value: date) -> date:
    current = value + timedelta(days=1)
    while not is_usa_trading_day(current):
        current += timedelta(days=1)
    return current


def usa_market_holidays(year: int) -> set[date]:
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _good_friday(year),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }
    if year >= 2022:
        holidays.add(_observed_fixed_holiday(year, 6, 19))
    return holidays


def build_desired_matches(
    conn: sqlite3.Connection,
    *,
    market: str = "usa",
    max_delay_days: int = DEFAULT_MAX_REPORTING_DELAY_DAYS,
    include_low_confidence: bool = True,
) -> tuple[list[PersistedQuarterEarningsMatch], list[MatchOutcome]]:
    conn.row_factory = sqlite3.Row
    if max_delay_days <= 0:
        raise ValueError("MAX_DELAY_DAYS_MUST_BE_POSITIVE")
    normalized_market = market.strip().lower()
    periods_by_ticker = _load_quarter_periods(conn, normalized_market)
    events_by_ticker = _load_earnings_events(conn, normalized_market)
    desired: list[PersistedQuarterEarningsMatch] = []
    outcomes: list[MatchOutcome] = []
    for ticker in sorted(periods_by_ticker):
        ticker_outcomes = _match_ticker_periods(
            periods_by_ticker[ticker],
            events_by_ticker.get(ticker, []),
            max_delay_days=max_delay_days,
        )
        outcomes.extend(ticker_outcomes)
        for outcome in ticker_outcomes:
            if outcome.matching_status not in MATCHED_STATUSES:
                continue
            if outcome.matching_confidence == "LOW" and not include_low_confidence:
                continue
            desired.append(
                PersistedQuarterEarningsMatch(
                    market=outcome.market,
                    ticker=outcome.ticker,
                    period_end_date=outcome.period_end_date,
                    earnings_event_id=int(outcome.earnings_event_id or 0),
                    announcement_at=str(outcome.announcement_at),
                    announcement_date=str(outcome.announcement_date),
                    announcement_session=str(outcome.announcement_session),
                    effective_trading_date=outcome.effective_trading_date,
                    effective_date_status=outcome.effective_date_status,
                    reporting_delay_days=int(outcome.reporting_delay_days or 0),
                    matching_status=outcome.matching_status,
                    matching_confidence=outcome.matching_confidence,
                    matching_method=str(outcome.matching_method),
                    candidate_count=outcome.candidate_count,
                    availability_policy=AVAILABILITY_POLICY,
                    matcher_version=MATCHER_VERSION,
                )
            )
    validate_desired_matches(desired, max_delay_days=max_delay_days)
    return desired, outcomes


def dry_run_rebuild(
    conn: sqlite3.Connection,
    *,
    max_delay_days: int = DEFAULT_MAX_REPORTING_DELAY_DAYS,
    include_low_confidence: bool = True,
) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    desired, outcomes = build_desired_matches(
        conn,
        max_delay_days=max_delay_days,
        include_low_confidence=include_low_confidence,
    )
    diff = diff_existing(conn, desired)
    summary = summarize_rebuild(
        conn,
        desired=desired,
        outcomes=outcomes,
        diff=diff,
        transaction_status="DRY_RUN",
        max_delay_days=max_delay_days,
        include_low_confidence=include_low_confidence,
    )
    summary["content_hash"] = content_hash(conn)
    return summary


def apply_rebuild(
    conn: sqlite3.Connection,
    *,
    max_delay_days: int = DEFAULT_MAX_REPORTING_DELAY_DAYS,
    include_low_confidence: bool = True,
    backup_verified: bool = False,
    applied_at_utc: str | None = None,
) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    if not backup_verified:
        raise RuntimeError("EARNINGS_MATCH_APPLY_REQUIRES_VERIFIED_BACKUP")
    desired, outcomes = build_desired_matches(
        conn,
        max_delay_days=max_delay_days,
        include_low_confidence=include_low_confidence,
    )
    diff = diff_existing(conn, desired)
    now_text = applied_at_utc or utc_now_text()
    before_hash = content_hash(conn)
    conn.execute("BEGIN")
    try:
        for key in diff["obsolete_keys"]:
            conn.execute(
                """
                DELETE FROM rc_fundamental_quarter_earnings_match
                WHERE market = ? AND ticker = ? AND period_end_date = ?
                """,
                key,
            )
        for record in diff["updated_records"]:
            _update_match(conn, record, now_text)
        for record in diff["inserted_records"]:
            _insert_match(conn, record, now_text)
        _verify_persisted(conn, desired, max_delay_days=max_delay_days)
    except Exception:
        conn.rollback()
        raise
    conn.commit()
    after_hash = content_hash(conn)
    verify_summary = verify_match_table(conn, max_delay_days=max_delay_days)
    summary = summarize_rebuild(
        conn,
        desired=desired,
        outcomes=outcomes,
        diff=diff,
        transaction_status="COMMITTED",
        max_delay_days=max_delay_days,
        include_low_confidence=include_low_confidence,
    )
    summary["content_hash_before"] = before_hash
    summary["content_hash_after"] = after_hash
    summary["verification"] = verify_summary
    return summary


def diff_existing(conn: sqlite3.Connection, desired: list[PersistedQuarterEarningsMatch]) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    existing = _load_existing_matches(conn)
    desired_by_key = {(record.market, record.ticker, record.period_end_date): record for record in desired}
    inserted: list[PersistedQuarterEarningsMatch] = []
    updated: list[PersistedQuarterEarningsMatch] = []
    unchanged = 0
    for key, record in desired_by_key.items():
        existing_row = existing.get(key)
        if existing_row is None:
            inserted.append(record)
        elif _material_dict(record) == {column: existing_row[column] for column in MATERIAL_COLUMNS}:
            unchanged += 1
        else:
            updated.append(record)
    obsolete = sorted(set(existing) - set(desired_by_key))
    return {
        "inserted_records": inserted,
        "updated_records": updated,
        "obsolete_keys": obsolete,
        "inserted_count": len(inserted),
        "updated_count": len(updated),
        "deleted_obsolete_count": len(obsolete),
        "unchanged_count": unchanged,
    }


def summarize_rebuild(
    conn: sqlite3.Connection,
    *,
    desired: list[PersistedQuarterEarningsMatch],
    outcomes: list[MatchOutcome],
    diff: Mapping[str, Any],
    transaction_status: str,
    max_delay_days: int,
    include_low_confidence: bool,
) -> dict[str, Any]:
    matched_high = sum(1 for outcome in outcomes if outcome.matching_status == "MATCHED_HIGH_CONFIDENCE")
    matched_medium = sum(1 for outcome in outcomes if outcome.matching_status == "MATCHED_MEDIUM_CONFIDENCE")
    matched_low = sum(1 for outcome in outcomes if outcome.matching_status == "MATCHED_LOW_CONFIDENCE")
    unmatched = sum(1 for outcome in outcomes if outcome.matching_status.startswith("UNMATCHED_") or outcome.matching_status == "INVALID_PERIOD")
    ambiguous = sum(1 for outcome in outcomes if outcome.matching_status.startswith("AMBIGUOUS_"))
    effective_unknown = sum(1 for record in desired if record.effective_trading_date is None)
    return {
        "quarterly_period_count": _count(conn, "rc_fundamental_quarterly"),
        "earnings_event_count": _count(conn, "rc_earnings_event"),
        "matched_high_count": matched_high,
        "matched_medium_count": matched_medium,
        "matched_low_count": matched_low,
        "persisted_match_count": len(desired),
        "unmatched_count": unmatched,
        "ambiguous_count": ambiguous,
        "effective_date_resolved_count": len(desired) - effective_unknown,
        "effective_date_unknown_count": effective_unknown,
        "inserted_count": int(diff["inserted_count"]),
        "updated_count": int(diff["updated_count"]),
        "deleted_obsolete_count": int(diff["deleted_obsolete_count"]),
        "unchanged_count": int(diff["unchanged_count"]),
        "transaction_status": transaction_status,
        "matcher_version": MATCHER_VERSION,
        "availability_policy": AVAILABILITY_POLICY,
        "max_delay_days": max_delay_days,
        "include_low_confidence": include_low_confidence,
    }


def verify_match_table(conn: sqlite3.Connection, *, max_delay_days: int = DEFAULT_MAX_REPORTING_DELAY_DAYS) -> dict[str, int | str]:
    conn.row_factory = sqlite3.Row
    quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    checks = {
        "quick_check": str(quick),
        "row_count": _count(conn, MATCH_TABLE),
        "duplicate_period_keys": _duplicate_count(conn, "market, ticker, period_end_date"),
        "duplicate_event_keys": _duplicate_count(conn, "market, ticker, earnings_event_id"),
        "unmatched_or_ambiguous_rows": int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {MATCH_TABLE}
                WHERE matching_status NOT IN ('MATCHED_HIGH_CONFIDENCE', 'MATCHED_MEDIUM_CONFIDENCE', 'MATCHED_LOW_CONFIDENCE')
                """
            ).fetchone()[0]
        ),
        "bad_availability_policy_rows": int(
            conn.execute(
                f"SELECT COUNT(*) FROM {MATCH_TABLE} WHERE availability_policy != ?",
                (AVAILABILITY_POLICY,),
            ).fetchone()[0]
        ),
        "bad_matcher_version_rows": int(
            conn.execute(
                f"SELECT COUNT(*) FROM {MATCH_TABLE} WHERE matcher_version != ?",
                (MATCHER_VERSION,),
            ).fetchone()[0]
        ),
        "bad_reporting_delay_rows": int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {MATCH_TABLE}
                WHERE reporting_delay_days < 0 OR reporting_delay_days > ?
                """,
                (max_delay_days,),
            ).fetchone()[0]
        ),
        "announcement_before_period_rows": int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {MATCH_TABLE}
                WHERE date(announcement_date) <= date(period_end_date)
                """
            ).fetchone()[0]
        ),
        "effective_date_mismatch_rows": _effective_date_mismatch_count(conn),
    }
    return checks


def create_verified_backup(db_path: Path, backup_path: Path) -> dict[str, Any]:
    destination = validate_temp_path(backup_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path.resolve())) as src, sqlite3.connect(str(destination)) as dst:
        src.backup(dst)
    if destination.stat().st_size <= 0:
        raise RuntimeError("BACKUP_FILE_EMPTY")
    counts = database_counts(destination)
    return {
        "path": str(destination),
        "verified": True,
        "created": True,
        "file_size_bytes": destination.stat().st_size,
        **counts,
    }


def database_counts(db_path: Path) -> dict[str, int | str]:
    with sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
        return {
            "quick_check": str(quick),
            "quarterly_rows": _count(conn, "rc_fundamental_quarterly"),
            "earnings_event_rows": _count(conn, "rc_earnings_event"),
            "match_rows": _count(conn, MATCH_TABLE) if _table_exists(conn, MATCH_TABLE) else 0,
            "duplicate_match_period_keys": _duplicate_count(conn, "market, ticker, period_end_date")
            if _table_exists(conn, MATCH_TABLE)
            else 0,
            "duplicate_match_event_keys": _duplicate_count(conn, "market, ticker, earnings_event_id")
            if _table_exists(conn, MATCH_TABLE)
            else 0,
        }


def content_hash(conn: sqlite3.Connection) -> str:
    conn.row_factory = sqlite3.Row
    if not _table_exists(conn, MATCH_TABLE):
        return hashlib.sha256(b"").hexdigest()
    rows = conn.execute(
        f"""
        SELECT {", ".join(MATERIAL_COLUMNS)}
        FROM {MATCH_TABLE}
        ORDER BY market, ticker, period_end_date
        """
    ).fetchall()
    payload = [
        {column: row[column] for column in MATERIAL_COLUMNS}
        for row in rows
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def representative_rows(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, list[dict[str, Any]]]:
    conn.row_factory = sqlite3.Row
    if not _table_exists(conn, MATCH_TABLE):
        return {normalize_ticker(ticker): [] for ticker in tickers}
    output: dict[str, list[dict[str, Any]]] = {}
    for ticker in tickers:
        normalized = normalize_ticker(ticker)
        rows = conn.execute(
            f"""
            SELECT ticker, period_end_date, earnings_event_id, announcement_date,
                   announcement_session, effective_trading_date, reporting_delay_days,
                   matching_confidence, matching_status
            FROM {MATCH_TABLE}
            WHERE ticker = ?
            ORDER BY date(period_end_date), period_end_date
            """,
            (normalized,),
        ).fetchall()
        output[normalized] = [dict(row) for row in rows]
    return output


def write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def write_outcomes_csv(path: Path, outcomes: list[MatchOutcome]) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    rows = [asdict(outcome) for outcome in outcomes]
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = list(rows[0].keys()) if rows else list(MatchOutcome.__dataclass_fields__)
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(resolved)


def _match_ticker_periods(
    periods: list[QuarterPeriod],
    events: list[EarningsEvent],
    *,
    max_delay_days: int,
) -> list[MatchOutcome]:
    event_index = 0
    outcomes: list[MatchOutcome] = []
    for period_index, period in enumerate(periods):
        period_end = _parse_date(period.period_end_date)
        if period_end is None:
            outcomes.append(_unmatched(period, "INVALID_PERIOD", "invalid period_end_date", 0))
            continue
        while event_index < len(events) and _parse_date(events[event_index].announcement_date) is not None and _parse_date(events[event_index].announcement_date) <= period_end:
            event_index += 1
        if event_index >= len(events):
            outcomes.append(_unmatched(period, "UNMATCHED_NO_EVENT", None, 0))
            continue
        event = events[event_index]
        event_date = _parse_date(event.announcement_date)
        if event_date is None:
            outcomes.append(_unmatched(period, "INVALID_PERIOD", "invalid announcement_date", 0))
            event_index += 1
            continue
        delay = (event_date - period_end).days
        if delay < 0:
            outcomes.append(_unmatched(period, "AMBIGUOUS_SEQUENCE_CONFLICT", "event precedes period end", 1))
            continue
        if delay > max_delay_days:
            outcomes.append(_unmatched(period, "UNMATCHED_OUTSIDE_WINDOW", f"delay {delay} > {max_delay_days}", 1))
            continue
        next_period_end = _parse_date(periods[period_index + 1].period_end_date) if period_index + 1 < len(periods) else None
        if next_period_end is not None and event_date > next_period_end:
            outcomes.append(_unmatched(period, "AMBIGUOUS_SEQUENCE_CONFLICT", "candidate event occurs after next period end", 1))
            continue
        next_event_index = event_index + 1
        if next_period_end is not None and next_event_index < len(events):
            next_event_date = _parse_date(events[next_event_index].announcement_date)
            if next_event_date is not None and period_end < next_event_date <= next_period_end:
                outcomes.append(_unmatched(period, "AMBIGUOUS_MULTIPLE_EVENTS", "multiple events between period end and next period end", 2))
                continue
        confidence = _confidence_for_delay(delay, max_delay_days)
        effective_date, effective_status = resolve_effective_trading_date(event.announcement_date, event.announcement_session)
        outcomes.append(
            MatchOutcome(
                market=period.market,
                ticker=period.ticker,
                period_end_date=period.period_end_date,
                earnings_event_id=event.id,
                announcement_at=event.announcement_at,
                announcement_date=event.announcement_date,
                announcement_session=event.announcement_session,
                effective_trading_date=effective_date,
                effective_date_status=effective_status,
                reporting_delay_days=delay,
                matching_status=f"MATCHED_{confidence}_CONFIDENCE",
                matching_confidence=confidence,
                matching_method=MATCHING_METHOD,
                candidate_count=1,
            )
        )
        event_index += 1
    return outcomes


def validate_desired_matches(records: list[PersistedQuarterEarningsMatch], *, max_delay_days: int) -> None:
    period_keys: set[tuple[str, str, str]] = set()
    event_keys: set[tuple[str, str, int]] = set()
    for record in records:
        period_key = (record.market, record.ticker, record.period_end_date)
        event_key = (record.market, record.ticker, record.earnings_event_id)
        if period_key in period_keys:
            raise RuntimeError(f"DUPLICATE_PERIOD_MATCH:{period_key}")
        if event_key in event_keys:
            raise RuntimeError(f"DUPLICATE_EVENT_MATCH:{event_key}")
        period_keys.add(period_key)
        event_keys.add(event_key)
        if record.matching_status not in MATCHED_STATUSES:
            raise RuntimeError(f"UNMATCHED_STATUS_IN_PERSIST_SET:{record.matching_status}")
        if record.availability_policy != AVAILABILITY_POLICY:
            raise RuntimeError("BAD_AVAILABILITY_POLICY")
        if record.matcher_version != MATCHER_VERSION:
            raise RuntimeError("BAD_MATCHER_VERSION")
        if record.reporting_delay_days < 0 or record.reporting_delay_days > max_delay_days:
            raise RuntimeError(f"BAD_REPORTING_DELAY:{record.ticker}:{record.period_end_date}")
        period_end = _parse_date(record.period_end_date)
        announcement_date = _parse_date(record.announcement_date)
        if period_end is None or announcement_date is None or announcement_date <= period_end:
            raise RuntimeError(f"BAD_ANNOUNCEMENT_SEQUENCE:{record.ticker}:{record.period_end_date}")


def _verify_persisted(
    conn: sqlite3.Connection,
    desired: list[PersistedQuarterEarningsMatch],
    *,
    max_delay_days: int,
) -> None:
    checks = verify_match_table(conn, max_delay_days=max_delay_days)
    failing = {key: value for key, value in checks.items() if key != "row_count" and value not in (0, "ok")}
    if checks["row_count"] != len(desired):
        failing["row_count"] = checks["row_count"]
    if failing:
        raise RuntimeError(f"EARNINGS_MATCH_VERIFY_FAILED:{failing}")


def _load_quarter_periods(conn: sqlite3.Connection, market: str) -> dict[str, list[QuarterPeriod]]:
    rows = conn.execute(
        """
        SELECT ticker, period_end_date
        FROM rc_fundamental_quarterly
        WHERE ticker IS NOT NULL
          AND period_end_date IS NOT NULL
          AND date(period_end_date) IS NOT NULL
        ORDER BY UPPER(ticker), date(period_end_date), period_end_date
        """
    ).fetchall()
    grouped: dict[str, list[QuarterPeriod]] = {}
    for row in rows:
        ticker = normalize_ticker(str(row["ticker"]))
        grouped.setdefault(ticker, []).append(
            QuarterPeriod(market=market, ticker=ticker, period_end_date=str(row["period_end_date"]))
        )
    return grouped


def _load_earnings_events(conn: sqlite3.Connection, market: str) -> dict[str, list[EarningsEvent]]:
    rows = conn.execute(
        """
        SELECT id, market, ticker, announcement_at, announcement_date, announcement_session
        FROM rc_earnings_event
        WHERE market = ?
          AND is_reported = 1
          AND date(announcement_date) IS NOT NULL
        ORDER BY UPPER(ticker), date(announcement_date), announcement_at, id
        """,
        (market,),
    ).fetchall()
    grouped: dict[str, list[EarningsEvent]] = {}
    for row in rows:
        ticker = normalize_ticker(str(row["ticker"]))
        grouped.setdefault(ticker, []).append(
            EarningsEvent(
                id=int(row["id"]),
                market=str(row["market"]),
                ticker=ticker,
                announcement_at=str(row["announcement_at"]),
                announcement_date=str(row["announcement_date"]),
                announcement_session=str(row["announcement_session"]),
            )
        )
    return grouped


def _load_existing_matches(conn: sqlite3.Connection) -> dict[tuple[str, str, str], sqlite3.Row]:
    if not _table_exists(conn, MATCH_TABLE):
        return {}
    rows = conn.execute(f"SELECT * FROM {MATCH_TABLE}").fetchall()
    return {
        (str(row["market"]), str(row["ticker"]), str(row["period_end_date"])): row
        for row in rows
    }


def _insert_match(conn: sqlite3.Connection, record: PersistedQuarterEarningsMatch, now_text: str) -> None:
    values = _material_dict(record)
    values["created_at_utc"] = now_text
    values["updated_at_utc"] = now_text
    columns = tuple(values)
    placeholders = ", ".join("?" for _ in columns)
    conn.execute(
        f"""
        INSERT INTO {MATCH_TABLE} ({", ".join(columns)})
        VALUES ({placeholders})
        """,
        tuple(values[column] for column in columns),
    )


def _update_match(conn: sqlite3.Connection, record: PersistedQuarterEarningsMatch, now_text: str) -> None:
    values = _material_dict(record)
    values["updated_at_utc"] = now_text
    assignments = ", ".join(f"{column} = ?" for column in values if column not in {"market", "ticker", "period_end_date"})
    params = [
        values[column]
        for column in values
        if column not in {"market", "ticker", "period_end_date"}
    ]
    params.extend([record.market, record.ticker, record.period_end_date])
    conn.execute(
        f"""
        UPDATE {MATCH_TABLE}
        SET {assignments}
        WHERE market = ? AND ticker = ? AND period_end_date = ?
        """,
        params,
    )


def _material_dict(record: PersistedQuarterEarningsMatch) -> dict[str, Any]:
    return {column: getattr(record, column) for column in MATERIAL_COLUMNS}


def _effective_date_mismatch_count(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        f"""
        SELECT announcement_date, announcement_session, effective_trading_date, effective_date_status
        FROM {MATCH_TABLE}
        """
    ).fetchall()
    mismatches = 0
    for row in rows:
        expected_date, expected_status = resolve_effective_trading_date(row["announcement_date"], row["announcement_session"])
        if row["effective_trading_date"] != expected_date or row["effective_date_status"] != expected_status:
            mismatches += 1
    return mismatches


def _duplicate_count(conn: sqlite3.Connection, columns: str) -> int:
    return int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT {columns}, COUNT(*) AS c
                FROM {MATCH_TABLE}
                GROUP BY {columns}
                HAVING c > 1
            )
            """
        ).fetchone()[0]
    )


def _count(conn: sqlite3.Connection, table_name: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute(
        """
        SELECT 1 FROM sqlite_master WHERE type='table' AND name=?
        """,
        (table_name,),
    ).fetchone() is not None


def match_table_exists(conn: sqlite3.Connection) -> bool:
    return _table_exists(conn, MATCH_TABLE)


def _unmatched(period: QuarterPeriod, status: str, reason: str | None, candidate_count: int) -> MatchOutcome:
    return MatchOutcome(
        market=period.market,
        ticker=period.ticker,
        period_end_date=period.period_end_date,
        earnings_event_id=None,
        announcement_at=None,
        announcement_date=None,
        announcement_session=None,
        effective_trading_date=None,
        effective_date_status="UNKNOWN_SESSION",
        reporting_delay_days=None,
        matching_status=status,
        matching_confidence="NONE",
        matching_method=None,
        candidate_count=candidate_count,
        ambiguity_reason=reason,
    )


def _confidence_for_delay(delay: int, max_delay_days: int) -> str:
    if delay <= HIGH_CONFIDENCE_MAX_DELAY_DAYS:
        return "HIGH"
    if delay <= min(MEDIUM_CONFIDENCE_MAX_DELAY_DAYS, max_delay_days):
        return "MEDIUM"
    return "LOW"


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    actual = date(year, month, day)
    if actual.weekday() == 5:
        return actual - timedelta(days=1)
    if actual.weekday() == 6:
        return actual + timedelta(days=1)
    return actual


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    return current + timedelta(days=7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _good_friday(year: int) -> date:
    return _easter_sunday(year) - timedelta(days=2)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)
