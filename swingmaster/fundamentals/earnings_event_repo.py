from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from swingmaster.fundamentals.earnings_events import EarningsEventRecord, YAHOO_SOURCE, normalize_ticker


NATURAL_KEY_COLUMNS = ("market", "ticker", "announcement_at", "source")
MUTABLE_COLUMNS = (
    "announcement_date",
    "announcement_session",
    "is_reported",
    "reported_eps",
    "estimated_eps",
    "surprise_pct",
    "source_observed_at_utc",
    "source_timezone",
)
MATERIAL_COMPARE_COLUMNS = tuple(
    column for column in MUTABLE_COLUMNS if column != "source_observed_at_utc"
)
INSERT_COLUMNS = (
    "market",
    "ticker",
    "announcement_at",
    "announcement_date",
    "announcement_session",
    "is_reported",
    "reported_eps",
    "estimated_eps",
    "surprise_pct",
    "source",
    "source_observed_at_utc",
    "source_timezone",
    "created_at_utc",
    "updated_at_utc",
)


@dataclass(frozen=True)
class EarningsEventApplySummary:
    ticker: str
    market: str
    source: str
    fetched_record_count: int
    eligible_record_count: int
    inserted_count: int
    updated_count: int
    unchanged_count: int
    skipped_count: int
    duplicate_count: int
    transaction_status: str
    source_observed_at_utc: str | None
    dry_run: bool = False
    error_message: str | None = None


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def verify_earnings_event_table(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='rc_earnings_event'
        """
    ).fetchone()
    if row is None:
        raise RuntimeError("EARNINGS_EVENT_TABLE_MISSING")


def count_events_for_ticker(conn: sqlite3.Connection, *, ticker: str, market: str = "usa", source: str = YAHOO_SOURCE) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_earnings_event
        WHERE market = ?
          AND ticker = ?
          AND source = ?
        """,
        (market.strip().lower(), normalize_ticker(ticker), source),
    ).fetchone()
    return int(row[0] or 0)


def plan_earnings_event_upsert(
    conn: sqlite3.Connection,
    records: list[EarningsEventRecord] | tuple[EarningsEventRecord, ...],
    *,
    ticker: str,
    market: str = "usa",
    persist_unreported: bool = False,
    dry_run: bool = True,
) -> EarningsEventApplySummary:
    return _upsert_records(
        conn,
        records,
        ticker=ticker,
        market=market,
        persist_unreported=persist_unreported,
        dry_run=dry_run,
        apply_changes=False,
    )


def apply_earnings_event_upsert(
    conn: sqlite3.Connection,
    records: list[EarningsEventRecord] | tuple[EarningsEventRecord, ...],
    *,
    ticker: str,
    market: str = "usa",
    persist_unreported: bool = False,
    applied_at_utc: str | None = None,
) -> EarningsEventApplySummary:
    conn.execute("BEGIN")
    try:
        summary = _upsert_records(
            conn,
            records,
            ticker=ticker,
            market=market,
            persist_unreported=persist_unreported,
            dry_run=False,
            apply_changes=True,
            applied_at_utc=applied_at_utc,
        )
    except Exception:
        conn.rollback()
        raise
    conn.commit()
    return summary


def _upsert_records(
    conn: sqlite3.Connection,
    records: list[EarningsEventRecord] | tuple[EarningsEventRecord, ...],
    *,
    ticker: str,
    market: str,
    persist_unreported: bool,
    dry_run: bool,
    apply_changes: bool,
    applied_at_utc: str | None = None,
) -> EarningsEventApplySummary:
    verify_earnings_event_table(conn)
    normalized_ticker = normalize_ticker(ticker)
    normalized_market = market.strip().lower()
    now_text = applied_at_utc or utc_now_text()
    fetched_count = len(records)
    eligible_count = 0
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    skipped_count = 0
    duplicate_count = 0
    source_observed_at_utc: str | None = None
    seen: set[tuple[str, str, str, str]] = set()

    for record in records:
        persisted = _record_to_persisted(record, now_text)
        _validate_persisted_record(persisted, normalized_ticker, normalized_market)
        if source_observed_at_utc is None:
            source_observed_at_utc = persisted["source_observed_at_utc"]
        key = tuple(str(persisted[column]) for column in NATURAL_KEY_COLUMNS)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        if not bool(persisted["is_reported"]) and not persist_unreported:
            skipped_count += 1
            continue
        eligible_count += 1
        existing = _load_existing(conn, persisted)
        if existing is None:
            inserted_count += 1
            if apply_changes:
                _insert_record(conn, persisted)
            continue
        if _mutable_values_equal(existing, persisted):
            unchanged_count += 1
            continue
        updated_count += 1
        if apply_changes:
            _update_record(conn, persisted, now_text)

    return EarningsEventApplySummary(
        ticker=normalized_ticker,
        market=normalized_market,
        source=YAHOO_SOURCE,
        fetched_record_count=fetched_count,
        eligible_record_count=eligible_count,
        inserted_count=inserted_count,
        updated_count=updated_count,
        unchanged_count=unchanged_count,
        skipped_count=skipped_count,
        duplicate_count=duplicate_count,
        transaction_status="DRY_RUN" if dry_run else "COMMITTED",
        source_observed_at_utc=source_observed_at_utc,
        dry_run=dry_run,
    )


def _record_to_persisted(record: EarningsEventRecord, now_text: str) -> dict[str, Any]:
    return {
        "market": str(record.market).strip().lower(),
        "ticker": normalize_ticker(record.ticker),
        "announcement_at": record.announcement_at,
        "announcement_date": record.announcement_date,
        "announcement_session": record.announcement_session,
        "is_reported": 1 if record.is_reported else 0,
        "reported_eps": _normalize_float(record.reported_eps),
        "estimated_eps": _normalize_float(record.estimated_eps),
        "surprise_pct": _normalize_float(record.surprise_pct),
        "source": record.source,
        "source_observed_at_utc": record.source_observed_at_utc,
        "source_timezone": record.source_timezone,
        "created_at_utc": now_text,
        "updated_at_utc": now_text,
    }


def _normalize_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _validate_persisted_record(record: dict[str, Any], ticker: str, market: str) -> None:
    if record["ticker"] != ticker:
        raise ValueError(f"EARNINGS_EVENT_TICKER_MISMATCH:{record['ticker']}!={ticker}")
    if record["market"] != market:
        raise ValueError(f"EARNINGS_EVENT_MARKET_MISMATCH:{record['market']}!={market}")
    for column in ("announcement_at", "announcement_date", "announcement_session", "source", "source_observed_at_utc", "source_timezone"):
        if record[column] is None or str(record[column]).strip() == "":
            raise ValueError(f"EARNINGS_EVENT_REQUIRED_VALUE_MISSING:{column}")
    if record["source"] != YAHOO_SOURCE:
        raise ValueError(f"EARNINGS_EVENT_SOURCE_UNSUPPORTED:{record['source']}")


def _load_existing(conn: sqlite3.Connection, record: dict[str, Any]) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM rc_earnings_event
        WHERE market = ?
          AND ticker = ?
          AND announcement_at = ?
          AND source = ?
        """,
        (
            record["market"],
            record["ticker"],
            record["announcement_at"],
            record["source"],
        ),
    ).fetchone()


def _insert_record(conn: sqlite3.Connection, record: dict[str, Any]) -> None:
    conn.execute(
        f"""
        INSERT INTO rc_earnings_event ({", ".join(INSERT_COLUMNS)})
        VALUES ({", ".join("?" for _ in INSERT_COLUMNS)})
        """,
        tuple(record[column] for column in INSERT_COLUMNS),
    )


def _update_record(conn: sqlite3.Connection, record: dict[str, Any], updated_at_utc: str) -> None:
    assignments = ", ".join(f"{column} = ?" for column in MUTABLE_COLUMNS) + ", updated_at_utc = ?"
    conn.execute(
        f"""
        UPDATE rc_earnings_event
        SET {assignments}
        WHERE market = ?
          AND ticker = ?
          AND announcement_at = ?
          AND source = ?
        """,
        tuple(record[column] for column in MUTABLE_COLUMNS)
        + (
            updated_at_utc,
            record["market"],
            record["ticker"],
            record["announcement_at"],
            record["source"],
        ),
    )


def _mutable_values_equal(existing: sqlite3.Row, record: dict[str, Any]) -> bool:
    for column in MATERIAL_COMPARE_COLUMNS:
        existing_value = existing[column]
        new_value = record[column]
        if column == "is_reported":
            if int(existing_value) != int(new_value):
                return False
        elif column in {"reported_eps", "estimated_eps", "surprise_pct"}:
            if _normalize_float(existing_value) != _normalize_float(new_value):
                return False
        else:
            if existing_value != new_value:
                return False
    return True


def summary_to_dict(summary: EarningsEventApplySummary) -> dict[str, Any]:
    return asdict(summary)
