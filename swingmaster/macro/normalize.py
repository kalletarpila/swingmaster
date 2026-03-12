from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any


NORMALIZE_ENGINE_VERSION = "MACRO_NORMALIZE_V1"
RAW_TABLE = "rc_macro_source_raw"
NORMALIZED_TABLE = "macro_source_daily"

SOURCE_CODES = (
    "BTC_USD_CBBTCUSD",
    "HY_OAS_BAMLH0A0HYM2",
    "FED_WALCL",
    "USD_BROAD_DTWEXBGS",
    "PCR_EQUITY_CBOE",
)

SOURCE_FREQUENCY = {
    "BTC_USD_CBBTCUSD": "DAILY_7D",
    "HY_OAS_BAMLH0A0HYM2": "DAILY_CLOSE",
    "FED_WALCL": "WEEKLY",
    "USD_BROAD_DTWEXBGS": "DAILY",
    "PCR_EQUITY_CBOE": "DAILY",
}

SOURCE_FORWARD_FILL_LIMIT_DAYS = {
    "BTC_USD_CBBTCUSD": 2,
    "HY_OAS_BAMLH0A0HYM2": 3,
    "FED_WALCL": None,
    "USD_BROAD_DTWEXBGS": 5,
    "PCR_EQUITY_CBOE": 3,
}


@dataclass(frozen=True)
class NormalizedRow:
    as_of_date: str
    source_code: str
    source_value: float
    source_value_raw_text: str | None
    source_frequency: str
    published_at_utc: str
    retrieved_at_utc: str
    revision_tag: str | None
    run_id: str


@dataclass(frozen=True)
class MacroNormalizeSummary:
    date_from: str
    date_to: str
    mode: str
    raw_rows_scanned: int
    normalized_rows_inserted: int
    normalized_rows_updated: int
    normalized_rows_deleted: int
    normalized_rows_skipped: int
    distinct_sources_normalized: int
    summary_status: str


def _validate_iso_date(value: str, arg_name: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"INVALID_{arg_name}") from exc
    return parsed.isoformat()


def _validate_mode(mode: str) -> str:
    if mode not in {"upsert", "replace-all", "insert-missing"}:
        raise ValueError("INVALID_MODE")
    return mode


def _default_retrieved_at_utc(date_to: str) -> str:
    return f"{date_to}T00:00:00+00:00"


def _compute_run_id(date_from: str, date_to: str, mode: str) -> str:
    return f"{NORMALIZE_ENGINE_VERSION}_{date_from.replace('-', '')}_{date_to.replace('-', '')}_{mode}"


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _iter_calendar_days(date_from: str, date_to: str) -> list[str]:
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _load_raw_rows(conn: Any, *, date_from: str, date_to: str) -> tuple[int, dict[str, list[dict[str, Any]]]]:
    has_unbounded = any(v is None for v in SOURCE_FORWARD_FILL_LIMIT_DAYS.values())
    bounded_limits = [int(v) for v in SOURCE_FORWARD_FILL_LIMIT_DAYS.values() if v is not None]
    if has_unbounded:
        min_needed = "0001-01-01"
    else:
        max_limit_days = max(bounded_limits) if bounded_limits else 0
        min_needed = (date.fromisoformat(date_from) - timedelta(days=max_limit_days)).isoformat()
    placeholders = ",".join("?" for _ in SOURCE_CODES)
    raw = conn.execute(
        f"""
        SELECT
          source_key,
          observation_date,
          raw_value,
          raw_value_text,
          loaded_at_utc
        FROM {RAW_TABLE}
        WHERE source_key IN ({placeholders})
          AND observation_date >= ?
          AND observation_date <= ?
        ORDER BY source_key, observation_date
        """,
        (*SOURCE_CODES, min_needed, date_to),
    ).fetchall()
    raw_rows_scanned = len(raw)
    by_source: dict[str, list[dict[str, Any]]] = {code: [] for code in SOURCE_CODES}
    for source_key, observation_date, raw_value, raw_value_text, loaded_at_utc in raw:
        if source_key not in by_source:
            continue
        if raw_value is None:
            continue
        by_source[source_key].append(
            {
                "observation_date": str(observation_date),
                "raw_value": float(raw_value),
                "raw_value_text": None if raw_value_text is None else str(raw_value_text),
                "loaded_at_utc": str(loaded_at_utc),
            }
        )
    return raw_rows_scanned, by_source


def _build_normalized_rows(
    *,
    date_from: str,
    date_to: str,
    retrieved_at_utc: str,
    run_id: str,
    raw_by_source: dict[str, list[dict[str, Any]]],
) -> list[NormalizedRow]:
    days = _iter_calendar_days(date_from, date_to)
    out: list[NormalizedRow] = []
    for source_code in SOURCE_CODES:
        rows = raw_by_source.get(source_code, [])
        idx = 0
        latest: dict[str, Any] | None = None
        for as_of in days:
            as_of_date = date.fromisoformat(as_of)
            while idx < len(rows):
                row_date = date.fromisoformat(str(rows[idx]["observation_date"]))
                if row_date <= as_of_date:
                    latest = rows[idx]
                    idx += 1
                    continue
                break
            if latest is None:
                continue
            obs_date = date.fromisoformat(str(latest["observation_date"]))
            age_days = (as_of_date - obs_date).days
            if age_days < 0:
                continue
            max_age_days = SOURCE_FORWARD_FILL_LIMIT_DAYS[source_code]
            if max_age_days is not None and age_days > max_age_days:
                continue
            out.append(
                NormalizedRow(
                    as_of_date=as_of,
                    source_code=source_code,
                    source_value=float(latest["raw_value"]),
                    source_value_raw_text=latest["raw_value_text"],
                    source_frequency=SOURCE_FREQUENCY[source_code],
                    published_at_utc=str(latest["loaded_at_utc"]),
                    retrieved_at_utc=retrieved_at_utc,
                    revision_tag=None,
                    run_id=run_id,
                )
            )
    return out


def _write_normalized_rows(
    conn: Any,
    rows: list[NormalizedRow],
    *,
    date_from: str,
    date_to: str,
    mode: str,
) -> tuple[int, int, int, int]:
    normalized_rows_deleted = 0
    if mode == "replace-all":
        placeholders = ",".join("?" for _ in SOURCE_CODES)
        before = conn.total_changes
        conn.execute(
            f"""
            DELETE FROM {NORMALIZED_TABLE}
            WHERE source_code IN ({placeholders})
              AND as_of_date >= ?
              AND as_of_date <= ?
            """,
            (*SOURCE_CODES, date_from, date_to),
        )
        normalized_rows_deleted = conn.total_changes - before

    existing = conn.execute(
        f"""
        SELECT as_of_date, source_code
        FROM {NORMALIZED_TABLE}
        WHERE as_of_date >= ?
          AND as_of_date <= ?
        """,
        (date_from, date_to),
    ).fetchall()
    existing_keys = {(str(row[0]), str(row[1])) for row in existing}

    normalized_rows_inserted = 0
    normalized_rows_updated = 0
    normalized_rows_skipped = 0

    for row in rows:
        key = (row.as_of_date, row.source_code)
        payload = (
            row.as_of_date,
            row.source_code,
            row.source_value,
            row.source_value_raw_text,
            row.source_frequency,
            row.published_at_utc,
            row.retrieved_at_utc,
            row.revision_tag,
            row.run_id,
        )
        if mode == "insert-missing":
            before = conn.total_changes
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {NORMALIZED_TABLE} (
                  as_of_date, source_code, source_value, source_value_raw_text,
                  source_frequency, published_at_utc, retrieved_at_utc, revision_tag, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            if conn.total_changes > before:
                normalized_rows_inserted += 1
            else:
                normalized_rows_skipped += 1
            continue

        conn.execute(
            f"""
            INSERT INTO {NORMALIZED_TABLE} (
              as_of_date, source_code, source_value, source_value_raw_text,
              source_frequency, published_at_utc, retrieved_at_utc, revision_tag, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(as_of_date, source_code) DO UPDATE SET
              source_value=excluded.source_value,
              source_value_raw_text=excluded.source_value_raw_text,
              source_frequency=excluded.source_frequency,
              published_at_utc=excluded.published_at_utc,
              retrieved_at_utc=excluded.retrieved_at_utc,
              revision_tag=excluded.revision_tag,
              run_id=excluded.run_id
            """,
            payload,
        )
        if key in existing_keys:
            normalized_rows_updated += 1
        else:
            normalized_rows_inserted += 1
            existing_keys.add(key)

    conn.commit()
    return (
        normalized_rows_inserted,
        normalized_rows_updated,
        normalized_rows_deleted,
        normalized_rows_skipped,
    )


def normalize_macro_sources(
    conn: Any,
    *,
    date_from: str,
    date_to: str,
    mode: str,
    computed_at: str | None = None,
) -> MacroNormalizeSummary:
    date_from = _validate_iso_date(date_from, "DATE_FROM")
    date_to = _validate_iso_date(date_to, "DATE_TO")
    if date_from > date_to:
        raise ValueError("INVALID_DATE_RANGE")
    mode = _validate_mode(mode)

    if not _table_exists(conn, RAW_TABLE):
        raise RuntimeError("MACRO_RAW_TABLE_MISSING_RUN_MIGRATIONS")
    if not _table_exists(conn, NORMALIZED_TABLE):
        raise RuntimeError("MACRO_SOURCE_DAILY_TABLE_MISSING_RUN_MIGRATIONS")

    retrieved_at_utc = computed_at or _default_retrieved_at_utc(date_to)
    run_id = _compute_run_id(date_from, date_to, mode)
    raw_rows_scanned, raw_by_source = _load_raw_rows(conn, date_from=date_from, date_to=date_to)
    normalized_rows = _build_normalized_rows(
        date_from=date_from,
        date_to=date_to,
        retrieved_at_utc=retrieved_at_utc,
        run_id=run_id,
        raw_by_source=raw_by_source,
    )
    (
        normalized_rows_inserted,
        normalized_rows_updated,
        normalized_rows_deleted,
        normalized_rows_skipped,
    ) = _write_normalized_rows(
        conn,
        normalized_rows,
        date_from=date_from,
        date_to=date_to,
        mode=mode,
    )
    distinct_sources_normalized = len({row.source_code for row in normalized_rows})
    return MacroNormalizeSummary(
        date_from=date_from,
        date_to=date_to,
        mode=mode,
        raw_rows_scanned=raw_rows_scanned,
        normalized_rows_inserted=normalized_rows_inserted,
        normalized_rows_updated=normalized_rows_updated,
        normalized_rows_deleted=normalized_rows_deleted,
        normalized_rows_skipped=normalized_rows_skipped,
        distinct_sources_normalized=distinct_sources_normalized,
        summary_status="OK",
    )
