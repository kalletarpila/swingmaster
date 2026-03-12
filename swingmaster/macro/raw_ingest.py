from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import socket
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


INGEST_ENGINE_VERSION = "MACRO_RAW_INGEST_V1"
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
CBOE_EQUITY_PCR_CSV_URL = "https://cdn.cboe.com/data/us/options/market_statistics/daily_market_statistics.csv"
RAW_TABLE = "rc_macro_source_raw"
HTTP_TIMEOUT_SECONDS = 30
HTTP_MAX_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_SECONDS = (1.0, 2.0)
HTTP_MIN_INTERVAL_SECONDS = 0.5
_LAST_HTTP_REQUEST_TS: float | None = None
_sleep = time.sleep
_monotonic = time.monotonic

SOURCE_DEFINITIONS = (
    ("BTC_USD_CBBTCUSD", "FRED", "CBBTCUSD"),
    ("HY_OAS_BAMLH0A0HYM2", "FRED", "BAMLH0A0HYM2"),
    ("FED_WALCL", "FRED", "WALCL"),
    ("USD_BROAD_DTWEXBGS", "FRED", "DTWEXBGS"),
    ("PCR_EQUITY_CBOE", "CBOE", "EQUITY_PUT_CALL_RATIO"),
)


@dataclass(frozen=True)
class RawObservation:
    source_key: str
    vendor: str
    external_series_id: str
    observation_date: str
    raw_value: float | None
    raw_value_text: str | None
    source_url: str


@dataclass(frozen=True)
class MacroIngestSummary:
    sources_requested: int
    date_from: str
    date_to: str
    mode: str
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    rows_skipped: int
    distinct_sources_loaded: int
    run_id: str
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


def _default_loaded_at_utc(date_to: str) -> str:
    return f"{date_to}T00:00:00+00:00"


def compute_run_id(date_from: str, date_to: str, mode: str) -> str:
    payload = {
        "date_from": date_from,
        "date_to": date_to,
        "engine_version": INGEST_ENGINE_VERSION,
        "mode": mode,
        "sources": [item[0] for item in SOURCE_DEFINITIONS],
    }
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    shortsha = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()[:10]
    return f"{INGEST_ENGINE_VERSION}_{date_from.replace('-', '')}_{date_to.replace('-', '')}_{shortsha}"


def _is_retryable_http_status(code: int) -> bool:
    return code == 429 or 500 <= code <= 599


def _apply_rate_limit() -> None:
    global _LAST_HTTP_REQUEST_TS
    now = _monotonic()
    if _LAST_HTTP_REQUEST_TS is not None:
        elapsed = now - _LAST_HTTP_REQUEST_TS
        wait_s = HTTP_MIN_INTERVAL_SECONDS - elapsed
        if wait_s > 0:
            _sleep(wait_s)
            now = _monotonic()
    _LAST_HTTP_REQUEST_TS = now


def _http_get_bytes(url: str, *, error_context: str) -> bytes:
    for attempt in range(1, HTTP_MAX_ATTEMPTS + 1):
        if attempt > 1:
            _sleep(HTTP_RETRY_BACKOFF_SECONDS[attempt - 2])
        _apply_rate_limit()
        try:
            with urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                return resp.read()
        except HTTPError as exc:
            status = int(exc.code)
            if _is_retryable_http_status(status):
                if attempt < HTTP_MAX_ATTEMPTS:
                    continue
                raise RuntimeError(f"{error_context}:RETRIES_EXHAUSTED") from exc
            raise RuntimeError(f"{error_context}:HTTP_{status}") from exc
        except (URLError, TimeoutError, socket.timeout, OSError) as exc:
            if attempt < HTTP_MAX_ATTEMPTS:
                continue
            raise RuntimeError(f"{error_context}:RETRIES_EXHAUSTED") from exc
    raise RuntimeError(f"{error_context}:RETRIES_EXHAUSTED")


def fetch_fred_series_observations(
    series_id: str,
    *,
    date_from: str,
    date_to: str,
    fred_api_key: str,
) -> tuple[dict[str, Any], str]:
    try:
        from fredapi import Fred  # type: ignore[import-not-found]
    except Exception as exc:
        raise RuntimeError("FREDAPI_REQUIRED_FOR_FRED_INGEST") from exc

    try:
        client = Fred(api_key=fred_api_key)
        series = client.get_series(
            series_id,
            observation_start=date_from,
            observation_end=date_to,
        )
    except Exception as exc:
        raise RuntimeError(f"FRED_FETCH_FAILED:{series_id}") from exc

    observations: list[dict[str, str]] = []
    for obs_ts, obs_value in series.items():
        if hasattr(obs_ts, "date"):
            obs_date = obs_ts.date().isoformat()
        else:
            obs_date = str(obs_ts)
        value_text = "."
        if obs_value is not None:
            try:
                as_float = float(obs_value)
                if not math.isnan(as_float):
                    value_text = str(obs_value)
            except (TypeError, ValueError):
                value_text = str(obs_value)

        observations.append(
            {
                "date": obs_date,
                "value": value_text,
            }
        )

    url = f"https://fred.stlouisfed.org/series/{series_id}"
    payload: dict[str, Any] = {"observations": observations}
    return payload, url


def fetch_cboe_equity_put_call_csv() -> tuple[str, str]:
    text = _http_get_bytes(
        CBOE_EQUITY_PCR_CSV_URL,
        error_context="CBOE_FETCH_FAILED:PCR_EQUITY_CBOE",
    ).decode("utf-8-sig")
    return text, CBOE_EQUITY_PCR_CSV_URL


def parse_fred_observations(
    payload: dict[str, Any],
    *,
    source_key: str,
    external_series_id: str,
    source_url: str,
) -> list[RawObservation]:
    observations = payload.get("observations")
    if not isinstance(observations, list):
        return []

    out: list[RawObservation] = []
    for item in observations:
        if not isinstance(item, dict):
            continue
        obs_date_raw = item.get("date")
        if not isinstance(obs_date_raw, str):
            continue
        try:
            obs_date = date.fromisoformat(obs_date_raw).isoformat()
        except ValueError:
            continue
        raw_value_text = item.get("value")
        value_text = str(raw_value_text) if raw_value_text is not None else None
        value_num: float | None = None
        if value_text not in {None, "", "."}:
            try:
                value_num = float(value_text)
            except ValueError:
                value_num = None
        out.append(
            RawObservation(
                source_key=source_key,
                vendor="FRED",
                external_series_id=external_series_id,
                observation_date=obs_date,
                raw_value=value_num,
                raw_value_text=value_text,
                source_url=source_url,
            )
        )
    return out


def _parse_csv_date(cell: str) -> str | None:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(cell.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _parse_csv_float(cell: str) -> float | None:
    text = cell.strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_cboe_equity_put_call_csv(
    csv_text: str,
    *,
    source_key: str,
    source_url: str,
) -> list[RawObservation]:
    reader = csv.reader(io.StringIO(csv_text))
    out: list[RawObservation] = []
    header_idx: int | None = None
    for row in reader:
        if not row:
            continue
        if header_idx is None:
            lowered = [cell.strip().lower() for cell in row]
            if "date" in lowered:
                for idx, col in enumerate(lowered):
                    if idx == 0:
                        continue
                    if "equity" in col and "put" in col and "call" in col and "ratio" in col:
                        header_idx = idx
                        break
                continue
        obs_date = _parse_csv_date(row[0])
        if obs_date is None:
            continue
        value_text: str | None = None
        if header_idx is not None and header_idx < len(row):
            value_text = row[header_idx].strip()
        elif len(row) > 1:
            value_text = row[1].strip()
        if value_text in {None, ""}:
            continue
        value_num = _parse_csv_float(value_text)
        if value_num is None:
            continue
        out.append(
            RawObservation(
                source_key=source_key,
                vendor="CBOE",
                external_series_id="EQUITY_PUT_CALL_RATIO",
                observation_date=obs_date,
                raw_value=value_num,
                raw_value_text=value_text,
                source_url=source_url,
            )
        )
    return out


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _dedupe_observations(rows: Iterable[RawObservation]) -> list[RawObservation]:
    deduped: dict[tuple[str, str], RawObservation] = {}
    for row in rows:
        deduped[(row.source_key, row.observation_date)] = row
    return sorted(deduped.values(), key=lambda x: (x.source_key, x.observation_date))


def _write_rows(
    conn: Any,
    rows: list[RawObservation],
    *,
    mode: str,
    date_from: str,
    date_to: str,
    loaded_at_utc: str,
    run_id: str,
) -> tuple[int, int, int, int]:
    rows_deleted = 0
    if mode == "replace-all":
        source_keys = [item[0] for item in SOURCE_DEFINITIONS]
        placeholders = ",".join("?" for _ in source_keys)
        before = conn.total_changes
        conn.execute(
            f"""
            DELETE FROM {RAW_TABLE}
            WHERE source_key IN ({placeholders})
              AND observation_date >= ?
              AND observation_date <= ?
            """,
            (*source_keys, date_from, date_to),
        )
        rows_deleted = conn.total_changes - before

    existing = conn.execute(
        f"""
        SELECT source_key, observation_date
        FROM {RAW_TABLE}
        WHERE observation_date >= ?
          AND observation_date <= ?
        """,
        (date_from, date_to),
    ).fetchall()
    existing_keys = {(str(row[0]), str(row[1])) for row in existing}

    rows_inserted = 0
    rows_updated = 0
    rows_skipped = 0

    for row in rows:
        key = (row.source_key, row.observation_date)
        payload = (
            row.source_key,
            row.vendor,
            row.external_series_id,
            row.observation_date,
            row.raw_value,
            row.raw_value_text,
            row.source_url,
            loaded_at_utc,
            run_id,
        )
        if mode == "insert-missing":
            before = conn.total_changes
            conn.execute(
                f"""
                INSERT OR IGNORE INTO {RAW_TABLE} (
                  source_key, vendor, external_series_id, observation_date,
                  raw_value, raw_value_text, source_url, loaded_at_utc, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            if conn.total_changes > before:
                rows_inserted += 1
            else:
                rows_skipped += 1
            continue

        conn.execute(
            f"""
            INSERT INTO {RAW_TABLE} (
              source_key, vendor, external_series_id, observation_date,
              raw_value, raw_value_text, source_url, loaded_at_utc, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_key, observation_date) DO UPDATE SET
              vendor=excluded.vendor,
              external_series_id=excluded.external_series_id,
              raw_value=excluded.raw_value,
              raw_value_text=excluded.raw_value_text,
              source_url=excluded.source_url,
              loaded_at_utc=excluded.loaded_at_utc,
              run_id=excluded.run_id
            """,
            payload,
        )
        if key in existing_keys:
            rows_updated += 1
        else:
            rows_inserted += 1
            existing_keys.add(key)

    conn.commit()
    return rows_inserted, rows_updated, rows_deleted, rows_skipped


def ingest_macro_raw(
    conn: Any,
    *,
    date_from: str,
    date_to: str,
    mode: str,
    computed_at: str | None = None,
    fred_api_key: str | None = None,
    fred_fetcher: Callable[[str, str, str, str], tuple[dict[str, Any], str]] | None = None,
    cboe_fetcher: Callable[[], tuple[str, str]] | None = None,
) -> MacroIngestSummary:
    date_from = _validate_iso_date(date_from, "DATE_FROM")
    date_to = _validate_iso_date(date_to, "DATE_TO")
    if date_from > date_to:
        raise ValueError("INVALID_DATE_RANGE")
    mode = _validate_mode(mode)

    if not _table_exists(conn, RAW_TABLE):
        raise RuntimeError("MACRO_RAW_TABLE_MISSING_RUN_MIGRATIONS")

    loaded_at_utc = computed_at or _default_loaded_at_utc(date_to)
    run_id = compute_run_id(date_from, date_to, mode)
    if fred_fetcher is not None:
        fred_loader = fred_fetcher
    else:
        def fred_loader(series_id: str, dfrom: str, dto: str, api_key: str) -> tuple[dict[str, Any], str]:
            if not api_key:
                raise RuntimeError("FRED_API_KEY_MISSING")
            return fetch_fred_series_observations(
                series_id,
                date_from=dfrom,
                date_to=dto,
                fred_api_key=api_key,
            )
    cboe_loader = cboe_fetcher or fetch_cboe_equity_put_call_csv

    rows: list[RawObservation] = []
    for source_key, vendor, external_series_id in SOURCE_DEFINITIONS:
        if vendor == "FRED":
            payload, url = fred_loader(external_series_id, date_from, date_to, fred_api_key)
            parsed = parse_fred_observations(
                payload,
                source_key=source_key,
                external_series_id=external_series_id,
                source_url=url,
            )
        elif vendor == "CBOE":
            csv_text, url = cboe_loader()
            parsed = parse_cboe_equity_put_call_csv(
                csv_text,
                source_key=source_key,
                source_url=url,
            )
        else:
            continue
        for item in parsed:
            if date_from <= item.observation_date <= date_to:
                rows.append(item)

    deduped_rows = _dedupe_observations(rows)
    rows_inserted, rows_updated, rows_deleted, rows_skipped = _write_rows(
        conn,
        deduped_rows,
        mode=mode,
        date_from=date_from,
        date_to=date_to,
        loaded_at_utc=loaded_at_utc,
        run_id=run_id,
    )
    distinct_sources_loaded = len({row.source_key for row in deduped_rows})
    return MacroIngestSummary(
        sources_requested=len(SOURCE_DEFINITIONS),
        date_from=date_from,
        date_to=date_to,
        mode=mode,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        rows_deleted=rows_deleted,
        rows_skipped=rows_skipped,
        distinct_sources_loaded=distinct_sources_loaded,
        run_id=run_id,
        summary_status="OK",
    )
