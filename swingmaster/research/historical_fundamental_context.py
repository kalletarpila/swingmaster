from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker, repository_root
from swingmaster.fundamentals.historical_snapshot import (
    STATUS_NO_AVAILABLE_TTM,
    STATUS_PARTIAL,
    build_historical_fundamental_snapshot,
)
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


HISTORICAL_STATE_FUNDAMENTAL_POLICY = "HISTORICAL_EFFECTIVE_DATE_SAFE_CONTEXT"
STATUS_OK = "OK"
STATUS_DISABLED = "DISABLED"
STATUS_NO_AVAILABLE_SCORE = "NO_AVAILABLE_SCORE"
STATUS_NO_AVAILABLE_PERCENTILE = "NO_AVAILABLE_PERCENTILE"
STATUS_VALUATION_UNAVAILABLE = "VALUATION_UNAVAILABLE"


@dataclass(frozen=True)
class HistoricalStateFundamentalContext:
    fundamental_context_status: str
    fundamental_policy: str
    fundamental_requested_as_of_date: str
    fundamental_source_ttm_as_of_date: str | None
    fundamental_effective_trading_date: str | None
    fundamental_score: float | None
    fundamental_warnings: list[str]
    fundamental_percentile: float | None
    fundamental_percentile_population_size: int | None
    historical_close_price: float | None
    historical_ev_ebit: float | None
    historical_fcf_yield: float | None
    valuation_status: str | None
    percentile_status: str | None


@dataclass(frozen=True)
class HistoricalStateFundamentalAuditSummary:
    rows_evaluated: int
    context_ok_count: int
    context_partial_count: int
    no_available_ttm_count: int
    percentile_unavailable_count: int
    valuation_unavailable_count: int
    state_difference_count: int
    reason_difference_count: int
    transition_difference_count: int
    signal_difference_count: int
    state_attrs_difference_count: int
    cache_hit_count: int
    cache_miss_count: int
    median_enrichment_seconds: float | None
    p95_enrichment_seconds: float | None


class HistoricalStateFundamentalContextCache:
    def __init__(self, max_entries: int = 512) -> None:
        self.max_entries = max_entries
        self._values: dict[tuple[str, str, str, bool, bool], HistoricalStateFundamentalContext] = {}
        self.hit_count = 0
        self.miss_count = 0

    def get_or_build(
        self,
        fundamentals_conn: sqlite3.Connection,
        price_conn: sqlite3.Connection,
        *,
        ticker: str,
        signal_date: str,
        market: str,
        include_percentile: bool = True,
        include_valuation: bool = True,
    ) -> HistoricalStateFundamentalContext:
        key = (normalize_ticker(ticker), signal_date, market, include_percentile, include_valuation)
        cached = self._values.get(key)
        if cached is not None:
            self.hit_count += 1
            return cached
        self.miss_count += 1
        context = build_historical_state_fundamental_context(
            fundamentals_conn,
            price_conn,
            ticker=key[0],
            signal_date=signal_date,
            market=market,
            include_percentile=include_percentile,
            include_valuation=include_valuation,
        )
        if len(self._values) >= self.max_entries:
            self._values.pop(next(iter(self._values)))
        self._values[key] = context
        return context


def build_historical_state_fundamental_context(
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    ticker: str,
    signal_date: str,
    market: str,
    include_percentile: bool = True,
    include_valuation: bool = True,
) -> HistoricalStateFundamentalContext:
    snapshot = build_historical_fundamental_snapshot(
        fundamentals_conn,
        price_conn,
        ticker=ticker,
        as_of_date=signal_date,
        market=market,
        include_percentiles=include_percentile,
        include_valuation=include_valuation,
        include_current_comparison=False,
    )
    warnings = list(snapshot.warnings)
    missing = set(snapshot.missing_components)
    for component in sorted(missing):
        warnings.append(f"MISSING_{component.upper()}")
    if not include_percentile:
        warnings.append("PERCENTILE_DISABLED")
    if not include_valuation:
        warnings.append("VALUATION_DISABLED")

    percentile_row = (snapshot.percentiles or {}).get("row") or {}
    valuation_row = (snapshot.valuation or {}).get("row") or {}
    return HistoricalStateFundamentalContext(
        fundamental_context_status=_context_status(snapshot.snapshot_status),
        fundamental_policy=HISTORICAL_STATE_FUNDAMENTAL_POLICY,
        fundamental_requested_as_of_date=snapshot.requested_as_of_date,
        fundamental_source_ttm_as_of_date=snapshot.source_ttm_as_of_date,
        fundamental_effective_trading_date=snapshot.source_ttm_effective_trading_date,
        fundamental_score=_coerce_float((snapshot.ttm or {}).get("fundamental_score")),
        fundamental_warnings=sorted(dict.fromkeys(warnings)),
        fundamental_percentile=_coerce_float(percentile_row.get("fundamental_score_percentile_blended_lifecycle_weighted")),
        fundamental_percentile_population_size=snapshot.score_percentile_population_size,
        historical_close_price=_coerce_float(snapshot.close_price),
        historical_ev_ebit=_coerce_float(valuation_row.get("valuation_ev_ebit")),
        historical_fcf_yield=_coerce_float(valuation_row.get("valuation_fcf_yield")),
        valuation_status=_valuation_status(snapshot.valuation_status, include_valuation),
        percentile_status=_percentile_status(snapshot, include_percentile),
    )


def build_historical_state_rows(
    state_conn: sqlite3.Connection,
    *,
    tickers: list[str],
    dates: list[str],
) -> list[dict[str, Any]]:
    previous_row_factory = state_conn.row_factory
    state_conn.row_factory = sqlite3.Row
    try:
        return [_load_state_payload(state_conn, ticker=ticker, signal_date=date) for date in dates for ticker in tickers]
    finally:
        state_conn.row_factory = previous_row_factory


def enrich_historical_state_rows(
    rows: list[dict[str, Any]],
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    market: str,
    include_percentile: bool = True,
    include_valuation: bool = True,
    cache: HistoricalStateFundamentalContextCache | None = None,
) -> tuple[list[dict[str, Any]], list[float], HistoricalStateFundamentalContextCache]:
    context_cache = cache or HistoricalStateFundamentalContextCache()
    enriched: list[dict[str, Any]] = []
    timings: list[float] = []
    for row in rows:
        copied = dict(row)
        started = time.perf_counter()
        context = context_cache.get_or_build(
            fundamentals_conn,
            price_conn,
            ticker=str(row["ticker"]),
            signal_date=str(row["date"]),
            market=market,
            include_percentile=include_percentile,
            include_valuation=include_valuation,
        )
        timings.append(time.perf_counter() - started)
        copied["fundamental_context"] = asdict(context)
        enriched.append(copied)
    return enriched, timings, context_cache


def audit_historical_state_fundamental_context(
    state_conn: sqlite3.Connection,
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    tickers: list[str],
    dates: list[str],
    market: str,
    include_percentile: bool = True,
    include_valuation: bool = True,
) -> tuple[HistoricalStateFundamentalAuditSummary, list[dict[str, Any]]]:
    base_rows = build_historical_state_rows(state_conn, tickers=tickers, dates=dates)
    enriched_rows, timings, cache = enrich_historical_state_rows(
        base_rows,
        fundamentals_conn,
        price_conn,
        market=market,
        include_percentile=include_percentile,
        include_valuation=include_valuation,
    )
    summary = HistoricalStateFundamentalAuditSummary(
        rows_evaluated=len(enriched_rows),
        context_ok_count=sum(1 for row in enriched_rows if row["fundamental_context"]["fundamental_context_status"] == STATUS_OK),
        context_partial_count=sum(1 for row in enriched_rows if row["fundamental_context"]["fundamental_context_status"] == STATUS_PARTIAL),
        no_available_ttm_count=sum(1 for row in enriched_rows if row["fundamental_context"]["fundamental_context_status"] == STATUS_NO_AVAILABLE_TTM),
        percentile_unavailable_count=sum(
            1 for row in enriched_rows if row["fundamental_context"]["percentile_status"] == STATUS_NO_AVAILABLE_PERCENTILE
        ),
        valuation_unavailable_count=sum(
            1 for row in enriched_rows if row["fundamental_context"]["valuation_status"] == STATUS_VALUATION_UNAVAILABLE
        ),
        state_difference_count=_difference_count(base_rows, enriched_rows, "state"),
        reason_difference_count=_difference_count(base_rows, enriched_rows, "reason_codes"),
        transition_difference_count=_difference_count(base_rows, enriched_rows, "transition"),
        signal_difference_count=_difference_count(base_rows, enriched_rows, "signal_values"),
        state_attrs_difference_count=_difference_count(base_rows, enriched_rows, "state_attrs"),
        cache_hit_count=cache.hit_count,
        cache_miss_count=cache.miss_count,
        median_enrichment_seconds=_rounded_stat(timings, "median"),
        p95_enrichment_seconds=_rounded_stat(timings, "p95"),
    )
    return summary, enriched_rows


def resolve_dates(state_conn: sqlite3.Connection, *, date: str | None, date_from: str | None, date_to: str | None) -> list[str]:
    if date:
        return [date]
    if not date_from or not date_to:
        raise ValueError("Either --date or both --date-from/--date-to are required")
    rows = state_conn.execute(
        """
        SELECT DISTINCT date
        FROM rc_state_daily
        WHERE date >= ?
          AND date <= ?
        ORDER BY date ASC
        """,
        (date_from, date_to),
    ).fetchall()
    return [str(row[0]) for row in rows]


def resolve_tickers(
    state_conn: sqlite3.Connection,
    *,
    tickers: list[str],
    tickers_file: Path | None,
    dates: list[str],
    first_n: int | None,
    sample_size: int | None,
    random_seed: int,
) -> list[str]:
    from random import Random

    resolved = [normalize_ticker(ticker) for ticker in tickers]
    if tickers_file is not None:
        path = validate_temp_path(tickers_file, must_exist=True)
        resolved.extend(normalize_ticker(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if not resolved:
        placeholders = ", ".join("?" for _ in dates)
        params: list[Any] = [*dates]
        sql = f"""
            SELECT DISTINCT ticker
            FROM rc_state_daily
            WHERE date IN ({placeholders})
            ORDER BY ticker ASC
        """
        if first_n is not None:
            sql += " LIMIT ?"
            params.append(first_n)
        rows = state_conn.execute(sql, params).fetchall()
        resolved = [normalize_ticker(row[0]) for row in rows]
    elif first_n is not None:
        resolved = resolved[:first_n]
    unique = sorted(dict.fromkeys(resolved))
    if sample_size is not None and sample_size < len(unique):
        unique = sorted(Random(random_seed).sample(unique, sample_size))
    return unique


def default_output_root() -> Path:
    return repository_root() / "temp" / "historical_state_fundamental_context" / utc_timestamp()


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def write_csv_atomic(path: Path, rows: list[Mapping[str, Any]]) -> None:
    import csv

    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in _flatten_for_csv(row)})
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(_flatten_for_csv(row))
    tmp.replace(resolved)


def asdict_context(value: HistoricalStateFundamentalContext | HistoricalStateFundamentalAuditSummary) -> dict[str, Any]:
    return asdict(value)


def _load_state_payload(conn: sqlite3.Connection, *, ticker: str, signal_date: str) -> dict[str, Any]:
    normalized = normalize_ticker(ticker)
    attrs_select = "state_attrs_json" if _table_has_column(conn, "rc_state_daily", "state_attrs_json") else "NULL AS state_attrs_json"
    row = conn.execute(
        f"""
        SELECT ticker, date, state, reasons_json, confidence, age, run_id,
               {attrs_select}
        FROM rc_state_daily
        WHERE ticker = ?
          AND date = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (normalized, signal_date),
    ).fetchone()
    if row is None:
        return {
            "ticker": normalized,
            "date": signal_date,
            "state": None,
            "previous_state": _previous_state(conn, ticker=normalized, signal_date=signal_date),
            "reason_codes": [],
            "transition": None,
            "signal_values": [],
            "state_attrs": {},
            "run_id": None,
            "state_row_found": False,
        }
    return {
        "ticker": str(row["ticker"]),
        "date": str(row["date"]),
        "state": row["state"],
        "previous_state": _previous_state(conn, ticker=normalized, signal_date=signal_date),
        "reason_codes": _parse_json_list(row["reasons_json"]),
        "transition": _transition(conn, ticker=normalized, signal_date=signal_date),
        "signal_values": _signals(conn, ticker=normalized, signal_date=signal_date),
        "state_attrs": _parse_json_dict(row["state_attrs_json"]),
        "run_id": row["run_id"],
        "state_row_found": True,
    }


def _previous_state(conn: sqlite3.Connection, *, ticker: str, signal_date: str) -> str | None:
    row = conn.execute(
        """
        SELECT state
        FROM rc_state_daily
        WHERE ticker = ?
          AND date < ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (ticker, signal_date),
    ).fetchone()
    return None if row is None else str(row["state"])


def _transition(conn: sqlite3.Connection, *, ticker: str, signal_date: str) -> dict[str, Any] | None:
    if not _table_exists(conn, "rc_transition"):
        return None
    row = conn.execute(
        """
        SELECT from_state, to_state, reasons_json
        FROM rc_transition
        WHERE ticker = ?
          AND date = ?
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (ticker, signal_date),
    ).fetchone()
    if row is None:
        return None
    return {
        "from_state": row["from_state"],
        "to_state": row["to_state"],
        "reason_codes": _parse_json_list(row["reasons_json"]),
    }


def _signals(conn: sqlite3.Connection, *, ticker: str, signal_date: str) -> list[str]:
    if not _table_exists(conn, "rc_signal_daily"):
        return []
    row = conn.execute(
        """
        SELECT signal_keys_json
        FROM rc_signal_daily
        WHERE ticker = ?
          AND date = ?
        LIMIT 1
        """,
        (ticker, signal_date),
    ).fetchone()
    if row is None:
        return []
    return _parse_json_list(row["signal_keys_json"])


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table_name,)).fetchone()
    return row is not None


def _table_has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    if not _table_exists(conn, table_name):
        return False
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    for row in rows:
        try:
            if str(row["name"]) == column_name:
                return True
        except (TypeError, KeyError, IndexError):
            if len(row) > 1 and str(row[1]) == column_name:
                return True
    return False


def _parse_json_list(raw: Any) -> list[str]:
    if raw is None:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def _parse_json_dict(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _context_status(snapshot_status: str) -> str:
    if snapshot_status == STATUS_NO_AVAILABLE_TTM:
        return STATUS_NO_AVAILABLE_TTM
    if snapshot_status == STATUS_PARTIAL:
        return STATUS_PARTIAL
    return STATUS_OK


def _percentile_status(snapshot: Any, include_percentile: bool) -> str:
    if not include_percentile:
        return STATUS_DISABLED
    return STATUS_OK if snapshot.percentiles is not None else STATUS_NO_AVAILABLE_PERCENTILE


def _valuation_status(raw_status: str | None, include_valuation: bool) -> str:
    if not include_valuation:
        return STATUS_DISABLED
    return STATUS_OK if raw_status == STATUS_OK else STATUS_VALUATION_UNAVAILABLE


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _difference_count(base_rows: list[dict[str, Any]], enriched_rows: list[dict[str, Any]], field: str) -> int:
    return sum(1 for base, enriched in zip(base_rows, enriched_rows) if base.get(field) != enriched.get(field))


def _rounded_stat(values: list[float], kind: str) -> float | None:
    if not values:
        return None
    if kind == "median":
        return round(median(values), 6)
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(0.95 * (len(ordered) - 1)))
    return round(ordered[index], 6)


def _flatten_for_csv(row: Mapping[str, Any]) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                flattened[f"{key}.{nested_key}"] = json.dumps(nested_value, sort_keys=True) if isinstance(nested_value, (dict, list)) else nested_value
        elif isinstance(value, list):
            flattened[key] = json.dumps(value, sort_keys=True)
        else:
            flattened[key] = value
    return flattened
