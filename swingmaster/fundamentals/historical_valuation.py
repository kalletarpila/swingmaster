from __future__ import annotations

import csv
import json
import random
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping

from swingmaster.cli.run_fundamental_valuation import (
    build_valuation_row,
    load_quarterly_ev_inputs,
    load_ttm_rows,
    resolve_created_at_utc,
)
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker, repository_root
from swingmaster.fundamentals.historical_percentile import calculate_ticker_historical_percentile_as_of
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path
from swingmaster.fundamentals.ttm_effective_date import select_latest_ttm_as_of


DEFAULT_PRICE_DB = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
HISTORICAL_VALUATION_POLICY = "LATEST_AVAILABLE_TTM_AND_CLOSE_AS_OF_DATE"
STATUS_OK = "OK"
STATUS_NO_AVAILABLE_TTM = "NO_AVAILABLE_TTM"
STATUS_NO_PRICE_AVAILABLE = "NO_PRICE_AVAILABLE"
STATUS_MISSING_REQUIRED_INPUT = "MISSING_REQUIRED_INPUT"
STATUS_INVALID_DENOMINATOR = "INVALID_DENOMINATOR"
STATUS_CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
PRICE_STATUS_EXACT = "EXACT_TRADING_DATE"
PRICE_STATUS_PREVIOUS = "PREVIOUS_TRADING_DATE"
PRICE_STATUS_NO_PRICE = "NO_PRICE_AVAILABLE"
MATERIAL_EV_EBIT_THRESHOLD = 2.0
MATERIAL_FCF_YIELD_THRESHOLD = 0.01


@dataclass(frozen=True)
class HistoricalPriceSelection:
    requested_as_of_date: str
    selected_price_date: str | None
    close_price: float | None
    price_selection_status: str


@dataclass(frozen=True)
class HistoricalValuationResult:
    ticker: str
    requested_as_of_date: str
    selected_price_date: str | None
    close_price: float | None
    price_selection_status: str
    source_ttm_as_of_date: str | None
    source_ttm_effective_trading_date: str | None
    source_ttm_status: str
    valuation_status: str
    valuation_policy: str
    missing_input_fields: list[str]
    valuation_row: dict[str, Any] | None
    current_comparison_row: dict[str, Any] | None = None
    historical_percentile: dict[str, Any] | None = None


@dataclass(frozen=True)
class HistoricalValuationAuditSummary:
    ticker_date_rows_evaluated: int
    rows_with_different_ttm_selection: int
    rows_with_no_available_ttm: int
    rows_with_no_price: int
    material_pe_difference_count: int
    material_ev_ebitda_difference_count: int
    material_fcf_yield_difference_count: int
    median_absolute_metric_differences: dict[str, float | None]
    p95_absolute_metric_differences: dict[str, float | None]
    elapsed_seconds: float


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def select_historical_close_as_of(
    price_conn: sqlite3.Connection,
    *,
    ticker: str,
    market: str,
    as_of_date: str,
) -> HistoricalPriceSelection:
    row = price_conn.execute(
        """
        SELECT pvm, close
        FROM osakedata
        WHERE osake = ?
          AND market = ?
          AND pvm <= ?
          AND close IS NOT NULL
        ORDER BY pvm DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker), market, as_of_date),
    ).fetchone()
    if row is None:
        return HistoricalPriceSelection(as_of_date, None, None, PRICE_STATUS_NO_PRICE)
    selected_date = str(row[0])
    return HistoricalPriceSelection(
        requested_as_of_date=as_of_date,
        selected_price_date=selected_date,
        close_price=float(row[1]),
        price_selection_status=PRICE_STATUS_EXACT if selected_date == as_of_date else PRICE_STATUS_PREVIOUS,
    )


def calculate_historical_valuation_as_of(
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str,
    include_current_comparison: bool = False,
    run_id: str = "HISTORICAL_VALUATION_READ_ONLY",
    created_at_utc: str | None = None,
) -> HistoricalValuationResult:
    normalized = normalize_ticker(ticker)
    ttm_selection = select_latest_ttm_as_of(fundamentals_conn, normalized, as_of_date)
    price = select_historical_close_as_of(price_conn, ticker=normalized, market=market, as_of_date=as_of_date)
    current_comparison = None
    if include_current_comparison:
        current_comparison = _current_style_valuation_row(
            fundamentals_conn,
            price_conn,
            ticker=normalized,
            as_of_date=as_of_date,
            market=market,
            run_id=run_id,
            created_at_utc=created_at_utc,
        )
    if not ttm_selection.found or ttm_selection.row is None:
        return HistoricalValuationResult(
            ticker=normalized,
            requested_as_of_date=as_of_date,
            selected_price_date=price.selected_price_date,
            close_price=price.close_price,
            price_selection_status=price.price_selection_status,
            source_ttm_as_of_date=None,
            source_ttm_effective_trading_date=None,
            source_ttm_status=STATUS_NO_AVAILABLE_TTM,
            valuation_status=STATUS_NO_AVAILABLE_TTM,
            valuation_policy=HISTORICAL_VALUATION_POLICY,
            missing_input_fields=["effective_ttm"],
            valuation_row=None,
            current_comparison_row=current_comparison,
        )
    ttm_row = _dict_to_sqlite_like_row(ttm_selection.row)
    quarterly_row = load_quarterly_ev_inputs(
        fundamentals_conn,
        normalized,
        str(ttm_selection.row["latest_period_end_date"]),
    )
    valuation_row = build_valuation_row(
        valuation_date=as_of_date,
        ttm_row=ttm_row,
        quarterly_row=quarterly_row,
        close_price=price.close_price,
        run_id=run_id,
        created_at_utc=_resolve_created_at_utc(created_at_utc),
    )
    return HistoricalValuationResult(
        ticker=normalized,
        requested_as_of_date=as_of_date,
        selected_price_date=price.selected_price_date,
        close_price=price.close_price,
        price_selection_status=price.price_selection_status,
        source_ttm_as_of_date=str(ttm_selection.row["as_of_date"]),
        source_ttm_effective_trading_date=_optional_str(ttm_selection.row.get("effective_trading_date")),
        source_ttm_status=STATUS_OK,
        valuation_status=_map_current_status(str(valuation_row["valuation_status"])),
        valuation_policy=HISTORICAL_VALUATION_POLICY,
        missing_input_fields=_missing_input_fields(valuation_row),
        valuation_row=valuation_row,
        current_comparison_row=current_comparison,
    )


def calculate_historical_valuation_with_percentiles_as_of(
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str,
    percentile_conn: sqlite3.Connection | None = None,
    include_current_comparison: bool = False,
) -> HistoricalValuationResult:
    result = calculate_historical_valuation_as_of(
        fundamentals_conn,
        price_conn,
        ticker=ticker,
        as_of_date=as_of_date,
        market=market,
        include_current_comparison=include_current_comparison,
    )
    if percentile_conn is None:
        return result
    percentile = calculate_ticker_historical_percentile_as_of(
        fundamentals_conn,
        percentile_conn,
        ticker=ticker,
        target_date=as_of_date,
        market=market,
    )
    return HistoricalValuationResult(
        **{**asdict(result), "historical_percentile": asdict(percentile)}
    )


def audit_historical_valuation(
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    tickers: list[str],
    as_of_date: str,
    market: str,
) -> tuple[HistoricalValuationAuditSummary, list[dict[str, Any]]]:
    started = time.perf_counter()
    details: list[dict[str, Any]] = []
    ev_diffs: list[float] = []
    fcf_yield_diffs: list[float] = []
    for ticker in tickers:
        safe = calculate_historical_valuation_as_of(
            fundamentals_conn,
            price_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=market,
            include_current_comparison=True,
        )
        current = safe.current_comparison_row
        safe_row = safe.valuation_row
        ev_diff = _absolute_metric_difference(safe_row, current, "valuation_ev_ebit")
        fcf_diff = _absolute_metric_difference(safe_row, current, "valuation_fcf_yield")
        if ev_diff is not None:
            ev_diffs.append(ev_diff)
        if fcf_diff is not None:
            fcf_yield_diffs.append(fcf_diff)
        current_ttm = None if current is None else current.get("valuation_fundamental_as_of_date")
        safe_ttm = safe.source_ttm_as_of_date
        details.append(
            {
                "ticker": normalize_ticker(ticker),
                "as_of_date": as_of_date,
                "selected_price_date": safe.selected_price_date,
                "current_style_ttm_as_of_date": current_ttm,
                "safe_ttm_as_of_date": safe_ttm,
                "ttm_selection_differs": current_ttm != safe_ttm,
                "valuation_metrics_compared": "valuation_ev_ebit,valuation_fcf_yield",
                "material_metric_difference": (ev_diff is not None and ev_diff >= MATERIAL_EV_EBIT_THRESHOLD)
                or (fcf_diff is not None and fcf_diff >= MATERIAL_FCF_YIELD_THRESHOLD),
                "missing_data_status": safe.valuation_status,
                "safe_valuation_ev_ebit": None if safe_row is None else safe_row.get("valuation_ev_ebit"),
                "current_style_valuation_ev_ebit": None if current is None else current.get("valuation_ev_ebit"),
                "absolute_valuation_ev_ebit_difference": ev_diff,
                "safe_valuation_fcf_yield": None if safe_row is None else safe_row.get("valuation_fcf_yield"),
                "current_style_valuation_fcf_yield": None if current is None else current.get("valuation_fcf_yield"),
                "absolute_valuation_fcf_yield_difference": fcf_diff,
            }
        )
    summary = HistoricalValuationAuditSummary(
        ticker_date_rows_evaluated=len(details),
        rows_with_different_ttm_selection=sum(1 for row in details if row["ttm_selection_differs"]),
        rows_with_no_available_ttm=sum(1 for row in details if row["missing_data_status"] == STATUS_NO_AVAILABLE_TTM),
        rows_with_no_price=sum(1 for row in details if row["missing_data_status"] == STATUS_NO_PRICE_AVAILABLE),
        material_pe_difference_count=0,
        material_ev_ebitda_difference_count=sum(1 for value in ev_diffs if value >= MATERIAL_EV_EBIT_THRESHOLD),
        material_fcf_yield_difference_count=sum(1 for value in fcf_yield_diffs if value >= MATERIAL_FCF_YIELD_THRESHOLD),
        median_absolute_metric_differences={
            "valuation_ev_ebit": _median_or_none(ev_diffs),
            "valuation_fcf_yield": _median_or_none(fcf_yield_diffs),
        },
        p95_absolute_metric_differences={
            "valuation_ev_ebit": _percentile(ev_diffs, 0.95),
            "valuation_fcf_yield": _percentile(fcf_yield_diffs, 0.95),
        },
        elapsed_seconds=round(time.perf_counter() - started, 4),
    )
    return summary, details


def load_market_tickers(price_conn: sqlite3.Connection, *, market: str, limit: int | None = None) -> list[str]:
    sql = """
        SELECT DISTINCT osake
        FROM osakedata
        WHERE market = ?
        ORDER BY osake ASC
    """
    params: list[Any] = [market]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return [normalize_ticker(row[0]) for row in price_conn.execute(sql, params).fetchall()]


def write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def write_csv_atomic(path: Path, rows: list[Mapping[str, Any]]) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(resolved)


def default_output_root() -> Path:
    return repository_root() / "temp" / "fundamental_historical_valuation" / utc_timestamp()


def asdict_result(result: HistoricalValuationResult | HistoricalValuationAuditSummary) -> dict[str, Any]:
    return asdict(result)


def default_db_paths() -> tuple[Path, Path]:
    return default_fundamentals_usa_db_path(), DEFAULT_PRICE_DB


def _current_style_valuation_row(
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str,
    run_id: str,
    created_at_utc: str | None,
) -> dict[str, Any] | None:
    rows = load_ttm_rows(fundamentals_conn, as_of_date, ticker, None)
    if not rows:
        return None
    ttm_row = rows[0]
    quarterly_row = load_quarterly_ev_inputs(fundamentals_conn, str(ttm_row["ticker"]), str(ttm_row["latest_period_end_date"]))
    price = select_historical_close_as_of(price_conn, ticker=ticker, market=market, as_of_date=as_of_date)
    return build_valuation_row(
        valuation_date=as_of_date,
        ttm_row=ttm_row,
        quarterly_row=quarterly_row,
        close_price=price.close_price,
        run_id=run_id,
        created_at_utc=_resolve_created_at_utc(created_at_utc),
    )


def _dict_to_sqlite_like_row(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return row


def _resolve_created_at_utc(value: str | None) -> str:
    return value if value is not None else resolve_created_at_utc()


def _missing_input_fields(row: Mapping[str, Any]) -> list[str]:
    status = str(row["valuation_status"])
    mapping = {
        "MISSING_PRICE": ["close_price"],
        "MISSING_SHARES": ["shares_outstanding"],
        "INVALID_EBIT": ["ebit_ttm"],
        "MISSING_FCF": ["fcf_ttm"],
        "INVALID_MARKET_CAP": ["market_cap"],
        "MISSING_EBIT_MARGIN": ["ebit_margin_ttm"],
        "TOO_STALE_FUNDAMENTALS": ["valuation_fundamental_staleness_days"],
    }
    return mapping.get(status, [])


def _map_current_status(status: str) -> str:
    if status in {"OK", "STALE_FUNDAMENTALS"}:
        return STATUS_OK
    if status in {"MISSING_PRICE"}:
        return STATUS_NO_PRICE_AVAILABLE
    if status in {"INVALID_EBIT", "INVALID_MARKET_CAP"}:
        return STATUS_INVALID_DENOMINATOR
    return STATUS_MISSING_REQUIRED_INPUT


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _absolute_metric_difference(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None, key: str) -> float | None:
    if left is None or right is None:
        return None
    if left.get(key) is None or right.get(key) is None:
        return None
    return abs(float(left[key]) - float(right[key]))


def _median_or_none(values: list[float]) -> float | None:
    return float(median(values)) if values else None


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    index = int(round((len(values) - 1) * fraction))
    return sorted(values)[index]


def sample_tickers(tickers: list[str], *, sample_size: int | None, random_seed: int) -> list[str]:
    if sample_size is None or sample_size >= len(tickers):
        return tickers
    rng = random.Random(random_seed)
    return sorted(rng.sample(tickers, sample_size))
