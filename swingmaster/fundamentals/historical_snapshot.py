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
from typing import Any, Mapping

from swingmaster.cli.run_fundamental_ticker_snapshot import load_quarterly_rows
from swingmaster.cli.run_fundamental_valuation import load_ttm_rows
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker, repository_root
from swingmaster.fundamentals.historical_percentile import (
    DEFAULT_METRIC as DEFAULT_PERCENTILE_METRIC,
    STATUS_OK as PERCENTILE_STATUS_OK,
    calculate_ticker_historical_percentile_as_of,
)
from swingmaster.fundamentals.historical_valuation import (
    DEFAULT_PRICE_DB,
    STATUS_OK as VALUATION_STATUS_OK,
    calculate_historical_valuation_as_of,
)
from swingmaster.fundamentals.score_effective_date import score_row_predicate, select_latest_score_as_of
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path
from swingmaster.fundamentals.ttm_effective_date import select_latest_ttm_as_of


HISTORICAL_SNAPSHOT_POLICY = "LATEST_AVAILABLE_EFFECTIVE_DATED_FUNDAMENTALS_AS_OF_DATE"
STATUS_OK = "OK"
STATUS_PARTIAL = "PARTIAL"
STATUS_NO_AVAILABLE_TTM = "NO_AVAILABLE_TTM"
STATUS_NO_AVAILABLE_SCORE = "NO_AVAILABLE_SCORE"
STATUS_NO_AVAILABLE_PERCENTILE = "NO_AVAILABLE_PERCENTILE"
STATUS_NO_PRICE_AVAILABLE = "NO_PRICE_AVAILABLE"
STATUS_VALUATION_UNAVAILABLE = "VALUATION_UNAVAILABLE"
MATERIAL_PERCENTILE_THRESHOLD = 5.0
MATERIAL_VALUATION_EV_EBIT_THRESHOLD = 2.0
MATERIAL_VALUATION_FCF_YIELD_THRESHOLD = 0.01

TTM_FIELDS = (
    "ticker",
    "as_of_date",
    "latest_period_end_date",
    "lifecycle_class",
    "fundamental_score",
    "fundamental_score_lifecycle",
    "growth_component",
    "margin_component",
    "margin_trend_component",
    "fcf_component",
    "consistency_component",
    "leverage_component",
    "dilution_component",
    "revenue_growth_ttm_yoy",
    "ebit_margin_ttm",
    "ebit_margin_trend_4q",
    "fcf_margin_ttm",
    "fcf_margin_trend_4q",
    "net_debt_to_ebitda",
    "share_dilution_yoy",
)
SCORE_FIELDS = (
    "as_of_date",
    "score_effective_trading_date",
    "score_rule_lifecycle",
    "lifecycle_class",
    "fundamental_score_lifecycle",
    "growth_component",
    "margin_component",
    "margin_trend_component",
    "fcf_component",
    "consistency_component",
    "leverage_component",
    "dilution_component",
)
QUARTERLY_FIELDS = (
    "period_end_date",
    "revenue",
    "operating_income",
    "free_cashflow",
    "shares_outstanding",
    "total_debt",
)
PERCENTILE_METRICS = (
    "fundamental_score_percentile_global",
    "fundamental_score_percentile_sector",
    "fundamental_score_percentile_industry",
    "fundamental_score_percentile_blended",
    "fundamental_score_percentile_blended_lifecycle_weighted",
    "growth_pct_global",
    "margin_pct_global",
    "margin_trend_pct_global",
    "fcf_pct_global",
    "consistency_pct_global",
    "leverage_pct_global",
    "dilution_pct_global",
)
VALUATION_METRICS = (
    "valuation_ev_ebit",
    "valuation_fcf_yield",
    "valuation_ebit_margin",
    "valuation_bucket",
    "valuation_status",
    "valuation_model_version",
    "market_cap",
    "enterprise_value",
)


@dataclass(frozen=True)
class HistoricalFundamentalSnapshot:
    ticker: str
    requested_as_of_date: str
    snapshot_status: str
    fundamentals_policy: str
    source_ttm_as_of_date: str | None
    latest_component_period_end: str | None
    source_ttm_effective_trading_date: str | None
    source_score_as_of_date: str | None
    source_score_effective_trading_date: str | None
    selected_price_date: str | None
    close_price: float | None
    score_percentile_population_size: int | None
    valuation_status: str | None
    missing_components: list[str]
    warnings: list[str]
    ttm: dict[str, Any] | None
    score: dict[str, Any] | None
    quarterly: dict[str, Any] | None
    percentiles: dict[str, Any] | None
    valuation: dict[str, Any] | None
    availability: dict[str, Any]
    comparison: dict[str, Any] | None = None


@dataclass(frozen=True)
class HistoricalSnapshotAuditSummary:
    ticker_date_rows_evaluated: int
    ok_snapshots: int
    partial_snapshots: int
    no_available_ttm: int
    no_available_score: int
    percentile_unavailable: int
    valuation_unavailable: int
    current_vs_safe_ttm_diff_count: int
    current_vs_safe_score_diff_count: int
    material_percentile_diff_count: int
    material_valuation_diff_count: int
    median_snapshot_query_seconds: float | None
    p95_snapshot_query_seconds: float | None
    elapsed_seconds: float


def build_historical_fundamental_snapshot(
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str,
    include_percentiles: bool = True,
    include_valuation: bool = True,
    include_current_comparison: bool = False,
) -> HistoricalFundamentalSnapshot:
    normalized = normalize_ticker(ticker)
    ttm_selection = select_latest_ttm_as_of(fundamentals_conn, normalized, as_of_date)
    score_selection = select_latest_score_as_of(fundamentals_conn, normalized, as_of_date)
    missing: list[str] = []
    warnings: list[str] = []
    if not ttm_selection.found or ttm_selection.row is None:
        missing.append("ttm")
        comparison = _current_style_comparison(
            fundamentals_conn,
            price_conn,
            ticker=normalized,
            as_of_date=as_of_date,
            market=market,
            safe_ttm_as_of_date=None,
            safe_score_as_of_date=None,
            percentile_row=None,
            valuation_row=None,
        ) if include_current_comparison else None
        return HistoricalFundamentalSnapshot(
            ticker=normalized,
            requested_as_of_date=as_of_date,
            snapshot_status=STATUS_NO_AVAILABLE_TTM,
            fundamentals_policy=HISTORICAL_SNAPSHOT_POLICY,
            source_ttm_as_of_date=None,
            latest_component_period_end=None,
            source_ttm_effective_trading_date=None,
            source_score_as_of_date=None,
            source_score_effective_trading_date=None,
            selected_price_date=None,
            close_price=None,
            score_percentile_population_size=None,
            valuation_status=None,
            missing_components=missing,
            warnings=warnings,
            ttm=None,
            score=None,
            quarterly=None,
            percentiles=None,
            valuation=None,
            availability=_availability(False, False, False, False),
            comparison=comparison,
        )

    ttm_row = dict(ttm_selection.row)
    score_row = dict(score_selection.row) if score_selection.found and score_selection.row is not None else None
    if score_row is None:
        missing.append("score")

    quarterly = _load_selected_quarterly(fundamentals_conn, normalized, str(ttm_row["latest_period_end_date"]))
    if quarterly is None:
        warnings.append("QUARTERLY_ROW_UNAVAILABLE")

    percentile_payload: dict[str, Any] | None = None
    percentile_status: str | None = None
    percentile_population: int | None = None
    if include_percentiles:
        percentile = calculate_ticker_historical_percentile_as_of(
            fundamentals_conn,
            price_conn,
            ticker=normalized,
            target_date=as_of_date,
            market=market,
        )
        percentile_status = percentile.status
        percentile_population = percentile.peer_population_size
        if percentile.status == PERCENTILE_STATUS_OK and percentile.percentile_row is not None:
            percentile_payload = {
                "status": percentile.status,
                "percentile_metrics_requested": list(PERCENTILE_METRICS),
                "percentile_metrics_available": [
                    metric for metric in PERCENTILE_METRICS if percentile.percentile_row.get(metric) is not None
                ],
                "peer_population_size": percentile.peer_population_size,
                "excluded_peer_count": percentile.excluded_no_available_score_count + percentile.excluded_null_effective_score_count,
                "row": _project(percentile.percentile_row, ("ticker", "target_date", "as_of_date", "score_effective_trading_date", *PERCENTILE_METRICS)),
            }
        else:
            missing.append("percentiles")
    else:
        warnings.append("PERCENTILES_DISABLED")

    valuation_payload: dict[str, Any] | None = None
    valuation_status: str | None = None
    selected_price_date: str | None = None
    close_price: float | None = None
    if include_valuation:
        valuation = calculate_historical_valuation_as_of(
            fundamentals_conn,
            price_conn,
            ticker=normalized,
            as_of_date=as_of_date,
            market=market,
            include_current_comparison=include_current_comparison,
        )
        valuation_status = valuation.valuation_status
        selected_price_date = valuation.selected_price_date
        close_price = valuation.close_price
        if valuation.valuation_status == VALUATION_STATUS_OK and valuation.valuation_row is not None:
            valuation_payload = {
                "status": valuation.valuation_status,
                "selected_price_date": valuation.selected_price_date,
                "close_price": valuation.close_price,
                "price_selection_status": valuation.price_selection_status,
                "row": _project(valuation.valuation_row, VALUATION_METRICS),
            }
        else:
            missing.append("valuation")
            if valuation.valuation_status == STATUS_NO_PRICE_AVAILABLE:
                missing.append("price")
    else:
        warnings.append("VALUATION_DISABLED")

    snapshot_status = _snapshot_status(ttm_available=True, score_available=score_row is not None, missing_components=missing)
    comparison = _current_style_comparison(
        fundamentals_conn,
        price_conn,
        ticker=normalized,
        as_of_date=as_of_date,
        market=market,
        safe_ttm_as_of_date=str(ttm_row["as_of_date"]),
        safe_score_as_of_date=None if score_row is None else str(score_row["as_of_date"]),
        percentile_row=None if percentile_payload is None else percentile_payload["row"],
        valuation_row=None if valuation_payload is None else valuation_payload["row"],
    ) if include_current_comparison else None
    return HistoricalFundamentalSnapshot(
        ticker=normalized,
        requested_as_of_date=as_of_date,
        snapshot_status=snapshot_status,
        fundamentals_policy=HISTORICAL_SNAPSHOT_POLICY,
        source_ttm_as_of_date=str(ttm_row["as_of_date"]),
        latest_component_period_end=str(ttm_row["latest_period_end_date"]),
        source_ttm_effective_trading_date=_optional_str(ttm_row.get("effective_trading_date")),
        source_score_as_of_date=None if score_row is None else str(score_row["as_of_date"]),
        source_score_effective_trading_date=None if score_row is None else _optional_str(score_row.get("score_effective_trading_date")),
        selected_price_date=selected_price_date,
        close_price=close_price,
        score_percentile_population_size=percentile_population,
        valuation_status=valuation_status,
        missing_components=sorted(set(missing)),
        warnings=warnings,
        ttm=_project(ttm_row, TTM_FIELDS),
        score=None if score_row is None else _project(score_row, SCORE_FIELDS),
        quarterly=quarterly,
        percentiles=percentile_payload,
        valuation=valuation_payload,
        availability=_availability(True, score_row is not None, percentile_payload is not None, valuation_payload is not None),
        comparison=comparison,
    )


def audit_historical_fundamental_snapshots(
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    tickers: list[str],
    as_of_date: str,
    market: str,
    include_percentiles: bool = True,
    include_valuation: bool = True,
) -> tuple[HistoricalSnapshotAuditSummary, list[dict[str, Any]]]:
    started = time.perf_counter()
    detail_rows: list[dict[str, Any]] = []
    query_times: list[float] = []
    for ticker in tickers:
        row_started = time.perf_counter()
        snapshot = build_historical_fundamental_snapshot(
            fundamentals_conn,
            price_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=market,
            include_percentiles=include_percentiles,
            include_valuation=include_valuation,
            include_current_comparison=True,
        )
        elapsed = time.perf_counter() - row_started
        query_times.append(elapsed)
        comparison = snapshot.comparison or {}
        percentile_diff = _metric_diff(
            (snapshot.percentiles or {}).get("row"),
            comparison.get("current_percentile_row"),
            DEFAULT_PERCENTILE_METRIC,
        )
        valuation_ev_diff = _metric_diff(
            (snapshot.valuation or {}).get("row"),
            comparison.get("current_valuation_row"),
            "valuation_ev_ebit",
        )
        valuation_fcf_diff = _metric_diff(
            (snapshot.valuation or {}).get("row"),
            comparison.get("current_valuation_row"),
            "valuation_fcf_yield",
        )
        detail_rows.append(
            {
                "ticker": snapshot.ticker,
                "as_of_date": as_of_date,
                "safe_snapshot_status": snapshot.snapshot_status,
                "current_ttm_as_of_date": comparison.get("current_ttm_as_of_date"),
                "safe_ttm_as_of_date": snapshot.source_ttm_as_of_date,
                "ttm_selection_differs": bool(comparison.get("ttm_selection_differs")),
                "current_score_as_of_date": comparison.get("current_score_as_of_date"),
                "safe_score_as_of_date": snapshot.source_score_as_of_date,
                "score_selection_differs": bool(comparison.get("score_selection_differs")),
                "percentile_difference_metrics": _diff_payload(percentile_diff, MATERIAL_PERCENTILE_THRESHOLD),
                "valuation_difference_metrics": {
                    "valuation_ev_ebit": _diff_payload(valuation_ev_diff, MATERIAL_VALUATION_EV_EBIT_THRESHOLD),
                    "valuation_fcf_yield": _diff_payload(valuation_fcf_diff, MATERIAL_VALUATION_FCF_YIELD_THRESHOLD),
                },
                "missing_sections": ",".join(snapshot.missing_components),
                "query_seconds": round(elapsed, 6),
            }
        )
    summary = HistoricalSnapshotAuditSummary(
        ticker_date_rows_evaluated=len(detail_rows),
        ok_snapshots=sum(1 for row in detail_rows if row["safe_snapshot_status"] == STATUS_OK),
        partial_snapshots=sum(1 for row in detail_rows if row["safe_snapshot_status"] == STATUS_PARTIAL),
        no_available_ttm=sum(1 for row in detail_rows if row["safe_snapshot_status"] == STATUS_NO_AVAILABLE_TTM),
        no_available_score=sum(1 for row in detail_rows if "score" in row["missing_sections"].split(",")),
        percentile_unavailable=sum(1 for row in detail_rows if "percentiles" in row["missing_sections"].split(",")),
        valuation_unavailable=sum(1 for row in detail_rows if "valuation" in row["missing_sections"].split(",")),
        current_vs_safe_ttm_diff_count=sum(1 for row in detail_rows if row["ttm_selection_differs"]),
        current_vs_safe_score_diff_count=sum(1 for row in detail_rows if row["score_selection_differs"]),
        material_percentile_diff_count=sum(1 for row in detail_rows if row["percentile_difference_metrics"]["material"]),
        material_valuation_diff_count=sum(
            1
            for row in detail_rows
            if row["valuation_difference_metrics"]["valuation_ev_ebit"]["material"]
            or row["valuation_difference_metrics"]["valuation_fcf_yield"]["material"]
        ),
        median_snapshot_query_seconds=_median_or_none(query_times),
        p95_snapshot_query_seconds=_percentile(query_times, 0.95),
        elapsed_seconds=round(time.perf_counter() - started, 4),
    )
    return summary, detail_rows


def load_market_tickers(price_conn: sqlite3.Connection, *, market: str, limit: int | None = None) -> list[str]:
    sql = "SELECT DISTINCT osake FROM osakedata WHERE market = ? ORDER BY osake ASC"
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
    return repository_root() / "temp" / "fundamental_historical_snapshot" / utc_timestamp()


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def asdict_snapshot(snapshot: HistoricalFundamentalSnapshot | HistoricalSnapshotAuditSummary) -> dict[str, Any]:
    return asdict(snapshot)


def default_db_paths() -> tuple[Path, Path]:
    return default_fundamentals_usa_db_path(), DEFAULT_PRICE_DB


def sample_tickers(tickers: list[str], *, sample_size: int | None, random_seed: int) -> list[str]:
    if sample_size is None or sample_size >= len(tickers):
        return tickers
    rng = random.Random(random_seed)
    return sorted(rng.sample(tickers, sample_size))


def _current_style_comparison(
    fundamentals_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str,
    safe_ttm_as_of_date: str | None,
    safe_score_as_of_date: str | None,
    percentile_row: Mapping[str, Any] | None,
    valuation_row: Mapping[str, Any] | None,
) -> dict[str, Any]:
    current_ttm = _current_style_ttm_as_of(fundamentals_conn, ticker, as_of_date)
    current_score = _current_style_score_as_of(fundamentals_conn, ticker, as_of_date)
    current_percentile = None
    if percentile_row is not None:
        percentile = calculate_ticker_historical_percentile_as_of(
            fundamentals_conn,
            price_conn,
            ticker=ticker,
            target_date=as_of_date,
            market=market,
        )
        current_percentile = percentile.current_percentile_row
    current_valuation = None
    if valuation_row is not None:
        valuation = calculate_historical_valuation_as_of(
            fundamentals_conn,
            price_conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=market,
            include_current_comparison=True,
        )
        current_valuation = valuation.current_comparison_row
    return {
        "current_ttm_as_of_date": current_ttm,
        "current_score_as_of_date": current_score,
        "ttm_selection_differs": current_ttm != safe_ttm_as_of_date,
        "score_selection_differs": current_score != safe_score_as_of_date,
        "current_percentile_row": current_percentile,
        "current_valuation_row": current_valuation,
    }


def _current_style_ttm_as_of(conn: sqlite3.Connection, ticker: str, as_of_date: str) -> str | None:
    rows = load_ttm_rows(conn, as_of_date, ticker, None)
    return None if not rows else str(rows[0]["as_of_date"])


def _current_style_score_as_of(conn: sqlite3.Connection, ticker: str, as_of_date: str) -> str | None:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            f"""
            SELECT as_of_date
            FROM rc_fundamental_ttm
            WHERE ticker = ?
              AND as_of_date <= ?
              AND {score_row_predicate()}
            ORDER BY as_of_date DESC, latest_period_end_date DESC, rowid DESC
            LIMIT 1
            """,
            (normalize_ticker(ticker), as_of_date),
        ).fetchone()
    finally:
        conn.row_factory = previous_row_factory
    return None if row is None else str(row["as_of_date"])


def _load_selected_quarterly(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> dict[str, Any] | None:
    rows = load_quarterly_rows(conn, ticker, [period_end_date])
    row = rows.get((normalize_ticker(ticker), period_end_date))
    return None if row is None else _project(dict(row), QUARTERLY_FIELDS)


def _snapshot_status(*, ttm_available: bool, score_available: bool, missing_components: list[str]) -> str:
    if not ttm_available:
        return STATUS_NO_AVAILABLE_TTM
    if not score_available:
        return STATUS_PARTIAL
    optional_missing = {component for component in missing_components if component not in {"ttm", "score"}}
    return STATUS_PARTIAL if optional_missing else STATUS_OK


def _availability(ttm: bool, score: bool, percentile: bool, valuation: bool) -> dict[str, Any]:
    return {
        "ttm_available": ttm,
        "score_available": score,
        "percentile_available": percentile,
        "valuation_available": valuation,
        "required_components": ["ttm", "score"],
        "optional_components": ["percentiles", "valuation"],
    }


def _project(row: Mapping[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: row.get(field) for field in fields}


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _metric_diff(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None, key: str) -> float | None:
    if left is None or right is None:
        return None
    if left.get(key) is None or right.get(key) is None:
        return None
    return abs(float(left[key]) - float(right[key]))


def _diff_payload(value: float | None, threshold: float) -> dict[str, Any]:
    return {"absolute_difference": value, "material_threshold": threshold, "material": value is not None and value >= threshold}


def _median_or_none(values: list[float]) -> float | None:
    return float(median(values)) if values else None


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    index = int(round((len(values) - 1) * fraction))
    return sorted(values)[index]
