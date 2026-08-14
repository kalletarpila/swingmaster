from __future__ import annotations

import csv
import json
import sqlite3
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker, repository_root
from swingmaster.fundamentals.score_percentile import (
    FUND_SCORE_PERCENTILE_V2_PRE,
    PercentileSnapshotRow,
    build_percentile_rows,
    load_latest_percentile_snapshot,
    resolve_created_at_utc,
    resolve_min_universe_size,
)
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


HISTORICAL_PERCENTILE_POLICY = "LATEST_AVAILABLE_PEER_SCORE_AS_OF_DATE"
STATUS_OK = "OK"
STATUS_NO_AVAILABLE_SCORE = "NO_AVAILABLE_SCORE"
STATUS_UNIVERSE_TOO_SMALL = "UNIVERSE_TOO_SMALL"
DEFAULT_OSAKEDATA_DB = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
DEFAULT_METRIC = "fundamental_score_percentile_blended_lifecycle_weighted"
MATERIAL_DIFFERENCE_THRESHOLD = 5.0


@dataclass(frozen=True)
class HistoricalPeerScoreRow:
    ticker: str
    as_of_date: str
    score_effective_trading_date: str
    revenue_growth_ttm_yoy: float | None
    ebit_margin_ttm: float | None
    ebit_margin_trend_4q: float | None
    fcf_margin_ttm: float | None
    net_debt_to_ebit: float | None
    share_dilution_yoy: float | None
    consistency_component_lifecycle: float | None
    fundamental_score_lifecycle: float
    lifecycle_class: str | None
    score_rule_lifecycle: str | None
    sector: str | None
    industry: str | None


@dataclass(frozen=True)
class HistoricalPopulationCounts:
    candidate_score_tickers: int
    selected_peer_count: int
    excluded_null_effective_score_count: int
    excluded_no_available_score_count: int
    sector_count: int
    industry_count: int


@dataclass(frozen=True)
class TickerHistoricalPercentileResult:
    status: str
    ticker: str
    target_date: str
    market: str
    population_date_policy: str
    metric: str
    ticker_score_period: str | None
    ticker_score_effective_trading_date: str | None
    peer_population_size: int
    excluded_null_effective_score_count: int
    excluded_no_available_score_count: int
    percentile_row: dict[str, Any] | None
    current_percentile_row: dict[str, Any] | None
    peers: list[dict[str, Any]] | None = None


@dataclass(frozen=True)
class HistoricalAuditSummary:
    dates_evaluated: int
    ticker_date_rows_evaluated: int
    current_vs_safe_percentile_diff_count: int
    material_percentile_diff_count: int
    median_absolute_percentile_difference: float | None
    p95_absolute_percentile_difference: float | None
    maximum_absolute_percentile_difference: float | None
    peer_population_median: int | None
    peer_population_minimum: int | None
    peer_population_maximum: int | None
    excluded_no_available_score_count: int
    excluded_null_effective_score_count: int
    material_difference_threshold: float
    metric: str
    elapsed_seconds: float


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def select_peer_scores_as_of(
    fundamentals_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    *,
    target_date: str,
    market: str,
    sector: str | None = None,
    industry: str | None = None,
) -> tuple[list[HistoricalPeerScoreRow], HistoricalPopulationCounts]:
    previous_row_factory = fundamentals_conn.row_factory
    fundamentals_conn.row_factory = sqlite3.Row
    try:
        score_tickers = _score_ticker_count(fundamentals_conn)
        null_effective = _null_effective_ticker_count(fundamentals_conn)
        rows = fundamentals_conn.execute(
            """
            WITH ranked AS (
                SELECT
                    t.ticker,
                    t.as_of_date,
                    t.score_effective_trading_date,
                    t.revenue_growth_ttm_yoy,
                    t.ebit_margin_ttm,
                    t.ebit_margin_trend_4q,
                    t.fcf_margin_ttm,
                    COALESCE(t.net_debt_to_ebitda, t.net_debt_to_ebit) AS net_debt_to_ebit,
                    t.share_dilution_yoy,
                    t.consistency_component_lifecycle,
                    t.fundamental_score_lifecycle,
                    t.lifecycle_class,
                    t.score_rule_lifecycle,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.ticker
                        ORDER BY t.score_effective_trading_date DESC,
                                 t.as_of_date DESC,
                                 t.score_rule_lifecycle DESC,
                                 t.rowid DESC
                    ) AS rn
                FROM rc_fundamental_ttm t
                WHERE t.fundamental_score_lifecycle IS NOT NULL
                  AND t.score_effective_trading_date IS NOT NULL
                  AND t.score_effective_trading_date <= ?
            )
            SELECT *
            FROM ranked
            WHERE rn = 1
            ORDER BY ticker ASC
            """,
            (target_date,),
        ).fetchall()
    finally:
        fundamentals_conn.row_factory = previous_row_factory

    metadata_by_ticker = _filtered_metadata(osakedata_conn, market=market, sector=sector, industry=industry)
    selected: list[HistoricalPeerScoreRow] = []
    for row in rows:
        ticker = str(row["ticker"])
        metadata = metadata_by_ticker.get(ticker)
        if metadata is None and (sector is not None or industry is not None):
            continue
        selected.append(
            HistoricalPeerScoreRow(
                ticker=ticker,
                as_of_date=str(row["as_of_date"]),
                score_effective_trading_date=str(row["score_effective_trading_date"]),
                revenue_growth_ttm_yoy=_coerce_optional_float(row["revenue_growth_ttm_yoy"]),
                ebit_margin_ttm=_coerce_optional_float(row["ebit_margin_ttm"]),
                ebit_margin_trend_4q=_coerce_optional_float(row["ebit_margin_trend_4q"]),
                fcf_margin_ttm=_coerce_optional_float(row["fcf_margin_ttm"]),
                net_debt_to_ebit=_coerce_optional_float(row["net_debt_to_ebit"]),
                share_dilution_yoy=_coerce_optional_float(row["share_dilution_yoy"]),
                consistency_component_lifecycle=_coerce_optional_float(row["consistency_component_lifecycle"]),
                fundamental_score_lifecycle=float(row["fundamental_score_lifecycle"]),
                lifecycle_class=str(row["lifecycle_class"]) if row["lifecycle_class"] is not None else None,
                score_rule_lifecycle=str(row["score_rule_lifecycle"]) if row["score_rule_lifecycle"] is not None else None,
                sector=metadata["sector"] if metadata is not None else None,
                industry=metadata["industry"] if metadata is not None else None,
            )
        )
    counts = HistoricalPopulationCounts(
        candidate_score_tickers=score_tickers,
        selected_peer_count=len(selected),
        excluded_null_effective_score_count=null_effective,
        excluded_no_available_score_count=max(0, score_tickers - null_effective - len(rows)),
        sector_count=len({row.sector for row in selected if row.sector is not None}),
        industry_count=len({row.industry for row in selected if row.industry is not None}),
    )
    return selected, counts


def calculate_historical_percentiles_as_of(
    fundamentals_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    *,
    target_date: str,
    market: str,
    sector: str | None = None,
    industry: str | None = None,
    rule_id: str = FUND_SCORE_PERCENTILE_V2_PRE,
    run_id: str = "HISTORICAL_PERCENTILE_READ_ONLY",
    created_at_utc: str | None = None,
) -> tuple[list[dict[str, Any]], HistoricalPopulationCounts]:
    peer_rows, counts = select_peer_scores_as_of(
        fundamentals_conn,
        osakedata_conn,
        target_date=target_date,
        market=market,
        sector=sector,
        industry=industry,
    )
    if counts.selected_peer_count < resolve_min_universe_size(market):
        return [], counts
    snapshot_rows = [_to_snapshot_row(row) for row in peer_rows]
    percentile_rows = build_percentile_rows(
        snapshot_rows=snapshot_rows,
        target_date=target_date,
        rule_id=rule_id,
        run_id=run_id,
        created_at_utc=resolve_created_at_utc(created_at_utc),
        market=market,
    )
    effective_by_ticker = {row.ticker: row.score_effective_trading_date for row in peer_rows}
    for row in percentile_rows:
        row["population_date_policy"] = HISTORICAL_PERCENTILE_POLICY
        row["score_effective_trading_date"] = effective_by_ticker.get(str(row["ticker"]))
    return percentile_rows, counts


def calculate_ticker_historical_percentile_as_of(
    fundamentals_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    *,
    ticker: str,
    target_date: str,
    market: str,
    sector: str | None = None,
    industry: str | None = None,
    metric: str = DEFAULT_METRIC,
    include_peers: bool = False,
) -> TickerHistoricalPercentileResult:
    normalized = normalize_ticker(ticker)
    start = time.perf_counter()
    percentile_rows, counts = calculate_historical_percentiles_as_of(
        fundamentals_conn,
        osakedata_conn,
        target_date=target_date,
        market=market,
        sector=sector,
        industry=industry,
    )
    by_ticker = {str(row["ticker"]): row for row in percentile_rows}
    percentile_row = by_ticker.get(normalized)
    current_row = _current_percentile_for_ticker(
        fundamentals_conn,
        osakedata_conn,
        ticker=normalized,
        target_date=target_date,
        market=market,
        metric=metric,
    )
    own_available = _own_score_available_as_of(fundamentals_conn, ticker=normalized, target_date=target_date)
    peers = None
    if include_peers:
        peers = sorted(
            (
                {
                    "ticker": str(row["ticker"]),
                    "as_of_date": row.get("as_of_date"),
                    "score_effective_trading_date": row.get("score_effective_trading_date"),
                    metric: row.get(metric),
                }
                for row in percentile_rows
            ),
            key=lambda item: str(item["ticker"]),
        )
    if percentile_row is None:
        _elapsed_marker(start)
        return TickerHistoricalPercentileResult(
            status=STATUS_NO_AVAILABLE_SCORE
            if own_available is None
            else STATUS_UNIVERSE_TOO_SMALL
            if counts.selected_peer_count < resolve_min_universe_size(market)
            else STATUS_NO_AVAILABLE_SCORE,
            ticker=normalized,
            target_date=target_date,
            market=market,
            population_date_policy=HISTORICAL_PERCENTILE_POLICY,
            metric=metric,
            ticker_score_period=None,
            ticker_score_effective_trading_date=None,
            peer_population_size=counts.selected_peer_count,
            excluded_null_effective_score_count=counts.excluded_null_effective_score_count,
            excluded_no_available_score_count=counts.excluded_no_available_score_count,
            percentile_row=None,
            current_percentile_row=current_row,
            peers=peers,
        )
    _elapsed_marker(start)
    return TickerHistoricalPercentileResult(
        status=STATUS_OK,
        ticker=normalized,
        target_date=target_date,
        market=market,
        population_date_policy=HISTORICAL_PERCENTILE_POLICY,
        metric=metric,
        ticker_score_period=str(percentile_row["as_of_date"]),
        ticker_score_effective_trading_date=str(percentile_row["score_effective_trading_date"]),
        peer_population_size=counts.selected_peer_count,
        excluded_null_effective_score_count=counts.excluded_null_effective_score_count,
        excluded_no_available_score_count=counts.excluded_no_available_score_count,
        percentile_row=percentile_row,
        current_percentile_row=current_row,
        peers=peers,
    )


def audit_current_vs_historical_percentiles(
    fundamentals_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    *,
    dates: list[str],
    market: str,
    metric: str = DEFAULT_METRIC,
    sample_size: int | None = None,
    material_threshold: float = MATERIAL_DIFFERENCE_THRESHOLD,
) -> tuple[HistoricalAuditSummary, list[dict[str, Any]]]:
    started = time.perf_counter()
    detail_rows: list[dict[str, Any]] = []
    absolute_diffs: list[float] = []
    peer_population_sizes: list[int] = []
    excluded_no_available = 0
    excluded_null = 0
    for target_date in dates:
        historical_rows, counts = calculate_historical_percentiles_as_of(
            fundamentals_conn,
            osakedata_conn,
            target_date=target_date,
            market=market,
        )
        current_rows = build_percentile_rows(
            snapshot_rows=load_latest_percentile_snapshot(
                fundamentals_conn=fundamentals_conn,
                osakedata_conn=osakedata_conn,
                target_date=target_date,
                market=market,
            ),
            target_date=target_date,
            rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
            run_id="CURRENT_COMPARISON_READ_ONLY",
            created_at_utc=resolve_created_at_utc(None),
            market=market,
        )
        current_by_ticker = {str(row["ticker"]): row for row in current_rows}
        rows_to_compare = historical_rows[:sample_size] if sample_size is not None else historical_rows
        peer_population_sizes.append(counts.selected_peer_count)
        excluded_no_available += counts.excluded_no_available_score_count
        excluded_null += counts.excluded_null_effective_score_count
        for historical in rows_to_compare:
            ticker = str(historical["ticker"])
            current = current_by_ticker.get(ticker)
            if current is None:
                continue
            current_value = current.get(metric)
            historical_value = historical.get(metric)
            if current_value is None or historical_value is None:
                continue
            diff = abs(float(current_value) - float(historical_value))
            absolute_diffs.append(diff)
            detail_rows.append(
                {
                    "target_date": target_date,
                    "ticker": ticker,
                    "historical_as_of_date": historical.get("as_of_date"),
                    "historical_score_effective_trading_date": historical.get("score_effective_trading_date"),
                    "historical_value": historical_value,
                    "current_as_of_date": current.get("as_of_date"),
                    "current_value": current_value,
                    "absolute_difference": diff,
                    "material_difference": diff >= material_threshold,
                    "peer_population_size": counts.selected_peer_count,
                }
            )
    summary = HistoricalAuditSummary(
        dates_evaluated=len(dates),
        ticker_date_rows_evaluated=len(detail_rows),
        current_vs_safe_percentile_diff_count=sum(1 for value in absolute_diffs if value != 0),
        material_percentile_diff_count=sum(1 for value in absolute_diffs if value >= material_threshold),
        median_absolute_percentile_difference=float(median(absolute_diffs)) if absolute_diffs else None,
        p95_absolute_percentile_difference=_percentile(absolute_diffs, 0.95),
        maximum_absolute_percentile_difference=max(absolute_diffs) if absolute_diffs else None,
        peer_population_median=int(median(peer_population_sizes)) if peer_population_sizes else None,
        peer_population_minimum=min(peer_population_sizes) if peer_population_sizes else None,
        peer_population_maximum=max(peer_population_sizes) if peer_population_sizes else None,
        excluded_no_available_score_count=excluded_no_available,
        excluded_null_effective_score_count=excluded_null,
        material_difference_threshold=material_threshold,
        metric=metric,
        elapsed_seconds=round(time.perf_counter() - started, 4),
    )
    return summary, detail_rows


def default_recent_percentile_dates(conn: sqlite3.Connection, limit: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT target_date
        FROM rc_fundamental_score_percentile
        ORDER BY target_date DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [str(row[0]) for row in rows]


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
    return repository_root() / "temp" / "fundamental_percentile_effective_date" / utc_timestamp()


def asdict_result(result: TickerHistoricalPercentileResult | HistoricalAuditSummary | HistoricalPopulationCounts) -> dict[str, Any]:
    return asdict(result)


def _filtered_metadata(
    osakedata_conn: sqlite3.Connection,
    *,
    market: str,
    sector: str | None,
    industry: str | None,
) -> dict[str, dict[str, str | None]]:
    previous_row_factory = osakedata_conn.row_factory
    osakedata_conn.row_factory = sqlite3.Row
    clauses = ["market = ?"]
    params: list[str] = [market]
    if sector is not None:
        clauses.append("sector = ?")
        params.append(sector)
    if industry is not None:
        clauses.append("industry = ?")
        params.append(industry)
    try:
        rows = osakedata_conn.execute(
            f"""
            SELECT ticker, sector, industry
            FROM ticker_meta
            WHERE {" AND ".join(clauses)}
            ORDER BY ticker ASC
            """,
            params,
        ).fetchall()
    finally:
        osakedata_conn.row_factory = previous_row_factory
    return {
        str(row["ticker"]): {
            "sector": str(row["sector"]) if row["sector"] is not None else None,
            "industry": str(row["industry"]) if row["industry"] is not None else None,
        }
        for row in rows
    }


def _to_snapshot_row(row: HistoricalPeerScoreRow) -> PercentileSnapshotRow:
    return PercentileSnapshotRow(
        ticker=row.ticker,
        as_of_date=row.as_of_date,
        revenue_growth_ttm_yoy=row.revenue_growth_ttm_yoy,
        ebit_margin_ttm=row.ebit_margin_ttm,
        ebit_margin_trend_4q=row.ebit_margin_trend_4q,
        fcf_margin_ttm=row.fcf_margin_ttm,
        net_debt_to_ebit=row.net_debt_to_ebit,
        share_dilution_yoy=row.share_dilution_yoy,
        consistency_component_lifecycle=row.consistency_component_lifecycle,
        fundamental_score_lifecycle=row.fundamental_score_lifecycle,
        lifecycle_class=row.lifecycle_class,
        sector=row.sector,
        industry=row.industry,
    )


def _current_percentile_for_ticker(
    fundamentals_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    *,
    ticker: str,
    target_date: str,
    market: str,
    metric: str,
) -> dict[str, Any] | None:
    current_rows = build_percentile_rows(
        snapshot_rows=load_latest_percentile_snapshot(
            fundamentals_conn=fundamentals_conn,
            osakedata_conn=osakedata_conn,
            target_date=target_date,
            market=market,
        ),
        target_date=target_date,
        rule_id=FUND_SCORE_PERCENTILE_V2_PRE,
        run_id="CURRENT_COMPARISON_READ_ONLY",
        created_at_utc=resolve_created_at_utc(None),
        market=market,
    )
    for row in current_rows:
        if str(row["ticker"]) == ticker:
            return {
                "ticker": ticker,
                "as_of_date": row.get("as_of_date"),
                metric: row.get(metric),
            }
    return None


def _own_score_available_as_of(conn: sqlite3.Connection, *, ticker: str, target_date: str) -> sqlite3.Row | None:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            """
            SELECT ticker, as_of_date, score_effective_trading_date
            FROM rc_fundamental_ttm
            WHERE ticker = ?
              AND fundamental_score_lifecycle IS NOT NULL
              AND score_effective_trading_date IS NOT NULL
              AND score_effective_trading_date <= ?
            ORDER BY score_effective_trading_date DESC,
                     as_of_date DESC,
                     score_rule_lifecycle DESC,
                     rowid DESC
            LIMIT 1
            """,
            (ticker, target_date),
        ).fetchone()
    finally:
        conn.row_factory = previous_row_factory


def _score_ticker_count(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(DISTINCT ticker)
            FROM rc_fundamental_ttm
            WHERE fundamental_score_lifecycle IS NOT NULL
            """
        ).fetchone()[0]
    )


def _null_effective_ticker_count(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT ticker
                FROM rc_fundamental_ttm
                WHERE fundamental_score_lifecycle IS NOT NULL
                GROUP BY ticker
                HAVING SUM(CASE WHEN score_effective_trading_date IS NOT NULL THEN 1 ELSE 0 END) = 0
            )
            """
        ).fetchone()[0]
    )


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    index = int(round((len(values) - 1) * fraction))
    return sorted(values)[index]


def _elapsed_marker(_started: float) -> None:
    return None
