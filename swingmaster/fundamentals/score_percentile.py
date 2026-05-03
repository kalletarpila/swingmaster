from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


FUND_SCORE_PERCENTILE_V2_PRE = "FUND_SCORE_PERCENTILE_V2_PRE"
FUND_SCORE_PERCENTILE_V2_2_LIFECYCLE_MULT_PRE = "FUND_SCORE_PERCENTILE_V2_2_LIFECYCLE_MULT_PRE"
SECTOR_MIN_SIZE = 10
INDUSTRY_MIN_SIZE = 10
MIN_UNIVERSE_SIZE = 500
MIN_UNIVERSE_SIZE_BY_MARKET = {
    "omxh": 50,
    "usa": 500,
}
INDUSTRY_MIN_SIZE_BY_MARKET = {
    "omxh": 5,
}
MIN_AVAILABLE_FACTORS = 4
FACTOR_WEIGHTS = {
    "growth": 20.0,
    "margin": 15.0,
    "margin_trend": 10.0,
    "fcf": 20.0,
    "consistency": 20.0,
    "leverage": 7.5,
    "dilution": 7.5,
}
BLENDED_WEIGHTS = {
    "global": 0.40,
    "sector": 0.35,
    "industry": 0.25,
}
FACTOR_COLUMNS = {
    "growth": "revenue_growth_ttm_yoy",
    "margin": "ebit_margin_ttm",
    "margin_trend": "ebit_margin_trend_4q",
    "fcf": "fcf_margin_ttm",
    "consistency": "consistency_component_lifecycle",
    "leverage": "net_debt_to_ebitda",
    "dilution": "share_dilution_yoy",
}
HIGHER_IS_BETTER_FACTORS = {"growth", "margin", "margin_trend", "fcf", "consistency"}
LIFECYCLE_FACTOR_MULTIPLIERS = {
    "SCALING": {
        "growth": 1.15,
        "margin": 0.95,
        "margin_trend": 1.20,
        "fcf": 1.05,
        "consistency": 1.10,
        "leverage": 1.00,
        "dilution": 1.00,
        "adjustment": 0.0,
    },
    "MATURE": {
        "growth": 0.90,
        "margin": 1.15,
        "margin_trend": 1.00,
        "fcf": 1.20,
        "consistency": 1.15,
        "leverage": 1.05,
        "dilution": 1.05,
        "adjustment": 0.0,
    },
    "GROWTH": {
        "growth": 1.15,
        "margin": 1.00,
        "margin_trend": 1.10,
        "fcf": 1.00,
        "consistency": 1.10,
        "leverage": 1.00,
        "dilution": 1.00,
        "adjustment": 0.0,
    },
    "TRANSITION": {
        "growth": 1.05,
        "margin": 1.05,
        "margin_trend": 1.10,
        "fcf": 1.15,
        "consistency": 1.20,
        "leverage": 1.00,
        "dilution": 1.05,
        "adjustment": 0.0,
    },
    "STARTUP": {
        "growth": 1.25,
        "margin": 0.70,
        "margin_trend": 1.00,
        "fcf": 0.70,
        "consistency": 1.15,
        "leverage": 0.90,
        "dilution": 1.00,
        "adjustment": 0.0,
    },
    "DECLINING": {
        "growth": 0.75,
        "margin": 0.90,
        "margin_trend": 0.75,
        "fcf": 1.00,
        "consistency": 0.85,
        "leverage": 1.05,
        "dilution": 1.05,
        "adjustment": -3.0,
    },
    "DISTRESSED": {
        "growth": 0.70,
        "margin": 0.75,
        "margin_trend": 0.70,
        "fcf": 1.10,
        "consistency": 0.85,
        "leverage": 1.15,
        "dilution": 1.10,
        "adjustment": -4.0,
    },
    "UNCLASSIFIED": {
        "growth": 1.00,
        "margin": 1.00,
        "margin_trend": 1.00,
        "fcf": 1.00,
        "consistency": 1.00,
        "leverage": 1.00,
        "dilution": 1.00,
        "adjustment": 0.0,
    },
}


@dataclass(frozen=True)
class PercentileSnapshotRow:
    ticker: str
    as_of_date: str
    revenue_growth_ttm_yoy: float | None
    ebit_margin_ttm: float | None
    ebit_margin_trend_4q: float | None
    fcf_margin_ttm: float | None
    net_debt_to_ebitda: float | None
    share_dilution_yoy: float | None
    consistency_component_lifecycle: float | None
    fundamental_score_lifecycle: float
    lifecycle_class: str | None
    sector: str | None
    industry: str | None


def resolve_created_at_utc(created_at_utc: str | None) -> str:
    if created_at_utc is not None:
        return created_at_utc
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_min_universe_size(market: str) -> int:
    return MIN_UNIVERSE_SIZE_BY_MARKET.get(str(market).lower(), MIN_UNIVERSE_SIZE)


def resolve_industry_min_size(market: str) -> int:
    return INDUSTRY_MIN_SIZE_BY_MARKET.get(str(market).lower(), INDUSTRY_MIN_SIZE)


def load_latest_percentile_snapshot(
    fundamentals_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    target_date: str,
    market: str,
) -> list[PercentileSnapshotRow]:
    previous_row_factory = fundamentals_conn.row_factory
    fundamentals_conn.row_factory = sqlite3.Row
    try:
        rows = fundamentals_conn.execute(
            """
            SELECT
                t.ticker,
                t.as_of_date,
                t.revenue_growth_ttm_yoy,
                t.ebit_margin_ttm,
                t.ebit_margin_trend_4q,
                t.fcf_margin_ttm,
                t.net_debt_to_ebitda,
                t.share_dilution_yoy,
                t.consistency_component_lifecycle,
                t.fundamental_score_lifecycle,
                t.lifecycle_class
            FROM rc_fundamental_ttm t
            JOIN (
                SELECT ticker, MAX(as_of_date) AS as_of_date
                FROM rc_fundamental_ttm
                WHERE as_of_date <= ?
                  AND fundamental_score_lifecycle IS NOT NULL
                GROUP BY ticker
            ) latest
              ON latest.ticker = t.ticker
             AND latest.as_of_date = t.as_of_date
            ORDER BY t.ticker ASC
            """,
            (target_date,),
        ).fetchall()
    finally:
        fundamentals_conn.row_factory = previous_row_factory

    metadata_by_ticker = load_market_metadata(osakedata_conn, market)
    snapshot_rows: list[PercentileSnapshotRow] = []
    for row in rows:
        metadata = metadata_by_ticker.get(str(row["ticker"]))
        snapshot_rows.append(
            PercentileSnapshotRow(
                ticker=str(row["ticker"]),
                as_of_date=str(row["as_of_date"]),
                revenue_growth_ttm_yoy=_coerce_optional_float(row["revenue_growth_ttm_yoy"]),
                ebit_margin_ttm=_coerce_optional_float(row["ebit_margin_ttm"]),
                ebit_margin_trend_4q=_coerce_optional_float(row["ebit_margin_trend_4q"]),
                fcf_margin_ttm=_coerce_optional_float(row["fcf_margin_ttm"]),
                net_debt_to_ebitda=_coerce_optional_float(row["net_debt_to_ebitda"]),
                share_dilution_yoy=_coerce_optional_float(row["share_dilution_yoy"]),
                consistency_component_lifecycle=_coerce_optional_float(row["consistency_component_lifecycle"]),
                fundamental_score_lifecycle=float(row["fundamental_score_lifecycle"]),
                lifecycle_class=str(row["lifecycle_class"]) if row["lifecycle_class"] is not None else None,
                sector=metadata["sector"] if metadata is not None else None,
                industry=metadata["industry"] if metadata is not None else None,
            )
        )
    return snapshot_rows


def load_market_metadata(
    osakedata_conn: sqlite3.Connection,
    market: str,
) -> dict[str, dict[str, str | None]]:
    previous_row_factory = osakedata_conn.row_factory
    osakedata_conn.row_factory = sqlite3.Row
    try:
        rows = osakedata_conn.execute(
            """
            SELECT ticker, sector, industry
            FROM ticker_meta
            WHERE market = ?
            ORDER BY ticker ASC
            """,
            (market,),
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


def build_percentile_rows(
    snapshot_rows: list[PercentileSnapshotRow],
    target_date: str,
    rule_id: str,
    run_id: str,
    created_at_utc: str,
    market: str = "usa",
) -> list[dict[str, Any]]:
    universe_size = len(snapshot_rows)
    industry_min_size = resolve_industry_min_size(market)
    global_percentiles = {
        factor_name: _compute_factor_percentiles(snapshot_rows, factor_name)
        for factor_name in FACTOR_COLUMNS
    }
    sector_groups = _group_rows(snapshot_rows, "sector")
    industry_groups = _group_rows(snapshot_rows, "industry")
    sector_percentiles = {
        group_name: {factor_name: _compute_factor_percentiles(group_rows, factor_name) for factor_name in FACTOR_COLUMNS}
        for group_name, group_rows in sector_groups.items()
    }
    industry_percentiles = {
        group_name: {factor_name: _compute_factor_percentiles(group_rows, factor_name) for factor_name in FACTOR_COLUMNS}
        for group_name, group_rows in industry_groups.items()
    }

    percentile_rows: list[dict[str, Any]] = []
    for snapshot_row in snapshot_rows:
        sector_size = len(sector_groups[snapshot_row.sector]) if snapshot_row.sector in sector_groups else None
        industry_size = len(industry_groups[snapshot_row.industry]) if snapshot_row.industry in industry_groups else None

        row_result: dict[str, Any] = {
            "ticker": snapshot_row.ticker,
            "as_of_date": snapshot_row.as_of_date,
            "target_date": target_date,
            "sector": snapshot_row.sector,
            "industry": snapshot_row.industry,
            "rule_id": rule_id,
            "run_id": run_id,
            "universe_size": universe_size,
            "sector_size": sector_size,
            "industry_size": industry_size,
            "created_at_utc": created_at_utc,
        }

        global_factor_scores = _level_factor_percentiles(
            global_percentiles,
            snapshot_row.ticker,
            allowed=True,
        )
        sector_factor_scores = _level_factor_percentiles(
            sector_percentiles.get(snapshot_row.sector, {}),
            snapshot_row.ticker,
            allowed=sector_size is not None and sector_size >= SECTOR_MIN_SIZE,
        )
        industry_factor_scores = _level_factor_percentiles(
            industry_percentiles.get(snapshot_row.industry, {}),
            snapshot_row.ticker,
            allowed=industry_size is not None and industry_size >= industry_min_size,
        )

        _store_factor_percentiles(row_result, global_factor_scores, "global")
        _store_factor_percentiles(row_result, sector_factor_scores, "sector")
        _store_factor_percentiles(row_result, industry_factor_scores, "industry")

        row_result["fundamental_score_percentile_global"] = compute_weighted_percentile_score(global_factor_scores)
        row_result["fundamental_score_percentile_sector"] = compute_weighted_percentile_score(sector_factor_scores)
        row_result["fundamental_score_percentile_industry"] = compute_weighted_percentile_score(industry_factor_scores)
        row_result["fundamental_score_percentile_blended"] = compute_blended_percentile_score(
            {
                "global": row_result["fundamental_score_percentile_global"],
                "sector": row_result["fundamental_score_percentile_sector"],
                "industry": row_result["fundamental_score_percentile_industry"],
            }
        )
        row_result["fundamental_score_percentile_global_lifecycle_weighted"] = compute_lifecycle_weighted_percentile_score(
            global_factor_scores,
            snapshot_row.lifecycle_class,
        )
        row_result["fundamental_score_percentile_sector_lifecycle_weighted"] = compute_lifecycle_weighted_percentile_score(
            sector_factor_scores,
            snapshot_row.lifecycle_class,
        )
        row_result["fundamental_score_percentile_industry_lifecycle_weighted"] = compute_lifecycle_weighted_percentile_score(
            industry_factor_scores,
            snapshot_row.lifecycle_class,
        )
        row_result["fundamental_score_percentile_blended_lifecycle_weighted"] = compute_blended_percentile_score(
            {
                "global": row_result["fundamental_score_percentile_global_lifecycle_weighted"],
                "sector": row_result["fundamental_score_percentile_sector_lifecycle_weighted"],
                "industry": row_result["fundamental_score_percentile_industry_lifecycle_weighted"],
            }
        )
        row_result["percentile_lifecycle_weight_rule"] = FUND_SCORE_PERCENTILE_V2_2_LIFECYCLE_MULT_PRE
        percentile_rows.append(row_result)
    _assign_partition_ranks(
        percentile_rows,
        partition_key="sector",
        partition_size_key="sector_size",
        min_size=SECTOR_MIN_SIZE,
        score_key="fundamental_score_percentile_blended",
        rank_key="sector_rank_blended",
    )
    _assign_partition_ranks(
        percentile_rows,
        partition_key="industry",
        partition_size_key="industry_size",
        min_size=industry_min_size,
        score_key="fundamental_score_percentile_blended",
        rank_key="industry_rank_blended",
    )
    _assign_partition_ranks(
        percentile_rows,
        partition_key="sector",
        partition_size_key="sector_size",
        min_size=SECTOR_MIN_SIZE,
        score_key="fundamental_score_percentile_blended_lifecycle_weighted",
        rank_key="sector_rank_blended_lifecycle_weighted",
    )
    _assign_partition_ranks(
        percentile_rows,
        partition_key="industry",
        partition_size_key="industry_size",
        min_size=industry_min_size,
        score_key="fundamental_score_percentile_blended_lifecycle_weighted",
        rank_key="industry_rank_blended_lifecycle_weighted",
    )
    return percentile_rows


def compute_weighted_percentile_score(factor_percentiles: dict[str, float | None]) -> float | None:
    available = [
        (factor_name, percentile)
        for factor_name, percentile in factor_percentiles.items()
        if percentile is not None
    ]
    if len(available) < MIN_AVAILABLE_FACTORS:
        return None
    total_weight = sum(FACTOR_WEIGHTS[factor_name] for factor_name, _percentile in available)
    if total_weight == 0:
        return None
    weighted_sum = sum(FACTOR_WEIGHTS[factor_name] * float(percentile) for factor_name, percentile in available)
    return weighted_sum / total_weight


def compute_blended_percentile_score(level_scores: dict[str, float | None]) -> float | None:
    available = [
        (level_name, score)
        for level_name, score in level_scores.items()
        if score is not None
    ]
    if not available:
        return None
    total_weight = sum(BLENDED_WEIGHTS[level_name] for level_name, _score in available)
    weighted_sum = sum(BLENDED_WEIGHTS[level_name] * float(score) for level_name, score in available)
    return weighted_sum / total_weight


def compute_lifecycle_weighted_percentile_score(
    factor_percentiles: dict[str, float | None],
    lifecycle_class: str | None,
) -> float | None:
    profile = LIFECYCLE_FACTOR_MULTIPLIERS.get(
        lifecycle_class if lifecycle_class is not None else "UNCLASSIFIED",
        LIFECYCLE_FACTOR_MULTIPLIERS["UNCLASSIFIED"],
    )
    available = [
        (factor_name, percentile)
        for factor_name, percentile in factor_percentiles.items()
        if percentile is not None
    ]
    if len(available) < MIN_AVAILABLE_FACTORS:
        return None
    effective_weights = {
        factor_name: FACTOR_WEIGHTS[factor_name] * float(profile[factor_name])
        for factor_name, _percentile in available
    }
    total_weight = sum(effective_weights.values())
    if total_weight == 0:
        return None
    weighted_sum = sum(
        effective_weights[factor_name] * float(percentile)
        for factor_name, percentile in available
    )
    score = (weighted_sum / total_weight) + float(profile["adjustment"])
    return float(min(100.0, max(0.0, score)))


def write_percentile_rows(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> int:
    rows_list = list(rows)
    conn.executemany(
        """
        INSERT OR REPLACE INTO rc_fundamental_score_percentile (
            ticker,
            as_of_date,
            target_date,
            sector,
            industry,
            rule_id,
            run_id,
            universe_size,
            sector_size,
            industry_size,
            growth_pct_global,
            growth_pct_sector,
            growth_pct_industry,
            margin_pct_global,
            margin_pct_sector,
            margin_pct_industry,
            margin_trend_pct_global,
            margin_trend_pct_sector,
            margin_trend_pct_industry,
            fcf_pct_global,
            fcf_pct_sector,
            fcf_pct_industry,
            leverage_pct_global,
            leverage_pct_sector,
            leverage_pct_industry,
            dilution_pct_global,
            dilution_pct_sector,
            dilution_pct_industry,
            consistency_pct_global,
            consistency_pct_sector,
            consistency_pct_industry,
            fundamental_score_percentile_global,
            fundamental_score_percentile_sector,
            fundamental_score_percentile_industry,
            fundamental_score_percentile_blended,
            sector_rank_blended,
            industry_rank_blended,
            fundamental_score_percentile_global_lifecycle_weighted,
            fundamental_score_percentile_sector_lifecycle_weighted,
            fundamental_score_percentile_industry_lifecycle_weighted,
            fundamental_score_percentile_blended_lifecycle_weighted,
            sector_rank_blended_lifecycle_weighted,
            industry_rank_blended_lifecycle_weighted,
            percentile_lifecycle_weight_rule,
            created_at_utc
        ) VALUES (
            :ticker,
            :as_of_date,
            :target_date,
            :sector,
            :industry,
            :rule_id,
            :run_id,
            :universe_size,
            :sector_size,
            :industry_size,
            :growth_pct_global,
            :growth_pct_sector,
            :growth_pct_industry,
            :margin_pct_global,
            :margin_pct_sector,
            :margin_pct_industry,
            :margin_trend_pct_global,
            :margin_trend_pct_sector,
            :margin_trend_pct_industry,
            :fcf_pct_global,
            :fcf_pct_sector,
            :fcf_pct_industry,
            :leverage_pct_global,
            :leverage_pct_sector,
            :leverage_pct_industry,
            :dilution_pct_global,
            :dilution_pct_sector,
            :dilution_pct_industry,
            :consistency_pct_global,
            :consistency_pct_sector,
            :consistency_pct_industry,
            :fundamental_score_percentile_global,
            :fundamental_score_percentile_sector,
            :fundamental_score_percentile_industry,
            :fundamental_score_percentile_blended,
            :sector_rank_blended,
            :industry_rank_blended,
            :fundamental_score_percentile_global_lifecycle_weighted,
            :fundamental_score_percentile_sector_lifecycle_weighted,
            :fundamental_score_percentile_industry_lifecycle_weighted,
            :fundamental_score_percentile_blended_lifecycle_weighted,
            :sector_rank_blended_lifecycle_weighted,
            :industry_rank_blended_lifecycle_weighted,
            :percentile_lifecycle_weight_rule,
            :created_at_utc
        )
        """,
        rows_list,
    )
    conn.commit()
    return len(rows_list)


def run_fundamental_score_percentile(
    fundamentals_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    target_date: str,
    rule_id: str,
    run_id: str,
    market: str,
    created_at_utc: str,
    dry_run: bool,
) -> dict[str, Any]:
    snapshot_rows = load_latest_percentile_snapshot(
        fundamentals_conn=fundamentals_conn,
        osakedata_conn=osakedata_conn,
        target_date=target_date,
        market=market,
    )
    min_universe_size = resolve_min_universe_size(market)
    if not snapshot_rows:
        raise RuntimeError(f"FUND_SCORE_PERCENTILE_NO_ROWS:{target_date}")
    if len(snapshot_rows) < min_universe_size:
        raise RuntimeError(f"FUND_SCORE_PERCENTILE_UNIVERSE_TOO_SMALL:{len(snapshot_rows)}")

    percentile_rows = build_percentile_rows(
        snapshot_rows=snapshot_rows,
        target_date=target_date,
        rule_id=rule_id,
        run_id=run_id,
        created_at_utc=created_at_utc,
        market=market,
    )
    rows_written = 0
    if not dry_run:
        rows_written = write_percentile_rows(fundamentals_conn, percentile_rows)
    return {
        "universe_size": len(snapshot_rows),
        "rows_computed": len(percentile_rows),
        "rows_written": rows_written,
        "lifecycle_weighted_rows_computed": len(percentile_rows),
        "lifecycle_weighted_rows_written": rows_written,
        "sector_count": len({row.sector for row in snapshot_rows if row.sector is not None}),
        "industry_count": len({row.industry for row in snapshot_rows if row.industry is not None}),
    }


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _group_rows(
    snapshot_rows: list[PercentileSnapshotRow],
    attribute_name: str,
) -> dict[str, list[PercentileSnapshotRow]]:
    groups: dict[str, list[PercentileSnapshotRow]] = defaultdict(list)
    for row in snapshot_rows:
        attribute_value = getattr(row, attribute_name)
        if attribute_value is None:
            continue
        groups[str(attribute_value)].append(row)
    return dict(groups)


def _compute_factor_percentiles(
    snapshot_rows: list[PercentileSnapshotRow],
    factor_name: str,
) -> dict[str, float]:
    column_name = FACTOR_COLUMNS[factor_name]
    values: list[tuple[str, float]] = []
    for row in snapshot_rows:
        value = getattr(row, column_name)
        if value is None:
            continue
        values.append((row.ticker, float(value)))
    return compute_percentiles(
        values=values,
        higher_is_better=factor_name in HIGHER_IS_BETTER_FACTORS,
    )


def compute_percentiles(
    values: list[tuple[str, float]],
    higher_is_better: bool,
) -> dict[str, float]:
    if not values:
        return {}
    sorted_values = sorted(values, key=lambda item: (item[1], item[0]))
    if len(sorted_values) == 1:
        return {sorted_values[0][0]: 100.0}

    percentiles: dict[str, float] = {}
    index = 0
    denominator = float(len(sorted_values) - 1)
    while index < len(sorted_values):
        group_end = index
        current_value = sorted_values[index][1]
        while group_end + 1 < len(sorted_values) and sorted_values[group_end + 1][1] == current_value:
            group_end += 1
        average_rank = (index + group_end) / 2.0
        higher_percentile = 100.0 * average_rank / denominator
        percentile = higher_percentile if higher_is_better else 100.0 - higher_percentile
        for ticker, _value in sorted_values[index : group_end + 1]:
            percentiles[ticker] = percentile
        index = group_end + 1
    return percentiles


def _level_factor_percentiles(
    percentile_maps: dict[str, dict[str, float]],
    ticker: str,
    allowed: bool,
) -> dict[str, float | None]:
    if not allowed:
        return {factor_name: None for factor_name in FACTOR_COLUMNS}
    return {
        factor_name: percentile_maps.get(factor_name, {}).get(ticker)
        for factor_name in FACTOR_COLUMNS
    }


def _store_factor_percentiles(
    row_result: dict[str, Any],
    factor_scores: dict[str, float | None],
    suffix: str,
) -> None:
    for factor_name, percentile in factor_scores.items():
        row_result[f"{factor_name}_pct_{suffix}"] = percentile


def _assign_partition_ranks(
    percentile_rows: list[dict[str, Any]],
    *,
    partition_key: str,
    partition_size_key: str,
    min_size: int,
    score_key: str,
    rank_key: str,
) -> None:
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in percentile_rows:
        row[rank_key] = None
        partition_value = row.get(partition_key)
        partition_size = row.get(partition_size_key)
        score = row.get(score_key)
        if partition_value is None or partition_size is None or partition_size < min_size or score is None:
            continue
        grouped_rows[str(partition_value)].append(row)

    for partition_value in grouped_rows.values():
        ranked_rows = sorted(
            partition_value,
            key=lambda item: (-float(item[score_key]), str(item["ticker"])),
        )
        for index, row in enumerate(ranked_rows, start=1):
            row[rank_key] = index
