from __future__ import annotations

import sqlite3
from statistics import mean, pstdev
from typing import Any, Mapping


FUND_SCORE_RULE_V1_1 = "FUND_SCORE_RULE_V1_1"
FUND_SCORE_RULE_V1 = FUND_SCORE_RULE_V1_1
FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE = "FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE"


def load_ttm_rows(conn: sqlite3.Connection, ticker: str | None) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        if ticker is None:
            rows = conn.execute(
                """
                SELECT
                    ticker,
                    as_of_date,
                    revenue_growth_ttm_yoy,
                    ebit_margin_ttm,
                    ebit_margin_trend_4q,
                    fcf_margin_ttm,
                    net_debt_to_ebitda,
                    share_dilution_yoy,
                    lifecycle_class,
                    fundamental_score
                FROM rc_fundamental_ttm
                ORDER BY ticker ASC, as_of_date ASC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    ticker,
                    as_of_date,
                    revenue_growth_ttm_yoy,
                    ebit_margin_ttm,
                    ebit_margin_trend_4q,
                    fcf_margin_ttm,
                    net_debt_to_ebitda,
                    share_dilution_yoy,
                    lifecycle_class,
                    fundamental_score
                FROM rc_fundamental_ttm
                WHERE ticker = ?
                ORDER BY ticker ASC, as_of_date ASC
                """,
                (ticker,),
            ).fetchall()
    finally:
        conn.row_factory = previous_row_factory

    if not rows:
        if ticker is None:
            raise RuntimeError("FUNDAMENTAL_TTM_NOT_FOUND")
        raise RuntimeError(f"FUNDAMENTAL_TTM_NOT_FOUND:{ticker}")
    return rows


def calculate_fundamental_score(
    row: Mapping[str, Any],
    ttm_series_history: list[Mapping[str, Any]] | None = None,
) -> float:
    return explain_score_components(row, ttm_series_history)["fundamental_score_recomputed"]


def explain_score_components(
    row: Mapping[str, Any],
    ttm_series_history: list[Mapping[str, Any]] | None = None,
) -> dict[str, float]:
    growth_component = float(_growth_component(row["revenue_growth_ttm_yoy"]))
    margin_component = float(_margin_component(row["ebit_margin_ttm"]))
    margin_trend_component = float(_margin_trend_component(row["ebit_margin_trend_4q"]))
    fcf_component = float(_fcf_component(row["fcf_margin_ttm"]))
    leverage_component = float(_leverage_component(row["net_debt_to_ebitda"]))
    dilution_component = float(_dilution_component(row["share_dilution_yoy"]))
    lifecycle_component = float(_lifecycle_component(row["lifecycle_class"]))
    consistency_component = float(compute_consistency_component(ttm_series_history or [row]))
    score_raw = (
        growth_component
        + margin_component
        + margin_trend_component
        + fcf_component
        + leverage_component
        + dilution_component
        + lifecycle_component
        + consistency_component
    )
    fundamental_score_recomputed = float(min(100, max(0, score_raw)))
    return {
        "growth_component": growth_component,
        "margin_component": margin_component,
        "margin_trend_component": margin_trend_component,
        "fcf_component": fcf_component,
        "leverage_component": leverage_component,
        "dilution_component": dilution_component,
        "lifecycle_component": lifecycle_component,
        "consistency_component": consistency_component,
        "score_raw": float(score_raw),
        "fundamental_score_recomputed": fundamental_score_recomputed,
    }


def compute_lifecycle_score_components(
    row: Mapping[str, Any],
    baseline_components: Mapping[str, float],
) -> dict[str, float | str]:
    distressed_penalty = 0.0
    if row["lifecycle_class"] == "SCALING":
        growth_component_lifecycle = baseline_components["growth_component"] * 1.25
        margin_component_lifecycle = baseline_components["margin_component"] * 0.90
        margin_trend_component_lifecycle = baseline_components["margin_trend_component"] * 1.25
        fcf_component_lifecycle = baseline_components["fcf_component"] * 0.90
        leverage_component_lifecycle = baseline_components["leverage_component"]
        dilution_component_lifecycle = baseline_components["dilution_component"]
        lifecycle_component_lifecycle = baseline_components["lifecycle_component"]
        consistency_component_lifecycle = baseline_components["consistency_component"] * 1.25
    elif row["lifecycle_class"] == "STARTUP":
        growth_component_lifecycle = baseline_components["growth_component"] * 1.40
        margin_component_lifecycle = baseline_components["margin_component"] * 0.60
        margin_trend_component_lifecycle = baseline_components["margin_trend_component"] * 0.90
        fcf_component_lifecycle = baseline_components["fcf_component"] * 0.60
        leverage_component_lifecycle = baseline_components["leverage_component"] * 0.70
        dilution_component_lifecycle = baseline_components["dilution_component"] * 1.00
        lifecycle_component_lifecycle = baseline_components["lifecycle_component"] * 1.00
        consistency_component_lifecycle = baseline_components["consistency_component"] * 1.15
    elif row["lifecycle_class"] == "DISTRESSED":
        growth_component_lifecycle = baseline_components["growth_component"] * 0.70
        margin_component_lifecycle = baseline_components["margin_component"] * 0.60
        margin_trend_component_lifecycle = baseline_components["margin_trend_component"] * 0.75
        fcf_component_lifecycle = baseline_components["fcf_component"] * 1.25
        leverage_component_lifecycle = baseline_components["leverage_component"] * 1.40
        dilution_component_lifecycle = baseline_components["dilution_component"] * 1.10
        lifecycle_component_lifecycle = baseline_components["lifecycle_component"] * 1.00
        consistency_component_lifecycle = baseline_components["consistency_component"] * 1.20
        distressed_penalty = 4.0
    elif row["lifecycle_class"] == "TRANSITION":
        growth_component_lifecycle = baseline_components["growth_component"] * 1.15
        margin_component_lifecycle = baseline_components["margin_component"] * 1.05
        margin_trend_component_lifecycle = baseline_components["margin_trend_component"] * 1.35
        fcf_component_lifecycle = baseline_components["fcf_component"] * 1.00
        leverage_component_lifecycle = baseline_components["leverage_component"] * 1.00
        dilution_component_lifecycle = baseline_components["dilution_component"] * 1.00
        lifecycle_component_lifecycle = baseline_components["lifecycle_component"] * 1.00
        consistency_component_lifecycle = baseline_components["consistency_component"] * 1.20
    elif row["lifecycle_class"] == "DECLINING":
        growth_component_lifecycle = baseline_components["growth_component"] * 0.65
        margin_component_lifecycle = baseline_components["margin_component"] * 0.85
        margin_trend_component_lifecycle = baseline_components["margin_trend_component"] * 0.70
        fcf_component_lifecycle = baseline_components["fcf_component"] * 1.00
        leverage_component_lifecycle = baseline_components["leverage_component"] * 1.10
        dilution_component_lifecycle = baseline_components["dilution_component"] * 1.10
        lifecycle_component_lifecycle = baseline_components["lifecycle_component"] * 1.00
        consistency_component_lifecycle = baseline_components["consistency_component"] * 0.80
        distressed_penalty = 3.0
    elif row["lifecycle_class"] == "GROWTH":
        growth_component_lifecycle = baseline_components["growth_component"] * 1.10
        margin_component_lifecycle = baseline_components["margin_component"] * 1.05
        margin_trend_component_lifecycle = baseline_components["margin_trend_component"] * 1.10
        fcf_component_lifecycle = baseline_components["fcf_component"] * 1.00
        leverage_component_lifecycle = baseline_components["leverage_component"] * 1.00
        dilution_component_lifecycle = baseline_components["dilution_component"] * 1.00
        lifecycle_component_lifecycle = baseline_components["lifecycle_component"] * 1.00
        consistency_component_lifecycle = baseline_components["consistency_component"] * 1.10
    elif row["lifecycle_class"] == "MATURE":
        growth_component_lifecycle = baseline_components["growth_component"] * 0.95
        margin_component_lifecycle = baseline_components["margin_component"] * 1.10
        margin_trend_component_lifecycle = baseline_components["margin_trend_component"] * 1.00
        fcf_component_lifecycle = baseline_components["fcf_component"] * 1.15
        leverage_component_lifecycle = baseline_components["leverage_component"] * 1.05
        dilution_component_lifecycle = baseline_components["dilution_component"] * 1.10
        lifecycle_component_lifecycle = baseline_components["lifecycle_component"] * 1.00
        consistency_component_lifecycle = baseline_components["consistency_component"] * 1.15
    else:
        growth_component_lifecycle = baseline_components["growth_component"]
        margin_component_lifecycle = baseline_components["margin_component"]
        margin_trend_component_lifecycle = baseline_components["margin_trend_component"]
        fcf_component_lifecycle = baseline_components["fcf_component"]
        leverage_component_lifecycle = baseline_components["leverage_component"]
        dilution_component_lifecycle = baseline_components["dilution_component"]
        lifecycle_component_lifecycle = baseline_components["lifecycle_component"]
        consistency_component_lifecycle = baseline_components["consistency_component"]

    score_raw_lifecycle = (
        growth_component_lifecycle
        + margin_component_lifecycle
        + margin_trend_component_lifecycle
        + fcf_component_lifecycle
        + leverage_component_lifecycle
        + dilution_component_lifecycle
        + lifecycle_component_lifecycle
        + consistency_component_lifecycle
    )
    score_raw_lifecycle -= distressed_penalty
    fundamental_score_lifecycle = float(min(100, max(0, score_raw_lifecycle)))
    return {
        "growth_component_lifecycle": float(growth_component_lifecycle),
        "margin_component_lifecycle": float(margin_component_lifecycle),
        "margin_trend_component_lifecycle": float(margin_trend_component_lifecycle),
        "fcf_component_lifecycle": float(fcf_component_lifecycle),
        "leverage_component_lifecycle": float(leverage_component_lifecycle),
        "dilution_component_lifecycle": float(dilution_component_lifecycle),
        "lifecycle_component_lifecycle": float(lifecycle_component_lifecycle),
        "consistency_component_lifecycle": float(consistency_component_lifecycle),
        "fundamental_score_lifecycle": fundamental_score_lifecycle,
        "score_rule_lifecycle": FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE,
    }


def _growth_component(value: float | None) -> int:
    if value is None:
        return 6
    if value >= 0.30:
        return 15
    if value >= 0.20:
        return 12
    if value >= 0.10:
        return 9
    if value >= 0:
        return 5
    return 0


def _margin_component(value: float | None) -> int:
    if value is None:
        return 0
    if value >= 0.25:
        return 15
    if value >= 0.15:
        return 12
    if value >= 0.08:
        return 8
    if value >= 0:
        return 4
    return 0


def _margin_trend_component(value: float | None) -> int:
    if value is None:
        return 6
    if value >= 0.05:
        return 15
    if value >= 0.02:
        return 10
    if value >= 0:
        return 6
    return 2


def _fcf_component(value: float | None) -> int:
    if value is None:
        return 0
    if value >= 0.20:
        return 15
    if value >= 0.10:
        return 12
    if value >= 0.05:
        return 8
    if value >= 0:
        return 4
    return 0


def _leverage_component(value: float | None) -> int:
    if value is None:
        return 8
    if value <= 0:
        return 15
    if value <= 1:
        return 12
    if value <= 2:
        return 8
    if value <= 3:
        return 4
    return 0


def _dilution_component(value: float | None) -> int:
    if value is not None and abs(value) > 0.50:
        value = None
    if value is None:
        return 5
    if value <= -0.02:
        return 10
    if value <= 0:
        return 8
    if value <= 0.02:
        return 5
    if value <= 0.05:
        return 2
    return 0


def _lifecycle_component(value: str | None) -> int:
    if value == "STARTUP":
        return -5
    if value == "GROWTH":
        return 2
    if value == "SCALING":
        return 4
    if value == "MATURE":
        return 5
    if value == "DECLINING":
        return -5
    if value == "DISTRESSED":
        return -10
    return 0


def compute_consistency_component(ttm_series_history: list[Mapping[str, Any]]) -> int:
    ordered_history = sorted(
        ttm_series_history,
        key=lambda row: str(_mapping_value(row, "as_of_date") or _mapping_value(row, "latest_period_end_date") or ""),
        reverse=True,
    )
    metric_names = (
        "revenue_growth_ttm_yoy",
        "ebit_margin_ttm",
        "fcf_margin_ttm",
    )
    collected_values: list[list[float]] = []
    for metric_name in metric_names:
        values: list[float] = []
        for row in ordered_history:
            value = _mapping_value(row, metric_name)
            if value is None:
                continue
            values.append(float(value))
            if len(values) == 4:
                break
        if len(values) < 3:
            return 0
        collected_values.append(values)

    avg_cv = mean(_coefficient_of_variation(values) for values in collected_values)
    if avg_cv <= 0.05:
        return 10
    if avg_cv <= 0.10:
        return 8
    if avg_cv <= 0.15:
        return 6
    if avg_cv <= 0.20:
        return 4
    if avg_cv <= 0.30:
        return 2
    return 0


def _coefficient_of_variation(values: list[float]) -> float:
    mean_value = mean(values)
    if mean_value == 0:
        return float("inf")
    return pstdev(values) / abs(mean_value)


def _mapping_value(row: Mapping[str, Any], key: str) -> Any:
    if isinstance(row, sqlite3.Row):
        return row[key]
    return row.get(key)


def update_scores(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
    dry_run: bool,
) -> tuple[int, float | None, float | None, float | None]:
    score_updates: list[
        tuple[
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            str,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            str,
            str,
            str,
        ]
    ] = []
    history_by_ticker: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        ticker = str(row["ticker"])
        ticker_history = history_by_ticker.setdefault(ticker, [])
        ticker_history.append(row)
        explained_components = explain_score_components(row, ticker_history)
        lifecycle_components = compute_lifecycle_score_components(row, explained_components)
        score_updates.append(
            (
                explained_components["fundamental_score_recomputed"],
                explained_components["growth_component"],
                explained_components["margin_component"],
                explained_components["margin_trend_component"],
                explained_components["fcf_component"],
                explained_components["leverage_component"],
                explained_components["dilution_component"],
                explained_components["lifecycle_component"],
                explained_components["consistency_component"],
                FUND_SCORE_RULE_V1_1,
                lifecycle_components["fundamental_score_lifecycle"],
                lifecycle_components["growth_component_lifecycle"],
                lifecycle_components["margin_component_lifecycle"],
                lifecycle_components["margin_trend_component_lifecycle"],
                lifecycle_components["fcf_component_lifecycle"],
                lifecycle_components["leverage_component_lifecycle"],
                lifecycle_components["dilution_component_lifecycle"],
                lifecycle_components["lifecycle_component_lifecycle"],
                lifecycle_components["consistency_component_lifecycle"],
                str(lifecycle_components["score_rule_lifecycle"]),
                ticker,
                str(row["as_of_date"]),
            )
        )
    scores = [score for score, *_rest in score_updates]
    min_score = min(scores) if scores else None
    max_score = max(scores) if scores else None
    avg_score = round(mean(scores), 4) if scores else None

    if not dry_run:
        conn.executemany(
            """
            UPDATE rc_fundamental_ttm
            SET fundamental_score = ?,
                growth_component = ?,
                margin_component = ?,
                margin_trend_component = ?,
                fcf_component = ?,
                leverage_component = ?,
                dilution_component = ?,
                lifecycle_component = ?,
                consistency_component = ?,
                score_rule = ?,
                fundamental_score_lifecycle = ?,
                growth_component_lifecycle = ?,
                margin_component_lifecycle = ?,
                margin_trend_component_lifecycle = ?,
                fcf_component_lifecycle = ?,
                leverage_component_lifecycle = ?,
                dilution_component_lifecycle = ?,
                lifecycle_component_lifecycle = ?,
                consistency_component_lifecycle = ?,
                score_rule_lifecycle = ?
            WHERE ticker = ? AND as_of_date = ?
            """,
            score_updates,
        )
        conn.commit()

    return len(rows), min_score, max_score, avg_score


def run_fundamental_scoring(
    conn: sqlite3.Connection,
    ticker: str | None,
    dry_run: bool,
) -> tuple[int, float | None, float | None, float | None]:
    rows = load_ttm_rows(conn, ticker)
    return update_scores(conn, rows, dry_run)
