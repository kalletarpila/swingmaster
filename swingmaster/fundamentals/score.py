from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Any, Mapping


FUND_SCORE_RULE_V1_1 = "FUND_SCORE_RULE_V1_1"
FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE = "FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE"
FUND_SCORE_RULE_V2_EBITDA_PROFITABILITY = "FUND_SCORE_RULE_V2_EBITDA_PROFITABILITY"
FUND_SCORE_RULE_V3_EBITDA_LIFECYCLE = "FUND_SCORE_RULE_V3_EBITDA_LIFECYCLE"
FUND_SCORE_RULE_V1 = FUND_SCORE_RULE_V2_EBITDA_PROFITABILITY
ACTIVE_FUND_SCORE_RULE = FUND_SCORE_RULE_V2_EBITDA_PROFITABILITY
ACTIVE_FUND_SCORE_LIFECYCLE_RULE = FUND_SCORE_RULE_V3_EBITDA_LIFECYCLE
SUPPORTED_SCORE_PROFILE = "ORDINARY"
SCORE_PROFILE_UNSUPPORTED = "SCORE_PROFILE_UNSUPPORTED"
SCORE_READY = "SCORE_READY"
SCORE_NOT_READY = "SCORE_NOT_READY"


@dataclass(frozen=True)
class ScoreReadiness:
    score_ready: bool
    score_profile_status: str
    growth_ready: bool
    ebitda_margin_ready: bool
    ebitda_margin_trend_ready: bool
    fcf_ready: bool
    ebitda_consistency_ready: bool
    leverage_ready: bool
    dilution_ready: bool
    missing_reasons: tuple[str, ...]


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
                    ebitda_margin_ttm,
                    ebitda_margin_trend_4q,
                    fcf_margin_ttm,
                    net_debt_to_ebitda,
                    net_debt_to_ebit,
                    share_dilution_yoy,
                    lifecycle_class,
                    fundamental_score,
                    growth_component,
                    margin_component,
                    margin_trend_component,
                    fcf_component,
                    leverage_component,
                    dilution_component,
                    lifecycle_component,
                    consistency_component,
                    score_rule,
                    fundamental_score_lifecycle,
                    growth_component_lifecycle,
                    margin_component_lifecycle,
                    margin_trend_component_lifecycle,
                    fcf_component_lifecycle,
                    leverage_component_lifecycle,
                    dilution_component_lifecycle,
                    lifecycle_component_lifecycle,
                    consistency_component_lifecycle,
                    score_rule_lifecycle
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
                    ebitda_margin_ttm,
                    ebitda_margin_trend_4q,
                    fcf_margin_ttm,
                    net_debt_to_ebitda,
                    net_debt_to_ebit,
                    share_dilution_yoy,
                    lifecycle_class,
                    fundamental_score,
                    growth_component,
                    margin_component,
                    margin_trend_component,
                    fcf_component,
                    leverage_component,
                    dilution_component,
                    lifecycle_component,
                    consistency_component,
                    score_rule,
                    fundamental_score_lifecycle,
                    growth_component_lifecycle,
                    margin_component_lifecycle,
                    margin_trend_component_lifecycle,
                    fcf_component_lifecycle,
                    leverage_component_lifecycle,
                    dilution_component_lifecycle,
                    lifecycle_component_lifecycle,
                    consistency_component_lifecycle,
                    score_rule_lifecycle
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
    margin_component = float(_margin_component(row["ebitda_margin_ttm"]))
    margin_trend_component = float(_margin_trend_component(row["ebitda_margin_trend_4q"]))
    fcf_component = float(_fcf_component(row["fcf_margin_ttm"]))
    leverage_component = float(_leverage_component(_leverage_ratio(row)))
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
        "score_rule_lifecycle": ACTIVE_FUND_SCORE_LIFECYCLE_RULE,
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
    if value >= 0.35:
        return 15
    if value >= 0.25:
        return 12
    if value >= 0.15:
        return 8
    if value >= 0:
        return 4
    return 0


def _margin_trend_component(value: float | None) -> int:
    if value is None:
        return 6
    if value >= 0.10:
        return 15
    if value >= 0.04:
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


def _leverage_ratio(row: Mapping[str, Any]) -> float | None:
    value = _mapping_value(row, "net_debt_to_ebitda")
    if value is not None:
        return float(value)
    value = _mapping_value(row, "net_debt_to_ebit")
    if value is not None:
        return float(value)
    return None


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
        "ebitda_margin_ttm",
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


def assess_latest_q_core_fields(row: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    missing: list[str] = []
    for field in ("ticker", "period_end_date", "revenue", "ebitda", "free_cashflow", "cash", "total_debt"):
        if not _metric_available(_mapping_value(row, field)):
            missing.append(field)
    shares = _mapping_value(row, "shares_outstanding")
    if not _metric_available(shares) or float(shares) <= 0:
        missing.append("shares_outstanding")
    return not missing, tuple(missing)


def assess_score_readiness(
    latest_row: Mapping[str, Any],
    ttm_series_history: list[Mapping[str, Any]],
    *,
    score_profile: str | None = SUPPORTED_SCORE_PROFILE,
) -> ScoreReadiness:
    if score_profile != SUPPORTED_SCORE_PROFILE:
        return ScoreReadiness(
            score_ready=False,
            score_profile_status=SCORE_PROFILE_UNSUPPORTED,
            growth_ready=False,
            ebitda_margin_ready=False,
            ebitda_margin_trend_ready=False,
            fcf_ready=False,
            ebitda_consistency_ready=False,
            leverage_ready=False,
            dilution_ready=False,
            missing_reasons=(SCORE_PROFILE_UNSUPPORTED,),
        )

    growth_ready = _metric_available(_mapping_value(latest_row, "revenue_growth_ttm_yoy"))
    ebitda_margin_ready = _metric_available(_mapping_value(latest_row, "ebitda_margin_ttm"))
    ebitda_margin_trend_ready = _metric_available(_mapping_value(latest_row, "ebitda_margin_trend_4q"))
    fcf_ready = _metric_available(_mapping_value(latest_row, "fcf_margin_ttm"))
    ebitda_consistency_ready = _consistency_inputs_ready(ttm_series_history)
    leverage_ready = _leverage_ratio(latest_row) is not None
    dilution_value = _mapping_value(latest_row, "share_dilution_yoy")
    dilution_ready = _metric_available(dilution_value) and abs(float(dilution_value)) <= 0.50
    checks = {
        "GROWTH_READY": growth_ready,
        "EBITDA_MARGIN_READY": ebitda_margin_ready,
        "EBITDA_MARGIN_TREND_READY": ebitda_margin_trend_ready,
        "FCF_READY": fcf_ready,
        "EBITDA_CONSISTENCY_READY": ebitda_consistency_ready,
        "LEVERAGE_READY": leverage_ready,
        "DILUTION_READY": dilution_ready,
    }
    missing = tuple(name for name, ready in checks.items() if not ready)
    return ScoreReadiness(
        score_ready=not missing,
        score_profile_status=SCORE_READY if not missing else SCORE_NOT_READY,
        growth_ready=growth_ready,
        ebitda_margin_ready=ebitda_margin_ready,
        ebitda_margin_trend_ready=ebitda_margin_trend_ready,
        fcf_ready=fcf_ready,
        ebitda_consistency_ready=ebitda_consistency_ready,
        leverage_ready=leverage_ready,
        dilution_ready=dilution_ready,
        missing_reasons=missing,
    )


def _metric_available(value: Any) -> bool:
    return value is not None


def _consistency_inputs_ready(ttm_series_history: list[Mapping[str, Any]]) -> bool:
    ordered_history = sorted(
        ttm_series_history,
        key=lambda row: str(_mapping_value(row, "as_of_date") or _mapping_value(row, "latest_period_end_date") or ""),
        reverse=True,
    )
    for metric_name in ("revenue_growth_ttm_yoy", "ebitda_margin_ttm", "fcf_margin_ttm"):
        values = [row for row in ordered_history if _mapping_value(row, metric_name) is not None]
        if len(values) < 3:
            return False
    return True


def update_scores(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
    dry_run: bool,
    as_of_dates: set[str] | None = None,
    skip_unchanged: bool = False,
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
        update = (
            explained_components["fundamental_score_recomputed"],
            explained_components["growth_component"],
            explained_components["margin_component"],
            explained_components["margin_trend_component"],
            explained_components["fcf_component"],
            explained_components["leverage_component"],
            explained_components["dilution_component"],
            explained_components["lifecycle_component"],
            explained_components["consistency_component"],
            ACTIVE_FUND_SCORE_RULE,
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
        if as_of_dates is not None and str(row["as_of_date"]) not in as_of_dates:
            continue
        if skip_unchanged and _score_update_matches_existing(row, update):
            continue
        score_updates.append(update)
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

    return len(score_updates), min_score, max_score, avg_score


def run_fundamental_scoring(
    conn: sqlite3.Connection,
    ticker: str | None,
    dry_run: bool,
    as_of_dates: list[str] | None = None,
    skip_unchanged: bool = False,
) -> tuple[int, float | None, float | None, float | None]:
    rows = load_ttm_rows(conn, ticker)
    return update_scores(
        conn,
        rows,
        dry_run,
        as_of_dates=None if as_of_dates is None else set(as_of_dates),
        skip_unchanged=skip_unchanged,
    )


def _score_update_matches_existing(row: sqlite3.Row, update: tuple[Any, ...]) -> bool:
    field_names = (
        "fundamental_score",
        "growth_component",
        "margin_component",
        "margin_trend_component",
        "fcf_component",
        "leverage_component",
        "dilution_component",
        "lifecycle_component",
        "consistency_component",
        "score_rule",
        "fundamental_score_lifecycle",
        "growth_component_lifecycle",
        "margin_component_lifecycle",
        "margin_trend_component_lifecycle",
        "fcf_component_lifecycle",
        "leverage_component_lifecycle",
        "dilution_component_lifecycle",
        "lifecycle_component_lifecycle",
        "consistency_component_lifecycle",
        "score_rule_lifecycle",
    )
    return all(row[field_name] == update[index] for index, field_name in enumerate(field_names))
