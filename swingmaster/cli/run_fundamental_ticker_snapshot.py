from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
import sys
from typing import Any

from analysis.candlestick_signal_reader import read_candlestick_signal_raw_export
from analysis.divergence_signal_reader import read_divergence_signal_raw_export
from analysis.moving_average_reader import read_moving_average_raw_export
from analysis.stock_dow_structure_reader import read_stock_dow_structure_raw_export
from swingmaster.fundamentals.price_behavior_snapshot import load_price_behavior_snapshot
from swingmaster.fundamentals.price_behavior_snapshot import _resolve_market_for_ticker
from swingmaster.fundamentals.score_percentile import FUND_SCORE_PERCENTILE_V2_PRE, compute_percentiles


FACTOR_SPECS = (
    ("growth_pct", "revenue_growth_ttm_yoy", True),
    ("margin_pct", "ebit_margin_ttm", True),
    ("margin_trend_pct", "ebit_margin_trend_4q", True),
    ("fcf_pct", "fcf_margin_ttm", True),
    ("leverage_pct", "net_debt_to_ebitda", False),
    ("dilution_pct", "share_dilution_yoy", False),
)
SECTIONED_METRICS: tuple[str | None, ...] = (
    "ticker",
    "quarter",
    "lifecycle_class",
    None,
    "fundamental_score_v1",
    "fundamental_score_v2_lifecycle",
    "score_lifecycle_delta",
    None,
    "growth_component",
    "margin_component",
    "margin_trend_component",
    "fcf_component",
    "consistency_component",
    "leverage_component",
    "dilution_component",
    None,
    "revenue_growth_ttm_yoy",
    "ebit_margin_ttm",
    "ebit_margin_trend_4q",
    "fcf_margin_ttm",
    "fcf_margin_trend_4q",
    "net_debt_to_ebitda",
    "share_dilution_yoy",
    None,
    "fundamental_score_percentile_global",
    "fundamental_score_percentile_sector",
    "fundamental_score_percentile_industry",
    "fundamental_score_percentile_blended",
    "fundamental_score_percentile_blended_lifecycle_weighted",
    "percentile_rank_bucket",
    "percentile_lifecycle_delta",
    None,
    "growth_pct_global",
    "margin_pct_global",
    "margin_trend_pct_global",
    "fcf_pct_global",
    "consistency_pct_global",
    "leverage_pct_global",
    "dilution_pct_global",
    None,
    "valuation_date",
    "valuation_fundamental_as_of_date",
    "valuation_fundamental_staleness_days",
    "valuation_ev_ebit",
    "valuation_fcf_yield",
    "valuation_ebit_margin",
    "valuation_bucket",
    "valuation_status",
    "valuation_model_version",
    None,
    "revenue",
    "operating_income",
    "free_cashflow",
    "shares_outstanding",
    "total_debt",
    None,
    "margin_trend_delta_4q",
    "fcf_margin_trend_delta_4q",
    "score_delta_qoq",
    "percentile_delta_qoq",
    "margin_trend_delta_qoq",
    "fcf_margin_trend_delta_qoq",
    "consistency_delta_qoq",
    "growth_pct_global_delta_qoq",
    "shares_outstanding_delta_4q",
    "net_debt_to_ebitda_delta_4q",
    None,
    "percentile_delta_4q",
    "score_delta_4q",
    "lifecycle_transition_4q",
    None,
    "sector_rank_position",
    "industry_rank_position",
)
DISPLAY_LABELS = {
    "growth_component": "growth_component (max 15p)",
    "margin_component": "margin_component (max 15p)",
    "margin_trend_component": "margin_trend_component (max 15p)",
    "fcf_component": "fcf_component (max 15p)",
    "consistency_component": "consistency_component (max 10p)",
    "leverage_component": "leverage_component (max 15p)",
    "dilution_component": "dilution_component (max 10p)",
}
CSV_OUTPUT_DIR = Path("/home/kalle/projects/swingmaster/ticker_fundamentals")
PRICE_BEHAVIOR_METRICS: tuple[str, ...] = (
    "price_behavior_as_of_date",
    "price_return_3m_pct",
    "price_return_6m_pct",
    "price_return_12m_pct",
    "distance_from_52w_high_pct",
    "relative_strength_6m_vs_sp500_pct",
    "price_return_since_last_report_pct",
    "relative_return_vs_sp500_since_last_report_pct",
    "earnings_reaction_1d_pct",
    "earnings_reaction_3d_pct",
    "post_earnings_drift_20d_pct",
    "volume_ratio_since_last_report_vs_3m_avg",
)
VALUATION_SNAPSHOT_METRICS: tuple[str, ...] = (
    "valuation_date",
    "valuation_fundamental_as_of_date",
    "valuation_fundamental_staleness_days",
    "valuation_ev_ebit",
    "valuation_fcf_yield",
    "valuation_ebit_margin",
    "adjusted_expensive_threshold",
    "valuation_debt_assumed_zero",
    "valuation_cash_assumed_zero",
    "valuation_bucket",
    "valuation_status",
    "valuation_model_version",
)
DOW_CONTEXT_SNAPSHOT_COLUMNS: tuple[str, ...] = (
    "ticker",
    "market",
    "as_of_date",
    "price_source",
    "pivot_radius",
    "status_found",
    "status_last_status",
    "calculated_from_date",
    "calculated_through_date",
    "latest_valid_close_date_on_or_before_as_of_date",
    "coverage_status",
    "coverage_reason",
    "latest_event_found",
    "latest_event_id",
    "latest_event_type",
    "latest_event_date",
    "latest_confirmed_as_of_date",
    "trend_state",
    "dow_label_high",
    "dow_label_low",
    "last_high_label",
    "last_high_label_date",
    "last_high_label_price",
    "last_low_label",
    "last_low_label_date",
    "last_low_label_price",
    "active_bos_high_date",
    "active_bos_high_price",
    "active_bos_low_date",
    "active_bos_low_price",
    "bos_up_count",
    "bos_down_count",
    "break_signal",
    "break_level_date",
    "break_level_price",
    "break_close_price",
    "reset_marker",
    "reset_reason",
    "structure_epoch_id",
    "structure_epoch_start_date",
    "recent_event_window_trading_days",
    "recent_event_available_trading_days",
    "recent_event_window_start_date",
    "recent_event_window_end_date",
    "dow_warning_flags",
)
DOW_RECENT_EVENTS_COLUMNS: tuple[str, ...] = (
    "ticker",
    "market",
    "as_of_date",
    "price_source",
    "pivot_radius",
    "sequence_window_trading_days",
    "sequence_available_trading_days",
    "sequence_window_start_date",
    "sequence_window_end_date",
    "sequence_index",
    "event_id",
    "event_date",
    "confirmed_as_of_date",
    "event_type",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "structure_price",
    "dow_label_high",
    "dow_label_low",
    "trend_state",
    "last_high_label",
    "last_high_label_date",
    "last_high_label_price",
    "last_low_label",
    "last_low_label_date",
    "last_low_label_price",
    "active_bos_high_date",
    "active_bos_high_price",
    "active_bos_low_date",
    "active_bos_low_price",
    "bos_up_count",
    "bos_down_count",
    "break_signal",
    "break_level_date",
    "break_level_price",
    "break_close_price",
    "reset_marker",
    "reset_reason",
    "structure_epoch_id",
    "structure_epoch_start_date",
    "calc_version",
    "run_id",
    "created_at_utc",
)
CANDLESTICK_EVENTS_COLUMNS: tuple[str, ...] = (
    "ticker",
    "market",
    "as_of_date",
    "sequence_window_trading_days",
    "sequence_available_trading_days",
    "sequence_window_start_date",
    "sequence_window_end_date",
    "sequence_index",
    "finding_id",
    "signal_date",
    "pattern",
    "pattern_group",
    "signal_strength",
    "rsi14",
    "created_at",
)
DIVERGENCE_CONTEXT_SNAPSHOT_COLUMNS: tuple[str, ...] = (
    "ticker",
    "market",
    "as_of_date",
    "sequence_window_trading_days",
    "sequence_available_trading_days",
    "sequence_window_start_date",
    "sequence_window_end_date",
    "latest_valid_close_date_on_or_before_as_of_date",
    "latest_divergence_date_on_or_before_as_of_date",
    "divergence_coverage_status",
    "divergence_coverage_reason",
    "latest_row_found",
    "latest_row_date",
    "latest_row_bullish_strength",
    "latest_row_bearish_strength",
    "latest_row_hidden_bullish_strength",
    "latest_row_hidden_bearish_strength",
    "latest_row_rsi",
    "latest_row_is_bullish_divergence_r2",
    "latest_row_is_bearish_divergence_r2",
    "latest_row_is_hidden_bullish_divergence_r2",
    "latest_row_is_hidden_bearish_divergence_r2",
    "latest_row_is_bullish_divergence_r3",
    "latest_row_is_bearish_divergence_r3",
    "latest_row_is_hidden_bullish_divergence_r3",
    "latest_row_is_hidden_bearish_divergence_r3",
    "latest_signal_found",
    "latest_signal_date",
    "latest_signal_pattern",
    "latest_signal_group",
    "latest_signal_variant",
    "latest_signal_direction",
    "latest_signal_radius",
    "latest_signal_source_flag",
    "divergence_warning_flags",
)
DIVERGENCE_SIGNALS_COLUMNS: tuple[str, ...] = (
    "ticker",
    "market",
    "as_of_date",
    "sequence_window_trading_days",
    "sequence_available_trading_days",
    "sequence_window_start_date",
    "sequence_window_end_date",
    "sequence_index",
    "signal_date",
    "divergence_pattern",
    "divergence_group",
    "divergence_variant",
    "divergence_direction",
    "divergence_radius",
    "signal_strength",
    "rsi",
    "pivot_gap",
    "pivot_drop_pct",
    "pivot2_date",
    "source_flag",
)
MOVING_AVERAGE_COLUMNS: tuple[str, ...] = (
    "ticker",
    "market",
    "as_of_date",
    "sequence_window_trading_days",
    "sequence_available_trading_days",
    "sequence_window_start_date",
    "sequence_window_end_date",
    "sequence_index",
    "trade_date",
    "stock_close",
    "stock_volume",
    "stock_ma50",
    "stock_ma200",
    "benchmark_ticker",
    "benchmark_trade_date",
    "benchmark_close",
    "benchmark_ma50",
    "benchmark_ma200",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print multi-quarter ticker snapshot with scores and percentiles")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--ticker", required=True, nargs="+", help="Ticker symbol or multiple ticker symbols")
    parser.add_argument("--output-dir", default=None, help="Optional directory for per-ticker snapshot files")
    parser.add_argument("--quarters", type=int, default=4, help="Number of latest quarters to print")
    parser.add_argument(
        "--rule-id",
        default=FUND_SCORE_PERCENTILE_V2_PRE,
        help="Stored percentile rule identifier used for percentile snapshot rows",
    )
    parser.add_argument(
        "--percentile-target-date",
        default=None,
        help="Optional stored percentile target_date; defaults to each row as_of_date",
    )
    parser.add_argument("--ohlcv-db", default=None, help="Optional OHLCV SQLite database path for price behavior snapshot")
    parser.add_argument(
        "--price-behavior-snapshot",
        action="store_true",
        help="Append latest price behavior snapshot block using OHLCV data",
    )
    parser.add_argument(
        "--dow-structure-snapshot",
        action="store_true",
        help="Append raw Dow structure snapshot blocks using analysis.db and OHLCV data",
    )
    parser.add_argument("--dow-analysis-db", default=None, help="Analysis SQLite database path for Dow structure reader")
    parser.add_argument("--dow-as-of-date", default=None, help="Explicit as-of date for Dow structure snapshot")
    parser.add_argument("--dow-market", default=None, help="Explicit market for Dow structure snapshot")
    parser.add_argument("--dow-pivot-radius", type=int, default=3, help="Pivot radius for Dow structure snapshot")
    parser.add_argument("--dow-price-source", default="close", help="Price source for Dow structure snapshot")
    parser.add_argument(
        "--dow-recent-window-trading-days",
        type=int,
        default=60,
        help="Recent trading-day window for Dow structure snapshot",
    )
    parser.add_argument(
        "--candlestick-snapshot",
        action="store_true",
        help="Append raw candlestick event rows using analysis.db and OHLCV data",
    )
    parser.add_argument("--candlestick-analysis-db", default=None, help="Analysis SQLite database path for candlestick reader")
    parser.add_argument("--candlestick-as-of-date", default=None, help="Explicit as-of date for candlestick snapshot")
    parser.add_argument("--candlestick-market", default=None, help="Explicit market for candlestick snapshot")
    parser.add_argument(
        "--candlestick-recent-window-trading-days",
        type=int,
        default=60,
        help="Recent trading-day window for candlestick snapshot",
    )
    parser.add_argument(
        "--divergence-snapshot",
        action="store_true",
        help="Append raw divergence snapshot blocks using analysis.db and OHLCV data",
    )
    parser.add_argument("--divergence-analysis-db", default=None, help="Analysis SQLite database path for divergence reader")
    parser.add_argument("--divergence-as-of-date", default=None, help="Explicit as-of date for divergence snapshot")
    parser.add_argument("--divergence-market", default=None, help="Explicit market for divergence snapshot")
    parser.add_argument(
        "--divergence-recent-window-trading-days",
        type=int,
        default=60,
        help="Recent trading-day window for divergence snapshot",
    )
    parser.add_argument(
        "--moving-average-snapshot",
        action="store_true",
        help="Append raw moving average rows using OHLCV data",
    )
    parser.add_argument("--moving-average-as-of-date", default=None, help="Explicit as-of date for moving average snapshot")
    parser.add_argument("--moving-average-market", default=None, help="Explicit market for moving average snapshot")
    parser.add_argument(
        "--moving-average-recent-window-trading-days",
        type=int,
        default=60,
        help="Recent trading-day window for moving average snapshot",
    )
    parser.add_argument("--moving-average-short-window", type=int, default=50, help="Short moving average window")
    parser.add_argument("--moving-average-long-window", type=int, default=200, help="Long moving average window")
    parser.add_argument("--moving-average-benchmark-ticker", default="^GSPC", help="Benchmark ticker for moving average snapshot")
    parser.add_argument("--moving-average-benchmark-market", default="usa", help="Benchmark market for moving average snapshot")
    args = parser.parse_args()
    if args.dow_structure_snapshot and not args.dow_analysis_db:
        parser.error("--dow-analysis-db is required when --dow-structure-snapshot is used")
    if args.dow_structure_snapshot and not args.ohlcv_db:
        parser.error("--ohlcv-db is required when --dow-structure-snapshot is used")
    if args.candlestick_snapshot and not args.candlestick_analysis_db:
        parser.error("--candlestick-analysis-db is required when --candlestick-snapshot is used")
    if args.candlestick_snapshot and not args.ohlcv_db:
        parser.error("--ohlcv-db is required when --candlestick-snapshot is used")
    if args.divergence_snapshot and not args.divergence_analysis_db:
        parser.error("--divergence-analysis-db is required when --divergence-snapshot is used")
    if args.divergence_snapshot and not args.ohlcv_db:
        parser.error("--ohlcv-db is required when --divergence-snapshot is used")
    if args.moving_average_snapshot and not args.ohlcv_db:
        parser.error("--ohlcv-db is required when --moving-average-snapshot is used")
    parsed_tickers = _parse_ticker_args(args.ticker)
    if not parsed_tickers:
        parser.error("--ticker must contain at least one ticker")
    if len(parsed_tickers) > 1 and not args.output_dir:
        parser.error("--output-dir is required when --ticker contains multiple tickers")
    args.ticker = parsed_tickers
    return args


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def _parse_ticker_args(raw_ticker_args: str | list[str] | tuple[str, ...]) -> list[str]:
    if isinstance(raw_ticker_args, str):
        raw_values = [raw_ticker_args]
    else:
        raw_values = list(raw_ticker_args)
    tickers: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        for part in re.split(r"[\s,]+", raw_value.strip()):
            if not part:
                continue
            ticker = part.upper()
            if ticker in seen:
                continue
            seen.add(ticker)
            tickers.append(ticker)
    return tickers


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return float(value)


def _format_optional_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def _format_rank_position(rank: int | None, size: int | None) -> str:
    if rank is None or size is None:
        return ""
    return f"Sijalla {rank}/{size}"


def load_latest_quarter_rows(conn: sqlite3.Connection, ticker: str, quarters: int) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                ticker,
                as_of_date,
                latest_period_end_date,
                lifecycle_class,
                fundamental_score,
                fundamental_score_lifecycle,
                growth_component,
                margin_component,
                margin_trend_component,
                fcf_component,
                consistency_component,
                leverage_component,
                dilution_component,
                revenue_growth_ttm_yoy,
                ebit_margin_ttm,
                ebit_margin_trend_4q,
                fcf_margin_ttm,
                fcf_margin_trend_4q,
                net_debt_to_ebitda,
                share_dilution_yoy
            FROM rc_fundamental_ttm
            WHERE ticker = ?
            ORDER BY as_of_date DESC
            LIMIT ?
            """,
            (ticker.upper(), quarters),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    if not rows:
        raise RuntimeError(f"FUNDAMENTAL_TTM_NOT_FOUND:{ticker.upper()}")
    return list(reversed(rows))


def load_quarterly_rows(conn: sqlite3.Connection, ticker: str, period_end_dates: list[str]) -> dict[tuple[str, str], sqlite3.Row]:
    placeholders = ", ".join("?" for _ in period_end_dates)
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT ticker, period_end_date, revenue, operating_income, free_cashflow, shares_outstanding, total_debt
            FROM rc_fundamental_quarterly
            WHERE ticker = ?
              AND period_end_date IN ({placeholders})
            """,
            [ticker.upper(), *period_end_dates],
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return {(str(row["ticker"]), str(row["period_end_date"])): row for row in rows}


def load_peer_rows_by_date(conn: sqlite3.Connection, as_of_dates: list[str]) -> dict[str, list[sqlite3.Row]]:
    placeholders = ", ".join("?" for _ in as_of_dates)
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT
                ticker,
                as_of_date,
                revenue_growth_ttm_yoy,
                ebit_margin_ttm,
                ebit_margin_trend_4q,
                fcf_margin_ttm,
                net_debt_to_ebitda,
                share_dilution_yoy
            FROM rc_fundamental_ttm
            WHERE as_of_date IN ({placeholders})
            ORDER BY as_of_date ASC, ticker ASC
            """,
            as_of_dates,
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory

    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        grouped[str(row["as_of_date"])].append(row)
    return grouped


def compute_factor_percentiles_by_date(peer_rows_by_date: dict[str, list[sqlite3.Row]]) -> dict[str, dict[str, dict[str, float]]]:
    result: dict[str, dict[str, dict[str, float]]] = {}
    for as_of_date, peer_rows in peer_rows_by_date.items():
        factor_scores: dict[str, dict[str, float]] = {}
        for metric_name, column_name, higher_is_better in FACTOR_SPECS:
            values: list[tuple[str, float]] = []
            for row in peer_rows:
                value = _coerce_optional_float(row[column_name])
                if value is None:
                    continue
                values.append((str(row["ticker"]), value))
            factor_scores[metric_name] = compute_percentiles(values, higher_is_better=higher_is_better)
        result[as_of_date] = factor_scores
    return result


def load_stored_percentile_rows(
    conn: sqlite3.Connection,
    rule_id: str,
    target_dates: list[str],
) -> dict[tuple[str, str, str], sqlite3.Row]:
    if not target_dates:
        return {}
    placeholders = ", ".join("?" for _ in target_dates)
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT
                p.*,
                RANK() OVER (
                    PARTITION BY p.target_date, p.sector
                    ORDER BY p.fundamental_score_percentile_blended_lifecycle_weighted DESC
                ) AS sector_rank,
                RANK() OVER (
                    PARTITION BY p.target_date, p.industry
                    ORDER BY p.fundamental_score_percentile_blended_lifecycle_weighted DESC
                ) AS industry_rank
            FROM rc_fundamental_score_percentile p
            WHERE p.rule_id = ?
              AND p.target_date IN ({placeholders})
            ORDER BY p.ticker ASC, p.as_of_date ASC
            """,
            [rule_id, *target_dates],
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return {(str(row["ticker"]), str(row["target_date"]), str(row["as_of_date"])): row for row in rows}


def load_stored_valuation_rows(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_dates: list[str],
) -> dict[tuple[str, str], sqlite3.Row]:
    if not as_of_dates:
        return {}
    placeholders = ", ".join("?" for _ in as_of_dates)
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT *
            FROM rc_fundamental_valuation
            WHERE ticker = ?
              AND as_of_date IN ({placeholders})
            ORDER BY as_of_date ASC
            """,
            [ticker.upper(), *as_of_dates],
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return {(str(row["ticker"]), str(row["as_of_date"])): row for row in rows}


def load_latest_valuation_snapshot(conn: sqlite3.Connection, ticker: str) -> dict[str, str]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT *
            FROM rc_fundamental_valuation
            WHERE ticker = ?
            ORDER BY as_of_date DESC
            LIMIT 1
            """,
            (ticker.upper(),),
        ).fetchone()
    finally:
        conn.row_factory = previous_row_factory

    if row is None:
        return {metric: "" for metric in VALUATION_SNAPSHOT_METRICS}

    return {
        "valuation_date": str(row["as_of_date"]) if row["as_of_date"] is not None else "",
        "valuation_fundamental_as_of_date": str(row["valuation_fundamental_as_of_date"]) if row["valuation_fundamental_as_of_date"] is not None else "",
        "valuation_fundamental_staleness_days": str(row["valuation_fundamental_staleness_days"]) if row["valuation_fundamental_staleness_days"] is not None else "",
        "valuation_ev_ebit": _format_optional_float(_coerce_optional_float(row["valuation_ev_ebit"])),
        "valuation_fcf_yield": _format_optional_float(_coerce_optional_float(row["valuation_fcf_yield"])),
        "valuation_ebit_margin": _format_optional_float(_coerce_optional_float(row["valuation_ebit_margin"])),
        "adjusted_expensive_threshold": _format_optional_float(_coerce_optional_float(row["adjusted_expensive_threshold"])),
        "valuation_debt_assumed_zero": str(row["debt_assumed_zero"]) if row["debt_assumed_zero"] is not None else "",
        "valuation_cash_assumed_zero": str(row["cash_assumed_zero"]) if row["cash_assumed_zero"] is not None else "",
        "valuation_bucket": str(row["valuation_bucket"]) if row["valuation_bucket"] is not None else "",
        "valuation_status": str(row["valuation_status"]) if row["valuation_status"] is not None else "",
        "valuation_model_version": str(row["valuation_model_version"]) if row["valuation_model_version"] is not None else "",
    }


def _delta_formatted(current_value: object, previous_value: object) -> str:
    current = _coerce_optional_float(current_value)
    previous = _coerce_optional_float(previous_value)
    if current is None or previous is None:
        return ""
    return f"{current - previous:.2f}"


def _percentile_rank_bucket(value: object) -> str:
    score = _coerce_optional_float(value)
    if score is None:
        return ""
    if score >= 90:
        return "Top 10%"
    if score >= 80:
        return "Top 20%"
    if score >= 70:
        return "Top 30%"
    if score >= 60:
        return "Top 40%"
    if score >= 50:
        return "Above median"
    if score >= 40:
        return "Neutral"
    if score >= 30:
        return "Weak"
    if score >= 20:
        return "Very weak"
    return "Bottom bucket"


def build_snapshot_matrix(
    conn: sqlite3.Connection,
    ticker: str,
    quarters: int,
    rule_id: str,
    percentile_target_date: str | None,
) -> list[dict[str, str]]:
    quarter_rows = load_latest_quarter_rows(conn, ticker, quarters)
    as_of_dates = [str(row["as_of_date"]) for row in quarter_rows]
    period_end_dates = [str(row["latest_period_end_date"]) for row in quarter_rows]
    quarterly_rows = load_quarterly_rows(conn, ticker, period_end_dates)
    peer_rows_by_date = load_peer_rows_by_date(conn, as_of_dates)
    factor_percentiles_by_date = compute_factor_percentiles_by_date(peer_rows_by_date)
    stored_target_dates = [percentile_target_date] if percentile_target_date is not None else as_of_dates
    stored_percentiles = load_stored_percentile_rows(conn, rule_id, stored_target_dates)
    stored_valuations = load_stored_valuation_rows(conn, ticker, as_of_dates)

    earliest_row = quarter_rows[0]
    latest_row = quarter_rows[-1]
    earliest_target_date = percentile_target_date if percentile_target_date is not None else str(earliest_row["as_of_date"])
    latest_target_date = percentile_target_date if percentile_target_date is not None else str(latest_row["as_of_date"])
    earliest_stored = stored_percentiles.get((ticker.upper(), earliest_target_date, str(earliest_row["as_of_date"])))
    latest_stored = stored_percentiles.get((ticker.upper(), latest_target_date, str(latest_row["as_of_date"])))
    earliest_quarterly = quarterly_rows.get((ticker.upper(), str(earliest_row["latest_period_end_date"])))
    latest_quarterly = quarterly_rows.get((ticker.upper(), str(latest_row["latest_period_end_date"])))
    percentile_delta_4q = _delta_formatted(
        latest_stored["fundamental_score_percentile_blended_lifecycle_weighted"] if latest_stored is not None else None,
        earliest_stored["fundamental_score_percentile_blended"] if earliest_stored is not None else None,
    )
    score_delta_4q = _delta_formatted(latest_row["fundamental_score_lifecycle"], earliest_row["fundamental_score_lifecycle"])
    margin_trend_delta_4q = _delta_formatted(latest_row["ebit_margin_trend_4q"], earliest_row["ebit_margin_trend_4q"])
    fcf_margin_trend_delta_4q = _delta_formatted(latest_row["fcf_margin_trend_4q"], earliest_row["fcf_margin_trend_4q"])
    shares_outstanding_delta_4q = _delta_formatted(
        latest_quarterly["shares_outstanding"] if latest_quarterly is not None else None,
        earliest_quarterly["shares_outstanding"] if earliest_quarterly is not None else None,
    )
    net_debt_to_ebitda_delta_4q = _delta_formatted(latest_row["net_debt_to_ebitda"], earliest_row["net_debt_to_ebitda"])
    lifecycle_transition_4q = ""
    if earliest_row["lifecycle_class"] is not None and latest_row["lifecycle_class"] is not None:
        lifecycle_transition_4q = f"{earliest_row['lifecycle_class']} to {latest_row['lifecycle_class']}"

    matrix_rows: list[dict[str, str]] = []
    for index, row in enumerate(quarter_rows):
        as_of_date = str(row["as_of_date"])
        period_end_date = str(row["latest_period_end_date"])
        row_target_date = percentile_target_date if percentile_target_date is not None else as_of_date
        stored_row = stored_percentiles.get((ticker.upper(), row_target_date, as_of_date))
        factor_percentiles = factor_percentiles_by_date.get(as_of_date, {})
        quarterly_row = quarterly_rows.get((ticker.upper(), period_end_date))
        valuation_row = stored_valuations.get((ticker.upper(), as_of_date))
        is_last_quarter = index == len(quarter_rows) - 1
        score_delta = _delta_formatted(row["fundamental_score_lifecycle"], row["fundamental_score"])
        percentile_delta = _delta_formatted(
            stored_row["fundamental_score_percentile_blended_lifecycle_weighted"] if stored_row is not None else None,
            stored_row["fundamental_score_percentile_blended"] if stored_row is not None else None,
        )

        matrix_rows.append(
            {
                "ticker": ticker.upper(),
                "quarter": as_of_date,
                "lifecycle_class": str(row["lifecycle_class"]) if row["lifecycle_class"] is not None else "",
                "fundamental_score_v1": _format_optional_float(_coerce_optional_float(row["fundamental_score"])),
                "fundamental_score_v2_lifecycle": _format_optional_float(_coerce_optional_float(row["fundamental_score_lifecycle"])),
                "score_lifecycle_delta": score_delta,
                "growth_component": _format_optional_float(_coerce_optional_float(row["growth_component"])),
                "margin_component": _format_optional_float(_coerce_optional_float(row["margin_component"])),
                "margin_trend_component": _format_optional_float(_coerce_optional_float(row["margin_trend_component"])),
                "fcf_component": _format_optional_float(_coerce_optional_float(row["fcf_component"])),
                "consistency_component": _format_optional_float(_coerce_optional_float(row["consistency_component"])),
                "leverage_component": _format_optional_float(_coerce_optional_float(row["leverage_component"])),
                "dilution_component": _format_optional_float(_coerce_optional_float(row["dilution_component"])),
                "revenue_growth_ttm_yoy": _format_optional_float(_coerce_optional_float(row["revenue_growth_ttm_yoy"])),
                "ebit_margin_ttm": _format_optional_float(_coerce_optional_float(row["ebit_margin_ttm"])),
                "ebit_margin_trend_4q": _format_optional_float(_coerce_optional_float(row["ebit_margin_trend_4q"])),
                "fcf_margin_ttm": _format_optional_float(_coerce_optional_float(row["fcf_margin_ttm"])),
                "fcf_margin_trend_4q": _format_optional_float(_coerce_optional_float(row["fcf_margin_trend_4q"])),
                "net_debt_to_ebitda": _format_optional_float(_coerce_optional_float(row["net_debt_to_ebitda"])),
                "share_dilution_yoy": _format_optional_float(_coerce_optional_float(row["share_dilution_yoy"])),
                "fundamental_score_percentile_global": _format_optional_float(_coerce_optional_float(stored_row["fundamental_score_percentile_global"] if stored_row is not None else None)),
                "fundamental_score_percentile_sector": _format_optional_float(_coerce_optional_float(stored_row["fundamental_score_percentile_sector"] if stored_row is not None else None)),
                "fundamental_score_percentile_industry": _format_optional_float(_coerce_optional_float(stored_row["fundamental_score_percentile_industry"] if stored_row is not None else None)),
                "fundamental_score_percentile_blended": _format_optional_float(_coerce_optional_float(stored_row["fundamental_score_percentile_blended"] if stored_row is not None else None)),
                "fundamental_score_percentile_blended_lifecycle_weighted": _format_optional_float(_coerce_optional_float(stored_row["fundamental_score_percentile_blended_lifecycle_weighted"] if stored_row is not None else None)),
                "percentile_rank_bucket": _percentile_rank_bucket(
                    stored_row["fundamental_score_percentile_blended_lifecycle_weighted"] if stored_row is not None else None
                ),
                "percentile_lifecycle_delta": percentile_delta,
                "growth_pct_global": _format_optional_float(_coerce_optional_float(stored_row["growth_pct_global"] if stored_row is not None else None)),
                "margin_pct_global": _format_optional_float(_coerce_optional_float(stored_row["margin_pct_global"] if stored_row is not None else None)),
                "margin_trend_pct_global": _format_optional_float(_coerce_optional_float(stored_row["margin_trend_pct_global"] if stored_row is not None else None)),
                "fcf_pct_global": _format_optional_float(_coerce_optional_float(stored_row["fcf_pct_global"] if stored_row is not None else None)),
                "consistency_pct_global": _format_optional_float(_coerce_optional_float(stored_row["consistency_pct_global"] if stored_row is not None else None)),
                "leverage_pct_global": _format_optional_float(_coerce_optional_float(stored_row["leverage_pct_global"] if stored_row is not None else None)),
                "dilution_pct_global": _format_optional_float(_coerce_optional_float(stored_row["dilution_pct_global"] if stored_row is not None else None)),
                "valuation_date": str(valuation_row["as_of_date"]) if valuation_row is not None and valuation_row["as_of_date"] is not None else "",
                "valuation_fundamental_as_of_date": str(valuation_row["valuation_fundamental_as_of_date"]) if valuation_row is not None and valuation_row["valuation_fundamental_as_of_date"] is not None else "",
                "valuation_fundamental_staleness_days": str(valuation_row["valuation_fundamental_staleness_days"]) if valuation_row is not None and valuation_row["valuation_fundamental_staleness_days"] is not None else "",
                "valuation_ev_ebit": _format_optional_float(_coerce_optional_float(valuation_row["valuation_ev_ebit"] if valuation_row is not None else None)),
                "valuation_fcf_yield": _format_optional_float(_coerce_optional_float(valuation_row["valuation_fcf_yield"] if valuation_row is not None else None)),
                "valuation_ebit_margin": _format_optional_float(_coerce_optional_float(valuation_row["valuation_ebit_margin"] if valuation_row is not None else None)),
                "valuation_bucket": str(valuation_row["valuation_bucket"]) if valuation_row is not None and valuation_row["valuation_bucket"] is not None else "",
                "valuation_status": str(valuation_row["valuation_status"]) if valuation_row is not None and valuation_row["valuation_status"] is not None else "",
                "valuation_model_version": str(valuation_row["valuation_model_version"]) if valuation_row is not None and valuation_row["valuation_model_version"] is not None else "",
                "revenue": _format_optional_float(_coerce_optional_float(quarterly_row["revenue"] if quarterly_row is not None else None)),
                "operating_income": _format_optional_float(_coerce_optional_float(quarterly_row["operating_income"] if quarterly_row is not None else None)),
                "free_cashflow": _format_optional_float(_coerce_optional_float(quarterly_row["free_cashflow"] if quarterly_row is not None else None)),
                "shares_outstanding": _format_optional_float(_coerce_optional_float(quarterly_row["shares_outstanding"] if quarterly_row is not None else None)),
                "total_debt": _format_optional_float(_coerce_optional_float(quarterly_row["total_debt"] if quarterly_row is not None else None)),
                "margin_trend_delta_4q": margin_trend_delta_4q if is_last_quarter else "",
                "fcf_margin_trend_delta_4q": fcf_margin_trend_delta_4q if is_last_quarter else "",
                "score_delta_qoq": "",
                "percentile_delta_qoq": "",
                "margin_trend_delta_qoq": "",
                "fcf_margin_trend_delta_qoq": "",
                "consistency_delta_qoq": "",
                "growth_pct_global_delta_qoq": "",
                "shares_outstanding_delta_4q": shares_outstanding_delta_4q if is_last_quarter else "",
                "net_debt_to_ebitda_delta_4q": net_debt_to_ebitda_delta_4q if is_last_quarter else "",
                "sector_rank_position": _format_rank_position(
                    int(stored_row["sector_rank"]) if stored_row is not None and stored_row["sector_rank"] is not None else None,
                    int(stored_row["sector_size"]) if stored_row is not None and stored_row["sector_size"] is not None else None,
                )
                + (
                    f" ({stored_row['sector']})"
                    if is_last_quarter and stored_row is not None and stored_row["sector"] is not None and stored_row["sector_rank"] is not None
                    else ""
                ),
                "industry_rank_position": _format_rank_position(
                    int(stored_row["industry_rank"]) if stored_row is not None and stored_row["industry_rank"] is not None else None,
                    int(stored_row["industry_size"]) if stored_row is not None and stored_row["industry_size"] is not None else None,
                )
                + (
                    f" ({stored_row['industry']})"
                    if is_last_quarter and stored_row is not None and stored_row["industry"] is not None and stored_row["industry_rank"] is not None
                    else ""
                ),
                "percentile_delta_4q": percentile_delta_4q if is_last_quarter else "",
                "score_delta_4q": score_delta_4q if is_last_quarter else "",
                "lifecycle_transition_4q": lifecycle_transition_4q if is_last_quarter else "",
                "growth_pct": _format_optional_float(factor_percentiles.get("growth_pct", {}).get(ticker.upper())),
                "margin_pct": _format_optional_float(factor_percentiles.get("margin_pct", {}).get(ticker.upper())),
                "margin_trend_pct": _format_optional_float(factor_percentiles.get("margin_trend_pct", {}).get(ticker.upper())),
                "fcf_pct": _format_optional_float(factor_percentiles.get("fcf_pct", {}).get(ticker.upper())),
                "leverage_pct": _format_optional_float(factor_percentiles.get("leverage_pct", {}).get(ticker.upper())),
                "dilution_pct": _format_optional_float(factor_percentiles.get("dilution_pct", {}).get(ticker.upper())),
            }
        )
    for index in range(1, len(matrix_rows)):
        previous_row = matrix_rows[index - 1]
        current_row = matrix_rows[index]
        current_row["score_delta_qoq"] = _delta_formatted(
            current_row["fundamental_score_v2_lifecycle"],
            previous_row["fundamental_score_v2_lifecycle"],
        )
        current_row["percentile_delta_qoq"] = _delta_formatted(
            current_row["fundamental_score_percentile_blended_lifecycle_weighted"],
            previous_row["fundamental_score_percentile_blended_lifecycle_weighted"],
        )
        current_row["margin_trend_delta_qoq"] = _delta_formatted(
            current_row["ebit_margin_trend_4q"],
            previous_row["ebit_margin_trend_4q"],
        )
        current_row["fcf_margin_trend_delta_qoq"] = _delta_formatted(
            current_row["fcf_margin_trend_4q"],
            previous_row["fcf_margin_trend_4q"],
        )
        current_row["consistency_delta_qoq"] = _delta_formatted(
            current_row["consistency_pct_global"],
            previous_row["consistency_pct_global"],
        )
        current_row["growth_pct_global_delta_qoq"] = _delta_formatted(
            current_row["growth_pct_global"],
            previous_row["growth_pct_global"],
        )
    return matrix_rows


def format_snapshot_matrix(
    matrix_rows: list[dict[str, str]],
    price_behavior_snapshot: dict[str, str] | None = None,
    valuation_snapshot: dict[str, str] | None = None,
    dow_structure_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    candlestick_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    divergence_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    moving_average_snapshot: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    lines: list[str] = []
    for metric in SECTIONED_METRICS:
        if metric is None:
            lines.append("")
            continue
        row_values = [row[metric] for row in matrix_rows]
        label = DISPLAY_LABELS.get(metric, metric)
        lines.append(";".join([label, *row_values]))
    if price_behavior_snapshot is not None:
        lines.append("")
        lines.append("price_behavior_snapshot")
        for metric in PRICE_BEHAVIOR_METRICS:
            lines.append(f"{metric};{price_behavior_snapshot.get(metric, '')}")
    if valuation_snapshot is not None:
        lines.append("")
        lines.append("valuation_snapshot")
        for metric in VALUATION_SNAPSHOT_METRICS:
            lines.append(f"{metric};{valuation_snapshot.get(metric, '')}")
    if dow_structure_snapshot is not None:
        lines.extend(_format_dow_structure_snapshot_lines(dow_structure_snapshot))
    if candlestick_snapshot is not None:
        lines.extend(_format_candlestick_snapshot_lines(candlestick_snapshot))
    if divergence_snapshot is not None:
        lines.extend(_format_divergence_snapshot_lines(divergence_snapshot))
    if moving_average_snapshot is not None:
        lines.extend(_format_moving_average_snapshot_lines(moving_average_snapshot))
    return "\n".join(lines)


def resolve_output_date() -> str:
    return datetime.now().astimezone().date().isoformat()


def _sanitize_ticker_for_filename(ticker: str) -> str:
    sanitized = re.sub(r"[/\\\\:\s]+", "_", ticker.strip())
    if not sanitized:
        raise RuntimeError(f"FUNDAMENTAL_TICKER_SNAPSHOT_INVALID_FILENAME_TICKER:{ticker}")
    return sanitized


def _resolve_output_dir(output_dir_arg: str) -> Path:
    output_dir = Path(output_dir_arg).expanduser().resolve()
    if output_dir.exists() and not output_dir.is_dir():
        raise RuntimeError(f"FUNDAMENTAL_TICKER_SNAPSHOT_OUTPUT_DIR_NOT_DIRECTORY:{output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def _write_snapshot_output_file(
    output_dir: Path,
    ticker: str,
    output_date: str,
    snapshot_output: str,
) -> Path:
    safe_ticker = _sanitize_ticker_for_filename(ticker.upper())
    output_path = output_dir / f"{safe_ticker}_{output_date}.csv"
    output_path.write_text(snapshot_output, encoding="utf-8", newline="\n")
    return output_path


def _format_csv_value(value: str) -> str:
    if value and value.replace(".", "", 1).replace("-", "", 1).isdigit() and value.count(".") == 1:
        return value.replace(".", ",")
    return value


def write_snapshot_csv(
    matrix_rows: list[dict[str, str]],
    ticker: str,
    output_date: str,
    price_behavior_snapshot: dict[str, str] | None = None,
    valuation_snapshot: dict[str, str] | None = None,
    dow_structure_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    candlestick_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    divergence_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    moving_average_snapshot: dict[str, list[dict[str, Any]]] | None = None,
) -> Path:
    CSV_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = CSV_OUTPUT_DIR / f"{ticker.upper()}_{output_date}.csv"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        for metric in SECTIONED_METRICS:
            if metric is None:
                writer.writerow([])
                continue
            row_values = [_format_csv_value(row[metric]) for row in matrix_rows]
            writer.writerow([DISPLAY_LABELS.get(metric, metric), *row_values])
        if price_behavior_snapshot is not None:
            writer.writerow([])
            writer.writerow(["price_behavior_snapshot"])
            for metric in PRICE_BEHAVIOR_METRICS:
                writer.writerow([metric, _format_csv_value(price_behavior_snapshot.get(metric, ""))])
        if valuation_snapshot is not None:
            writer.writerow([])
            writer.writerow(["valuation_snapshot"])
            for metric in VALUATION_SNAPSHOT_METRICS:
                writer.writerow([metric, _format_csv_value(valuation_snapshot.get(metric, ""))])
        if dow_structure_snapshot is not None:
            _write_dow_structure_snapshot_csv(writer, dow_structure_snapshot)
        if candlestick_snapshot is not None:
            _write_candlestick_snapshot_csv(writer, candlestick_snapshot)
        if divergence_snapshot is not None:
            _write_divergence_snapshot_csv(writer, divergence_snapshot)
        if moving_average_snapshot is not None:
            _write_moving_average_snapshot_csv(writer, moving_average_snapshot)
    return output_path


def ensure_snapshot_csv_written(
    matrix_rows: list[dict[str, str]],
    ticker: str,
    output_date: str,
    price_behavior_snapshot: dict[str, str] | None = None,
    valuation_snapshot: dict[str, str] | None = None,
    dow_structure_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    candlestick_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    divergence_snapshot: dict[str, list[dict[str, Any]]] | None = None,
    moving_average_snapshot: dict[str, list[dict[str, Any]]] | None = None,
) -> Path:
    output_path = write_snapshot_csv(
        matrix_rows,
        ticker,
        output_date,
        price_behavior_snapshot,
        valuation_snapshot,
        dow_structure_snapshot,
        candlestick_snapshot,
        divergence_snapshot,
        moving_average_snapshot,
    )
    if output_path.exists() and output_path.stat().st_size > 0:
        return output_path
    output_path = write_snapshot_csv(
        matrix_rows,
        ticker,
        output_date,
        price_behavior_snapshot,
        valuation_snapshot,
        dow_structure_snapshot,
        candlestick_snapshot,
        divergence_snapshot,
        moving_average_snapshot,
    )
    if output_path.exists() and output_path.stat().st_size > 0:
        return output_path
    raise RuntimeError(f"FUNDAMENTAL_TICKER_SNAPSHOT_CSV_NOT_WRITTEN:{output_path}")


def _format_dow_export_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return "|".join(str(item) for item in value)
    return str(value)


def _format_dow_row_values(row: dict[str, Any], columns: tuple[str, ...]) -> list[str]:
    return [_format_dow_export_value(row.get(column)) for column in columns]


def _format_candlestick_row_values(row: dict[str, Any], columns: tuple[str, ...]) -> list[str]:
    return [_format_csv_value(_format_dow_export_value(row.get(column))) for column in columns]


def _format_divergence_row_values(row: dict[str, Any], columns: tuple[str, ...]) -> list[str]:
    return [_format_csv_value(_format_dow_export_value(row.get(column))) for column in columns]


def _format_moving_average_row_values(row: dict[str, Any], columns: tuple[str, ...]) -> list[str]:
    return [_format_csv_value(_format_dow_export_value(row.get(column))) for column in columns]


def _format_dow_structure_snapshot_lines(dow_structure_snapshot: dict[str, list[dict[str, Any]]]) -> list[str]:
    lines = ["", "section;dow_context_snapshot", ";".join(DOW_CONTEXT_SNAPSHOT_COLUMNS)]
    context_rows = dow_structure_snapshot.get("context_snapshot_rows", [])
    for row in context_rows:
        lines.append(";".join(_format_dow_row_values(row, DOW_CONTEXT_SNAPSHOT_COLUMNS)))
    lines.append("")
    lines.append("section;dow_recent_events_60td")
    lines.append(";".join(DOW_RECENT_EVENTS_COLUMNS))
    recent_rows = dow_structure_snapshot.get("recent_event_rows_60td", [])
    for row in recent_rows:
        lines.append(";".join(_format_dow_row_values(row, DOW_RECENT_EVENTS_COLUMNS)))
    return lines


def _write_dow_structure_snapshot_csv(
    writer: csv.writer,
    dow_structure_snapshot: dict[str, list[dict[str, Any]]],
) -> None:
    writer.writerow([])
    writer.writerow(["section", "dow_context_snapshot"])
    writer.writerow(list(DOW_CONTEXT_SNAPSHOT_COLUMNS))
    for row in dow_structure_snapshot.get("context_snapshot_rows", []):
        writer.writerow([_format_csv_value(value) for value in _format_dow_row_values(row, DOW_CONTEXT_SNAPSHOT_COLUMNS)])
    writer.writerow([])
    writer.writerow(["section", "dow_recent_events_60td"])
    writer.writerow(list(DOW_RECENT_EVENTS_COLUMNS))
    for row in dow_structure_snapshot.get("recent_event_rows_60td", []):
        writer.writerow([_format_csv_value(value) for value in _format_dow_row_values(row, DOW_RECENT_EVENTS_COLUMNS)])


def _format_candlestick_snapshot_lines(candlestick_snapshot: dict[str, list[dict[str, Any]]]) -> list[str]:
    lines = ["", "section;candlestick_events_60td", ";".join(CANDLESTICK_EVENTS_COLUMNS)]
    for row in candlestick_snapshot.get("candlestick_event_rows_60td", []):
        lines.append(";".join(_format_candlestick_row_values(row, CANDLESTICK_EVENTS_COLUMNS)))
    return lines


def _write_candlestick_snapshot_csv(
    writer: csv.writer,
    candlestick_snapshot: dict[str, list[dict[str, Any]]],
) -> None:
    writer.writerow([])
    writer.writerow(["section", "candlestick_events_60td"])
    writer.writerow(list(CANDLESTICK_EVENTS_COLUMNS))
    for row in candlestick_snapshot.get("candlestick_event_rows_60td", []):
        writer.writerow([_format_csv_value(value) for value in _format_candlestick_row_values(row, CANDLESTICK_EVENTS_COLUMNS)])


def _format_divergence_snapshot_lines(divergence_snapshot: dict[str, list[dict[str, Any]]]) -> list[str]:
    lines = ["", "section;divergence_context_snapshot", ";".join(DIVERGENCE_CONTEXT_SNAPSHOT_COLUMNS)]
    for row in divergence_snapshot.get("divergence_context_snapshot_rows", []):
        lines.append(";".join(_format_divergence_row_values(row, DIVERGENCE_CONTEXT_SNAPSHOT_COLUMNS)))
    lines.append("")
    lines.append("section;divergence_signals_60td")
    lines.append(";".join(DIVERGENCE_SIGNALS_COLUMNS))
    for row in divergence_snapshot.get("divergence_signal_rows_60td", []):
        lines.append(";".join(_format_divergence_row_values(row, DIVERGENCE_SIGNALS_COLUMNS)))
    return lines


def _write_divergence_snapshot_csv(
    writer: csv.writer,
    divergence_snapshot: dict[str, list[dict[str, Any]]],
) -> None:
    writer.writerow([])
    writer.writerow(["section", "divergence_context_snapshot"])
    writer.writerow(list(DIVERGENCE_CONTEXT_SNAPSHOT_COLUMNS))
    for row in divergence_snapshot.get("divergence_context_snapshot_rows", []):
        writer.writerow([_format_csv_value(value) for value in _format_divergence_row_values(row, DIVERGENCE_CONTEXT_SNAPSHOT_COLUMNS)])
    writer.writerow([])
    writer.writerow(["section", "divergence_signals_60td"])
    writer.writerow(list(DIVERGENCE_SIGNALS_COLUMNS))
    for row in divergence_snapshot.get("divergence_signal_rows_60td", []):
        writer.writerow([_format_csv_value(value) for value in _format_divergence_row_values(row, DIVERGENCE_SIGNALS_COLUMNS)])


def _format_moving_average_snapshot_lines(moving_average_snapshot: dict[str, list[dict[str, Any]]]) -> list[str]:
    lines = ["", "section;moving_averages_60td", ";".join(MOVING_AVERAGE_COLUMNS)]
    for row in moving_average_snapshot.get("moving_average_rows_60td", []):
        lines.append(";".join(_format_moving_average_row_values(row, MOVING_AVERAGE_COLUMNS)))
    return lines


def _write_moving_average_snapshot_csv(
    writer: csv.writer,
    moving_average_snapshot: dict[str, list[dict[str, Any]]],
) -> None:
    writer.writerow([])
    writer.writerow(["section", "moving_averages_60td"])
    writer.writerow(list(MOVING_AVERAGE_COLUMNS))
    for row in moving_average_snapshot.get("moving_average_rows_60td", []):
        writer.writerow([_format_csv_value(value) for value in _format_moving_average_row_values(row, MOVING_AVERAGE_COLUMNS)])


def _resolve_dow_market(args: argparse.Namespace, ticker: str) -> str | None:
    dow_market = getattr(args, "dow_market", None)
    if dow_market is not None:
        return dow_market
    if getattr(args, "price_behavior_snapshot", False):
        return _resolve_market_for_ticker(ticker)
    return None


def _fetch_latest_valid_close_date_for_dow(
    ohlcv_db_path: Path,
    ticker: str,
    market: str | None,
) -> str | None:
    with sqlite3.connect(str(ohlcv_db_path)) as conn:
        query = """
        SELECT MAX(pvm)
        FROM osakedata
        WHERE osake = ?
          AND close IS NOT NULL
        """
        params: list[Any] = [ticker]
        if market is not None:
            query += " AND market = ?"
            params.append(market)
        row = conn.execute(query, params).fetchone()
    return None if row is None else row[0]


def _fetch_latest_valid_close_date_for_candlestick(
    ohlcv_db_path: Path,
    ticker: str,
    market: str | None,
) -> str | None:
    return _fetch_latest_valid_close_date_for_dow(ohlcv_db_path, ticker, market)


def _fetch_latest_valid_close_date_for_divergence(
    ohlcv_db_path: Path,
    ticker: str,
    market: str | None,
) -> str | None:
    return _fetch_latest_valid_close_date_for_dow(ohlcv_db_path, ticker, market)


def _fetch_latest_valid_close_date_for_moving_average(
    ohlcv_db_path: Path,
    ticker: str,
    market: str | None,
) -> str | None:
    return _fetch_latest_valid_close_date_for_dow(ohlcv_db_path, ticker, market)


def _derive_dow_as_of_date(
    args: argparse.Namespace,
    ticker: str,
    ohlcv_db_path: Path,
    price_behavior_snapshot: dict[str, str] | None,
    market: str | None,
) -> str:
    dow_as_of_date = getattr(args, "dow_as_of_date", None)
    if dow_as_of_date:
        return dow_as_of_date
    if price_behavior_snapshot is not None:
        price_behavior_as_of_date = price_behavior_snapshot.get("price_behavior_as_of_date", "")
        if price_behavior_as_of_date:
            return price_behavior_as_of_date
    latest_valid_close_date = _fetch_latest_valid_close_date_for_dow(ohlcv_db_path, ticker, market)
    if latest_valid_close_date is None:
        raise RuntimeError(f"DOW_AS_OF_DATE_NOT_FOUND:{ticker}")
    return latest_valid_close_date


def _load_dow_structure_snapshot(
    args: argparse.Namespace,
    ticker: str,
    ohlcv_db_path: Path,
    price_behavior_snapshot: dict[str, str] | None,
) -> dict[str, list[dict[str, Any]]]:
    market = _resolve_dow_market(args, ticker)
    as_of_date = _derive_dow_as_of_date(args, ticker, ohlcv_db_path, price_behavior_snapshot, market)
    return read_stock_dow_structure_raw_export(
        analysis_db_path=str(resolve_db_path(args.dow_analysis_db)),
        osakedata_db_path=str(ohlcv_db_path),
        ticker=ticker,
        as_of_date=as_of_date,
        market=market,
        pivot_radius=getattr(args, "dow_pivot_radius", 3),
        price_source=getattr(args, "dow_price_source", "close"),
        recent_window_trading_days=getattr(args, "dow_recent_window_trading_days", 60),
    ).to_dict()


def _resolve_candlestick_market(args: argparse.Namespace, ticker: str) -> str | None:
    candlestick_market = getattr(args, "candlestick_market", None)
    if candlestick_market is not None:
        return candlestick_market
    if getattr(args, "price_behavior_snapshot", False):
        return _resolve_market_for_ticker(ticker)
    return None


def _derive_candlestick_as_of_date(
    args: argparse.Namespace,
    ticker: str,
    ohlcv_db_path: Path,
    price_behavior_snapshot: dict[str, str] | None,
    market: str | None,
) -> str:
    candlestick_as_of_date = getattr(args, "candlestick_as_of_date", None)
    if candlestick_as_of_date:
        return candlestick_as_of_date
    if price_behavior_snapshot is not None:
        price_behavior_as_of_date = price_behavior_snapshot.get("price_behavior_as_of_date", "")
        if price_behavior_as_of_date:
            return price_behavior_as_of_date
    latest_valid_close_date = _fetch_latest_valid_close_date_for_candlestick(ohlcv_db_path, ticker, market)
    if latest_valid_close_date is None:
        raise RuntimeError(f"CANDLESTICK_AS_OF_DATE_NOT_FOUND:{ticker}")
    return latest_valid_close_date


def _load_candlestick_snapshot(
    args: argparse.Namespace,
    ticker: str,
    ohlcv_db_path: Path,
    price_behavior_snapshot: dict[str, str] | None,
) -> dict[str, list[dict[str, Any]]]:
    market = _resolve_candlestick_market(args, ticker)
    as_of_date = _derive_candlestick_as_of_date(args, ticker, ohlcv_db_path, price_behavior_snapshot, market)
    return read_candlestick_signal_raw_export(
        analysis_db_path=str(resolve_db_path(args.candlestick_analysis_db)),
        osakedata_db_path=str(ohlcv_db_path),
        ticker=ticker,
        as_of_date=as_of_date,
        market=market,
        recent_window_trading_days=getattr(args, "candlestick_recent_window_trading_days", 60),
    ).to_dict()


def _resolve_divergence_market(args: argparse.Namespace, ticker: str) -> str | None:
    divergence_market = getattr(args, "divergence_market", None)
    if divergence_market is not None:
        return divergence_market
    if getattr(args, "price_behavior_snapshot", False):
        return _resolve_market_for_ticker(ticker)
    return None


def _derive_divergence_as_of_date(
    args: argparse.Namespace,
    ticker: str,
    ohlcv_db_path: Path,
    price_behavior_snapshot: dict[str, str] | None,
    market: str | None,
) -> str:
    divergence_as_of_date = getattr(args, "divergence_as_of_date", None)
    if divergence_as_of_date:
        return divergence_as_of_date
    if price_behavior_snapshot is not None:
        price_behavior_as_of_date = price_behavior_snapshot.get("price_behavior_as_of_date", "")
        if price_behavior_as_of_date:
            return price_behavior_as_of_date
    latest_valid_close_date = _fetch_latest_valid_close_date_for_divergence(ohlcv_db_path, ticker, market)
    if latest_valid_close_date is None:
        raise RuntimeError(f"DIVERGENCE_AS_OF_DATE_NOT_FOUND:{ticker}")
    return latest_valid_close_date


def _load_divergence_snapshot(
    args: argparse.Namespace,
    ticker: str,
    ohlcv_db_path: Path,
    price_behavior_snapshot: dict[str, str] | None,
) -> dict[str, list[dict[str, Any]]]:
    market = _resolve_divergence_market(args, ticker)
    as_of_date = _derive_divergence_as_of_date(args, ticker, ohlcv_db_path, price_behavior_snapshot, market)
    return read_divergence_signal_raw_export(
        analysis_db_path=str(resolve_db_path(args.divergence_analysis_db)),
        osakedata_db_path=str(ohlcv_db_path),
        ticker=ticker,
        as_of_date=as_of_date,
        market=market,
        recent_window_trading_days=getattr(args, "divergence_recent_window_trading_days", 60),
    ).to_dict()


def _resolve_moving_average_market(args: argparse.Namespace, ticker: str) -> str | None:
    moving_average_market = getattr(args, "moving_average_market", None)
    if moving_average_market is not None:
        return moving_average_market
    if getattr(args, "price_behavior_snapshot", False):
        return _resolve_market_for_ticker(ticker)
    return None


def _derive_moving_average_as_of_date(
    args: argparse.Namespace,
    ticker: str,
    ohlcv_db_path: Path,
    price_behavior_snapshot: dict[str, str] | None,
    market: str | None,
) -> str:
    moving_average_as_of_date = getattr(args, "moving_average_as_of_date", None)
    if moving_average_as_of_date:
        return moving_average_as_of_date
    if price_behavior_snapshot is not None:
        price_behavior_as_of_date = price_behavior_snapshot.get("price_behavior_as_of_date", "")
        if price_behavior_as_of_date:
            return price_behavior_as_of_date
    latest_valid_close_date = _fetch_latest_valid_close_date_for_moving_average(ohlcv_db_path, ticker, market)
    if latest_valid_close_date is None:
        raise RuntimeError(f"MOVING_AVERAGE_AS_OF_DATE_NOT_FOUND:{ticker}")
    return latest_valid_close_date


def _load_moving_average_snapshot(
    args: argparse.Namespace,
    ticker: str,
    ohlcv_db_path: Path,
    price_behavior_snapshot: dict[str, str] | None,
) -> dict[str, list[dict[str, Any]]]:
    market = _resolve_moving_average_market(args, ticker)
    as_of_date = _derive_moving_average_as_of_date(args, ticker, ohlcv_db_path, price_behavior_snapshot, market)
    return read_moving_average_raw_export(
        osakedata_db_path=str(ohlcv_db_path),
        ticker=ticker,
        as_of_date=as_of_date,
        market=market,
        recent_window_trading_days=getattr(args, "moving_average_recent_window_trading_days", 60),
        ma_short_window=getattr(args, "moving_average_short_window", 50),
        ma_long_window=getattr(args, "moving_average_long_window", 200),
        benchmark_ticker=getattr(args, "moving_average_benchmark_ticker", "^GSPC"),
        benchmark_market=getattr(args, "moving_average_benchmark_market", "usa"),
    ).to_dict()


def _build_ticker_snapshot_output(
    args: argparse.Namespace,
    ticker: str,
) -> str:
    db_path = resolve_db_path(args.db)
    if args.price_behavior_snapshot and not args.ohlcv_db:
        raise RuntimeError("PRICE_BEHAVIOR_SNAPSHOT_REQUIRES_OHLCV_DB")
    dow_structure_snapshot_enabled = getattr(args, "dow_structure_snapshot", False)
    candlestick_snapshot_enabled = getattr(args, "candlestick_snapshot", False)
    divergence_snapshot_enabled = getattr(args, "divergence_snapshot", False)
    moving_average_snapshot_enabled = getattr(args, "moving_average_snapshot", False)
    with sqlite3.connect(str(db_path)) as conn:
        matrix_rows = build_snapshot_matrix(
            conn=conn,
            ticker=ticker,
            quarters=args.quarters,
            rule_id=args.rule_id,
            percentile_target_date=args.percentile_target_date,
        )
        valuation_snapshot = load_latest_valuation_snapshot(conn, ticker)
    price_behavior_snapshot: dict[str, str] | None = None
    ohlcv_db_path: Path | None = None
    if args.price_behavior_snapshot:
        ohlcv_db_path = resolve_db_path(args.ohlcv_db)
        latest_quarter_date = matrix_rows[-1]["quarter"]
        price_behavior_snapshot = load_price_behavior_snapshot(ohlcv_db_path, ticker, latest_quarter_date)
    elif dow_structure_snapshot_enabled or candlestick_snapshot_enabled or divergence_snapshot_enabled or moving_average_snapshot_enabled:
        ohlcv_db_path = resolve_db_path(args.ohlcv_db)
    dow_structure_snapshot: dict[str, list[dict[str, Any]]] | None = None
    if dow_structure_snapshot_enabled:
        if ohlcv_db_path is None:
            ohlcv_db_path = resolve_db_path(args.ohlcv_db)
        dow_structure_snapshot = _load_dow_structure_snapshot(args, ticker, ohlcv_db_path, price_behavior_snapshot)
    candlestick_snapshot: dict[str, list[dict[str, Any]]] | None = None
    if candlestick_snapshot_enabled:
        if ohlcv_db_path is None:
            ohlcv_db_path = resolve_db_path(args.ohlcv_db)
        candlestick_snapshot = _load_candlestick_snapshot(args, ticker, ohlcv_db_path, price_behavior_snapshot)
    divergence_snapshot: dict[str, list[dict[str, Any]]] | None = None
    if divergence_snapshot_enabled:
        if ohlcv_db_path is None:
            ohlcv_db_path = resolve_db_path(args.ohlcv_db)
        divergence_snapshot = _load_divergence_snapshot(args, ticker, ohlcv_db_path, price_behavior_snapshot)
    moving_average_snapshot: dict[str, list[dict[str, Any]]] | None = None
    if moving_average_snapshot_enabled:
        if ohlcv_db_path is None:
            ohlcv_db_path = resolve_db_path(args.ohlcv_db)
        moving_average_snapshot = _load_moving_average_snapshot(args, ticker, ohlcv_db_path, price_behavior_snapshot)
    return format_snapshot_matrix(
        matrix_rows,
        price_behavior_snapshot,
        valuation_snapshot,
        dow_structure_snapshot,
        candlestick_snapshot,
        divergence_snapshot,
        moving_average_snapshot,
    )


def main() -> None:
    args = parse_args()
    ticker_list = _parse_ticker_args(args.ticker)
    output_dir_arg = getattr(args, "output_dir", None)
    if len(ticker_list) > 1 and not output_dir_arg:
        raise SystemExit("--output-dir is required when --ticker contains multiple tickers")
    try:
        if output_dir_arg:
            output_dir = _resolve_output_dir(output_dir_arg)
            output_date = resolve_output_date()
            for ticker in ticker_list:
                try:
                    snapshot_output = _build_ticker_snapshot_output(args, ticker)
                except RuntimeError as exc:
                    print(f"ERROR ticker={ticker} message={exc}", file=sys.stderr)
                    continue
                _write_snapshot_output_file(output_dir, ticker, output_date, snapshot_output)
            return
        print(_build_ticker_snapshot_output(args, ticker_list[0]))
    except RuntimeError as exc:
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
