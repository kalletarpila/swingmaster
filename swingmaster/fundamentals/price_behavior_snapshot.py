from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


MARKET = "usa"
BENCHMARK_TICKER = "^GSPC"


@dataclass(frozen=True)
class OhlcvRow:
    pvm: str
    high: float | None
    close: float | None
    volume: float | None


def load_price_behavior_snapshot(
    ohlcv_db_path: Path,
    ticker: str,
    latest_quarter_date: str,
) -> dict[str, str]:
    if not ohlcv_db_path.exists():
        raise RuntimeError(f"OHLCV_DB_NOT_FOUND:{ohlcv_db_path}")

    with sqlite3.connect(str(ohlcv_db_path)) as conn:
        ticker_rows = _load_ohlcv_rows(conn, ticker.upper())
        benchmark_rows = _load_ohlcv_rows(conn, BENCHMARK_TICKER)

    snapshot = {
        "price_behavior_as_of_date": "",
        "price_return_3m_pct": "",
        "price_return_6m_pct": "",
        "price_return_12m_pct": "",
        "distance_from_52w_high_pct": "",
        "relative_strength_6m_vs_sp500_pct": "",
        "price_return_since_last_report_pct": "",
        "relative_return_vs_sp500_since_last_report_pct": "",
        "earnings_reaction_1d_pct": "",
        "earnings_reaction_3d_pct": "",
        "post_earnings_drift_20d_pct": "",
        "volume_ratio_since_last_report_vs_3m_avg": "",
    }

    if not ticker_rows:
        return snapshot

    current_anchor_index = len(ticker_rows) - 1
    current_anchor = ticker_rows[current_anchor_index]
    snapshot["price_behavior_as_of_date"] = current_anchor.pvm

    return_3m = _trading_day_return_pct(ticker_rows, current_anchor_index, 63)
    return_6m = _trading_day_return_pct(ticker_rows, current_anchor_index, 126)
    return_12m = _trading_day_return_pct(ticker_rows, current_anchor_index, 252)
    distance_52w = _distance_from_rolling_high_pct(ticker_rows, current_anchor_index, 252)

    report_anchor_index = _latest_index_on_or_before(ticker_rows, latest_quarter_date)
    since_last_report = _point_to_point_return_pct(ticker_rows, report_anchor_index, current_anchor_index)
    earnings_reaction_1d = _point_to_point_return_pct(ticker_rows, report_anchor_index, _offset_index(report_anchor_index, 1, len(ticker_rows)))
    earnings_reaction_3d = _point_to_point_return_pct(ticker_rows, report_anchor_index, _offset_index(report_anchor_index, 3, len(ticker_rows)))
    # Descriptive/reporting only; do not use as a production signal without as-of constraints.
    post_earnings_drift_20d = _point_to_point_return_pct(ticker_rows, report_anchor_index, _offset_index(report_anchor_index, 20, len(ticker_rows)))
    volume_ratio = _volume_ratio_since_last_report_vs_3m_avg(ticker_rows, report_anchor_index, current_anchor_index)

    benchmark_current_anchor_index = _latest_index_on_or_before(benchmark_rows, current_anchor.pvm)
    benchmark_report_anchor_index = _latest_index_on_or_before(benchmark_rows, latest_quarter_date)
    benchmark_return_6m = _trading_day_return_pct(benchmark_rows, benchmark_current_anchor_index, 126)
    benchmark_since_report = _point_to_point_return_pct(benchmark_rows, benchmark_report_anchor_index, benchmark_current_anchor_index)

    snapshot["price_return_3m_pct"] = _format_optional_float(return_3m)
    snapshot["price_return_6m_pct"] = _format_optional_float(return_6m)
    snapshot["price_return_12m_pct"] = _format_optional_float(return_12m)
    snapshot["distance_from_52w_high_pct"] = _format_optional_float(distance_52w)
    snapshot["relative_strength_6m_vs_sp500_pct"] = _format_optional_float(_difference(return_6m, benchmark_return_6m))
    snapshot["price_return_since_last_report_pct"] = _format_optional_float(since_last_report)
    snapshot["relative_return_vs_sp500_since_last_report_pct"] = _format_optional_float(_difference(since_last_report, benchmark_since_report))
    snapshot["earnings_reaction_1d_pct"] = _format_optional_float(earnings_reaction_1d)
    snapshot["earnings_reaction_3d_pct"] = _format_optional_float(earnings_reaction_3d)
    snapshot["post_earnings_drift_20d_pct"] = _format_optional_float(post_earnings_drift_20d)
    snapshot["volume_ratio_since_last_report_vs_3m_avg"] = _format_optional_float(volume_ratio)
    return snapshot


def _load_ohlcv_rows(conn: sqlite3.Connection, ticker: str) -> list[OhlcvRow]:
    rows = conn.execute(
        """
        SELECT pvm, high, close, volume
        FROM osakedata
        WHERE osake = ?
          AND market = ?
        ORDER BY pvm ASC
        """,
        (ticker.upper(), MARKET),
    ).fetchall()
    return [OhlcvRow(str(row[0]), _to_float(row[1]), _to_float(row[2]), _to_float(row[3])) for row in rows]


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _format_optional_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def _latest_index_on_or_before(rows: list[OhlcvRow], date_text: str) -> int | None:
    latest_index: int | None = None
    for index, row in enumerate(rows):
        if row.pvm <= date_text:
            latest_index = index
        else:
            break
    return latest_index


def _offset_index(base_index: int | None, offset: int, length: int) -> int | None:
    if base_index is None:
        return None
    index = base_index + offset
    if index < 0 or index >= length:
        return None
    return index


def _trading_day_return_pct(rows: list[OhlcvRow], anchor_index: int | None, trading_day_offset: int) -> float | None:
    lookback_index = _offset_index(anchor_index, -trading_day_offset, len(rows))
    return _point_to_point_return_pct(rows, lookback_index, anchor_index)


def _point_to_point_return_pct(rows: list[OhlcvRow], start_index: int | None, end_index: int | None) -> float | None:
    if start_index is None or end_index is None:
        return None
    start_close = rows[start_index].close
    end_close = rows[end_index].close
    if start_close is None or end_close is None or start_close == 0:
        return None
    return 100.0 * (end_close / start_close - 1.0)


def _distance_from_rolling_high_pct(rows: list[OhlcvRow], anchor_index: int | None, window_size: int) -> float | None:
    if anchor_index is None or anchor_index + 1 < window_size:
        return None
    window_rows = rows[anchor_index - window_size + 1 : anchor_index + 1]
    anchor_close = rows[anchor_index].close
    highs = [row.high for row in window_rows if row.high is not None]
    if anchor_close is None or not highs:
        return None
    max_high = max(highs)
    if max_high == 0:
        return None
    return 100.0 * (anchor_close / max_high - 1.0)


def _difference(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _volume_ratio_since_last_report_vs_3m_avg(
    rows: list[OhlcvRow],
    report_anchor_index: int | None,
    current_anchor_index: int | None,
) -> float | None:
    if report_anchor_index is None or current_anchor_index is None or current_anchor_index <= report_anchor_index:
        return None
    if report_anchor_index + 1 < 63:
        return None
    numerator_rows = rows[report_anchor_index + 1 : current_anchor_index + 1]
    denominator_rows = rows[report_anchor_index - 62 : report_anchor_index + 1]
    numerator_volumes = [row.volume for row in numerator_rows if row.volume is not None]
    denominator_volumes = [row.volume for row in denominator_rows if row.volume is not None]
    if not numerator_volumes or len(denominator_volumes) != 63:
        return None
    denominator_average = sum(denominator_volumes) / len(denominator_volumes)
    if denominator_average == 0:
        return None
    numerator_average = sum(numerator_volumes) / len(numerator_volumes)
    return numerator_average / denominator_average
