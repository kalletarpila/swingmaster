from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import sqlite3
from typing import Any


@dataclass
class MovingAverageRawExport:
    moving_average_rows_60td: list[dict[str, Any]]

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        return asdict(self)


def read_moving_average_raw_export(
    osakedata_db_path: str,
    ticker: str,
    as_of_date: str,
    market: str | None = None,
    recent_window_trading_days: int = 60,
    ma_short_window: int = 50,
    ma_long_window: int = 200,
    benchmark_ticker: str = "^GSPC",
    benchmark_market: str | None = "usa",
) -> MovingAverageRawExport:
    db_path = Path(osakedata_db_path).resolve()
    stock_limit = recent_window_trading_days + ma_long_window - 1

    with _connect_readonly(db_path) as conn:
        stock_rows_desc = _fetch_price_rows_desc(
            conn,
            ticker=ticker,
            as_of_date=as_of_date,
            market=market,
            limit=stock_limit,
        )
        if not stock_rows_desc:
            return MovingAverageRawExport(moving_average_rows_60td=[])

        stock_rows_asc = list(reversed(stock_rows_desc))
        output_rows = stock_rows_asc[-recent_window_trading_days:]
        window_meta = _derive_window_metadata(output_rows, recent_window_trading_days)
        benchmark_rows = _fetch_price_rows_asc(
            conn,
            ticker=benchmark_ticker,
            as_of_date=as_of_date,
            market=benchmark_market,
        )

    stock_ma_values = _compute_ma_values(stock_rows_asc, ma_short_window, ma_long_window)
    benchmark_ma_values = _compute_ma_values(benchmark_rows, ma_short_window, ma_long_window)
    benchmark_lookup = _build_benchmark_lookup(benchmark_rows, benchmark_ma_values)
    export_rows = _build_export_rows(
        output_rows=output_rows,
        stock_ma_values=stock_ma_values,
        benchmark_lookup=benchmark_lookup,
        ticker=ticker,
        market=market,
        as_of_date=as_of_date,
        window_meta=window_meta,
        benchmark_ticker=benchmark_ticker,
    )
    return MovingAverageRawExport(moving_average_rows_60td=export_rows)


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_price_rows_desc(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str | None,
    limit: int,
) -> list[sqlite3.Row]:
    query = """
    SELECT pvm, close, volume
    FROM osakedata
    WHERE osake = ?
      AND close IS NOT NULL
      AND pvm <= ?
    """
    params: list[Any] = [ticker, as_of_date]
    if market is not None:
        query += " AND market = ?"
        params.append(market)
    query += " ORDER BY pvm DESC LIMIT ?"
    params.append(limit)
    return conn.execute(query, params).fetchall()


def _fetch_price_rows_asc(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    as_of_date: str,
    market: str | None,
) -> list[sqlite3.Row]:
    query = """
    SELECT pvm, close, volume
    FROM osakedata
    WHERE osake = ?
      AND close IS NOT NULL
      AND pvm <= ?
    """
    params: list[Any] = [ticker, as_of_date]
    if market is not None:
        query += " AND market = ?"
        params.append(market)
    query += " ORDER BY pvm ASC"
    return conn.execute(query, params).fetchall()


def _derive_window_metadata(rows: list[sqlite3.Row], recent_window_trading_days: int) -> dict[str, Any]:
    if not rows:
        return {
            "sequence_window_trading_days": recent_window_trading_days,
            "sequence_available_trading_days": 0,
            "sequence_window_start_date": None,
            "sequence_window_end_date": None,
        }
    return {
        "sequence_window_trading_days": recent_window_trading_days,
        "sequence_available_trading_days": len(rows),
        "sequence_window_start_date": rows[0]["pvm"],
        "sequence_window_end_date": rows[-1]["pvm"],
    }


def _compute_ma_values(
    rows: list[sqlite3.Row],
    short_window: int,
    long_window: int,
) -> dict[str, dict[str, float | None]]:
    closes = [float(row["close"]) for row in rows]
    prefix_sums = [0.0]
    for close in closes:
        prefix_sums.append(prefix_sums[-1] + close)

    values: dict[str, dict[str, float | None]] = {}
    for index, row in enumerate(rows):
        values[row["pvm"]] = {
            "ma_short": _window_average(prefix_sums, index, short_window),
            "ma_long": _window_average(prefix_sums, index, long_window),
        }
    return values


def _window_average(prefix_sums: list[float], index: int, window: int) -> float | None:
    if index + 1 < window:
        return None
    end = index + 1
    start = end - window
    return (prefix_sums[end] - prefix_sums[start]) / window


def _build_benchmark_lookup(
    benchmark_rows: list[sqlite3.Row],
    benchmark_ma_values: dict[str, dict[str, float | None]],
) -> dict[str, dict[str, Any]]:
    return {
        row["pvm"]: {
            "benchmark_trade_date": row["pvm"],
            "benchmark_close": row["close"],
            "benchmark_ma50": benchmark_ma_values[row["pvm"]]["ma_short"],
            "benchmark_ma200": benchmark_ma_values[row["pvm"]]["ma_long"],
        }
        for row in benchmark_rows
    }


def _build_export_rows(
    *,
    output_rows: list[sqlite3.Row],
    stock_ma_values: dict[str, dict[str, float | None]],
    benchmark_lookup: dict[str, dict[str, Any]],
    ticker: str,
    market: str | None,
    as_of_date: str,
    window_meta: dict[str, Any],
    benchmark_ticker: str,
) -> list[dict[str, Any]]:
    benchmark_dates = sorted(benchmark_lookup)
    benchmark_index = 0
    latest_benchmark_date: str | None = None
    rows: list[dict[str, Any]] = []

    for sequence_index, row in enumerate(output_rows, start=1):
        trade_date = row["pvm"]
        while benchmark_index < len(benchmark_dates) and benchmark_dates[benchmark_index] <= trade_date:
            latest_benchmark_date = benchmark_dates[benchmark_index]
            benchmark_index += 1

        benchmark_row = benchmark_lookup.get(latest_benchmark_date) if latest_benchmark_date is not None else None
        rows.append(
            {
                "ticker": ticker,
                "market": market,
                "as_of_date": as_of_date,
                "sequence_window_trading_days": window_meta["sequence_window_trading_days"],
                "sequence_available_trading_days": window_meta["sequence_available_trading_days"],
                "sequence_window_start_date": window_meta["sequence_window_start_date"],
                "sequence_window_end_date": window_meta["sequence_window_end_date"],
                "sequence_index": sequence_index,
                "trade_date": trade_date,
                "stock_close": row["close"],
                "stock_volume": row["volume"],
                "stock_ma50": stock_ma_values[trade_date]["ma_short"],
                "stock_ma200": stock_ma_values[trade_date]["ma_long"],
                "benchmark_ticker": benchmark_ticker,
                "benchmark_trade_date": None if benchmark_row is None else benchmark_row["benchmark_trade_date"],
                "benchmark_close": None if benchmark_row is None else benchmark_row["benchmark_close"],
                "benchmark_ma50": None if benchmark_row is None else benchmark_row["benchmark_ma50"],
                "benchmark_ma200": None if benchmark_row is None else benchmark_row["benchmark_ma200"],
            }
        )
    return rows
