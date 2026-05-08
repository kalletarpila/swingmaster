from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.moving_average_reader import read_moving_average_raw_export


def test_recent_60_trading_day_output_window_uses_latest_valid_stock_close_rows(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    dates = _date_range("2026-01-01", 70)
    for index, trade_date in enumerate(dates, start=1):
        _insert_ohlcv_row(db_path, "AAA", trade_date, float(index), 1000 + index, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", dates[-1], market="usa")

    assert len(result.moving_average_rows_60td) == 60
    assert result.moving_average_rows_60td[0]["trade_date"] == dates[-60]
    assert result.moving_average_rows_60td[-1]["trade_date"] == dates[-1]
    assert result.moving_average_rows_60td[0]["sequence_available_trading_days"] == 60


def test_short_available_stock_trading_history_outputs_all_rows(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    for index, trade_date in enumerate(("2026-04-28", "2026-04-29", "2026-04-30"), start=1):
        _insert_ohlcv_row(db_path, "AAA", trade_date, float(index), 1000, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")

    assert [row["trade_date"] for row in result.moving_average_rows_60td] == ["2026-04-28", "2026-04-29", "2026-04-30"]
    assert result.moving_average_rows_60td[0]["sequence_available_trading_days"] == 3


def test_no_valid_stock_close_data_returns_empty_rows(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", None, 1000, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")

    assert result.moving_average_rows_60td == []


def test_null_stock_close_rows_are_ignored(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-28", 10.0, 1000, "usa")
    _insert_ohlcv_row(db_path, "AAA", "2026-04-29", None, 1000, "usa")
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 20.0, 1000, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")

    assert [row["trade_date"] for row in result.moving_average_rows_60td] == ["2026-04-28", "2026-04-30"]


def test_stock_ma50_calculation_and_insufficient_history(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    dates = _date_range("2026-01-01", 50)
    for index, trade_date in enumerate(dates, start=1):
        _insert_ohlcv_row(db_path, "AAA", trade_date, float(index), 1000, "usa")

    result = read_moving_average_raw_export(
        str(db_path),
        "AAA",
        dates[-1],
        market="usa",
        recent_window_trading_days=50,
        ma_short_window=50,
        ma_long_window=200,
    )

    assert result.moving_average_rows_60td[0]["stock_ma50"] is None
    assert result.moving_average_rows_60td[-1]["stock_ma50"] == 25.5


def test_stock_ma200_calculation_and_insufficient_history(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    dates = _date_range("2026-01-01", 200)
    for index, trade_date in enumerate(dates, start=1):
        _insert_ohlcv_row(db_path, "AAA", trade_date, float(index), 1000, "usa")

    result = read_moving_average_raw_export(
        str(db_path),
        "AAA",
        dates[-1],
        market="usa",
        recent_window_trading_days=60,
        ma_short_window=50,
        ma_long_window=200,
    )

    assert result.moving_average_rows_60td[0]["stock_ma200"] is None
    assert result.moving_average_rows_60td[-1]["stock_ma200"] == 100.5


def test_no_lookahead_for_stock_excludes_future_rows(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    for index, trade_date in enumerate(("2026-04-28", "2026-04-29", "2026-04-30", "2026-05-01"), start=1):
        _insert_ohlcv_row(db_path, "AAA", trade_date, float(index), 1000, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")

    assert [row["trade_date"] for row in result.moving_average_rows_60td] == ["2026-04-28", "2026-04-29", "2026-04-30"]


def test_stock_volume_is_returned_and_not_used_in_ma(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    dates = _date_range("2026-01-01", 50)
    for index, trade_date in enumerate(dates, start=1):
        _insert_ohlcv_row(db_path, "AAA", trade_date, 10.0, 1000 * index, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", dates[-1], market="usa", recent_window_trading_days=50)

    assert result.moving_average_rows_60td[-1]["stock_volume"] == 50000
    assert result.moving_average_rows_60td[-1]["stock_ma50"] == 10.0


def test_benchmark_close_on_same_date_is_used(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 10.0, 1000, "usa")
    _insert_ohlcv_row(db_path, "^GSPC", "2026-04-30", 5000.0, 0, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")
    row = result.moving_average_rows_60td[0]

    assert row["benchmark_trade_date"] == "2026-04-30"
    assert row["benchmark_close"] == 5000.0


def test_benchmark_falls_back_to_previous_valid_date(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 10.0, 1000, "usa")
    _insert_ohlcv_row(db_path, "^GSPC", "2026-04-29", 4900.0, 0, "usa")
    _insert_ohlcv_row(db_path, "^GSPC", "2026-05-01", 5100.0, 0, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")
    row = result.moving_average_rows_60td[0]

    assert row["benchmark_trade_date"] == "2026-04-29"
    assert row["benchmark_close"] == 4900.0


def test_benchmark_ma_calculations_and_missing_history(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    stock_dates = _date_range("2026-01-01", 200)
    for trade_date in stock_dates:
        _insert_ohlcv_row(db_path, "AAA", trade_date, 100.0, 1000, "usa")
    for index, trade_date in enumerate(stock_dates, start=1):
        _insert_ohlcv_row(db_path, "^GSPC", trade_date, float(index), 0, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", stock_dates[-1], market="usa")
    first_row = result.moving_average_rows_60td[0]
    last_row = result.moving_average_rows_60td[-1]

    assert first_row["benchmark_ma50"] == 116.5
    assert first_row["benchmark_ma200"] is None
    assert last_row["benchmark_ma50"] == 175.5
    assert last_row["benchmark_ma200"] == 100.5


def test_no_lookahead_for_benchmark_excludes_future_rows(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 10.0, 1000, "usa")
    _insert_ohlcv_row(db_path, "^GSPC", "2026-05-01", 5100.0, 0, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")
    row = result.moving_average_rows_60td[0]

    assert row["benchmark_trade_date"] is None
    assert row["benchmark_close"] is None
    assert row["benchmark_ma50"] is None
    assert row["benchmark_ma200"] is None


def test_missing_benchmark_data_keeps_stock_fields(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 10.0, 1234, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")
    row = result.moving_average_rows_60td[0]

    assert row["stock_close"] == 10.0
    assert row["stock_volume"] == 1234
    assert row["benchmark_trade_date"] is None
    assert row["benchmark_close"] is None
    assert row["benchmark_ma50"] is None
    assert row["benchmark_ma200"] is None


def test_market_filtering_for_stock_and_benchmark(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 10.0, 1000, "usa")
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 20.0, 1000, "omxh")
    _insert_ohlcv_row(db_path, "^GSPC", "2026-04-30", 5000.0, 0, "usa")
    _insert_ohlcv_row(db_path, "^GSPC", "2026-04-30", 6000.0, 0, "other")

    usa_result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa", benchmark_market="usa")
    none_result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market=None, benchmark_market="usa")

    assert usa_result.moving_average_rows_60td[0]["stock_close"] == 10.0
    assert usa_result.moving_average_rows_60td[0]["benchmark_close"] == 5000.0
    assert len(none_result.moving_average_rows_60td) == 2


def test_output_ordering_and_sequence_indexes(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    for index, trade_date in enumerate(("2026-04-28", "2026-04-29", "2026-04-30"), start=1):
        _insert_ohlcv_row(db_path, "AAA", trade_date, float(index), 1000, "usa")

    result = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")

    assert [row["trade_date"] for row in result.moving_average_rows_60td] == ["2026-04-28", "2026-04-29", "2026-04-30"]
    assert [row["sequence_index"] for row in result.moving_average_rows_60td] == [1, 2, 3]


def test_no_interpretation_fields_are_returned(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 10.0, 1000, "usa")

    export_dict = read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa").to_dict()

    assert "ma_trend" not in export_dict
    assert "ma_crossover" not in export_dict
    assert "golden_cross" not in export_dict
    assert "death_cross" not in export_dict
    assert "relative_strength" not in export_dict


def test_reader_does_not_write_to_database(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_ohlcv_row(db_path, "AAA", "2026-04-30", 10.0, 1000, "usa")
    before_rows = _count_rows(db_path, "osakedata")

    read_moving_average_raw_export(str(db_path), "AAA", "2026-04-30", market="usa")

    assert _count_rows(db_path, "osakedata") == before_rows


def _create_test_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "osakedata.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE osakedata (
                osake TEXT,
                pvm TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                market TEXT
            )
            """
        )
        conn.commit()
    return db_path


def _insert_ohlcv_row(
    db_path: Path,
    ticker: str,
    trade_date: str,
    close: float | None,
    volume: int,
    market: str,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
            VALUES (?, ?, 1.0, 2.0, 0.5, ?, ?, ?)
            """,
            (ticker, trade_date, close, volume, market),
        )
        conn.commit()


def _date_range(start_date: str, count: int) -> list[str]:
    start = date.fromisoformat(start_date)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _count_rows(db_path: Path, table_name: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return 0 if row is None else int(row[0])
