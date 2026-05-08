from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.stock_dow_structure_reader import read_stock_dow_structure_raw_export


def test_no_lookahead_for_latest_event(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-08", 10.0, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-08", last_status="OK")
    _insert_event(
        analysis_db,
        event_id=1,
        ticker="AAA",
        market="omxh",
        event_date="2026-01-08",
        confirmed_as_of_date="2026-01-10",
        event_type="PIVOT_HIGH",
        trend_state="UP",
    )

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-08", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["latest_event_found"] is False
    assert row["latest_event_id"] is None
    assert "NO_EVENT_FOUND" in row["dow_warning_flags"]


def test_no_lookahead_for_recent_event_sequence(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for day in range(1, 6):
        _insert_close(osakedata_db, "AAA", f"2026-01-0{day}", 10.0 + day, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-05", last_status="OK")
    _insert_event(analysis_db, 1, "AAA", "omxh", "2026-01-03", "2026-01-03", "PIVOT_LOW", trend_state="UP")
    _insert_event(analysis_db, 2, "AAA", "omxh", "2026-01-04", "2026-01-07", "BOS_UP", trend_state="UP")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-05", market="omxh")

    assert [row["event_id"] for row in result.recent_event_rows_60td] == [1]


def test_latest_confirmed_event_ordering_uses_confirmed_as_of_date_desc_then_id_desc(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-10", last_status="OK")
    _insert_event(analysis_db, 1, "AAA", "omxh", "2026-01-04", "2026-01-08", "PIVOT_LOW", trend_state="DOWN")
    _insert_event(analysis_db, 2, "AAA", "omxh", "2026-01-05", "2026-01-09", "BOS_UP", trend_state="UP")
    _insert_event(analysis_db, 3, "AAA", "omxh", "2026-01-06", "2026-01-09", "TREND_CHANGE", trend_state="NEUTRAL")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["latest_event_found"] is True
    assert row["latest_event_id"] == 3
    assert row["latest_event_type"] == "TREND_CHANGE"


def test_recent_event_sequence_ordering_uses_confirmed_as_of_date_asc_then_id_asc(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for day in range(1, 8):
        _insert_close(osakedata_db, "AAA", f"2026-01-0{day}", 10.0 + day, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-07", last_status="OK")
    _insert_event(analysis_db, 3, "AAA", "omxh", "2026-01-04", "2026-01-05", "TREND_CHANGE")
    _insert_event(analysis_db, 1, "AAA", "omxh", "2026-01-02", "2026-01-03", "PIVOT_LOW")
    _insert_event(analysis_db, 2, "AAA", "omxh", "2026-01-03", "2026-01-05", "BOS_UP")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-07", market="omxh")

    assert [row["event_id"] for row in result.recent_event_rows_60td] == [1, 2, 3]
    assert [row["sequence_index"] for row in result.recent_event_rows_60td] == [1, 2, 3]


def test_recent_60_trading_day_window_uses_oldest_of_latest_60_valid_close_dates(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    dates = _date_range("2026-01-01", 65)
    for idx, pvm in enumerate(dates, start=1):
        _insert_close(osakedata_db, "AAA", pvm, 100.0 + idx, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date=dates[-1], last_status="OK")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", dates[-1], market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["recent_event_available_trading_days"] == 60
    assert row["recent_event_window_start_date"] == dates[-60]
    assert row["recent_event_window_end_date"] == dates[-1]


def test_short_available_trading_history_is_supported(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for day in range(1, 6):
        _insert_close(osakedata_db, "AAA", f"2026-02-0{day}", 20.0 + day, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-02-05", last_status="OK")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-02-05", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["recent_event_available_trading_days"] == 5
    assert row["recent_event_window_start_date"] == "2026-02-01"
    assert row["recent_event_window_end_date"] == "2026-02-05"


def test_coverage_ok(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-10", last_status="OK")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")

    assert result.context_snapshot_rows[0]["coverage_status"] == "OK"


def test_coverage_stale_sets_warning_flag(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-09", last_status="OK")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["coverage_status"] == "STALE"
    assert "STALE_COVERAGE" in row["dow_warning_flags"]


def test_missing_status_sets_warning_flag(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["coverage_status"] == "MISSING_STATUS"
    assert "MISSING_STATUS" in row["dow_warning_flags"]


def test_no_valid_close_data_returns_empty_recent_rows(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", None, "omxh")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["coverage_status"] == "NO_VALID_CLOSE_DATA"
    assert "NO_VALID_CLOSE_DATA" in row["dow_warning_flags"]
    assert result.recent_event_rows_60td == []


def test_last_run_error_sets_error_coverage_and_warning_flag(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-10", last_status="ERROR")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["coverage_status"] == "ERROR"
    assert "LAST_RUN_ERROR" in row["dow_warning_flags"]


def test_null_close_latest_row_uses_earlier_valid_close_date(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-09", 10.0, "omxh")
    _insert_close(osakedata_db, "AAA", "2026-01-10", None, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-09", last_status="OK")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["latest_valid_close_date_on_or_before_as_of_date"] == "2026-01-09"
    assert row["recent_event_window_end_date"] == "2026-01-09"


def test_event_table_empty_but_status_ok_sets_no_event_warning(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-10", last_status="OK")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["coverage_status"] == "OK"
    assert row["latest_event_found"] is False
    assert result.recent_event_rows_60td == []
    assert "NO_EVENT_FOUND" in row["dow_warning_flags"]


def test_no_summary_counts_are_returned(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-10", last_status="OK")

    export_dict = read_stock_dow_structure_raw_export(
        str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh"
    ).to_dict()
    flat_keys = set(export_dict.keys()) | set(export_dict["context_snapshot_rows"][0].keys())

    assert "recent_event_summary" not in flat_keys
    assert "bos_up_count_60td" not in flat_keys
    assert "reset_count_60td" not in flat_keys


def test_reader_does_not_write_to_databases(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")
    _insert_status(analysis_db, "AAA", "omxh", calculated_through_date="2026-01-10", last_status="OK")
    _insert_event(analysis_db, 1, "AAA", "omxh", "2026-01-09", "2026-01-10", "PIVOT_LOW")

    before_analysis = _table_count(analysis_db, "stock_dow_structure_events")
    before_status = _table_count(analysis_db, "stock_dow_structure_status")
    before_osakedata = _table_count(osakedata_db, "osakedata")

    read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")

    assert _table_count(analysis_db, "stock_dow_structure_events") == before_analysis
    assert _table_count(analysis_db, "stock_dow_structure_status") == before_status
    assert _table_count(osakedata_db, "osakedata") == before_osakedata


def test_frozen_document_consistency_rules(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-01-07", "2026-01-08", "2026-01-09", "2026-01-10"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "omxh")
    _insert_close(osakedata_db, "AAA", "2026-01-11", None, "omxh")
    _insert_status(
        analysis_db,
        "AAA",
        "omxh",
        calculated_through_date="2026-01-09",
        last_status="OK",
        latest_event_date="2026-01-10",
        latest_event_confirmed_as_of_date="2026-01-10",
    )
    _insert_event(analysis_db, 1, "AAA", "omxh", "2026-01-07", "2026-01-08", "PIVOT_LOW")
    _insert_event(analysis_db, 3, "AAA", "omxh", "2026-01-09", "2026-01-09", "BOS_UP")
    _insert_event(analysis_db, 2, "AAA", "omxh", "2026-01-08", "2026-01-09", "TREND_CHANGE")
    _insert_event(analysis_db, 4, "AAA", "omxh", "2026-01-10", "2026-01-12", "RESET")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10", market="omxh")
    row = result.context_snapshot_rows[0]

    assert row["latest_event_id"] == 3
    assert row["coverage_status"] == "STALE"
    assert row["latest_valid_close_date_on_or_before_as_of_date"] == "2026-01-10"
    assert [event["event_id"] for event in result.recent_event_rows_60td] == [1, 2, 3]


def test_market_is_none_without_status_resolution_or_explicit_market(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "omxh")
    _insert_close(osakedata_db, "AAA", "2026-01-10", 10.0, "usa")

    result = read_stock_dow_structure_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-01-10")

    assert result.context_snapshot_rows[0]["market"] is None


def _create_test_dbs(tmp_path: Path) -> tuple[Path, Path]:
    analysis_db = tmp_path / "analysis.db"
    osakedata_db = tmp_path / "osakedata.db"
    with sqlite3.connect(analysis_db) as conn:
        conn.execute(
            """
            CREATE TABLE stock_dow_structure_status (
                ticker TEXT,
                market TEXT,
                price_source TEXT,
                pivot_radius INTEGER,
                calculated_from_date TEXT,
                calculated_through_date TEXT,
                latest_ohlcv_date_at_run TEXT,
                latest_event_date TEXT,
                latest_event_confirmed_as_of_date TEXT,
                last_run_id TEXT,
                last_run_mode TEXT,
                last_rows_deleted INTEGER,
                last_rows_inserted INTEGER,
                last_status TEXT,
                last_error_message TEXT,
                updated_at_utc TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_dow_structure_events (
                id INTEGER PRIMARY KEY,
                ticker TEXT,
                market TEXT,
                event_date TEXT,
                confirmed_as_of_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                price_source TEXT,
                structure_price REAL,
                pivot_radius INTEGER,
                event_type TEXT,
                dow_label_high TEXT,
                dow_label_low TEXT,
                trend_state TEXT,
                active_bos_high_date TEXT,
                active_bos_high_price REAL,
                active_bos_low_date TEXT,
                active_bos_low_price REAL,
                last_high_label TEXT,
                last_high_label_date TEXT,
                last_high_label_price REAL,
                last_low_label TEXT,
                last_low_label_date TEXT,
                last_low_label_price REAL,
                bos_up_count INTEGER,
                bos_down_count INTEGER,
                break_signal TEXT,
                break_level_date TEXT,
                break_level_price REAL,
                break_close_price REAL,
                reset_marker TEXT,
                reset_reason TEXT,
                structure_epoch_id INTEGER,
                structure_epoch_start_date TEXT,
                calc_version TEXT,
                run_id TEXT,
                created_at_utc TEXT
            )
            """
        )
        conn.commit()
    with sqlite3.connect(osakedata_db) as conn:
        conn.execute(
            """
            CREATE TABLE osakedata (
                osake TEXT,
                market TEXT,
                pvm TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER
            )
            """
        )
        conn.commit()
    return analysis_db, osakedata_db


def _insert_status(
    db_path: Path,
    ticker: str,
    market: str,
    *,
    calculated_through_date: str | None,
    last_status: str,
    latest_event_date: str | None = None,
    latest_event_confirmed_as_of_date: str | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_dow_structure_status (
                ticker, market, price_source, pivot_radius, calculated_from_date,
                calculated_through_date, latest_ohlcv_date_at_run, latest_event_date,
                latest_event_confirmed_as_of_date, last_run_id, last_run_mode,
                last_rows_deleted, last_rows_inserted, last_status, last_error_message,
                updated_at_utc
            ) VALUES (?, ?, 'close', 3, '2026-01-01', ?, ?, ?, ?, 'RUN1', 'incremental', 0, 0, ?, NULL, '2026-01-10T00:00:00Z')
            """,
            (
                ticker,
                market,
                calculated_through_date,
                calculated_through_date,
                latest_event_date,
                latest_event_confirmed_as_of_date,
                last_status,
            ),
        )
        conn.commit()


def _insert_event(
    db_path: Path,
    event_id: int,
    ticker: str,
    market: str,
    event_date: str,
    confirmed_as_of_date: str,
    event_type: str,
    *,
    trend_state: str | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO stock_dow_structure_events (
                id, ticker, market, event_date, confirmed_as_of_date, open, high, low, close, volume,
                price_source, structure_price, pivot_radius, event_type, dow_label_high, dow_label_low,
                trend_state, active_bos_high_date, active_bos_high_price, active_bos_low_date, active_bos_low_price,
                last_high_label, last_high_label_date, last_high_label_price, last_low_label, last_low_label_date,
                last_low_label_price, bos_up_count, bos_down_count, break_signal, break_level_date, break_level_price,
                break_close_price, reset_marker, reset_reason, structure_epoch_id, structure_epoch_start_date,
                calc_version, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, 1.0, 2.0, 0.5, 1.5, 1000, 'close', 1.5, 3, ?, 'HH', 'HL', ?, NULL, NULL, NULL, NULL, 'HH', ?, 2.0, 'HL', ?, 1.0, 1, 0, NULL, NULL, NULL, NULL, NULL, NULL, 1, '2026-01-01', 'stock_dow_v1', 'RUN1', '2026-01-10T00:00:00Z')
            """,
            (
                event_id,
                ticker,
                market,
                event_date,
                confirmed_as_of_date,
                event_type,
                trend_state,
                event_date,
                event_date,
            ),
        )
        conn.commit()


def _insert_close(db_path: Path, ticker: str, pvm: str, close: float | None, market: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO osakedata (osake, market, pvm, open, high, low, close, volume)
            VALUES (?, ?, ?, 1.0, 2.0, 0.5, ?, 1000)
            """,
            (ticker, market, pvm, close),
        )
        conn.commit()


def _table_count(db_path: Path, table_name: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0])


def _date_range(start_date: str, count: int) -> list[str]:
    start = date.fromisoformat(start_date)
    return [(start + timedelta(days=offset)).isoformat() for offset in range(count)]
