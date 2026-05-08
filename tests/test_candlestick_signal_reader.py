from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.candlestick_signal_reader import read_candlestick_signal_raw_export


def test_no_lookahead_excludes_future_finding(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_finding(analysis_db, 1, "AAA", "2026-05-01", "Hammer", 0.8, 30.0)

    result = read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert result.candlestick_event_rows_60td == []


def test_recent_60_trading_day_window_uses_oldest_of_latest_60_valid_close_dates(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    dates = _date_range("2026-01-01", 65)
    for idx, pvm in enumerate(dates, start=1):
        _insert_close(osakedata_db, "AAA", pvm, 100.0 + idx, "usa")
    _insert_finding(analysis_db, 1, "AAA", dates[-1], "Hammer", 0.9, 31.0)

    result = read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", dates[-1], market="usa")
    row = result.candlestick_event_rows_60td[0]

    assert row["sequence_available_trading_days"] == 60
    assert row["sequence_window_start_date"] == dates[-60]
    assert row["sequence_window_end_date"] == dates[-1]


def test_short_available_trading_history_is_supported(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_finding(analysis_db, 1, "AAA", "2026-04-30", "Hammer", 0.8, 30.0)

    result = read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.candlestick_event_rows_60td[0]

    assert row["sequence_available_trading_days"] == 3
    assert row["sequence_window_start_date"] == "2026-04-28"
    assert row["sequence_window_end_date"] == "2026-04-30"


def test_event_sequence_ordering_uses_date_asc_then_id_asc(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_finding(analysis_db, 3, "AAA", "2026-04-30", "Morning Star", 0.7, 32.0)
    _insert_finding(analysis_db, 1, "AAA", "2026-04-29", "Hammer", 0.8, 30.0)
    _insert_finding(analysis_db, 2, "AAA", "2026-04-30", "Shooting Star", 0.6, 70.0)

    result = read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert [row["finding_id"] for row in result.candlestick_event_rows_60td] == [1, 2, 3]
    assert [row["sequence_index"] for row in result.candlestick_event_rows_60td] == [1, 2, 3]


def test_pattern_whitelist_excludes_combos_and_unrelated_patterns(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in _date_range("2026-04-01", 20):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    for index, pattern in enumerate(_allowed_patterns(), start=1):
        _insert_finding(analysis_db, index, "AAA", "2026-04-20", pattern, 0.5, 40.0)
    _insert_finding(analysis_db, 100, "AAA", "2026-04-20", "BullDiv & Hammer", 0.9, 28.0)
    _insert_finding(analysis_db, 101, "AAA", "2026-04-20", "Random Pattern", 0.1, 50.0)

    result = read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-20", market="usa")
    patterns = [row["pattern"] for row in result.candlestick_event_rows_60td]

    assert patterns == list(_allowed_patterns())
    assert "BullDiv & Hammer" not in patterns
    assert "Random Pattern" not in patterns


def test_pattern_groups_map_only_to_basic_bullish_and_bearish_candles(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in _date_range("2026-04-01", 20):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_finding(analysis_db, 1, "AAA", "2026-04-19", "Hammer", 0.8, 30.0)
    _insert_finding(analysis_db, 2, "AAA", "2026-04-20", "Bearish Engulfing", 0.7, 70.0)

    result = read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-20", market="usa")
    groups = {row["pattern"]: row["pattern_group"] for row in result.candlestick_event_rows_60td}

    assert groups["Hammer"] == "BULLISH_CANDLE"
    assert groups["Bearish Engulfing"] == "BEARISH_CANDLE"
    assert all(group not in {"BULLDIV_COMBO", "COMBO", "DIVERGENCE"} for group in groups.values())


def test_no_summary_counts_are_returned(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")

    export_dict = read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa").to_dict()

    assert "recent_event_summary" not in export_dict
    assert "bullish_count_60td" not in export_dict
    assert "bearish_count_60td" not in export_dict
    assert "combo_count_60td" not in export_dict


def test_no_coverage_inference_fields_are_returned(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_finding(analysis_db, 1, "AAA", "2026-04-30", "Hammer", 0.8, 30.0)

    result = read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    keys = set(result.candlestick_event_rows_60td[0].keys())

    assert "coverage_status" not in keys
    assert "latest_finding_date" not in keys


def test_reader_does_not_write_to_databases(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_finding(analysis_db, 1, "AAA", "2026-04-30", "Hammer", 0.8, 30.0)
    before_findings = _table_count(analysis_db, "analysis_findings")
    before_ohlcv = _table_count(osakedata_db, "osakedata")

    read_candlestick_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert _table_count(analysis_db, "analysis_findings") == before_findings
    assert _table_count(osakedata_db, "osakedata") == before_ohlcv


def _create_test_dbs(tmp_path: Path) -> tuple[Path, Path]:
    analysis_db = tmp_path / "analysis.db"
    osakedata_db = tmp_path / "osakedata.db"
    with sqlite3.connect(analysis_db) as conn:
        conn.execute(
            """
            CREATE TABLE analysis_findings (
                id INTEGER PRIMARY KEY,
                ticker TEXT,
                date TEXT,
                pattern TEXT,
                signal_strength REAL,
                rsi14 REAL,
                created_at TEXT
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


def _insert_finding(
    db_path: Path,
    finding_id: int,
    ticker: str,
    signal_date: str,
    pattern: str,
    signal_strength: float | None,
    rsi14: float | None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO analysis_findings (id, ticker, date, pattern, signal_strength, rsi14, created_at)
            VALUES (?, ?, ?, ?, ?, ?, '2026-04-30T00:00:00Z')
            """,
            (finding_id, ticker, signal_date, pattern, signal_strength, rsi14),
        )
        conn.commit()


def _table_count(db_path: Path, table_name: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0])


def _date_range(start_date: str, count: int) -> list[str]:
    start = date.fromisoformat(start_date)
    return [(start + timedelta(days=offset)).isoformat() for offset in range(count)]


def _allowed_patterns() -> tuple[str, ...]:
    return (
        "Hammer",
        "Bullish Engulfing",
        "Piercing Pattern",
        "Three White Soldiers",
        "Morning Star",
        "Dragonfly Doji",
        "Bearish Engulfing",
        "Shooting Star",
        "Dark Cloud Cover",
        "Evening Star",
        "Hanging Man",
    )
