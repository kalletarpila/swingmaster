from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.divergence_signal_reader import read_divergence_signal_raw_export


def test_no_lookahead_excludes_future_row_from_latest_and_signal_rows(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-05-01", bullish_strength=0.5)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["latest_row_found"] is False
    assert row["latest_signal_found"] is False
    assert result.divergence_signal_rows_60td == []


def test_pivot2_date_does_not_override_row_date_for_no_lookahead(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(
        analysis_db,
        "AAA",
        "2026-05-01",
        is_bullish_divergence_r3=1,
        pivot2_date_r3="2026-04-10",
    )

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert result.divergence_context_snapshot_rows[0]["latest_signal_found"] is False
    assert result.divergence_context_snapshot_rows[0]["latest_signal_pattern"] is None
    assert result.divergence_context_snapshot_rows[0]["latest_signal_group"] is None
    assert result.divergence_context_snapshot_rows[0]["latest_signal_variant"] is None
    assert result.divergence_context_snapshot_rows[0]["latest_signal_direction"] is None
    assert result.divergence_context_snapshot_rows[0]["latest_signal_radius"] is None
    assert result.divergence_context_snapshot_rows[0]["latest_signal_source_flag"] is None
    assert result.divergence_signal_rows_60td == []


def test_recent_60_trading_day_window_uses_oldest_of_latest_60_valid_close_dates(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    dates = _date_range("2026-01-01", 65)
    for idx, pvm in enumerate(dates, start=1):
        _insert_close(osakedata_db, "AAA", pvm, 100.0 + idx, "usa")
    _insert_divergence_row(analysis_db, "AAA", dates[-1], bullish_strength=0.4)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", dates[-1], market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["sequence_available_trading_days"] == 60
    assert row["sequence_window_start_date"] == dates[-60]
    assert row["sequence_window_end_date"] == dates[-1]


def test_short_available_trading_history_is_supported(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", bullish_strength=0.4)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["sequence_available_trading_days"] == 3
    assert row["sequence_window_start_date"] == "2026-04-28"
    assert row["sequence_window_end_date"] == "2026-04-30"


def test_signal_rows_include_only_actual_signal_rows(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-28")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-29", bullish_strength=0.1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", is_bullish_divergence_r2=1)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert [row["signal_date"] for row in result.divergence_signal_rows_60td] == ["2026-04-30"]
    assert result.divergence_signal_rows_60td[0]["divergence_pattern"] == "Bullish Divergence R2"


def test_strength_only_rows_are_excluded(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-04-27", "2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-27", bullish_strength=0.1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-28", bearish_strength=0.2)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-29", hidden_bullish_strength=0.3)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", hidden_bearish_strength=0.4)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert result.divergence_signal_rows_60td == []


def test_r2_r3_rows_expand_into_event_rows(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in _date_range("2026-04-25", 8):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-25", is_bullish_divergence_r2=1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-26", is_bearish_divergence_r2=1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-27", is_hidden_bullish_divergence_r2=1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-28", is_hidden_bearish_divergence_r2=1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-29", is_bullish_divergence_r3=1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", is_hidden_bearish_divergence_r3=1)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert len(result.divergence_signal_rows_60td) == 6
    assert result.divergence_signal_rows_60td[1]["divergence_pattern"] == "Bearish Divergence R2"
    assert result.divergence_signal_rows_60td[1]["divergence_group"] == "BEARISH_DIVERGENCE"
    assert result.divergence_signal_rows_60td[1]["divergence_variant"] == "REGULAR"
    assert result.divergence_signal_rows_60td[1]["divergence_direction"] == "BEARISH"
    assert result.divergence_signal_rows_60td[1]["divergence_radius"] == "R2"
    assert result.divergence_signal_rows_60td[1]["source_flag"] == "is_bearish_divergence_r2"


def test_latest_signal_metadata_fields_are_populated(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", is_hidden_bullish_divergence_r3=1)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["latest_signal_found"] is True
    assert row["latest_signal_date"] == "2026-04-30"
    assert row["latest_signal_pattern"] == "Hidden Bullish Divergence R3"
    assert row["latest_signal_group"] == "HIDDEN_BULLISH_DIVERGENCE"
    assert row["latest_signal_variant"] == "HIDDEN"
    assert row["latest_signal_direction"] == "BULLISH"
    assert row["latest_signal_radius"] == "R3"
    assert row["latest_signal_source_flag"] == "is_hidden_bullish_divergence_r3"


def test_multiple_flags_on_same_date_produce_multiple_rows_in_deterministic_order(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(
        analysis_db,
        "AAA",
        "2026-04-30",
        bullish_strength=0.4,
        bearish_strength=0.2,
        hidden_bullish_strength=0.3,
        hidden_bearish_strength=0.1,
        is_bullish_divergence_r2=1,
        is_bearish_divergence_r2=1,
        is_hidden_bullish_divergence_r2=1,
        is_hidden_bearish_divergence_r2=1,
        is_bullish_divergence_r3=1,
        is_bearish_divergence_r3=1,
        is_hidden_bullish_divergence_r3=1,
        is_hidden_bearish_divergence_r3=1,
        rsi=55.5,
    )

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert [row["divergence_pattern"] for row in result.divergence_signal_rows_60td] == [
        "Bullish Divergence R2",
        "Bearish Divergence R2",
        "Hidden Bullish Divergence R2",
        "Hidden Bearish Divergence R2",
        "Bullish Divergence R3",
        "Bearish Divergence R3",
        "Hidden Bullish Divergence R3",
        "Hidden Bearish Divergence R3",
    ]
    assert [row["sequence_index"] for row in result.divergence_signal_rows_60td] == [1, 2, 3, 4, 5, 6, 7, 8]


def test_latest_signal_metadata_uses_reverse_priority_on_same_date(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(
        analysis_db,
        "AAA",
        "2026-04-30",
        is_bullish_divergence_r2=1,
        is_hidden_bullish_divergence_r3=1,
        is_hidden_bearish_divergence_r3=1,
    )

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["latest_signal_pattern"] == "Hidden Bearish Divergence R3"
    assert row["latest_signal_group"] == "HIDDEN_BEARISH_DIVERGENCE"
    assert row["latest_signal_variant"] == "HIDDEN"
    assert row["latest_signal_direction"] == "BEARISH"
    assert row["latest_signal_radius"] == "R3"
    assert row["latest_signal_source_flag"] == "is_hidden_bearish_divergence_r3"


def test_strength_and_pivot_fields_map_to_correct_event(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(
        analysis_db,
        "AAA",
        "2026-04-30",
        bullish_strength=0.4,
        bearish_strength=0.5,
        hidden_bullish_strength=0.6,
        hidden_bearish_strength=0.7,
        rsi=55.5,
        is_bullish_divergence_r2=1,
        is_hidden_bearish_divergence_r2=1,
        is_bearish_divergence_r3=1,
        is_hidden_bullish_divergence_r3=1,
        pivot_gap_r2=12,
        pivot_drop_pct_r2=1.2,
        hidden_pivot_gap_r2=13,
        hidden_pivot_drop_pct_r2=1.3,
        pivot2_date_r2="2026-04-20",
        pivot_gap_r3=22,
        pivot_drop_pct_r3=2.2,
        hidden_pivot_gap_r3=23,
        hidden_pivot_drop_pct_r3=2.3,
        pivot2_date_r3="2026-04-10",
    )

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    rows_by_pattern = {row["divergence_pattern"]: row for row in result.divergence_signal_rows_60td}

    assert rows_by_pattern["Bullish Divergence R2"]["signal_strength"] == 0.4
    assert rows_by_pattern["Bullish Divergence R2"]["pivot_gap"] == 12
    assert rows_by_pattern["Bullish Divergence R2"]["pivot_drop_pct"] == 1.2
    assert rows_by_pattern["Bullish Divergence R2"]["pivot2_date"] == "2026-04-20"
    assert rows_by_pattern["Hidden Bearish Divergence R2"]["signal_strength"] == 0.7
    assert rows_by_pattern["Hidden Bearish Divergence R2"]["pivot_gap"] == 13
    assert rows_by_pattern["Hidden Bearish Divergence R2"]["pivot_drop_pct"] == 1.3
    assert rows_by_pattern["Hidden Bearish Divergence R2"]["pivot2_date"] == "2026-04-20"
    assert rows_by_pattern["Bearish Divergence R3"]["signal_strength"] == 0.5
    assert rows_by_pattern["Bearish Divergence R3"]["pivot_gap"] == 22
    assert rows_by_pattern["Bearish Divergence R3"]["pivot_drop_pct"] == 2.2
    assert rows_by_pattern["Bearish Divergence R3"]["pivot2_date"] == "2026-04-10"
    assert rows_by_pattern["Hidden Bullish Divergence R3"]["signal_strength"] == 0.6
    assert rows_by_pattern["Hidden Bullish Divergence R3"]["pivot_gap"] == 23
    assert rows_by_pattern["Hidden Bullish Divergence R3"]["pivot_drop_pct"] == 2.3
    assert rows_by_pattern["Hidden Bullish Divergence R3"]["pivot2_date"] == "2026-04-10"
    assert all(row["rsi"] == 55.5 for row in result.divergence_signal_rows_60td)


def test_event_sequence_ordering_uses_date_asc(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-04-28", "2026-04-29", "2026-04-30"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", is_bearish_divergence_r3=1, bearish_strength=0.3)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-28", is_bullish_divergence_r2=1, bullish_strength=0.1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-29", is_hidden_bullish_divergence_r2=1, hidden_bullish_strength=0.2)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert [row["signal_date"] for row in result.divergence_signal_rows_60td] == ["2026-04-28", "2026-04-29", "2026-04-30"]
    assert [row["sequence_index"] for row in result.divergence_signal_rows_60td] == [1, 2, 3]


def test_coverage_ok(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30")

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert result.divergence_context_snapshot_rows[0]["divergence_coverage_status"] == "OK"


def test_coverage_stale_sets_warning_flag(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-29")

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["divergence_coverage_status"] == "STALE"
    assert "DIVERGENCE_STALE" in row["divergence_warning_flags"]


def test_coverage_missing_sets_warning_flag(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["divergence_coverage_status"] == "MISSING"
    assert "DIVERGENCE_MISSING" in row["divergence_warning_flags"]


def test_no_valid_close_data_sets_warning_flag_and_empty_rows(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", None, "usa")

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["divergence_coverage_status"] == "NO_VALID_CLOSE_DATA"
    assert "NO_VALID_CLOSE_DATA" in row["divergence_warning_flags"]
    assert result.divergence_signal_rows_60td == []


def test_latest_available_row_may_differ_from_latest_signal_row(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    for pvm in ("2026-04-29", "2026-04-30"):
        _insert_close(osakedata_db, "AAA", pvm, 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-29", bullish_strength=0.4, is_bullish_divergence_r2=1)
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", bearish_strength=0.2)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_context_snapshot_rows[0]

    assert row["latest_row_date"] == "2026-04-30"
    assert row["latest_signal_date"] == "2026-04-29"
    assert row["latest_signal_pattern"] == "Bullish Divergence R2"
    assert row["latest_signal_group"] == "BULLISH_DIVERGENCE"
    assert row["latest_signal_variant"] == "REGULAR"
    assert row["latest_signal_direction"] == "BULLISH"
    assert row["latest_signal_radius"] == "R2"
    assert row["latest_signal_source_flag"] == "is_bullish_divergence_r2"


def test_signal_rows_do_not_include_wide_boolean_fields(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", bullish_strength=0.4, is_bullish_divergence_r2=1)

    result = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")
    row = result.divergence_signal_rows_60td[0]

    assert "is_bullish_divergence_r2" not in row
    assert "is_bearish_divergence_r2" not in row
    assert "is_hidden_bullish_divergence_r2" not in row
    assert "is_hidden_bearish_divergence_r2" not in row
    assert "is_bullish_divergence_r3" not in row
    assert "is_bearish_divergence_r3" not in row
    assert "is_hidden_bullish_divergence_r3" not in row
    assert "is_hidden_bearish_divergence_r3" not in row
    assert "has_bullish_signal" not in row
    assert "has_bearish_signal" not in row
    assert "has_hidden_bullish_signal" not in row
    assert "has_hidden_bearish_signal" not in row
    assert "has_r2_signal" not in row
    assert "has_r3_signal" not in row


def test_no_summary_counts_are_returned(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")

    export_dict = read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa").to_dict()

    assert "recent_signal_summary" not in export_dict
    assert "bullish_count_60td" not in export_dict
    assert "bearish_count_60td" not in export_dict
    assert "hidden_bullish_count_60td" not in export_dict


def test_reader_does_not_write_to_databases(tmp_path: Path) -> None:
    analysis_db, osakedata_db = _create_test_dbs(tmp_path)
    _insert_close(osakedata_db, "AAA", "2026-04-30", 10.0, "usa")
    _insert_divergence_row(analysis_db, "AAA", "2026-04-30", bullish_strength=0.4)
    before_divergence = _table_count(analysis_db, "divergence_data")
    before_ohlcv = _table_count(osakedata_db, "osakedata")

    read_divergence_signal_raw_export(str(analysis_db), str(osakedata_db), "AAA", "2026-04-30", market="usa")

    assert _table_count(analysis_db, "divergence_data") == before_divergence
    assert _table_count(osakedata_db, "osakedata") == before_ohlcv


def _create_test_dbs(tmp_path: Path) -> tuple[Path, Path]:
    analysis_db = tmp_path / "analysis.db"
    osakedata_db = tmp_path / "osakedata.db"
    with sqlite3.connect(analysis_db) as conn:
        conn.execute(
            """
            CREATE TABLE divergence_data (
                ticker TEXT,
                date TEXT,
                bullish_strength REAL,
                bearish_strength REAL,
                hidden_bullish_strength REAL,
                hidden_bearish_strength REAL,
                rsi REAL,
                is_bullish_divergence INTEGER,
                is_bearish_divergence INTEGER,
                is_hidden_bullish_divergence INTEGER,
                is_hidden_bearish_divergence INTEGER,
                is_bullish_divergence_r2 INTEGER,
                is_bearish_divergence_r2 INTEGER,
                is_hidden_bullish_divergence_r2 INTEGER,
                is_hidden_bearish_divergence_r2 INTEGER,
                is_bullish_divergence_r3 INTEGER,
                is_bearish_divergence_r3 INTEGER,
                is_hidden_bullish_divergence_r3 INTEGER,
                is_hidden_bearish_divergence_r3 INTEGER,
                pivot_gap INTEGER,
                pivot_drop_pct REAL,
                pivot_gap_r2 INTEGER,
                pivot_drop_pct_r2 REAL,
                hidden_pivot_gap_r2 INTEGER,
                hidden_pivot_drop_pct_r2 REAL,
                pivot2_date_r2 TEXT,
                pivot_gap_r3 INTEGER,
                pivot_drop_pct_r3 REAL,
                hidden_pivot_gap_r3 INTEGER,
                hidden_pivot_drop_pct_r3 REAL,
                pivot2_date_r3 TEXT,
                PRIMARY KEY (ticker, date)
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


def _insert_divergence_row(
    db_path: Path,
    ticker: str,
    signal_date: str,
    *,
    bullish_strength: float | None = None,
    bearish_strength: float | None = None,
    hidden_bullish_strength: float | None = None,
    hidden_bearish_strength: float | None = None,
    rsi: float | None = None,
    is_bullish_divergence: int | None = None,
    is_bearish_divergence: int | None = None,
    is_hidden_bullish_divergence: int | None = None,
    is_hidden_bearish_divergence: int | None = None,
    is_bullish_divergence_r2: int | None = None,
    is_bearish_divergence_r2: int | None = None,
    is_hidden_bullish_divergence_r2: int | None = None,
    is_hidden_bearish_divergence_r2: int | None = None,
    is_bullish_divergence_r3: int | None = None,
    is_bearish_divergence_r3: int | None = None,
    is_hidden_bullish_divergence_r3: int | None = None,
    is_hidden_bearish_divergence_r3: int | None = None,
    pivot_gap: int | None = None,
    pivot_drop_pct: float | None = None,
    pivot_gap_r2: int | None = None,
    pivot_drop_pct_r2: float | None = None,
    hidden_pivot_gap_r2: int | None = None,
    hidden_pivot_drop_pct_r2: float | None = None,
    pivot2_date_r2: str | None = None,
    pivot_gap_r3: int | None = None,
    pivot_drop_pct_r3: float | None = None,
    hidden_pivot_gap_r3: int | None = None,
    hidden_pivot_drop_pct_r3: float | None = None,
    pivot2_date_r3: str | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO divergence_data (
                ticker, date, bullish_strength, bearish_strength, hidden_bullish_strength, hidden_bearish_strength,
                rsi, is_bullish_divergence, is_bearish_divergence, is_hidden_bullish_divergence, is_hidden_bearish_divergence,
                is_bullish_divergence_r2, is_bearish_divergence_r2, is_hidden_bullish_divergence_r2, is_hidden_bearish_divergence_r2,
                is_bullish_divergence_r3, is_bearish_divergence_r3, is_hidden_bullish_divergence_r3, is_hidden_bearish_divergence_r3,
                pivot_gap, pivot_drop_pct, pivot_gap_r2, pivot_drop_pct_r2, hidden_pivot_gap_r2, hidden_pivot_drop_pct_r2,
                pivot2_date_r2, pivot_gap_r3, pivot_drop_pct_r3, hidden_pivot_gap_r3, hidden_pivot_drop_pct_r3, pivot2_date_r3
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                signal_date,
                bullish_strength,
                bearish_strength,
                hidden_bullish_strength,
                hidden_bearish_strength,
                rsi,
                is_bullish_divergence,
                is_bearish_divergence,
                is_hidden_bullish_divergence,
                is_hidden_bearish_divergence,
                is_bullish_divergence_r2,
                is_bearish_divergence_r2,
                is_hidden_bullish_divergence_r2,
                is_hidden_bearish_divergence_r2,
                is_bullish_divergence_r3,
                is_bearish_divergence_r3,
                is_hidden_bullish_divergence_r3,
                is_hidden_bearish_divergence_r3,
                pivot_gap,
                pivot_drop_pct,
                pivot_gap_r2,
                pivot_drop_pct_r2,
                hidden_pivot_gap_r2,
                hidden_pivot_drop_pct_r2,
                pivot2_date_r2,
                pivot_gap_r3,
                pivot_drop_pct_r3,
                hidden_pivot_gap_r3,
                hidden_pivot_drop_pct_r3,
                pivot2_date_r3,
            ),
        )
        conn.commit()


def _table_count(db_path: Path, table_name: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0])


def _date_range(start_date: str, count: int) -> list[str]:
    start = date.fromisoformat(start_date)
    return [(start + timedelta(days=offset)).isoformat() for offset in range(count)]
