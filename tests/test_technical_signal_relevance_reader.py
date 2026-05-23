from __future__ import annotations

import ast
from datetime import date, timedelta
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis import technical_signal_relevance_reader
from analysis.technical_signal_relevance_reader import read_technical_signal_relevance_raw_export


def test_reader_returns_rows_for_exact_run_id_ticker_and_timeframe(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        timeframe="1d",
        signal_date="2026-05-12",
        signal_confirmed_as_of_date="2026-05-13",
        signal_name="Bullish Engulfing",
        signal_source_type="CANDLE",
        signal_source_id="src-1",
    )

    result = read_technical_signal_relevance_raw_export(
        str(db_path),
        ticker="AAA",
        run_id="RUN_A",
        as_of_date="2026-05-20",
        lookback_days=45,
        timeframe="1d",
    )

    assert result.run_id == "RUN_A"
    assert result.ticker == "AAA"
    assert result.timeframe == "1d"
    assert result.as_of_date == "2026-05-20"
    assert result.lookback_days == 45
    assert len(result.rows) == 1
    assert result.rows[0]["signal_name"] == "Bullish Engulfing"


def test_reader_filters_out_other_run_ids(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)
    _insert_relevance_row(db_path, run_id="RUN_A", ticker="AAA", signal_source_id="src-a")
    _insert_relevance_row(db_path, run_id="RUN_B", ticker="AAA", signal_source_id="src-b")

    result = read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20")

    assert [row["run_id"] for row in result.rows] == ["RUN_A"]


def test_reader_filters_out_other_tickers(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)
    _insert_relevance_row(db_path, run_id="RUN_A", ticker="AAA", signal_source_id="src-a")
    _insert_relevance_row(db_path, run_id="RUN_A", ticker="BBB", signal_source_id="src-b")

    result = read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20")

    assert [row["ticker"] for row in result.rows] == ["AAA"]


def test_reader_filters_out_other_timeframes(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)
    _insert_relevance_row(db_path, run_id="RUN_A", ticker="AAA", timeframe="1d", signal_source_id="src-a")
    _insert_relevance_row(db_path, run_id="RUN_A", ticker="AAA", timeframe="1wk", signal_source_id="src-b")

    result = read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20", timeframe="1d")

    assert [row["timeframe"] for row in result.rows] == ["1d"]


def test_reader_enforces_no_lookahead_with_confirmed_as_of_date(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        signal_date="2026-05-20",
        signal_confirmed_as_of_date="2026-05-20",
        signal_source_id="src-ok",
    )
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        signal_date="2026-05-21",
        signal_confirmed_as_of_date="2026-05-21",
        signal_source_id="src-future",
    )

    result = read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20")

    assert [row["signal_source_id"] for row in result.rows] == ["src-ok"]


def test_reader_applies_lookback_days_to_confirmed_date(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        signal_date="2026-05-15",
        signal_confirmed_as_of_date="2026-05-15",
        signal_source_id="src-in",
    )
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        signal_date="2026-04-01",
        signal_confirmed_as_of_date="2026-04-01",
        signal_source_id="src-out",
    )

    result = read_technical_signal_relevance_raw_export(
        str(db_path),
        "AAA",
        "RUN_A",
        "2026-05-20",
        lookback_days=30,
    )

    assert [row["signal_source_id"] for row in result.rows] == ["src-in"]


def test_reader_ordering_is_deterministic(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        signal_date="2026-05-18",
        signal_confirmed_as_of_date="2026-05-20",
        signal_name="B Signal",
        signal_source_type="DIVERGENCE",
        signal_source_id=None,
    )
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        signal_date="2026-05-18",
        signal_confirmed_as_of_date="2026-05-20",
        signal_name="B Signal",
        signal_source_type="DIVERGENCE",
        signal_source_id="src-2",
    )
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        signal_date="2026-05-18",
        signal_confirmed_as_of_date="2026-05-20",
        signal_name="A Signal",
        signal_source_type="CANDLE",
        signal_source_id="src-1",
    )
    _insert_relevance_row(
        db_path,
        run_id="RUN_A",
        ticker="AAA",
        signal_date="2026-05-17",
        signal_confirmed_as_of_date="2026-05-19",
        signal_name="Z Signal",
        signal_source_type="CANDLE",
        signal_source_id="src-9",
    )

    result = read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20")

    assert [(row["signal_confirmed_as_of_date"], row["signal_date"], row["signal_name"], row["signal_source_type"], row["signal_source_id"]) for row in result.rows] == [
        ("2026-05-20", "2026-05-18", "A Signal", "CANDLE", "src-1"),
        ("2026-05-20", "2026-05-18", "B Signal", "DIVERGENCE", None),
        ("2026-05-20", "2026-05-18", "B Signal", "DIVERGENCE", "src-2"),
        ("2026-05-19", "2026-05-17", "Z Signal", "CANDLE", "src-9"),
    ]


def test_empty_result_returns_metadata_and_empty_rows(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)

    result = read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20")

    assert result.run_id == "RUN_A"
    assert result.ticker == "AAA"
    assert result.rows == []


def test_blank_run_id_validation_fails_clearly(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)

    try:
        read_technical_signal_relevance_raw_export(str(db_path), "AAA", "   ", "2026-05-20")
    except ValueError as exc:
        assert str(exc) == "run_id must be non-empty"
    else:
        raise AssertionError("Expected ValueError for blank run_id")


def test_blank_ticker_validation_fails_clearly(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)

    try:
        read_technical_signal_relevance_raw_export(str(db_path), "   ", "RUN_A", "2026-05-20")
    except ValueError as exc:
        assert str(exc) == "ticker must be non-empty"
    else:
        raise AssertionError("Expected ValueError for blank ticker")


def test_invalid_as_of_date_validation_fails_clearly(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)

    try:
        read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-02-30")
    except ValueError as exc:
        assert str(exc) == "as_of_date must be a valid YYYY-MM-DD string"
    else:
        raise AssertionError("Expected ValueError for invalid as_of_date")


def test_negative_lookback_days_validation_fails_clearly(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)

    try:
        read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20", lookback_days=-1)
    except ValueError as exc:
        assert str(exc) == "lookback_days must be >= 0"
    else:
        raise AssertionError("Expected ValueError for negative lookback_days")


def test_missing_table_fails_clearly(tmp_path: Path) -> None:
    db_path = tmp_path / "analysis.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE technical_signal_relevance_runs (
                run_id TEXT PRIMARY KEY NOT NULL,
                relevance_rule_version TEXT NOT NULL,
                mapping_version TEXT NOT NULL,
                reason_version TEXT NOT NULL,
                config_snapshot_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        conn.commit()

    try:
        read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20")
    except RuntimeError as exc:
        assert str(exc) == "TECHNICAL_SIGNAL_RELEVANCE_TABLE_NOT_FOUND"
    else:
        raise AssertionError("Expected RuntimeError for missing table")


def test_reader_does_not_write_to_db(tmp_path: Path) -> None:
    db_path = _create_test_db(tmp_path)
    _insert_default_runs(db_path)
    _insert_relevance_row(db_path, run_id="RUN_A", ticker="AAA", signal_source_id="src-a")
    before_rows = _table_count(db_path, "technical_signal_relevance")
    before_runs = _table_count(db_path, "technical_signal_relevance_runs")

    result = read_technical_signal_relevance_raw_export(str(db_path), "AAA", "RUN_A", "2026-05-20")

    assert len(result.rows) == 1
    assert _table_count(db_path, "technical_signal_relevance") == before_rows
    assert _table_count(db_path, "technical_signal_relevance_runs") == before_runs


def test_module_does_not_import_rawcandle() -> None:
    source = Path(technical_signal_relevance_reader.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(alias.name.split(".")[0] != "rawcandle" for alias in node.names)
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            assert node.module.split(".")[0] != "rawcandle"


def _create_test_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "analysis.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE technical_signal_relevance_runs (
                run_id TEXT PRIMARY KEY NOT NULL,
                relevance_rule_version TEXT NOT NULL,
                mapping_version TEXT NOT NULL,
                reason_version TEXT NOT NULL,
                config_snapshot_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE technical_signal_relevance (
                ticker TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                signal_date TEXT NOT NULL,
                signal_confirmed_as_of_date TEXT NOT NULL,
                signal_name TEXT NOT NULL,
                signal_close_price REAL NULL,
                signal_direction TEXT NULL,
                signal_family TEXT NULL,
                signal_source_type TEXT NOT NULL,
                signal_source_id TEXT NULL,
                dow_trend_state TEXT NULL,
                dow_context_state TEXT NULL,
                latest_bos_direction TEXT NULL,
                bars_since_latest_bos INTEGER NULL,
                latest_reset_reason TEXT NULL,
                bars_since_latest_reset INTEGER NULL,
                near_latest_pivot INTEGER NOT NULL,
                near_active_bos_level INTEGER NOT NULL,
                is_trend_aligned INTEGER NOT NULL,
                is_counter_trend INTEGER NOT NULL,
                relevance_class TEXT NOT NULL,
                relevance_reason TEXT NOT NULL,
                relevance_rule_version TEXT NOT NULL,
                mapping_version TEXT NOT NULL,
                reason_version TEXT NOT NULL,
                rule_trace TEXT NULL,
                created_at_utc TEXT NOT NULL,
                run_id TEXT NOT NULL
            )
            """
        )
        conn.commit()
    return db_path


def _insert_default_runs(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO technical_signal_relevance_runs (
                run_id, relevance_rule_version, mapping_version, reason_version, config_snapshot_json, created_at_utc
            ) VALUES (?, 'RULE_V1', 'MAP_V1', 'REASON_V1', '{}', '2026-05-20T00:00:00Z')
            """,
            [("RUN_A",), ("RUN_B",)],
        )
        conn.commit()


def _insert_relevance_row(
    db_path: Path,
    *,
    run_id: str,
    ticker: str,
    timeframe: str = "1d",
    signal_date: str = "2026-05-10",
    signal_confirmed_as_of_date: str = "2026-05-10",
    signal_name: str = "Signal",
    signal_source_type: str = "CANDLE",
    signal_source_id: str | None = "src-1",
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO technical_signal_relevance (
                ticker, timeframe, signal_date, signal_confirmed_as_of_date, signal_name,
                signal_close_price, signal_direction, signal_family, signal_source_type, signal_source_id,
                dow_trend_state, dow_context_state, latest_bos_direction, bars_since_latest_bos,
                latest_reset_reason, bars_since_latest_reset, near_latest_pivot, near_active_bos_level,
                is_trend_aligned, is_counter_trend, relevance_class, relevance_reason,
                relevance_rule_version, mapping_version, reason_version, rule_trace, created_at_utc, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                timeframe,
                signal_date,
                signal_confirmed_as_of_date,
                signal_name,
                100.0,
                "BULLISH",
                "STRUCTURAL_PATTERN",
                signal_source_type,
                signal_source_id,
                "UP",
                "AFTER_BOS",
                "BOS_UP",
                3,
                "RESET",
                5,
                1,
                0,
                1,
                0,
                "RELEVANT",
                "TEST_REASON",
                "RULE_V1",
                "MAP_V1",
                "REASON_V1",
                '["trace"]',
                "2026-05-20T00:00:00Z",
                run_id,
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
