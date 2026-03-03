from __future__ import annotations

import json
import sqlite3

from swingmaster.cli import run_transactions_simu_fast
from swingmaster.infra.sqlite.migrator import apply_migrations


def _conn_memory() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _insert_osakedata_rows(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    market: str,
    end_date: str,
    close: float,
    volume: int,
    days: int,
) -> None:
    year, month, day = end_date.split("-")
    end_day = int(day)
    for idx in range(days):
        trade_day = end_day - idx
        conn.execute(
            """
            INSERT INTO osakedata (osake, pvm, close, volume, market)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ticker, f"{year}-{month}-{trade_day:02d}", close, volume, market),
        )


def _create_osakedata_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE osakedata (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          osake TEXT,
          pvm TEXT,
          open REAL,
          high REAL,
          low REAL,
          close REAL,
          volume INTEGER,
          market TEXT DEFAULT 'usa'
        )
        """
    )


def _create_analysis_findings_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE analysis_findings (
          id INTEGER PRIMARY KEY,
          ticker TEXT NOT NULL,
          date TEXT NOT NULL,
          pattern TEXT,
          signal_strength REAL,
          rsi14 REAL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(ticker, date, pattern)
        )
        """
    )


def test_apply_migrations_adds_buy_badges_column_to_rc_transactions_simu() -> None:
    conn = _conn_memory()
    apply_migrations(conn)

    columns = conn.execute("PRAGMA table_info(rc_transactions_simu)").fetchall()
    by_name = {str(row["name"]): row for row in columns}

    assert "buy_badges" in by_name
    assert by_name["buy_badges"]["type"] == "TEXT"
    assert by_name["buy_badges"]["notnull"] == 1
    assert by_name["buy_badges"]["dflt_value"] == "'[]'"


def test_resolve_buy_badges_returns_deterministic_json_array() -> None:
    rc_conn = _conn_memory()
    os_conn = _conn_memory()
    analysis_conn = _conn_memory()
    apply_migrations(rc_conn)
    _create_osakedata_table(os_conn)
    _create_analysis_findings_table(analysis_conn)
    rc_conn.execute(
        """
        INSERT INTO rc_state_daily (
          ticker, date, state, reasons_json, confidence, age, run_id, state_attrs_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAA",
            "2026-01-20",
            "PASS",
            "[]",
            None,
            0,
            None,
            (
                '{"entry_quality":"A","decline_profile":"SLOW_DRIFT",'
                '"entry_gate":"EARLY_STAB_MA20_HL","downtrend_origin":"SLOW",'
                '"downtrend_entry_type":"SLOW_STRUCTURAL"}'
            ),
        ),
    )
    _insert_osakedata_rows(
        os_conn,
        ticker="AAA",
        market="omxh",
        end_date="2026-01-20",
        close=10.0,
        volume=1000,
        days=20,
    )
    analysis_conn.execute(
        """
        INSERT INTO analysis_findings (id, ticker, date, pattern)
        VALUES (?, ?, ?, ?)
        """,
        (1, "AAA", "2026-01-18", "bUlLdIv & hAmMeR"),
    )

    expected = '["downtrend_entry_type=SLOW_STRUCTURAL","LOW_VOLUME","BULL_DIV_IN_LAST_20_DAYS"]'

    assert (
        run_transactions_simu_fast.resolve_buy_badges(
            rc_conn,
            os_conn,
            analysis_conn,
            "AAA",
            "2026-01-20",
            "FIN",
            True,
            {},
        )
        == expected
    )


def test_resolve_buy_badges_skips_low_volume_when_less_than_20_days() -> None:
    rc_conn = _conn_memory()
    os_conn = _conn_memory()
    analysis_conn = _conn_memory()
    apply_migrations(rc_conn)
    _create_osakedata_table(os_conn)
    _create_analysis_findings_table(analysis_conn)
    _insert_osakedata_rows(
        os_conn,
        ticker="AAA",
        market="omxh",
        end_date="2026-01-20",
        close=10.0,
        volume=1000,
        days=19,
    )

    badges = json.loads(
        run_transactions_simu_fast.resolve_buy_badges(
            rc_conn,
            os_conn,
            analysis_conn,
            "AAA",
            "2026-01-20",
            "FIN",
            False,
            {},
        )
    )
    assert badges == []


def test_resolve_buy_badges_skips_low_volume_when_threshold_not_breached() -> None:
    rc_conn = _conn_memory()
    os_conn = _conn_memory()
    analysis_conn = _conn_memory()
    apply_migrations(rc_conn)
    _create_osakedata_table(os_conn)
    _create_analysis_findings_table(analysis_conn)
    _insert_osakedata_rows(
        os_conn,
        ticker="AAA",
        market="omxh",
        end_date="2026-01-20",
        close=100.0,
        volume=1000,
        days=20,
    )

    badges = json.loads(
        run_transactions_simu_fast.resolve_buy_badges(
            rc_conn,
            os_conn,
            analysis_conn,
            "AAA",
            "2026-01-20",
            "FIN",
            False,
            {},
        )
    )
    assert badges == []


def test_resolve_buy_badges_adds_penny_stock_when_sma20_close_is_below_threshold() -> None:
    rc_conn = _conn_memory()
    os_conn = _conn_memory()
    analysis_conn = _conn_memory()
    apply_migrations(rc_conn)
    _create_osakedata_table(os_conn)
    _create_analysis_findings_table(analysis_conn)
    _insert_osakedata_rows(
        os_conn,
        ticker="AAA",
        market="omxh",
        end_date="2026-01-20",
        close=0.5,
        volume=100000,
        days=20,
    )

    badges = json.loads(
        run_transactions_simu_fast.resolve_buy_badges(
            rc_conn,
            os_conn,
            analysis_conn,
            "AAA",
            "2026-01-20",
            "FIN",
            False,
            {},
        )
    )
    assert badges == ["PENNY_STOCK"]


def test_resolve_buy_badges_skips_penny_stock_when_threshold_not_breached() -> None:
    rc_conn = _conn_memory()
    os_conn = _conn_memory()
    analysis_conn = _conn_memory()
    apply_migrations(rc_conn)
    _create_osakedata_table(os_conn)
    _create_analysis_findings_table(analysis_conn)
    _insert_osakedata_rows(
        os_conn,
        ticker="AAA",
        market="omxh",
        end_date="2026-01-20",
        close=1.0,
        volume=100000,
        days=20,
    )

    badges = json.loads(
        run_transactions_simu_fast.resolve_buy_badges(
            rc_conn,
            os_conn,
            analysis_conn,
            "AAA",
            "2026-01-20",
            "FIN",
            False,
            {},
        )
    )
    assert badges == []


def test_resolve_buy_badges_skips_bull_div_when_pattern_is_outside_20_trading_days() -> None:
    rc_conn = _conn_memory()
    os_conn = _conn_memory()
    analysis_conn = _conn_memory()
    apply_migrations(rc_conn)
    _create_osakedata_table(os_conn)
    _create_analysis_findings_table(analysis_conn)
    _insert_osakedata_rows(
        os_conn,
        ticker="AAA",
        market="omxh",
        end_date="2026-01-20",
        close=10.0,
        volume=100000,
        days=20,
    )
    analysis_conn.execute(
        """
        INSERT INTO analysis_findings (id, ticker, date, pattern)
        VALUES (?, ?, ?, ?)
        """,
        (1, "AAA", "2025-12-31", "Bullish Divergence"),
    )

    badges = json.loads(
        run_transactions_simu_fast.resolve_buy_badges(
            rc_conn,
            os_conn,
            analysis_conn,
            "AAA",
            "2026-01-20",
            "FIN",
            False,
            {},
        )
    )
    assert badges == []
