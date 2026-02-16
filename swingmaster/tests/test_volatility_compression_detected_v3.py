"""Tests for VOLATILITY_COMPRESSION_DETECTED in v3."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

from swingmaster.app_api.providers.osakedata_signal_provider_v3 import OsakeDataSignalProviderV3
from swingmaster.app_api.providers.signals_v3.context import SignalContextV3
from swingmaster.app_api.providers.signals_v3.volatility_compression_detected import (
    eval_volatility_compression_detected,
)
from swingmaster.core.signals.enums import SignalKey


def _setup_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE osakedata (
            osake TEXT,
            pvm TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            market TEXT
        )
        """
    )
    return conn


def _insert_rows(conn: sqlite3.Connection, rows) -> None:
    conn.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _make_rows(ticker: str, start_date: date, count: int, close: float = 100.0):
    rows = []
    for i in range(count):
        day = start_date + timedelta(days=i)
        high = close + 1.0
        low = close - 1.0
        rows.append((ticker, day.isoformat(), close, high, low, close, 1_000_000, "X"))
    return rows


def _apply_recent_ranges_desc(rows, ranges_desc: list[float]) -> None:
    for offset, day_range in enumerate(ranges_desc):
        idx = len(rows) - 1 - offset
        ticker, pvm, _o, _h, _l, close, vol, market = rows[idx]
        high = close + (day_range / 2.0)
        low = close - (day_range / 2.0)
        rows[idx] = (ticker, pvm, close, high, low, close, vol, market)


def test_volatility_compression_detected_true() -> None:
    conn = _setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()

    rows = _make_rows("AAA", date(2026, 1, 1), required, close=100.0)
    ranges_desc = [1.0 + (0.10 * i) for i in range(40)]
    _apply_recent_ranges_desc(rows, ranges_desc)
    _insert_rows(conn, rows)

    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.VOLATILITY_COMPRESSION_DETECTED in signals
    conn.close()


def test_volatility_compression_detected_false_when_not_compressed() -> None:
    conn = _setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()

    rows = _make_rows("AAA", date(2026, 1, 1), required, close=100.0)
    ranges_desc = [2.0 for _ in range(40)]
    _apply_recent_ranges_desc(rows, ranges_desc)
    _insert_rows(conn, rows)

    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.VOLATILITY_COMPRESSION_DETECTED not in signals
    conn.close()


def test_volatility_compression_detected_false_when_insufficient_data() -> None:
    closes = [100.0 for _ in range(20)]
    highs = [101.0 for _ in range(20)]
    lows = [99.0 for _ in range(20)]
    ohlc = []
    start = date(2026, 1, 1)
    for i in range(20):
        day = (start + timedelta(days=i)).isoformat()
        ohlc.append((day, 100.0, 101.0, 99.0, 100.0, 1_000_000))
    ohlc_desc = list(reversed(ohlc))
    ctx = SignalContextV3(closes=closes, highs=highs, lows=lows, ohlc=ohlc_desc)

    def _compute_atr(_ohlc, _period):
        return None

    assert eval_volatility_compression_detected(ctx, _compute_atr) is False
