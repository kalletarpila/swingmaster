"""Tests for slow decline started v2."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

from swingmaster.app_api.providers.osakedata_signal_provider_v2 import OsakeDataSignalProviderV2
from swingmaster.app_api.providers.signals_v2.context import SignalContextV2
from swingmaster.app_api.providers.signals_v2.slow_decline_started import eval_slow_decline_started
from swingmaster.core.signals.enums import SignalKey


def _ctx(closes_desc: list[float]) -> SignalContextV2:
    return SignalContextV2(closes=closes_desc, highs=closes_desc, lows=closes_desc, ohlc=[])


def test_slow_decline_started_true_case() -> None:
    closes = [95.0, 96.2, 97.0, 97.8, 98.2, 98.5, 98.9, 99.2, 99.6, 99.8, 100.0, 101.0]
    assert eval_slow_decline_started(_ctx(closes), min_decline_percent=3.0, use_ma_filter=True) is True


def test_slow_decline_started_false_not_strictly_decreasing() -> None:
    closes = [95.0, 96.2, 97.0, 97.8, 98.2, 97.0, 98.9, 99.2, 99.6, 99.8, 100.0, 101.0]
    assert eval_slow_decline_started(_ctx(closes), min_decline_percent=3.0, use_ma_filter=True) is False


def test_slow_decline_started_false_decline_below_threshold() -> None:
    closes = [98.0, 98.2, 98.6, 98.7, 98.8, 99.0, 99.1, 99.2, 99.3, 99.4, 100.0, 101.0]
    assert eval_slow_decline_started(_ctx(closes), min_decline_percent=3.0, use_ma_filter=True) is False


def test_slow_decline_started_false_when_ma_filter_fails() -> None:
    closes = [95.0, 95.0, 95.0, 95.0, 110.0, 98.5, 98.9, 99.2, 99.6, 99.8, 100.0, 101.0]
    assert eval_slow_decline_started(_ctx(closes), min_decline_percent=3.0, use_ma_filter=True) is False


def test_slow_decline_started_false_insufficient_history() -> None:
    closes = [95.0, 96.0, 97.0, 98.0, 99.0]
    assert eval_slow_decline_started(_ctx(closes), min_decline_percent=3.0, use_ma_filter=True) is False


def test_slow_decline_started_false_when_t10_non_positive() -> None:
    closes_zero = [95.0, 96.2, 97.0, 97.8, 98.2, 98.5, 98.9, 99.2, 99.6, 99.8, 0.0, 101.0]
    closes_negative = [95.0, 96.2, 97.0, 97.8, 98.2, 98.5, 98.9, 99.2, 99.6, 99.8, -1.0, 101.0]
    assert eval_slow_decline_started(_ctx(closes_zero), min_decline_percent=3.0, use_ma_filter=True) is False
    assert eval_slow_decline_started(_ctx(closes_negative), min_decline_percent=3.0, use_ma_filter=True) is False


def test_provider_emits_slow_decline_started_when_true() -> None:
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
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)

    rows = []
    for i in range(required):
        day = base + timedelta(days=i)
        price = 110.0
        rows.append(("AAA", day.isoformat(), price, price + 1.0, price - 1.0, price, 1_000_000, "X"))

    staircase = [100.0, 99.8, 99.6, 99.2, 98.9, 98.5, 98.2, 97.8, 97.0, 96.2, 95.0]
    for i, price in enumerate(staircase):
        idx = required - 11 + i
        day = rows[idx][1]
        rows[idx] = ("AAA", day, price, price + 1.0, price - 1.0, price, 1_000_000, "X")

    conn.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.SLOW_DECLINE_STARTED in signals
    conn.close()
