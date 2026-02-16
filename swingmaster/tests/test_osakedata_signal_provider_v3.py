"""Tests for osakedata signal provider v3."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
import pytest

import swingmaster.app_api.providers.osakedata_signal_provider_v3 as provider_v3_module
from swingmaster.app_api.providers.osakedata_signal_provider_v3 import OsakeDataSignalProviderV3
from swingmaster.core.signals.enums import SignalKey


def setup_db() -> sqlite3.Connection:
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


def insert_rows(conn: sqlite3.Connection, rows) -> None:
    conn.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def make_rows(ticker: str, start_date: date, count: int, close: float = 100.0):
    rows = []
    for i in range(count):
        day = start_date + timedelta(days=i)
        rows.append((ticker, day.isoformat(), close, close + 1.0, close - 1.0, close, 1_000_000, "X"))
    return rows


def test_slow_drift_detected_triggers() -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=110.0)

    staircase = [100.0, 99.8, 99.6, 99.2, 98.9, 98.5, 98.2, 97.8, 97.0, 96.2, 95.0]
    for i, price in enumerate(staircase):
        idx = required - 11 + i
        day = rows[idx][1]
        rows[idx] = ("AAA", day, price, price + 1.0, price - 1.0, price, 1_000_000, "X")

    insert_rows(conn, rows)
    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.SLOW_DRIFT_DETECTED in signals
    conn.close()


def test_v3_emits_slow_decline_started_when_slow_drift_detected() -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=110.0)

    staircase = [100.0, 99.8, 99.6, 99.2, 98.9, 98.5, 98.2, 97.8, 97.0, 96.2, 95.0]
    for i, price in enumerate(staircase):
        idx = required - 11 + i
        day = rows[idx][1]
        rows[idx] = ("AAA", day, price, price + 1.0, price - 1.0, price, 1_000_000, "X")

    insert_rows(conn, rows)
    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.SLOW_DRIFT_DETECTED in signals
    assert SignalKey.SLOW_DECLINE_STARTED in signals
    conn.close()


def test_v3_does_not_emit_slow_decline_started_when_slow_drift_not_detected() -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=100.0)

    insert_rows(conn, rows)
    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.SLOW_DRIFT_DETECTED not in signals
    assert SignalKey.SLOW_DECLINE_STARTED not in signals
    conn.close()


def test_sharp_sell_off_detected_triggers() -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=100.0)

    last = rows[-1]
    rows[-1] = (last[0], last[1], 90.0, 91.0, 89.0, 90.0, 1_000_000, "X")

    insert_rows(conn, rows)
    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.SHARP_SELL_OFF_DETECTED in signals
    conn.close()


def test_structural_downtrend_detected_triggers() -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=120.0)

    pattern_asc = [110.0, 108.0, 109.0, 107.0, 108.0, 106.0, 107.0, 105.0, 106.0, 104.0, 105.0, 103.0, 104.0, 102.0, 103.0]
    for i, price in enumerate(pattern_asc):
        idx = required - len(pattern_asc) + i
        day = rows[idx][1]
        rows[idx] = ("AAA", day, price, price + 1.0, price - 1.0, price, 1_000_000, "X")

    insert_rows(conn, rows)
    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.STRUCTURAL_DOWNTREND_DETECTED in signals
    conn.close()


def test_data_insufficient_v3() -> None:
    conn = setup_db()
    rows = make_rows("AAA", date(2026, 1, 1), 5, close=100.0)
    insert_rows(conn, rows)

    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    signals = set(provider.get_signals("AAA", rows[-1][1]).signals.keys())
    assert signals == {SignalKey.DATA_INSUFFICIENT}
    conn.close()


def test_higher_low_confirmed_wrapper_true(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=100.0)
    insert_rows(conn, rows)

    monkeypatch.setattr(
        provider_v3_module,
        "compute_dow_signal_facts",
        lambda *a, **k: {SignalKey.DOW_LAST_LOW_HL: True},
    )

    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.DOW_LAST_LOW_HL in signals
    assert SignalKey.HIGHER_LOW_CONFIRMED in signals
    conn.close()


def test_higher_low_confirmed_wrapper_false(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=100.0)
    insert_rows(conn, rows)

    monkeypatch.setattr(
        provider_v3_module,
        "compute_dow_signal_facts",
        lambda *a, **k: {SignalKey.DOW_LAST_LOW_L: True},
    )

    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.DOW_LAST_LOW_HL not in signals
    assert SignalKey.HIGHER_LOW_CONFIRMED not in signals
    conn.close()


def test_structure_breakout_up_confirmed_wrapper_true(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=100.0)
    insert_rows(conn, rows)

    monkeypatch.setattr(
        provider_v3_module,
        "compute_dow_signal_facts",
        lambda *a, **k: {SignalKey.DOW_BOS_BREAK_UP: True},
    )

    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.DOW_BOS_BREAK_UP in signals
    assert SignalKey.STRUCTURE_BREAKOUT_UP_CONFIRMED in signals
    conn.close()


def test_structure_breakout_up_confirmed_wrapper_false(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV3(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required, close=100.0)
    insert_rows(conn, rows)

    monkeypatch.setattr(
        provider_v3_module,
        "compute_dow_signal_facts",
        lambda *a, **k: {SignalKey.DOW_LAST_LOW_HL: True},
    )

    signals = provider.get_signals("AAA", rows[-1][1]).signals
    assert SignalKey.DOW_BOS_BREAK_UP not in signals
    assert SignalKey.STRUCTURE_BREAKOUT_UP_CONFIRMED not in signals
    conn.close()
