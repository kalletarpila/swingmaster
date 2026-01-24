"""Regression tests for DATA_INSUFFICIENT gating in osakedata signal provider v2."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

from swingmaster.app_api.providers.osakedata_signal_provider_v2 import OsakeDataSignalProviderV2
from swingmaster.core.signals.enums import SignalKey


def setup_db():
    conn = sqlite3.connect(":memory:")
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
            market TEXT DEFAULT 'usa',
            UNIQUE(osake, pvm)
        )
        """
    )
    return conn


def make_rows(ticker: str, start_day: date, count: int, price: float = 100.0):
    rows = []
    for i in range(count):
        day = start_day + timedelta(days=i)
        rows.append((ticker, day.isoformat(), price, price + 1, price - 1, price, 1_000_000))
    return rows


def insert_rows(conn, rows) -> None:
    conn.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def test_missing_asof_row_with_require_row_on_date() -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata", require_row_on_date=True)
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, price=100.0)
    insert_rows(conn, rows)
    missing_date = (base + timedelta(days=required)).isoformat()
    signals = provider.get_signals("AAA", missing_date).signals
    assert set(signals.keys()) == {SignalKey.DATA_INSUFFICIENT}
    conn.close()


def test_insufficient_history_rows() -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required - 1, price=100.0)
    insert_rows(conn, rows)
    as_of_date = (base + timedelta(days=required - 2)).isoformat()
    signals = provider.get_signals("AAA", as_of_date).signals
    assert set(signals.keys()) == {SignalKey.DATA_INSUFFICIENT}
    conn.close()


def test_sufficient_history_no_data_insufficient() -> None:
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata", require_row_on_date=True)
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, price=100.0)
    insert_rows(conn, rows)
    as_of_date = (base + timedelta(days=required - 1)).isoformat()
    signals = provider.get_signals("AAA", as_of_date).signals
    assert SignalKey.DATA_INSUFFICIENT not in signals
    conn.close()
