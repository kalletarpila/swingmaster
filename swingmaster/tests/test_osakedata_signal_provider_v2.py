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


def insert_rows(conn, rows) -> None:
    conn.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def make_rows(
    ticker: str,
    start_date: date,
    count: int,
    close: float = 100.0,
    high_offset: float = 1.0,
    low_offset: float = 1.0,
):
    rows = []
    for i in range(count):
        day = start_date + timedelta(days=i)
        price = close
        high = price + high_offset
        low = price - low_offset
        rows.append((ticker, day.isoformat(), price, high, low, price, 1_000_000, "X"))
    return rows


def get_signals(provider, as_of_date: str):
    return set(provider.get_signals("AAA", as_of_date).signals.keys())


def get_signals_with_flag(conn, as_of_date: str, require_row_on_date: bool):
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata", require_row_on_date=require_row_on_date)
    return set(provider.get_signals("AAA", as_of_date).signals.keys())


def test_trend_started_v2():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, close=100.0)
    rows[-2] = (rows[-2][0], rows[-2][1], 101.0, 102.0, 100.0, 101.0, 1_000_000, "X")
    rows[-1] = (rows[-1][0], rows[-1][1], 102.0, 103.0, 101.0, 102.0, 1_000_000, "X")
    insert_rows(conn, rows)
    as_of_date = rows[-1][1]
    signals = get_signals(provider, as_of_date)
    assert SignalKey.TREND_STARTED in signals
    conn.close()


def test_trend_matured_v2():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, close=100.0)
    for i in range(1, 6):
        idx = -i
        rows[idx] = (rows[idx][0], rows[idx][1], 90.0, 91.0, 89.0, 90.0, 1_000_000, "X")
    insert_rows(conn, rows)
    as_of_date = rows[-1][1]
    signals = get_signals(provider, as_of_date)
    assert SignalKey.TREND_MATURED in signals
    conn.close()


def test_stabilization_confirmed_v2():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, close=100.0, high_offset=0.2, low_offset=0.2)
    insert_rows(conn, rows)
    as_of_date = rows[-1][1]
    signals = get_signals(provider, as_of_date)
    assert SignalKey.STABILIZATION_CONFIRMED in signals
    conn.close()


def test_entry_setup_valid_v2():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, close=100.0)
    for i in range(1, 6):
        idx = -i
        rows[idx] = (rows[idx][0], rows[idx][1], 100.0, 100.0, 99.0, 100.0, 1_000_000, "X")
    rows[-1] = (rows[-1][0], rows[-1][1], 105.0, 106.0, 104.0, 105.0, 1_000_000, "X")
    insert_rows(conn, rows)
    as_of_date = rows[-1][1]
    signals = get_signals(provider, as_of_date)
    assert SignalKey.ENTRY_SETUP_VALID in signals
    conn.close()


def test_invalidated_v2():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, close=100.0)
    for i in range(1, 11):
        idx = -i
        rows[idx] = (rows[idx][0], rows[idx][1], 100.0, 101.0, 95.0, 100.0, 1_000_000, "X")
    rows[-1] = (rows[-1][0], rows[-1][1], 100.0, 101.0, 90.0, 100.0, 1_000_000, "X")
    insert_rows(conn, rows)
    as_of_date = rows[-1][1]
    signals = get_signals(provider, as_of_date)
    assert SignalKey.INVALIDATED in signals
    conn.close()


def test_invalidated_not_triggered_on_equal_low():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, close=100.0)
    for i in range(1, 11):
        idx = -i
        rows[idx] = (rows[idx][0], rows[idx][1], 100.0, 101.0, 95.0, 100.0, 1_000_000, "X")
    rows[-1] = (rows[-1][0], rows[-1][1], 100.0, 101.0, 95.0, 100.0, 1_000_000, "X")
    insert_rows(conn, rows)
    as_of_date = rows[-1][1]
    signals = get_signals(provider, as_of_date)
    assert SignalKey.INVALIDATED not in signals
    conn.close()


def test_data_insufficient_v2():
    conn = setup_db()
    rows = make_rows("AAA", date(2026, 1, 1), 5, close=100.0)
    insert_rows(conn, rows)
    signals = get_signals(OsakeDataSignalProviderV2(conn, table_name="osakedata"), rows[-1][1])
    assert signals == {SignalKey.DATA_INSUFFICIENT}
    conn.close()


def test_require_row_on_date_blocks_signals_when_missing_day():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    rows = make_rows("AAA", date(2026, 1, 1), required + 5, close=100.0)
    insert_rows(conn, rows)
    missing_date = (date.fromisoformat(rows[-1][1]) + timedelta(days=1)).isoformat()
    # Without require_row_on_date: uses history up to prior day, should not mark insufficient
    signals_no_flag = get_signals_with_flag(conn, missing_date, require_row_on_date=False)
    assert SignalKey.DATA_INSUFFICIENT not in signals_no_flag
    # With require_row_on_date: no row on that date, must be insufficient
    signals_flag = get_signals_with_flag(conn, missing_date, require_row_on_date=True)
    assert signals_flag == {SignalKey.DATA_INSUFFICIENT}
    conn.close()


def test_no_signal_emitted_when_no_other_signals_and_not_insufficient():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, close=100.0, high_offset=10.0, low_offset=10.0)
    for i in range(1, 26):
        idx = -i
        rows[idx] = (rows[idx][0], rows[idx][1], 101.0, 111.0, 91.0, 101.0, 1_000_000, "X")
    insert_rows(conn, rows)
    as_of_date = rows[-1][1]
    signals = get_signals(provider, as_of_date)
    assert signals == {SignalKey.NO_SIGNAL}
    conn.close()


def test_no_signal_not_emitted_when_any_other_signal_present():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    required = provider._required_rows()
    base = date(2026, 1, 1)
    rows = make_rows("AAA", base, required, close=100.0)
    rows[-2] = (rows[-2][0], rows[-2][1], 101.0, 102.0, 100.0, 101.0, 1_000_000, "X")
    rows[-1] = (rows[-1][0], rows[-1][1], 102.0, 103.0, 101.0, 102.0, 1_000_000, "X")
    insert_rows(conn, rows)
    as_of_date = rows[-1][1]
    signals = get_signals(provider, as_of_date)
    assert SignalKey.NO_SIGNAL not in signals
    assert SignalKey.TREND_STARTED in signals
    conn.close()


def test_no_signal_not_emitted_when_data_insufficient():
    conn = setup_db()
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    rows = make_rows("AAA", date(2026, 1, 1), 5, close=100.0)
    insert_rows(conn, rows)
    signals = get_signals(provider, rows[-1][1])
    assert signals == {SignalKey.DATA_INSUFFICIENT}
    assert SignalKey.NO_SIGNAL not in signals
    conn.close()
