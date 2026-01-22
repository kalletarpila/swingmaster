from __future__ import annotations

import sqlite3

from swingmaster.app_api.providers.osakedata_signal_provider_v2 import OsakeDataSignalProviderV2
from swingmaster.core.signals.enums import SignalKey


def setup_db(rows):
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
    conn.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return conn


def make_rows(ticker: str, start_date: int, prices: list[float], high_offset=0.5, low_offset=0.5):
    rows = []
    for i, close in enumerate(prices):
        day = start_date + i
        date = f"2026-01-{day:02d}"
        high = close + high_offset
        low = close - low_offset
        rows.append((ticker, date, close, high, low, close, 1_000_000, "X"))
    return rows


def get_signals(conn, date: str):
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata")
    return set(provider.get_signals("AAA", date).signals.keys())


def get_signals_with_flag(conn, date: str, require_row_on_date: bool):
    provider = OsakeDataSignalProviderV2(conn, table_name="osakedata", require_row_on_date=require_row_on_date)
    return set(provider.get_signals("AAA", date).signals.keys())


def test_trend_started_v2():
    prices = [100 + i for i in range(25)]
    rows = make_rows("AAA", 1, prices)
    conn = setup_db(rows)
    signals = get_signals(conn, "2026-01-25")
    assert SignalKey.TREND_STARTED in signals
    conn.close()


def test_trend_matured_v2():
    prices = [200 - i for i in range(25)]
    rows = make_rows("AAA", 1, prices)
    conn = setup_db(rows)
    signals = get_signals(conn, "2026-01-25")
    assert SignalKey.TREND_MATURED in signals
    conn.close()


def test_stabilization_confirmed_v2():
    # Enough history then tight range last days
    prices = [120 - i * 0.2 for i in range(20)] + [100 for _ in range(10)]
    rows = make_rows("AAA", 1, prices, high_offset=0.2, low_offset=0.2)
    conn = setup_db(rows)
    signals = get_signals(conn, "2026-01-30")
    assert SignalKey.STABILIZATION_CONFIRMED in signals
    conn.close()


def test_entry_setup_valid_v2():
    prices = [100 for _ in range(24)] + [106]
    highs = [101 for _ in range(24)] + [106.5]
    lows = [99 for _ in range(24)] + [105]
    rows = []
    for i in range(25):
        day = i + 1
        date = f"2026-02-{day:02d}"
        close = prices[i]
        rows.append(("AAA", date, close, highs[i], lows[i], close, 1_000_000, "X"))
    conn = setup_db(rows)
    signals = get_signals(conn, "2026-02-25")
    assert SignalKey.ENTRY_SETUP_VALID in signals
    conn.close()


def test_invalidated_v2():
    prices = [100 for _ in range(20)] + [95, 94, 93, 92, 90]
    lows = [p - 1 for p in prices]
    highs = [p + 1 for p in prices]
    rows = []
    for i, close in enumerate(prices):
        day = i + 1
        date = f"2026-03-{day:02d}"
        rows.append(("AAA", date, close, highs[i], lows[i], close, 1_000_000, "X"))
    conn = setup_db(rows)
    signals = get_signals(conn, "2026-03-25")
    assert SignalKey.INVALIDATED in signals
    conn.close()


def test_invalidated_not_triggered_on_equal_low():
    prices = [100 for _ in range(20)] + [95, 94, 93, 92, 90]
    lows = [p - 1 for p in prices]
    # Make today's low equal to prior min low (no new low)
    lows[-1] = min(lows[:-1])
    highs = [p + 1 for p in prices]
    rows = []
    for i, close in enumerate(prices):
        day = i + 1
        date = f"2026-03-{day:02d}"
        rows.append(("AAA", date, close, highs[i], lows[i], close, 1_000_000, "X"))
    conn = setup_db(rows)
    signals = get_signals(conn, "2026-03-25")
    assert SignalKey.INVALIDATED not in signals
    conn.close()


def test_data_insufficient_v2():
    prices = [100 + i for i in range(5)]
    rows = make_rows("AAA", 1, prices)
    conn = setup_db(rows)
    signals = get_signals(conn, "2026-01-05")
    assert signals == {SignalKey.DATA_INSUFFICIENT}
    conn.close()


def test_require_row_on_date_blocks_signals_when_missing_day():
    prices = [100 + i for i in range(40)]
    rows = []
    for i, close in enumerate(prices):
        day = i + 1
        date = f"2026-01-{day:02d}"
        rows.append(("AAA", date, close, close + 1, close - 1, close, 1_000_000, "X"))
    conn = setup_db(rows)
    # Without require_row_on_date: uses history up to prior day, should not mark insufficient
    signals_no_flag = get_signals_with_flag(conn, "2026-01-41", require_row_on_date=False)
    assert SignalKey.DATA_INSUFFICIENT not in signals_no_flag
    # With require_row_on_date: no row on that date, must be insufficient
    signals_flag = get_signals_with_flag(conn, "2026-01-41", require_row_on_date=True)
    assert signals_flag == {SignalKey.DATA_INSUFFICIENT}
    conn.close()
