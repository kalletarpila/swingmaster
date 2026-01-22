from __future__ import annotations

import sqlite3

from swingmaster.infra.market_data.osakedata_reader import (
    OsakeDataReader,
    ensure_osakedata_indexes,
)


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
            volume REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO osakedata (osake, pvm, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return conn


def test_n_validation():
    conn = setup_db([])
    reader = OsakeDataReader(conn)
    for method in (reader.get_last_n_closes, reader.get_last_n_ohlc, reader.get_last_n_ohlc_required):
        try:
            method("AAA", "2026-01-01", 0)
        except ValueError:
            pass
        else:
            assert False, "Expected ValueError for n<=0"
    conn.close()


def test_blank_ticker_or_date():
    conn = setup_db([])
    reader = OsakeDataReader(conn)
    for ticker, date in [("", "2026-01-01"), ("AAA", ""), (" ", "2026-01-01")]:
        try:
            reader.get_last_n_closes(ticker, date, 1)
        except ValueError:
            continue
        assert False, "Expected ValueError for blank inputs"
    conn.close()


def test_require_row_on_date_behavior():
    rows = [
        ("AAA", "2026-01-01", 1, 2, 0.5, 1.5, 100),
        ("AAA", "2026-01-02", 2, 3, 1.5, 2.5, 110),
    ]
    conn = setup_db(rows)
    reader = OsakeDataReader(conn)
    res_missing = reader.get_last_n_ohlc_required("AAA", "2026-01-03", 2, require_row_on_date=True)
    assert res_missing == []
    res_present = reader.get_last_n_ohlc_required("AAA", "2026-01-02", 2, require_row_on_date=True)
    assert len(res_present) == 2
    assert res_present[0][0] == "2026-01-02"
    conn.close()


def test_list_trading_days_sorted_distinct():
    rows = [
        ("AAA", "2026-01-02", 1, 2, 1, 1.5, 100),
        ("BBB", "2026-01-01", 1, 2, 1, 1.5, 100),
        ("AAA", "2026-01-03", 1, 2, 1, 1.5, 100),
        ("BBB", "2026-01-02", 1, 2, 1, 1.5, 100),
    ]
    conn = setup_db(rows)
    reader = OsakeDataReader(conn)
    days = reader.list_trading_days("2026-01-01", "2026-01-03")
    assert days == ["2026-01-01", "2026-01-02", "2026-01-03"]
    conn.close()


def test_ensure_indexes_created():
    rows = [("AAA", "2026-01-01", 1, 2, 1, 1.5, 100)]
    conn = setup_db(rows)
    ensure_osakedata_indexes(conn)
    idxs = conn.execute("PRAGMA index_list(osakedata)").fetchall()
    names = {row[1] for row in idxs}
    assert f"idx_osakedata_osake_pvm" in names
    assert f"idx_osakedata_pvm" in names
    conn.close()
