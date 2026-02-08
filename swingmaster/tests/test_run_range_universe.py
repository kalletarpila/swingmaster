"""Tests for run range universe."""

from __future__ import annotations

import sqlite3

from swingmaster.cli.run_range_universe import build_trading_days


def setup_md(rows):
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


def test_trading_days_scoped_to_selected_tickers():
    rows = [
        ("AAA", "2026-01-01", 1, 2, 0.5, 1.5, 100),
        ("AAA", "2026-01-02", 2, 3, 1.5, 2.5, 110),
        ("BBB", "2026-01-02", 5, 6, 4.5, 5.5, 200),
    ]
    conn = setup_md(rows)
    days_bbb = build_trading_days(conn, ["BBB"], "2026-01-01", "2026-01-03")
    assert days_bbb == ["2026-01-02"]
    days_all = build_trading_days(conn, ["AAA", "BBB"], "2026-01-01", "2026-01-03")
    assert days_all == ["2026-01-01", "2026-01-02"]
    conn.close()
