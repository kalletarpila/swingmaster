"""Tests for run range universe."""

from __future__ import annotations

import argparse
import sqlite3

import pytest

import swingmaster.cli.run_range_universe as run_range_universe
from swingmaster.cli.run_range_universe import build_trading_days, filter_tickers_with_row_on_date


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


def test_filter_tickers_with_row_on_date_scoped_to_asof_day():
    rows = [
        ("AAA", "2026-01-01", 1, 2, 0.5, 1.5, 100),
        ("AAA", "2026-01-02", 2, 3, 1.5, 2.5, 110),
        ("BBB", "2026-01-02", 5, 6, 4.5, 5.5, 200),
    ]
    conn = setup_md(rows)
    assert filter_tickers_with_row_on_date(conn, ["AAA", "BBB"], "2026-01-01") == ["AAA"]
    assert filter_tickers_with_row_on_date(conn, ["AAA", "BBB"], "2026-01-02") == ["AAA", "BBB"]
    conn.close()


class _StopAfterBuild(RuntimeError):
    pass


class _FakeUniverseReader:
    def __init__(self, _conn: sqlite3.Connection) -> None:
        pass

    def resolve_tickers(self, _spec) -> list[str]:
        return ["AAA"]

    def filter_by_osakedata(self, **kwargs):
        return kwargs["tickers"]


def _memory_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_range_run_forces_require_row_on_date(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        date_from="2026-01-01",
        date_to="2026-01-02",
        md_db="unused",
        rc_db="unused",
        mode="market",
        tickers=None,
        market="OMXH",
        sector=None,
        industry=None,
        limit=1,
        sample="first_n",
        seed=1,
        min_history_rows=0,
        require_row_on_date=False,
        max_days=1,
        dry_run=False,
        policy_id="rule_v2",
        policy_version="v3",
        signal_version="v3",
        debug=False,
        debug_limit=0,
        debug_show_tickers=False,
        debug_show_mismatches=False,
        print_signals=False,
        print_signals_limit=0,
        report=False,
        ew_score=False,
        ew_score_rule="EW_SCORE_DAY3_V1_FIN",
        osakedata_db="unused",
    )

    monkeypatch.setattr(run_range_universe, "parse_args", lambda: args)
    monkeypatch.setattr(run_range_universe, "get_readonly_connection", lambda _path: _memory_conn())
    monkeypatch.setattr(run_range_universe, "get_connection", lambda _path: _memory_conn())
    monkeypatch.setattr(run_range_universe, "apply_migrations", lambda _conn: None)
    monkeypatch.setattr(run_range_universe, "TickerUniverseReader", _FakeUniverseReader)
    monkeypatch.setattr(run_range_universe, "build_trading_days", lambda *a, **k: ["2026-01-02"])
    monkeypatch.setattr(run_range_universe, "ensure_rc_pipeline_episode_table", lambda _conn: None)

    def _stop_build(*_a, **kwargs):
        assert kwargs["require_row_on_date"] is True
        raise _StopAfterBuild()

    monkeypatch.setattr(run_range_universe, "build_swingmaster_app", _stop_build)

    with pytest.raises(_StopAfterBuild):
        run_range_universe.main()


def test_range_run_filters_day_tickers_before_run_daily(monkeypatch: pytest.MonkeyPatch) -> None:
    args = argparse.Namespace(
        date_from="2026-01-01",
        date_to="2026-01-02",
        md_db="unused",
        rc_db="unused",
        mode="market",
        tickers=None,
        market="OMXH",
        sector=None,
        industry=None,
        limit=2,
        sample="first_n",
        seed=1,
        min_history_rows=0,
        require_row_on_date=False,
        max_days=1,
        dry_run=False,
        policy_id="rule_v2",
        policy_version="v3",
        signal_version="v3",
        debug=False,
        debug_limit=0,
        debug_show_tickers=False,
        debug_show_mismatches=False,
        print_signals=False,
        print_signals_limit=0,
        report=False,
        ew_score=False,
        ew_score_rule="EW_SCORE_DAY3_V1_FIN",
        osakedata_db="unused",
    )

    md_conn = setup_md(
        [
            ("AAA", "2026-01-02", 1, 2, 0.5, 1.5, 100),
        ]
    )

    class _UniverseReaderTwoTickers:
        def __init__(self, _conn: sqlite3.Connection) -> None:
            pass

        def resolve_tickers(self, _spec) -> list[str]:
            return ["AAA", "BBB"]

        def filter_by_osakedata(self, **kwargs):
            return kwargs["tickers"]

    class _FakeApp:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[str]]] = []
            self._signal_provider = object()

        def run_daily(self, as_of_date: str, tickers: list[str]) -> str:
            self.calls.append((as_of_date, tickers))
            raise _StopAfterBuild()

    fake_app = _FakeApp()

    monkeypatch.setattr(run_range_universe, "parse_args", lambda: args)
    monkeypatch.setattr(run_range_universe, "get_readonly_connection", lambda _path: md_conn)
    monkeypatch.setattr(run_range_universe, "get_connection", lambda _path: _memory_conn())
    monkeypatch.setattr(run_range_universe, "apply_migrations", lambda _conn: None)
    monkeypatch.setattr(run_range_universe, "TickerUniverseReader", _UniverseReaderTwoTickers)
    monkeypatch.setattr(run_range_universe, "build_trading_days", lambda *a, **k: ["2026-01-02"])
    monkeypatch.setattr(run_range_universe, "ensure_rc_pipeline_episode_table", lambda _conn: None)
    monkeypatch.setattr(run_range_universe, "build_swingmaster_app", lambda *_a, **_k: fake_app)

    with pytest.raises(_StopAfterBuild):
        run_range_universe.main()

    assert fake_app.calls == [("2026-01-02", ["AAA"])]
    md_conn.close()
