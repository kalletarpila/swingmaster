from __future__ import annotations

import sys
from dataclasses import dataclass

from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet


def _ss(*keys: SignalKey) -> SignalSet:
    return SignalSet(
        signals={k: Signal(key=k, value=True, confidence=None, source="test") for k in keys}
    )


@dataclass
class _DummyConn:
    def close(self) -> None:  # pragma: no cover
        return


class _FakeSignalProvider:
    def __init__(self, by_ticker_day: dict[tuple[str, str], SignalSet]) -> None:
        self._m = by_ticker_day

    def get_signals(self, ticker: str, date: str) -> SignalSet:
        return self._m.get((ticker, date), SignalSet(signals={}))


@dataclass
class _FakeApp:
    _signal_provider: object
    _policy: object = None
    _prev_state_provider: object = None


def _run(monkeypatch, argv: list[str], *, tickers: list[str], days: list[str], provider) -> None:
    from swingmaster.cli import run_signal_audit as mod

    monkeypatch.setattr(mod, "get_readonly_connection", lambda _p: _DummyConn())
    monkeypatch.setattr(mod, "resolve_tickers", lambda _c, _m, _t, _mx: tickers)
    monkeypatch.setattr(mod, "build_trading_days", lambda _c, _t, _a, _b: days)
    monkeypatch.setattr(mod, "build_swingmaster_app", lambda *_a, **_kw: _FakeApp(provider))

    monkeypatch.setattr(sys, "argv", ["run_signal_audit.py", *argv])
    mod.main()


def test_debug_ohlcv_disabled_prints_no_debug_block(monkeypatch, capsys):
    day = "2026-01-20"
    tickers = ["AAA"]
    days = [day]
    provider = _FakeSignalProvider({("AAA", day): _ss(SignalKey.ENTRY_SETUP_VALID)})

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            day,
            "--end-date",
            day,
            "--ticker",
            "AAA",
            "--focus-signal",
            "ENTRY_SETUP_VALID",
            "--print-focus-only",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert "DEBUG_OHLCV" not in out


def test_debug_ohlcv_enabled_prints_rows_and_limits_window(monkeypatch, capsys):
    day = "2026-01-20"
    tickers = ["AAA"]
    days = [day]
    provider = _FakeSignalProvider({("AAA", day): _ss(SignalKey.ENTRY_SETUP_VALID)})

    from swingmaster.cli import run_signal_audit as mod

    # Build 25 days of OHLC tuples (DESC by date), but debug window should print <= 20 rows.
    ohlc = []
    # closes[0]=90, closes[1..24]=100 so sma20[0] = 99.5 and new_low at idx 0 is True.
    for i in range(25):
        d = f"2026-01-{20 - i:02d}"
        c = 90.0 if i == 0 else 100.0
        ohlc.append((d, c, c, c, c, 1.0))

    monkeypatch.setattr(mod, "_load_debug_ohlcv_window", lambda *_a, **_kw: ohlc)

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            day,
            "--end-date",
            day,
            "--ticker",
            "AAA",
            "--focus-signal",
            "ENTRY_SETUP_VALID",
            "--print-focus-only",
            "--debug-ohlcv",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )

    out = capsys.readouterr().out
    assert "DEBUG_OHLCV (window=20)" in out
    assert "DEBUG_OHLCV_ROW date=2026-01-20 close=90.0" in out
    assert "below_sma=Y" in out
    assert "new_low=Y" in out

    row_lines = [ln for ln in out.splitlines() if ln.startswith("DEBUG_OHLCV_ROW ")]
    assert len(row_lines) <= 20


def test_trend_matured_debug_line_prints_only_when_debug_enabled(monkeypatch, capsys):
    day = "2026-01-20"
    tickers = ["AAA"]
    days = [day]
    provider = _FakeSignalProvider({("AAA", day): _ss(SignalKey.TREND_MATURED)})

    from swingmaster.cli import run_signal_audit as mod

    ohlc = []
    for i in range(60):
        d = f"2026-01-{20 - i:02d}"
        c = 100.0 - i * 0.1
        ohlc.append((d, c, c, c, c, 1.0))
    monkeypatch.setattr(mod, "_load_debug_ohlcv_window", lambda *_a, **_kw: ohlc)

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            day,
            "--end-date",
            day,
            "--ticker",
            "AAA",
            "--focus-signal",
            "TREND_MATURED",
            "--print-focus-only",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert "TREND_MATURED_DEBUG" not in out

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            day,
            "--end-date",
            day,
            "--ticker",
            "AAA",
            "--focus-signal",
            "TREND_MATURED",
            "--print-focus-only",
            "--debug-ohlcv",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert "TREND_MATURED_DEBUG " in out


def test_stabilization_debug_line_prints_only_when_debug_enabled(monkeypatch, capsys):
    day = "2026-01-20"
    tickers = ["AAA"]
    days = [day]
    provider = _FakeSignalProvider({("AAA", day): _ss(SignalKey.STABILIZATION_CONFIRMED)})

    from swingmaster.cli import run_signal_audit as mod

    ohlc = []
    for i in range(40):
        d = f"2026-01-{20 - i:02d}"
        c = 100.0
        ohlc.append((d, c, c, c, c, 1.0))
    monkeypatch.setattr(mod, "_load_debug_ohlcv_window", lambda *_a, **_kw: ohlc)

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            day,
            "--end-date",
            day,
            "--ticker",
            "AAA",
            "--focus-signal",
            "STABILIZATION_CONFIRMED",
            "--print-focus-only",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert "DEBUG_STABILIZATION" not in out

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            day,
            "--end-date",
            day,
            "--ticker",
            "AAA",
            "--focus-signal",
            "STABILIZATION_CONFIRMED",
            "--print-focus-only",
            "--debug-ohlcv",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert "DEBUG_STABILIZATION " in out
