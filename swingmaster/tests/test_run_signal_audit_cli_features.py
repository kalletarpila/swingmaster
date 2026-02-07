from __future__ import annotations

import sys
from dataclasses import dataclass

import pytest

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


def _run(monkeypatch, argv: list[str], *, tickers: list[str], days: list[str], provider) -> str:
    from swingmaster.cli import run_signal_audit as mod

    monkeypatch.setattr(mod, "get_readonly_connection", lambda _p: _DummyConn())
    monkeypatch.setattr(mod, "resolve_tickers", lambda _c, _m, _t, _mx: tickers)
    monkeypatch.setattr(mod, "build_trading_days", lambda _c, _t, _a, _b: days)
    monkeypatch.setattr(mod, "build_swingmaster_app", lambda *_a, **_kw: _FakeApp(provider))

    monkeypatch.setattr(sys, "argv", ["run_signal_audit.py", *argv])
    mod.main()
    return ""


def test_streaks_computes_runs_and_suppresses_day_rows(monkeypatch, capsys):
    days = [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
        "2026-01-05",
        "2026-01-06",
    ]
    tickers = ["AAA"]
    focus = SignalKey.ENTRY_SETUP_VALID
    by = {
        ("AAA", days[0]): _ss(focus),
        ("AAA", days[1]): _ss(focus),
        ("AAA", days[3]): _ss(focus),
        ("AAA", days[4]): _ss(focus),
        ("AAA", days[5]): _ss(focus),
    }
    provider = _FakeSignalProvider(by)

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            days[0],
            "--end-date",
            days[-1],
            "--ticker",
            "AAA",
            "--focus-signal",
            focus.name,
            "--streaks",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert "STREAKS SUMMARY" in out
    assert "STREAKS ticker=AAA runs_total=2 max_run_len=3 avg_run_len=2.50" in out
    assert "STREAKS_AGG streaks_tickers_with_runs=1 streaks_runs_total=2 streaks_max_run_len_overall=3" in out
    assert "TICKER " not in out


def test_first_hit_only_prints_only_first_day_of_each_run(monkeypatch, capsys):
    days = [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
        "2026-01-05",
        "2026-01-06",
    ]
    tickers = ["AAA"]
    focus = SignalKey.ENTRY_SETUP_VALID
    by = {
        ("AAA", days[0]): _ss(focus),
        ("AAA", days[1]): _ss(focus),
        ("AAA", days[3]): _ss(focus),
        ("AAA", days[4]): _ss(focus),
        ("AAA", days[5]): _ss(focus),
    }
    provider = _FakeSignalProvider(by)

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            days[0],
            "--end-date",
            days[-1],
            "--ticker",
            "AAA",
            "--focus-signal",
            focus.name,
            "--print-focus-only",
            "--first-hit-only",
            "--summary",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert "TICKER AAA DATE 2026-01-01" in out
    assert "TICKER AAA DATE 2026-01-04" in out
    assert "TICKER AAA DATE 2026-01-02" not in out
    assert "TICKER AAA DATE 2026-01-05" not in out
    assert "focus_match_days_total=2" in out
    assert "focus_first_match_date=2026-01-01" in out
    assert "focus_last_match_date=2026-01-04" in out
    assert "focus_first_match_days_from_anchor=None" in out
    assert "focus_last_match_days_from_anchor=None" in out


@pytest.mark.parametrize(
    ("anchor_mode", "expected_dates", "expected_anchor"),
    [
        ("first", {"2026-01-03", "2026-01-04"}, "2026-01-02"),
        ("last", {"2026-01-06"}, "2026-01-05"),
    ],
)
def test_anchor_mode_first_vs_last(monkeypatch, capsys, anchor_mode, expected_dates, expected_anchor):
    days = [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
        "2026-01-05",
        "2026-01-06",
    ]
    tickers = ["AAA"]
    after = SignalKey.TREND_STARTED
    focus = SignalKey.ENTRY_SETUP_VALID
    by = {
        ("AAA", days[1]): _ss(after),
        ("AAA", days[2]): _ss(focus),
        ("AAA", days[3]): _ss(focus),
        ("AAA", days[4]): _ss(after),
        ("AAA", days[5]): _ss(focus),
    }
    provider = _FakeSignalProvider(by)

    argv = [
        "--market",
        "OMXH",
        "--begin-date",
        days[0],
        "--end-date",
        days[-1],
        "--ticker",
        "AAA",
        "--focus-signal",
        focus.name,
        "--after-signal",
        after.name,
        "--window-days",
        "2",
        "--print-focus-only",
        "--summary",
    ]
    if anchor_mode == "first":
        argv += ["--anchor-mode", "first"]

    _run(monkeypatch, argv, tickers=tickers, days=days, provider=provider)
    out = capsys.readouterr().out

    printed_dates = set()
    for line in out.splitlines():
        if line.startswith("TICKER AAA DATE "):
            printed_dates.add(line.split()[-1])
    assert printed_dates == expected_dates
    assert f"after_signal_anchor_date={expected_anchor}" in out


def test_debug_show_mismatches_prints_require_miss_lines_only_when_enabled(monkeypatch, capsys):
    days = ["2026-01-01"]
    tickers = ["AAA"]
    focus = SignalKey.ENTRY_SETUP_VALID
    provider = _FakeSignalProvider({("AAA", days[0]): _ss(focus)})

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            days[0],
            "--end-date",
            days[0],
            "--ticker",
            "AAA",
            "--focus-signal",
            focus.name,
            "--print-focus-only",
            "--require-signal",
            SignalKey.INVALIDATED.name,
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert "MISMATCH REQUIRE_MISS" not in out

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            days[0],
            "--end-date",
            days[0],
            "--ticker",
            "AAA",
            "--focus-signal",
            focus.name,
            "--print-focus-only",
            "--require-signal",
            SignalKey.INVALIDATED.name,
            "--debug-show-mismatches",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    assert out.splitlines()[0].startswith("MISMATCH REQUIRE_MISS ")


def test_debug_show_mismatches_respects_debug_limit(monkeypatch, capsys):
    days = ["2026-01-01", "2026-01-02"]
    tickers = ["AAA"]
    focus = SignalKey.ENTRY_SETUP_VALID
    provider = _FakeSignalProvider(
        {
            ("AAA", days[0]): _ss(focus),
            ("AAA", days[1]): _ss(focus),
        }
    )

    _run(
        monkeypatch,
        [
            "--market",
            "OMXH",
            "--begin-date",
            days[0],
            "--end-date",
            days[-1],
            "--ticker",
            "AAA",
            "--focus-signal",
            focus.name,
            "--print-focus-only",
            "--require-signal",
            SignalKey.INVALIDATED.name,
            "--debug-show-mismatches",
            "--debug-limit",
            "1",
        ],
        tickers=tickers,
        days=days,
        provider=provider,
    )
    out = capsys.readouterr().out
    mismatch_lines = [ln for ln in out.splitlines() if ln.startswith("MISMATCH REQUIRE_MISS ")]
    assert len(mismatch_lines) == 1
