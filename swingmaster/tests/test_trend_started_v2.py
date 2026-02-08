"""Tests for trend started v2."""

from __future__ import annotations

from swingmaster.app_api.providers.signals_v2.context import SignalContextV2
from swingmaster.app_api.providers.signals_v2.trend_started import eval_trend_started


def _make_ctx(closes: list[float]) -> SignalContextV2:
    return SignalContextV2(closes=closes, highs=closes, lows=closes, ohlc=[])


def _base_closes(length: int = 80) -> list[float]:
    # Desc-ordered closes (index 0 = today) with an upward regime.
    return [200.0 - (i * 0.5) for i in range(length)]


def test_trend_started_fires_on_fresh_cross_down():
    closes = _base_closes()
    closes[0] = 160.0  # fresh cross-down and breakdown
    ctx = _make_ctx(closes)
    assert eval_trend_started(ctx, sma_window=20, momentum_lookback=1) is True


def test_trend_started_debounced_when_recent_below_sma():
    closes = _base_closes()
    closes[0] = 160.0  # fresh cross-down and breakdown
    closes[3] = 192.0  # recent day below SMA but still above today's close
    ctx = _make_ctx(closes)
    assert eval_trend_started(ctx, sma_window=20, momentum_lookback=1) is False
