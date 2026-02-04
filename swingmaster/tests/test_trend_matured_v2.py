from __future__ import annotations

from swingmaster.app_api.providers.signals_v2.context import SignalContextV2
from swingmaster.app_api.providers.signals_v2.trend_matured import eval_trend_matured


def _ctx_from_chron(closes_chron: list[float]) -> SignalContextV2:
    closes_desc = list(reversed(closes_chron))
    return SignalContextV2(closes=closes_desc, highs=closes_desc, lows=closes_desc, ohlc=[])


def test_trend_matured_true():
    closes = [120.0 - i * 0.3 for i in range(60)]
    ctx = _ctx_from_chron(closes)
    assert eval_trend_matured(ctx, sma_window=20, matured_below_sma_days=5) is True


def test_trend_matured_fails_momentum():
    closes = [120.0 - i * 0.3 for i in range(57)]
    closes.extend([95.0, 85.0, 75.0])
    ctx = _ctx_from_chron(closes)
    assert eval_trend_matured(ctx, sma_window=20, matured_below_sma_days=5) is False


def test_trend_matured_fails_time_proxy():
    closes = [120.0 - i * 0.4 for i in range(40)]
    closes.extend([100.0, 99.6, 99.2, 98.8, 98.4])
    closes.extend([110.0] * 10)
    closes.extend([109.0] * 5)
    ctx = _ctx_from_chron(closes)
    assert eval_trend_matured(ctx, sma_window=20, matured_below_sma_days=5) is False


def test_trend_matured_fails_structure():
    closes = [105.0] * 40
    closes.extend([104.0, 103.5, 103.0])  # new lows in last 20 but outside last 15
    closes.extend([103.2] * 17)
    ctx = _ctx_from_chron(closes)
    assert eval_trend_matured(ctx, sma_window=20, matured_below_sma_days=5) is False


def test_trend_matured_insufficient_data():
    closes = [100.0, 99.5, 99.0, 98.5]
    ctx = _ctx_from_chron(closes)
    assert eval_trend_matured(ctx, sma_window=20, matured_below_sma_days=5) is False
