"""Tests for MA20_RECLAIMED in v3."""

from __future__ import annotations

from swingmaster.app_api.providers.signals_v3.context import SignalContextV3
from swingmaster.app_api.providers.signals_v3.ma20_reclaimed import eval_ma20_reclaimed


def _ctx(closes: list[float]) -> SignalContextV3:
    highs = [c + 1.0 for c in closes]
    lows = [c - 1.0 for c in closes]
    ohlc = []
    return SignalContextV3(closes=closes, highs=highs, lows=lows, ohlc=ohlc)


def test_ma20_reclaimed_true() -> None:
    closes = [102.0, 100.0] + [100.0] * 19
    assert len(closes) == 21
    assert eval_ma20_reclaimed(_ctx(closes)) is True


def test_ma20_reclaimed_false() -> None:
    closes = [102.0, 101.0] + [100.0] * 19
    assert len(closes) == 21
    assert eval_ma20_reclaimed(_ctx(closes)) is False


def test_ma20_reclaimed_insufficient_data_false() -> None:
    closes = [100.0] * 20
    assert eval_ma20_reclaimed(_ctx(closes)) is False

