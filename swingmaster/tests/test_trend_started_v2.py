from __future__ import annotations

from swingmaster.app_api.providers.signals_v2.context import SignalContextV2
from swingmaster.app_api.providers.signals_v2.trend_started import eval_trend_started


def _ctx_from_closes_desc(closes_desc: list[float]) -> SignalContextV2:
    return SignalContextV2(closes=closes_desc, highs=closes_desc, lows=closes_desc, ohlc=[])


def _eval_at(closes_desc: list[float], idx: int) -> bool:
    ctx = _ctx_from_closes_desc(closes_desc[idx:])
    return eval_trend_started(ctx, sma_window=20, momentum_lookback=1)


def _make_chronological(start: float, count: int, step: float) -> list[float]:
    return [start + i * step for i in range(count)]


def test_trend_started_fires_once_on_breakdown():
    chron = _make_chronological(100.0, 60, 1.0)
    chron[-1] = 140.0  # breakdown day
    closes_desc = list(reversed(chron))
    assert _eval_at(closes_desc, 0) is True
    for idx in range(1, 6):
        assert _eval_at(closes_desc, idx) is False


def test_trend_started_does_not_repeat_after_breakdown():
    chron = _make_chronological(100.0, 60, 1.0)
    chron.extend([130.0, 129.0, 128.0, 127.0, 126.0])
    closes_desc = list(reversed(chron))
    assert _eval_at(closes_desc, 4) is True  # breakdown day
    for idx in range(0, 4):
        assert _eval_at(closes_desc, idx) is False


def test_trend_started_false_without_regime():
    chron = [100.0] * 60
    chron[-1] = 90.0  # crossdown + break low, but no rising regime
    closes_desc = list(reversed(chron))
    assert _eval_at(closes_desc, 0) is False


def test_trend_started_false_without_breaking_low():
    chron = _make_chronological(100.0, 60, 1.0)
    chron[-6] = 130.0  # lower prev-10 min
    chron[-1] = 140.0  # crossdown without breaking low
    closes_desc = list(reversed(chron))
    assert _eval_at(closes_desc, 0) is False
