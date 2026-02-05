from __future__ import annotations

from swingmaster.app_api.providers.signals_v2.context import SignalContextV2
from swingmaster.app_api.providers.signals_v2.entry_setup_valid import (
    ATR_LEN,
    BASE_WINDOW,
    SMA_LEN,
    eval_entry_setup_valid,
)


def _ctx_from_chron(data: list[tuple[float, float, float]]) -> SignalContextV2:
    highs = [h for _c, h, _l in data]
    lows = [l for _c, _h, l in data]
    closes = [c for c, _h, _l in data]
    closes_desc = list(reversed(closes))
    highs_desc = list(reversed(highs))
    lows_desc = list(reversed(lows))
    return SignalContextV2(closes=closes_desc, highs=highs_desc, lows=lows_desc, ohlc=[])


def _eval_ctx(ctx: SignalContextV2) -> bool:
    return eval_entry_setup_valid(ctx, stabilization_days=0, entry_sma_window=0)


def _baseline_block(count: int, close: float, range_size: float) -> list[tuple[float, float, float]]:
    out = []
    for _ in range(count):
        low = close - range_size / 2.0
        high = close + range_size / 2.0
        out.append((close, high, low))
    return out


def test_entry_setup_base_range_valid():
    base = _baseline_block(BASE_WINDOW, close=100.0, range_size=4.0)
    extra = _baseline_block(max(SMA_LEN, ATR_LEN), close=100.0, range_size=4.0)
    ctx = _ctx_from_chron(extra + base)
    assert _eval_ctx(ctx) is True


def test_entry_setup_base_range_fails_risk():
    base = _baseline_block(BASE_WINDOW, close=100.0, range_size=0.2)
    extra = _baseline_block(max(SMA_LEN, ATR_LEN), close=100.0, range_size=0.2)
    base[0] = (100.0, 100.1, 98.0)
    ctx = _ctx_from_chron(extra + base)
    assert _eval_ctx(ctx) is False


def test_entry_setup_reclaim_ma20_valid():
    # build a gentle down period to keep SMA above, then reclaim
    chron = _baseline_block(SMA_LEN + ATR_LEN + 5, close=100.0, range_size=6.0)
    chron[-2] = (98.0, 100.0, 95.0)  # yesterday below SMA
    chron[-1] = (106.5, 107.0, 103.0)  # today above SMA, close high in range
    ctx = _ctx_from_chron(chron)
    assert _eval_ctx(ctx) is True


def test_entry_setup_reclaim_ma20_fails_close_pos():
    chron = _baseline_block(SMA_LEN + ATR_LEN + 5, close=100.0, range_size=6.0)
    chron[-2] = (98.0, 100.0, 95.0)
    chron[-1] = (103.1, 107.0, 103.0)  # close near low
    ctx = _ctx_from_chron(chron)
    assert _eval_ctx(ctx) is False


def test_entry_setup_support_fails():
    base = _baseline_block(BASE_WINDOW, close=100.0, range_size=4.0)
    extra = _baseline_block(max(SMA_LEN, ATR_LEN), close=100.0, range_size=4.0)
    base[-1] = (100.0, 102.0, 99.0)
    base[-2] = (93.0, 96.0, 95.0)
    base[0] = (100.0, 102.0, 95.0)
    ctx = _ctx_from_chron(extra + base)
    assert _eval_ctx(ctx) is False


def test_entry_setup_insufficient_data():
    ctx = _ctx_from_chron([(100.0, 101.0, 99.0)] * 5)
    assert _eval_ctx(ctx) is False
