"""Tests for stabilization confirmed v2."""

from __future__ import annotations

from swingmaster.app_api.providers.signals_v2.context import SignalContextV2
from swingmaster.app_api.providers.signals_v2.stabilization_confirmed import (
    BASELINE_WINDOW,
    NO_NEW_LOW_WINDOW,
    STAB_WINDOW,
    eval_stabilization_confirmed,
)


def _ctx_from_chron(data: list[tuple[float, float, float]]) -> SignalContextV2:
    highs = [h for _c, h, _l in data]
    lows = [l for _c, _h, l in data]
    closes = [c for c, _h, _l in data]
    closes_desc = list(reversed(closes))
    highs_desc = list(reversed(highs))
    lows_desc = list(reversed(lows))
    return SignalContextV2(closes=closes_desc, highs=highs_desc, lows=lows_desc, ohlc=[])


def _build_baseline_block(count: int, close: float, range_size: float) -> list[tuple[float, float, float]]:
    out = []
    for _ in range(count):
        low = close - range_size / 2.0
        high = close + range_size / 2.0
        out.append((close, high, low))
    return out


def _build_recent_block(
    count: int,
    close: float,
    range_size: float,
    low_override: list[float] | None = None,
    close_pos: list[float] | None = None,
) -> list[tuple[float, float, float]]:
    out = []
    for i in range(count):
        low = close - range_size / 2.0
        high = close + range_size / 2.0
        if low_override is not None:
            low = low_override[i]
            high = low + range_size
        if close_pos is not None:
            c = low + (high - low) * close_pos[i]
        else:
            c = close
        out.append((c, high, low))
    return out


def _eval_ctx(ctx: SignalContextV2) -> bool:
    return eval_stabilization_confirmed(ctx, 0, 0, 0.0, 0.0, lambda _: 0.0)


def test_stabilization_confirmed_true():
    baseline = _build_baseline_block(BASELINE_WINDOW, close=100.0, range_size=10.0)
    recent_lows = [95.0, 95.0, 94.8, 95.0, 95.0, 95.0, 95.0]
    close_pos = [0.7, 0.7, 0.7, 0.4, 0.4, 0.4, 0.7]
    recent = _build_recent_block(STAB_WINDOW, close=98.0, range_size=4.0, low_override=recent_lows, close_pos=close_pos)
    tail = _build_baseline_block(NO_NEW_LOW_WINDOW, close=100.0, range_size=10.0)
    ctx = _ctx_from_chron(tail + baseline + recent)
    assert _eval_ctx(ctx) is True


def test_stabilization_confirmed_fails_new_low():
    baseline = _build_baseline_block(BASELINE_WINDOW, close=100.0, range_size=10.0)
    recent_lows = [94.5, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0]
    close_pos = [0.7] * STAB_WINDOW
    recent = _build_recent_block(STAB_WINDOW, close=98.0, range_size=4.0, low_override=recent_lows, close_pos=close_pos)
    tail = _build_baseline_block(NO_NEW_LOW_WINDOW, close=100.0, range_size=10.0)
    ctx = _ctx_from_chron(tail + baseline + recent)
    assert _eval_ctx(ctx) is False


def test_stabilization_confirmed_fails_volatility():
    baseline = _build_baseline_block(BASELINE_WINDOW, close=100.0, range_size=10.0)
    recent_lows = [95.0] * STAB_WINDOW
    close_pos = [0.7] * STAB_WINDOW
    recent = _build_recent_block(STAB_WINDOW, close=98.0, range_size=9.0, low_override=recent_lows, close_pos=close_pos)
    tail = _build_baseline_block(NO_NEW_LOW_WINDOW, close=100.0, range_size=10.0)
    ctx = _ctx_from_chron(tail + baseline + recent)
    assert _eval_ctx(ctx) is False


def test_stabilization_confirmed_fails_close_behavior():
    baseline = _build_baseline_block(BASELINE_WINDOW, close=100.0, range_size=10.0)
    recent_lows = [95.0] * STAB_WINDOW
    close_pos = [0.2] * STAB_WINDOW
    recent = _build_recent_block(STAB_WINDOW, close=98.0, range_size=4.0, low_override=recent_lows, close_pos=close_pos)
    tail = _build_baseline_block(NO_NEW_LOW_WINDOW, close=100.0, range_size=10.0)
    ctx = _ctx_from_chron(tail + baseline + recent)
    assert _eval_ctx(ctx) is False


def test_stabilization_confirmed_sweep_limit():
    baseline = _build_baseline_block(BASELINE_WINDOW, close=110.0, range_size=20.0)
    recent_lows_ok = [99.8, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]
    close_pos = [0.7] * STAB_WINDOW
    recent_ok = _build_recent_block(STAB_WINDOW, close=102.0, range_size=4.0, low_override=recent_lows_ok, close_pos=close_pos)
    tail = _build_baseline_block(NO_NEW_LOW_WINDOW, close=110.0, range_size=20.0)
    ctx_ok = _ctx_from_chron(tail + baseline + recent_ok)
    assert _eval_ctx(ctx_ok) is True

    recent_lows_fail = [99.8, 100.0, 100.0, 100.0, 100.0, 100.0, 99.75]
    recent_fail = _build_recent_block(STAB_WINDOW, close=102.0, range_size=4.0, low_override=recent_lows_fail, close_pos=close_pos)
    ctx_fail = _ctx_from_chron(tail + baseline + recent_fail)
    assert _eval_ctx(ctx_fail) is False


def test_stabilization_confirmed_insufficient_data():
    ctx = _ctx_from_chron([(100.0, 101.0, 99.0)] * 10)
    assert _eval_ctx(ctx) is False
