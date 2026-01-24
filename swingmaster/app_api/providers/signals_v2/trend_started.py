from __future__ import annotations

from .context import SignalContextV2


def eval_trend_started(ctx: SignalContextV2, sma_window: int, momentum_lookback: int) -> bool:
    if len(ctx.closes) > momentum_lookback and len(ctx.closes) >= sma_window:
        latest = ctx.closes[0]
        prev = ctx.closes[momentum_lookback]
        sma = sum(ctx.closes[:sma_window]) / float(sma_window)
        return latest > prev and latest > sma
    return False
