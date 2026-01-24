from __future__ import annotations

from .context import SignalContextV2


def eval_trend_matured(ctx: SignalContextV2, sma_window: int, matured_below_sma_days: int) -> bool:
    if len(ctx.closes) >= max(sma_window, matured_below_sma_days):
        sma_today = sum(ctx.closes[:sma_window]) / float(sma_window)
        last_below = all(c < sma_today for c in ctx.closes[:matured_below_sma_days])
        slope_neg = False
        if len(ctx.closes) >= sma_window + 5:
            sma_5ago = sum(ctx.closes[5 : 5 + sma_window]) / float(sma_window)
            slope_neg = sma_today < sma_5ago
        return last_below or slope_neg
    return False
