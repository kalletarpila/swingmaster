from __future__ import annotations

from .context import SignalContextV2


def eval_entry_setup_valid(ctx: SignalContextV2, stabilization_days: int, entry_sma_window: int) -> bool:
    if len(ctx.ohlc) >= max(stabilization_days + 1, entry_sma_window):
        latest_close = ctx.closes[0]
        prev_highs = ctx.highs[1 : 1 + stabilization_days]
        if prev_highs:
            breakout_level = max(prev_highs)
            sma_entry = sum(ctx.closes[:entry_sma_window]) / float(entry_sma_window)
            return latest_close > breakout_level and latest_close > sma_entry
    return False
