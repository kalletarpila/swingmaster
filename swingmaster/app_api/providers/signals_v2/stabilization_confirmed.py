from __future__ import annotations

from typing import Callable

from .context import SignalContextV2


def eval_stabilization_confirmed(
    ctx: SignalContextV2,
    atr_window: int,
    stabilization_days: int,
    atr_pct_threshold: float,
    range_pct_threshold: float,
    compute_atr: Callable[[list[tuple]], float],
) -> bool:
    if len(ctx.ohlc) >= max(atr_window + 1, stabilization_days + 1):
        atr = compute_atr(ctx.ohlc[: atr_window + 1])
        latest_close = ctx.closes[0]
        atr_pct = atr / latest_close if latest_close else 0.0
        window_high = max(ctx.highs[:stabilization_days])
        window_low = min(ctx.lows[:stabilization_days])
        range_pct = (window_high - window_low) / latest_close if latest_close else 0.0
        return atr_pct <= atr_pct_threshold and range_pct <= range_pct_threshold
    return False
