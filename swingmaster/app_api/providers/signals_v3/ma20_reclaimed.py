"""Signal: MA20_RECLAIMED.

Definition:
  - Reclaim event where close crosses from at/below SMA20 to above SMA20.
"""

from __future__ import annotations

from .context import SignalContextV3


def eval_ma20_reclaimed(ctx: SignalContextV3, window: int = 20) -> bool:
    closes = ctx.closes
    if len(closes) < window + 1:
        return False

    needed = closes[: window + 1]
    for value in needed:
        if value is None or value <= 0:
            return False

    sma_t0 = sum(closes[:window]) / float(window)
    sma_t1 = sum(closes[1 : window + 1]) / float(window)

    return closes[0] > sma_t0 and closes[1] <= sma_t1

