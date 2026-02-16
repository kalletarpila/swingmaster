"""Signal: SHARP_SELL_OFF_DETECTED.

Definition:
  - ATR14 relative-volatility thresholds against 1-day or 3-day return.
  - One day return <= -(2.5 * atr_pct) OR three day return <= -(3.5 * atr_pct).
"""

from __future__ import annotations

from typing import Callable, List, Tuple

from .context import SignalContextV3


ATR_LEN = 14
ONE_DAY_MULT = 2.5
THREE_DAY_MULT = 3.5


def eval_sharp_sell_off_detected(
    ctx: SignalContextV3,
    compute_atr: Callable[[List[Tuple[str, float, float, float, float, float]], int], float | None],
) -> bool:
    closes = ctx.closes
    if len(closes) < 4:
        return False
    c_t0 = closes[0]
    c_t1 = closes[1]
    c_t3 = closes[3]
    if c_t0 is None or c_t1 is None or c_t3 is None:
        return False
    if c_t0 <= 0 or c_t1 <= 0 or c_t3 <= 0:
        return False

    atr14 = compute_atr(ctx.ohlc, ATR_LEN)
    if atr14 is None:
        return False

    atr_pct = atr14 / c_t0
    if atr_pct <= 0:
        return False

    one_day_return = (c_t0 / c_t1) - 1.0
    three_day_return = (c_t0 / c_t3) - 1.0

    return (
        one_day_return <= -(ONE_DAY_MULT * atr_pct)
        or three_day_return <= -(THREE_DAY_MULT * atr_pct)
    )
