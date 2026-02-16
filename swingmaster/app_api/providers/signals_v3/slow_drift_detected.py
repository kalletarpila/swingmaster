"""Signal: SLOW_DRIFT_DETECTED.

Definition:
  - Staircase decline over lookbacks t-10 > t-5 > t-2 > t0.
  - Ten-day decline must be at least 3%.
  - SMA5 < SMA10 and close[t0] < SMA10.
"""

from __future__ import annotations

from .context import SignalContextV3


LOOKBACK_LONG_DAYS = 10
MA_SHORT = 5
MA_LONG = 10
MIN_DECLINE = -0.03


def eval_slow_drift_detected(ctx: SignalContextV3) -> bool:
    closes = ctx.closes
    if len(closes) < 11:
        return False

    c_t0 = closes[0]
    c_t2 = closes[2]
    c_t5 = closes[5]
    c_t10 = closes[10]

    if c_t0 is None or c_t2 is None or c_t5 is None or c_t10 is None:
        return False
    if c_t10 <= 0:
        return False

    if not (c_t10 > c_t5 > c_t2 > c_t0):
        return False

    decline = (c_t0 / c_t10) - 1.0
    if decline > MIN_DECLINE:
        return False

    ma10_window = closes[:MA_LONG]
    ma5_window = closes[:MA_SHORT]
    if any(v is None for v in ma10_window) or any(v is None for v in ma5_window):
        return False

    ma10 = sum(ma10_window) / float(MA_LONG)
    ma5 = sum(ma5_window) / float(MA_SHORT)
    return ma5 < ma10 and c_t0 < ma10
