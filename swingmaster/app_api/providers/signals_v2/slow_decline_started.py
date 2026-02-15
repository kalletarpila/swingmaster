"""Signal: SLOW_DECLINE_STARTED.

Definition:
  - Staircase decline over lookbacks t-10 > t-5 > t-2 > t0.
  - Total decline from t-10 to t0 must meet min_decline_percent.
  - Optional MA filter at t0: close[t0] < MA10 and MA5 < MA10.
"""

from __future__ import annotations

from .context import SignalContextV2


LOOKBACK_LONG_DAYS = 10
MA_SHORT = 5
MA_LONG = 10


def eval_slow_decline_started(
    ctx: SignalContextV2,
    min_decline_percent: float = 4.5,
    use_ma_filter: bool = True,
) -> bool:
    closes = ctx.closes
    min_required = max(LOOKBACK_LONG_DAYS + 1, MA_LONG)
    if len(closes) < min_required:
        return False

    idx_t0 = 0
    idx_t2 = 2
    idx_t5 = 5
    idx_t10 = LOOKBACK_LONG_DAYS

    c_t0 = closes[idx_t0]
    c_t2 = closes[idx_t2]
    c_t5 = closes[idx_t5]
    c_t10 = closes[idx_t10]

    if c_t0 is None or c_t2 is None or c_t5 is None or c_t10 is None:
        return False
    if c_t10 <= 0:
        return False

    staircase_ok = c_t10 > c_t5 > c_t2 > c_t0
    if not staircase_ok:
        return False

    decline_pct = ((c_t10 - c_t0) / c_t10) * 100.0
    if decline_pct < min_decline_percent:
        return False

    if not use_ma_filter:
        return True

    ma10_window = closes[:MA_LONG]
    ma5_window = closes[:MA_SHORT]
    if any(v is None for v in ma10_window) or any(v is None for v in ma5_window):
        return False

    ma10 = sum(ma10_window) / float(MA_LONG)
    ma5 = sum(ma5_window) / float(MA_SHORT)
    return c_t0 < ma10 and ma5 < ma10
