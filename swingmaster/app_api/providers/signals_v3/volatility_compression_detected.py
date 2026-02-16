"""Signal: VOLATILITY_COMPRESSION_DETECTED.

Definition:
  - Compare ATR14/close at t0 against t-5, t-10, and rolling max over t-19..t0.
  - Emits true only when volatility is compressed versus recent history.
"""

from __future__ import annotations

from typing import Callable, List, Tuple

from .context import SignalContextV3


ATR_LEN = 14
ROLLING_WINDOW = 20
OFFSET_T5 = 5
OFFSET_T10 = 10
DEFAULT_COMPRESSION_RATIO = 0.75


def eval_volatility_compression_detected(
    ctx: SignalContextV3,
    compute_atr: Callable[[List[Tuple[str, float, float, float, float, float]], int], float | None],
    compression_ratio: float = DEFAULT_COMPRESSION_RATIO,
) -> bool:
    closes = ctx.closes
    ohlc = ctx.ohlc

    min_required = (ROLLING_WINDOW - 1) + ATR_LEN + 1
    if len(closes) < min_required or len(ohlc) < min_required:
        return False

    atr_pct_values: List[float] = []
    for offset in range(ROLLING_WINDOW):
        close_val = closes[offset]
        if close_val is None or close_val <= 0:
            return False

        atr_val = compute_atr(ohlc[offset:], ATR_LEN)
        if atr_val is None:
            return False

        atr_pct = atr_val / close_val
        if atr_pct <= 0:
            return False
        atr_pct_values.append(atr_pct)

    atr_t0 = atr_pct_values[0]
    atr_t5 = atr_pct_values[OFFSET_T5]
    atr_t10 = atr_pct_values[OFFSET_T10]
    rolling_max = max(atr_pct_values)

    return (
        atr_t0 < atr_t5
        and atr_t0 < atr_t10
        and atr_t0 <= (compression_ratio * rolling_max)
    )
