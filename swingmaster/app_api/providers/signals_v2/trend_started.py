"""Signal: TREND_STARTED.

Category:
  - Trend / structure breakdown.

Contract:
  - Inputs: close series (most recent first).
  - Output: boolean signal.
  - Determinism: must not depend on policy state or history.

Trigger summary:
  - Requires sufficient history for SMA20 and regime windows.
  - Regime must be above SMA20 with positive SMA slope.
  - Requires a cross from above to below SMA today vs. yesterday.
  - Debounces if any recent day was already below SMA.
  - Confirms a breakdown below the prior BREAK_LOW_WINDOW low.

Edge cases:
  - Returns False on insufficient history or invalid SMA window.

Complexity:
  - O(N) over fixed windows, O(1) extra space.
"""

from __future__ import annotations

from .context import SignalContextV2


SMA_LEN = 20
SLOPE_LOOKBACK = 5
REGIME_WINDOW = 30
ABOVE_RATIO_MIN = 0.70
BREAK_LOW_WINDOW = 10
DEBOUNCE_DAYS = 0


def eval_trend_started(ctx: SignalContextV2, sma_window: int, momentum_lookback: int) -> bool:
    _ = (sma_window, momentum_lookback)
    closes = ctx.closes
    min_required = max(
        SMA_LEN + REGIME_WINDOW - 1,
        SMA_LEN + SLOPE_LOOKBACK,
        SMA_LEN + DEBOUNCE_DAYS + 1,
        BREAK_LOW_WINDOW + 1,
    )
    if len(closes) < min_required:
        return False

    sma20 = _sma_series(closes, SMA_LEN)
    if sma20 is None:
        return False

    above_count = 0
    for i in range(REGIME_WINDOW):
        if closes[i] > sma20[i]:
            above_count += 1
    above_ratio = above_count / float(REGIME_WINDOW)
    sma20_slope = sma20[0] - sma20[SLOPE_LOOKBACK]
    regime_ok = above_ratio >= ABOVE_RATIO_MIN and sma20_slope > 0

    yesterday_close = closes[1]
    yesterday_sma = sma20[1]
    today_close = closes[0]
    today_sma = sma20[0]

    if not (yesterday_close >= yesterday_sma and today_close < today_sma):
        return False

    # Debounce: avoid repeated triggers if we were already below SMA recently.
    for i in range(1, 1 + DEBOUNCE_DAYS + 1):
        if closes[i] < sma20[i]:
            return False

    prev_low = min(closes[1 : 1 + BREAK_LOW_WINDOW])
    breakdown_ok = today_close < prev_low

    return regime_ok and breakdown_ok


def _sma_series(closes: list[float], window: int) -> list[float] | None:
    if window <= 0:
        return None
    if len(closes) < window:
        return None
    out: list[float] = []
    for i in range(len(closes) - window + 1):
        out.append(sum(closes[i : i + window]) / float(window))
    return out
