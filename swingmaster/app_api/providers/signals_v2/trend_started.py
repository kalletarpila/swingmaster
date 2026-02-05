from __future__ import annotations

from .context import SignalContextV2


SMA_LEN = 20
SLOPE_LOOKBACK = 5
REGIME_WINDOW = 30
ABOVE_RATIO_MIN = 0.70
BREAK_LOW_WINDOW = 10
DEBOUNCE_DAYS = 5


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
