from __future__ import annotations

from .context import SignalContextV2

SMA_LEN = 20
STRUCT_WINDOW = 15
NEW_LOW_LOOKBACK = 10
DRAW_REF_LOOKBACK_A = 20
DRAW_REF_LOOKBACK_B = 5
DRAW_MIN_DD = 0.10
MIN_AGE_DAYS = 10
MOMENTUM_WINDOW = 20
MOMENTUM_NEWLOW_COUNT = 3
MOMENTUM_DROP_MAX = 0.02


def eval_trend_matured(ctx: SignalContextV2, sma_window: int, matured_below_sma_days: int) -> bool:
    _ = (sma_window, matured_below_sma_days)
    closes = ctx.closes
    max_index = max(
        MIN_AGE_DAYS - 1,
        STRUCT_WINDOW - 1,
        MOMENTUM_WINDOW - 1,
        DRAW_REF_LOOKBACK_A,
    )
    min_required = max(
        SMA_LEN + max_index + 1,
        STRUCT_WINDOW + NEW_LOW_LOOKBACK,
        MOMENTUM_WINDOW + NEW_LOW_LOOKBACK,
        DRAW_REF_LOOKBACK_A + 1,
    )
    if len(closes) < min_required:
        return False

    sma20 = _sma_series(closes, SMA_LEN)
    if sma20 is None:
        return False

    new_lows = 0
    for i in range(STRUCT_WINDOW):
        if _is_new_low(closes, i, NEW_LOW_LOOKBACK):
            new_lows += 1
    structure_new_lows = new_lows >= 2

    ref_slice = closes[DRAW_REF_LOOKBACK_B : DRAW_REF_LOOKBACK_A + 1]
    if not ref_slice:
        return False
    ref_high = max(ref_slice)
    if ref_high <= 0:
        return False
    drawdown = (ref_high - closes[0]) / ref_high
    structure_drawdown = drawdown >= DRAW_MIN_DD

    structure_ok = structure_new_lows or structure_drawdown

    below_ma_days = 0
    for i in range(MIN_AGE_DAYS):
        if closes[i] < sma20[i]:
            below_ma_days += 1
    time_ok = below_ma_days >= _ceil_ratio(MIN_AGE_DAYS, 0.70)

    new_low_indices = []
    for i in range(MOMENTUM_WINDOW):
        if _is_new_low(closes, i, NEW_LOW_LOOKBACK):
            new_low_indices.append(i)
    if len(new_low_indices) < MOMENTUM_NEWLOW_COUNT:
        return False
    chron_indices = sorted(new_low_indices, reverse=True)
    last_three = chron_indices[-MOMENTUM_NEWLOW_COUNT:]
    l1, l2, l3 = (closes[i] for i in last_three)
    if l1 <= 0 or l2 <= 0:
        return False
    momentum_ok = (
        abs(l2 - l1) / l1 <= MOMENTUM_DROP_MAX
        and abs(l3 - l2) / l2 <= MOMENTUM_DROP_MAX
    )

    return structure_ok and time_ok and momentum_ok


def _sma_series(closes: list[float], window: int) -> list[float] | None:
    if window <= 0:
        return None
    if len(closes) < window:
        return None
    out: list[float] = []
    for i in range(len(closes) - window + 1):
        out.append(sum(closes[i : i + window]) / float(window))
    return out


def _is_new_low(closes: list[float], idx: int, lookback: int) -> bool:
    if idx + lookback >= len(closes):
        return False
    prior = closes[idx + 1 : idx + 1 + lookback]
    if not prior:
        return False
    return closes[idx] < min(prior)


def _ceil_ratio(n: int, ratio: float) -> int:
    if n <= 0:
        return 0
    raw = n * ratio
    return int(raw) if raw.is_integer() else int(raw) + 1
