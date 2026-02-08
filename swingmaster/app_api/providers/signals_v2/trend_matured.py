"""Signal: TREND_MATURED.

Category:
  - Trend / structure progression.

Contract:
  - Inputs: close series (most recent first).
  - Output: boolean signal.
  - Determinism: must not depend on policy state or history.

Trigger summary:
  - Requires sufficient history for SMA20, structure, momentum, and drawdown windows.
  - Structure passes if enough recent new lows OR drawdown from a reference high.
  - Time passes if a majority of the last MIN_AGE_DAYS closes are below SMA20.
  - Momentum passes if last three new lows are spaced with limited percent drop.
  - Fires only when structure, time, and momentum all pass.

Edge cases:
  - Returns False on insufficient history or invalid SMA window.

Complexity:
  - O(N) over fixed windows, O(1) extra space.
"""

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
    return _eval_trend_matured(ctx, sma_window, matured_below_sma_days)[0]


def eval_trend_matured_debug(
    ctx: SignalContextV2, sma_window: int, matured_below_sma_days: int
) -> tuple[bool, str]:
    result, debug_info = _eval_trend_matured(ctx, sma_window, matured_below_sma_days)
    return result, debug_info


def _min_required() -> int:
    max_index = max(
        MIN_AGE_DAYS - 1,
        STRUCT_WINDOW - 1,
        MOMENTUM_WINDOW - 1,
        DRAW_REF_LOOKBACK_A,
    )
    return max(
        SMA_LEN + max_index + 1,
        STRUCT_WINDOW + NEW_LOW_LOOKBACK,
        MOMENTUM_WINDOW + NEW_LOW_LOOKBACK,
        DRAW_REF_LOOKBACK_A + 1,
    )


def _eval_trend_matured(
    ctx: SignalContextV2, sma_window: int, matured_below_sma_days: int
) -> tuple[bool, str]:
    _ = (sma_window, matured_below_sma_days)
    closes = ctx.closes
    min_required = _min_required()
    if len(closes) < min_required:
        return False, "TREND_MATURED_DEBUG insufficient_data=True result=False"

    sma20 = _sma_series(closes, SMA_LEN)
    if sma20 is None:
        return False, "TREND_MATURED_DEBUG insufficient_data=True result=False"

    new_lows = 0
    for i in range(STRUCT_WINDOW):
        if _is_new_low(closes, i, NEW_LOW_LOOKBACK):
            new_lows += 1
    structure_new_lows = new_lows >= 2

    ref_slice = closes[DRAW_REF_LOOKBACK_B : DRAW_REF_LOOKBACK_A + 1]
    if not ref_slice:
        return False, "TREND_MATURED_DEBUG insufficient_data=True result=False"
    ref_high = max(ref_slice)
    if ref_high <= 0:
        return False, "TREND_MATURED_DEBUG insufficient_data=True result=False"
    drawdown = (ref_high - closes[0]) / ref_high
    structure_drawdown = drawdown >= DRAW_MIN_DD

    structure_ok = structure_new_lows or structure_drawdown

    below_ma_days = 0
    for i in range(MIN_AGE_DAYS):
        if closes[i] < sma20[i]:
            below_ma_days += 1
    required_days = _ceil_ratio(MIN_AGE_DAYS, 0.70)
    time_ok = below_ma_days >= required_days

    new_low_indices = []
    for i in range(MOMENTUM_WINDOW):
        if _is_new_low(closes, i, NEW_LOW_LOOKBACK):
            new_low_indices.append(i)
    if len(new_low_indices) < MOMENTUM_NEWLOW_COUNT:
        debug_info = (
            "TREND_MATURED_DEBUG "
            f"structure_new_lows_count={new_lows} structure_new_lows_ok={structure_new_lows} "
            f"structure_drawdown_ref_high={ref_high} structure_drawdown_close0={closes[0]} "
            f"structure_drawdown={drawdown} structure_drawdown_ok={structure_drawdown} "
            f"time_below_ma_days={below_ma_days} time_required_days={required_days} time_ok={time_ok} "
            f"momentum_new_low_count={len(new_low_indices)} momentum_indices=[] "
            "momentum_l1=None momentum_l2=None momentum_l3=None "
            "momentum_step1_pct=None momentum_step2_pct=None momentum_ok=False "
            "result=False"
        )
        return False, debug_info
    chron_indices = sorted(new_low_indices, reverse=True)
    last_three = chron_indices[-MOMENTUM_NEWLOW_COUNT:]
    l1, l2, l3 = (closes[i] for i in last_three)
    if l1 <= 0 or l2 <= 0:
        debug_info = (
            "TREND_MATURED_DEBUG "
            f"structure_new_lows_count={new_lows} structure_new_lows_ok={structure_new_lows} "
            f"structure_drawdown_ref_high={ref_high} structure_drawdown_close0={closes[0]} "
            f"structure_drawdown={drawdown} structure_drawdown_ok={structure_drawdown} "
            f"time_below_ma_days={below_ma_days} time_required_days={required_days} time_ok={time_ok} "
            f"momentum_new_low_count={len(new_low_indices)} momentum_indices={last_three} "
            f"momentum_l1={l1} momentum_l2={l2} momentum_l3={l3} "
            "momentum_step1_pct=None momentum_step2_pct=None momentum_ok=False "
            "result=False"
        )
        return False, debug_info
    step1_pct = abs(l2 - l1) / l1
    step2_pct = abs(l3 - l2) / l2
    momentum_ok = (
        step1_pct <= MOMENTUM_DROP_MAX and step2_pct <= MOMENTUM_DROP_MAX
    )

    result = structure_ok and time_ok and momentum_ok
    debug_info = (
        "TREND_MATURED_DEBUG "
        f"structure_new_lows_count={new_lows} structure_new_lows_ok={structure_new_lows} "
        f"structure_drawdown_ref_high={ref_high} structure_drawdown_close0={closes[0]} "
        f"structure_drawdown={drawdown} structure_drawdown_ok={structure_drawdown} "
        f"time_below_ma_days={below_ma_days} time_required_days={required_days} time_ok={time_ok} "
        f"momentum_new_low_count={len(new_low_indices)} momentum_indices={last_three} "
        f"momentum_l1={l1} momentum_l2={l2} momentum_l3={l3} "
        f"momentum_step1_pct={step1_pct} momentum_step2_pct={step2_pct} "
        f"momentum_ok={momentum_ok} result={result}"
    )
    return result, debug_info


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
