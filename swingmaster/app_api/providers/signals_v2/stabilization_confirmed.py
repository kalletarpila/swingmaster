from __future__ import annotations

from typing import Callable

from .context import SignalContextV2

BASELINE_WINDOW = 20
STAB_WINDOW = 7
NO_NEW_LOW_WINDOW = 7
SIGNIFICANT_LOW_EPS = 0.003
RANGE_SHRINK_RATIO = 0.75
WIDE_DAY_MULT = 1.5
WIDE_DAY_RATIO_MAX = 0.20
CLOSE_UPPER_FRAC_MIN = 0.55
CLOSE_UPPER_DAYS_MIN = 3
SWEEP_MAX_COUNT = 1

_TINY = 1e-12


def eval_stabilization_confirmed(
    ctx: SignalContextV2,
    atr_window: int,
    stabilization_days: int,
    atr_pct_threshold: float,
    range_pct_threshold: float,
    compute_atr: Callable[[list[tuple]], float],
) -> bool:
    return _eval_stabilization_confirmed(
        ctx, atr_window, stabilization_days, atr_pct_threshold, range_pct_threshold, compute_atr
    )[0]


def eval_stabilization_confirmed_debug(
    ctx: SignalContextV2,
    atr_window: int,
    stabilization_days: int,
    atr_pct_threshold: float,
    range_pct_threshold: float,
    compute_atr: Callable[[list[tuple]], float],
) -> tuple[bool, str]:
    result, debug_info = _eval_stabilization_confirmed(
        ctx, atr_window, stabilization_days, atr_pct_threshold, range_pct_threshold, compute_atr
    )
    return result, debug_info


def _eval_stabilization_confirmed(
    ctx: SignalContextV2,
    atr_window: int,
    stabilization_days: int,
    atr_pct_threshold: float,
    range_pct_threshold: float,
    compute_atr: Callable[[list[tuple]], float],
) -> tuple[bool, str]:
    _ = (atr_window, stabilization_days, atr_pct_threshold, range_pct_threshold, compute_atr)
    if not _has_required_data(ctx):
        return False, "DEBUG_STABILIZATION insufficient_data=True result=False"

    closes = ctx.closes
    highs = ctx.highs
    lows = ctx.lows

    recent_range = _range_pct_series(highs[:STAB_WINDOW], lows[:STAB_WINDOW], closes[:STAB_WINDOW])
    baseline_range = _range_pct_series(
        highs[STAB_WINDOW : STAB_WINDOW + BASELINE_WINDOW],
        lows[STAB_WINDOW : STAB_WINDOW + BASELINE_WINDOW],
        closes[STAB_WINDOW : STAB_WINDOW + BASELINE_WINDOW],
    )
    if not recent_range or not baseline_range:
        return False, "DEBUG_STABILIZATION insufficient_data=True result=False"

    baseline_median = _median(baseline_range)
    recent_median = _median(recent_range)
    if baseline_median is None or recent_median is None:
        return False, "DEBUG_STABILIZATION insufficient_data=True result=False"

    range_shrink_ok = recent_median <= baseline_median * RANGE_SHRINK_RATIO

    wide_days = 0
    for r in recent_range:
        if r >= baseline_median * WIDE_DAY_MULT:
            wide_days += 1
    wide_days_ratio = wide_days / float(STAB_WINDOW)
    wide_days_ok = wide_days_ratio <= WIDE_DAY_RATIO_MAX

    significant_new_low_count = 0
    sweep_count = 0
    first_new_low_d = None
    first_new_low = None
    first_new_low_ref = None
    first_new_low_threshold = None
    for d in range(STAB_WINDOW):
        ref_low = min(lows[d + 1 : d + 1 + NO_NEW_LOW_WINDOW])
        low_d = lows[d]
        if low_d < ref_low * (1 - SIGNIFICANT_LOW_EPS):
            significant_new_low_count += 1
            if first_new_low_d is None:
                first_new_low_d = d
                first_new_low = low_d
                first_new_low_ref = ref_low
                first_new_low_threshold = ref_low * (1 - SIGNIFICANT_LOW_EPS)
        elif ref_low * (1 - SIGNIFICANT_LOW_EPS) <= low_d < ref_low:
            sweep_count += 1
    no_new_low_ok = significant_new_low_count == 0 and sweep_count <= SWEEP_MAX_COUNT

    upper_closes = 0
    for d in range(STAB_WINDOW):
        pos = _close_pos(closes[d], highs[d], lows[d])
        if pos >= CLOSE_UPPER_FRAC_MIN:
            upper_closes += 1
    upper_closes_ok = upper_closes >= CLOSE_UPPER_DAYS_MIN

    first_failed = None
    if not range_shrink_ok:
        first_failed = "range_shrink"
    elif not wide_days_ok:
        first_failed = "wide_days"
    elif not no_new_low_ok:
        first_failed = "no_new_low"
    elif not upper_closes_ok:
        first_failed = "upper_closes"

    result = range_shrink_ok and wide_days_ok and no_new_low_ok and upper_closes_ok
    debug_info = (
        "DEBUG_STABILIZATION "
        f"stab_recent_median={recent_median:.6f} "
        f"stab_baseline_median={baseline_median:.6f} "
        f"stab_range_shrink_ok={range_shrink_ok} "
        f"stab_wide_days={wide_days} "
        f"stab_wide_days_ratio={wide_days_ratio:.6f} "
        f"stab_wide_days_ok={wide_days_ok} "
        f"stab_significant_new_low_count={significant_new_low_count} "
        f"stab_sweep_count={sweep_count} "
        f"stab_no_new_low_ok={no_new_low_ok} "
        f"stab_upper_closes={upper_closes} "
        f"stab_upper_closes_ok={upper_closes_ok} "
        f"stab_first_failed={first_failed} "
        f"stab_final={result}"
    )
    if significant_new_low_count > 0:
        debug_info += (
            f" stab_first_new_low_d={first_new_low_d} "
            f"stab_first_new_low={first_new_low} "
            f"stab_ref_low={first_new_low_ref} "
            f"stab_new_low_threshold={first_new_low_threshold}"
        )
    return result, debug_info


def _has_required_data(ctx: SignalContextV2) -> bool:
    needed = BASELINE_WINDOW + STAB_WINDOW + NO_NEW_LOW_WINDOW
    if len(ctx.closes) < needed:
        return False
    if len(ctx.highs) < needed or len(ctx.lows) < needed:
        return False
    return True


def _range_pct_series(highs: list[float], lows: list[float], closes: list[float]) -> list[float]:
    out: list[float] = []
    for h, l, c in zip(highs, lows, closes):
        out.append((h - l) / max(c, _TINY))
    return out


def _close_pos(close: float, high: float, low: float) -> float:
    return (close - low) / max(high - low, _TINY)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    vals = sorted(values)
    mid = len(vals) // 2
    if len(vals) % 2 == 1:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0
