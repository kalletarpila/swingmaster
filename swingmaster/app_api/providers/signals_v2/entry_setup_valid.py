from __future__ import annotations

from .context import SignalContextV2

_DEBUG_ENTRY_SETUP_VALID = False
_LAST_ENTRY_SETUP_VALID_DEBUG = None


def set_entry_setup_valid_debug(enabled: bool) -> None:
    global _DEBUG_ENTRY_SETUP_VALID
    _DEBUG_ENTRY_SETUP_VALID = enabled


def _debug_print(msg: str) -> None:
    if _DEBUG_ENTRY_SETUP_VALID:
        print(msg)


def get_entry_setup_valid_debug() -> str | None:
    return _LAST_ENTRY_SETUP_VALID_DEBUG


def eval_entry_setup_valid(ctx: SignalContextV2, stabilization_days: int, entry_sma_window: int) -> bool:
    _ = (stabilization_days, entry_sma_window)
    closes = ctx.closes
    highs = ctx.highs
    lows = ctx.lows
    if len(closes) < _min_required_len():
        global _LAST_ENTRY_SETUP_VALID_DEBUG
        _LAST_ENTRY_SETUP_VALID_DEBUG = None
        return False
    if len(highs) < _min_required_len() or len(lows) < _min_required_len():
        _LAST_ENTRY_SETUP_VALID_DEBUG = None
        return False

    base_ok, base_invalidation = _base_range(closes, highs, lows)
    reclaim_ok, reclaim_invalidation = _reclaim_ma20(closes, highs, lows)
    if not (base_ok or reclaim_ok):
        _LAST_ENTRY_SETUP_VALID_DEBUG = None
        return False

    if base_ok:
        invalidation_level = base_invalidation
    else:
        invalidation_level = reclaim_invalidation

    if invalidation_level is None:
        _LAST_ENTRY_SETUP_VALID_DEBUG = None
        return False

    entry_price = closes[0]
    if entry_price <= invalidation_level:
        _LAST_ENTRY_SETUP_VALID_DEBUG = None
        return False

    atr14 = _atr14(highs, lows, closes)
    risk_atr = None
    risk_pct = None
    if atr14 is not None:
        risk_atr = (entry_price - invalidation_level) / max(atr14, _TINY)
        if risk_atr > RISK_ATR_MAX:
            _LAST_ENTRY_SETUP_VALID_DEBUG = None
            return False
    else:
        risk_pct = (entry_price - invalidation_level) / entry_price
        if risk_pct > RISK_PCT_MAX:
            _LAST_ENTRY_SETUP_VALID_DEBUG = None
            return False

    support_ok = _support_ok(closes, invalidation_level)
    if not support_ok:
        _LAST_ENTRY_SETUP_VALID_DEBUG = None
        return False

    date_val = getattr(ctx, "as_of_date", None)
    if date_val is None:
        date_val = getattr(ctx, "date", None)
    if hasattr(date_val, "isoformat"):
        date_val = date_val.isoformat()
    date_part = f"date={date_val} " if date_val is not None else ""
    debug_info = (
        "DEBUG_ENTRY_SETUP_VALID "
        f"{date_part}"
        f"base_ok={base_ok} base_invalidation={base_invalidation} "
        f"reclaim_ok={reclaim_ok} reclaim_invalidation={reclaim_invalidation} "
        f"invalidation_level={invalidation_level} entry_price={entry_price} "
        f"atr14={atr14} risk_atr={risk_atr} risk_pct={risk_pct} "
        f"support_ok={support_ok} result=True"
    )
    _LAST_ENTRY_SETUP_VALID_DEBUG = debug_info
    return True


SMA_LEN = 20

BASE_WINDOW = 10
BASE_MAX_WIDTH_PCT = 0.06
LOW_DRIFT_EPS = 0.003

CLOSE_POS_MIN = 0.55

ATR_LEN = 14
RISK_ATR_MAX = 2.5
RISK_PCT_MAX = 0.06

SUPPORT_LOOKBACK = 3
SUPPORT_BREAK_EPS = 0.003

_TINY = 1e-12


def _min_required_len() -> int:
    return max(SMA_LEN + 1, BASE_WINDOW, ATR_LEN + 1, SUPPORT_LOOKBACK, 6)


def _base_range(closes: list[float], highs: list[float], lows: list[float]) -> tuple[bool, float | None]:
    window_high = max(highs[:BASE_WINDOW])
    window_low = min(lows[:BASE_WINDOW])
    width_pct = (window_high - window_low) / max(closes[0], _TINY)
    if width_pct > BASE_MAX_WIDTH_PCT:
        return False, None

    first_half = range(BASE_WINDOW - 1, BASE_WINDOW // 2 - 1, -1)
    second_half = range(BASE_WINDOW // 2 - 1, -1, -1)
    min_first = min(lows[i] for i in first_half)
    min_second = min(lows[i] for i in second_half)
    if min_second < min_first * (1 - LOW_DRIFT_EPS):
        return False, None

    return True, window_low


def _reclaim_ma20(closes: list[float], highs: list[float], lows: list[float]) -> tuple[bool, float | None]:
    sma20 = _sma_series(closes, SMA_LEN)
    if sma20 is None:
        return False, None
    if not (closes[1] <= sma20[1] and closes[0] > sma20[0]):
        return False, None
    if _close_pos(closes[0], highs[0], lows[0]) < CLOSE_POS_MIN:
        return False, None

    invalidation = min(lows[:6])
    return True, invalidation


def _support_ok(closes: list[float], invalidation: float) -> bool:
    for i in range(SUPPORT_LOOKBACK):
        if closes[i] < invalidation * (1 - SUPPORT_BREAK_EPS):
            return False
    return True


def _sma_series(closes: list[float], window: int) -> list[float] | None:
    if window <= 0:
        return None
    if len(closes) < window:
        return None
    out: list[float] = []
    for i in range(len(closes) - window + 1):
        out.append(sum(closes[i : i + window]) / float(window))
    return out


def _atr14(highs: list[float], lows: list[float], closes: list[float]) -> float | None:
    if len(closes) < ATR_LEN + 1:
        return None
    trs: list[float] = []
    for i in range(ATR_LEN):
        high = highs[i]
        low = lows[i]
        prev_close = closes[i + 1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    return sum(trs) / float(ATR_LEN)


def _close_pos(close: float, high: float, low: float) -> float:
    return (close - low) / max(high - low, _TINY)
