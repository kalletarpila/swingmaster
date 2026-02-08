"""Signal: INVALIDATED.

Category:
  - Invalidation / breakdown guard.

Contract:
  - Inputs: low-price series (most recent first) and invalidation lookback length.
  - Output: boolean signal (True when today's low breaks prior lows).
  - Determinism: must not depend on policy state or history.

Trigger summary:
  - Fires when lows[0] is strictly less than the minimum low in the prior
    invalidation_lookback days.

Edge cases:
  - Returns False when insufficient history is available.

Complexity:
  - O(lookback) time, O(1) extra space.
"""

from __future__ import annotations

_DEBUG_INVALIDATED = False
_LAST_INVALIDATED_DEBUG = None
_INVALIDATED_DATE = None


def set_invalidated_debug(enabled: bool) -> None:
    global _DEBUG_INVALIDATED
    _DEBUG_INVALIDATED = enabled


def set_invalidated_debug_date(date_str: str | None) -> None:
    global _INVALIDATED_DATE
    _INVALIDATED_DATE = date_str


def get_invalidated_debug() -> str | None:
    return _LAST_INVALIDATED_DEBUG


def _debug_print(msg: str) -> None:
    if _DEBUG_INVALIDATED:
        print(msg)


def eval_invalidated(lows: list[float], invalidation_lookback: int) -> bool:
    global _LAST_INVALIDATED_DEBUG
    if len(lows) < invalidation_lookback + 1:
        _LAST_INVALIDATED_DEBUG = None
        return False
    prior_lows = lows[1 : invalidation_lookback + 1]
    if not prior_lows:
        _LAST_INVALIDATED_DEBUG = None
        return False
    prior_min = min(prior_lows)
    result = lows[0] < prior_min
    if result:
        debug_info = (
            "DEBUG_INVALIDATED "
            f"date={_INVALIDATED_DATE} invalidation_lookback={invalidation_lookback} "
            f"today_low={lows[0]} prior_min_low={prior_min} result=True"
        )
        _LAST_INVALIDATED_DEBUG = debug_info
    else:
        _LAST_INVALIDATED_DEBUG = None
    return result
