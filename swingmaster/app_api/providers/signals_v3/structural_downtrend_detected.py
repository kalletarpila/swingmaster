"""Signal: STRUCTURAL_DOWNTREND_DETECTED.

Definition:
  - Prefer existing Dow facts (DOW_TREND_DOWN or DOW_NEW_LL).
  - Fallback to simple pivot structure: last two highs and lows are decreasing.
"""

from __future__ import annotations

from typing import Dict, Optional

from swingmaster.core.signals.enums import SignalKey

from .context import SignalContextV3


LOOKBACK_WINDOW = 30


def eval_structural_downtrend_detected(
    ctx: SignalContextV3,
    dow_facts: Optional[Dict[SignalKey, bool]] = None,
) -> bool:
    if dow_facts:
        if dow_facts.get(SignalKey.DOW_TREND_DOWN, False):
            return True
        if dow_facts.get(SignalKey.DOW_NEW_LL, False):
            return True

    closes = ctx.closes[:LOOKBACK_WINDOW]
    if len(closes) < 5 or any(v is None for v in closes):
        return False

    asc = list(reversed(closes))
    highs = []
    lows = []
    for i in range(1, len(asc) - 1):
        prev_v = asc[i - 1]
        curr_v = asc[i]
        next_v = asc[i + 1]
        if curr_v > prev_v and curr_v > next_v:
            highs.append(curr_v)
        if curr_v < prev_v and curr_v < next_v:
            lows.append(curr_v)

    if len(highs) < 2 or len(lows) < 2:
        return False

    return highs[-2] > highs[-1] and lows[-2] > lows[-1]
