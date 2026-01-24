from __future__ import annotations


def eval_invalidated(lows: list[float], invalidation_lookback: int) -> bool:
    if len(lows) < invalidation_lookback + 1:
        return False
    prior_lows = lows[1 : invalidation_lookback + 1]
    return bool(prior_lows) and lows[0] < min(prior_lows)
