"""Signal evaluation context for v3 providers.

Responsibilities:
  - Provide immutable slices of OHLCV data for signal evaluation.
  - Must not encode or depend on policy state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class SignalContextV3:
    closes: List[float]
    highs: List[float]
    lows: List[float]
    ohlc: List[Tuple[str, float, float, float, float, float]]
    as_of_date: str | None = None
