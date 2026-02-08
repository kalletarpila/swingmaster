"""Signal evaluation context for v2 providers.

Responsibilities:
  - Provide immutable slices of OHLCV data for signal evaluation.
  - Must not encode or depend on policy state.

Key definitions:
  - SignalContextV2: closes/highs/lows/ohlc with optional as_of_date.

Inputs/Outputs:
  - Inputs: preloaded price series in most-recent-first order.
  - Outputs: pure data container for signal modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class SignalContextV2:
    closes: List[float]
    highs: List[float]
    lows: List[float]
    ohlc: List[Tuple[str, float, float, float, float, float]]
    as_of_date: str | None = None
