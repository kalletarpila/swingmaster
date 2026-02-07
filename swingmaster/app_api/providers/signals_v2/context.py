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
