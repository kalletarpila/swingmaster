from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .enums import SignalKey


@dataclass
class Signal:
    key: SignalKey
    value: bool | float | int  # boolean observation or scalar strength; must not be None
    confidence: Optional[int]
    source: Optional[str]


@dataclass
class SignalSet:
    """Immutable snapshot of observed signals for a single ticker and date."""
    signals: dict[SignalKey, Signal]

    def has(self, key: SignalKey) -> bool:
        return key in self.signals

    def get(self, key: SignalKey) -> Optional[Signal]:
        return self.signals.get(key)
