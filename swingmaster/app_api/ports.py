"""Port definitions for app-level dependencies.

Responsibilities:
  - Define interface contracts for providers and persistence.
Must not:
  - Implement logic; interfaces only.
"""

from __future__ import annotations

from typing import Protocol

from swingmaster.core.domain.enums import State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.signals.models import SignalSet


class SignalProvider(Protocol):
    def get_signals(self, ticker: str, date: str) -> SignalSet:
        ...


class PrevStateProvider(Protocol):
    def get_prev(self, ticker: str, date: str) -> tuple[State, StateAttrs]:
        ...
