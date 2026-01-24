from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.signals.enums import SignalKey


@dataclass(frozen=True)
class StateHistoryDay:
    date: str
    state: State
    reason_codes: list[ReasonCode]
    signal_keys: Optional[list[SignalKey]] = None
    churn_guard_hits: Optional[int] = None


class StateHistoryPort(Protocol):
    def get_recent_days(self, ticker: str, as_of_date: str, limit: int) -> list[StateHistoryDay]:
        ...
