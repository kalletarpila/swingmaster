from __future__ import annotations

from typing import Callable, Iterable, Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import SignalSet
from .types import Proposal

Rule = Callable[[SignalSet], Optional[Proposal]]


def apply_hard_exclusions(signals: SignalSet) -> Optional[Proposal]:
    # Ordered precedence: DATA_INSUFFICIENT > INVALIDATED > EDGE_GONE
    if signals.has(SignalKey.DATA_INSUFFICIENT):
        return Proposal(next_state=State.NO_TRADE, reasons=[ReasonCode.DATA_INSUFFICIENT])
    if signals.has(SignalKey.INVALIDATED):
        return Proposal(next_state=State.NO_TRADE, reasons=[ReasonCode.INVALIDATED])
    return None


def first_match(rules: Iterable[Rule], signals: SignalSet) -> Optional[Proposal]:
    for rule in rules:
        proposed = rule(signals)
        if proposed is not None:
            return proposed
    return None
