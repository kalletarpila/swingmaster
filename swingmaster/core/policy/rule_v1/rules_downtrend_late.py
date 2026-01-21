from __future__ import annotations

from typing import Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import SignalSet
from .types import Proposal


def rule_stabilizing(signals: SignalSet) -> Optional[Proposal]:
    if signals.has(SignalKey.STABILIZATION_CONFIRMED):
        return Proposal(next_state=State.STABILIZING, reasons=[ReasonCode.STABILIZATION_CONFIRMED])
    if signals.has(SignalKey.SELLING_PRESSURE_EASED):
        return Proposal(next_state=State.STABILIZING, reasons=[ReasonCode.SELLING_PRESSURE_EASED])
    return None
