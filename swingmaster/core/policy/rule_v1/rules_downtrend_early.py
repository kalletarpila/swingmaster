"""Rules for DOWNTREND_EARLY state transitions in rule_v1.

Responsibilities:
  - Map specific SignalSet patterns to Proposal transitions.
Must not:
  - Access OHLCV or policy history; rules are signal-only and deterministic.
Key definitions:
  - rule_trend_matured, rule_stabilizing.
"""

from __future__ import annotations

from typing import Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import SignalSet
from .types import Proposal


def rule_trend_matured(signals: SignalSet) -> Optional[Proposal]:
    if signals.has(SignalKey.TREND_MATURED):
        return Proposal(next_state=State.DOWNTREND_LATE, reasons=[ReasonCode.TREND_MATURED])
    return None


def rule_stabilizing(signals: SignalSet) -> Optional[Proposal]:
    if signals.has(SignalKey.STABILIZATION_CONFIRMED):
        return Proposal(next_state=State.STABILIZING, reasons=[ReasonCode.STABILIZATION_CONFIRMED])
    if signals.has(SignalKey.SELLING_PRESSURE_EASED):
        return Proposal(next_state=State.STABILIZING, reasons=[ReasonCode.SELLING_PRESSURE_EASED])
    return None
