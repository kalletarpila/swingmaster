"""Rules for PASS state transitions in rule_v1.

Responsibilities:
  - Reset or remain in PASS based on signals.
Must not:
  - Access OHLCV or policy history; rules are signal-only and deterministic.
Key definitions:
  - rule_reset.
"""

from __future__ import annotations

from typing import Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.signals.models import SignalSet
from .types import Proposal


def rule_reset(signals: SignalSet) -> Optional[Proposal]:
    return Proposal(next_state=State.NO_TRADE, reasons=[])
