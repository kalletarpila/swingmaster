"""Rules for ENTRY_WINDOW state behavior in rule_v1.

Responsibilities:
  - Keep entry window open or pass based on signals.
Must not:
  - Access OHLCV or policy history; rules are signal-only and deterministic.
Key definitions:
  - rule_keep_window, rule_pass.
"""

from __future__ import annotations

from typing import Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import SignalSet
from .types import Proposal


def rule_keep_window(signals: SignalSet) -> Optional[Proposal]:
    if signals.has(SignalKey.ENTRY_SETUP_VALID):
        return Proposal(next_state=State.ENTRY_WINDOW, reasons=[ReasonCode.ENTRY_CONDITIONS_MET])
    return None


def rule_pass(signals: SignalSet) -> Optional[Proposal]:
    return Proposal(next_state=State.PASS, reasons=[])
