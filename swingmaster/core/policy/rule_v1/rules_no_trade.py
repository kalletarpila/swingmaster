from __future__ import annotations

from typing import Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import SignalSet
from .types import Proposal


def rule_trend_started(signals: SignalSet) -> Optional[Proposal]:
    if signals.has(SignalKey.TREND_STARTED):
        return Proposal(next_state=State.DOWNTREND_EARLY, reasons=[ReasonCode.TREND_STARTED])
    return None
