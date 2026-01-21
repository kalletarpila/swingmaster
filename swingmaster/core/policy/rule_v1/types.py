from __future__ import annotations

from dataclasses import dataclass
from typing import List

from swingmaster.core.domain.enums import ReasonCode, State


@dataclass(frozen=True)
class Proposal:
    next_state: State
    reasons: List[ReasonCode]
