from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .enums import ReasonCode, State


@dataclass
class StateAttrs:
    confidence: Optional[int]
    age: int
    status: Optional[str]


@dataclass
class Decision:
    next_state: State
    reason_codes: list[ReasonCode]
    attrs_update: Optional[StateAttrs]


@dataclass
class Transition:
    from_state: State
    to_state: State
    reason_codes: list[ReasonCode]
