"""Domain models for policy decisions and transitions.

Responsibilities:
  - Define immutable data carriers for state attributes, decisions, and transitions.

Inputs/Outputs:
  - Decision and Transition are persisted/audited by infra layers.

Invariants:
  - Models must be deterministic containers with no behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .enums import ReasonCode, State


@dataclass
class StateAttrs:
    confidence: Optional[int]
    age: int
    status: Optional[str]
    downtrend_origin: Optional[str] = None


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
