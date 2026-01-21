from __future__ import annotations

from dataclasses import dataclass

from ..domain.enums import ReasonCode, State
from ..domain.models import StateAttrs, Transition


@dataclass
class EvaluationResult:
    prev_state: State
    final_state: State
    reasons: list[ReasonCode]
    transition: Transition | None
    final_attrs: StateAttrs
    guardrails_blocked: bool
    proposed_state: State
