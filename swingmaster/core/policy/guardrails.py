"""Policy guardrails for state transitions.

Responsibilities:
  - Enforce allowed transitions and minimum state ages.
  - Provide guardrail reason codes when blocking transitions.

Inputs/Outputs:
  - Inputs: previous state/attrs and proposed next state.
  - Outputs: GuardrailResult with allowed flag and reason codes.

Invariants:
  - Must be deterministic and independent of OHLCV.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..domain.enums import ReasonCode, State
from ..domain.models import StateAttrs
from ..domain.transition_graph import ALLOWED_TRANSITIONS


@dataclass
class GuardrailResult:
    allowed: bool
    final_state: State
    reason_codes: list[ReasonCode]


MIN_STATE_AGE: dict[State, int] = {
    State.NO_TRADE: 0,
    State.DOWNTREND_EARLY: 2,
    State.DOWNTREND_LATE: 3,
    State.STABILIZING: 2,
    State.ENTRY_WINDOW: 1,
    State.PASS: 1,
}


def apply_guardrails(
    prev_state: State, prev_attrs: StateAttrs, proposed_state: State
) -> GuardrailResult:
    if proposed_state == prev_state:
        return GuardrailResult(
            allowed=True,
            final_state=prev_state,
            reason_codes=[],
        )

    if proposed_state not in ALLOWED_TRANSITIONS[prev_state]:
        return GuardrailResult(
            allowed=False,
            final_state=prev_state,
            reason_codes=[ReasonCode.DISALLOWED_TRANSITION],
        )

    if prev_attrs.age < MIN_STATE_AGE[prev_state]:
        return GuardrailResult(
            allowed=False,
            final_state=prev_state,
            reason_codes=[ReasonCode.MIN_STATE_AGE_LOCK],
        )

    return GuardrailResult(
        allowed=True,
        final_state=proposed_state,
        reason_codes=[],
    )
