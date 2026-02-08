"""State machine evaluation for a single ticker/day.

Responsibilities:
  - Apply policy decision and guardrails deterministically.
  - Merge policy and guardrail reasons into final reasons list.
  - Produce EvaluationResult and optional Transition for persistence.

Inputs/Outputs:
  - Inputs: previous State/StateAttrs, SignalSet, and a TransitionPolicy.
  - Outputs: EvaluationResult with final state, reasons, and transition.

Invariants:
  - Must not read OHLCV directly; signals are precomputed.
  - Must remain deterministic and audit-friendly.
"""

from __future__ import annotations

from typing import Callable, Protocol

from ..domain.enums import ReasonCode, State
from ..domain.models import Decision, StateAttrs, Transition
from ..policy.guardrails import MIN_STATE_AGE, apply_guardrails
from ..signals.models import SignalSet
from .result import EvaluationResult

_DEBUG_FN: Callable[[str], None] | None = None


def set_evaluator_debug(fn: Callable[[str], None] | None) -> None:
    global _DEBUG_FN
    _DEBUG_FN = fn


class TransitionPolicy(Protocol):
    def decide(self, prev_state: State, prev_attrs: StateAttrs, signals: SignalSet) -> Decision:
        ...


def evaluate_step(
    prev_state: State,
    prev_attrs: StateAttrs,
    signals: SignalSet,
    policy: TransitionPolicy,
    ticker: str | None = None,
    as_of_date: str | None = None,
) -> EvaluationResult:
    decision = policy.decide(prev_state, prev_attrs, signals)
    proposed_state = decision.next_state
    policy_reasons = decision.reason_codes
    proposed_attrs = decision.attrs_update
    if proposed_attrs is None:
        proposed_attrs = prev_attrs

    guardrail_result = apply_guardrails(prev_state, prev_attrs, proposed_state)

    if guardrail_result.allowed:
        final_state = proposed_state
        guardrails_blocked = False
        guardrail_reasons: list[ReasonCode] = []
        final_attrs = proposed_attrs
    else:
        final_state = prev_state
        guardrails_blocked = True
        guardrail_reasons = guardrail_result.reason_codes
        final_attrs = StateAttrs(
            confidence=prev_attrs.confidence,
            age=prev_attrs.age + 1,
            status=prev_attrs.status,
        )

    if _DEBUG_FN is not None and ReasonCode.CHURN_GUARD in guardrail_reasons:
        min_age = MIN_STATE_AGE.get(prev_state)
        _DEBUG_FN(
            "CHURN_GUARD_ORIGIN=GUARDRAILS "
            f"ticker={ticker} date={as_of_date} prev_state={prev_state} "
            f"prev_age={prev_attrs.age} min_age={min_age}"
        )

    if ReasonCode.CHURN_GUARD in guardrail_reasons:
        disallowed = {
            ReasonCode.INVALIDATED,
            ReasonCode.DATA_INSUFFICIENT,
            ReasonCode.TREND_STARTED,
            ReasonCode.TREND_MATURED,
            ReasonCode.STABILIZATION_CONFIRMED,
            ReasonCode.ENTRY_CONDITIONS_MET,
        }
        if any(reason in disallowed for reason in policy_reasons):
            guardrail_reasons = [r for r in guardrail_reasons if r != ReasonCode.CHURN_GUARD]

    reasons = policy_reasons + guardrail_reasons

    transition: Transition | None
    if final_state != prev_state:
        transition = Transition(
            from_state=prev_state,
            to_state=final_state,
            reason_codes=reasons,
        )
    else:
        transition = None

    return EvaluationResult(
        prev_state=prev_state,
        final_state=final_state,
        reasons=reasons,
        transition=transition,
        final_attrs=final_attrs,
        guardrails_blocked=guardrails_blocked,
        proposed_state=proposed_state,
    )
