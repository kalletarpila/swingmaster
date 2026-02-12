"""Tests for engine evaluator guardrails."""

from __future__ import annotations

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import Decision, StateAttrs
from swingmaster.core.engine.evaluator import evaluate_step
from swingmaster.core.policy.guardrails import apply_guardrails
from swingmaster.core.signals.models import SignalSet


class _PolicyWithInvalidated:
    def decide(self, prev_state: State, prev_attrs: StateAttrs, signals: SignalSet) -> Decision:
        return Decision(
            next_state=State.DOWNTREND_LATE,
            reason_codes=[ReasonCode.INVALIDATED],
            attrs_update=None,
        )


def test_churn_guard_not_combined_with_invalidated() -> None:
    prev_state = State.DOWNTREND_EARLY
    prev_attrs = StateAttrs(confidence=None, age=2, status=None)
    signals = SignalSet({})
    policy = _PolicyWithInvalidated()

    result = evaluate_step(prev_state, prev_attrs, signals, policy)

    assert result.reasons == [ReasonCode.INVALIDATED]


def test_guardrails_min_state_age_lock_reason() -> None:
    prev_state = State.DOWNTREND_EARLY
    prev_attrs = StateAttrs(confidence=None, age=0, status=None)
    result = apply_guardrails(prev_state, prev_attrs, State.DOWNTREND_LATE)

    assert result.reason_codes == [ReasonCode.MIN_STATE_AGE_LOCK]
    assert ReasonCode.CHURN_GUARD not in result.reason_codes


def test_guardrails_disallowed_transition_reason() -> None:
    prev_state = State.NO_TRADE
    prev_attrs = StateAttrs(confidence=None, age=0, status=None)
    result = apply_guardrails(prev_state, prev_attrs, State.ENTRY_WINDOW)

    assert result.reason_codes == [ReasonCode.DISALLOWED_TRANSITION]
