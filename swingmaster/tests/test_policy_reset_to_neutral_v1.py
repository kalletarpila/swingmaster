from __future__ import annotations

import json

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_v1.policy import (
    CHURN_GUARD_THRESHOLD,
    CHURN_GUARD_WINDOW_DAYS,
    RESET_NO_SIGNAL_DAYS,
    RuleBasedTransitionPolicyV1Impl,
)
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet


def make_signals(*keys: SignalKey) -> SignalSet:
    signals = {}
    for key in keys:
        signals[key] = Signal(key=key, value=True, confidence=None, source="test")
    return SignalSet(signals=signals)


def test_edge_gone_triggers_reset_to_neutral_when_no_blockers():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = StateAttrs(confidence=None, age=0, status=None)
    signals = make_signals(SignalKey.EDGE_GONE)
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_stuck_pass_resets_after_threshold_on_no_signal():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = StateAttrs(confidence=None, age=RESET_NO_SIGNAL_DAYS - 1, status=None)
    signals = make_signals(SignalKey.NO_SIGNAL)
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_progress_signal_blocks_reset_even_if_age_high():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = StateAttrs(confidence=None, age=RESET_NO_SIGNAL_DAYS - 1, status=None)
    signals = make_signals(SignalKey.NO_SIGNAL, SignalKey.STABILIZATION_CONFIRMED)
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes


def test_invalidated_blocks_reset_even_if_edge_gone_or_churn_hits():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = StateAttrs(confidence=None, age=RESET_NO_SIGNAL_DAYS - 1, status=None)
    signals = make_signals(SignalKey.INVALIDATED, SignalKey.EDGE_GONE)
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes


def test_churn_hits_trigger_reset_when_threshold_met():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    status = json.dumps(
        {
            "churn_guard_hits": CHURN_GUARD_THRESHOLD,
            "churn_window_days": CHURN_GUARD_WINDOW_DAYS,
        }
    )
    prev_attrs = StateAttrs(confidence=None, age=0, status=status)
    signals = make_signals(SignalKey.NO_SIGNAL)
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_empty_signalset_does_not_count_as_quiet_day_for_stuck_reset():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = StateAttrs(confidence=None, age=RESET_NO_SIGNAL_DAYS - 1, status=None)
    signals = SignalSet(signals={})
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes
