"""Tests for rule policy v2 dow."""

from __future__ import annotations

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_policy_v2 import RuleBasedTransitionPolicyV2
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet


def make_signals(*keys: SignalKey) -> SignalSet:
    signals = {k: Signal(key=k, value=True, confidence=None, source="test") for k in keys}
    return SignalSet(signals=signals)


def test_dow_new_ll_invalidates_in_stabilizing_context():
    policy = RuleBasedTransitionPolicyV2()
    signals = make_signals(SignalKey.DOW_NEW_LL)
    decision = policy.decide(
        prev_state=State.STABILIZING,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=signals,
    )
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.INVALIDATED in decision.reason_codes


def test_dow_new_ll_does_not_invalidate_outside_context():
    policy = RuleBasedTransitionPolicyV2()
    signals = make_signals(SignalKey.DOW_NEW_LL)
    decision = policy.decide(
        prev_state=State.NO_TRADE,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=signals,
    )
    assert ReasonCode.INVALIDATED not in decision.reason_codes


def test_no_trade_to_downtrend_early_on_slow_decline_started_when_not_uptrend():
    policy = RuleBasedTransitionPolicyV2()
    signals = make_signals(SignalKey.SLOW_DECLINE_STARTED, SignalKey.DOW_TREND_NEUTRAL)
    decision = policy.decide(
        prev_state=State.NO_TRADE,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=signals,
    )
    assert decision.next_state == State.DOWNTREND_EARLY
    assert ReasonCode.SLOW_DECLINE_STARTED in decision.reason_codes


def test_no_trade_stays_no_trade_on_slow_decline_started_if_dow_trend_up_present():
    policy = RuleBasedTransitionPolicyV2()
    signals = make_signals(SignalKey.SLOW_DECLINE_STARTED, SignalKey.DOW_TREND_UP)
    decision = policy.decide(
        prev_state=State.NO_TRADE,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=signals,
    )
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.SLOW_DECLINE_STARTED not in decision.reason_codes
