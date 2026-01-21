from __future__ import annotations

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_policy_v1 import RuleBasedTransitionPolicyV1
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet


def make_signals(*pairs) -> SignalSet:
    signals = {}
    for key, value in pairs:
        signals[key] = Signal(key=key, value=value, confidence=None, source="test")
    return SignalSet(signals=signals)


def test_hard_exclusions_precedence():
    policy = RuleBasedTransitionPolicyV1()
    prev_state = State.NO_TRADE
    prev_attrs = StateAttrs(confidence=None, age=0, status=None)
    signals = make_signals(
        (SignalKey.DATA_INSUFFICIENT, True),
        (SignalKey.INVALIDATED, True),
        (SignalKey.EDGE_GONE, True),
    )
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert decision.reason_codes == [ReasonCode.DATA_INSUFFICIENT]


def test_no_trade_to_early_on_trend_started():
    policy = RuleBasedTransitionPolicyV1()
    prev_state = State.NO_TRADE
    prev_attrs = StateAttrs(confidence=None, age=0, status=None)
    signals = make_signals((SignalKey.TREND_STARTED, True))
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.DOWNTREND_EARLY
    assert decision.reason_codes == [ReasonCode.TREND_STARTED]
    assert decision.attrs_update.age == 0


def test_fallback_reason_no_signal_in_no_trade():
    policy = RuleBasedTransitionPolicyV1()
    prev_state = State.NO_TRADE
    prev_attrs = StateAttrs(confidence=None, age=2, status=None)
    signals = make_signals()  # empty
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert decision.reason_codes == [ReasonCode.NO_SIGNAL]
    assert decision.attrs_update.age == prev_attrs.age + 1


def test_fallback_reason_trend_started_in_early_when_no_signals():
    policy = RuleBasedTransitionPolicyV1()
    prev_state = State.DOWNTREND_EARLY
    prev_attrs = StateAttrs(confidence=None, age=3, status=None)
    signals = make_signals()
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.DOWNTREND_EARLY
    assert decision.reason_codes == [ReasonCode.TREND_STARTED]
    assert decision.attrs_update.age == prev_attrs.age + 1


def test_stabilizing_to_entry_window_requires_both_signals():
    policy = RuleBasedTransitionPolicyV1()
    prev_state = State.STABILIZING
    prev_attrs = StateAttrs(confidence=None, age=1, status=None)
    signals = make_signals(
        (SignalKey.STABILIZATION_CONFIRMED, True),
        (SignalKey.ENTRY_SETUP_VALID, True),
    )
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.ENTRY_WINDOW
    assert decision.reason_codes == [ReasonCode.STABILIZATION_CONFIRMED, ReasonCode.ENTRY_CONDITIONS_MET]


def test_entry_window_to_pass_when_setup_invalid():
    policy = RuleBasedTransitionPolicyV1()
    prev_state = State.ENTRY_WINDOW
    prev_attrs = StateAttrs(confidence=None, age=1, status=None)
    signals = make_signals()  # no valid setup signal
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.PASS
    assert decision.reason_codes == []
    assert decision.attrs_update.age == 0


def test_hard_exclusion_resets_to_no_trade_from_early():
    policy = RuleBasedTransitionPolicyV1()
    prev_state = State.DOWNTREND_EARLY
    prev_attrs = StateAttrs(confidence=None, age=5, status=None)
    signals = make_signals((SignalKey.DATA_INSUFFICIENT, True))
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert decision.reason_codes == [ReasonCode.DATA_INSUFFICIENT]
    assert decision.attrs_update.age == 0


def test_fallback_reason_trend_matured_in_late_when_no_signals():
    policy = RuleBasedTransitionPolicyV1()
    prev_state = State.DOWNTREND_LATE
    prev_attrs = StateAttrs(confidence=None, age=7, status=None)
    signals = make_signals()
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.DOWNTREND_LATE
    assert decision.reason_codes == [ReasonCode.TREND_MATURED]
    assert decision.attrs_update.age == 8
