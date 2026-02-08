"""Tests for policy factory wiring and rule set behavior."""

from __future__ import annotations

import pytest

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.factory import PolicyFactory, default_policy_factory
from swingmaster.core.policy.rule_policy_v1 import RuleBasedTransitionPolicyV1
from swingmaster.core.policy.rule_v1.policy import (
    RuleSet,
    apply_ruleset,
    build_ruleset_rule_v1,
    RuleBasedTransitionPolicyV1Impl,
)
from swingmaster.core.policy.rule_v1.rules_common import apply_hard_exclusions
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet


def make_signals(*pairs) -> SignalSet:
    signals = {}
    for key, value in pairs:
        signals[key] = Signal(key=key, value=value, confidence=None, source="test")
    return SignalSet(signals=signals)


def test_apply_ruleset_hard_exclusion_precedence():
    # Hard exclusion should win even if per-state rules would match.
    ruleset = build_ruleset_rule_v1()
    signals = make_signals((SignalKey.DATA_INSUFFICIENT, True), (SignalKey.TREND_STARTED, True))
    proposal = apply_ruleset(State.NO_TRADE, signals, ruleset)
    assert proposal.next_state == State.NO_TRADE
    assert proposal.reasons == [ReasonCode.DATA_INSUFFICIENT]


def test_factory_returns_policy_and_errors_on_unknown():
    factory = PolicyFactory()
    factory.register("rule_v1", "dev", lambda: RuleBasedTransitionPolicyV1())
    policy = factory.create("rule_v1", "dev")
    assert isinstance(policy, RuleBasedTransitionPolicyV1)
    with pytest.raises(ValueError):
        factory.create("unknown", "v0")


def test_impl_with_injected_ruleset_behaves_same_for_no_trade_stay():
    ruleset = build_ruleset_rule_v1()
    impl = RuleBasedTransitionPolicyV1Impl(ruleset)
    prev_state = State.NO_TRADE
    prev_attrs = StateAttrs(confidence=None, age=2, status=None)
    signals = make_signals()  # no signals -> stay NO_TRADE with NO_SIGNAL fallback
    decision = impl.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert decision.reason_codes == [ReasonCode.NO_SIGNAL]
    assert decision.attrs_update.age == prev_attrs.age + 1
