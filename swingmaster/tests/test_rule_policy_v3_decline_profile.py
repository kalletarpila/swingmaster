"""Tests for decline profile behavior in rule policy v3."""

from __future__ import annotations

from swingmaster.core.domain.enums import State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_policy_v3 import RuleBasedTransitionPolicyV3
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet


def make_signals(*keys: SignalKey) -> SignalSet:
    return SignalSet(signals={k: Signal(key=k, value=True, confidence=None, source="test") for k in keys})


def test_origin_and_profile_set_on_no_trade_to_downtrend_early_with_slow_drift() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.NO_TRADE,
        prev_attrs=StateAttrs(confidence=None, age=0, status=None),
        signals=make_signals(SignalKey.TREND_STARTED, SignalKey.SLOW_DRIFT_DETECTED),
    )
    assert decision.next_state == State.DOWNTREND_EARLY
    assert decision.attrs_update is not None
    assert decision.attrs_update.downtrend_origin == "TREND"
    assert decision.attrs_update.decline_profile == "SLOW_DRIFT"


def test_profile_stability_does_not_override_specific_profile() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.DOWNTREND_EARLY,
        prev_attrs=StateAttrs(
            confidence=None,
            age=2,
            status='{"downtrend_origin":"TREND","decline_profile":"SLOW_DRIFT"}',
            downtrend_origin="TREND",
            decline_profile="SLOW_DRIFT",
        ),
        signals=make_signals(SignalKey.SHARP_SELL_OFF_DETECTED),
    )
    assert decision.attrs_update is not None
    assert decision.attrs_update.decline_profile == "SLOW_DRIFT"


def test_profile_upgrade_from_unknown_to_structural_downtrend() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(
            confidence=None,
            age=3,
            status='{"decline_profile":"UNKNOWN"}',
            decline_profile="UNKNOWN",
        ),
        signals=make_signals(SignalKey.STRUCTURAL_DOWNTREND_DETECTED),
    )
    assert decision.attrs_update is not None
    assert decision.attrs_update.decline_profile == "STRUCTURAL_DOWNTREND"
