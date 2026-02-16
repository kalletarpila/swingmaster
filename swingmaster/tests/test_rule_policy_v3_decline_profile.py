"""Tests for decline profile behavior in rule policy v3."""

from __future__ import annotations

from swingmaster.core.domain.enums import State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_v3.policy import _resolve_stabilization_phase
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


def test_stabilization_phase_early_stabilization_default() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(confidence=None, age=1, status=None),
        signals=make_signals(SignalKey.SELLING_PRESSURE_EASED),
    )
    assert decision.next_state == State.STABILIZING
    assert decision.attrs_update is not None
    assert decision.attrs_update.stabilization_phase == "EARLY_STABILIZATION"


def test_stabilization_phase_base_building_requires_confirmed_and_compression() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(confidence=None, age=1, status=None),
        signals=make_signals(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.VOLATILITY_COMPRESSION_DETECTED,
        ),
    )
    assert decision.next_state == State.STABILIZING
    assert decision.attrs_update is not None
    assert decision.attrs_update.stabilization_phase == "BASE_BUILDING"


def test_stabilization_phase_early_reversal_on_entry_setup_valid() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(confidence=None, age=1, status=None),
        signals=make_signals(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.ENTRY_SETUP_VALID,
        ),
    )
    assert decision.next_state == State.STABILIZING
    assert decision.attrs_update is not None
    assert decision.attrs_update.stabilization_phase == "EARLY_REVERSAL"


def test_stabilization_phase_invalidated_blocks_promotion() -> None:
    phase = _resolve_stabilization_phase(
        State.STABILIZING,
        make_signals(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.VOLATILITY_COMPRESSION_DETECTED,
            SignalKey.ENTRY_SETUP_VALID,
            SignalKey.INVALIDATED,
        ),
        prev_phase="BASE_BUILDING",
    )
    assert phase == "EARLY_STABILIZATION"


def test_entry_window_forces_early_reversal() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.STABILIZING,
        prev_attrs=StateAttrs(
            confidence=None,
            age=2,
            status='{"stabilization_phase":"BASE_BUILDING"}',
            stabilization_phase="BASE_BUILDING",
        ),
        signals=make_signals(SignalKey.ENTRY_SETUP_VALID),
    )
    assert decision.next_state == State.ENTRY_WINDOW
    assert decision.attrs_update is not None
    assert decision.attrs_update.stabilization_phase == "EARLY_REVERSAL"


def test_gate_a_overrides_to_entry_window() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(confidence=None, age=2, status=None),
        signals=make_signals(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.MA20_RECLAIMED,
            SignalKey.HIGHER_LOW_CONFIRMED,
        ),
    )
    assert decision.next_state == State.ENTRY_WINDOW
    assert decision.attrs_update is not None
    assert decision.attrs_update.entry_gate == "EARLY_STAB_MA20_HL"
    assert decision.attrs_update.entry_quality == "A"
    assert decision.attrs_update.stabilization_phase == "EARLY_REVERSAL"


def test_gate_b_overrides_to_entry_window_when_ma20_only() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(confidence=None, age=2, status=None),
        signals=make_signals(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.MA20_RECLAIMED,
        ),
    )
    assert decision.next_state == State.ENTRY_WINDOW
    assert decision.attrs_update is not None
    assert decision.attrs_update.entry_gate == "EARLY_STAB_MA20"
    assert decision.attrs_update.entry_quality == "B"
    assert decision.attrs_update.stabilization_phase == "EARLY_REVERSAL"


def test_invalidated_blocks_gates() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.DOWNTREND_LATE,
        prev_attrs=StateAttrs(confidence=None, age=2, status=None),
        signals=make_signals(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.MA20_RECLAIMED,
            SignalKey.HIGHER_LOW_CONFIRMED,
            SignalKey.INVALIDATED,
        ),
    )
    assert decision.next_state != State.ENTRY_WINDOW
    assert decision.attrs_update is not None
    assert decision.attrs_update.entry_gate is None
    assert decision.attrs_update.entry_quality is None


def test_legacy_entry_window_labeled() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.STABILIZING,
        prev_attrs=StateAttrs(confidence=None, age=2, status=None),
        signals=make_signals(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.ENTRY_SETUP_VALID,
        ),
    )
    assert decision.next_state == State.ENTRY_WINDOW
    assert decision.attrs_update is not None
    assert decision.attrs_update.entry_gate == "LEGACY_ENTRY_SETUP_VALID"
    assert decision.attrs_update.entry_quality == "LEGACY"


def test_invalidated_blocks_legacy_entry_setup_valid() -> None:
    policy = RuleBasedTransitionPolicyV3()
    decision = policy.decide(
        prev_state=State.STABILIZING,
        prev_attrs=StateAttrs(
            confidence=None,
            age=2,
            status='{"stabilization_phase":"EARLY_STABILIZATION"}',
            stabilization_phase="EARLY_STABILIZATION",
        ),
        signals=make_signals(
            SignalKey.STABILIZATION_CONFIRMED,
            SignalKey.ENTRY_SETUP_VALID,
            SignalKey.INVALIDATED,
        ),
    )
    assert decision.next_state != State.ENTRY_WINDOW
    assert decision.attrs_update is not None
    assert decision.attrs_update.entry_gate is None
    assert decision.attrs_update.entry_quality is None
    assert decision.attrs_update.stabilization_phase == "EARLY_STABILIZATION"
