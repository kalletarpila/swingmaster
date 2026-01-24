from __future__ import annotations

import json
from typing import Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_v1.policy import (
    CHURN_GUARD_THRESHOLD,
    CHURN_GUARD_WINDOW_DAYS,
    RESET_NO_SIGNAL_DAYS,
    RuleBasedTransitionPolicyV1Impl,
)
from swingmaster.core.policy.ports.state_history_port import StateHistoryDay, StateHistoryPort
from swingmaster.core.policy.rule_policy_v1 import RuleBasedTransitionPolicyV1
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet


def make_signals(*pairs) -> SignalSet:
    signals = {}
    for key, value in pairs:
        signals[key] = Signal(key=key, value=value, confidence=None, source="test")
    return SignalSet(signals=signals)


def mk_signalset(keys: list[SignalKey]) -> SignalSet:
    signals = {k: Signal(key=k, value=True, confidence=None, source="test") for k in keys}
    return SignalSet(signals=signals)


def mk_attrs(age: int = 0, status: Optional[str] = None) -> StateAttrs:
    return StateAttrs(confidence=None, age=age, status=status)


class FakeHistoryPort(StateHistoryPort):
    def __init__(self, days: list[StateHistoryDay]) -> None:
        self._days = days

    def get_recent_days(self, ticker: str, as_of_date: str, limit: int) -> list[StateHistoryDay]:
        return self._days[:limit]


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
    assert set(decision.reason_codes) == {
        ReasonCode.STABILIZATION_CONFIRMED,
        ReasonCode.ENTRY_CONDITIONS_MET,
    }


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


def test_edge_gone_triggers_reset_to_neutral_when_no_blockers():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.EDGE_GONE])
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_stuck_pass_resets_after_threshold_on_no_signal():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=RESET_NO_SIGNAL_DAYS - 1)
    signals = mk_signalset([SignalKey.NO_SIGNAL])
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_progress_signal_blocks_reset_even_if_age_high():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=RESET_NO_SIGNAL_DAYS - 1)
    signals = mk_signalset([SignalKey.NO_SIGNAL, SignalKey.STABILIZATION_CONFIRMED])
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes


def test_invalidated_blocks_reset_even_if_edge_gone():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=RESET_NO_SIGNAL_DAYS - 1)
    signals = mk_signalset([SignalKey.INVALIDATED, SignalKey.EDGE_GONE])
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes


def test_churn_hits_trigger_reset_when_threshold_met():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    status = json.dumps(
        {"churn_guard_hits": CHURN_GUARD_THRESHOLD, "churn_window_days": CHURN_GUARD_WINDOW_DAYS}
    )
    prev_attrs = mk_attrs(age=0, status=status)
    signals = mk_signalset([SignalKey.NO_SIGNAL])
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_empty_signalset_does_not_count_as_quiet_day_for_stuck_reset():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=RESET_NO_SIGNAL_DAYS - 1)
    signals = SignalSet(signals={})
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes


def test_history_pass_resets_after_threshold_on_no_signal():
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        )
        for i in range(1, RESET_NO_SIGNAL_DAYS + 1)
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.NO_SIGNAL])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_history_quiet_days_blocked_by_progress_signal():
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        )
        for i in range(1, RESET_NO_SIGNAL_DAYS)
    ]
    days.append(
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.STABILIZATION_CONFIRMED],
            signal_keys=[SignalKey.STABILIZATION_CONFIRMED],
        )
    )
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.NO_SIGNAL])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes


def test_history_edge_gone_resets_without_invalidated():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.EDGE_GONE],
            signal_keys=[SignalKey.EDGE_GONE],
        )
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.NO_SIGNAL])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_history_invalidated_blocks_reset_even_with_edge_gone():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.INVALIDATED, ReasonCode.EDGE_GONE],
            signal_keys=[SignalKey.INVALIDATED, SignalKey.EDGE_GONE],
        )
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.NO_SIGNAL])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes


def test_history_churn_hits_trigger_reset():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
            churn_guard_hits=CHURN_GUARD_THRESHOLD,
        )
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.NO_SIGNAL])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.RESET_TO_NEUTRAL in decision.reason_codes


def test_history_empty_signalset_day_not_quiet():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[],
        )
        for _ in range(RESET_NO_SIGNAL_DAYS)
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.PASS
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.NO_SIGNAL])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.RESET_TO_NEUTRAL not in decision.reason_codes
