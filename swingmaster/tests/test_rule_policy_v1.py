from __future__ import annotations

import json
from typing import Optional

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.policy.rule_v1.policy import (
    CHURN_GUARD_THRESHOLD,
    CHURN_GUARD_WINDOW_DAYS,
    CHURN_COOLDOWN_DAYS,
    CHURN_REPEAT_WINDOW_DAYS,
    SETUP_FRESH_DAYS,
    STAB_RECENCY_DAYS,
    EDGE_GONE_ENTRY_WINDOW_MAX_AGE,
    EDGE_GONE_RECENT_SETUP_LOOKBACK,
    EDGE_GONE_STABILIZING_MAX_AGE,
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
    assert decision.reason_codes == [ReasonCode.ENTRY_CONDITIONS_MET]


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
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        )
        for i in range(EDGE_GONE_ENTRY_WINDOW_MAX_AGE, 0, -1)
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.ENTRY_WINDOW
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.next_state == State.PASS
    assert ReasonCode.EDGE_GONE in decision.reason_codes


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
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        )
        for i in range(EDGE_GONE_ENTRY_WINDOW_MAX_AGE, 0, -1)
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.ENTRY_WINDOW
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.INVALIDATED])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.next_state == State.NO_TRADE
    assert decision.reason_codes == [ReasonCode.INVALIDATED]
    assert ReasonCode.EDGE_GONE not in decision.reason_codes


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
            date=f"2026-01-{i:02d}",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.STABILIZATION_CONFIRMED],
            signal_keys=[SignalKey.STABILIZATION_CONFIRMED],
        )
        for i in range(EDGE_GONE_STABILIZING_MAX_AGE, 0, -1)
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-20")
    assert decision.next_state == State.NO_TRADE
    assert ReasonCode.EDGE_GONE in decision.reason_codes


def test_history_invalidated_blocks_reset_even_with_edge_gone():
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.STABILIZATION_CONFIRMED],
            signal_keys=[SignalKey.STABILIZATION_CONFIRMED],
        )
        for i in range(EDGE_GONE_STABILIZING_MAX_AGE, 0, -1)
    ]
    recent_setup = StateHistoryDay(
        date="2026-01-19",
        state=State.ENTRY_WINDOW,
        reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
        signal_keys=[SignalKey.ENTRY_SETUP_VALID],
    )
    days.insert(0, recent_setup)
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-20")
    assert ReasonCode.EDGE_GONE not in decision.reason_codes


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


def test_edge_gone_blocked_by_data_insufficient():
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        )
        for i in range(EDGE_GONE_ENTRY_WINDOW_MAX_AGE, 0, -1)
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.ENTRY_WINDOW
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.DATA_INSUFFICIENT])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.reason_codes == [ReasonCode.DATA_INSUFFICIENT]


def test_stabilizing_edge_gone_blocked_by_recent_setup_without_signal_keys():
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.STABILIZATION_CONFIRMED],
            signal_keys=None,
        )
        for i in range(EDGE_GONE_STABILIZING_MAX_AGE, 0, -1)
    ]
    recent_setup = StateHistoryDay(
        date="2026-01-19",
        state=State.ENTRY_WINDOW,
        reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
        signal_keys=None,
    )
    days.insert(0, recent_setup)
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-20")
    assert ReasonCode.EDGE_GONE not in decision.reason_codes


def test_entry_window_edge_gone_not_triggered_when_run_broken():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        ),
        StateHistoryDay(
            date="2026-01-14",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.STABILIZATION_CONFIRMED],
            signal_keys=[SignalKey.STABILIZATION_CONFIRMED],
        ),
    ]
    days.extend(
        [
            StateHistoryDay(
                date=f"2026-01-{i:02d}",
                state=State.ENTRY_WINDOW,
                reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
                signal_keys=[SignalKey.ENTRY_SETUP_VALID],
            )
            for i in range(EDGE_GONE_ENTRY_WINDOW_MAX_AGE - 2, 0, -1)
        ]
    )
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.ENTRY_WINDOW
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.EDGE_GONE not in decision.reason_codes


def test_churn_guard_blocks_entry_after_recent_exit():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        ),
        StateHistoryDay(
            date="2026-01-14",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        ),
    ]
    for i in range(3, CHURN_COOLDOWN_DAYS + 2):
        days.append(
            StateHistoryDay(
                date=f"2026-01-{16 - i:02d}",
                state=State.PASS,
                reason_codes=[ReasonCode.NO_SIGNAL],
                signal_keys=[SignalKey.NO_SIGNAL],
            )
        )
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.CHURN_GUARD in decision.reason_codes
    assert decision.next_state == State.STABILIZING


def test_churn_guard_not_triggered_when_exit_old():
    days = []
    for i in range(CHURN_COOLDOWN_DAYS):
        days.append(
            StateHistoryDay(
                date=f"2026-01-{15 - i:02d}",
                state=State.PASS,
                reason_codes=[ReasonCode.NO_SIGNAL],
                signal_keys=None,
            )
        )
    days.append(
        StateHistoryDay(
            date="2026-01-05",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=None,
        )
    )
    days.append(
        StateHistoryDay(
            date="2026-01-04",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=None,
        )
    )
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.CHURN_GUARD not in decision.reason_codes


def test_stabilization_confirmed_disables_churn_guard():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        ),
        StateHistoryDay(
            date="2026-01-14",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        ),
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID, SignalKey.STABILIZATION_CONFIRMED])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.CHURN_GUARD not in decision.reason_codes


def test_trend_started_reason_added_when_stabilizing_no_transition():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=2)
    signals = mk_signalset([SignalKey.TREND_STARTED])
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.next_state == State.STABILIZING
    assert ReasonCode.TREND_STARTED in decision.reason_codes
    assert ReasonCode.INVALIDATED not in decision.reason_codes


def test_invalidated_overrides_churn_guard():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        ),
        StateHistoryDay(
            date="2026-01-14",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        ),
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID, SignalKey.INVALIDATED])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.reason_codes == [ReasonCode.INVALIDATED]
    assert ReasonCode.CHURN_GUARD not in decision.reason_codes


def test_data_insufficient_blocks_churn_guard():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        ),
        StateHistoryDay(
            date="2026-01-14",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        ),
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID, SignalKey.DATA_INSUFFICIENT])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.reason_codes == [ReasonCode.DATA_INSUFFICIENT]
    assert ReasonCode.CHURN_GUARD not in decision.reason_codes


def test_churn_guard_repeat_setup_without_stabilization():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        )
    ]
    for i in range(1, CHURN_REPEAT_WINDOW_DAYS):
        days.append(
            StateHistoryDay(
                date=f"2026-01-{15 - i:02d}",
                state=State.STABILIZING,
                reason_codes=[ReasonCode.NO_SIGNAL],
                signal_keys=[SignalKey.NO_SIGNAL],
            )
        )
    days.insert(
        1,
        StateHistoryDay(
            date="2026-01-14",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        ),
    )
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.CHURN_GUARD in decision.reason_codes


def test_entry_conditions_met_happy_path():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.STABILIZATION_CONFIRMED],
            signal_keys=[SignalKey.STABILIZATION_CONFIRMED],
        )
    ]
    for i in range(1, max(STAB_RECENCY_DAYS, SETUP_FRESH_DAYS)):
        days.append(
            StateHistoryDay(
                date=f"2026-01-{15 - i:02d}",
                state=State.STABILIZING,
                reason_codes=[ReasonCode.NO_SIGNAL],
                signal_keys=[SignalKey.ENTRY_SETUP_VALID] if i == 1 else [SignalKey.NO_SIGNAL],
            )
        )
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert decision.next_state == State.ENTRY_WINDOW
    assert decision.reason_codes == [ReasonCode.ENTRY_CONDITIONS_MET]


def test_entry_conditions_blocked_by_churn_guard():
    days = [
        StateHistoryDay(
            date="2026-01-15",
            state=State.PASS,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        ),
        StateHistoryDay(
            date="2026-01-14",
            state=State.ENTRY_WINDOW,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            signal_keys=[SignalKey.ENTRY_SETUP_VALID],
        ),
        StateHistoryDay(
            date="2026-01-13",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.STABILIZATION_CONFIRMED],
            signal_keys=[SignalKey.STABILIZATION_CONFIRMED],
        ),
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-15")
    assert ReasonCode.CHURN_GUARD in decision.reason_codes
    assert ReasonCode.ENTRY_CONDITIONS_MET not in decision.reason_codes


def test_entry_conditions_blocked_by_edge_gone():
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.STABILIZATION_CONFIRMED],
            signal_keys=[SignalKey.STABILIZATION_CONFIRMED],
        )
        for i in range(EDGE_GONE_STABILIZING_MAX_AGE, 0, -1)
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID, SignalKey.STABILIZATION_CONFIRMED])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-20")
    assert ReasonCode.EDGE_GONE in decision.reason_codes
    assert ReasonCode.ENTRY_CONDITIONS_MET not in decision.reason_codes


def test_entry_conditions_blocked_by_invalidated():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID, SignalKey.INVALIDATED])
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.reason_codes == [ReasonCode.INVALIDATED]
    assert ReasonCode.ENTRY_CONDITIONS_MET not in decision.reason_codes


def test_entry_conditions_blocked_by_data_insufficient():
    policy = RuleBasedTransitionPolicyV1Impl()
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID, SignalKey.DATA_INSUFFICIENT])
    decision = policy.decide(prev_state, prev_attrs, signals)
    assert decision.reason_codes == [ReasonCode.DATA_INSUFFICIENT]
    assert ReasonCode.ENTRY_CONDITIONS_MET not in decision.reason_codes


def test_entry_conditions_freshness_fails():
    days = [
        StateHistoryDay(
            date=f"2026-01-{i:02d}",
            state=State.STABILIZING,
            reason_codes=[ReasonCode.NO_SIGNAL],
            signal_keys=[SignalKey.NO_SIGNAL],
        )
        for i in range(SETUP_FRESH_DAYS + 2, 0, -1)
    ]
    history = FakeHistoryPort(days)
    policy = RuleBasedTransitionPolicyV1Impl(history_port=history)
    prev_state = State.STABILIZING
    prev_attrs = mk_attrs(age=0)
    signals = mk_signalset([SignalKey.ENTRY_SETUP_VALID, SignalKey.STABILIZATION_CONFIRMED])
    decision = policy.decide(prev_state, prev_attrs, signals, ticker="AAA", as_of_date="2026-01-20")
    assert ReasonCode.ENTRY_CONDITIONS_MET not in decision.reason_codes


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
