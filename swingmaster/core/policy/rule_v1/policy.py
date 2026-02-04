from __future__ import annotations

import json

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import Decision, StateAttrs
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import SignalSet
from swingmaster.core.policy.ports.state_history_port import StateHistoryDay, StateHistoryPort
from .rules_common import Rule, apply_hard_exclusions, first_match
from .rules_downtrend_early import rule_stabilizing as rule_de_stabilizing, rule_trend_matured
from .rules_downtrend_late import rule_stabilizing as rule_dl_stabilizing
from .rules_entry_window import rule_keep_window, rule_pass
from .rules_no_trade import rule_trend_started
from .rules_pass import rule_reset
from .rules_stabilizing import rule_entry_window, rule_stay_with_confirmed
from .types import Proposal
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class RuleSet:
    hard_exclusions: List[Rule]
    per_state_rules: Dict[State, List[Rule]]
    fallback_reasons: Dict[State, ReasonCode]
    default_next_state_when_no_match: bool = True


def apply_ruleset(prev_state: State, signals: SignalSet, ruleset: RuleSet) -> Proposal:
    hard = first_match(ruleset.hard_exclusions, signals)
    if hard:
        return hard

    state_rules = ruleset.per_state_rules.get(prev_state, [])
    proposed = first_match(state_rules, signals)
    if proposed:
        return proposed

    if ruleset.default_next_state_when_no_match:
        return Proposal(next_state=prev_state, reasons=[])
    return Proposal(next_state=prev_state, reasons=[])


def build_ruleset_rule_v1() -> RuleSet:
    per_state: Dict[State, List[Rule]] = {
        State.NO_TRADE: [rule_trend_started],
        State.DOWNTREND_EARLY: [rule_trend_matured, rule_de_stabilizing],
        State.DOWNTREND_LATE: [rule_dl_stabilizing],
        State.STABILIZING: [rule_entry_window, rule_stay_with_confirmed],
        State.ENTRY_WINDOW: [rule_keep_window, rule_pass],
        State.PASS: [rule_reset],
    }
    fallback_reasons = {
        State.NO_TRADE: ReasonCode.NO_SIGNAL,
        State.DOWNTREND_EARLY: ReasonCode.TREND_STARTED,
        State.DOWNTREND_LATE: ReasonCode.TREND_MATURED,
    }
    return RuleSet(
        hard_exclusions=[apply_hard_exclusions],
        per_state_rules=per_state,
        fallback_reasons=fallback_reasons,
    )


class RuleBasedTransitionPolicyV1Impl:
    def __init__(
        self, ruleset: Optional[RuleSet] = None, history_port: Optional[StateHistoryPort] = None
    ) -> None:
        self._ruleset = ruleset or build_ruleset_rule_v1()
        self._history_port = history_port

    def decide(
        self,
        prev_state: State,
        prev_attrs: StateAttrs,
        signals: SignalSet,
        ticker: Optional[str] = None,
        as_of_date: Optional[str] = None,
    ) -> Decision:
        if _should_reset_to_neutral(
            prev_state,
            prev_attrs,
            signals,
            history_port=self._history_port,
            ticker=ticker,
            as_of_date=as_of_date,
        ):
            return Decision(
                next_state=State.NO_TRADE,
                reason_codes=[ReasonCode.RESET_TO_NEUTRAL],
                attrs_update=StateAttrs(confidence=None, age=0, status=None),
            )
        proposal = apply_ruleset(prev_state, signals, self._ruleset)

        if proposal.next_state != prev_state:
            attrs_update = StateAttrs(confidence=None, age=0, status=None)
        else:
            reasons = proposal.reasons
            if not reasons:
                fallback = self._ruleset.fallback_reasons.get(prev_state)
                if fallback:
                    reasons = [fallback]
            proposal = Proposal(next_state=proposal.next_state, reasons=reasons)
            attrs_update = StateAttrs(
                confidence=prev_attrs.confidence,
                age=prev_attrs.age + 1,
                status=prev_attrs.status,
            )

        return Decision(
            next_state=proposal.next_state,
            reason_codes=proposal.reasons,
            attrs_update=attrs_update,
        )


RESET_NO_SIGNAL_DAYS = 15
CHURN_GUARD_THRESHOLD = 3
CHURN_GUARD_WINDOW_DAYS = 10
PROGRESS_SIGNALS = {
    SignalKey.TREND_STARTED,
    SignalKey.TREND_MATURED,
    SignalKey.STABILIZATION_CONFIRMED,
    SignalKey.ENTRY_SETUP_VALID,
    SignalKey.INVALIDATED,
}
ALLOWED_QUIET_SIGNALS = {SignalKey.NO_SIGNAL}
if hasattr(SignalKey, "CHURN_GUARD"):
    ALLOWED_QUIET_SIGNALS.add(SignalKey.CHURN_GUARD)

DOW_QUIET_SIGNALS = {
    SignalKey.DOW_TREND_UP,
    SignalKey.DOW_TREND_DOWN,
    SignalKey.DOW_TREND_NEUTRAL,
    SignalKey.DOW_LAST_LOW_L,
    SignalKey.DOW_LAST_LOW_HL,
    SignalKey.DOW_LAST_LOW_LL,
    SignalKey.DOW_LAST_HIGH_H,
    SignalKey.DOW_LAST_HIGH_HH,
    SignalKey.DOW_LAST_HIGH_LH,
}


def _extract_churn_guard_hits(status: Optional[str]) -> int:
    if not status:
        return 0
    try:
        parsed = json.loads(status)
    except Exception:
        return 0
    if not isinstance(parsed, dict):
        return 0
    count = parsed.get("churn_guard_hits")
    if count is None:
        count = parsed.get("churn_guard_count")
    window_days = parsed.get("churn_window_days")
    if window_days is None:
        window_days = parsed.get("churn_guard_window_days")
    if window_days is not None and window_days < CHURN_GUARD_WINDOW_DAYS:
        return 0
    if isinstance(count, int):
        return count
    return 0


def _has_any_progress_signal(signals: SignalSet) -> bool:
    for key in PROGRESS_SIGNALS:
        if signals.has(key):
            return True
    return False


def _is_quiet_day(signals: SignalSet) -> bool:
    if signals.has(SignalKey.DATA_INSUFFICIENT):
        return False
    if signals.has(SignalKey.INVALIDATED):
        return False
    if signals.has(SignalKey.EDGE_GONE):
        return False
    if _has_any_progress_signal(signals):
        return False
    try:
        signal_keys = signals.signals.keys()
    except Exception:
        return signals.has(SignalKey.NO_SIGNAL)
    if not signal_keys:
        return False
    if SignalKey.NO_SIGNAL not in signal_keys:
        return False
    for key in signal_keys:
        if key not in ALLOWED_QUIET_SIGNALS and key not in DOW_QUIET_SIGNALS:
            return False
    return True


def _day_has_reason(day: StateHistoryDay, reason: ReasonCode) -> bool:
    return reason in day.reason_codes


def _day_has_signal(day: StateHistoryDay, signal: SignalKey) -> bool:
    if day.signal_keys is None:
        return False
    return signal in day.signal_keys


def _window_has_invalidated(history: list[StateHistoryDay]) -> bool:
    for day in history:
        if _day_has_reason(day, ReasonCode.INVALIDATED) or _day_has_signal(day, SignalKey.INVALIDATED):
            return True
    return False


def _window_has_edge_gone(history: list[StateHistoryDay]) -> bool:
    for day in history:
        if _day_has_reason(day, ReasonCode.EDGE_GONE) or _day_has_signal(day, SignalKey.EDGE_GONE):
            return True
    return False


def _max_churn_hits(history: list[StateHistoryDay]) -> int:
    max_hits = 0
    for day in history:
        if day.churn_guard_hits is None:
            continue
        if day.churn_guard_hits > max_hits:
            max_hits = day.churn_guard_hits
    return max_hits


def _is_quiet_history_day(day: StateHistoryDay) -> bool:
    if _day_has_reason(day, ReasonCode.DATA_INSUFFICIENT) or _day_has_signal(day, SignalKey.DATA_INSUFFICIENT):
        return False
    if _day_has_reason(day, ReasonCode.INVALIDATED) or _day_has_signal(day, SignalKey.INVALIDATED):
        return False
    if _day_has_reason(day, ReasonCode.EDGE_GONE) or _day_has_signal(day, SignalKey.EDGE_GONE):
        return False
    if day.signal_keys is None:
        return False
    if SignalKey.NO_SIGNAL not in day.signal_keys:
        return False
    for key in day.signal_keys:
        if key not in ALLOWED_QUIET_SIGNALS and key not in DOW_QUIET_SIGNALS:
            return False
    return True


def _should_reset_to_neutral(
    prev_state: State,
    prev_attrs: StateAttrs,
    signals: SignalSet,
    history_port: Optional[StateHistoryPort] = None,
    ticker: Optional[str] = None,
    as_of_date: Optional[str] = None,
) -> bool:
    if signals.has(SignalKey.INVALIDATED):
        return False
    if signals.has(SignalKey.DATA_INSUFFICIENT):
        return False
    if _has_any_progress_signal(signals):
        return False

    if history_port is not None and ticker and as_of_date:
        history = history_port.get_recent_days(ticker, as_of_date, limit=RESET_NO_SIGNAL_DAYS)
        if _window_has_invalidated(history):
            return False
        if signals.has(SignalKey.EDGE_GONE) or _window_has_edge_gone(history):
            return True
        if _max_churn_hits(history) >= CHURN_GUARD_THRESHOLD:
            return True
        if prev_state in {State.PASS, State.STABILIZING} and len(history) >= RESET_NO_SIGNAL_DAYS:
            if all(_is_quiet_history_day(day) for day in history):
                return True
        return False

    if signals.has(SignalKey.EDGE_GONE):
        return True

    if prev_state in {State.PASS, State.STABILIZING}:
        if prev_attrs.age >= RESET_NO_SIGNAL_DAYS - 1:
            if _is_quiet_day(signals):
                return True

    churn_guard_hits = _extract_churn_guard_hits(prev_attrs.status)
    if churn_guard_hits >= CHURN_GUARD_THRESHOLD:
        return True

    return False
