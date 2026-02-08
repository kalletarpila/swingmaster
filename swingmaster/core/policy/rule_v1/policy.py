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
from .rules_stabilizing import rule_stay_with_confirmed
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
        State.STABILIZING: [rule_stay_with_confirmed],
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
        edge_decision = _edge_gone_decision(
            prev_state,
            prev_attrs,
            signals,
            history_port=self._history_port,
            ticker=ticker,
            as_of_date=as_of_date,
        )
        if edge_decision is not None:
            return _with_trend_started_reason(edge_decision, signals)
        churn_decision = _churn_guard_decision(
            prev_state,
            prev_attrs,
            signals,
            history_port=self._history_port,
            ticker=ticker,
            as_of_date=as_of_date,
        )
        if churn_decision is not None:
            return _with_trend_started_reason(churn_decision, signals)
        entry_decision = _entry_conditions_decision(
            prev_state,
            prev_attrs,
            signals,
            history_port=self._history_port,
            ticker=ticker,
            as_of_date=as_of_date,
        )
        if entry_decision is not None:
            return _with_trend_started_reason(entry_decision, signals)
        if _should_reset_to_neutral(
            prev_state,
            prev_attrs,
            signals,
            history_port=self._history_port,
            ticker=ticker,
            as_of_date=as_of_date,
        ):
            decision = Decision(
                next_state=State.NO_TRADE,
                reason_codes=[ReasonCode.RESET_TO_NEUTRAL],
                attrs_update=StateAttrs(confidence=None, age=0, status=None),
            )
            return _with_trend_started_reason(decision, signals)
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

        decision = Decision(
            next_state=proposal.next_state,
            reason_codes=proposal.reasons,
            attrs_update=attrs_update,
        )
        return _with_trend_started_reason(decision, signals)


def _with_trend_started_reason(decision: Decision, signals: SignalSet) -> Decision:
    if ReasonCode.ENTRY_CONDITIONS_MET in decision.reason_codes:
        return Decision(
            next_state=decision.next_state,
            reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
            attrs_update=decision.attrs_update,
        )
    if signals.has(SignalKey.DATA_INSUFFICIENT):
        return decision
    if signals.has(SignalKey.INVALIDATED):
        return decision
    if ReasonCode.INVALIDATED in decision.reason_codes:
        return decision
    if not signals.has(SignalKey.TREND_STARTED):
        return decision
    if ReasonCode.TREND_STARTED in decision.reason_codes:
        return decision
    return Decision(
        next_state=decision.next_state,
        reason_codes=[*decision.reason_codes, ReasonCode.TREND_STARTED],
        attrs_update=decision.attrs_update,
    )


RESET_NO_SIGNAL_DAYS = 15
CHURN_GUARD_THRESHOLD = 3
CHURN_GUARD_WINDOW_DAYS = 10
CHURN_COOLDOWN_DAYS = 7
CHURN_REPEAT_WINDOW_DAYS = 10
STAB_RECENCY_DAYS = 10
SETUP_FRESH_DAYS = 5
EDGE_GONE_ENTRY_WINDOW_MAX_AGE = 12
EDGE_GONE_STABILIZING_MAX_AGE = 20
EDGE_GONE_RECENT_SETUP_LOOKBACK = 10
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


def _edge_gone_decision(
    prev_state: State,
    prev_attrs: StateAttrs,
    signals: SignalSet,
    history_port: Optional[StateHistoryPort] = None,
    ticker: Optional[str] = None,
    as_of_date: Optional[str] = None,
) -> Optional[Decision]:
    if signals.has(SignalKey.DATA_INSUFFICIENT):
        return None
    if signals.has(SignalKey.INVALIDATED):
        return None
    if prev_state not in {State.ENTRY_WINDOW, State.STABILIZING}:
        return None

    history = None
    if history_port is not None and ticker and as_of_date:
        history = history_port.get_recent_days(
            ticker,
            as_of_date,
            limit=max(
                EDGE_GONE_ENTRY_WINDOW_MAX_AGE,
                EDGE_GONE_STABILIZING_MAX_AGE,
                EDGE_GONE_RECENT_SETUP_LOOKBACK,
            ),
        )

    consecutive = _consecutive_state_days(history, prev_state, prev_attrs)

    if prev_state == State.ENTRY_WINDOW:
        if consecutive >= EDGE_GONE_ENTRY_WINDOW_MAX_AGE:
            return Decision(
                next_state=State.PASS,
                reason_codes=[ReasonCode.EDGE_GONE],
                attrs_update=StateAttrs(confidence=None, age=0, status=None),
            )
        return None

    if prev_state == State.STABILIZING:
        if consecutive >= EDGE_GONE_STABILIZING_MAX_AGE:
            if history is not None and _has_recent_setup_activity(history):
                return None
            return Decision(
                next_state=State.NO_TRADE,
                reason_codes=[ReasonCode.EDGE_GONE],
                attrs_update=StateAttrs(confidence=None, age=0, status=None),
            )
    return None


def _consecutive_state_days(
    history: Optional[list[StateHistoryDay]],
    state: State,
    prev_attrs: StateAttrs,
) -> int:
    if not history:
        return prev_attrs.age + 1
    if history[0].state != state:
        return prev_attrs.age + 1
    count = 0
    for day in history:
        if day.state != state:
            break
        count += 1
    return count


def _has_recent_setup_activity(history: list[StateHistoryDay]) -> bool:
    window = history[:EDGE_GONE_RECENT_SETUP_LOOKBACK]
    if not window:
        return False
    if any(day.signal_keys is not None for day in window):
        for day in window:
            if day.signal_keys and SignalKey.ENTRY_SETUP_VALID in day.signal_keys:
                return True
        return False
    for day in window:
        if day.state == State.ENTRY_WINDOW:
            return True
    return False


def _churn_guard_decision(
    prev_state: State,
    prev_attrs: StateAttrs,
    signals: SignalSet,
    history_port: Optional[StateHistoryPort] = None,
    ticker: Optional[str] = None,
    as_of_date: Optional[str] = None,
) -> Optional[Decision]:
    if signals.has(SignalKey.DATA_INSUFFICIENT):
        return None
    if signals.has(SignalKey.INVALIDATED):
        return None
    if signals.has(SignalKey.STABILIZATION_CONFIRMED):
        return None
    if prev_state not in {State.STABILIZING, State.PASS, State.NO_TRADE, State.ENTRY_WINDOW}:
        return None
    if not signals.has(SignalKey.ENTRY_SETUP_VALID):
        return None

    history = None
    if history_port is not None and ticker and as_of_date:
        history = history_port.get_recent_days(
            ticker,
            as_of_date,
            limit=max(CHURN_COOLDOWN_DAYS, CHURN_REPEAT_WINDOW_DAYS) + 1,
        )

    churn_guard = False
    if history is not None:
        if _recent_entry_window_exit(history):
            churn_guard = True
        elif _repeat_setup_without_stabilization(history):
            churn_guard = True

    if churn_guard:
        return Decision(
            next_state=prev_state,
            reason_codes=[ReasonCode.CHURN_GUARD],
            attrs_update=StateAttrs(
                confidence=prev_attrs.confidence,
                age=prev_attrs.age + 1,
                status=prev_attrs.status,
            ),
        )
    return None


def _recent_entry_window_exit(history: list[StateHistoryDay]) -> bool:
    for idx in range(len(history) - 1):
        today = history[idx]
        prior = history[idx + 1]
        if today.state == State.PASS and prior.state == State.ENTRY_WINDOW:
            return idx < CHURN_COOLDOWN_DAYS
    return False


def _repeat_setup_without_stabilization(history: list[StateHistoryDay]) -> bool:
    window = history[:CHURN_REPEAT_WINDOW_DAYS]
    if not window:
        return False
    if not any(day.signal_keys is not None for day in window):
        # Signal keys not available; cannot evaluate repeat-setup safely.
        return False
    idx_setup = None
    for i, day in enumerate(window):
        if day.signal_keys and SignalKey.ENTRY_SETUP_VALID in day.signal_keys:
            idx_setup = i
            break
    if idx_setup is None:
        return False
    for day in window[:idx_setup]:
        if day.signal_keys and SignalKey.STABILIZATION_CONFIRMED in day.signal_keys:
            return False
    return True


def _entry_conditions_decision(
    prev_state: State,
    prev_attrs: StateAttrs,
    signals: SignalSet,
    history_port: Optional[StateHistoryPort] = None,
    ticker: Optional[str] = None,
    as_of_date: Optional[str] = None,
) -> Optional[Decision]:
    if signals.has(SignalKey.DATA_INSUFFICIENT):
        return None
    if signals.has(SignalKey.INVALIDATED):
        return None
    if signals.has(SignalKey.EDGE_GONE):
        return None
    if signals.has(SignalKey.NO_SIGNAL):
        return None
    if signals.has(SignalKey.TREND_STARTED):
        return None
    if signals.has(SignalKey.TREND_MATURED):
        return None
    if prev_state != State.STABILIZING:
        return None
    if not signals.has(SignalKey.ENTRY_SETUP_VALID):
        return None

    history = None
    if history_port is not None and ticker and as_of_date:
        history = history_port.get_recent_days(
            ticker,
            as_of_date,
            limit=max(STAB_RECENCY_DAYS, SETUP_FRESH_DAYS),
        )

    has_signal_keys = _history_has_signal_keys(history)
    stabilization_recent = False
    if signals.has(SignalKey.STABILIZATION_CONFIRMED):
        stabilization_recent = True
    elif history is not None and has_signal_keys:
        stabilization_recent = _history_has_signal(history, SignalKey.STABILIZATION_CONFIRMED, STAB_RECENCY_DAYS)
    else:
        stabilization_recent = prev_state == State.STABILIZING

    if not stabilization_recent:
        return None

    setup_fresh = True
    if history is not None and has_signal_keys:
        setup_fresh = _history_has_signal(history, SignalKey.ENTRY_SETUP_VALID, SETUP_FRESH_DAYS)
    elif history is not None and not has_signal_keys:
        # No signal keys; use ENTRY_WINDOW as a conservative freshness proxy.
        setup_fresh = _history_has_state(history, State.ENTRY_WINDOW, SETUP_FRESH_DAYS)

    if not setup_fresh:
        return None

    return Decision(
        next_state=State.ENTRY_WINDOW,
        reason_codes=[ReasonCode.ENTRY_CONDITIONS_MET],
        attrs_update=StateAttrs(confidence=None, age=0, status=None),
    )


def _history_has_signal_keys(history: Optional[list[StateHistoryDay]]) -> bool:
    if not history:
        return False
    return any(day.signal_keys is not None for day in history)


def _history_has_signal(history: list[StateHistoryDay], signal: SignalKey, limit: int) -> bool:
    for day in history[:limit]:
        if day.signal_keys and signal in day.signal_keys:
            return True
    return False


def _history_has_state(history: list[StateHistoryDay], state: State, limit: int) -> bool:
    for day in history[:limit]:
        if day.state == state:
            return True
    return False
