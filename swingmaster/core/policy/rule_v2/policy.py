"""Rule-based transition policy v2 for daily market state decisions.

Responsibilities:
  - Consume provider-level signals and optional history to decide next state.
Must not:
  - Read OHLCV directly; uses signals and ports only.
Outputs:
  - Decision objects for the state machine with audit reasons.
"""

from __future__ import annotations

from typing import Optional

from swingmaster.core.domain.models import Decision, StateAttrs
from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.policy.ports.state_history_port import StateHistoryPort
from swingmaster.core.policy.rule_v1 import RuleBasedTransitionPolicyV1Impl
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet


class RuleBasedTransitionPolicyV2Impl:
    """Policy V2 scaffold; delegates to V1; same ports; enables future behavior changes."""

    def __init__(
        self,
        ruleset: Optional[object] = None,
        history_port: Optional[StateHistoryPort] = None,
    ) -> None:
        self._v1 = RuleBasedTransitionPolicyV1Impl(history_port=history_port)

    def decide(
        self,
        prev_state: State,
        prev_attrs: StateAttrs,
        signals: SignalSet,
        ticker: Optional[str] = None,
        as_of_date: Optional[str] = None,
    ) -> Decision:
        enriched = _apply_dow_invalidation(prev_state, signals)
        decision = self._v1.decide(
            prev_state,
            prev_attrs,
            enriched,
            ticker=ticker,
            as_of_date=as_of_date,
        )
        if prev_state != State.NO_TRADE:
            return decision
        if decision.next_state != State.NO_TRADE:
            return decision
        if decision.reason_codes != [ReasonCode.NO_SIGNAL]:
            return decision
        if not enriched.has(SignalKey.SLOW_DECLINE_STARTED):
            return decision
        if enriched.has(SignalKey.DOW_TREND_UP):
            return decision
        return Decision(
            next_state=State.DOWNTREND_EARLY,
            reason_codes=[ReasonCode.SLOW_DECLINE_STARTED],
            attrs_update=StateAttrs(confidence=None, age=0, status=None),
        )


def _apply_dow_invalidation(prev_state: State, signals: SignalSet) -> SignalSet:
    if signals.has(SignalKey.DATA_INSUFFICIENT):
        return signals
    if signals.has(SignalKey.INVALIDATED):
        return signals
    if prev_state not in {State.STABILIZING, State.ENTRY_WINDOW}:
        return signals
    if not signals.has(SignalKey.DOW_NEW_LL):
        return signals

    new_signals = dict(signals.signals)
    new_signals[SignalKey.INVALIDATED] = Signal(
        key=SignalKey.INVALIDATED,
        value=True,
        confidence=None,
        source="dow_structure",
    )
    return SignalSet(signals=new_signals)
