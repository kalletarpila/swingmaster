from __future__ import annotations

from typing import Optional

from swingmaster.core.domain.models import Decision, StateAttrs
from swingmaster.core.domain.enums import State
from swingmaster.core.policy.ports.state_history_port import StateHistoryPort
from swingmaster.core.policy.rule_v1 import RuleBasedTransitionPolicyV1Impl
from swingmaster.core.signals.models import SignalSet


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
        return self._v1.decide(
            prev_state,
            prev_attrs,
            signals,
            ticker=ticker,
            as_of_date=as_of_date,
        )
