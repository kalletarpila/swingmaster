"""Adapter for rule_v1 policy construction and identification.

Responsibilities:
  - Provide a stable factory entry point for rule_v1 policy instances.
Must not:
  - Perform policy logic; delegates to rule_v1 implementation.
"""

from __future__ import annotations

from ..domain.models import Decision, StateAttrs
from ..domain.enums import State
from ..signals.models import SignalSet
from typing import Optional

from .rule_v1 import RuleBasedTransitionPolicyV1Impl
from .ports.state_history_port import StateHistoryPort


class RuleBasedTransitionPolicyV1:
    def __init__(self, history_port: Optional[StateHistoryPort] = None) -> None:
        self._impl = RuleBasedTransitionPolicyV1Impl(history_port=history_port)

    def decide(
        self,
        prev_state: State,
        prev_attrs: StateAttrs,
        signals: SignalSet,
        ticker: Optional[str] = None,
        as_of_date: Optional[str] = None,
    ) -> Decision:
        return self._impl.decide(prev_state, prev_attrs, signals, ticker=ticker, as_of_date=as_of_date)
