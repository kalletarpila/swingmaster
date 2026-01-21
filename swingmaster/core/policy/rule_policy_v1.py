from __future__ import annotations

from ..domain.models import Decision, StateAttrs
from ..domain.enums import State
from ..signals.models import SignalSet
from .rule_v1 import RuleBasedTransitionPolicyV1Impl


class RuleBasedTransitionPolicyV1:
    def __init__(self) -> None:
        self._impl = RuleBasedTransitionPolicyV1Impl()

    def decide(
        self,
        prev_state: State,
        prev_attrs: StateAttrs,
        signals: SignalSet,
    ) -> Decision:
        return self._impl.decide(prev_state, prev_attrs, signals)
