from __future__ import annotations

from ..domain.enums import ReasonCode, State
from ..domain.models import Decision, StateAttrs
from ..signals.enums import SignalKey
from ..signals.models import SignalSet


class RuleBasedTransitionPolicyV1:
    def decide(
        self,
        prev_state: State,
        prev_attrs: StateAttrs,
        signals: SignalSet,
    ) -> Decision:
        # Hard exclusions
        if signals.has(SignalKey.DATA_INSUFFICIENT):
            next_state = State.NO_TRADE
            reasons = [ReasonCode.DATA_INSUFFICIENT]
        elif signals.has(SignalKey.INVALIDATED):
            next_state = State.NO_TRADE
            reasons = [ReasonCode.INVALIDATED]
        elif signals.has(SignalKey.EDGE_GONE):
            next_state = State.NO_TRADE
            reasons = [ReasonCode.EDGE_GONE]
        else:
            # State-driven proposals
            if prev_state == State.NO_TRADE:
                if signals.has(SignalKey.TREND_STARTED):
                    next_state = State.DOWNTREND_EARLY
                    reasons = [ReasonCode.TREND_STARTED]
                else:
                    next_state = State.NO_TRADE
                    reasons = []

            elif prev_state == State.DOWNTREND_EARLY:
                if signals.has(SignalKey.TREND_MATURED):
                    next_state = State.DOWNTREND_LATE
                    reasons = [ReasonCode.TREND_MATURED]
                elif signals.has(SignalKey.STABILIZATION_CONFIRMED) or signals.has(
                    SignalKey.SELLING_PRESSURE_EASED
                ):
                    if signals.has(SignalKey.STABILIZATION_CONFIRMED):
                        next_state = State.STABILIZING
                        reasons = [ReasonCode.STABILIZATION_CONFIRMED]
                    else:
                        next_state = State.STABILIZING
                        reasons = [ReasonCode.SELLING_PRESSURE_EASED]
                else:
                    next_state = State.DOWNTREND_EARLY
                    reasons = []

            elif prev_state == State.DOWNTREND_LATE:
                if signals.has(SignalKey.STABILIZATION_CONFIRMED) or signals.has(
                    SignalKey.SELLING_PRESSURE_EASED
                ):
                    if signals.has(SignalKey.STABILIZATION_CONFIRMED):
                        next_state = State.STABILIZING
                        reasons = [ReasonCode.STABILIZATION_CONFIRMED]
                    else:
                        next_state = State.STABILIZING
                        reasons = [ReasonCode.SELLING_PRESSURE_EASED]
                else:
                    next_state = State.DOWNTREND_LATE
                    reasons = []

            elif prev_state == State.STABILIZING:
                if signals.has(SignalKey.STABILIZATION_CONFIRMED) and signals.has(
                    SignalKey.ENTRY_SETUP_VALID
                ):
                    next_state = State.ENTRY_WINDOW
                    reasons = [
                        ReasonCode.STABILIZATION_CONFIRMED,
                        ReasonCode.ENTRY_CONDITIONS_MET,
                    ]
                elif signals.has(SignalKey.STABILIZATION_CONFIRMED):
                    next_state = State.STABILIZING
                    reasons = [ReasonCode.STABILIZATION_CONFIRMED]
                else:
                    next_state = State.STABILIZING
                    reasons = []

            elif prev_state == State.ENTRY_WINDOW:
                if signals.has(SignalKey.ENTRY_SETUP_VALID):
                    next_state = State.ENTRY_WINDOW
                    reasons = [ReasonCode.ENTRY_CONDITIONS_MET]
                else:
                    next_state = State.PASS
                    reasons = []

            elif prev_state == State.PASS:
                next_state = State.NO_TRADE
                reasons = []

            else:
                next_state = prev_state
                reasons = []

        if next_state != prev_state:
            attrs_update = StateAttrs(confidence=None, age=0, status=None)
        else:
            if not reasons:
                if prev_state == State.NO_TRADE:
                    reasons = [ReasonCode.NO_SIGNAL]
                elif prev_state == State.DOWNTREND_EARLY:
                    reasons = [ReasonCode.TREND_STARTED]
                elif prev_state == State.DOWNTREND_LATE:
                    reasons = [ReasonCode.TREND_MATURED]
            attrs_update = StateAttrs(
                confidence=prev_attrs.confidence,
                age=prev_attrs.age + 1,
                status=prev_attrs.status,
            )

        return Decision(
            next_state=next_state,
            reason_codes=reasons,
            attrs_update=attrs_update,
        )
