from __future__ import annotations

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import Decision, StateAttrs
from swingmaster.core.signals.models import SignalSet
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
    def __init__(self, ruleset: Optional[RuleSet] = None) -> None:
        self._ruleset = ruleset or build_ruleset_rule_v1()

    def decide(self, prev_state: State, prev_attrs: StateAttrs, signals: SignalSet) -> Decision:
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
