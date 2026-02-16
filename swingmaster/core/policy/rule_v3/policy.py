"""Rule-based transition policy v3.

Responsibilities:
  - Delegate transition decisions to v2 policy logic.
  - Persist decline profile metadata in state attrs for downtrend lifecycle modeling.
Must not:
  - Read OHLCV directly; uses signals and existing policy contracts only.
"""

from __future__ import annotations

import json
from typing import Optional

from swingmaster.core.domain.enums import State
from swingmaster.core.domain.models import Decision, StateAttrs
from swingmaster.core.policy.ports.state_history_port import StateHistoryPort
from swingmaster.core.policy.rule_v2 import RuleBasedTransitionPolicyV2Impl
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import SignalSet


PROFILE_SLOW_DRIFT = "SLOW_DRIFT"
PROFILE_SHARP_SELL_OFF = "SHARP_SELL_OFF"
PROFILE_STRUCTURAL = "STRUCTURAL_DOWNTREND"
PROFILE_UNKNOWN = "UNKNOWN"

SPECIFIC_PROFILES = {
    PROFILE_SLOW_DRIFT,
    PROFILE_SHARP_SELL_OFF,
    PROFILE_STRUCTURAL,
}


class RuleBasedTransitionPolicyV3Impl:
    def __init__(
        self,
        ruleset: Optional[object] = None,
        history_port: Optional[StateHistoryPort] = None,
    ) -> None:
        self._v2 = RuleBasedTransitionPolicyV2Impl(ruleset=ruleset, history_port=history_port)

    def decide(
        self,
        prev_state: State,
        prev_attrs: StateAttrs,
        signals: SignalSet,
        ticker: Optional[str] = None,
        as_of_date: Optional[str] = None,
    ) -> Decision:
        decision = self._v2.decide(
            prev_state,
            prev_attrs,
            signals,
            ticker=ticker,
            as_of_date=as_of_date,
        )

        prev_origin, prev_profile = _resolve_prev_attrs(prev_attrs)

        next_origin = prev_origin
        next_profile = prev_profile

        candidate_profile = _classify_decline_profile(signals)

        if prev_state == State.NO_TRADE and decision.next_state == State.DOWNTREND_EARLY:
            next_origin = _resolve_downtrend_origin(signals, prev_origin)
            next_profile = _apply_one_way_profile(prev_profile, candidate_profile, allow_unknown=True)
        elif (
            prev_state in {State.DOWNTREND_EARLY, State.DOWNTREND_LATE}
            and decision.next_state in {State.DOWNTREND_EARLY, State.DOWNTREND_LATE}
        ):
            next_profile = _upgrade_unknown_profile(prev_profile, candidate_profile)

        attrs = decision.attrs_update or prev_attrs
        status = _merge_status_json(attrs.status, next_origin, next_profile)

        return Decision(
            next_state=decision.next_state,
            reason_codes=decision.reason_codes,
            attrs_update=StateAttrs(
                confidence=attrs.confidence,
                age=attrs.age,
                status=status,
                downtrend_origin=next_origin,
                decline_profile=next_profile,
            ),
        )


def _classify_decline_profile(signals: SignalSet) -> str:
    if signals.has(SignalKey.SLOW_DRIFT_DETECTED):
        return PROFILE_SLOW_DRIFT
    if signals.has(SignalKey.SHARP_SELL_OFF_DETECTED):
        return PROFILE_SHARP_SELL_OFF
    if (
        signals.has(SignalKey.STRUCTURAL_DOWNTREND_DETECTED)
        or signals.has(SignalKey.TREND_MATURED)
        or signals.has(SignalKey.DOW_TREND_DOWN)
    ):
        return PROFILE_STRUCTURAL
    return PROFILE_UNKNOWN


def _resolve_downtrend_origin(signals: SignalSet, prev_origin: Optional[str]) -> Optional[str]:
    if signals.has(SignalKey.TREND_STARTED):
        return "TREND"
    if signals.has(SignalKey.SLOW_DECLINE_STARTED):
        return "SLOW"
    return prev_origin


def _apply_one_way_profile(
    prev_profile: Optional[str],
    candidate_profile: str,
    *,
    allow_unknown: bool,
) -> str:
    if prev_profile in SPECIFIC_PROFILES:
        return prev_profile
    if prev_profile == PROFILE_UNKNOWN:
        if candidate_profile in SPECIFIC_PROFILES:
            return candidate_profile
        return PROFILE_UNKNOWN
    if candidate_profile in SPECIFIC_PROFILES:
        return candidate_profile
    if allow_unknown:
        return PROFILE_UNKNOWN
    return prev_profile or PROFILE_UNKNOWN


def _upgrade_unknown_profile(prev_profile: Optional[str], candidate_profile: str) -> Optional[str]:
    if prev_profile in SPECIFIC_PROFILES:
        return prev_profile
    if prev_profile == PROFILE_UNKNOWN and candidate_profile in SPECIFIC_PROFILES:
        return candidate_profile
    return prev_profile


def _resolve_prev_attrs(prev_attrs: StateAttrs) -> tuple[Optional[str], Optional[str]]:
    origin = prev_attrs.downtrend_origin
    profile = prev_attrs.decline_profile
    if (origin is not None) and (profile is not None):
        return origin, profile
    if not prev_attrs.status:
        return origin, profile
    try:
        parsed = json.loads(prev_attrs.status)
    except Exception:
        return origin, profile
    if not isinstance(parsed, dict):
        return origin, profile
    if origin is None:
        value = parsed.get("downtrend_origin")
        if isinstance(value, str):
            origin = value
    if profile is None:
        value = parsed.get("decline_profile")
        if isinstance(value, str):
            profile = value
    return origin, profile


def _merge_status_json(
    status: Optional[str],
    downtrend_origin: Optional[str],
    decline_profile: Optional[str],
) -> Optional[str]:
    payload = {}
    if status:
        try:
            parsed = json.loads(status)
            if isinstance(parsed, dict):
                payload.update(parsed)
        except Exception:
            payload = {}

    if isinstance(downtrend_origin, str):
        payload["downtrend_origin"] = downtrend_origin
    elif "downtrend_origin" in payload:
        del payload["downtrend_origin"]

    if isinstance(decline_profile, str):
        payload["decline_profile"] = decline_profile
    elif "decline_profile" in payload:
        del payload["decline_profile"]

    if not payload:
        return None
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
