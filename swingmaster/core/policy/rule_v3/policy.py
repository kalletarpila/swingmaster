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

from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import Decision, StateAttrs
from swingmaster.core.policy.ports.state_history_port import StateHistoryPort
from swingmaster.core.policy.rule_v2 import RuleBasedTransitionPolicyV2Impl
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import SignalSet


PROFILE_SLOW_DRIFT = "SLOW_DRIFT"
PROFILE_SHARP_SELL_OFF = "SHARP_SELL_OFF"
PROFILE_STRUCTURAL = "STRUCTURAL_DOWNTREND"
PROFILE_UNKNOWN = "UNKNOWN"
PHASE_EARLY_STABILIZATION = "EARLY_STABILIZATION"
PHASE_BASE_BUILDING = "BASE_BUILDING"
PHASE_EARLY_REVERSAL = "EARLY_REVERSAL"
ENTRY_GATE_A = "EARLY_STAB_MA20_HL"
ENTRY_GATE_B = "EARLY_STAB_MA20"
ENTRY_GATE_LEGACY = "LEGACY_ENTRY_SETUP_VALID"
ENTRY_QUALITY_A = "A"
ENTRY_QUALITY_B = "B"
ENTRY_QUALITY_LEGACY = "LEGACY"
ENTRY_TYPE_SLOW_STRUCTURAL = "SLOW_STRUCTURAL"
ENTRY_TYPE_SLOW_SOFT = "SLOW_SOFT"
ENTRY_TYPE_TREND_STRUCTURAL = "TREND_STRUCTURAL"
ENTRY_TYPE_TREND_SOFT = "TREND_SOFT"
ENTRY_TYPE_UNKNOWN = "UNKNOWN"

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

        (
            prev_origin,
            prev_entry_type,
            prev_profile,
            prev_stabilization_phase,
            prev_entry_gate,
            prev_entry_quality,
        ) = _resolve_prev_attrs(prev_attrs)

        next_origin = prev_origin
        next_entry_type = prev_entry_type
        next_profile = prev_profile
        final_next_state = decision.next_state
        gate_a_triggered = False
        gate_b_triggered = False
        if decision.next_state == State.STABILIZING:
            if (
                signals.has(SignalKey.MA20_RECLAIMED)
                and signals.has(SignalKey.HIGHER_LOW_CONFIRMED)
                and not signals.has(SignalKey.INVALIDATED)
            ):
                final_next_state = State.ENTRY_WINDOW
                gate_a_triggered = True
            elif (
                signals.has(SignalKey.MA20_RECLAIMED)
                and not signals.has(SignalKey.HIGHER_LOW_CONFIRMED)
                and not signals.has(SignalKey.INVALIDATED)
            ):
                final_next_state = State.ENTRY_WINDOW
                gate_b_triggered = True

        next_stabilization_phase = _resolve_stabilization_phase(
            final_next_state,
            signals,
            prev_stabilization_phase,
        )
        next_entry_gate = prev_entry_gate
        next_entry_quality = prev_entry_quality
        if gate_a_triggered:
            next_entry_gate = ENTRY_GATE_A
            next_entry_quality = ENTRY_QUALITY_A
        elif gate_b_triggered:
            next_entry_gate = ENTRY_GATE_B
            next_entry_quality = ENTRY_QUALITY_B
        elif (
            final_next_state == State.ENTRY_WINDOW
            and prev_entry_gate is None
            and prev_entry_quality is None
        ):
            next_entry_gate = ENTRY_GATE_LEGACY
            next_entry_quality = ENTRY_QUALITY_LEGACY

        candidate_profile = _classify_decline_profile(signals)

        if prev_state == State.NO_TRADE and final_next_state == State.DOWNTREND_EARLY:
            next_origin = _resolve_downtrend_origin(signals, prev_origin)
            if next_entry_type is None:
                next_entry_type = _classify_downtrend_entry_type(signals)
            if ReasonCode.TREND_STARTED in decision.reason_codes:
                if next_entry_type == ENTRY_TYPE_SLOW_STRUCTURAL:
                    next_entry_type = ENTRY_TYPE_TREND_STRUCTURAL
                elif next_entry_type == ENTRY_TYPE_SLOW_SOFT:
                    next_entry_type = ENTRY_TYPE_TREND_SOFT
            next_profile = _apply_one_way_profile(prev_profile, candidate_profile, allow_unknown=True)
        elif (
            prev_state in {State.DOWNTREND_EARLY, State.DOWNTREND_LATE}
            and final_next_state in {State.DOWNTREND_EARLY, State.DOWNTREND_LATE}
        ):
            next_profile = _upgrade_unknown_profile(prev_profile, candidate_profile)

        if final_next_state == State.NO_TRADE:
            next_origin = None
            next_entry_type = None
            next_profile = None
            next_stabilization_phase = None
            next_entry_gate = None
            next_entry_quality = None

        attrs = decision.attrs_update or prev_attrs
        status_source = attrs.status
        if final_next_state == State.NO_TRADE and status_source is None:
            status_source = prev_attrs.status
        status = _merge_status_json(
            status_source,
            next_origin,
            next_entry_type,
            next_profile,
            next_stabilization_phase,
            next_entry_gate,
            next_entry_quality,
        )

        return Decision(
            next_state=final_next_state,
            reason_codes=decision.reason_codes,
            attrs_update=StateAttrs(
                confidence=attrs.confidence,
                age=attrs.age,
                status=status,
                downtrend_origin=next_origin,
                downtrend_entry_type=next_entry_type,
                decline_profile=next_profile,
                stabilization_phase=next_stabilization_phase,
                entry_gate=next_entry_gate,
                entry_quality=next_entry_quality,
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


def _classify_downtrend_entry_type(signals: SignalSet) -> str:
    origin = _resolve_entry_origin_for_entry_type(signals)
    structural_confirmed = (
        signals.has(SignalKey.STRUCTURAL_DOWNTREND_DETECTED)
        or signals.has(SignalKey.DOW_TREND_DOWN)
        or signals.has(SignalKey.DOW_NEW_LL)
        or signals.has(SignalKey.DOW_BOS_BREAK_DOWN)
    )
    if origin == "SLOW" and structural_confirmed:
        return ENTRY_TYPE_SLOW_STRUCTURAL
    if origin == "SLOW":
        return ENTRY_TYPE_SLOW_SOFT
    if origin == "TREND" and structural_confirmed:
        return ENTRY_TYPE_TREND_STRUCTURAL
    if origin == "TREND":
        return ENTRY_TYPE_TREND_SOFT
    return ENTRY_TYPE_UNKNOWN


def _resolve_entry_origin_for_entry_type(signals: SignalSet) -> str:
    if signals.has(SignalKey.SLOW_DECLINE_STARTED):
        return "SLOW"
    if signals.has(SignalKey.TREND_STARTED):
        return "TREND"
    return "UNKNOWN"


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


def _resolve_stabilization_phase(
    to_state: State,
    signals: SignalSet,
    prev_phase: Optional[str],
) -> Optional[str]:
    if to_state == State.STABILIZING:
        if signals.has(SignalKey.ENTRY_SETUP_VALID) and not signals.has(SignalKey.INVALIDATED):
            return PHASE_EARLY_REVERSAL
        if (
            signals.has(SignalKey.STABILIZATION_CONFIRMED)
            and signals.has(SignalKey.VOLATILITY_COMPRESSION_DETECTED)
            and not signals.has(SignalKey.INVALIDATED)
        ):
            return PHASE_BASE_BUILDING
        return PHASE_EARLY_STABILIZATION
    if to_state == State.ENTRY_WINDOW:
        return PHASE_EARLY_REVERSAL
    return prev_phase


def _resolve_prev_attrs(
    prev_attrs: StateAttrs,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    origin = prev_attrs.downtrend_origin
    entry_type = prev_attrs.downtrend_entry_type
    profile = prev_attrs.decline_profile
    stabilization_phase = prev_attrs.stabilization_phase
    entry_gate = prev_attrs.entry_gate
    entry_quality = prev_attrs.entry_quality

    if (
        origin is None
        or entry_type is None
        or profile is None
        or stabilization_phase is None
        or entry_gate is None
        or entry_quality is None
    ) and prev_attrs.status:
        try:
            parsed = json.loads(prev_attrs.status)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            if origin is None:
                value = parsed.get("downtrend_origin")
                if isinstance(value, str):
                    origin = value
            if entry_type is None:
                value = parsed.get("downtrend_entry_type")
                if isinstance(value, str):
                    entry_type = value
            if profile is None:
                value = parsed.get("decline_profile")
                if isinstance(value, str):
                    profile = value
            if stabilization_phase is None:
                value = parsed.get("stabilization_phase")
                if isinstance(value, str):
                    stabilization_phase = value
            if entry_gate is None:
                value = parsed.get("entry_gate")
                if isinstance(value, str):
                    entry_gate = value
            if entry_quality is None:
                value = parsed.get("entry_quality")
                if isinstance(value, str):
                    entry_quality = value

    return origin, entry_type, profile, stabilization_phase, entry_gate, entry_quality


def _merge_status_json(
    status: Optional[str],
    downtrend_origin: Optional[str],
    downtrend_entry_type: Optional[str],
    decline_profile: Optional[str],
    stabilization_phase: Optional[str],
    entry_gate: Optional[str],
    entry_quality: Optional[str],
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

    if isinstance(downtrend_entry_type, str):
        payload["downtrend_entry_type"] = downtrend_entry_type
    elif "downtrend_entry_type" in payload:
        del payload["downtrend_entry_type"]

    if isinstance(decline_profile, str):
        payload["decline_profile"] = decline_profile
    elif "decline_profile" in payload:
        del payload["decline_profile"]

    if isinstance(stabilization_phase, str):
        payload["stabilization_phase"] = stabilization_phase
    elif "stabilization_phase" in payload:
        del payload["stabilization_phase"]

    if isinstance(entry_gate, str):
        payload["entry_gate"] = entry_gate
    elif "entry_gate" in payload:
        del payload["entry_gate"]

    if isinstance(entry_quality, str):
        payload["entry_quality"] = entry_quality
    elif "entry_quality" in payload:
        del payload["entry_quality"]

    if not payload:
        return None
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
