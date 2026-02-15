"""Domain enums for state machine and reasoning.

Responsibilities:
  - Define State and ReasonCode identifiers persisted in RC storage.
  - Provide stable reason categories and audit metadata.

Invariants:
  - Enum values must remain stable for persistence and audits.
  - ReasonCode metadata must be complete and deterministic.
"""

from __future__ import annotations

from enum import Enum


class State(Enum):
    NO_TRADE = "NO_TRADE"
    DOWNTREND_EARLY = "DOWNTREND_EARLY"
    DOWNTREND_LATE = "DOWNTREND_LATE"
    STABILIZING = "STABILIZING"
    ENTRY_WINDOW = "ENTRY_WINDOW"
    PASS = "PASS"


class ReasonCategory(Enum):
    EXCLUSION = "EXCLUSION"
    ENTRY = "ENTRY"
    INFO = "INFO"


# Stable identifiers for decision reasoning; value is the persisted code.
class ReasonCode(Enum):
    SLOW_DECLINE_STARTED = "SLOW_DECLINE_STARTED"
    TREND_STARTED = "TREND_STARTED"
    TREND_MATURED = "TREND_MATURED"
    SELLING_PRESSURE_EASED = "SELLING_PRESSURE_EASED"
    STABILIZATION_CONFIRMED = "STABILIZATION_CONFIRMED"
    ENTRY_CONDITIONS_MET = "ENTRY_CONDITIONS_MET"
    EDGE_GONE = "EDGE_GONE"
    INVALIDATED = "INVALIDATED"
    INVALIDATION_BLOCKED_BY_LOCK = "INVALIDATION_BLOCKED_BY_LOCK"
    DISALLOWED_TRANSITION = "DISALLOWED_TRANSITION"
    PASS_COMPLETED = "PASS_COMPLETED"
    ENTRY_WINDOW_COMPLETED = "ENTRY_WINDOW_COMPLETED"
    RESET_TO_NEUTRAL = "RESET_TO_NEUTRAL"
    CHURN_GUARD = "CHURN_GUARD"
    MIN_STATE_AGE_LOCK = "MIN_STATE_AGE_LOCK"
    DATA_INSUFFICIENT = "DATA_INSUFFICIENT"
    NO_SIGNAL = "NO_SIGNAL"


# UI/audit metadata keyed by reason code.
REASON_METADATA: dict[ReasonCode, dict[str, object]] = {
    ReasonCode.SLOW_DECLINE_STARTED: {
        "category": ReasonCategory.INFO,
        "message": "Slow staircase decline has started.",
    },
    ReasonCode.TREND_STARTED: {
        "category": ReasonCategory.INFO,
        "message": "Trend has begun and is being tracked.",
    },
    ReasonCode.TREND_MATURED: {
        "category": ReasonCategory.INFO,
        "message": "Trend has progressed into a later stage.",
    },
    ReasonCode.SELLING_PRESSURE_EASED: {
        "category": ReasonCategory.INFO,
        "message": "Selling pressure has diminished from prior levels.",
    },
    ReasonCode.STABILIZATION_CONFIRMED: {
        "category": ReasonCategory.INFO,
        "message": "Price action shows signs of stabilizing.",
    },
    ReasonCode.ENTRY_CONDITIONS_MET: {
        "category": ReasonCategory.ENTRY,
        "message": "Entry conditions have been satisfied.",
    },
    ReasonCode.EDGE_GONE: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Previously identified edge is no longer present.",
    },
    ReasonCode.INVALIDATED: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Prior setup or thesis has been invalidated.",
    },
    ReasonCode.INVALIDATION_BLOCKED_BY_LOCK: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Invalidation blocked by minimum age guardrail.",
    },
    ReasonCode.DISALLOWED_TRANSITION: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Proposed transition is not allowed by the transition graph.",
    },
    ReasonCode.PASS_COMPLETED: {
        "category": ReasonCategory.INFO,
        "message": "Pass period completed; returning to neutral.",
    },
    ReasonCode.ENTRY_WINDOW_COMPLETED: {
        "category": ReasonCategory.INFO,
        "message": "Entry window completed; transitioning to pass.",
    },
    ReasonCode.RESET_TO_NEUTRAL: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Lifecycle reset to neutral state.",
    },
    ReasonCode.CHURN_GUARD: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Transition blocked to prevent rapid oscillation.",
    },
    ReasonCode.MIN_STATE_AGE_LOCK: {
        "category": ReasonCategory.EXCLUSION,
        "message": "State blocked by minimum age guardrail.",
    },
    ReasonCode.DATA_INSUFFICIENT: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Available data is insufficient for a decision.",
    },
    ReasonCode.NO_SIGNAL: {
        "category": ReasonCategory.INFO,
        "message": "No actionable signals were present; state remains unchanged.",
    },
}

_REASON_PERSIST_OVERLAP = {
    ReasonCode.DATA_INSUFFICIENT,
    ReasonCode.EDGE_GONE,
    ReasonCode.INVALIDATED,
    ReasonCode.INVALIDATION_BLOCKED_BY_LOCK,
    ReasonCode.PASS_COMPLETED,
    ReasonCode.ENTRY_WINDOW_COMPLETED,
    ReasonCode.NO_SIGNAL,
    ReasonCode.SELLING_PRESSURE_EASED,
    ReasonCode.STABILIZATION_CONFIRMED,
    ReasonCode.TREND_MATURED,
    ReasonCode.TREND_STARTED,
}


def reason_to_persisted(reason: ReasonCode) -> str:
    return f"POLICY:{reason.value}"


def reason_from_persisted(label: str) -> ReasonCode | None:
    if not label:
        return None
    if label.startswith("POLICY:"):
        label = label[len("POLICY:") :]
    try:
        return ReasonCode(label)
    except Exception:
        return None


_missing = [rc for rc in ReasonCode if rc not in REASON_METADATA]
if _missing:
    raise RuntimeError(f"Missing REASON_METADATA for: {[m.value for m in _missing]}")

_extra = [k for k in REASON_METADATA.keys() if k not in set(ReasonCode)]
if _extra:
    raise RuntimeError(f"Extra REASON_METADATA keys: {[e.value for e in _extra]}")
