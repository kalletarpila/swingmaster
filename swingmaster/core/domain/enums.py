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
    TREND_STARTED = "TREND_STARTED"
    TREND_MATURED = "TREND_MATURED"
    SELLING_PRESSURE_EASED = "SELLING_PRESSURE_EASED"
    STABILIZATION_CONFIRMED = "STABILIZATION_CONFIRMED"
    ENTRY_CONDITIONS_MET = "ENTRY_CONDITIONS_MET"
    EDGE_GONE = "EDGE_GONE"
    INVALIDATED = "INVALIDATED"
    RESET_TO_NEUTRAL = "RESET_TO_NEUTRAL"
    CHURN_GUARD = "CHURN_GUARD"
    DATA_INSUFFICIENT = "DATA_INSUFFICIENT"
    NO_SIGNAL = "NO_SIGNAL"


# UI/audit metadata keyed by reason code.
REASON_METADATA: dict[ReasonCode, dict[str, object]] = {
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
    ReasonCode.RESET_TO_NEUTRAL: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Lifecycle reset to neutral state.",
    },
    ReasonCode.CHURN_GUARD: {
        "category": ReasonCategory.EXCLUSION,
        "message": "Transition blocked to prevent rapid oscillation.",
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

_missing = [rc for rc in ReasonCode if rc not in REASON_METADATA]
if _missing:
    raise RuntimeError(f"Missing REASON_METADATA for: {[m.value for m in _missing]}")

_extra = [k for k in REASON_METADATA.keys() if k not in set(ReasonCode)]
if _extra:
    raise RuntimeError(f"Extra REASON_METADATA keys: {[e.value for e in _extra]}")
