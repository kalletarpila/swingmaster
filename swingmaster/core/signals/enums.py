from __future__ import annotations

from enum import Enum


# Signals are observations only; different extractors may emit them in the future.
class SignalKey(Enum):
    TREND_STARTED = "TREND_STARTED"
    TREND_MATURED = "TREND_MATURED"
    SELLING_PRESSURE_EASED = "SELLING_PRESSURE_EASED"
    STABILIZATION_CONFIRMED = "STABILIZATION_CONFIRMED"
    ENTRY_SETUP_VALID = "ENTRY_SETUP_VALID"
    EDGE_GONE = "EDGE_GONE"
    INVALIDATED = "INVALIDATED"
    DATA_INSUFFICIENT = "DATA_INSUFFICIENT"
    NO_SIGNAL = "NO_SIGNAL"
