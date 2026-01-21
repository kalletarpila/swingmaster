from __future__ import annotations

from .enums import State

ALLOWED_TRANSITIONS: dict[State, set[State]] = {
    State.NO_TRADE: {State.DOWNTREND_EARLY},
    State.DOWNTREND_EARLY: {State.DOWNTREND_LATE, State.STABILIZING, State.NO_TRADE},
    State.DOWNTREND_LATE: {State.STABILIZING, State.NO_TRADE},
    State.STABILIZING: {State.ENTRY_WINDOW, State.NO_TRADE},
    State.ENTRY_WINDOW: {State.PASS, State.NO_TRADE},
    State.PASS: {State.NO_TRADE},
}
