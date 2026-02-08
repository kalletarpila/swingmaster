"""Allowed state transitions for the deterministic state machine.

Responsibilities:
  - Define legal next states per current state.
  - Guardrails and policies must respect this graph.

Invariants:
  - Must remain stable for auditability; changes require coordinated migration.
"""

from __future__ import annotations

from .enums import State

ALLOWED_TRANSITIONS: dict[State, set[State]] = {
    State.NO_TRADE: {State.NO_TRADE, State.DOWNTREND_EARLY},
    State.DOWNTREND_EARLY: {State.DOWNTREND_EARLY, State.DOWNTREND_LATE, State.STABILIZING, State.NO_TRADE},
    State.DOWNTREND_LATE: {State.DOWNTREND_LATE, State.STABILIZING, State.NO_TRADE},
    State.STABILIZING: {State.STABILIZING, State.ENTRY_WINDOW, State.NO_TRADE},
    State.ENTRY_WINDOW: {State.ENTRY_WINDOW, State.PASS, State.NO_TRADE},
    State.PASS: {State.PASS, State.NO_TRADE},
}
