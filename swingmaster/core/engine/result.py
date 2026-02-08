"""Evaluation result payload for a single state-machine step.

Responsibilities:
  - Capture final state, reasons, and transition metadata for persistence/audit.

Inputs/Outputs:
  - Inputs: produced by evaluator.evaluate_step.
  - Outputs: immutable dataclass consumed by app/infra layers.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..domain.enums import ReasonCode, State
from ..domain.models import StateAttrs, Transition


@dataclass
class EvaluationResult:
    prev_state: State
    final_state: State
    reasons: list[ReasonCode]
    transition: Transition | None
    final_attrs: StateAttrs
    guardrails_blocked: bool
    proposed_state: State
