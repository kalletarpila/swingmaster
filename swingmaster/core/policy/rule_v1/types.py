"""Shared type definitions for rule_v1 policy rules.

Responsibilities:
  - Define Proposal dataclass used to carry next state + reasons.
Must not:
  - Implement logic; data-only types for rule evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from swingmaster.core.domain.enums import ReasonCode, State


@dataclass(frozen=True)
class Proposal:
    next_state: State
    reasons: List[ReasonCode]
