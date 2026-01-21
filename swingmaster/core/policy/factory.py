from __future__ import annotations

from typing import Callable, Dict, Tuple

from swingmaster.core.policy.rule_policy_v1 import RuleBasedTransitionPolicyV1
from swingmaster.core.engine.evaluator import TransitionPolicy


class PolicyFactory:
    def __init__(self) -> None:
        self._registry: Dict[Tuple[str, str], Callable[[], TransitionPolicy]] = {}

    def register(self, policy_id: str, policy_version: str, builder: Callable[[], TransitionPolicy]) -> None:
        self._registry[(policy_id, policy_version)] = builder

    def create(self, policy_id: str, policy_version: str) -> TransitionPolicy:
        key = (policy_id, policy_version)
        if key not in self._registry:
            raise ValueError(f"Unknown policy_id/version: {policy_id}:{policy_version}")
        return self._registry[key]()


default_policy_factory = PolicyFactory()
default_policy_factory.register("rule_v1", "dev", lambda: RuleBasedTransitionPolicyV1())
default_policy_factory.register("rule_v1", "v1", lambda: RuleBasedTransitionPolicyV1())

__all__ = ["PolicyFactory", "default_policy_factory"]
