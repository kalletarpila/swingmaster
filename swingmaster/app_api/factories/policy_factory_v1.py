from __future__ import annotations

import sqlite3

from swingmaster.app_api.policy_factory import build_rule_policy_v1 as _build_rule_policy_v1
from swingmaster.core.policy.rule_policy_v1 import RuleBasedTransitionPolicyV1


def build_rule_policy_v1(
    conn: sqlite3.Connection,
    *,
    history_table: str = "rc_state_daily",
    enable_history: bool = True,
) -> RuleBasedTransitionPolicyV1:
    return _build_rule_policy_v1(conn, history_table=history_table, enable_history=enable_history)
