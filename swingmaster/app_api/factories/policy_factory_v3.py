"""Factory for constructing rule_v3 policy instances."""

from __future__ import annotations

import sqlite3

from swingmaster.core.policy.rule_policy_v3 import RuleBasedTransitionPolicyV3
from swingmaster.infra.sqlite.state_history_port_sqlite import StateHistoryPortSqlite


def build_rule_policy_v3(
    conn: sqlite3.Connection,
    *,
    history_table: str = "rc_state_daily",
    enable_history: bool = True,
) -> RuleBasedTransitionPolicyV3:
    """Composition root for policy v3 with deterministic wiring."""
    if enable_history:
        history_port = StateHistoryPortSqlite(conn, table_name=history_table)
        return RuleBasedTransitionPolicyV3(history_port=history_port)
    return RuleBasedTransitionPolicyV3()
