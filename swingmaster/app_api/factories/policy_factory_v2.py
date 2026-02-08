"""Factory for constructing rule_v2 policy instances."""

from __future__ import annotations

import sqlite3

from swingmaster.core.policy.rule_policy_v2 import RuleBasedTransitionPolicyV2
from swingmaster.infra.sqlite.state_history_port_sqlite import StateHistoryPortSqlite


def build_rule_policy_v2(
    conn: sqlite3.Connection,
    *,
    history_table: str = "rc_state_daily",
    enable_history: bool = True,
) -> RuleBasedTransitionPolicyV2:
    """Composition root for policy v2 with deterministic wiring.

    History is optional but recommended for RESET_TO_NEUTRAL evaluation.
    """
    if enable_history:
        history_port = StateHistoryPortSqlite(conn, table_name=history_table)
        return RuleBasedTransitionPolicyV2(history_port=history_port)
    return RuleBasedTransitionPolicyV2()
