from __future__ import annotations

import sqlite3

from swingmaster.core.policy.rule_policy_v1 import RuleBasedTransitionPolicyV1
from swingmaster.infra.sqlite.state_history_port_sqlite import StateHistoryPortSqlite


def build_rule_policy_v1(
    conn: sqlite3.Connection,
    *,
    history_table: str = "rc_state_daily",
    enable_history: bool = True,
) -> RuleBasedTransitionPolicyV1:
    """Composition root for policy v1 with deterministic wiring.

    History is optional but recommended for RESET_TO_NEUTRAL evaluation.
    """
    if enable_history:
        history_port = StateHistoryPortSqlite(conn, table_name=history_table)
        return RuleBasedTransitionPolicyV1(history_port=history_port)
    return RuleBasedTransitionPolicyV1()
