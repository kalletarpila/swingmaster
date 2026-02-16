"""Factory for creating policy instances from app configuration."""

from __future__ import annotations

import sqlite3

from swingmaster.app_api.factories.policy_factory_v1 import build_rule_policy_v1
from swingmaster.app_api.factories.policy_factory_v2 import build_rule_policy_v2
from swingmaster.app_api.factories.policy_factory_v3 import build_rule_policy_v3
from swingmaster.app_api.factories.policy_versions import (
    ALLOWED_POLICY_VERSIONS,
    POLICY_V1,
    POLICY_V2,
    POLICY_V3,
)


def build_policy(
    conn: sqlite3.Connection,
    *,
    policy_version: str = "v2",
    history_table: str = "rc_state_daily",
    enable_history: bool = True,
):
    """Composition root: build a policy instance by version.

    Supported versions: "v2" (v1 disabled).
    Wiring is deterministic; history is optional but recommended.
    """
    if policy_version == POLICY_V1:
        raise RuntimeError("v1 disabled")
    if policy_version == POLICY_V2:
        return build_rule_policy_v2(
            conn,
            history_table=history_table,
            enable_history=enable_history,
        )
    if policy_version == POLICY_V3:
        return build_rule_policy_v3(
            conn,
            history_table=history_table,
            enable_history=enable_history,
        )
    allowed = ", ".join(sorted(ALLOWED_POLICY_VERSIONS))
    raise ValueError(f"Unsupported policy_version: {policy_version}. Allowed: {allowed}")
