"""Construct a fully wired app instance for running the engine.

Responsibilities:
  - Assemble providers, policies, and persistence ports based on config.
Must not:
  - Implement signal/policy logic; composition only.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from swingmaster.app_api.facade import SwingmasterApplication
from swingmaster.app_api.providers.osakedata_signal_provider_v1 import OsakeDataSignalProviderV1
from swingmaster.app_api.providers.sqlite_prev_state_provider import SQLitePrevStateProvider
from swingmaster.app_api.factories.policy_factory import build_policy
from swingmaster.app_api.factories.signal_provider_factory import build_signal_provider


def build_swingmaster_app(
    conn: sqlite3.Connection,
    policy_version: str = "v1",
    enable_history: bool = True,
    provider: str = "osakedata_v2",
    debug: bool = False,
    **kwargs: Any,
) -> SwingmasterApplication:
    """
    Composition root: build and wire all runtime components (policy, providers, ports)
    and return the application facade.
    """
    md_conn = kwargs.pop("md_conn", conn)
    policy_id = kwargs.pop("policy_id", "rule_v1" if policy_version == "v1" else "rule_v2")
    engine_version = kwargs.pop("engine_version", "dev")
    history_table = kwargs.pop("history_table", "rc_state_daily")
    table_name = kwargs.pop("table_name", "osakedata")
    require_row_on_date = kwargs.pop("require_row_on_date", False)

    policy = build_policy(
        conn,
        policy_version=policy_version,
        history_table=history_table,
        enable_history=enable_history,
    )

    if provider == "osakedata_v1":
        signal_provider = OsakeDataSignalProviderV1(md_conn, table_name=table_name)
    else:
        signal_provider = build_signal_provider(
            provider=provider,
            conn=md_conn,
            table_name=table_name,
            require_row_on_date=require_row_on_date,
            debug=debug,
            **kwargs,
        )

    prev_state_provider = SQLitePrevStateProvider(conn)

    return SwingmasterApplication(
        conn=conn,
        policy=policy,
        signal_provider=signal_provider,
        prev_state_provider=prev_state_provider,
        engine_version=engine_version,
        policy_id=policy_id,
        policy_version=policy_version,
    )
