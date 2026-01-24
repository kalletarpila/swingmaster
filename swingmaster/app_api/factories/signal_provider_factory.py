"""Composition root for signal provider wiring.

Providers remain pure and testable; infra dependencies are injected here.
Keep wiring explicit and deterministic.
"""

from __future__ import annotations

import sqlite3

from swingmaster.app_api.ports import SignalProvider
from swingmaster.app_api.providers.osakedata_signal_provider_v2 import OsakeDataSignalProviderV2

SUPPORTED_SIGNAL_PROVIDERS = ("osakedata_v2",)


def list_supported_signal_providers() -> tuple[str, ...]:
    return SUPPORTED_SIGNAL_PROVIDERS


def build_signal_provider(
    *,
    provider: str,
    conn: sqlite3.Connection,
    table_name: str = "osakedata",
    require_row_on_date: bool = False,
    **kwargs,
) -> SignalProvider:
    """Composition root for signal providers.

    Providers remain pure and testable; infra wiring happens here.
    """
    if provider == "osakedata_v2":
        return OsakeDataSignalProviderV2(
            conn,
            table_name=table_name,
            require_row_on_date=require_row_on_date,
            **kwargs,
        )
    raise ValueError(
        f"Unknown signal provider: {provider}. Supported: {list_supported_signal_providers()}"
    )
