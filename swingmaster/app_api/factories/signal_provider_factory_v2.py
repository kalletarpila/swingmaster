from __future__ import annotations

import sqlite3
from typing import List

from swingmaster.app_api.ports import SignalProvider
from swingmaster.app_api.providers.osakedata_signal_provider_v2 import OsakeDataSignalProviderV2


def build_osakedata_signal_provider_v2(
    conn: sqlite3.Connection,
    *,
    table_name: str = "osakedata",
    require_row_on_date: bool = False,
    sma_window: int = 20,
    momentum_lookback: int = 1,
    matured_below_sma_days: int = 5,
    atr_window: int = 14,
    stabilization_days: int = 5,
    atr_pct_threshold: float = 0.03,
    range_pct_threshold: float = 0.05,
    entry_sma_window: int = 5,
    invalidation_lookback: int = 10,
) -> OsakeDataSignalProviderV2:
    """Composition root for signals; infra wiring only; deterministic args; no global state."""
    return OsakeDataSignalProviderV2(
        conn,
        table_name=table_name,
        sma_window=sma_window,
        momentum_lookback=momentum_lookback,
        matured_below_sma_days=matured_below_sma_days,
        atr_window=atr_window,
        stabilization_days=stabilization_days,
        atr_pct_threshold=atr_pct_threshold,
        range_pct_threshold=range_pct_threshold,
        entry_sma_window=entry_sma_window,
        invalidation_lookback=invalidation_lookback,
        require_row_on_date=require_row_on_date,
    )


def build_signal_providers_v2(
    conn: sqlite3.Connection,
    *,
    osakedata_enabled: bool = True,
    table_name: str = "osakedata",
    require_row_on_date: bool = False,
    sma_window: int = 20,
    momentum_lookback: int = 1,
    matured_below_sma_days: int = 5,
    atr_window: int = 14,
    stabilization_days: int = 5,
    atr_pct_threshold: float = 0.03,
    range_pct_threshold: float = 0.05,
    entry_sma_window: int = 5,
    invalidation_lookback: int = 10,
) -> List[SignalProvider]:
    providers: List[SignalProvider] = []
    if osakedata_enabled:
        providers.append(
            build_osakedata_signal_provider_v2(
                conn,
                table_name=table_name,
                require_row_on_date=require_row_on_date,
                sma_window=sma_window,
                momentum_lookback=momentum_lookback,
                matured_below_sma_days=matured_below_sma_days,
                atr_window=atr_window,
                stabilization_days=stabilization_days,
                atr_pct_threshold=atr_pct_threshold,
                range_pct_threshold=range_pct_threshold,
                entry_sma_window=entry_sma_window,
                invalidation_lookback=invalidation_lookback,
            )
        )
    return providers
