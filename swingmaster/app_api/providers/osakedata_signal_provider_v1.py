"""Signal provider v1 backed by osakedata OHLCV.

Responsibilities:
  - Fetch OHLCV data and compute SignalSet for each ticker/day.
Must not:
  - Apply policy logic; emits signals only.
"""

from __future__ import annotations

import sqlite3

from swingmaster.app_api.ports import SignalProvider
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet
from swingmaster.infra.market_data.osakedata_reader import OsakeDataReader


class OsakeDataSignalProviderV1(SignalProvider):
    def __init__(
        self,
        conn: sqlite3.Connection,
        table_name: str = "osakedata",
        sma_window: int = 20,
        momentum_lookback: int = 1,
    ) -> None:
        if sma_window < 2 or momentum_lookback < 1:
            raise ValueError("Invalid parameters")
        self._reader = OsakeDataReader(conn, table_name)
        self._sma_window = sma_window
        self._momentum_lookback = momentum_lookback

    def get_signals(self, ticker: str, date: str) -> SignalSet:
        required = self._sma_window + self._momentum_lookback
        closes = self._reader.get_last_n_closes(ticker, date, n=required)

        if len(closes) < required:
            return SignalSet(
                signals={
                    SignalKey.DATA_INSUFFICIENT: Signal(
                        key=SignalKey.DATA_INSUFFICIENT,
                        value=True,
                        confidence=None,
                        source="osakedata_v1",
                    )
                }
            )

        latest = closes[0]
        prev = closes[self._momentum_lookback]
        sma = sum(closes[0 : self._sma_window]) / float(self._sma_window)

        if latest > prev and latest > sma:
            return SignalSet(
                signals={
                    SignalKey.TREND_STARTED: Signal(
                        key=SignalKey.TREND_STARTED,
                        value=True,
                        confidence=None,
                        source="osakedata_v1",
                    )
                }
            )

        return SignalSet(signals={})
