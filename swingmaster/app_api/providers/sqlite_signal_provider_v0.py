from __future__ import annotations

import sqlite3

from swingmaster.app_api.ports import SignalProvider
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet
from swingmaster.infra.market_data.ohlcv_reader import OhlcvReader


class SQLiteSignalProviderV0(SignalProvider):
    def __init__(
        self,
        conn: sqlite3.Connection,
        table_name: str = "daily_ohlcv",
        ticker_col: str = "ticker",
        date_col: str = "date",
        close_col: str = "close",
    ) -> None:
        self._reader = OhlcvReader(
            conn,
            table_name=table_name,
            ticker_col=ticker_col,
            date_col=date_col,
            close_col=close_col,
        )

    def get_signals(self, ticker: str, date: str) -> SignalSet:
        closes = self._reader.get_last_n_closes(ticker, date, n=21)
        if len(closes) < 21:
            return SignalSet(
                signals={
                    SignalKey.DATA_INSUFFICIENT: Signal(
                        key=SignalKey.DATA_INSUFFICIENT,
                        value=True,
                        confidence=None,
                        source="ohlcv_v1",
                    )
                }
            )

        latest = closes[0]
        prev = closes[1]
        sma20 = sum(closes[0:20]) / 20.0

        if latest > prev and latest > sma20:
            return SignalSet(
                signals={
                    SignalKey.TREND_STARTED: Signal(
                        key=SignalKey.TREND_STARTED,
                        value=True,
                        confidence=None,
                        source="ohlcv_v1",
                    )
                }
            )

        return SignalSet(signals={})
