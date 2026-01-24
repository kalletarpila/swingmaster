from __future__ import annotations

import sqlite3
from typing import List

from swingmaster.app_api.ports import SignalProvider
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet
from swingmaster.infra.market_data.osakedata_reader import OsakeDataReader

SAFETY_MARGIN_ROWS = 2


class OsakeDataSignalProviderV2(SignalProvider):
    def __init__(
        self,
        conn: sqlite3.Connection,
        table_name: str = "osakedata",
        sma_window: int = 20,
        momentum_lookback: int = 1,
        matured_below_sma_days: int = 5,
        atr_window: int = 14,
        stabilization_days: int = 5,
        atr_pct_threshold: float = 0.03,
        range_pct_threshold: float = 0.05,
        entry_sma_window: int = 5,
        invalidation_lookback: int = 10,
        require_row_on_date: bool = False,
    ) -> None:
        for name, value, min_val in [
            ("sma_window", sma_window, 2),
            ("momentum_lookback", momentum_lookback, 1),
            ("matured_below_sma_days", matured_below_sma_days, 1),
            ("atr_window", atr_window, 2),
            ("stabilization_days", stabilization_days, 1),
            ("entry_sma_window", entry_sma_window, 2),
            ("invalidation_lookback", invalidation_lookback, 1),
        ]:
            if value < min_val:
                raise ValueError(f"Invalid parameters: {name} must be >= {min_val}")
        if not isinstance(require_row_on_date, bool):
            raise ValueError("require_row_on_date must be a bool")
        self._reader = OsakeDataReader(conn, table_name)
        self._sma_window = sma_window
        self._momentum_lookback = momentum_lookback
        self._matured_below_sma_days = matured_below_sma_days
        self._atr_window = atr_window
        self._stabilization_days = stabilization_days
        self._atr_pct_threshold = atr_pct_threshold
        self._range_pct_threshold = range_pct_threshold
        self._entry_sma_window = entry_sma_window
        self._invalidation_lookback = invalidation_lookback
        self._require_row_on_date = require_row_on_date

    def get_signals(self, ticker: str, date: str) -> SignalSet:
        required = self._required_rows()
        ohlc = self._reader.get_last_n_ohlc(ticker, date, required)
        if len(ohlc) < required:
            return self._insufficient()
        if self._require_row_on_date:
            if ohlc[0][0] != date:
                return self._insufficient()

        closes = [row[4] for row in ohlc]
        highs = [row[2] for row in ohlc]
        lows = [row[3] for row in ohlc]

        signals = {}

        # TREND_STARTED
        if len(closes) > self._momentum_lookback and len(closes) >= self._sma_window:
            latest = closes[0]
            prev = closes[self._momentum_lookback]
            sma20 = sum(closes[: self._sma_window]) / float(self._sma_window)
            if latest > prev and latest > sma20:
                signals[SignalKey.TREND_STARTED] = self._signal(SignalKey.TREND_STARTED)

        # TREND_MATURED
        if len(closes) >= max(self._sma_window, self._matured_below_sma_days):
            sma20_today = sum(closes[: self._sma_window]) / float(self._sma_window)
            last_below = all(c < sma20_today for c in closes[: self._matured_below_sma_days])
            slope_neg = False
            if len(closes) >= self._sma_window + 5:
                sma20_5ago = sum(closes[5 : 5 + self._sma_window]) / float(self._sma_window)
                slope_neg = sma20_today < sma20_5ago
            if last_below or slope_neg:
                signals[SignalKey.TREND_MATURED] = self._signal(SignalKey.TREND_MATURED)

        # STABILIZATION_CONFIRMED
        if len(ohlc) >= max(self._atr_window + 1, self._stabilization_days + 1):
            atr = self._compute_atr(ohlc[: self._atr_window + 1])
            latest_close = closes[0]
            atr_pct = atr / latest_close if latest_close else 0.0
            window_high = max(highs[: self._stabilization_days])
            window_low = min(lows[: self._stabilization_days])
            range_pct = (window_high - window_low) / latest_close if latest_close else 0.0
            if atr_pct <= self._atr_pct_threshold and range_pct <= self._range_pct_threshold:
                signals[SignalKey.STABILIZATION_CONFIRMED] = self._signal(SignalKey.STABILIZATION_CONFIRMED)

        # ENTRY_SETUP_VALID
        if len(ohlc) >= max(self._stabilization_days + 1, self._entry_sma_window):
            latest_close = closes[0]
            prev_highs = highs[1 : 1 + self._stabilization_days]
            if prev_highs:
                breakout_level = max(prev_highs)
                sma_entry = sum(closes[: self._entry_sma_window]) / float(self._entry_sma_window)
                if latest_close > breakout_level and latest_close > sma_entry:
                    signals[SignalKey.ENTRY_SETUP_VALID] = self._signal(SignalKey.ENTRY_SETUP_VALID)

        # INVALIDATED
        if len(ohlc) >= self._invalidation_lookback + 1:
            prior_lows = lows[1 : self._invalidation_lookback + 1]
            min_low = min(prior_lows) if prior_lows else lows[0]
            if lows[0] < min_low:
                signals[SignalKey.INVALIDATED] = self._signal(SignalKey.INVALIDATED)

        if not signals:
            return SignalSet(signals={})
        return SignalSet(signals=signals)

    def _compute_atr(self, ohlc: List[tuple]) -> float:
        # ohlc ordered DESC by date, length >= 2
        trs = []
        for i in range(len(ohlc) - 1):
            _, _o, h, l, c, _v = ohlc[i]
            prev_close = ohlc[i + 1][4]
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
            trs.append(tr)
        if not trs:
            return 0.0
        return sum(trs[: self._atr_window]) / float(len(trs[: self._atr_window]))

    def _signal(self, key: SignalKey) -> Signal:
        return Signal(key=key, value=True, confidence=None, source="osakedata_v2")

    def _required_rows(self) -> int:
        return max(
            self._sma_window + self._momentum_lookback,
            self._sma_window + 5,  # slope check
            self._atr_window + 1,
            max(self._stabilization_days + 1, self._entry_sma_window),
            self._invalidation_lookback + 1,
        ) + SAFETY_MARGIN_ROWS

    def _insufficient(self) -> SignalSet:
        return SignalSet(
            signals={
                SignalKey.DATA_INSUFFICIENT: Signal(
                    key=SignalKey.DATA_INSUFFICIENT,
                    value=True,
                    confidence=None,
                    source="osakedata_v2",
                )
            }
        )
