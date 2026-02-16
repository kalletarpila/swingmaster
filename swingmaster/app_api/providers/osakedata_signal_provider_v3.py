"""Signal provider v3 backed by osakedata OHLCV.

Responsibilities:
  - Build SignalContextV3 and compute signals_v3 for each ticker/day.
Must not:
  - Apply policy logic; emits signals only.
"""

from __future__ import annotations

import sqlite3
from typing import List

from swingmaster.app_api.ports import SignalProvider
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet
from swingmaster.infra.market_data.osakedata_reader import OsakeDataReader
from swingmaster.app_api.providers.signals_v2.dow_structure import compute_dow_signal_facts
from swingmaster.app_api.providers.signals_v2.entry_setup_valid import eval_entry_setup_valid
from swingmaster.app_api.providers.signals_v2.invalidated import eval_invalidated
from swingmaster.app_api.providers.signals_v2.stabilization_confirmed import eval_stabilization_confirmed
from swingmaster.app_api.providers.signals_v2.trend_matured import eval_trend_matured
from swingmaster.app_api.providers.signals_v2.trend_started import (
    BREAK_LOW_WINDOW,
    REGIME_WINDOW,
    SMA_LEN,
    SLOPE_LOOKBACK,
    eval_trend_started,
)
from swingmaster.app_api.providers.signals_v3.context import SignalContextV3
from swingmaster.app_api.providers.signals_v3.ma20_reclaimed import eval_ma20_reclaimed
from swingmaster.app_api.providers.signals_v3.sharp_sell_off_detected import eval_sharp_sell_off_detected
from swingmaster.app_api.providers.signals_v3.slow_drift_detected import eval_slow_drift_detected
from swingmaster.app_api.providers.signals_v3.structural_downtrend_detected import (
    eval_structural_downtrend_detected,
)
from swingmaster.app_api.providers.signals_v3.volatility_compression_detected import (
    eval_volatility_compression_detected,
)

SAFETY_MARGIN_ROWS = 2


class OsakeDataSignalProviderV3(SignalProvider):
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
        dow_window: int = 3,
        dow_use_high_low: bool = True,
        dow_sensitive_down_reset: bool = False,
        debug: bool = False,
        debug_dow_markers: bool = False,
    ) -> None:
        for name, value, min_val in [
            ("sma_window", sma_window, 2),
            ("momentum_lookback", momentum_lookback, 1),
            ("matured_below_sma_days", matured_below_sma_days, 1),
            ("atr_window", atr_window, 2),
            ("stabilization_days", stabilization_days, 1),
            ("entry_sma_window", entry_sma_window, 2),
            ("invalidation_lookback", invalidation_lookback, 1),
            ("dow_window", dow_window, 2),
        ]:
            if value < min_val:
                raise ValueError(f"Invalid parameters: {name} must be >= {min_val}")
        if not isinstance(require_row_on_date, bool):
            raise ValueError("require_row_on_date must be a bool")
        if not isinstance(debug_dow_markers, bool):
            raise ValueError("debug_dow_markers must be a bool")
        self._table_name = table_name
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
        self._dow_window = dow_window
        self._dow_use_high_low = dow_use_high_low
        self._dow_sensitive_down_reset = dow_sensitive_down_reset
        self._debug = debug
        self._debug_dow_markers = debug_dow_markers

    def get_signals(self, ticker: str, date: str) -> SignalSet:
        required = self._required_rows()
        ohlc = self._reader.get_last_n_ohlc(ticker, date, required)
        if len(ohlc) < required:
            self._debug_insufficient(ticker, date, required, ohlc)
            return self._insufficient()
        if self._require_row_on_date and ohlc[0][0] != date:
            self._debug_insufficient(ticker, date, required, ohlc)
            return self._insufficient()

        closes = [row[4] for row in ohlc]
        highs = [row[2] for row in ohlc]
        lows = [row[3] for row in ohlc]

        ctx = SignalContextV3(closes=closes, highs=highs, lows=lows, ohlc=ohlc, as_of_date=date)
        signals = {}
        primary_signals = set()

        if eval_slow_drift_detected(ctx):
            signals[SignalKey.SLOW_DRIFT_DETECTED] = self._signal(SignalKey.SLOW_DRIFT_DETECTED)
            primary_signals.add(SignalKey.SLOW_DRIFT_DETECTED)
            signals[SignalKey.SLOW_DECLINE_STARTED] = self._signal(SignalKey.SLOW_DECLINE_STARTED)
            primary_signals.add(SignalKey.SLOW_DECLINE_STARTED)

        if eval_sharp_sell_off_detected(ctx, self._compute_atr):
            signals[SignalKey.SHARP_SELL_OFF_DETECTED] = self._signal(SignalKey.SHARP_SELL_OFF_DETECTED)
            primary_signals.add(SignalKey.SHARP_SELL_OFF_DETECTED)

        if eval_volatility_compression_detected(ctx, self._compute_atr):
            signals[SignalKey.VOLATILITY_COMPRESSION_DETECTED] = self._signal(
                SignalKey.VOLATILITY_COMPRESSION_DETECTED
            )
            primary_signals.add(SignalKey.VOLATILITY_COMPRESSION_DETECTED)

        if eval_ma20_reclaimed(ctx):
            signals[SignalKey.MA20_RECLAIMED] = self._signal(SignalKey.MA20_RECLAIMED)
            primary_signals.add(SignalKey.MA20_RECLAIMED)

        trend_started_base = eval_trend_started(ctx, self._sma_window, self._momentum_lookback)

        if eval_trend_matured(ctx, self._sma_window, self._matured_below_sma_days):
            signals[SignalKey.TREND_MATURED] = self._signal(SignalKey.TREND_MATURED)
            primary_signals.add(SignalKey.TREND_MATURED)

        if eval_stabilization_confirmed(
            ctx,
            self._atr_window,
            self._stabilization_days,
            self._atr_pct_threshold,
            self._range_pct_threshold,
            self._compute_atr,
        ):
            signals[SignalKey.STABILIZATION_CONFIRMED] = self._signal(SignalKey.STABILIZATION_CONFIRMED)
            primary_signals.add(SignalKey.STABILIZATION_CONFIRMED)

        if eval_entry_setup_valid(ctx, self._stabilization_days, self._entry_sma_window):
            signals[SignalKey.ENTRY_SETUP_VALID] = self._signal(SignalKey.ENTRY_SETUP_VALID)
            primary_signals.add(SignalKey.ENTRY_SETUP_VALID)

        invalidated = eval_invalidated(ctx.lows, self._invalidation_lookback)
        if invalidated:
            signals[SignalKey.INVALIDATED] = self._signal(SignalKey.INVALIDATED)
            primary_signals.add(SignalKey.INVALIDATED)
            signals.pop(SignalKey.STABILIZATION_CONFIRMED, None)
            signals.pop(SignalKey.ENTRY_SETUP_VALID, None)
            primary_signals.discard(SignalKey.STABILIZATION_CONFIRMED)
            primary_signals.discard(SignalKey.ENTRY_SETUP_VALID)

        dow_facts = compute_dow_signal_facts(
            ohlc,
            date,
            window=self._dow_window,
            use_high_low=self._dow_use_high_low,
            sensitive_down_reset=self._dow_sensitive_down_reset,
            debug=self._debug_dow_markers,
        )

        if eval_structural_downtrend_detected(ctx, dow_facts=dow_facts):
            signals[SignalKey.STRUCTURAL_DOWNTREND_DETECTED] = self._signal(
                SignalKey.STRUCTURAL_DOWNTREND_DETECTED
            )
            primary_signals.add(SignalKey.STRUCTURAL_DOWNTREND_DETECTED)

        if (
            SignalKey.DOW_TREND_CHANGE_UP_TO_NEUTRAL in dow_facts
            and SignalKey.DOW_LAST_LOW_LL in dow_facts
        ):
            signals[SignalKey.TREND_STARTED] = self._signal(SignalKey.TREND_STARTED)
            primary_signals.add(SignalKey.TREND_STARTED)
        elif trend_started_base:
            signals[SignalKey.TREND_STARTED] = self._signal(SignalKey.TREND_STARTED)
            primary_signals.add(SignalKey.TREND_STARTED)

        for key in dow_facts.keys():
            signals[key] = self._signal(key)

        if SignalKey.DOW_LAST_LOW_HL in signals:
            signals[SignalKey.HIGHER_LOW_CONFIRMED] = self._signal(SignalKey.HIGHER_LOW_CONFIRMED)
            primary_signals.add(SignalKey.HIGHER_LOW_CONFIRMED)
        if SignalKey.DOW_BOS_BREAK_UP in signals:
            signals[SignalKey.STRUCTURE_BREAKOUT_UP_CONFIRMED] = self._signal(
                SignalKey.STRUCTURE_BREAKOUT_UP_CONFIRMED
            )
            primary_signals.add(SignalKey.STRUCTURE_BREAKOUT_UP_CONFIRMED)

        if not primary_signals and SignalKey.INVALIDATED not in signals:
            signals[SignalKey.NO_SIGNAL] = Signal(
                key=SignalKey.NO_SIGNAL,
                value=True,
                confidence=None,
                source="osakedata_v3",
            )
        return SignalSet(signals=signals)

    def _compute_atr(self, ohlc: List[tuple], atr_len: int | None = None) -> float | None:
        period = atr_len if atr_len is not None else self._atr_window
        if len(ohlc) < period + 1:
            return None
        trs = []
        for i in range(len(ohlc) - 1):
            _, _o, h, l, _c, _v = ohlc[i]
            prev_close = ohlc[i + 1][4]
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
            trs.append(tr)
        if len(trs) < period:
            return None
        return sum(trs[:period]) / float(period)

    def _signal(self, key: SignalKey) -> Signal:
        return Signal(key=key, value=True, confidence=None, source="osakedata_v3")

    def _debug_insufficient(self, ticker: str, date: str, required: int, ohlc: List[tuple]) -> None:
        if not self._debug:
            return
        latest_row_date = ohlc[0][0] if ohlc else None
        print(
            "[debug][DATA_INSUFFICIENT] "
            f"ticker={ticker} date={date} required_rows={required} available_rows={len(ohlc)} "
            f"require_row_on_date={self._require_row_on_date} latest_row_date={latest_row_date} "
            f"table={self._table_name}"
        )

    def _required_rows(self) -> int:
        return max(
            self._sma_window + self._momentum_lookback,
            self._sma_window + 5,
            self._atr_window + 1,
            max(self._stabilization_days + 1, self._entry_sma_window),
            self._invalidation_lookback + 1,
            (2 * self._dow_window) + 1,
            SMA_LEN + REGIME_WINDOW - 1,
            SMA_LEN + SLOPE_LOOKBACK,
            BREAK_LOW_WINDOW + 1,
        ) + SAFETY_MARGIN_ROWS

    def _insufficient(self) -> SignalSet:
        return SignalSet(
            signals={
                SignalKey.DATA_INSUFFICIENT: Signal(
                    key=SignalKey.DATA_INSUFFICIENT,
                    value=True,
                    confidence=None,
                    source="osakedata_v3",
                )
            }
        )
