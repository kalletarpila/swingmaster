from __future__ import annotations

from swingmaster.app_api.providers.signals_v2.dow_structure import (
    build_dow_series_from_ohlc,
    compute_dow_markers,
    compute_dow_signal_facts,
)
from swingmaster.core.signals.enums import SignalKey


def _build_ohlc_desc(highs: list[float], lows: list[float], closes: list[float]) -> list[tuple]:
    asc_rows: list[tuple] = []
    for idx, (h, l, c) in enumerate(zip(highs, lows, closes)):
        date = f"2020-01-{idx + 1:02d}"
        asc_rows.append((date, c, h, l, c, 1000.0))
    return list(reversed(asc_rows))


def test_pivot_marker_is_dated_on_confirmation_day() -> None:
    highs = [10.0, 11.0, 12.0, 20.0, 13.0, 12.0, 11.0, 10.0, 9.0, 8.0]
    lows = [5.0] * len(highs)
    closes = [9.0, 10.0, 11.0, 15.0, 12.0, 11.0, 10.0, 9.0, 8.0, 7.0]
    window = 3
    ohlc_desc = _build_ohlc_desc(highs, lows, closes)
    series = build_dow_series_from_ohlc(ohlc_desc)
    dates_asc = [row[0] for row in reversed(ohlc_desc)]

    markers, _ = compute_dow_markers(series, window=window, use_high_low=True)
    high_marker = next(
        m
        for m in markers
        if m.get("label") in {"H", "HH", "LH"} and float(m.get("pivot")) == 20.0
    )
    assert high_marker["date"] == dates_asc[6]
    assert high_marker["date"] != dates_asc[3]

    facts_before_confirmation = compute_dow_signal_facts(
        ohlc_desc,
        as_of_date=dates_asc[3],
        window=window,
        use_high_low=True,
    )
    assert SignalKey.DOW_LAST_HIGH_H not in facts_before_confirmation
    assert SignalKey.DOW_LAST_HIGH_HH not in facts_before_confirmation
    assert SignalKey.DOW_LAST_HIGH_LH not in facts_before_confirmation

    facts_on_confirmation = compute_dow_signal_facts(
        ohlc_desc,
        as_of_date=dates_asc[6],
        window=window,
        use_high_low=True,
    )
    assert SignalKey.DOW_LAST_HIGH_H in facts_on_confirmation


def test_pivot_not_emitted_when_insufficient_future() -> None:
    highs = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 20.0, 10.0]
    lows = [5.0] * len(highs)
    closes = [9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 18.0, 9.0]
    window = 3
    ohlc_desc = _build_ohlc_desc(highs, lows, closes)
    series = build_dow_series_from_ohlc(ohlc_desc)

    markers, _ = compute_dow_markers(series, window=window, use_high_low=True)
    assert not any(
        m.get("label") in {"H", "HH", "LH"} and float(m.get("pivot")) == 20.0
        for m in markers
    )

