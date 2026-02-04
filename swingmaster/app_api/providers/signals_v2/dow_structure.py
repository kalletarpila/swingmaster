from __future__ import annotations

from typing import Dict, List, Tuple

from swingmaster.core.signals.enums import SignalKey

IndexSeries = List[Dict[str, object]]

HIGH_LABELS = {"H", "HH", "LH"}
LOW_LABELS = {"L", "HL", "LL"}
EPS_PCT = 0.0001


def build_dow_series_from_ohlc(ohlc_desc: List[tuple]) -> IndexSeries:
    """Convert DESC-ordered OHLC tuples to ASC-ordered Dow series."""
    series: IndexSeries = []
    for row in reversed(ohlc_desc):
        date, _o, h, l, c, _v = row
        series.append(
            {
                "date": date,
                "value": c,
                "high": h,
                "low": l,
            }
        )
    return series


def compute_dow_markers(
    series: IndexSeries,
    window: int = 5,
    use_high_low: bool = False,
    sensitive_down_reset: bool = False,
    debug: bool = True,
) -> Tuple[List[Dict], str]:
    """
    Laske pivotit (HH/HL/LH/LL) ja trendiyhteenveto.

    Args:
        series: Datasarja (value tai high/low)
        window: Pivot-ikkuna (N)
        use_high_low: Jos True, käytä 'high' ja 'low' kenttiä, muuten 'value'
        sensitive_down_reset: Jos True, päivitä ASH myös LH-pivotista DOWN-tilassa
        debug: Tulosta debug-rivejä

    Returns:
        markers: lista dict-olioita (date, value, label, pivot?)
        summary: lyhyt trenditeksti
    """
    if not series:
        return [], "NEUTRAL"

    if not hasattr(compute_dow_markers, "_call_seq"):
        compute_dow_markers._call_seq = 0
    compute_dow_markers._call_seq += 1
    call_id = compute_dow_markers._call_seq

    scope_keys = [
        "scope",
        "level",
        "kind",
        "series_scope",
        "series_kind",
        "source_type",
    ]
    name_keys = [
        "name",
        "series_name",
        "symbol",
        "ticker",
        "label",
        "id",
    ]

    scope = None
    name = None
    for row in series[:20]:
        if not isinstance(row, dict):
            continue
        if scope is None:
            for key in scope_keys:
                value = row.get(key)
                if value:
                    scope = value
                    break
        if name is None:
            for key in name_keys:
                value = row.get(key)
                if value:
                    name = value
                    break
        if scope is not None and name is not None:
            break
    if scope is None:
        scope = "UNKNOWN"
    if name is None:
        name = "UNKNOWN"

    if use_high_low:
        highs = [row.get("high") for row in series]
        lows = [row.get("low") for row in series]
        values = [row.get("value") or row.get("close") for row in series]
    else:
        values = [row["value"] for row in series]
        highs = values
        lows = values

    dates = [row["date"] for row in series]
    n = len(values)
    pivots: List[Tuple[int, str, float]] = []

    for i in range(n):
        # Tarkista high-pivot: vertaa taakse ja eteen erikseen
        # high[t0] > max(high[t0-N … t0-1]) AND high[t0] > max(high[t0+1 … t0+N])
        if highs[i] is not None:
            is_high = True
            # Tarkista taakse: kaikki edeltävät arvot pitää olla pienempiä
            for j in range(max(0, i - window), i):
                if highs[j] is not None and highs[j] >= highs[i]:
                    is_high = False
                    break
            # Tarkista eteen: kaikki seuraavat arvot pitää olla pienempiä
            if is_high:
                for j in range(i + 1, min(n, i + window + 1)):
                    if highs[j] is not None and highs[j] >= highs[i]:
                        is_high = False
                        break
            if is_high:
                pivots.append((i, "H", highs[i]))

        # Tarkista low-pivot: vertaa taakse ja eteen erikseen
        # low[t0] < min(low[t0-N … t0-1]) AND low[t0] < min(low[t0+1 … t0+N])
        if lows[i] is not None:
            is_low = True
            # Tarkista taakse: kaikki edeltävät arvot pitää olla suurempia
            for j in range(max(0, i - window), i):
                if lows[j] is not None and lows[j] <= lows[i]:
                    is_low = False
                    break
            # Tarkista eteen: kaikki seuraavat arvot pitää olla suurempia
            if is_low:
                for j in range(i + 1, min(n, i + window + 1)):
                    if lows[j] is not None and lows[j] <= lows[i]:
                        is_low = False
                        break
            if is_low:
                pivots.append((i, "L", lows[i]))

    pivots_by_idx: Dict[int, List[Tuple[str, float]]] = {}
    for idx, kind, pivot_val in pivots:
        pivots_by_idx.setdefault(idx, []).append((kind, pivot_val))

    markers: List[Dict] = []
    active_structural_high = None
    active_structural_low = None
    trend = "NEUTRAL"
    last_change_date = None
    last_trend_change_date = None
    MEANINGLESS_PCT = 0.0001
    EPS_PCT_LOCAL = 0.0001
    bos_down_count = 0
    bos_up_count = 0

    def _trend_from_markers(
        markers_list: List[Dict],
    ) -> Tuple[str, str | None, str | None]:
        last_reset_idx = None
        for i, marker in enumerate(markers_list):
            if marker.get("label") == "R":
                last_reset_idx = i
        if last_reset_idx is None:
            markers_view = markers_list
        else:
            markers_view = markers_list[last_reset_idx + 1 :]
        highs_so_far = [m for m in markers_view if m.get("label") in HIGH_LABELS]
        lows_so_far = [m for m in markers_view if m.get("label") in LOW_LABELS]
        last_high_label = highs_so_far[-1]["label"] if highs_so_far else None
        last_low_label = lows_so_far[-1]["label"] if lows_so_far else None
        if last_high_label == "HH" and last_low_label == "HL":
            trend_value = "UP"
        elif last_high_label == "LH" and last_low_label == "LL":
            trend_value = "DOWN"
        else:
            trend_value = "NEUTRAL"
        return trend_value, last_high_label, last_low_label

    for i in range(n):
        val = values[i]
        date = dates[i]
        prev_trend = trend
        trend, last_high_label, last_low_label = _trend_from_markers(markers)

        if trend != prev_trend and trend in ("UP", "DOWN"):
            markers.append(
                {
                    "date": date,
                    "value": val,
                    "label": "U" if trend == "UP" else "D",
                }
            )
            if debug:
                print(
                    f"[CALL {call_id}] [MARKER] {scope} | {name} | {date} | {markers[-1]['label']} | price={val}"
                )

        if trend == "NEUTRAL":
            bos_down_count = 0
            bos_up_count = 0

        # Regime-reset logic: check for regime death on every bar BEFORE pivots
        prev_bos_down_count = bos_down_count
        if trend == "UP" and active_structural_low is not None and val is not None:
            if val < active_structural_low[1]:
                bos_down_count += 1
            else:
                bos_down_count = 0
        else:
            bos_down_count = 0
        if debug and bos_down_count != prev_bos_down_count:
            if bos_down_count:
                asl_price = active_structural_low[1] if active_structural_low else None
                print(
                    f"[CALL {call_id}] [BoS] UP-break counter incremented | date={date} | price={val} | asl={asl_price} | bos_down_count={bos_down_count}"
                )
            else:
                print(
                    f"[CALL {call_id}] [BoS] UP-break counter reset | date={date} | price={val}"
                )

        prev_bos_up_count = bos_up_count
        if trend == "DOWN" and active_structural_high is not None and val is not None:
            if val > active_structural_high[1]:
                bos_up_count += 1
            else:
                bos_up_count = 0
        else:
            bos_up_count = 0
        if debug and bos_up_count != prev_bos_up_count:
            if bos_up_count:
                ash_price = (
                    active_structural_high[1] if active_structural_high else None
                )
                print(
                    f"[CALL {call_id}] [BoS] DOWN-break counter incremented | date={date} | price={val} | ash={ash_price} | bos_up_count={bos_up_count}"
                )
            else:
                print(
                    f"[CALL {call_id}] [BoS] DOWN-break counter reset | date={date} | price={val}"
                )

        if trend == "UP" and bos_down_count >= 2:
            if debug:
                asl_price = active_structural_low[1] if active_structural_low else None
                print(
                    f"[CALL {call_id}] [RESET] {scope} | {name} | {date} | trend={trend} | break_price={val} | asl={asl_price} | bos_down_count={bos_down_count}"
                )
            markers.append({"date": date, "value": val, "label": "R"})
            if debug:
                print(
                    f"[CALL {call_id}] [MARKER] {scope} | {name} | {date} | R | price={val}"
                )
            active_structural_high = None
            active_structural_low = None
            bos_down_count = 0
            bos_up_count = 0
            updated_trend, last_high_label, last_low_label = _trend_from_markers(
                markers
            )
            if updated_trend != trend:
                last_trend_change_date = date
                if debug:
                    print(
                        f"[CALL {call_id}] [TREND] {scope} | {name} | {date} | {trend} -> {updated_trend} | last_high={last_high_label} | last_low={last_low_label}"
                    )
            trend = updated_trend
            continue
        if trend == "DOWN" and bos_up_count >= 2:
            if debug:
                ash_price = (
                    active_structural_high[1] if active_structural_high else None
                )
                print(
                    f"[CALL {call_id}] [RESET] {scope} | {name} | {date} | trend={trend} | break_price={val} | ash={ash_price} | bos_up_count={bos_up_count}"
                )
            markers.append({"date": date, "value": val, "label": "R"})
            if debug:
                print(
                    f"[CALL {call_id}] [MARKER] {scope} | {name} | {date} | R | price={val}"
                )
            active_structural_low = None
            active_structural_high = None
            bos_down_count = 0
            bos_up_count = 0
            updated_trend, last_high_label, last_low_label = _trend_from_markers(
                markers
            )
            if updated_trend != trend:
                last_trend_change_date = date
                if debug:
                    print(
                        f"[CALL {call_id}] [TREND] {scope} | {name} | {date} | {trend} -> {updated_trend} | last_high={last_high_label} | last_low={last_low_label}"
                    )
            trend = updated_trend
            continue

        pivots_here = pivots_by_idx.get(i)
        if not pivots_here:
            continue

        pivots_here_sorted = sorted(
            pivots_here, key=lambda item: 0 if item[0] == "H" else 1
        )
        for kind, pivot_val in pivots_here_sorted:
            if kind == "H" and active_structural_high is not None:
                ref_price = active_structural_high[1]
                if ref_price:
                    rel_diff = abs(pivot_val - ref_price) / ref_price
                    if rel_diff < MEANINGLESS_PCT:
                        continue
            if kind == "L" and active_structural_low is not None:
                ref_price = active_structural_low[1]
                if ref_price:
                    rel_diff = abs(pivot_val - ref_price) / ref_price
                    if rel_diff < MEANINGLESS_PCT:
                        continue

            effective_kind = kind
            if kind == "L" and active_structural_high is not None:
                if pivot_val >= active_structural_high[1] * (1 - EPS_PCT_LOCAL):
                    effective_kind = "H"
            elif kind == "H" and active_structural_low is not None:
                if pivot_val <= active_structural_low[1] * (1 + EPS_PCT_LOCAL):
                    effective_kind = "L"

            if debug:
                ash_price = (
                    active_structural_high[1] if active_structural_high else None
                )
                asl_price = active_structural_low[1] if active_structural_low else None
                print(
                    f"[CALL {call_id}] [PIVOT_CTX] {scope} | {name} | {date} | kind={kind} effective={effective_kind} pivot_val={pivot_val} val={val} trend={trend} | ash={ash_price} | asl={asl_price}"
                )

            if effective_kind == "H":
                if active_structural_high is not None:
                    if pivot_val > active_structural_high[1]:
                        label = "HH"
                        active_structural_high = (date, pivot_val)  # HH paivittaa
                    else:
                        label = "LH"
                        # LH EI paivita active_structural_high
                else:
                    label = "H"
                    active_structural_high = (date, pivot_val)
            else:  # effective_kind == "L"
                if active_structural_low is not None:
                    if pivot_val > active_structural_low[1]:
                        label = "HL"
                    else:
                        label = "LL"
                    # Molemmat HL ja LL paivittavat active_structural_low
                    active_structural_low = (date, pivot_val)
                else:
                    label = "L"
                    active_structural_low = (date, pivot_val)

            if (
                label == "LH"
                and sensitive_down_reset
                and trend == "DOWN"
                and active_structural_high is not None
            ):
                old_ash = active_structural_high[1]
                active_structural_high = (date, pivot_val)
                if debug:
                    print(
                        f"[CALL {call_id}] [SENSITIVE_DOWN] ASH updated on LH | date={date} | old_ash={old_ash} | new_ash={pivot_val}"
                    )

            markers.append(
                {
                    "date": date,
                    "value": val,
                    "label": label,
                    "pivot": pivot_val,
                }
            )
            if debug:
                print(
                    f"[CALL {call_id}] [MARKER] {scope} | {name} | {date} | {label} | price={val}"
                )
            last_change_date = date
            updated_trend, last_high_label, last_low_label = _trend_from_markers(
                markers
            )
            if updated_trend != trend:
                last_trend_change_date = date
                if debug:
                    print(
                        f"[CALL {call_id}] [TREND] {scope} | {name} | {date} | {trend} -> {updated_trend} | last_high={last_high_label} | last_low={last_low_label}"
                    )
            trend = updated_trend

    if markers:
        trend, last_high_label, last_low_label = _trend_from_markers(markers)
    summary = f"{trend} (pivot {last_change_date})" if last_change_date else trend
    return markers, summary


def _trend_from_markers(markers: List[Dict]) -> Tuple[str, str | None, str | None]:
    last_reset_idx = None
    for i, marker in enumerate(markers):
        if marker.get("label") == "R":
            last_reset_idx = i
    if last_reset_idx is None:
        markers_view = markers
    else:
        markers_view = markers[last_reset_idx + 1 :]
    highs_so_far = [m for m in markers_view if m.get("label") in HIGH_LABELS]
    lows_so_far = [m for m in markers_view if m.get("label") in LOW_LABELS]
    last_high_label = highs_so_far[-1]["label"] if highs_so_far else None
    last_low_label = lows_so_far[-1]["label"] if lows_so_far else None
    if last_high_label == "HH" and last_low_label == "HL":
        trend_value = "UP"
    elif last_high_label == "LH" and last_low_label == "LL":
        trend_value = "DOWN"
    else:
        trend_value = "NEUTRAL"
    return trend_value, last_high_label, last_low_label


def _trend_changes(markers: List[Dict]) -> List[Tuple[str, str, str]]:
    changes: List[Tuple[str, str, str]] = []
    prev_trend = None
    for i in range(len(markers)):
        trend, _last_high, _last_low = _trend_from_markers(markers[: i + 1])
        if prev_trend is None:
            prev_trend = trend
            continue
        if trend != prev_trend:
            changes.append((markers[i]["date"], prev_trend, trend))
            prev_trend = trend
    return changes


def compute_dow_signal_facts(
    ohlc_desc: List[tuple],
    as_of_date: str,
    window: int = 3,
    use_high_low: bool = True,
    sensitive_down_reset: bool = False,
) -> Dict[SignalKey, bool]:
    series = build_dow_series_from_ohlc(ohlc_desc)
    markers, _summary = compute_dow_markers(
        series,
        window=window,
        use_high_low=use_high_low,
        sensitive_down_reset=sensitive_down_reset,
    )
    markers = [m for m in markers if m.get("date") and m["date"] <= as_of_date]
    if not markers:
        return {}

    facts: Dict[SignalKey, bool] = {}

    trend, last_high_label, last_low_label = _trend_from_markers(markers)
    if trend == "UP":
        facts[SignalKey.DOW_TREND_UP] = True
    elif trend == "DOWN":
        facts[SignalKey.DOW_TREND_DOWN] = True
    else:
        facts[SignalKey.DOW_TREND_NEUTRAL] = True

    if last_low_label == "LL":
        facts[SignalKey.DOW_LAST_LOW_LL] = True
    elif last_low_label == "HL":
        facts[SignalKey.DOW_LAST_LOW_HL] = True
    elif last_low_label == "L":
        facts[SignalKey.DOW_LAST_LOW_L] = True

    if last_high_label == "HH":
        facts[SignalKey.DOW_LAST_HIGH_HH] = True
    elif last_high_label == "LH":
        facts[SignalKey.DOW_LAST_HIGH_LH] = True
    elif last_high_label == "H":
        facts[SignalKey.DOW_LAST_HIGH_H] = True

    low_markers = [m for m in markers if m.get("label") in LOW_LABELS]
    if low_markers:
        last_low = low_markers[-1]
        prev_low = low_markers[-2] if len(low_markers) >= 2 else None
        last_low_price = last_low.get("pivot") or last_low.get("value")
        prev_low_price = prev_low.get("pivot") if prev_low else None
        if (
            last_low_label == "LL"
            and isinstance(last_low_price, (int, float))
            and isinstance(prev_low_price, (int, float))
            and last_low_price < prev_low_price * (1 - EPS_PCT)
        ):
            facts[SignalKey.DOW_NEW_LL] = True

    high_markers = [m for m in markers if m.get("label") in HIGH_LABELS]
    if high_markers:
        last_high = high_markers[-1]
        prev_high = high_markers[-2] if len(high_markers) >= 2 else None
        last_high_price = last_high.get("pivot") or last_high.get("value")
        prev_high_price = prev_high.get("pivot") if prev_high else None
        if (
            last_high_label == "HH"
            and isinstance(last_high_price, (int, float))
            and isinstance(prev_high_price, (int, float))
            and last_high_price > prev_high_price * (1 + EPS_PCT)
        ):
            facts[SignalKey.DOW_NEW_HH] = True

    changes = _trend_changes(markers)
    for change_date, prev_trend, new_trend in changes:
        if change_date != as_of_date:
            continue
        if prev_trend == "UP" and new_trend == "NEUTRAL":
            facts[SignalKey.DOW_TREND_CHANGE_UP_TO_NEUTRAL] = True
        elif prev_trend == "DOWN" and new_trend == "NEUTRAL":
            facts[SignalKey.DOW_TREND_CHANGE_DOWN_TO_NEUTRAL] = True
        elif prev_trend == "NEUTRAL" and new_trend == "UP":
            facts[SignalKey.DOW_TREND_CHANGE_NEUTRAL_TO_UP] = True
        elif prev_trend == "NEUTRAL" and new_trend == "DOWN":
            facts[SignalKey.DOW_TREND_CHANGE_NEUTRAL_TO_DOWN] = True

    for idx, marker in enumerate(markers):
        if marker.get("label") != "R":
            continue
        if marker.get("date") != as_of_date:
            continue
        facts[SignalKey.DOW_RESET] = True
        prev_trend, _prev_high, _prev_low = _trend_from_markers(markers[:idx])
        if prev_trend == "UP":
            facts[SignalKey.DOW_BOS_BREAK_DOWN] = True
        elif prev_trend == "DOWN":
            facts[SignalKey.DOW_BOS_BREAK_UP] = True

    return facts
