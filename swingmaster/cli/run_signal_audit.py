from __future__ import annotations

import argparse
import sqlite3
from datetime import date

from swingmaster.app_api.dto import UniverseSpec
from swingmaster.app_api.factories import build_swingmaster_app
from swingmaster.app_api.providers.signals_v2.context import SignalContextV2
from swingmaster.app_api.providers.signals_v2.stabilization_confirmed import (
    eval_stabilization_confirmed_debug,
)
from swingmaster.app_api.providers.signals_v2.trend_matured import (
    eval_trend_matured_debug,
    _min_required as _trend_matured_min_required,
)
from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.engine.evaluator import evaluate_step
from swingmaster.core.signals.enums import SignalKey
from swingmaster.infra.market_data.osakedata_reader import OsakeDataReader
from swingmaster.infra.sqlite.db_readonly import get_readonly_connection
from swingmaster.infra.sqlite.repos.ticker_universe_reader import TickerUniverseReader

MD_DB_DEFAULT = "/home/kalle/projects/rawcandle/data/osakedata.db"
RC_DB_DEFAULT = "swingmaster_rc.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deterministic audit of provider signals and optional policy behavior"
    )
    parser.add_argument("--market", required=True, help="Market identifier (e.g. US, OMXH, XETRA)")
    parser.add_argument("--begin-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--ticker",
        required=True,
        nargs="+",
        help="Ticker symbol(s) or ALL (comma-separated; spaces around commas allowed)",
    )
    parser.add_argument(
        "--focus-signal",
        choices=[key.name for key in SignalKey],
        help="Print only days where this signal is emitted (DATA_INSUFFICIENT always prints)",
    )
    parser.add_argument(
        "--streaks",
        action="store_true",
        help="Compute focus-signal streak/run statistics per ticker",
    )
    parser.add_argument(
        "--first-hit-only",
        action="store_true",
        help="Treat focus-signal as edge view: only first day of each run counts/prints",
    )
    parser.add_argument("--with-policy", action="store_true", help="Include policy evaluation")
    parser.add_argument("--debug", action="store_true", help="Enable provider-level debug output")
    parser.add_argument(
        "--debug-dow-markers",
        dest="debug_dow_markers",
        action="store_true",
        help="Enable compute_dow_markers debug output",
    )
    parser.add_argument(
        "--debug-ohlcv",
        action="store_true",
        help="Print OHLCV-derived diagnostics around printed event-days",
    )
    parser.add_argument(
        "--debug-limit",
        type=int,
        default=0,
        help="Max number of printed event-days per ticker (0 = unlimited)",
    )
    parser.add_argument(
        "--print-focus-only",
        action="store_true",
        help="Print only focus-signal matches (plus DATA_INSUFFICIENT)",
    )
    parser.add_argument("--summary", action="store_true", help="Print summary counters at end")
    parser.add_argument(
        "--debug-show-mismatches",
        action="store_true",
        help="Print debug lines for require/exclude-signal miss counters",
    )
    parser.add_argument(
        "--require-signal",
        action="append",
        choices=[key.name for key in SignalKey],
        help="Require signal(s) to be present for printing (repeatable)",
    )
    parser.add_argument(
        "--exclude-signal",
        action="append",
        choices=[key.name for key in SignalKey],
        help="Exclude days where any of these signals are present (repeatable)",
    )
    parser.add_argument(
        "--after-signal",
        choices=[key.name for key in SignalKey],
        help="Only include event-days after the most recent prior occurrence of this signal",
    )
    parser.add_argument(
        "--after-signal-anchor-date",
        default=None,
        help="Override after-signal anchor date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--anchor-mode",
        choices=["last", "first"],
        default="last",
        help="Anchor selection mode for --after-signal (default: last)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=0,
        help="Max days after after-signal (ignored if --after-signal not set)",
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=0,
        help="Safety cap when ticker=ALL (0 = no cap)",
    )
    return parser.parse_args()


def resolve_tickers(
    conn,
    market: str,
    ticker_arg: str,
    max_tickers: int,
) -> list[str]:
    if ticker_arg.upper() != "ALL":
        seen = set()
        tickers = []
        for raw in ticker_arg.split(","):
            t = raw.strip()
            if not t or t in seen:
                continue
            seen.add(t)
            tickers.append(t)
        return tickers

    limit = 1_000_000
    spec = UniverseSpec(
        mode="market",
        market=market,
        limit=limit,
        sample="first_n",
        seed=1,
    )
    reader = TickerUniverseReader(conn)
    tickers = reader.resolve_tickers(spec)
    if not tickers:
        tickers = _fallback_tickers_from_osakedata(conn, market)
    tickers = sorted(tickers)
    if max_tickers > 0:
        tickers = tickers[:max_tickers]
    return tickers


def _fallback_tickers_from_osakedata(conn, market: str) -> list[str]:
    if market != "OMXH":
        return []
    try:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM osakedata WHERE ticker LIKE '%.HE' ORDER BY ticker"
        ).fetchall()
        return [row[0] for row in rows]
    except sqlite3.Error:
        rows = conn.execute(
            "SELECT DISTINCT osake FROM osakedata WHERE osake LIKE '%.HE' ORDER BY osake"
        ).fetchall()
        return [row[0] for row in rows]


def build_trading_days(md_conn, tickers: list[str], date_from: str, date_to: str) -> list[str]:
    if not tickers:
        return []
    days = set()
    chunk_size = 500
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        query = (
            f"SELECT DISTINCT pvm FROM osakedata "
            f"WHERE pvm>=? AND pvm<=? AND osake IN ({placeholders})"
        )
        params = [date_from, date_to, *chunk]
        rows = md_conn.execute(query, params).fetchall()
        for row in rows:
            days.add(row[0])
    return sorted(days)


def _is_policy_event(final_state: State, reasons: list[ReasonCode]) -> bool:
    if final_state == State.ENTRY_WINDOW:
        return True
    event_reasons = {
        ReasonCode.INVALIDATED,
        ReasonCode.EDGE_GONE,
        ReasonCode.CHURN_GUARD,
        ReasonCode.ENTRY_CONDITIONS_MET,
    }
    return any(reason in event_reasons for reason in reasons)


def _print_event_block(
    *,
    ticker: str,
    day: str,
    signal_keys: list[SignalKey],
    data_insufficient: bool,
    with_policy: bool,
    prev_state: State | None,
    next_state: State | None,
    reasons: list[ReasonCode] | None,
) -> None:
    print(f"TICKER {ticker} DATE {day}")
    signal_names = ",".join(key.name for key in signal_keys) if signal_keys else "(none)"
    print(f"SIGNALS {signal_names}")
    if data_insufficient:
        print("POLICY skipped DATA_INSUFFICIENT")
    elif with_policy and prev_state is not None and next_state is not None and reasons is not None:
        print(f"POLICY {prev_state.name}->{next_state.name}")
        reason_names = ",".join(code.value for code in reasons) if reasons else "(none)"
        print(f"REASONS {reason_names}")
    print("")


def _print_summary(
    *,
    tickers_resolved: int,
    tickers_processed: int,
    dates_processed_total: int,
    event_days_printed_total: int,
    data_insufficient_days_total: int,
    focus_signal: str | None,
    focus_match_days_total: int,
    trend_started_days_total: int,
    trend_started_overridden_by_invalidated_days_total: int,
    trend_started_clean_days_total: int,
    require_signal_days_total: int,
    require_signal_miss_days_total: int,
    exclude_signal_days_total: int,
    exclude_signal_miss_days_total: int,
    after_signal: str | None,
    after_signal_anchor_date: str | None,
    window_days: int | None,
    after_signal_anchors_found_total: int,
    after_signal_anchors_missing_total: int,
    anchor_mode: str,
    after_signal_anchor_mode_first_count: int,
    after_signal_anchor_mode_last_count: int,
    anchor_source: str,
) -> None:
    print("SUMMARY")
    print(f"tickers_resolved={tickers_resolved}")
    print(f"tickers_processed={tickers_processed}")
    print(f"dates_processed_total={dates_processed_total}")
    print(f"event_days_printed_total={event_days_printed_total}")
    print(f"data_insufficient_days_total={data_insufficient_days_total}")
    print(f"focus_signal={focus_signal}")
    print(f"focus_match_days_total={focus_match_days_total}")
    print(f"require_signal_days_total={require_signal_days_total}")
    print(f"require_signal_miss_days_total={require_signal_miss_days_total}")
    print(f"exclude_signal_days_total={exclude_signal_days_total}")
    print(f"exclude_signal_miss_days_total={exclude_signal_miss_days_total}")
    if after_signal is not None:
        print(f"after_signal={after_signal}")
        print(f"after_signal_anchor_date={after_signal_anchor_date}")
        print(f"anchor_source={anchor_source}")
        print(f"window_days={window_days}")
        print(f"anchor_mode={anchor_mode}")
        print(f"after_signal_anchors_found_total={after_signal_anchors_found_total}")
        print(f"after_signal_anchors_missing_total={after_signal_anchors_missing_total}")
        print(
            "after_signal_anchor_mode_first_count="
            f"{after_signal_anchor_mode_first_count}"
        )
        print(
            "after_signal_anchor_mode_last_count="
            f"{after_signal_anchor_mode_last_count}"
        )
    if focus_signal == "TREND_STARTED" or trend_started_days_total > 0:
        print(f"trend_started_days_total={trend_started_days_total}")
        print(
            "trend_started_overridden_by_invalidated_days_total="
            f"{trend_started_overridden_by_invalidated_days_total}"
        )
        print(f"trend_started_clean_days_total={trend_started_clean_days_total}")


def _print_streaks_summary(streaks_by_ticker: dict[str, dict[str, object]]) -> None:
    # Deterministic, compact streak summary for focus-signal presence.
    items = []
    for ticker, stats in streaks_by_ticker.items():
        runs_total = int(stats["runs_total"])
        if runs_total <= 0:
            continue
        items.append((ticker, stats))

    items.sort(
        key=lambda x: (
            -int(x[1]["max_run_len"]),
            -int(x[1]["runs_total"]),
            x[0],
        )
    )

    streaks_tickers_with_runs = len(items)
    streaks_runs_total = sum(int(stats["runs_total"]) for _t, stats in items)
    streaks_max_run_len_overall = max((int(stats["max_run_len"]) for _t, stats in items), default=0)

    print("STREAKS SUMMARY")
    for ticker, stats in items:
        print(
            f"STREAKS ticker={ticker} "
            f"runs_total={stats['runs_total']} "
            f"max_run_len={stats['max_run_len']} "
            f"avg_run_len={stats['avg_run_len']}"
        )
    print(
        "STREAKS_AGG "
        f"streaks_tickers_with_runs={streaks_tickers_with_runs} "
        f"streaks_runs_total={streaks_runs_total} "
        f"streaks_max_run_len_overall={streaks_max_run_len_overall}"
    )


def _sma_series(values: list[float], window: int) -> list[float]:
    if window <= 0 or len(values) < window:
        return []
    out: list[float] = []
    for i in range(len(values) - window + 1):
        out.append(sum(values[i : i + window]) / float(window))
    return out


def _is_new_low(values: list[float], idx: int, lookback: int) -> bool:
    if idx + lookback >= len(values):
        return False
    prior = values[idx + 1 : idx + 1 + lookback]
    if not prior:
        return False
    return values[idx] < min(prior)


def _load_debug_ohlcv_window(md_conn, ticker: str, as_of_date: str, n: int) -> list[tuple]:
    # Keep this local and simple for determinism and testability (can be monkeypatched).
    return OsakeDataReader(md_conn, "osakedata").get_last_n_ohlc(ticker, as_of_date, n)


def _print_debug_ohlcv(md_conn, ticker: str, as_of_date: str, window: int = 20) -> None:
    # Uses the same DESC-ordered close series convention as SignalContextV2.
    sma_len = 20
    new_low_lookback = 10
    required = window + sma_len - 1
    ohlc = _load_debug_ohlcv_window(md_conn, ticker, as_of_date, required)
    if not ohlc:
        print(f"DEBUG_OHLCV (window={window})")
        return

    dates = [row[0] for row in ohlc]
    closes = [row[4] for row in ohlc]
    sma20 = _sma_series(closes, sma_len)

    print(f"DEBUG_OHLCV (window={window})")
    rows = min(window, len(closes))
    for i in range(rows):
        close = closes[i]
        sma_val = sma20[i] if i < len(sma20) else None
        below_sma = "Y" if (sma_val is not None and close < sma_val) else "N"
        new_low = "Y" if _is_new_low(closes, i, new_low_lookback) else "N"
        print(
            "DEBUG_OHLCV_ROW "
            f"date={dates[i]} close={close} sma20={sma_val} below_sma={below_sma} new_low={new_low}"
        )


def _print_trend_matured_debug(md_conn, ticker: str, as_of_date: str) -> None:
    required = _trend_matured_min_required()
    ohlc = _load_debug_ohlcv_window(md_conn, ticker, as_of_date, required)
    if not ohlc:
        print("TREND_MATURED_DEBUG insufficient_data=True result=False")
        return
    closes = [row[4] for row in ohlc]
    highs = [row[2] for row in ohlc]
    lows = [row[3] for row in ohlc]
    ctx = SignalContextV2(closes=closes, highs=highs, lows=lows, ohlc=ohlc)
    _result, debug_info = eval_trend_matured_debug(ctx, sma_window=20, matured_below_sma_days=5)
    print(debug_info)


def _print_stabilization_debug(md_conn, ticker: str, as_of_date: str) -> None:
    required = 40
    ohlc = _load_debug_ohlcv_window(md_conn, ticker, as_of_date, required)
    if not ohlc:
        print("DEBUG_STABILIZATION insufficient_data=True result=False")
        return
    closes = [row[4] for row in ohlc]
    highs = [row[2] for row in ohlc]
    lows = [row[3] for row in ohlc]
    ctx = SignalContextV2(closes=closes, highs=highs, lows=lows, ohlc=ohlc)
    _result, debug_info = eval_stabilization_confirmed_debug(
        ctx,
        atr_window=14,
        stabilization_days=5,
        atr_pct_threshold=0.03,
        range_pct_threshold=0.05,
        compute_atr=lambda _o: 0.0,
    )
    print(debug_info)


def main() -> None:
    args = parse_args()
    if args.begin_date > args.end_date:
        raise ValueError("begin-date must be <= end-date")
    explicit_anchor_date = None
    if args.after_signal_anchor_date is not None:
        try:
            explicit_anchor_date = date.fromisoformat(args.after_signal_anchor_date)
        except ValueError as exc:
            raise ValueError("after-signal-anchor-date must be YYYY-MM-DD") from exc

    tickers_resolved = 0
    tickers_processed = 0
    dates_processed_total = 0
    event_days_printed_total = 0
    data_insufficient_days_total = 0
    focus_match_days_total = 0
    trend_started_days_total = 0
    trend_started_overridden_by_invalidated_days_total = 0
    trend_started_clean_days_total = 0
    require_signal_days_total = 0
    require_signal_miss_days_total = 0
    exclude_signal_days_total = 0
    exclude_signal_miss_days_total = 0
    focus_signal_value = args.focus_signal
    after_signal_value = args.after_signal
    after_signal_anchor_date = None
    window_days_value = args.window_days if args.window_days and args.window_days > 0 else None
    after_signal_anchors_found_total = 0
    after_signal_anchors_missing_total = 0
    streaks_by_ticker: dict[str, dict[str, object]] = {}

    md_conn = get_readonly_connection(MD_DB_DEFAULT)
    rc_conn = get_readonly_connection(RC_DB_DEFAULT)
    try:
        ticker_arg = " ".join(args.ticker)
        tickers = resolve_tickers(md_conn, args.market, ticker_arg, args.max_tickers)
        tickers_resolved = len(tickers)
        if not tickers:
            if args.summary:
                _print_summary(
                    tickers_resolved=tickers_resolved,
                    tickers_processed=tickers_processed,
                    dates_processed_total=dates_processed_total,
                    event_days_printed_total=event_days_printed_total,
                    data_insufficient_days_total=data_insufficient_days_total,
                    focus_signal=focus_signal_value,
                    focus_match_days_total=focus_match_days_total,
                    trend_started_days_total=trend_started_days_total,
                    trend_started_overridden_by_invalidated_days_total=trend_started_overridden_by_invalidated_days_total,
                    trend_started_clean_days_total=trend_started_clean_days_total,
                    require_signal_days_total=require_signal_days_total,
                    require_signal_miss_days_total=require_signal_miss_days_total,
                    exclude_signal_days_total=exclude_signal_days_total,
                    exclude_signal_miss_days_total=exclude_signal_miss_days_total,
                    after_signal=after_signal_value,
                    after_signal_anchor_date=after_signal_anchor_date,
                    window_days=window_days_value,
                    after_signal_anchors_found_total=after_signal_anchors_found_total,
                    after_signal_anchors_missing_total=after_signal_anchors_missing_total,
                    anchor_mode=args.anchor_mode,
                    after_signal_anchor_mode_first_count=0,
                    after_signal_anchor_mode_last_count=0,
                    anchor_source="explicit" if explicit_anchor_date else "inferred",
                )
            return

        focus_signal = SignalKey[args.focus_signal] if args.focus_signal else None
        require_signals = (
            {SignalKey[name] for name in args.require_signal} if args.require_signal else set()
        )
        exclude_signals = (
            {SignalKey[name] for name in args.exclude_signal} if args.exclude_signal else set()
        )
        after_signal = SignalKey[args.after_signal] if args.after_signal else None
        window_days = window_days_value
        trading_days = build_trading_days(md_conn, tickers, args.begin_date, args.end_date)
        if not trading_days:
            if args.summary:
                _print_summary(
                    tickers_resolved=tickers_resolved,
                    tickers_processed=tickers_processed,
                    dates_processed_total=dates_processed_total,
                    event_days_printed_total=event_days_printed_total,
                    data_insufficient_days_total=data_insufficient_days_total,
                    focus_signal=focus_signal_value,
                    focus_match_days_total=focus_match_days_total,
                    trend_started_days_total=trend_started_days_total,
                    trend_started_overridden_by_invalidated_days_total=trend_started_overridden_by_invalidated_days_total,
                    trend_started_clean_days_total=trend_started_clean_days_total,
                    require_signal_days_total=require_signal_days_total,
                    require_signal_miss_days_total=require_signal_miss_days_total,
                    exclude_signal_days_total=exclude_signal_days_total,
                    exclude_signal_miss_days_total=exclude_signal_miss_days_total,
                    after_signal=after_signal_value,
                    after_signal_anchor_date=after_signal_anchor_date,
                    window_days=window_days_value,
                    after_signal_anchors_found_total=after_signal_anchors_found_total,
                    after_signal_anchors_missing_total=after_signal_anchors_missing_total,
                    anchor_mode=args.anchor_mode,
                    after_signal_anchor_mode_first_count=0,
                    after_signal_anchor_mode_last_count=0,
                    anchor_source="explicit" if explicit_anchor_date else "inferred",
                )
            return

        app = build_swingmaster_app(
            rc_conn,
            policy_version="v1",
            enable_history=True,
            provider="osakedata_v2",
            md_conn=md_conn,
            debug=args.debug,
            debug_dow_markers=args.debug_dow_markers,
        )
        signal_provider = app._signal_provider
        policy = app._policy
        prev_state_provider = app._prev_state_provider

        allow_day_print = (not args.streaks) or args.print_focus_only
        debug_limit = args.debug_limit if args.debug_limit > 0 else None
        debug_matured = args.debug_show_mismatches or args.debug_ohlcv
        debug_stabilization = args.debug_show_mismatches or args.debug_ohlcv
        global_after_signal_anchor_date: date | None = None

        def _is_eligible_after_anchor(
            day_date: date, anchor_date: date | None, window_days: int | None
        ) -> bool:
            if anchor_date is None:
                return False
            if day_date <= anchor_date:
                return False
            if window_days is None:
                return True
            return (day_date - anchor_date).days <= window_days

        for ticker in tickers:
            printed = 0
            mismatches_printed = 0
            tickers_processed += 1
            day_signals = []
            first_after_signal_date = None
            last_after_signal_date = None
            anchor_date = None
            prev_focus_present = False
            debug_stabilization_needed = (
                focus_signal == SignalKey.STABILIZATION_CONFIRMED
                or SignalKey.STABILIZATION_CONFIRMED in require_signals
                or SignalKey.STABILIZATION_CONFIRMED in exclude_signals
            )
            if explicit_anchor_date is not None:
                anchor_date = explicit_anchor_date
            for day in trading_days:
                signal_set = signal_provider.get_signals(ticker, day)
                day_signals.append((day, signal_set))
                if explicit_anchor_date is None and after_signal is not None and after_signal in signal_set.signals:
                    if first_after_signal_date is None:
                        first_after_signal_date = date.fromisoformat(day)
                    last_after_signal_date = date.fromisoformat(day)
            if explicit_anchor_date is None and after_signal is not None:
                if args.anchor_mode == "first":
                    anchor_date = first_after_signal_date
                else:
                    anchor_date = last_after_signal_date
            if explicit_anchor_date is None and after_signal is not None:
                if anchor_date is None:
                    after_signal_anchors_missing_total += 1
                else:
                    after_signal_anchors_found_total += 1
                    if (
                        global_after_signal_anchor_date is None
                        or anchor_date > global_after_signal_anchor_date
                    ):
                        global_after_signal_anchor_date = anchor_date

            if args.streaks and focus_signal is not None:
                runs: list[int] = []
                run_len = 0
                for _day, ss in day_signals:
                    if focus_signal in ss.signals:
                        run_len += 1
                    else:
                        if run_len:
                            runs.append(run_len)
                            run_len = 0
                if run_len:
                    runs.append(run_len)
                runs_total = len(runs)
                max_run_len = max(runs) if runs else 0
                avg_run_len = (sum(runs) / float(runs_total)) if runs_total else 0.0
                streaks_by_ticker[ticker] = {
                    "runs_total": runs_total,
                    "max_run_len": max_run_len,
                    "avg_run_len": f"{avg_run_len:.2f}",
                }
            for day, signal_set in day_signals:
                dates_processed_total += 1
                day_date = date.fromisoformat(day)
                signal_keys = sorted(signal_set.signals.keys(), key=lambda k: k.name)
                data_insufficient = SignalKey.DATA_INSUFFICIENT in signal_set.signals
                if data_insufficient:
                    data_insufficient_days_total += 1
                if SignalKey.TREND_STARTED in signal_set.signals:
                    trend_started_days_total += 1
                    if SignalKey.INVALIDATED in signal_set.signals:
                        trend_started_overridden_by_invalidated_days_total += 1
                    else:
                        trend_started_clean_days_total += 1

                focus_present = focus_signal is not None and focus_signal in signal_set.signals
                focus_match = focus_present and (not args.first_hit_only or not prev_focus_present)
                prev_focus_present = focus_present

                if after_signal is not None:
                    if not _is_eligible_after_anchor(day_date, anchor_date, window_days):
                        continue

                if focus_match:
                    focus_match_days_total += 1

                prev_state = None
                next_state = None
                reasons: list[ReasonCode] | None = None
                policy_event = False

                if data_insufficient:
                    event = True
                else:
                    if args.with_policy:
                        prev_state, prev_attrs = prev_state_provider.get_prev(ticker, day)
                        evaluation = evaluate_step(
                            prev_state=prev_state,
                            prev_attrs=prev_attrs,
                            signals=signal_set,
                            policy=policy,
                        )
                        next_state = evaluation.final_state
                        reasons = evaluation.reasons
                        policy_event = _is_policy_event(next_state, reasons)
                    if args.print_focus_only:
                        event = focus_match
                    else:
                        event = False
                        if focus_match:
                            event = True
                        if args.with_policy and policy_event:
                            event = True

                if not event:
                    continue

                if require_signals:
                    missing_required = [sig for sig in require_signals if sig not in signal_set.signals]
                    if missing_required:
                        require_signal_miss_days_total += 1
                        if args.debug_show_mismatches and (
                            debug_limit is None or mismatches_printed < debug_limit
                        ):
                            anchor_str = anchor_date.isoformat() if anchor_date else None
                            eligible_str = (
                                "Y"
                                if (
                                    after_signal is None
                                    or _is_eligible_after_anchor(day_date, anchor_date, window_days)
                                )
                                else "N"
                            )
                            signals_str = ",".join(k.name for k in signal_keys) if signal_keys else "(none)"
                            print(
                                "MISMATCH REQUIRE_MISS "
                                f"ticker={ticker} date={day} signals={signals_str} "
                                f"anchor={anchor_str} window_days={window_days} eligible={eligible_str}"
                            )
                            mismatches_printed += 1
                            if debug_matured and SignalKey.TREND_MATURED in signal_set.signals:
                                _print_trend_matured_debug(md_conn, ticker, day)
                            if (
                                debug_stabilization
                                and debug_stabilization_needed
                            ):
                                _print_stabilization_debug(md_conn, ticker, day)
                        continue
                if exclude_signals:
                    has_excluded = [sig for sig in exclude_signals if sig in signal_set.signals]
                    if has_excluded:
                        exclude_signal_miss_days_total += 1
                        if args.debug_show_mismatches and (
                            debug_limit is None or mismatches_printed < debug_limit
                        ):
                            anchor_str = anchor_date.isoformat() if anchor_date else None
                            eligible_str = (
                                "Y"
                                if (
                                    after_signal is None
                                    or _is_eligible_after_anchor(day_date, anchor_date, window_days)
                                )
                                else "N"
                            )
                            signals_str = ",".join(k.name for k in signal_keys) if signal_keys else "(none)"
                            print(
                                "MISMATCH EXCLUDE_MISS "
                                f"ticker={ticker} date={day} signals={signals_str} "
                                f"anchor={anchor_str} window_days={window_days} eligible={eligible_str}"
                            )
                            mismatches_printed += 1
                            if debug_matured and SignalKey.TREND_MATURED in signal_set.signals:
                                _print_trend_matured_debug(md_conn, ticker, day)
                            if (
                                debug_stabilization
                                and debug_stabilization_needed
                            ):
                                _print_stabilization_debug(md_conn, ticker, day)
                        continue

                if debug_limit is not None and printed >= debug_limit:
                    continue

                if allow_day_print:
                    _print_event_block(
                        ticker=ticker,
                        day=day,
                        signal_keys=signal_keys,
                        data_insufficient=data_insufficient,
                        with_policy=args.with_policy,
                        prev_state=prev_state,
                        next_state=next_state,
                        reasons=reasons,
                    )
                    if debug_matured and SignalKey.TREND_MATURED in signal_set.signals:
                        _print_trend_matured_debug(md_conn, ticker, day)
                    if (
                        debug_stabilization
                        and SignalKey.STABILIZATION_CONFIRMED in signal_set.signals
                    ):
                        _print_stabilization_debug(md_conn, ticker, day)
                    if args.debug_ohlcv:
                        _print_debug_ohlcv(md_conn, ticker, day, window=20)
                    printed += 1
                    event_days_printed_total += 1
                    if require_signals:
                        require_signal_days_total += 1
                    if exclude_signals:
                        exclude_signal_days_total += 1
            if len(tickers) == 1:
                after_signal_anchor_date = anchor_date.isoformat() if anchor_date else None
        if len(tickers) != 1:
            after_signal_anchor_date = (
                global_after_signal_anchor_date.isoformat() if global_after_signal_anchor_date else None
            )
        if explicit_anchor_date is not None:
            after_signal_anchor_date = explicit_anchor_date.isoformat()
        if args.summary:
            if args.anchor_mode == "first":
                anchor_mode_first_count = after_signal_anchors_found_total
                anchor_mode_last_count = 0
            else:
                anchor_mode_first_count = 0
                anchor_mode_last_count = after_signal_anchors_found_total
            anchor_source = "explicit" if explicit_anchor_date else "inferred"
            if args.streaks:
                _print_streaks_summary(streaks_by_ticker)
            _print_summary(
                tickers_resolved=tickers_resolved,
                tickers_processed=tickers_processed,
                dates_processed_total=dates_processed_total,
                event_days_printed_total=event_days_printed_total,
                data_insufficient_days_total=data_insufficient_days_total,
                focus_signal=focus_signal_value,
                focus_match_days_total=focus_match_days_total,
                trend_started_days_total=trend_started_days_total,
                trend_started_overridden_by_invalidated_days_total=trend_started_overridden_by_invalidated_days_total,
                trend_started_clean_days_total=trend_started_clean_days_total,
                require_signal_days_total=require_signal_days_total,
                require_signal_miss_days_total=require_signal_miss_days_total,
                exclude_signal_days_total=exclude_signal_days_total,
                exclude_signal_miss_days_total=exclude_signal_miss_days_total,
                after_signal=after_signal_value,
                after_signal_anchor_date=after_signal_anchor_date,
                window_days=window_days_value,
                after_signal_anchors_found_total=after_signal_anchors_found_total,
                after_signal_anchors_missing_total=after_signal_anchors_missing_total,
                anchor_mode=args.anchor_mode,
                after_signal_anchor_mode_first_count=anchor_mode_first_count,
                after_signal_anchor_mode_last_count=anchor_mode_last_count,
                anchor_source=anchor_source,
            )
        elif args.streaks:
            _print_streaks_summary(streaks_by_ticker)
    finally:
        md_conn.close()
        rc_conn.close()


if __name__ == "__main__":
    main()
