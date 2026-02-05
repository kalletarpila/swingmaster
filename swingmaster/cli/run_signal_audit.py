from __future__ import annotations

import argparse
import sqlite3
from datetime import date

from swingmaster.app_api.dto import UniverseSpec
from swingmaster.app_api.factories import build_swingmaster_app
from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.engine.evaluator import evaluate_step
from swingmaster.core.signals.enums import SignalKey
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
    parser.add_argument("--ticker", required=True, help="Ticker symbol(s) or ALL (comma-separated)")
    parser.add_argument(
        "--focus-signal",
        choices=[key.name for key in SignalKey],
        help="Print only days where this signal is emitted (DATA_INSUFFICIENT always prints)",
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
    if focus_signal == "TREND_STARTED" or trend_started_days_total > 0:
        print(f"trend_started_days_total={trend_started_days_total}")
        print(
            "trend_started_overridden_by_invalidated_days_total="
            f"{trend_started_overridden_by_invalidated_days_total}"
        )
        print(f"trend_started_clean_days_total={trend_started_clean_days_total}")


def main() -> None:
    args = parse_args()
    if args.begin_date > args.end_date:
        raise ValueError("begin-date must be <= end-date")

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

    md_conn = get_readonly_connection(MD_DB_DEFAULT)
    rc_conn = get_readonly_connection(RC_DB_DEFAULT)
    try:
        tickers = resolve_tickers(md_conn, args.market, args.ticker, args.max_tickers)
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
        window_days = args.window_days if args.window_days and args.window_days > 0 else None
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

        debug_limit = args.debug_limit if args.debug_limit > 0 else None

        for ticker in tickers:
            printed = 0
            tickers_processed += 1
            last_after_signal_day = None
            for day in trading_days:
                dates_processed_total += 1
                signal_set = signal_provider.get_signals(ticker, day)
                signal_keys = sorted(signal_set.signals.keys(), key=lambda k: k.name)
                data_insufficient = SignalKey.DATA_INSUFFICIENT in signal_set.signals
                if data_insufficient:
                    data_insufficient_days_total += 1
                if focus_signal is not None and focus_signal in signal_set.signals:
                    focus_match_days_total += 1
                if SignalKey.TREND_STARTED in signal_set.signals:
                    trend_started_days_total += 1
                    if SignalKey.INVALIDATED in signal_set.signals:
                        trend_started_overridden_by_invalidated_days_total += 1
                    else:
                        trend_started_clean_days_total += 1
                if after_signal is not None and after_signal in signal_set.signals:
                    last_after_signal_day = day

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
                        event = focus_signal is not None and focus_signal in signal_set.signals
                    else:
                        event = False
                        if focus_signal is not None and focus_signal in signal_set.signals:
                            event = True
                        if args.with_policy and policy_event:
                            event = True

                if not event:
                    continue

                if after_signal is not None:
                    if last_after_signal_day is None:
                        continue
                    if day <= last_after_signal_day:
                        continue
                    if window_days is not None:
                        delta_days = (date.fromisoformat(day) - date.fromisoformat(last_after_signal_day)).days
                        if delta_days <= 0 or delta_days > window_days:
                            continue

                if require_signals:
                    missing_required = [sig for sig in require_signals if sig not in signal_set.signals]
                    if missing_required:
                        require_signal_miss_days_total += 1
                        continue
                if exclude_signals:
                    has_excluded = [sig for sig in exclude_signals if sig in signal_set.signals]
                    if has_excluded:
                        exclude_signal_miss_days_total += 1
                        continue

                if debug_limit is not None and printed >= debug_limit:
                    continue

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
                printed += 1
                event_days_printed_total += 1
                if require_signals:
                    require_signal_days_total += 1
                if exclude_signals:
                    exclude_signal_days_total += 1
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
            )
    finally:
        md_conn.close()
        rc_conn.close()


if __name__ == "__main__":
    main()
