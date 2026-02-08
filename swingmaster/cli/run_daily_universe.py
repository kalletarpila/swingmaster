"""Run the daily universe pipeline for a single date or small window.

Purpose:
  - Compute signals and apply policy per ticker for a given day, writing results.
Inputs:
  - CLI args for date, market/universe selection, DB paths, policy/signal versions.
Outputs:
  - Writes rc_state_daily / rc_transition records and prints progress to stdout.
Example:
  - PYTHONPATH=. python3 swingmaster/cli/run_daily_universe.py --market OMXH --date 2025-01-10
Debug:
  - --debug / --debug-limit control diagnostic output volume.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Dict, List

from swingmaster.app_api.dto import UniverseSpec, UniverseMode, UniverseSample
from swingmaster.app_api.factories import build_swingmaster_app
from swingmaster.cli._debug_utils import (
    _dbg,
    _debug_enabled,
    _debug_limit,
    _effective_limit,
    _take_head_tail,
    infer_entry_blocker,
)
from swingmaster.core.signals.enums import SignalKey
from swingmaster.infra.sqlite.db import get_connection
from swingmaster.infra.sqlite.db_readonly import get_readonly_connection
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.infra.sqlite.repos.ticker_universe_reader import TickerUniverseReader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run swingmaster over a universe")
    parser.add_argument("--date", required=True, help="As-of date YYYY-MM-DD")
    parser.add_argument("--md-db", default="/home/kalle/projects/rawcandle/data/osakedata.db", help="Market data SQLite path")
    parser.add_argument("--rc-db", default="swingmaster_rc.db", help="RC database path")
    parser.add_argument("--mode", required=True, choices=[
        "tickers",
        "market",
        "market_sector",
        "market_sector_industry",
    ])
    parser.add_argument("--tickers", help="Comma-separated tickers for mode=tickers")
    parser.add_argument("--market", help="Market code")
    parser.add_argument("--sector", help="Sector name")
    parser.add_argument("--industry", help="Industry name")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--sample", choices=["first_n", "random"], default="first_n")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--min-history-rows", type=int, default=0, help="Min rows in osakedata (0 to disable)")
    parser.add_argument("--require-row-on-date", action="store_true", help="Require row on the as-of date")
    parser.add_argument("--policy-id", default="rule_v1", help="Policy id")
    parser.add_argument("--policy-version", default="dev", help="Policy version")
    parser.add_argument("--signal-version", choices=["v1", "v2"], default="v1", help="Signal provider version")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--debug-limit", type=int, default=25, help="Max items to show in debug lists (0 = no limit)")
    parser.add_argument("--debug-show-tickers", action="store_true", help="Show per-ticker debug lines")
    parser.add_argument("--debug-show-mismatches", action="store_true", help="Show entry-like vs RC mismatches")
    return parser.parse_args()


def build_spec(args: argparse.Namespace) -> UniverseSpec:
    tickers_list = None
    if args.tickers:
        tickers_list = [t.strip() for t in args.tickers.split(",") if t.strip()]

    mode: UniverseMode = args.mode
    sample: UniverseSample = args.sample

    spec = UniverseSpec(
        mode=mode,
        tickers=tickers_list,
        market=args.market,
        sector=args.sector,
        industry=args.industry,
        limit=args.limit,
        sample=sample,
        seed=args.seed,
    )
    spec.validate()
    return spec


def parse_reasons(reasons_raw: str | None) -> List[str]:
    if reasons_raw is None:
        return []
    try:
        parsed = json.loads(reasons_raw)
        if isinstance(parsed, list):
            return [r for r in parsed if isinstance(r, str)]
    except Exception:
        return []
    return []


def print_report(rc_conn, as_of_date: str, run_id: str) -> int:
    dist_rows = rc_conn.execute(
        """
        SELECT state, COUNT(*) as c
        FROM rc_state_daily
        WHERE date=? AND run_id=?
        GROUP BY state
        ORDER BY c DESC
        """,
        (as_of_date, run_id),
    ).fetchall()

    rows = rc_conn.execute(
        """
        SELECT ticker, state, reasons_json
        FROM rc_state_daily
        WHERE date=? AND run_id=?
        """,
        (as_of_date, run_id),
    ).fetchall()

    overall = Counter()
    per_state: Dict[str, Counter] = {}
    data_insufficient_tickers: List[str] = []

    for row in rows:
        reasons = parse_reasons(row["reasons_json"])

        overall.update(reasons)
        st = row["state"]
        if st not in per_state:
            per_state[st] = Counter()
        per_state[st].update(reasons)
        if "DATA_INSUFFICIENT" in reasons:
            data_insufficient_tickers.append(row["ticker"])

    data_insufficient = len(data_insufficient_tickers)

    entry_rows = rc_conn.execute(
        """
        SELECT ticker FROM rc_state_daily
        WHERE date=? AND run_id=? AND state='ENTRY_WINDOW'
        ORDER BY ticker
        LIMIT 200
        """,
        (as_of_date, run_id),
    ).fetchall()

    print(f"RUN {run_id}")
    for row in dist_rows:
        print(f"STATE {row['state']}: {row['c']}")
    print(f"DATA_INSUFFICIENT: {data_insufficient}")
    print("REASONS_TOP_OVERALL:")
    if overall:
        for reason, count in sorted(overall.items(), key=lambda x: (-x[1], x[0]))[:10]:
            print(f"  {reason}: {count}")
    else:
        print("  (none)")
    print("REASONS_TOP_BY_STATE:")
    for row in dist_rows:
        state = row["state"]
        print(f"  STATE {state}:")
        c = per_state.get(state, Counter())
        if c:
            for reason, count in sorted(c.items(), key=lambda x: (-x[1], x[0]))[:5]:
                print(f"    {reason}: {count}")
        else:
            print("    (none)")
    if data_insufficient_tickers:
        limited = sorted(data_insufficient_tickers)[:50]
        print(f"DATA_INSUFFICIENT_TICKERS: {','.join(limited)}")
    if entry_rows:
        tickers = ",".join([r[0] for r in entry_rows])
        print(f"ENTRY_WINDOW: {tickers}")
    else:
        print("ENTRY_WINDOW: none")
    return data_insufficient


def collect_signal_stats(signal_provider, tickers: List[str], day: str) -> tuple[Counter, dict[str, int], Dict[str, object]]:
    signals_counter: Counter[SignalKey] = Counter()
    entry = stab = both = invalidated = data_insufficient = 0
    signals_by_ticker: Dict[str, object] = {}
    for ticker in tickers:
        signal_set = signal_provider.get_signals(ticker, day)
        signals_by_ticker[ticker] = signal_set
        keys = set(signal_set.signals.keys())
        signals_counter.update(keys)
        has_entry = SignalKey.ENTRY_SETUP_VALID in keys
        has_stab = SignalKey.STABILIZATION_CONFIRMED in keys
        if has_entry:
            entry += 1
        if has_stab:
            stab += 1
        if has_entry and has_stab:
            both += 1
        if SignalKey.INVALIDATED in keys:
            invalidated += 1
        if SignalKey.DATA_INSUFFICIENT in keys:
            data_insufficient += 1
    focused = {
        "entry": entry,
        "stab": stab,
        "both": both,
        "invalidated": invalidated,
        "data_insufficient": data_insufficient,
    }
    return signals_counter, focused, signals_by_ticker


def main() -> None:
    args = parse_args()

    md_conn = get_readonly_connection(args.md_db)
    rc_conn = get_connection(args.rc_db)
    try:
        apply_migrations(rc_conn)
        rc_conn.commit()

        universe_reader = TickerUniverseReader(md_conn)
        spec = build_spec(args)
        tickers = universe_reader.resolve_tickers(spec)
        if args.min_history_rows > 0:
            tickers_before = list(tickers)
            before_filter = len(tickers_before)
            tickers = universe_reader.filter_by_osakedata(
                tickers=tickers,
                as_of_date=args.date,
                osakedata_table="osakedata",
                min_history_rows=args.min_history_rows,
                require_row_on_date=args.require_row_on_date,
            )
            removed_list = sorted(set(tickers_before) - set(tickers))
            _dbg(
                args,
                f"FILTER min_history_rows before={before_filter} after={len(tickers)} removed={len(removed_list)}",
            )
            limit_val = _effective_limit(args, removed_list)
            if removed_list and (limit_val == len(removed_list)):
                _dbg(args, f"FILTER removed_tickers={removed_list}")
        orig_tickers = list(tickers)
        resolved_count = len(orig_tickers)
        seen = set()
        tickers_unique = []
        for t in orig_tickers:
            if t not in seen:
                seen.add(t)
                tickers_unique.append(t)
        tickers = tickers_unique
        print(f"TICKERS_RESOLVED: {resolved_count} UNIQUE: {len(tickers)}")
        if resolved_count != len(tickers):
            dup_counter = Counter(orig_tickers)
            dupes = [(t, c) for t, c in dup_counter.items() if c > 1]
            if dupes:
                _dbg(args, "TICKER_DUPLICATES_TOP:")
                for t, c in sorted(dupes, key=lambda x: (-x[1], x[0]))[:10]:
                    _dbg(args, f"  {t}: {c}")

        if _debug_enabled(args):
            _dbg(
                args,
                (
                    f"ARGS date={args.date} mode={args.mode} market={args.market} "
                    f"sector={args.sector} industry={args.industry} limit={args.limit} sample={args.sample} seed={args.seed} "
                    f"signal_version={args.signal_version} require_row_on_date={args.require_row_on_date} "
                    f"min_history_rows={args.min_history_rows}"
                ),
            )
            if resolved_count:
                head, tail = _take_head_tail(tickers, _effective_limit(args, tickers))
                _dbg(args, f"TICKERS_SAMPLE_HEAD={head}")
                if tail:
                    _dbg(args, f"TICKERS_SAMPLE_TAIL={tail}")

        policy_version = args.policy_version
        if policy_version == "dev":
            policy_version = "v1"
        provider_name = "osakedata_v2" if args.signal_version == "v2" else "osakedata_v1"
        app = build_swingmaster_app(
            rc_conn,
            policy_version=policy_version,
            enable_history=False,
            provider=provider_name,
            md_conn=md_conn,
            require_row_on_date=args.require_row_on_date,
            policy_id=args.policy_id,
            engine_version="dev",
            debug=args.debug,
        )
        signal_provider = app._signal_provider

        print(
            f"UNIVERSE mode={args.mode} market={args.market} sector={args.sector} "
            f"industry={args.industry} sample={args.sample} seed={args.seed} limit={args.limit} "
            f"signal_version={args.signal_version}"
        )
        if args.min_history_rows > 0:
            print(
                f"FILTER min_history_rows={args.min_history_rows} "
                f"require_row_on_date={args.require_row_on_date} -> TICKERS_AFTER_FILTER={len(tickers)}"
            )
        if args.require_row_on_date and args.signal_version == "v2":
            print(
                "NOTE: With signal_version=v2 and --require-row-on-date, signals require an osakedata row "
                "exactly on the as-of date; missing row emits DATA_INSUFFICIENT."
            )
        run_id = app.run_daily(as_of_date=args.date, tickers=tickers)
        print(f"TICKERS={len(tickers)}")
        missing_count = print_report(rc_conn, args.date, run_id)
        if _debug_enabled(args):
            rc_rows = rc_conn.execute(
                "SELECT COUNT(*) FROM rc_state_daily WHERE date=? AND run_id=?",
                (args.date, run_id),
            ).fetchone()[0]
            distinct_rc = rc_conn.execute(
                "SELECT COUNT(DISTINCT ticker) FROM rc_state_daily WHERE date=? AND run_id=?",
                (args.date, run_id),
            ).fetchone()[0]
            _dbg(args, f"RC_ROWS={rc_rows} RC_DISTINCT_TICKERS={distinct_rc}")
        rc_rows_full = rc_conn.execute(
            "SELECT ticker, state, reasons_json FROM rc_state_daily WHERE date=? AND run_id=?",
            (args.date, run_id),
        ).fetchall()
        rc_by_ticker = {}
        for row in rc_rows_full:
            rc_by_ticker[row["ticker"]] = {
                "state": row["state"],
                "reasons": parse_reasons(row["reasons_json"]),
            }
        if _debug_enabled(args):
            if rc_rows_full:
                state_counter = Counter(r["state"] for r in rc_rows_full)
                _dbg(args, f"RC_STATE_DIST={dict(state_counter)}")
            expected = len(tickers)
            ok_rows = rc_rows == expected
            ok_distinct = distinct_rc == expected
            _dbg(
                args,
                f"INVARIANTS expected_tickers={expected} rc_rows={rc_rows} rc_distinct={distinct_rc} "
                f"ok_rows={ok_rows} ok_distinct={ok_distinct}",
            )
            if not (ok_rows and ok_distinct):
                rc_tickers_set = set(rc_by_ticker.keys())
                ticker_set = set(tickers)
                missing = [t for t in tickers if t not in rc_tickers_set]
                extra = [t for t in rc_tickers_set if t not in ticker_set]
                limit = _effective_limit(args, missing)
                extra_limit = _effective_limit(args, extra)
                _dbg(
                    args,
                    f"INVARIANT_FAIL missing_in_rc_count={len(missing)} extra_in_rc_count={len(extra)}",
                )
                if missing:
                    sample_miss = missing if limit == len(missing) else missing[:limit]
                    _dbg(args, f"INVARIANT_FAIL missing_in_rc_sample={sample_miss}")
                if extra:
                    sample_extra = extra if extra_limit == len(extra) else extra[:extra_limit]
                    _dbg(args, f"INVARIANT_FAIL extra_in_rc_sample={sample_extra}")
        signals_counter, focused, signals_by_ticker = collect_signal_stats(signal_provider, tickers, args.date)
        if _debug_enabled(args):
            entry_candidates = [t for t, s in signals_by_ticker.items() if SignalKey.ENTRY_SETUP_VALID in s.signals]
            stab_candidates = [t for t, s in signals_by_ticker.items() if SignalKey.STABILIZATION_CONFIRMED in s.signals]
            both_candidates = [t for t in entry_candidates if t in stab_candidates]

            def _show_list(label, items):
                limit = _effective_limit(args, items)
                sample = items if limit == len(items) else items[:limit]
                _dbg(args, f"{label} count={len(items)} sample={sample}")

            _show_list("ENTRY_CANDIDATES", entry_candidates)
            _show_list("STAB_CANDIDATES", stab_candidates)
            _show_list("BOTH_STAB_AND_ENTRY", both_candidates)
            entry_window_rc = [t for t, v in rc_by_ticker.items() if v["state"] == "ENTRY_WINDOW"]
            pass_rc = [t for t, v in rc_by_ticker.items() if v["state"] == "PASS"]
            _show_list("ENTRY_WINDOW_TICKERS", entry_window_rc)
            _show_list("PASS_TICKERS", pass_rc)
            if args.debug_show_mismatches:
                mismatches = []
                for t in both_candidates:
                    rc = rc_by_ticker.get(t)
                    if not rc or rc["state"] != "ENTRY_WINDOW":
                        reasons_json = json.dumps(rc["reasons"]) if rc else "[]"
                        state = rc["state"] if rc else "MISSING"
                        mismatches.append((t, state, reasons_json))
                _dbg(args, f"MISMATCH entry_like_not_entry_window total={len(mismatches)}")
                limit_val = _effective_limit(args, mismatches)
                subset = mismatches if limit_val == len(mismatches) else mismatches[:limit_val]
                for t, state, reasons_json in subset:
                    reasons_list = json.loads(reasons_json)
                    blocker = infer_entry_blocker(state, reasons_list)
                    sigset = signals_by_ticker.get(t)
                    signals_list = sorted(k.name for k in sigset.signals.keys()) if sigset else []
                    _dbg(
                        args,
                        f"MISMATCH_FULL_CONTEXT ticker={t} blocker={blocker} rc_state={state} "
                        f"rc_reasons={json.dumps(reasons_list)} signal_keys={json.dumps(signals_list)}",
                    )
            if args.debug_show_tickers:
                limit_val = _effective_limit(args, sorted(tickers))
                limited = sorted(tickers) if limit_val == len(tickers) else sorted(tickers)[:limit_val]
                for t in limited:
                    rc = rc_by_ticker.get(t)
                    state = rc["state"] if rc else "MISSING"
                    reasons_json = json.dumps(rc["reasons"]) if rc else "[]"
                    signal_keys = signals_by_ticker.get(t)
                    signal_names = ",".join(sorted(k.name for k in signal_keys.signals.keys())) if signal_keys else ""
                    _dbg(args, f"TICKER {t} rc_state={state} reasons={reasons_json} signals={signal_names}")
                missing_rc = [t for t in tickers if t not in rc_by_ticker]
                if missing_rc:
                    miss_limit = _effective_limit(args, missing_rc)
                    sample = missing_rc if miss_limit == len(missing_rc) else missing_rc[:miss_limit]
                    _dbg(args, f"TICKERS_NOT_IN_RC count={len(missing_rc)} sample={sample}")
        if args.signal_version == "v2" and args.require_row_on_date:
            print(f"MISSING_ASOF_DAILY: {missing_count}")
        print("NOTE: reasons_* are policy reasons stored in rc_state_daily; SIGNALS_* are signal provider keys computed from market data.")
        print("SIGNALS_TOP (provider_keys):")
        for key, count in sorted(signals_counter.items(), key=lambda x: (-x[1], x[0].name))[:10]:
            print(f"  {key.name}: {count}")
        print(f"SIGNALS_ENTRY_SETUP_VALID: {focused['entry']}")
        print(f"SIGNALS_STABILIZATION_CONFIRMED: {focused['stab']}")
        print(f"SIGNALS_BOTH_STAB_AND_ENTRY: {focused['both']}")
        print(f"SIGNALS_INVALIDATED: {focused['invalidated']}")
        print(f"SIGNALS_DATA_INSUFFICIENT: {focused['data_insufficient']}")
    finally:
        md_conn.close()
        rc_conn.close()


if __name__ == "__main__":
    main()
