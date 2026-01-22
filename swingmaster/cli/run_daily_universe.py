from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Dict, List

from swingmaster.app_api.dto import UniverseSpec, UniverseMode, UniverseSample
from swingmaster.app_api.facade import SwingmasterApplication
from swingmaster.app_api.providers.osakedata_signal_provider_v1 import OsakeDataSignalProviderV1
from swingmaster.app_api.providers.osakedata_signal_provider_v2 import OsakeDataSignalProviderV2
from swingmaster.app_api.providers.sqlite_prev_state_provider import SQLitePrevStateProvider
from swingmaster.core.policy.factory import default_policy_factory
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


def collect_signal_stats(signal_provider, tickers: List[str], day: str) -> tuple[Counter, dict[str, int]]:
    signals_counter: Counter[SignalKey] = Counter()
    entry = stab = both = invalidated = data_insufficient = 0
    for ticker in tickers:
        signal_set = signal_provider.get_signals(ticker, day)
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
    return signals_counter, focused


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
            tickers = universe_reader.filter_by_osakedata(
                tickers=tickers,
                as_of_date=args.date,
                osakedata_table="osakedata",
                min_history_rows=args.min_history_rows,
                require_row_on_date=args.require_row_on_date,
            )
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
                print("TICKER_DUPLICATES_TOP:")
                for t, c in sorted(dupes, key=lambda x: (-x[1], x[0]))[:10]:
                    print(f"  {t}: {c}")

        if args.signal_version == "v2":
            signal_provider = OsakeDataSignalProviderV2(
                md_conn, table_name="osakedata", require_row_on_date=args.require_row_on_date
            )
        else:
            signal_provider = OsakeDataSignalProviderV1(md_conn, table_name="osakedata")
        prev_state_provider = SQLitePrevStateProvider(rc_conn)
        policy = default_policy_factory.create(args.policy_id, args.policy_version)

        app = SwingmasterApplication(
            conn=rc_conn,
            policy=policy,
            signal_provider=signal_provider,
            prev_state_provider=prev_state_provider,
            engine_version="dev",
            policy_id=args.policy_id,
            policy_version=args.policy_version,
        )

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
        if args.signal_version == "v2" and args.require_row_on_date:
            print(f"MISSING_ASOF_DAILY: {missing_count}")
        signals_counter, focused = collect_signal_stats(signal_provider, tickers, args.date)
        print("SIGNALS_TOP:")
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
