from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from typing import Dict, List

from swingmaster.app_api.dto import UniverseSpec, UniverseMode, UniverseSample
from swingmaster.app_api.facade import SwingmasterApplication
from swingmaster.app_api.providers.osakedata_signal_provider_v1 import OsakeDataSignalProviderV1
from swingmaster.app_api.providers.osakedata_signal_provider_v2 import OsakeDataSignalProviderV2
from swingmaster.app_api.providers.sqlite_prev_state_provider import SQLitePrevStateProvider
from swingmaster.core.policy.factory import default_policy_factory
from swingmaster.infra.sqlite.db import get_connection
from swingmaster.infra.sqlite.db_readonly import get_readonly_connection
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.infra.sqlite.repos.ticker_universe_reader import TickerUniverseReader

# Example:
# python3 -m swingmaster.cli.run_range_universe --date-from 2026-01-01 --date-to 2026-01-31 --signal-version v2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run swingmaster over a date range")
    parser.add_argument("--date-from", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--date-to", required=True, help="End date YYYY-MM-DD (inclusive)")
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
    parser.add_argument("--max-days", type=int, default=0, help="Max trading days to process (0 = no limit)")
    parser.add_argument("--dry-run", action="store_true", help="Resolve and show counts only")
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


def print_report(rc_conn, as_of_date: str, run_id: str) -> None:
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


def print_missing_asof_summary(
    rc_conn,
    date_from: str,
    date_to: str,
    tickers: List[str],
    final_day: str,
    final_run_id: str,
    top_n: int = 20,
    list_limit: int = 50,
) -> None:
    final_rows = rc_conn.execute(
        """
        SELECT ticker, reasons_json
        FROM rc_state_daily
        WHERE date=? AND run_id=?
        """,
        (final_day, final_run_id),
    ).fetchall()
    final_missing = []
    for row in final_rows:
        reasons = parse_reasons(row["reasons_json"])
        if "DATA_INSUFFICIENT" in reasons:
            final_missing.append(row["ticker"])

    print(f"MISSING_ASOF_FINAL_DAY: {len(final_missing)}")
    if final_missing:
        print(f"MISSING_ASOF_FINAL_DAY_TICKERS: {','.join(sorted(final_missing)[:list_limit])}")

    if not tickers:
        return

    per_ticker = Counter()
    chunk_size = 500
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        query = (
            f"SELECT ticker, reasons_json FROM rc_state_daily "
            f"WHERE date>=? AND date<=? AND ticker IN ({placeholders})"
        )
        params = [date_from, date_to, *chunk]
        rows = rc_conn.execute(query, params).fetchall()
        for row in rows:
            reasons = parse_reasons(row["reasons_json"])
            if "DATA_INSUFFICIENT" in reasons:
                per_ticker[row["ticker"]] += 1

    total_missing_days = sum(per_ticker.values())
    print(f"MISSING_ASOF_RANGE_TOTAL_DAYS: {total_missing_days}")
    print("MISSING_ASOF_RANGE_TOP:")
    if per_ticker:
        for ticker, count in sorted(per_ticker.items(), key=lambda x: (-x[1], x[0]))[:top_n]:
            print(f"  {ticker} days_insufficient={count}")
    else:
        print("  (none)")


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
                as_of_date=args.date_from,
                osakedata_table="osakedata",
                min_history_rows=args.min_history_rows,
                require_row_on_date=args.require_row_on_date,
            )

        days = md_conn.execute(
            """
            SELECT DISTINCT pvm FROM osakedata
            WHERE pvm >= ? AND pvm <= ?
            ORDER BY pvm
            """,
            (args.date_from, args.date_to),
        ).fetchall()
        trading_days = [row[0] for row in days]
        if args.max_days > 0:
            trading_days = trading_days[: args.max_days]
        if not trading_days:
            print("RANGE: no trading days found in osakedata for given dates")
            return
        if args.max_days == 0 and len(trading_days) > 300:
            print(
                f"SAFETY_STOP: trading_days={len(trading_days)} exceeds 300. "
                "Re-run with --max-days or narrower date range."
            )
            return

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
            if args.require_row_on_date:
                print(
                    "NOTE: Universe filtering applies --require-row-on-date only against date_from. "
                    "With signal_version=v2, each day also requires an as-of row; missing days emit DATA_INSUFFICIENT."
                )
        print(
            f"RANGE date_from={args.date_from} date_to={args.date_to} "
            f"trading_days={len(trading_days)} tickers={len(tickers)}"
        )

        if args.dry_run:
            if len(trading_days) <= 10:
                print("DAYS:", ",".join(trading_days))
            else:
                head = trading_days[:5]
                tail = trading_days[-5:]
                print("DAYS head:", ",".join(head))
                print("...")
                print("DAYS tail:", ",".join(tail))
            return

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

        last_run_id = None
        for idx, day in enumerate(trading_days, start=1):
            start = time.perf_counter()
            run_id = app.run_daily(as_of_date=day, tickers=tickers)
            elapsed_ms = (time.perf_counter() - start) * 1000
            last_run_id = run_id
            if idx == 1 or idx == len(trading_days) or idx % 5 == 0:
                print(f"DAY {idx}/{len(trading_days)} {day} run_id={run_id} ms={elapsed_ms:.1f}")

        if last_run_id and trading_days:
            last_day = trading_days[-1]
            print(f"FINAL_DAY {last_day} run_id={last_run_id}")
            print_report(rc_conn, last_day, last_run_id)
            if args.signal_version == "v2" and args.require_row_on_date:
                print_missing_asof_summary(
                    rc_conn,
                    args.date_from,
                    args.date_to,
                    tickers,
                    last_day,
                    last_run_id,
                )
    finally:
        md_conn.close()
        rc_conn.close()


if __name__ == "__main__":
    main()
