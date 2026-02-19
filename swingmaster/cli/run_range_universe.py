"""Run the universe pipeline across a date range and persist results.

Purpose:
  - Iterate trading days, compute signals, apply policy, and write state/transition rows.
Inputs:
  - CLI args for market/universe selection, date range, DB paths, policy/signal versions.
Outputs:
  - Writes rc_state_daily / rc_transition records and prints progress to stdout.
Examples:
  - PYTHONPATH=. python3 swingmaster/cli/run_range_universe.py --market OMXH --mode market --date-from 2024-02-08 --date-to 2026-02-02 --max-days 550 --signal-version v2
Debug:
  - --debug / --debug-limit control diagnostic output volume.
  - --print-signals / --print-signals-limit emit per-day signal keys when enabled.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from datetime import datetime, timezone
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
from swingmaster.core.engine.evaluator import set_evaluator_debug
from swingmaster.core.policy.rule_v1.policy import set_churn_guard_debug
from swingmaster.core.domain.enums import reason_from_persisted, reason_to_persisted
from swingmaster.core.signals.enums import SignalKey
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
    parser.add_argument("--policy-id", default="rule_v2", help="Policy id")
    parser.add_argument("--policy-version", default="v2", help="Policy version")
    parser.add_argument("--signal-version", choices=["v2", "v3"], default="v2", help="Signal provider version")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--debug-limit", type=int, default=25, help="Max items to show in debug lists (0 = no limit)")
    parser.add_argument("--debug-show-tickers", action="store_true", help="Show per-ticker debug lines on final day")
    parser.add_argument("--debug-show-mismatches", action="store_true", help="Show entry-like vs RC mismatches on final day")
    parser.add_argument("--print-signals", action="store_true", help="Print per-day per-ticker signal keys")
    parser.add_argument("--print-signals-limit", type=int, default=20, help="Max tickers per day for --print-signals")
    parser.add_argument("--report", action="store_true", help="Print episode report after run completes")
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
            normalized: List[str] = []
            for value in parsed:
                if not isinstance(value, str):
                    continue
                code = reason_from_persisted(value)
                if code is None:
                    continue
                normalized.append(reason_to_persisted(code))
            return normalized
    except Exception:
        return []
    return []


def build_trading_days(md_conn, tickers: List[str], date_from: str, date_to: str) -> List[str]:
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
    tickers: List[str],
    run_ids_by_day: Dict[str, str],
    trading_days: List[str],
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

    if not run_ids_by_day:
        print("MISSING_ASOF_RANGE: no runs captured; skipping summary")
        return

    per_ticker = Counter()
    per_day = Counter()
    affected_tickers = set()
    affected_days = set()
    for day in sorted(run_ids_by_day.keys()):
        run_id = run_ids_by_day[day]
        rows = rc_conn.execute(
            """
            SELECT ticker, reasons_json
            FROM rc_state_daily
            WHERE date=? AND run_id=? AND reasons_json LIKE '%DATA_INSUFFICIENT%'
            """,
            (day, run_id),
        ).fetchall()
        missing_this_day = 0
        for row in rows:
            reasons = parse_reasons(row["reasons_json"])
            if "DATA_INSUFFICIENT" in reasons:
                per_ticker[row["ticker"]] += 1
                missing_this_day += 1
                affected_tickers.add(row["ticker"])
        if missing_this_day > 0:
            per_day[day] = missing_this_day
            affected_days.add(day)

    total_missing_days = sum(per_ticker.values())
    print(f"MISSING_ASOF_RANGE_TICKER_DAY_COUNT: {total_missing_days}")
    print(f"MISSING_ASOF_RANGE_AFFECTED_TICKERS: {len(affected_tickers)}/{len(tickers)}")
    print(f"MISSING_ASOF_RANGE_AFFECTED_DAYS: {len(affected_days)}/{len(trading_days)}")
    if 0 < len(affected_days) <= 5:
        print("MISSING_ASOF_RANGE_AFFECTED_DAYS_LIST: " + ",".join(sorted(affected_days)))
    print("MISSING_ASOF_RANGE_DAY_COUNTS_TOP:")
    if per_day:
        for day, count in sorted(per_day.items(), key=lambda x: (-x[1], x[0]))[:10]:
            print(f"  {day} missing_tickers={count}")
    else:
        print("  (none)")
    print("MISSING_ASOF_RANGE_TOP:")
    if per_ticker:
        for ticker, count in sorted(per_ticker.items(), key=lambda x: (-x[1], x[0]))[:top_n]:
            print(f"  {ticker} days_insufficient={count}")
    else:
        print("  (none)")


def print_episode_report(rc_conn, rc_db_path: str, md_db_path: str) -> None:
    def _fmt_report_num(v):
        if isinstance(v, float):
            s = f"{v:.4f}".rstrip("0").rstrip(".")
            return s.replace(".", ",")
        return str(v)

    def _fmt_report_cell(v):
        if v is None:
            return "NA"
        return _fmt_report_num(v)

    columns = {
        row[1] for row in rc_conn.execute("PRAGMA table_info(rc_pipeline_episode)").fetchall()
    }
    has_confirm_above_5 = "ew_confirm_above_5" in columns
    has_confirm_confirmed = "ew_confirm_confirmed" in columns
    has_peak60_days = "peak60_days_from_ew_start" in columns
    has_peak60_sma5 = "peak60_sma5" in columns
    has_pipe = "pipe_min_sma3" in columns and "pipe_max_sma3" in columns
    has_pre40 = "pre40_min_sma5" in columns and "pre40_max_sma5" in columns
    has_post60 = "post60_min_sma5" in columns and "post60_max_sma5" in columns
    has_post60_peak = "post60_peak_sma5" in columns

    pipeline_version_row = rc_conn.execute(
        """
        SELECT pipeline_version
        FROM rc_pipeline_episode
        WHERE pipeline_version IS NOT NULL
        ORDER BY computed_at DESC
        LIMIT 1
        """
    ).fetchone()
    pipeline_version = pipeline_version_row[0] if pipeline_version_row else "NA"
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("")
    print("EPISODE_REPORT")
    print(f"db_path={rc_db_path}")
    print(f"pipeline_version={pipeline_version}")
    print(f"generated_at_utc={now_utc}")

    counts_row = rc_conn.execute(
        """
        SELECT
          COUNT(*) AS n_total,
          SUM(CASE WHEN entry_window_exit_date IS NULL THEN 1 ELSE 0 END) AS n_open,
          SUM(CASE WHEN entry_window_exit_date IS NOT NULL THEN 1 ELSE 0 END) AS n_closed,
          SUM(CASE WHEN close_at_entry IS NULL OR close_at_ew_start IS NULL THEN 1 ELSE 0 END) AS n_missing_core_close,
          SUM(CASE WHEN days_entry_to_ew_trading IS NULL THEN 1 ELSE 0 END) AS n_missing_days_to_ew
        FROM rc_pipeline_episode
        """
    ).fetchone()
    print("")
    print("COUNTS")
    print(f"n_total={counts_row['n_total']}")
    print(f"n_open={counts_row['n_open']}")
    print(f"n_closed={counts_row['n_closed']}")
    print(f"n_missing_core_close={counts_row['n_missing_core_close']}")
    print(f"n_missing_days_to_ew={counts_row['n_missing_days_to_ew']}")

    exit_rows = rc_conn.execute(
        """
        SELECT
          COALESCE(entry_window_exit_state, 'NULL') AS exit_state,
          COUNT(*) AS n,
          ROUND(COUNT(*) * 1.0 / (SELECT COUNT(*) FROM rc_pipeline_episode), 4) AS pct
        FROM rc_pipeline_episode
        GROUP BY COALESCE(entry_window_exit_state, 'NULL')
        ORDER BY n DESC, exit_state
        """
    ).fetchall()
    print("")
    print("EXIT_STATE_DISTRIBUTION")
    print(f"{'exit_state':<24} {'n':>8} {'pct':>8}")
    for row in exit_rows:
        print(f"{row['exit_state']:<24} {row['n']:>8} {row['pct']:>8}")

    if has_confirm_confirmed:
        confirm_row = rc_conn.execute(
            """
            SELECT
              SUM(CASE WHEN ew_confirm_confirmed IS NOT NULL THEN 1 ELSE 0 END) AS n_has_confirm,
              SUM(CASE WHEN ew_confirm_confirmed = 1 THEN 1 ELSE 0 END) AS n_confirmed
            FROM rc_pipeline_episode
            """
        ).fetchone()
        n_has_confirm = confirm_row["n_has_confirm"] or 0
        n_confirmed = confirm_row["n_confirmed"] or 0
        pct_confirmed = round((n_confirmed * 1.0 / n_has_confirm), 4) if n_has_confirm > 0 else 0.0
        group_rows = rc_conn.execute(
            """
            SELECT
              CASE
                WHEN ew_confirm_confirmed IS NULL THEN 'NULL'
                WHEN ew_confirm_confirmed = 1 THEN '1'
                ELSE '0'
              END AS confirmed_bucket,
              COUNT(*) AS n,
              ROUND(COUNT(*) * 1.0 / (SELECT COUNT(*) FROM rc_pipeline_episode), 4) AS pct
            FROM rc_pipeline_episode
            GROUP BY confirmed_bucket
            ORDER BY confirmed_bucket
            """
        ).fetchall()
        print("")
        print("CONFIRMATION_DISTRIBUTION")
        print(f"n_has_confirm={n_has_confirm}")
        print(f"n_confirmed={n_confirmed}")
        print(f"pct_confirmed_among_has_confirm={_fmt_report_num(pct_confirmed)}")
        print(f"{'ew_confirm_confirmed':<24} {'n':>8} {'pct':>8}")
        for row in group_rows:
            print(f"{row['confirmed_bucket']:<24} {row['n']:>8} {_fmt_report_num(row['pct']):>8}")

    rc_conn.execute("ATTACH DATABASE ? AS os_report", (md_db_path,))
    entry_type_h_rows = rc_conn.execute(
        """
        WITH episode_base AS (
          SELECT
            e.episode_id,
            e.ticker,
            e.entry_window_date AS ew_date,
            e.peak60_sma5,
            json_extract(sd.state_attrs_json, '$.downtrend_entry_type') AS downtrend_entry_type
          FROM rc_pipeline_episode e
          JOIN rc_state_daily sd
            ON sd.ticker = e.ticker
           AND sd.date = e.entry_window_date
          WHERE sd.state = 'ENTRY_WINDOW'
            AND json_extract(sd.state_attrs_json, '$.downtrend_entry_type') IS NOT NULL
        ),
        fwd AS (
          SELECT
            b.episode_id,
            b.downtrend_entry_type,
            o.pvm,
            o.close,
            ROW_NUMBER() OVER (PARTITION BY b.episode_id ORDER BY o.pvm) AS fwd_idx
          FROM episode_base b
          JOIN os_report.osakedata o
            ON o.osake = b.ticker
           AND o.pvm >= b.ew_date
        ),
        per_episode AS (
          SELECT
            episode_id,
            downtrend_entry_type,
            MAX(CASE WHEN fwd_idx = 1 THEN close END) AS c0,
            MAX(CASE WHEN fwd_idx = 11 THEN close END) AS c10,
            MAX(CASE WHEN fwd_idx = 21 THEN close END) AS c20,
            MAX(CASE WHEN fwd_idx = 41 THEN close END) AS c40
          FROM fwd
          WHERE fwd_idx <= 41
          GROUP BY episode_id, downtrend_entry_type
        ),
        ret AS (
          SELECT
            p.episode_id,
            p.downtrend_entry_type,
            CASE WHEN p.c0 > 0 AND p.c10 IS NOT NULL THEN (p.c10 / p.c0 - 1.0) * 100.0 END AS r10,
            CASE WHEN p.c0 > 0 AND p.c20 IS NOT NULL THEN (p.c20 / p.c0 - 1.0) * 100.0 END AS r20,
            CASE WHEN p.c0 > 0 AND p.c40 IS NOT NULL THEN (p.c40 / p.c0 - 1.0) * 100.0 END AS r40,
            CASE WHEN p.c0 > 0 AND b.peak60_sma5 IS NOT NULL THEN (b.peak60_sma5 / p.c0 - 1.0) * 100.0 END AS r_peak60
          FROM per_episode p
          JOIN episode_base b
            ON b.episode_id = p.episode_id
        ),
        long AS (
          SELECT downtrend_entry_type, 'H10' AS horizon, r10 AS ret_pct FROM ret WHERE r10 IS NOT NULL
          UNION ALL
          SELECT downtrend_entry_type, 'H20' AS horizon, r20 AS ret_pct FROM ret WHERE r20 IS NOT NULL
          UNION ALL
          SELECT downtrend_entry_type, 'H40' AS horizon, r40 AS ret_pct FROM ret WHERE r40 IS NOT NULL
          UNION ALL
          SELECT downtrend_entry_type, 'PEAK60' AS horizon, r_peak60 AS ret_pct FROM ret WHERE r_peak60 IS NOT NULL
        ),
        ranked AS (
          SELECT
            downtrend_entry_type,
            horizon,
            ret_pct,
            ROW_NUMBER() OVER (PARTITION BY downtrend_entry_type, horizon ORDER BY ret_pct) AS rn,
            COUNT(*) OVER (PARTITION BY downtrend_entry_type, horizon) AS n
          FROM long
        )
        SELECT
          downtrend_entry_type,
          horizon,
          COUNT(*) AS n,
          ROUND(AVG(ret_pct), 2) AS avg_ret_pct,
          ROUND(
            (SELECT r2.ret_pct
             FROM ranked r2
             WHERE r2.downtrend_entry_type = ranked.downtrend_entry_type
               AND r2.horizon = ranked.horizon
               AND r2.rn = CAST((r2.n + 1) / 2 AS INT)
             LIMIT 1), 2
          ) AS median_ret_pct,
          ROUND(
            (SELECT r2.ret_pct
             FROM ranked r2
             WHERE r2.downtrend_entry_type = ranked.downtrend_entry_type
               AND r2.horizon = ranked.horizon
               AND r2.rn = MAX(1, CAST(r2.n * 0.10 AS INT))
             LIMIT 1), 2
          ) AS p10_ret_pct,
          ROUND(
            (SELECT r2.ret_pct
             FROM ranked r2
             WHERE r2.downtrend_entry_type = ranked.downtrend_entry_type
               AND r2.horizon = ranked.horizon
               AND r2.rn = MAX(1, CAST(r2.n * 0.90 AS INT))
             LIMIT 1), 2
          ) AS p90_ret_pct,
          ROUND(SUM(CASE WHEN ret_pct > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) AS pct_positive
        FROM ranked
        GROUP BY downtrend_entry_type, horizon
        ORDER BY downtrend_entry_type, horizon
        """
    ).fetchall()
    rc_conn.execute("DETACH DATABASE os_report")

    if entry_type_h_rows:
        print("")
        print("DOWNTREND_ENTRY_TYPE_RETURNS_H10_H20_H40")
        print(
            f"{'downtrend_entry_type':<20} {'horizon':<6} {'n':>6} {'avg_ret_pct':>12} "
            f"{'median_ret_pct':>14} {'p10_ret_pct':>12} {'p90_ret_pct':>12} {'pct_positive':>14}"
        )
        for row in entry_type_h_rows:
            print(
                f"{row['downtrend_entry_type']:<20} {row['horizon']:<6} {row['n']:>6} "
                f"{_fmt_report_num(row['avg_ret_pct']):>12} {_fmt_report_num(row['median_ret_pct']):>14} "
                f"{_fmt_report_num(row['p10_ret_pct']):>12} {_fmt_report_num(row['p90_ret_pct']):>12} "
                f"{_fmt_report_num(row['pct_positive']):>14}"
            )

    confirm_expr = "ew_confirm_confirmed" if has_confirm_confirmed else "NULL AS ew_confirm_confirmed"

    if has_peak60_sma5:
        growth_bucket_rows = rc_conn.execute(
            f"""
            WITH growth AS (
              SELECT
                ((peak60_sma5 / close_at_ew_start) - 1.0) * 100.0 AS growth_pct,
                entry_window_exit_state,
                {confirm_expr}
              FROM rc_pipeline_episode
              WHERE peak60_sma5 IS NOT NULL
                AND close_at_ew_start IS NOT NULL
                AND close_at_ew_start > 0
            )
            SELECT
              CASE
                WHEN growth_pct < -50 THEN '<-50%'
                WHEN growth_pct < -40 THEN '-50..-40%'
                WHEN growth_pct < -30 THEN '-40..-30%'
                WHEN growth_pct < -20 THEN '-30..-20%'
                WHEN growth_pct < -10 THEN '-20..-10%'
                WHEN growth_pct < 0 THEN '-10..0%'
                WHEN growth_pct < 10 THEN '0..10%'
                WHEN growth_pct < 20 THEN '10..20%'
                WHEN growth_pct < 30 THEN '20..30%'
                WHEN growth_pct < 40 THEN '30..40%'
                WHEN growth_pct < 50 THEN '40..50%'
                WHEN growth_pct < 60 THEN '50..60%'
                WHEN growth_pct < 70 THEN '60..70%'
                WHEN growth_pct < 80 THEN '70..80%'
                WHEN growth_pct < 90 THEN '80..90%'
                WHEN growth_pct < 100 THEN '90..100%'
                ELSE '100%+'
              END AS growth_bucket_10pct,
              COUNT(*) AS n,
              ROUND(COUNT(*) * 1.0 / (SELECT COUNT(*) FROM growth), 4) AS pct,
              SUM(CASE WHEN entry_window_exit_state = 'PASS' THEN 1 ELSE 0 END) AS n_exit_pass,
              SUM(CASE WHEN entry_window_exit_state = 'NO_TRADE' THEN 1 ELSE 0 END) AS n_exit_no_trade,
              SUM(CASE WHEN ew_confirm_confirmed = 0 THEN 1 ELSE 0 END) AS n_confirm_0,
              SUM(CASE WHEN ew_confirm_confirmed = 1 THEN 1 ELSE 0 END) AS n_confirm_1
            FROM growth
            GROUP BY growth_bucket_10pct
            ORDER BY
              CASE growth_bucket_10pct
                WHEN '<-50%' THEN 0
                WHEN '-50..-40%' THEN 1
                WHEN '-40..-30%' THEN 2
                WHEN '-30..-20%' THEN 3
                WHEN '-20..-10%' THEN 4
                WHEN '-10..0%' THEN 5
                WHEN '0..10%' THEN 6
                WHEN '10..20%' THEN 7
                WHEN '20..30%' THEN 8
                WHEN '30..40%' THEN 9
                WHEN '40..50%' THEN 10
                WHEN '50..60%' THEN 11
                WHEN '60..70%' THEN 12
                WHEN '70..80%' THEN 13
                WHEN '80..90%' THEN 14
                WHEN '90..100%' THEN 15
                ELSE 16
              END
            """
        ).fetchall()
        print("")
        print("PEAK60_SMA5_GROWTH_BUCKETS_10")
        print(
            f"{'bucket':<16} {'n':>8} {'pct':>8} {'n_exit_pass':>12} {'n_exit_no_trade':>16} "
            f"{'n_confirm_0':>12} {'n_confirm_1':>12}"
        )
        for row in growth_bucket_rows:
            print(
                f"{row['growth_bucket_10pct']:<16} {row['n']:>8} {_fmt_report_num(row['pct']):>8} "
                f"{row['n_exit_pass']:>12} {row['n_exit_no_trade']:>16} "
                f"{row['n_confirm_0']:>12} {row['n_confirm_1']:>12}"
            )

    if has_post60_peak:
        peak_growth_bucket_rows = rc_conn.execute(
            f"""
            WITH growth AS (
              SELECT
                ((post60_peak_sma5 / close_at_ew_start) - 1.0) * 100.0 AS growth_pct,
                entry_window_exit_state,
                {confirm_expr}
              FROM rc_pipeline_episode
              WHERE post60_peak_sma5 IS NOT NULL
                AND close_at_ew_start IS NOT NULL
                AND close_at_ew_start > 0
            )
            SELECT
              CASE
                WHEN growth_pct < -50 THEN '<-50%'
                WHEN growth_pct < -40 THEN '-50..-40%'
                WHEN growth_pct < -30 THEN '-40..-30%'
                WHEN growth_pct < -20 THEN '-30..-20%'
                WHEN growth_pct < -10 THEN '-20..-10%'
                WHEN growth_pct < 0 THEN '-10..0%'
                WHEN growth_pct < 10 THEN '0..10%'
                WHEN growth_pct < 20 THEN '10..20%'
                WHEN growth_pct < 30 THEN '20..30%'
                WHEN growth_pct < 40 THEN '30..40%'
                WHEN growth_pct < 50 THEN '40..50%'
                WHEN growth_pct < 60 THEN '50..60%'
                WHEN growth_pct < 70 THEN '60..70%'
                WHEN growth_pct < 80 THEN '70..80%'
                WHEN growth_pct < 90 THEN '80..90%'
                WHEN growth_pct < 100 THEN '90..100%'
                ELSE '100%+'
              END AS growth_bucket_10pct,
              COUNT(*) AS n,
              ROUND(COUNT(*) * 1.0 / (SELECT COUNT(*) FROM growth), 4) AS pct,
              SUM(CASE WHEN entry_window_exit_state = 'PASS' THEN 1 ELSE 0 END) AS n_exit_pass,
              SUM(CASE WHEN entry_window_exit_state = 'NO_TRADE' THEN 1 ELSE 0 END) AS n_exit_no_trade,
              SUM(CASE WHEN ew_confirm_confirmed = 0 THEN 1 ELSE 0 END) AS n_confirm_0,
              SUM(CASE WHEN ew_confirm_confirmed = 1 THEN 1 ELSE 0 END) AS n_confirm_1
            FROM growth
            GROUP BY growth_bucket_10pct
            ORDER BY
              CASE growth_bucket_10pct
                WHEN '<-50%' THEN 0
                WHEN '-50..-40%' THEN 1
                WHEN '-40..-30%' THEN 2
                WHEN '-30..-20%' THEN 3
                WHEN '-20..-10%' THEN 4
                WHEN '-10..0%' THEN 5
                WHEN '0..10%' THEN 6
                WHEN '10..20%' THEN 7
                WHEN '20..30%' THEN 8
                WHEN '30..40%' THEN 9
                WHEN '40..50%' THEN 10
                WHEN '50..60%' THEN 11
                WHEN '60..70%' THEN 12
                WHEN '70..80%' THEN 13
                WHEN '80..90%' THEN 14
                WHEN '90..100%' THEN 15
                ELSE 16
              END
            """
        ).fetchall()
        print("")
        print("POST60_PEAK_SMA5_GROWTH_BUCKETS_10")
        print(
            f"{'bucket':<16} {'n':>8} {'pct':>8} {'n_exit_pass':>12} {'n_exit_no_trade':>16} "
            f"{'n_confirm_0':>12} {'n_confirm_1':>12}"
        )
        for row in peak_growth_bucket_rows:
            print(
                f"{row['growth_bucket_10pct']:<16} {row['n']:>8} {_fmt_report_num(row['pct']):>8} "
                f"{row['n_exit_pass']:>12} {row['n_exit_no_trade']:>16} "
                f"{row['n_confirm_0']:>12} {row['n_confirm_1']:>12}"
            )

    if "peak60_days_from_ew_start" in columns:
        peak60_bucket_rows = rc_conn.execute(
            f"""
            WITH base AS (
              SELECT
                peak60_days_from_ew_start AS days_to_peak60,
                entry_window_exit_state,
                {confirm_expr}
              FROM rc_pipeline_episode
              WHERE peak60_days_from_ew_start IS NOT NULL
            )
            SELECT
              CASE
                WHEN days_to_peak60 < 0 THEN '<0'
                WHEN days_to_peak60 <= 10 THEN '0-10'
                WHEN days_to_peak60 <= 20 THEN '11-20'
                WHEN days_to_peak60 <= 30 THEN '21-30'
                WHEN days_to_peak60 <= 40 THEN '31-40'
                WHEN days_to_peak60 <= 50 THEN '41-50'
                WHEN days_to_peak60 <= 60 THEN '51-60'
                ELSE '61+'
              END AS peak60_date_bucket_10d,
              COUNT(*) AS n,
              ROUND(COUNT(*) * 1.0 / (SELECT COUNT(*) FROM base), 4) AS pct,
              SUM(CASE WHEN entry_window_exit_state = 'PASS' THEN 1 ELSE 0 END) AS n_exit_pass,
              SUM(CASE WHEN entry_window_exit_state = 'NO_TRADE' THEN 1 ELSE 0 END) AS n_exit_no_trade,
              SUM(CASE WHEN ew_confirm_confirmed = 0 THEN 1 ELSE 0 END) AS n_confirm_0,
              SUM(CASE WHEN ew_confirm_confirmed = 1 THEN 1 ELSE 0 END) AS n_confirm_1
            FROM base
            GROUP BY peak60_date_bucket_10d
            ORDER BY
              CASE peak60_date_bucket_10d
                WHEN '<0' THEN 0
                WHEN '0-10' THEN 1
                WHEN '11-20' THEN 2
                WHEN '21-30' THEN 3
                WHEN '31-40' THEN 4
                WHEN '41-50' THEN 5
                WHEN '51-60' THEN 6
                ELSE 7
              END
            """
        ).fetchall()
        print("")
        print("PEAK60_SMA5_DATE_BUCKETS_10D")
        print(
            f"{'bucket':<8} {'n':>8} {'pct':>8} {'n_exit_pass':>12} {'n_exit_no_trade':>16} "
            f"{'n_confirm_0':>12} {'n_confirm_1':>12}"
        )
        for row in peak60_bucket_rows:
            print(
                f"{row['peak60_date_bucket_10d']:<8} {row['n']:>8} {_fmt_report_num(row['pct']):>8} "
                f"{row['n_exit_pass']:>12} {row['n_exit_no_trade']:>16} "
                f"{row['n_confirm_0']:>12} {row['n_confirm_1']:>12}"
            )

    if "post60_peak_days_from_exit" in columns:
        post60_peak_bucket_rows = rc_conn.execute(
            f"""
            WITH base AS (
              SELECT
                post60_peak_days_from_exit AS days_to_post60_peak,
                entry_window_exit_state,
                {confirm_expr}
              FROM rc_pipeline_episode
              WHERE post60_peak_days_from_exit IS NOT NULL
            )
            SELECT
              CASE
                WHEN days_to_post60_peak < 0 THEN '<0'
                WHEN days_to_post60_peak <= 10 THEN '0-10'
                WHEN days_to_post60_peak <= 20 THEN '11-20'
                WHEN days_to_post60_peak <= 30 THEN '21-30'
                WHEN days_to_post60_peak <= 40 THEN '31-40'
                WHEN days_to_post60_peak <= 50 THEN '41-50'
                WHEN days_to_post60_peak <= 60 THEN '51-60'
                ELSE '61+'
              END AS post60_peak_date_bucket_10d,
              COUNT(*) AS n,
              ROUND(COUNT(*) * 1.0 / (SELECT COUNT(*) FROM base), 4) AS pct,
              SUM(CASE WHEN entry_window_exit_state = 'PASS' THEN 1 ELSE 0 END) AS n_exit_pass,
              SUM(CASE WHEN entry_window_exit_state = 'NO_TRADE' THEN 1 ELSE 0 END) AS n_exit_no_trade,
              SUM(CASE WHEN ew_confirm_confirmed = 0 THEN 1 ELSE 0 END) AS n_confirm_0,
              SUM(CASE WHEN ew_confirm_confirmed = 1 THEN 1 ELSE 0 END) AS n_confirm_1
            FROM base
            GROUP BY post60_peak_date_bucket_10d
            ORDER BY
              CASE post60_peak_date_bucket_10d
                WHEN '<0' THEN 0
                WHEN '0-10' THEN 1
                WHEN '11-20' THEN 2
                WHEN '21-30' THEN 3
                WHEN '31-40' THEN 4
                WHEN '41-50' THEN 5
                WHEN '51-60' THEN 6
                ELSE 7
              END
            """
        ).fetchall()
        print("")
        print("POST60_PEAK_SMA5_DATE_BUCKETS_10D")
        print(
            f"{'bucket':<8} {'n':>8} {'pct':>8} {'n_exit_pass':>12} {'n_exit_no_trade':>16} "
            f"{'n_confirm_0':>12} {'n_confirm_1':>12}"
        )
        for row in post60_peak_bucket_rows:
            print(
                f"{row['post60_peak_date_bucket_10d']:<8} {row['n']:>8} {_fmt_report_num(row['pct']):>8} "
                f"{row['n_exit_pass']:>12} {row['n_exit_no_trade']:>16} "
                f"{row['n_confirm_0']:>12} {row['n_confirm_1']:>12}"
            )

    if has_peak60_sma5:
        top_sql = f"""
            SELECT
              ticker,
              entry_window_date,
              entry_window_exit_state,
              ROUND((peak60_sma5 / close_at_ew_start - 1.0) * 100.0, 4) AS growth_pct,
              peak60_sma5,
              close_at_ew_start,
              {'peak60_days_from_ew_start' if has_peak60_days else 'NULL AS peak60_days_from_ew_start'}
            FROM rc_pipeline_episode
            WHERE peak60_sma5 IS NOT NULL
              AND close_at_ew_start IS NOT NULL
              AND close_at_ew_start > 0
            ORDER BY growth_pct DESC
            LIMIT 10
        """
        top_rows = rc_conn.execute(top_sql).fetchall()
        print("")
        print("TOP_10_BY_EW_TO_PEAK60_SMA5_GROWTH")
        print(
            "ticker | entry_window_date | exit_state | growth_pct | peak60_sma5 | "
            "close_at_ew_start | peak60_days_from_ew_start"
        )
        for row in top_rows:
            vals = [
                row["ticker"],
                row["entry_window_date"],
                _fmt_report_cell(row["entry_window_exit_state"]),
                _fmt_report_cell(row["growth_pct"]),
                _fmt_report_cell(row["peak60_sma5"]),
                _fmt_report_cell(row["close_at_ew_start"]),
                _fmt_report_cell(row["peak60_days_from_ew_start"]),
            ]
            print(" | ".join(str(v) for v in vals))


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


def ensure_rc_pipeline_episode_table(rc_conn) -> None:
    rc_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rc_pipeline_episode (
          episode_id TEXT PRIMARY KEY,

          ticker TEXT NOT NULL,
          downtrend_entry_date TEXT NOT NULL,
          entry_window_date TEXT NOT NULL,

          entry_window_exit_date TEXT NULL,
          entry_window_exit_state TEXT NULL,

          close_at_entry REAL NULL,
          close_at_ew_start REAL NULL,
          close_at_ew_exit REAL NULL,

          days_entry_to_ew_trading INTEGER NULL,
          days_in_entry_window_trading INTEGER NULL,

          pipe_min_sma3 REAL NULL,
          pipe_max_sma3 REAL NULL,

          pre40_min_sma5 REAL NULL,
          pre40_max_sma5 REAL NULL,

          post60_min_sma5 REAL NULL,
          post60_max_sma5 REAL NULL,

          exit_reasons_json TEXT NULL,

          computed_at TEXT NOT NULL,
          pipeline_version TEXT NOT NULL
        );
        """
    )
    rc_conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rc_pipeline_episode_ticker
          ON rc_pipeline_episode(ticker);
        """
    )
    rc_conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rc_pipeline_episode_entry_window_date
          ON rc_pipeline_episode(entry_window_date);
        """
    )
    rc_conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rc_pipeline_episode_entry_window_exit_date
          ON rc_pipeline_episode(entry_window_exit_date);
        """
    )
    rc_conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rc_pipeline_episode_pipeline_version
          ON rc_pipeline_episode(pipeline_version);
        """
    )
    existing_columns = {
        row[1]
        for row in rc_conn.execute("PRAGMA table_info(rc_pipeline_episode)").fetchall()
    }
    columns_to_add = [
        ("peak60_sma5_date", "TEXT NULL"),
        ("peak60_days_from_ew_start", "INTEGER NULL"),
        ("peak60_sma5", "REAL NULL"),
        ("peak60_growth_pct_close_ew_to_peak", "REAL NULL"),
        ("pre40_peak_sma5_date", "TEXT NULL"),
        ("pre40_peak_days_before_entry", "INTEGER NULL"),
        ("pre40_peak_sma5", "REAL NULL"),
        ("pre40_trough_sma5_date", "TEXT NULL"),
        ("pre40_trough_days_before_entry", "INTEGER NULL"),
        ("pre40_trough_sma5", "REAL NULL"),
        ("post60_peak_sma5_date", "TEXT NULL"),
        ("post60_peak_days_from_exit", "INTEGER NULL"),
        ("post60_peak_sma5", "REAL NULL"),
        ("ew_confirm_rule", "TEXT NULL"),
        ("ew_confirm_above_5", "INTEGER NULL"),
        ("ew_confirm_confirmed", "INTEGER NULL"),
    ]
    for column_name, column_type in columns_to_add:
        if column_name not in existing_columns:
            rc_conn.execute(
                f"ALTER TABLE rc_pipeline_episode ADD COLUMN {column_name} {column_type}"
            )
    transition_columns = {
        row[1] for row in rc_conn.execute("PRAGMA table_info(rc_transition)").fetchall()
    }
    if "state_attrs_json" not in transition_columns:
        rc_conn.execute(
            "ALTER TABLE rc_transition ADD COLUMN state_attrs_json TEXT NULL"
        )
    state_daily_columns = {
        row[1] for row in rc_conn.execute("PRAGMA table_info(rc_state_daily)").fetchall()
    }
    if "state_attrs_json" not in state_daily_columns:
        rc_conn.execute(
            "ALTER TABLE rc_state_daily ADD COLUMN state_attrs_json TEXT NULL"
        )


def populate_rc_pipeline_episode(
    rc_conn,
    md_conn,
    date_from: str,
    date_to: str,
    pipeline_version: str = "rc_pipeline_episode_v1",
) -> None:
    entry_window_starts = rc_conn.execute(
        """
        SELECT ticker, date
        FROM rc_transition
        WHERE to_state='ENTRY_WINDOW' AND date>=? AND date<=?
        GROUP BY ticker, date
        ORDER BY ticker, date
        """,
        (date_from, date_to),
    ).fetchall()

    upsert_sql = """
    INSERT INTO rc_pipeline_episode (
      episode_id,
      ticker,
      downtrend_entry_date,
      entry_window_date,
      entry_window_exit_date,
      entry_window_exit_state,
      close_at_entry,
      close_at_ew_start,
      close_at_ew_exit,
      days_entry_to_ew_trading,
      days_in_entry_window_trading,
      exit_reasons_json,
      computed_at,
      pipeline_version
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(episode_id) DO UPDATE SET
      ticker=excluded.ticker,
      downtrend_entry_date=excluded.downtrend_entry_date,
      entry_window_date=excluded.entry_window_date,
      entry_window_exit_date=excluded.entry_window_exit_date,
      entry_window_exit_state=excluded.entry_window_exit_state,
      close_at_entry=excluded.close_at_entry,
      close_at_ew_start=excluded.close_at_ew_start,
      close_at_ew_exit=excluded.close_at_ew_exit,
      days_entry_to_ew_trading=excluded.days_entry_to_ew_trading,
      days_in_entry_window_trading=excluded.days_in_entry_window_trading,
      exit_reasons_json=excluded.exit_reasons_json,
      computed_at=excluded.computed_at,
      pipeline_version=excluded.pipeline_version
    """

    for row in entry_window_starts:
        ticker = row["ticker"]
        entry_window_date = row["date"]

        downtrend_row = rc_conn.execute(
            """
            SELECT MAX(date)
            FROM rc_transition
            WHERE ticker=? AND from_state='NO_TRADE' AND to_state='DOWNTREND_EARLY' AND date<=?
            """,
            (ticker, entry_window_date),
        ).fetchone()
        downtrend_entry_date = downtrend_row[0] if downtrend_row else None
        if not downtrend_entry_date:
            continue

        exit_row = rc_conn.execute(
            """
            SELECT date, to_state, reasons_json
            FROM rc_transition
            WHERE ticker=? AND from_state='ENTRY_WINDOW' AND date>?
            ORDER BY date ASC
            LIMIT 1
            """,
            (ticker, entry_window_date),
        ).fetchone()

        entry_window_exit_date = exit_row["date"] if exit_row else None
        entry_window_exit_state = exit_row["to_state"] if exit_row else None
        exit_reasons_json = exit_row["reasons_json"] if exit_row else None

        close_entry_row = md_conn.execute(
            "SELECT close FROM osakedata WHERE osake=? AND pvm=? LIMIT 1",
            (ticker, downtrend_entry_date),
        ).fetchone()
        close_at_entry = close_entry_row[0] if close_entry_row else None

        close_ew_row = md_conn.execute(
            "SELECT close FROM osakedata WHERE osake=? AND pvm=? LIMIT 1",
            (ticker, entry_window_date),
        ).fetchone()
        close_at_ew_start = close_ew_row[0] if close_ew_row else None

        close_at_ew_exit = None
        if entry_window_exit_date:
            close_exit_row = md_conn.execute(
                "SELECT close FROM osakedata WHERE osake=? AND pvm=? LIMIT 1",
                (ticker, entry_window_exit_date),
            ).fetchone()
            close_at_ew_exit = close_exit_row[0] if close_exit_row else None

        days_entry_to_ew_trading = md_conn.execute(
            """
            SELECT COUNT(*)
            FROM osakedata
            WHERE osake=? AND pvm>=? AND pvm<=?
            """,
            (ticker, downtrend_entry_date, entry_window_date),
        ).fetchone()[0]

        days_in_entry_window_trading = None
        if entry_window_exit_date:
            days_in_entry_window_trading = md_conn.execute(
                """
                SELECT COUNT(*)
                FROM osakedata
                WHERE osake=? AND pvm>=? AND pvm<=?
                """,
                (ticker, entry_window_date, entry_window_exit_date),
            ).fetchone()[0]

        episode_id = f"{ticker}|{downtrend_entry_date}|{entry_window_date}"
        computed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        rc_conn.execute(
            upsert_sql,
            (
                episode_id,
                ticker,
                downtrend_entry_date,
                entry_window_date,
                entry_window_exit_date,
                entry_window_exit_state,
                close_at_entry,
                close_at_ew_start,
                close_at_ew_exit,
                days_entry_to_ew_trading,
                days_in_entry_window_trading,
                exit_reasons_json,
                computed_at,
                pipeline_version,
            ),
        )


def populate_rc_pipeline_episode_sma_extremes(rc_conn, md_db_path: str) -> None:
    rc_conn.execute("ATTACH DATABASE ? AS os", (md_db_path,))
    rc_conn.executescript(
        """
        WITH
        episodes AS (
          SELECT
            episode_id,
            ticker,
            downtrend_entry_date AS entry_date,
            entry_window_exit_date AS exit_date
          FROM rc_pipeline_episode
          WHERE entry_window_exit_date IS NOT NULL
        ),

        pipe_prices AS (
          SELECT
            e.episode_id,
            o.pvm,
            o.close,
            AVG(o.close) OVER (
              PARTITION BY e.episode_id
              ORDER BY o.pvm
              ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS sma3
          FROM episodes e
          JOIN os.osakedata o
            ON o.osake = e.ticker
           AND o.pvm BETWEEN e.entry_date AND e.exit_date
        ),
        pipe_sma3_minmax AS (
          SELECT
            episode_id,
            MIN(sma3) AS pipe_min_sma3,
            MAX(sma3) AS pipe_max_sma3
          FROM pipe_prices
          WHERE sma3 IS NOT NULL
          GROUP BY episode_id
        ),

        pre40_base AS (
          SELECT
            e.episode_id,
            o.pvm,
            o.close,
            ROW_NUMBER() OVER (
              PARTITION BY e.episode_id
              ORDER BY o.pvm DESC
            ) AS rn_desc
          FROM episodes e
          JOIN os.osakedata o
            ON o.osake = e.ticker
           AND o.pvm < e.entry_date
        ),
        pre40_limited AS (
          SELECT
            episode_id,
            pvm,
            close
          FROM pre40_base
          WHERE rn_desc <= 40
        ),
        pre40_sma AS (
          SELECT
            episode_id,
            pvm,
            AVG(close) OVER (
              PARTITION BY episode_id
              ORDER BY pvm
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS sma5
          FROM pre40_limited
        ),
        pre40_sma5_minmax AS (
          SELECT
            episode_id,
            MIN(sma5) AS pre40_min_sma5,
            MAX(sma5) AS pre40_max_sma5
          FROM pre40_sma
          WHERE sma5 IS NOT NULL
          GROUP BY episode_id
        ),

        post60_base AS (
          SELECT
            e.episode_id,
            o.pvm,
            o.close,
            ROW_NUMBER() OVER (
              PARTITION BY e.episode_id
              ORDER BY o.pvm ASC
            ) AS rn_asc
          FROM episodes e
          JOIN os.osakedata o
            ON o.osake = e.ticker
           AND o.pvm > e.exit_date
        ),
        post60_limited AS (
          SELECT
            episode_id,
            pvm,
            close
          FROM post60_base
          WHERE rn_asc <= 60
        ),
        post60_sma AS (
          SELECT
            episode_id,
            pvm,
            AVG(close) OVER (
              PARTITION BY episode_id
              ORDER BY pvm
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS sma5
          FROM post60_limited
        ),
        post60_sma5_minmax AS (
          SELECT
            episode_id,
            MIN(sma5) AS post60_min_sma5,
            MAX(sma5) AS post60_max_sma5
          FROM post60_sma
          WHERE sma5 IS NOT NULL
          GROUP BY episode_id
        ),

        all_updates AS (
          SELECT
            e.episode_id,
            p.pipe_min_sma3,
            p.pipe_max_sma3,
            pre.pre40_min_sma5,
            pre.pre40_max_sma5,
            post.post60_min_sma5,
            post.post60_max_sma5
          FROM episodes e
          LEFT JOIN pipe_sma3_minmax p  ON p.episode_id = e.episode_id
          LEFT JOIN pre40_sma5_minmax pre ON pre.episode_id = e.episode_id
          LEFT JOIN post60_sma5_minmax post ON post.episode_id = e.episode_id
        )

        UPDATE rc_pipeline_episode
        SET
          pipe_min_sma3 = (SELECT a.pipe_min_sma3 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pipe_max_sma3 = (SELECT a.pipe_max_sma3 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pre40_min_sma5 = (SELECT a.pre40_min_sma5 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pre40_max_sma5 = (SELECT a.pre40_max_sma5 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          post60_min_sma5 = (SELECT a.post60_min_sma5 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          post60_max_sma5 = (SELECT a.post60_max_sma5 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          computed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE episode_id IN (SELECT episode_id FROM all_updates);
        """
    )
    rc_conn.execute("DETACH DATABASE os")


def populate_rc_pipeline_episode_peak_timing_fields(rc_conn, md_db_path: str) -> None:
    rc_conn.execute("ATTACH DATABASE ? AS os", (md_db_path,))
    rc_conn.executescript(
        """
        WITH
        episodes AS (
          SELECT
            episode_id,
            ticker,
            downtrend_entry_date,
            entry_window_date,
            entry_window_exit_date,
            close_at_ew_start
          FROM rc_pipeline_episode
        ),

        forward_base AS (
          SELECT
            e.episode_id,
            o.pvm,
            o.close,
            ROW_NUMBER() OVER (
              PARTITION BY e.episode_id
              ORDER BY o.pvm
            ) - 1 AS day_idx
          FROM episodes e
          JOIN os.osakedata o
            ON o.osake = e.ticker
           AND o.pvm >= e.entry_window_date
        ),
        forward_0_60 AS (
          SELECT *
          FROM forward_base
          WHERE day_idx BETWEEN 0 AND 60
        ),
        forward_sma AS (
          SELECT
            episode_id,
            pvm,
            close,
            day_idx,
            AVG(close) OVER (
              PARTITION BY episode_id
              ORDER BY pvm
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS sma5
          FROM forward_0_60
        ),
        peak60_ranked AS (
          SELECT
            fs.episode_id,
            fs.pvm AS peak60_sma5_date,
            fs.day_idx AS peak60_days_from_ew_start,
            fs.sma5 AS peak60_sma5,
            ((fs.close / e.close_at_ew_start) - 1.0) * 100.0 AS peak60_growth_pct_close_ew_to_peak,
            ROW_NUMBER() OVER (
              PARTITION BY fs.episode_id
              ORDER BY fs.sma5 DESC, fs.pvm ASC
            ) AS rn
          FROM forward_sma fs
          JOIN episodes e
            ON e.episode_id = fs.episode_id
          WHERE fs.sma5 IS NOT NULL
            AND e.close_at_ew_start IS NOT NULL
        ),
        peak60_pick AS (
          SELECT
            episode_id,
            peak60_sma5_date,
            peak60_days_from_ew_start,
            peak60_sma5,
            peak60_growth_pct_close_ew_to_peak
          FROM peak60_ranked
          WHERE rn = 1
        ),

        pre40_base AS (
          SELECT
            e.episode_id,
            o.pvm,
            o.close,
            ROW_NUMBER() OVER (
              PARTITION BY e.episode_id
              ORDER BY o.pvm DESC
            ) AS days_before_entry
          FROM episodes e
          JOIN os.osakedata o
            ON o.osake = e.ticker
           AND o.pvm < e.downtrend_entry_date
        ),
        pre40_limited AS (
          SELECT
            episode_id,
            pvm,
            close,
            days_before_entry
          FROM pre40_base
          WHERE days_before_entry <= 40
        ),
        pre40_sma AS (
          SELECT
            episode_id,
            pvm,
            close,
            days_before_entry,
            AVG(close) OVER (
              PARTITION BY episode_id
              ORDER BY pvm
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS sma5
          FROM pre40_limited
        ),
        pre40_peak_ranked AS (
          SELECT
            episode_id,
            pvm AS pre40_peak_sma5_date,
            days_before_entry AS pre40_peak_days_before_entry,
            sma5 AS pre40_peak_sma5,
            ROW_NUMBER() OVER (
              PARTITION BY episode_id
              ORDER BY sma5 DESC, pvm ASC
            ) AS rn
          FROM pre40_sma
          WHERE sma5 IS NOT NULL
        ),
        pre40_peak_pick AS (
          SELECT
            episode_id,
            pre40_peak_sma5_date,
            pre40_peak_days_before_entry,
            pre40_peak_sma5
          FROM pre40_peak_ranked
          WHERE rn = 1
        ),
        pre40_trough_ranked AS (
          SELECT
            episode_id,
            pvm AS pre40_trough_sma5_date,
            days_before_entry AS pre40_trough_days_before_entry,
            sma5 AS pre40_trough_sma5,
            ROW_NUMBER() OVER (
              PARTITION BY episode_id
              ORDER BY sma5 ASC, pvm ASC
            ) AS rn
          FROM pre40_sma
          WHERE sma5 IS NOT NULL
        ),
        pre40_trough_pick AS (
          SELECT
            episode_id,
            pre40_trough_sma5_date,
            pre40_trough_days_before_entry,
            pre40_trough_sma5
          FROM pre40_trough_ranked
          WHERE rn = 1
        ),

        post60_base AS (
          SELECT
            e.episode_id,
            o.pvm,
            o.close,
            ROW_NUMBER() OVER (
              PARTITION BY e.episode_id
              ORDER BY o.pvm ASC
            ) AS days_from_exit
          FROM episodes e
          JOIN os.osakedata o
            ON o.osake = e.ticker
           AND e.entry_window_exit_date IS NOT NULL
           AND o.pvm > e.entry_window_exit_date
        ),
        post60_limited AS (
          SELECT
            episode_id,
            pvm,
            close,
            days_from_exit
          FROM post60_base
          WHERE days_from_exit <= 60
        ),
        post60_sma AS (
          SELECT
            episode_id,
            pvm,
            close,
            days_from_exit,
            AVG(close) OVER (
              PARTITION BY episode_id
              ORDER BY pvm
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS sma5
          FROM post60_limited
        ),
        post60_peak_ranked AS (
          SELECT
            episode_id,
            pvm AS post60_peak_sma5_date,
            days_from_exit AS post60_peak_days_from_exit,
            sma5 AS post60_peak_sma5,
            ROW_NUMBER() OVER (
              PARTITION BY episode_id
              ORDER BY sma5 DESC, pvm ASC
            ) AS rn
          FROM post60_sma
          WHERE sma5 IS NOT NULL
        ),
        post60_peak_pick AS (
          SELECT
            episode_id,
            post60_peak_sma5_date,
            post60_peak_days_from_exit,
            post60_peak_sma5
          FROM post60_peak_ranked
          WHERE rn = 1
        ),

        all_updates AS (
          SELECT
            e.episode_id,
            p60.peak60_sma5_date,
            p60.peak60_days_from_ew_start,
            p60.peak60_sma5,
            p60.peak60_growth_pct_close_ew_to_peak,
            p40p.pre40_peak_sma5_date,
            p40p.pre40_peak_days_before_entry,
            p40p.pre40_peak_sma5,
            p40t.pre40_trough_sma5_date,
            p40t.pre40_trough_days_before_entry,
            p40t.pre40_trough_sma5,
            p60x.post60_peak_sma5_date,
            p60x.post60_peak_days_from_exit,
            p60x.post60_peak_sma5
          FROM episodes e
          LEFT JOIN peak60_pick p60
            ON p60.episode_id = e.episode_id
          LEFT JOIN pre40_peak_pick p40p
            ON p40p.episode_id = e.episode_id
          LEFT JOIN pre40_trough_pick p40t
            ON p40t.episode_id = e.episode_id
          LEFT JOIN post60_peak_pick p60x
            ON p60x.episode_id = e.episode_id
        )

        UPDATE rc_pipeline_episode
        SET
          peak60_sma5_date = (SELECT a.peak60_sma5_date FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          peak60_days_from_ew_start = (SELECT a.peak60_days_from_ew_start FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          peak60_sma5 = (SELECT a.peak60_sma5 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          peak60_growth_pct_close_ew_to_peak = (SELECT a.peak60_growth_pct_close_ew_to_peak FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pre40_peak_sma5_date = (SELECT a.pre40_peak_sma5_date FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pre40_peak_days_before_entry = (SELECT a.pre40_peak_days_before_entry FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pre40_peak_sma5 = (SELECT a.pre40_peak_sma5 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pre40_trough_sma5_date = (SELECT a.pre40_trough_sma5_date FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pre40_trough_days_before_entry = (SELECT a.pre40_trough_days_before_entry FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          pre40_trough_sma5 = (SELECT a.pre40_trough_sma5 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          post60_peak_sma5_date = (SELECT a.post60_peak_sma5_date FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          post60_peak_days_from_exit = (SELECT a.post60_peak_days_from_exit FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          post60_peak_sma5 = (SELECT a.post60_peak_sma5 FROM all_updates a WHERE a.episode_id = rc_pipeline_episode.episode_id),
          computed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE episode_id IN (SELECT episode_id FROM all_updates);
        """
    )
    rc_conn.execute("DETACH DATABASE os")


def populate_rc_pipeline_episode_entry_confirmation(rc_conn, md_db_path: str) -> None:
    rc_conn.execute("ATTACH DATABASE ? AS os", (md_db_path,))
    rc_conn.executescript(
        """
        DROP TABLE IF EXISTS temp._ew_confirm_updates;

        CREATE TEMP TABLE _ew_confirm_updates AS
        WITH episodes AS (
          SELECT
            episode_id,
            ticker,
            entry_window_date AS ew_date
          FROM rc_pipeline_episode
          WHERE entry_window_date IS NOT NULL
        ),
        series AS (
          SELECT
            e.episode_id,
            e.ticker,
            e.ew_date,
            o.pvm,
            o.close,
            AVG(o.close) OVER (
              PARTITION BY e.episode_id
              ORDER BY o.pvm
              ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS sma5
          FROM episodes e
          JOIN os.osakedata o
            ON o.osake = e.ticker
           AND o.pvm BETWEEN date(e.ew_date, '-40 day') AND date(e.ew_date, '+10 day')
        ),
        forward AS (
          SELECT
            episode_id,
            ticker,
            ew_date,
            pvm,
            close,
            sma5,
            ROW_NUMBER() OVER (
              PARTITION BY episode_id
              ORDER BY pvm
            ) AS fwd_idx
          FROM series
          WHERE pvm >= ew_date
        ),
        per_episode AS (
          SELECT
            episode_id,
            ticker,
            ew_date,
            SUM(CASE WHEN fwd_idx <= 5 AND sma5 IS NOT NULL AND close > sma5 THEN 1 ELSE 0 END) AS above_5,
            MAX(CASE WHEN fwd_idx = 5 THEN pvm END) AS decision_date,
            MAX(fwd_idx) AS max_fwd_idx
          FROM forward
          WHERE fwd_idx <= 5
          GROUP BY episode_id, ticker, ew_date
        )
        SELECT
          episode_id,
          ticker,
          ew_date,
          decision_date,
          above_5,
          CASE WHEN above_5 >= 3 THEN 1 ELSE 0 END AS confirmed
        FROM per_episode
        WHERE max_fwd_idx = 5;

        UPDATE rc_pipeline_episode
        SET
          ew_confirm_rule = 'CLOSE_GT_SMA5_3_OF_5',
          ew_confirm_above_5 = (
            SELECT u.above_5
            FROM _ew_confirm_updates u
            WHERE u.episode_id = rc_pipeline_episode.episode_id
          ),
          ew_confirm_confirmed = (
            SELECT u.confirmed
            FROM _ew_confirm_updates u
            WHERE u.episode_id = rc_pipeline_episode.episode_id
          ),
          computed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE episode_id IN (SELECT episode_id FROM _ew_confirm_updates);

        UPDATE rc_transition
        SET
          state_attrs_json = json_set(
            json_set(
              json_set(
                CASE
                  WHEN COALESCE(state_attrs_json, '') = '' THEN '{}'
                  ELSE state_attrs_json
                END,
                '$.status.entry_continuation_rule',
                'CLOSE_GT_SMA5_3_OF_5'
              ),
              '$.status.entry_continuation_above_5',
              (
                SELECT u.above_5
                FROM _ew_confirm_updates u
                WHERE u.ticker = rc_transition.ticker
                  AND u.ew_date = rc_transition.date
              )
            ),
            '$.status.entry_continuation_confirmed',
            CASE
              WHEN (
                SELECT u.confirmed
                FROM _ew_confirm_updates u
                WHERE u.ticker = rc_transition.ticker
                  AND u.ew_date = rc_transition.date
              ) = 1 THEN json('true')
              ELSE json('false')
            END
          )
        WHERE to_state = 'ENTRY_WINDOW'
          AND EXISTS (
            SELECT 1
            FROM _ew_confirm_updates u
            WHERE u.ticker = rc_transition.ticker
              AND u.ew_date = rc_transition.date
          );

        UPDATE rc_state_daily
        SET
          state_attrs_json = json_set(
            CASE
              WHEN COALESCE(state_attrs_json, '') = '' THEN '{}'
              ELSE state_attrs_json
            END,
            '$.status.entry_continuation_confirmed',
            CASE
              WHEN (
                SELECT u.confirmed
                FROM _ew_confirm_updates u
                WHERE u.ticker = rc_state_daily.ticker
                  AND u.decision_date = rc_state_daily.date
              ) = 1 THEN json('true')
              ELSE json('false')
            END
          )
        WHERE EXISTS (
          SELECT 1
          FROM _ew_confirm_updates u
          WHERE u.ticker = rc_state_daily.ticker
            AND u.decision_date = rc_state_daily.date
        );

        DROP TABLE IF EXISTS temp._ew_confirm_updates;
        """
    )
    rc_conn.execute("DETACH DATABASE os")


def main() -> None:
    args = parse_args()

    if _debug_enabled(args):
        set_evaluator_debug(lambda msg: _dbg(args, msg))
        set_churn_guard_debug(lambda msg: _dbg(args, msg))
    else:
        set_evaluator_debug(None)
        set_churn_guard_debug(None)

    md_conn = get_readonly_connection(args.md_db)
    rc_conn = get_connection(args.rc_db)
    try:
        apply_migrations(rc_conn)
        ensure_rc_pipeline_episode_table(rc_conn)
        rc_conn.commit()

        universe_reader = TickerUniverseReader(md_conn)
        spec = build_spec(args)
        tickers = universe_reader.resolve_tickers(spec)
        if args.min_history_rows > 0:
            tickers_before = list(tickers)
            before_filter = len(tickers_before)
            tickers = universe_reader.filter_by_osakedata(
                tickers=tickers,
                as_of_date=args.date_from,
                osakedata_table="osakedata",
                min_history_rows=args.min_history_rows,
                require_row_on_date=args.require_row_on_date,
            )
            removed_list = [t for t in tickers_before if t not in set(tickers)]
            _dbg(args, f"FILTER min_history_rows before={before_filter} after={len(tickers)} removed={len(removed_list)}")
            rem_limit = _effective_limit(args, removed_list)
            if removed_list and rem_limit == len(removed_list):
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
                    f"ARGS date_from={args.date_from} date_to={args.date_to} mode={args.mode} market={args.market} "
                    f"sector={args.sector} industry={args.industry} limit={args.limit} sample={args.sample} seed={args.seed} "
                    f"signal_version={args.signal_version} require_row_on_date={args.require_row_on_date} "
                    f"min_history_rows={args.min_history_rows} max_days={args.max_days}"
                ),
            )
            if resolved_count:
                head, tail = _take_head_tail(tickers, _effective_limit(args, tickers))
                _dbg(args, f"TICKERS_SAMPLE_HEAD={head}")
                if tail:
                    _dbg(args, f"TICKERS_SAMPLE_TAIL={tail}")

        trading_days = build_trading_days(md_conn, tickers, args.date_from, args.date_to)
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
                    "Trading days are derived only from the selected tickers (no cross-market dates). "
                    "With signal_version=v2, each processed day also requires an as-of row; missing days emit DATA_INSUFFICIENT "
                    "and a MISSING_ASOF summary is printed after the run."
                )
        if args.require_row_on_date and args.signal_version == "v2":
            print(
                "NOTE: signal_version=v2 enforces an as-of row for every processed trading day; missing rows emit DATA_INSUFFICIENT."
            )
        print(
            f"RANGE date_from={args.date_from} date_to={args.date_to} "
            f"trading_days={len(trading_days)} tickers={len(tickers)}"
        )

        if args.dry_run:
            limit = _debug_limit(args)
            if len(trading_days) <= 10 or limit is None:
                print("DAYS:", ",".join(trading_days))
            else:
                head, tail = _take_head_tail(trading_days, 5)
                print("DAYS head:", ",".join(head))
                if tail:
                    print("...")
                    print("DAYS tail:", ",".join(tail))
            return

        policy_version = args.policy_version
        if policy_version == "dev":
            policy_version = "v2"
        if (args.signal_version == "v3") != (policy_version == "v3"):
            raise RuntimeError(
                "Incompatible versions: signal-version and policy-version must both be v3, or both non-v3."
            )
        policy_id = args.policy_id
        if policy_version == "v3" and policy_id == "rule_v2":
            policy_id = "rule_v3"
        provider_name = "osakedata_v3" if args.signal_version == "v3" else "osakedata_v2"
        app = build_swingmaster_app(
            rc_conn,
            policy_version=policy_version,
            enable_history=False,
            provider=provider_name,
            md_conn=md_conn,
            require_row_on_date=args.require_row_on_date,
            policy_id=policy_id,
            engine_version="dev",
            debug=args.debug,
        )
        signal_provider = app._signal_provider

        last_run_id = None
        run_ids_by_day: Dict[str, str] = {}
        for idx, day in enumerate(trading_days, start=1):
            _dbg(args, f"DAY_START {idx}/{len(trading_days)} date={day}")
            start = time.perf_counter()
            run_id = app.run_daily(as_of_date=day, tickers=tickers)
            elapsed_ms = (time.perf_counter() - start) * 1000
            last_run_id = run_id
            run_ids_by_day[day] = run_id
            if idx == 1 or idx == len(trading_days) or idx % 5 == 0:
                print(f"DAY {idx}/{len(trading_days)} {day} run_id={run_id} ms={elapsed_ms:.1f}")
            _dbg(args, f"DAY_END date={day} run_id={run_id} ms={elapsed_ms:.1f} rc_rows_pending=unknown")
            if _debug_enabled(args):
                rc_rows = rc_conn.execute(
                    "SELECT COUNT(*) FROM rc_state_daily WHERE date=? AND run_id=?",
                    (day, run_id),
                ).fetchone()[0]
                distinct_rc = rc_conn.execute(
                    "SELECT COUNT(DISTINCT ticker) FROM rc_state_daily WHERE date=? AND run_id=?",
                    (day, run_id),
                ).fetchone()[0]
                expected = len(tickers)
                ok_rows = rc_rows == expected
                ok_distinct = distinct_rc == expected
                _dbg(
                    args,
                    f"INVARIANTS expected_tickers={expected} rc_rows={rc_rows} rc_distinct={distinct_rc} "
                    f"ok_rows={ok_rows} ok_distinct={ok_distinct}",
                )
                if not (ok_rows and ok_distinct):
                    rc_tickers = rc_conn.execute(
                        "SELECT ticker FROM rc_state_daily WHERE date=? AND run_id=?",
                        (day, run_id),
                    ).fetchall()
                    rc_set = {r[0] for r in rc_tickers}
                    ticker_set = set(tickers)
                    missing = [t for t in tickers if t not in rc_set]
                    extra = [t for t in rc_set if t not in ticker_set]
                    miss_limit = _effective_limit(args, missing)
                    extra_limit = _effective_limit(args, extra)
                    _dbg(
                        args,
                        f"INVARIANT_FAIL missing_in_rc_count={len(missing)} extra_in_rc_count={len(extra)}",
                    )
                    if missing:
                        sample = missing if miss_limit == len(missing) else missing[:miss_limit]
                        _dbg(args, f"INVARIANT_FAIL missing_in_rc_sample={sample}")
                    if extra:
                        sample = extra if extra_limit == len(extra) else extra[:extra_limit]
                        _dbg(args, f"INVARIANT_FAIL extra_in_rc_sample={sample}")
            if args.print_signals:
                limit = args.print_signals_limit
                tickers_to_show = tickers if limit <= 0 or len(tickers) <= limit else tickers[:limit]
                print(f"PRINT_SIGNALS date={day}")
                for t in tickers_to_show:
                    signal_set = signal_provider.get_signals(t, day)
                    signals_dict = getattr(signal_set, "signals", None) if signal_set is not None else None
                    if signals_dict:
                        names = sorted(k.name for k in signals_dict.keys())
                    else:
                        names = []
                    print(f"SIGNALS ticker={t} signals={json.dumps(names)}")

        populate_rc_pipeline_episode(
            rc_conn=rc_conn,
            md_conn=md_conn,
            date_from=args.date_from,
            date_to=args.date_to,
            pipeline_version="rc_pipeline_episode_v1",
        )
        populate_rc_pipeline_episode_sma_extremes(rc_conn=rc_conn, md_db_path=args.md_db)
        populate_rc_pipeline_episode_peak_timing_fields(rc_conn=rc_conn, md_db_path=args.md_db)
        populate_rc_pipeline_episode_entry_confirmation(
            rc_conn=rc_conn, md_db_path=args.md_db
        )
        rc_conn.commit()
        if args.report:
            print_episode_report(rc_conn, args.rc_db, args.md_db)

        if last_run_id and trading_days:
            last_day = trading_days[-1]
            print(f"FINAL_DAY {last_day} run_id={last_run_id}")
            print_report(rc_conn, last_day, last_run_id)
            rc_rows_full = rc_conn.execute(
                "SELECT ticker, state, reasons_json FROM rc_state_daily WHERE date=? AND run_id=?",
                (last_day, last_run_id),
            ).fetchall()
            rc_by_ticker = {
                row["ticker"]: {
                    "state": row["state"],
                    "reasons": parse_reasons(row["reasons_json"]),
                }
                for row in rc_rows_full
            }
            if args.signal_version == "v2" and args.require_row_on_date:
                _dbg(
                    args,
                    "NOTE: trading_days derived from selected tickers; osakedata holds only trading days. "
                    "Missing-as-of means no row for ticker on a processed trading day.",
                )
                print(
                    "NOTE: osakedata contains only trading days; missing-as-of means a ticker has no osakedata row "
                    "on a processed trading day (often illiquidity or data gap)."
                )
                print(
                    "MISSING_ASOF_SUMMARY scope=run_range_universe "
                    f"signal_version=v2 require_row_on_date=True"
                )
                print(
                    f"MISSING_ASOF_SUMMARY days={len(trading_days)} "
                    f"tickers={len(tickers)} runs={len(run_ids_by_day)}"
                )
                print_missing_asof_summary(
                    rc_conn,
                    tickers,
                    run_ids_by_day,
                    trading_days,
                    last_day,
                    last_run_id,
                )
        signals_counter, focused, signals_by_ticker = collect_signal_stats(
            signal_provider, tickers, last_day
        )
        if _debug_enabled(args):
            entry_candidates = [t for t, s in signals_by_ticker.items() if SignalKey.ENTRY_SETUP_VALID in s.signals]
            stab_candidates = [t for t, s in signals_by_ticker.items() if SignalKey.STABILIZATION_CONFIRMED in s.signals]
            both_candidates = [t for t in entry_candidates if t in stab_candidates]

            def _debug_show(label, items):
                limit_val = _effective_limit(args, items)
                sample = items if limit_val == len(items) else items[:limit_val]
                _dbg(args, f"FINAL_DAY {label} count={len(items)} sample={sample}")

            _debug_show("ENTRY_CANDIDATES", entry_candidates)
            _debug_show("STAB_CANDIDATES", stab_candidates)
            _debug_show("BOTH_STAB_AND_ENTRY", both_candidates)
            entry_window_rc = sorted(t for t, v in rc_by_ticker.items() if v["state"] == "ENTRY_WINDOW")
            pass_rc = sorted(t for t, v in rc_by_ticker.items() if v["state"] == "PASS")
            _debug_show("ENTRY_WINDOW_TICKERS", entry_window_rc)
            _debug_show("PASS_TICKERS", pass_rc)
            if args.debug_show_mismatches:
                mismatches = []
                for t in both_candidates:
                    rc = rc_by_ticker.get(t)
                    rc_state = rc["state"] if rc else "MISSING"
                    reasons_list = rc["reasons"] if rc else []
                    if rc_state != "ENTRY_WINDOW":
                        blocker = infer_entry_blocker(rc_state, reasons_list)
                        reasons_json = json.dumps(reasons_list)
                        signal_keys_list = sorted(k.name for k in signals_by_ticker.get(t, {}).signals.keys()) if signals_by_ticker.get(t) else []
                        mismatches.append((t, rc_state, blocker, reasons_json, signal_keys_list))
                _dbg(args, f"FINAL_DAY MISMATCH entry_like_not_entry_window total={len(mismatches)}")
                limit_val = _effective_limit(args, mismatches)
                subset = mismatches if limit_val == len(mismatches) else mismatches[:limit_val]
                for t, state, blocker, reasons_json, signal_keys_list in subset:
                    _dbg(
                        args,
                        f"MISMATCH_FULL_CONTEXT ticker={t} blocker={blocker} rc_state={state} "
                        f"rc_reasons={reasons_json} signal_keys={json.dumps(signal_keys_list)}",
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
                    _dbg(args, f"FINAL_DAY TICKER {t} rc_state={state} reasons={reasons_json} signals={signal_names}")
                missing_rc = [t for t in tickers if not rc_conn.execute(
                    "SELECT 1 FROM rc_state_daily WHERE date=? AND run_id=? AND ticker=?",
                    (last_day, last_run_id, t),
                ).fetchone()]
                if missing_rc:
                    miss_limit = _effective_limit(args, missing_rc)
                    sample = missing_rc if miss_limit == len(missing_rc) else missing_rc[:miss_limit]
                    _dbg(args, f"FINAL_DAY TICKERS_NOT_IN_RC count={len(missing_rc)} sample={sample}")
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
