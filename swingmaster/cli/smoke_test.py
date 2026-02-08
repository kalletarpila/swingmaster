"""Lightweight CLI smoke test entry point for end-to-end sanity checks.

Purpose:
  - Execute a minimal run to validate wiring and DB connectivity.
Inputs:
  - CLI args for market/universe selection and DB paths.
Outputs:
  - Printed status/progress to stdout.
Example:
  - PYTHONPATH=. python3 swingmaster/cli/smoke_test.py --market OMXH --limit 5
"""

from __future__ import annotations

from pathlib import Path

from swingmaster.app_api.facade import SwingmasterApplication
from swingmaster.app_api.providers.sqlite_prev_state_provider import SQLitePrevStateProvider
from swingmaster.app_api.providers.sqlite_signal_provider_v0 import SQLiteSignalProviderV0
from swingmaster.core.policy.rule_policy_v2 import RuleBasedTransitionPolicyV2
from swingmaster.infra.sqlite.db import get_connection
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.infra.market_data.ohlcv_reader import OhlcvReader


def main() -> None:
    db_path = "swingmaster_smoke.db"
    db_file = Path(db_path)
    if db_file.exists():
        db_file.unlink()
    conn = get_connection(db_path)
    try:
        apply_migrations(conn)
        conn.commit()

        conn.execute("CREATE TABLE daily_ohlcv (ticker TEXT, date TEXT, close REAL);")
        start_date = "2025-12-29"
        end_date = "2026-01-22"
        # Insert 25 consecutive days with increasing close to satisfy SMA/prev checks.
        conn.executemany(
            "INSERT INTO daily_ohlcv (ticker, date, close) VALUES (?, ?, ?)",
            [
                ("AAA", "2025-12-29", 100.0),
                ("AAA", "2025-12-30", 101.0),
                ("AAA", "2025-12-31", 102.0),
                ("AAA", "2026-01-01", 103.0),
                ("AAA", "2026-01-02", 104.0),
                ("AAA", "2026-01-03", 105.0),
                ("AAA", "2026-01-04", 106.0),
                ("AAA", "2026-01-05", 107.0),
                ("AAA", "2026-01-06", 108.0),
                ("AAA", "2026-01-07", 109.0),
                ("AAA", "2026-01-08", 110.0),
                ("AAA", "2026-01-09", 111.0),
                ("AAA", "2026-01-10", 112.0),
                ("AAA", "2026-01-11", 113.0),
                ("AAA", "2026-01-12", 114.0),
                ("AAA", "2026-01-13", 115.0),
                ("AAA", "2026-01-14", 116.0),
                ("AAA", "2026-01-15", 117.0),
                ("AAA", "2026-01-16", 118.0),
                ("AAA", "2026-01-17", 119.0),
                ("AAA", "2026-01-18", 120.0),
                ("AAA", "2026-01-19", 121.0),
                ("AAA", "2026-01-20", 122.0),
                ("AAA", "2026-01-21", 123.0),
                ("AAA", "2026-01-22", 124.0),
            ],
        )
        conn.commit()

        reader = OhlcvReader(
            conn,
            table_name="daily_ohlcv",
            ticker_col="ticker",
            date_col="date",
            close_col="close",
        )
        for as_of_date in ["2026-01-21", "2026-01-22"]:
            pairs = reader.get_last_n_date_closes("AAA", as_of_date, n=21)
            print(f"AAA {as_of_date} last21:")
            for d, c in pairs:
                print(f"  {d} close={c}")
            latest = pairs[0][1]
            prev = pairs[1][1]
            sma20 = sum(c for _, c in pairs[0:20]) / 20.0
            print(f"  latest={latest} prev={prev} sma20={sma20}")

        policy = RuleBasedTransitionPolicyV2()
        app = SwingmasterApplication(
            conn=conn,
            policy=policy,
            signal_provider=SQLiteSignalProviderV0(conn, table_name="daily_ohlcv"),
            prev_state_provider=SQLitePrevStateProvider(conn),
            engine_version="dev",
            policy_id="rule_v2",
            policy_version="v2",
        )

        run_id_day1 = app.run_daily(as_of_date="2026-01-21", tickers=["AAA", "BBB", "CCC"])

        rc_run_count = conn.execute("SELECT COUNT(*) FROM rc_run").fetchone()[0]
        rc_state_count = conn.execute("SELECT COUNT(*) FROM rc_state_daily").fetchone()[0]
        rc_transition_count = conn.execute("SELECT COUNT(*) FROM rc_transition").fetchone()[0]

        print(f"OK day1 run_id={run_id_day1}")
        print(f"rc_run={rc_run_count} rc_state_daily={rc_state_count} rc_transition={rc_transition_count}")

        run_id_day2 = app.run_daily(as_of_date="2026-01-22", tickers=["AAA", "BBB", "CCC"])

        rc_run_count = conn.execute("SELECT COUNT(*) FROM rc_run").fetchone()[0]
        rc_state_count = conn.execute("SELECT COUNT(*) FROM rc_state_daily").fetchone()[0]
        rc_transition_count = conn.execute("SELECT COUNT(*) FROM rc_transition").fetchone()[0]

        print(f"OK day2 run_id={run_id_day2}")
        print(f"rc_run={rc_run_count} rc_state_daily={rc_state_count} rc_transition={rc_transition_count}")

        row = conn.execute(
            "SELECT state, age FROM rc_state_daily WHERE ticker=? AND date=?",
            ("AAA", "2026-01-22"),
        ).fetchone()
        if row:
            print(f"AAA 2026-01-22 state={row['state']} age={row['age']}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
