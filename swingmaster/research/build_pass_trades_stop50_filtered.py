from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


DEFAULT_RC_DB = "/home/kalle/projects/swingmaster/swingmaster_rc_usa_500.db"
DEFAULT_OSAKEDATA_DB = "/home/kalle/projects/rawcandle/data/osakedata.db"
DEFAULT_MARKET = "usa"
DEFAULT_OUT_CSV = "/tmp/usa_pass_trades_stop_or_50d_filtered.csv"


@dataclass(frozen=True)
class PassEvent:
    ticker: str
    entry_date: str


@dataclass(frozen=True)
class Episode:
    entry_window_date: str
    entry_window_exit_date: Optional[str]
    ew_confirm_confirmed: Optional[int]
    close_at_ew_start: Optional[float]
    close_at_ew_exit: Optional[float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build filtered PASS trades with STOP_OUT or TIME_50D exits"
    )
    parser.add_argument("--rc-db", default=DEFAULT_RC_DB)
    parser.add_argument("--osakedata-db", default=DEFAULT_OSAKEDATA_DB)
    parser.add_argument("--market", default=DEFAULT_MARKET)
    parser.add_argument("--out-csv", default=DEFAULT_OUT_CSV)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--pipeline-version", default=None)
    parser.add_argument("--date-from", default=None)
    parser.add_argument("--date-to", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--ew-level-column",
        default="ew_level_rolling",
        choices=["ew_level_rolling", "ew_level_fastpass"],
    )
    parser.add_argument("--require-ew-window-positive", action="store_true")
    parser.add_argument("--require-confirmed", action="store_true")
    parser.add_argument("--min-fastpass-score", type=float, default=None)
    return parser.parse_args()


def fetch_pass_events(conn: sqlite3.Connection, args: argparse.Namespace) -> List[PassEvent]:
    where_parts: List[str] = ["to_state = 'PASS'"]
    params: List[object] = []
    if args.run_id is not None:
        where_parts.append("run_id = ?")
        params.append(args.run_id)
    if args.date_from is not None:
        where_parts.append("date >= ?")
        params.append(args.date_from)
    if args.date_to is not None:
        where_parts.append("date <= ?")
        params.append(args.date_to)

    sql = f"""
        SELECT ticker, date
        FROM rc_transition
        WHERE {' AND '.join(where_parts)}
        ORDER BY date ASC, ticker ASC
    """
    if args.limit is not None:
        sql += "\nLIMIT ?"
        params.append(args.limit)

    rows = conn.execute(sql, params).fetchall()
    return [PassEvent(ticker=str(r[0]), entry_date=str(r[1])) for r in rows]


def load_episodes_for_ticker(
    conn: sqlite3.Connection,
    ticker: str,
    pipeline_version: Optional[str],
    cache: Dict[Tuple[str, Optional[str]], List[Episode]],
) -> List[Episode]:
    key = (ticker, pipeline_version)
    cached = cache.get(key)
    if cached is not None:
        return cached

    where_parts = ["ticker = ?"]
    params: List[object] = [ticker]
    if pipeline_version is not None:
        where_parts.append("pipeline_version = ?")
        params.append(pipeline_version)

    sql = f"""
        SELECT
            entry_window_date,
            entry_window_exit_date,
            ew_confirm_confirmed,
            close_at_ew_start,
            close_at_ew_exit
        FROM rc_pipeline_episode
        WHERE {' AND '.join(where_parts)}
          AND entry_window_date IS NOT NULL
        ORDER BY entry_window_date ASC
    """
    rows = conn.execute(sql, params).fetchall()
    episodes = [
        Episode(
            entry_window_date=str(r[0]),
            entry_window_exit_date=(None if r[1] is None else str(r[1])),
            ew_confirm_confirmed=(None if r[2] is None else int(r[2])),
            close_at_ew_start=(None if r[3] is None else float(r[3])),
            close_at_ew_exit=(None if r[4] is None else float(r[4])),
        )
        for r in rows
    ]
    cache[key] = episodes
    return episodes


def find_matching_episode(episodes: List[Episode], entry_date: str) -> Optional[Episode]:
    matched: Optional[Episode] = None
    for ep in episodes:
        if ep.entry_window_date <= entry_date:
            if ep.entry_window_exit_date is None or entry_date <= ep.entry_window_exit_date:
                matched = ep
            else:
                continue
        else:
            break
    return matched


def load_prices_for_ticker(
    conn: sqlite3.Connection,
    ticker: str,
    market: str,
    cache: Dict[Tuple[str, str], Tuple[List[str], List[Optional[float]], Dict[str, int]]],
) -> Tuple[List[str], List[Optional[float]], Dict[str, int]]:
    key = (ticker, market)
    cached = cache.get(key)
    if cached is not None:
        return cached

    rows = conn.execute(
        """
        SELECT pvm, close
        FROM osakedata
        WHERE market = ?
          AND osake = ?
        ORDER BY pvm ASC
        """,
        [market, ticker],
    ).fetchall()

    dates: List[str] = []
    closes: List[Optional[float]] = []
    date_to_rn: Dict[str, int] = {}
    for idx, row in enumerate(rows):
        d = str(row[0])
        c = None if row[1] is None else float(row[1])
        dates.append(d)
        closes.append(c)
        date_to_rn[d] = idx

    out = (dates, closes, date_to_rn)
    cache[key] = out
    return out


def load_ew_levels_for_ticker(
    conn: sqlite3.Connection,
    ticker: str,
    ew_level_column: str,
    cache: Dict[Tuple[str, str], Dict[str, Optional[int]]],
) -> Dict[str, Optional[int]]:
    key = (ticker, ew_level_column)
    cached = cache.get(key)
    if cached is not None:
        return cached

    rows = conn.execute(
        f"""
        SELECT date, {ew_level_column}
        FROM rc_ew_score_daily
        WHERE ticker = ?
        """,
        [ticker],
    ).fetchall()

    levels: Dict[str, Optional[int]] = {}
    for row in rows:
        levels[str(row[0])] = None if row[1] is None else int(row[1])
    cache[key] = levels
    return levels


def load_ew_fastpass_scores_for_ticker(
    conn: sqlite3.Connection,
    ticker: str,
    cache: Dict[str, Dict[str, Optional[float]]],
) -> Dict[str, Optional[float]]:
    cached = cache.get(ticker)
    if cached is not None:
        return cached

    rows = conn.execute(
        """
        SELECT date, ew_score_fastpass
        FROM rc_ew_score_daily
        WHERE ticker = ?
        """,
        [ticker],
    ).fetchall()

    scores: Dict[str, Optional[float]] = {}
    for row in rows:
        scores[str(row[0])] = None if row[1] is None else float(row[1])
    cache[ticker] = scores
    return scores


def main() -> None:
    args = parse_args()

    rc_conn = sqlite3.connect(args.rc_db)
    od_conn = sqlite3.connect(args.osakedata_db)
    try:
        pass_events = fetch_pass_events(rc_conn, args)

        trades_pass_total = len(pass_events)
        trades_with_episode = 0
        trades_pass_within_4td = 0
        trades_prevday_ew_ok = 0
        trades_final_written = 0

        dropped_no_episode = 0
        dropped_entry_missing_close = 0
        dropped_pass_gt_4td = 0
        dropped_prevday_missing = 0
        dropped_prevday_ew_not_ok = 0
        trades_r_ew_window_pos = 0
        dropped_ew_window_not_pos = 0
        dropped_not_confirmed = 0
        dropped_min_fastpass_score = 0
        dropped_exit_oob = 0

        exit_stop_out = 0
        exit_time_50d = 0

        episode_cache: Dict[Tuple[str, Optional[str]], List[Episode]] = {}
        price_cache: Dict[Tuple[str, str], Tuple[List[str], List[Optional[float]], Dict[str, int]]] = {}
        ew_cache: Dict[Tuple[str, str], Dict[str, Optional[int]]] = {}
        ew_score_cache: Dict[str, Dict[str, Optional[float]]] = {}

        out_rows: List[Dict[str, object]] = []

        for ev in pass_events:
            episodes = load_episodes_for_ticker(
                rc_conn, ev.ticker, args.pipeline_version, episode_cache
            )
            ep = find_matching_episode(episodes, ev.entry_date)
            if ep is None:
                dropped_no_episode += 1
                continue
            trades_with_episode += 1

            dates, closes, date_to_rn = load_prices_for_ticker(
                od_conn, ev.ticker, args.market, price_cache
            )
            entry_rn = date_to_rn.get(ev.entry_date)
            ew_start_rn = date_to_rn.get(ep.entry_window_date)
            if entry_rn is None or ew_start_rn is None:
                dropped_entry_missing_close += 1
                continue

            buy_close = closes[entry_rn]
            if buy_close is None:
                dropped_entry_missing_close += 1
                continue

            if (entry_rn - ew_start_rn) > 3:
                dropped_pass_gt_4td += 1
                continue
            trades_pass_within_4td += 1

            if entry_rn <= 0:
                dropped_prevday_missing += 1
                continue
            prev_date = dates[entry_rn - 1]
            ew_levels = load_ew_levels_for_ticker(
                rc_conn, ev.ticker, args.ew_level_column, ew_cache
            )
            prev_level = ew_levels.get(prev_date)
            if prev_level != 1:
                dropped_prevday_ew_not_ok += 1
                continue
            trades_prevday_ew_ok += 1

            if args.min_fastpass_score is not None:
                ew_scores = load_ew_fastpass_scores_for_ticker(
                    rc_conn, ev.ticker, ew_score_cache
                )
                prev_score = ew_scores.get(prev_date)
                if prev_score is None or prev_score < args.min_fastpass_score:
                    dropped_min_fastpass_score += 1
                    continue

            r_ew_window: Optional[float] = None
            if (
                ep.close_at_ew_start is not None
                and ep.close_at_ew_exit is not None
                and ep.close_at_ew_start != 0
            ):
                r_ew_window = (ep.close_at_ew_exit / ep.close_at_ew_start) - 1.0
            if r_ew_window is not None and r_ew_window > 0:
                trades_r_ew_window_pos += 1
            if args.require_ew_window_positive and not (
                r_ew_window is not None and r_ew_window > 0
            ):
                dropped_ew_window_not_pos += 1
                continue

            is_confirmed = ep.ew_confirm_confirmed == 1
            if args.require_confirmed and not is_confirmed:
                dropped_not_confirmed += 1
                continue

            time_exit_rn = entry_rn + 49
            if time_exit_rn >= len(dates):
                dropped_exit_oob += 1
                continue

            stop_rn: Optional[int] = None
            scan_end = min(entry_rn + 49, len(dates) - 1)
            for rn in range(entry_rn + 1, scan_end + 1):
                c = closes[rn]
                if c is not None and c < buy_close:
                    stop_rn = rn
                    break

            if stop_rn is not None:
                exit_rn = stop_rn
                exit_reason = "STOP_OUT"
                exit_stop_out += 1
            else:
                exit_rn = time_exit_rn
                exit_reason = "TIME_50D"
                exit_time_50d += 1

            sell_close = closes[exit_rn]
            if sell_close is None:
                dropped_exit_oob += 1
                if exit_reason == "STOP_OUT":
                    exit_stop_out -= 1
                else:
                    exit_time_50d -= 1
                continue

            out_rows.append(
                {
                    "ticker": ev.ticker,
                    "entry_date": ev.entry_date,
                    "exit_date": dates[exit_rn],
                    "exit_reason": exit_reason,
                    "buy_close": buy_close,
                    "sell_close": sell_close,
                    "holding_days_trading": (exit_rn - entry_rn + 1),
                    "r_trade": (sell_close / buy_close) - 1.0,
                }
            )

        out_rows.sort(key=lambda r: (str(r["entry_date"]), str(r["ticker"])))
        trades_final_written = len(out_rows)

        with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "ticker",
                    "entry_date",
                    "exit_date",
                    "exit_reason",
                    "buy_close",
                    "sell_close",
                    "holding_days_trading",
                    "r_trade",
                ],
            )
            writer.writeheader()
            for row in out_rows:
                writer.writerow(row)

        print(f"SUMMARY trades_pass_total={trades_pass_total}")
        print(f"SUMMARY trades_with_episode={trades_with_episode}")
        print(f"SUMMARY trades_pass_within_4td={trades_pass_within_4td}")
        print(f"SUMMARY trades_prevday_ew_ok={trades_prevday_ew_ok}")
        print(f"SUMMARY trades_final_written={trades_final_written}")
        print(f"SUMMARY dropped_no_episode={dropped_no_episode}")
        print(f"SUMMARY dropped_entry_missing_close={dropped_entry_missing_close}")
        print(f"SUMMARY dropped_pass_gt_4td={dropped_pass_gt_4td}")
        print(f"SUMMARY dropped_prevday_missing={dropped_prevday_missing}")
        print(f"SUMMARY dropped_prevday_ew_not_ok={dropped_prevday_ew_not_ok}")
        print(f"SUMMARY dropped_exit_oob={dropped_exit_oob}")
        print(f"SUMMARY exit_STOP_OUT={exit_stop_out}")
        print(f"SUMMARY exit_TIME_50D={exit_time_50d}")
        print(
            f"SUMMARY require_ew_window_positive={1 if args.require_ew_window_positive else 0}"
        )
        print(f"SUMMARY require_confirmed={1 if args.require_confirmed else 0}")
        print(f"SUMMARY trades_r_ew_window_pos={trades_r_ew_window_pos}")
        print(f"SUMMARY dropped_ew_window_not_pos={dropped_ew_window_not_pos}")
        print(f"SUMMARY dropped_not_confirmed={dropped_not_confirmed}")
        if args.min_fastpass_score is not None:
            print(f"SUMMARY min_fastpass_score={args.min_fastpass_score}")
            print(f"SUMMARY dropped_min_fastpass_score={dropped_min_fastpass_score}")
    finally:
        rc_conn.close()
        od_conn.close()


if __name__ == "__main__":
    main()
