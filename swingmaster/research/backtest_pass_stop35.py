from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import timedelta
import math

import pandas as pd


DEFAULT_TRADES_CSV = "/tmp/usa_pass_trades_stop_or_35d.csv"
DEFAULT_OSAKEDATA_DB = "/home/kalle/projects/rawcandle/data/osakedata.db"
DEFAULT_MARKET = "usa"
DEFAULT_OUT_CSV = "/tmp/usa_pass_portfolio_daily.csv"


@dataclass(frozen=True)
class Trade:
    ticker: str
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute PASS->STOP/TIME_35D daily equal-weight portfolio returns"
    )
    parser.add_argument("--trades-csv", default=DEFAULT_TRADES_CSV)
    parser.add_argument("--osakedata-db", default=DEFAULT_OSAKEDATA_DB)
    parser.add_argument("--market", default=DEFAULT_MARKET)
    parser.add_argument("--out-csv", default=DEFAULT_OUT_CSV)
    parser.add_argument("--include-equity", action="store_true")
    parser.add_argument("--metrics", action="store_true")
    parser.add_argument("--trade-metrics", action="store_true")
    parser.add_argument("--debug-date", default=None)
    parser.add_argument("--benchmark", default=None)
    parser.add_argument("--from", dest="report_from", default=None)
    parser.add_argument("--to", dest="report_to", default=None)
    parser.add_argument("--rolling-window", type=int, default=None)
    parser.add_argument("--exposure-turnover", action="store_true")
    parser.add_argument("--cost-bps", type=float, default=0.0)
    parser.add_argument("--net-metrics", action="store_true")
    parser.add_argument("--regime-filter-sma200", action="store_true")
    parser.add_argument("--regime-symbol", default="SPY")
    parser.add_argument("--max-open-positions", type=int, default=0)
    parser.add_argument("--rank-by-fastpass-score", action="store_true")
    parser.add_argument("--rc-db", default=None)
    return parser.parse_args()


def _read_and_deoverlap_trades(path: str) -> tuple[int, list[Trade], pd.DataFrame]:
    df = pd.read_csv(path)
    trades_total = int(len(df))
    df["entry_date"] = pd.to_datetime(df["entry_date"]).dt.normalize()
    df["exit_date"] = pd.to_datetime(df["exit_date"]).dt.normalize()
    df = df.sort_values(["ticker", "entry_date", "exit_date"], kind="mergesort")

    kept: list[Trade] = []
    kept_rows: list[dict[str, object]] = []
    last_exit_by_ticker: dict[str, pd.Timestamp] = {}
    for row in df.itertuples(index=False):
        ticker = str(row.ticker)
        entry_date = row.entry_date
        exit_date = row.exit_date
        last_exit = last_exit_by_ticker.get(ticker)
        if last_exit is None or entry_date > last_exit:
            kept.append(Trade(ticker=ticker, entry_date=entry_date, exit_date=exit_date))
            last_exit_by_ticker[ticker] = exit_date
            kept_rows.append(
                {
                    "ticker": ticker,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "exit_reason": getattr(row, "exit_reason", None),
                    "r_trade": getattr(row, "r_trade", None),
                }
            )
    return trades_total, kept, pd.DataFrame(kept_rows)


def _load_prices(
    osakedata_db: str,
    market: str,
    tickers: list[str],
    min_entry_date: pd.Timestamp,
    max_exit_date: pd.Timestamp,
) -> pd.DataFrame:
    start_date = (min_entry_date - timedelta(days=10)).date().isoformat()
    end_date = max_exit_date.date().isoformat()
    placeholders = ",".join("?" for _ in tickers)
    sql = f"""
        SELECT osake, pvm, close
        FROM osakedata
        WHERE market = ?
          AND osake IN ({placeholders})
          AND pvm >= ?
          AND pvm <= ?
        ORDER BY osake, pvm
    """
    params = [market, *tickers, start_date, end_date]
    conn = sqlite3.connect(osakedata_db)
    try:
        prices = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    prices["pvm"] = pd.to_datetime(prices["pvm"]).dt.normalize()
    prices = prices.sort_values(["osake", "pvm"], kind="mergesort")
    return prices


def _build_portfolio_series(
    trades: list[Trade],
    prices: pd.DataFrame,
    include_equity: bool,
    debug_date: pd.Timestamp | None = None,
    max_open_positions: int = 0,
    rank_by_fastpass_score: bool = False,
    fastpass_score_by_key: dict[tuple[str, str], float] | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    prices = prices.copy()
    prices["r"] = prices.groupby("osake", sort=False)["close"].pct_change()

    calendar = sorted(prices["pvm"].drop_duplicates().tolist())
    calendar_index = {d: i for i, d in enumerate(calendar)}
    returns_pivot = prices.pivot(index="pvm", columns="osake", values="r")

    remove_events: dict[pd.Timestamp, set[str]] = {}
    entry_candidates_by_date: dict[pd.Timestamp, list[Trade]] = {}
    for trade in trades:
        entry_candidates_by_date.setdefault(trade.entry_date, []).append(trade)

    open_set: set[str] = set()
    rows: list[dict[str, float | int | str]] = []
    equity = 1.0
    debug_snapshot: dict[str, object] | None = None
    entries_count_by_date: dict[pd.Timestamp, int] = {}
    exits_count_by_date: dict[pd.Timestamp, int] = {}
    entry_candidates_count_by_date: dict[pd.Timestamp, int] = {}
    dropped_entries_count_by_date: dict[pd.Timestamp, int] = {}
    cap_ranking_by_date: dict[pd.Timestamp, list[tuple[str, float | None, bool]]] = {}

    for d in calendar:
        remove_today = sorted(remove_events.get(d, set()))
        exits_count_by_date[d] = len(remove_today)

        for ticker in remove_today:
            open_set.discard(ticker)

        candidates_today = entry_candidates_by_date.get(d, [])
        entry_candidates_count_by_date[d] = len(candidates_today)

        ranked_candidates: list[tuple[Trade, float | None]] = []
        if rank_by_fastpass_score:
            for tr in candidates_today:
                score_date = (tr.entry_date - timedelta(days=1)).date().isoformat()
                score = None
                if fastpass_score_by_key is not None:
                    score = fastpass_score_by_key.get((tr.ticker, score_date))
                ranked_candidates.append((tr, score))
            ranked_candidates.sort(
                key=lambda x: (
                    -x[1] if x[1] is not None else float("inf"),
                    x[0].ticker,
                )
            )
        else:
            ranked_candidates = sorted(
                [(tr, None) for tr in candidates_today],
                key=lambda x: x[0].ticker,
            )

        accepted_today: list[tuple[Trade, float | None]] = []
        dropped_today: list[tuple[Trade, float | None]] = []
        if max_open_positions > 0:
            available_slots = max_open_positions - len(open_set)
            if available_slots <= 0:
                dropped_today = ranked_candidates
            else:
                accepted_today = ranked_candidates[:available_slots]
                dropped_today = ranked_candidates[available_slots:]
        else:
            accepted_today = ranked_candidates

        for tr, _ in accepted_today:
            open_set.add(tr.ticker)
            exit_idx = calendar_index.get(tr.exit_date)
            if exit_idx is not None and (exit_idx + 1) < len(calendar):
                remove_date = calendar[exit_idx + 1]
                remove_events.setdefault(remove_date, set()).add(tr.ticker)

        entries_count_by_date[d] = len(accepted_today)
        dropped_entries_count_by_date[d] = len(dropped_today)
        cap_ranking_by_date[d] = (
            [(tr.ticker, score, True) for tr, score in accepted_today]
            + [(tr.ticker, score, False) for tr, score in dropped_today]
        )

        n_open = len(open_set)
        if n_open == 0:
            portfolio_r = 0.0
        else:
            tickers = sorted(open_set)
            tickers_present = [t for t in tickers if t in returns_pivot.columns]
            if not tickers_present:
                portfolio_r = 0.0
            else:
                day_vals = returns_pivot.loc[d, tickers_present]
                mean_r = day_vals.mean(skipna=True)
                portfolio_r = 0.0 if pd.isna(mean_r) else float(mean_r)

        if debug_date is not None and d == debug_date:
            open_tickers = sorted(open_set)
            debug_positions: list[dict[str, object]] = []
            mean_used = 0
            if open_tickers:
                weight = 1.0 / len(open_tickers)
                for ticker in open_tickers:
                    if ticker in returns_pivot.columns:
                        r_val = returns_pivot.loc[d, ticker]
                    else:
                        r_val = float("nan")
                    if pd.isna(r_val):
                        debug_positions.append(
                            {
                                "ticker": ticker,
                                "r": None,
                                "weight": weight,
                                "contrib": 0.0,
                            }
                        )
                    else:
                        rv = float(r_val)
                        debug_positions.append(
                            {
                                "ticker": ticker,
                                "r": rv,
                                "weight": weight,
                                "contrib": weight * rv,
                            }
                        )
                        mean_used += 1
            else:
                debug_positions = []
            debug_snapshot = {
                "date": d,
                "n_open": len(open_tickers) if open_tickers else 0,
                "portfolio_r": portfolio_r,
                "positions": debug_positions,
                "mean_used": mean_used,
                "entries_candidates": int(entry_candidates_count_by_date.get(d, 0)),
                "entries_accepted": int(entries_count_by_date.get(d, 0)),
                "entries_dropped": int(dropped_entries_count_by_date.get(d, 0)),
                "cap_ranked_rows": cap_ranking_by_date.get(d, []),
            }

        row: dict[str, float | int | str] = {
            "date": d.date().isoformat(),
            "n_open": n_open,
            "portfolio_r": portfolio_r,
        }
        if include_equity:
            equity *= (1.0 + portfolio_r)
            row["equity"] = equity
        rows.append(row)

    return pd.DataFrame(rows), {
        "calendar": calendar,
        "snapshot": debug_snapshot,
        "entries_count_by_date": entries_count_by_date,
        "exits_count_by_date": exits_count_by_date,
        "entry_candidates_count_by_date": entry_candidates_count_by_date,
        "dropped_entries_count_by_date": dropped_entries_count_by_date,
    }


def _load_fastpass_scores(
    rc_db: str,
) -> dict[tuple[str, str], float]:
    conn = sqlite3.connect(rc_db)
    try:
        rows = conn.execute(
            """
            SELECT ticker, date, ew_score_fastpass
            FROM rc_ew_score_daily
            """
        ).fetchall()
    finally:
        conn.close()

    out: dict[tuple[str, str], float] = {}
    for ticker, date_s, score in rows:
        if ticker is None or date_s is None or score is None:
            continue
        out[(str(ticker), str(date_s))] = float(score)
    return out


def _compute_metrics_from_daily(out_df: pd.DataFrame) -> dict[str, float]:
    if out_df.empty:
        return {
            "cagr": 0.0,
            "ann_vol": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "final_equity": 1.0,
        }

    daily_r = out_df["portfolio_r"].astype(float)
    equity = (1.0 + daily_r).cumprod()

    n_days = float(len(out_df))
    years = n_days / 252.0
    final_equity = float(equity.iloc[-1])
    cagr = 0.0 if years <= 0.0 else (final_equity ** (1.0 / years) - 1.0)

    std_r = float(daily_r.std(ddof=0))
    mean_r = float(daily_r.mean())
    ann_vol = std_r * math.sqrt(252.0)
    sharpe = 0.0 if std_r == 0.0 else (mean_r / std_r) * math.sqrt(252.0)

    running_peak = equity.cummax()
    drawdown = (equity / running_peak) - 1.0
    max_drawdown = float(drawdown.min())

    return {
        "cagr": cagr,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "final_equity": final_equity,
    }


def _compute_trade_metrics(trades_total: int, kept_df: pd.DataFrame) -> dict[str, float | int]:
    kept_n = int(len(kept_df))
    dropped = trades_total - kept_n
    if kept_n == 0:
        return {
            "trades_total": trades_total,
            "trades_after_no_overlap": kept_n,
            "trades_dropped_overlap": dropped,
            "win_rate": 0.0,
            "avg_trade_return": 0.0,
            "median_trade_return": 0.0,
            "n_exit_STOP_OUT": 0,
            "n_exit_TIME_35D": 0,
            "n_exit_OTHER": 0,
        }

    r_trade = pd.to_numeric(kept_df["r_trade"], errors="coerce")
    win_rate = float((r_trade > 0).mean())
    avg_trade_return = 0.0 if r_trade.empty else float(r_trade.mean())
    median_trade_return = 0.0 if r_trade.empty else float(r_trade.median())
    exit_reason = kept_df["exit_reason"].astype(str)
    n_stop = int((exit_reason == "STOP_OUT").sum())
    n_time = int((exit_reason == "TIME_35D").sum())
    n_other = kept_n - n_stop - n_time

    return {
        "trades_total": trades_total,
        "trades_after_no_overlap": kept_n,
        "trades_dropped_overlap": dropped,
        "win_rate": win_rate,
        "avg_trade_return": avg_trade_return,
        "median_trade_return": median_trade_return,
        "n_exit_STOP_OUT": n_stop,
        "n_exit_TIME_35D": n_time,
        "n_exit_OTHER": n_other,
    }


def _compute_net_metrics(report_df: pd.DataFrame) -> dict[str, float]:
    if report_df.empty:
        return {
            "cagr": 0.0,
            "ann_vol": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "final_equity": 1.0,
            "total_cost": 0.0,
        }

    daily_net = report_df["portfolio_r_net"].astype(float)
    equity_net = (1.0 + daily_net).cumprod()
    n_days = float(len(daily_net))
    years = n_days / 252.0
    final_equity = float(equity_net.iloc[-1])
    cagr = 0.0 if years <= 0.0 else (final_equity ** (1.0 / years) - 1.0)

    std_r = float(daily_net.std(ddof=0))
    mean_r = float(daily_net.mean())
    ann_vol = std_r * math.sqrt(252.0)
    sharpe = 0.0 if std_r == 0.0 else (mean_r / std_r) * math.sqrt(252.0)

    running_peak = equity_net.cummax()
    drawdown = (equity_net / running_peak) - 1.0
    max_drawdown = float(drawdown.min())
    total_cost = float(report_df["cost_daily"].sum())

    return {
        "cagr": cagr,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
        "final_equity": final_equity,
        "total_cost": total_cost,
    }


def _compute_benchmark_and_relative_metrics(
    osakedata_db: str,
    market: str,
    symbol: str,
    calendar: list[pd.Timestamp],
    strategy_daily: pd.Series,
    strategy_cagr: float,
) -> tuple[dict[str, float], dict[str, float], pd.Series]:
    if not calendar:
        bench_zero = {
            "cagr": 0.0,
            "ann_vol": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "final_equity": 0.0,
        }
        rel_zero = {"excess_cagr": 0.0, "alpha_daily_mean": 0.0, "information_ratio": 0.0}
        return bench_zero, rel_zero, pd.Series(dtype=float)

    max_date = max(calendar).date().isoformat()
    conn = sqlite3.connect(osakedata_db)
    try:
        bdf = pd.read_sql_query(
            """
            SELECT pvm, close
            FROM osakedata
            WHERE market = ?
              AND osake = ?
              AND pvm <= ?
            ORDER BY pvm
            """,
            conn,
            params=[market, symbol, max_date],
        )
    finally:
        conn.close()

    if bdf.empty:
        bench_zero = {
            "cagr": 0.0,
            "ann_vol": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "final_equity": 0.0,
        }
        rel_zero = {"excess_cagr": 0.0, "alpha_daily_mean": 0.0, "information_ratio": 0.0}
        bench_r_global = pd.Series(index=pd.Index(calendar), dtype=float)
        bench_r_global[:] = float("nan")
        return bench_zero, rel_zero, bench_r_global

    bdf["pvm"] = pd.to_datetime(bdf["pvm"]).dt.normalize()
    bdf["r"] = bdf["close"].astype(float) / bdf["close"].astype(float).shift(1) - 1.0
    bench_r_by_date = bdf.set_index("pvm")["r"]
    bench_r_global = pd.Series(index=pd.Index(calendar), dtype=float)
    for d in calendar:
        if d in bench_r_by_date.index:
            bench_r_global.loc[d] = bench_r_by_date.loc[d]
        else:
            bench_r_global.loc[d] = float("nan")

    bench_valid = bench_r_global.dropna().astype(float)
    if len(bench_valid) < 2:
        bench_metrics = {
            "cagr": 0.0,
            "ann_vol": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "final_equity": 0.0,
        }
        rel_metrics = {"excess_cagr": 0.0, "alpha_daily_mean": 0.0, "information_ratio": 0.0}
        return bench_metrics, rel_metrics, bench_r_global

    bench_equity = (1.0 + bench_valid).cumprod()
    n_valid = float(len(bench_valid))
    years = n_valid / 252.0
    bench_final_equity = float(bench_equity.iloc[-1])
    bench_cagr = 0.0 if years <= 0.0 else (bench_final_equity ** (1.0 / years) - 1.0)
    bench_std = float(bench_valid.std(ddof=0))
    bench_mean = float(bench_valid.mean())
    bench_ann_vol = bench_std * math.sqrt(252.0)
    bench_sharpe = 0.0 if bench_std == 0.0 else (bench_mean / bench_std) * math.sqrt(252.0)
    bench_peak = bench_equity.cummax()
    bench_drawdown = (bench_equity / bench_peak) - 1.0
    bench_max_dd = float(bench_drawdown.min())
    bench_metrics = {
        "cagr": bench_cagr,
        "ann_vol": bench_ann_vol,
        "sharpe": bench_sharpe,
        "max_drawdown": bench_max_dd,
        "final_equity": bench_final_equity,
    }

    strategy_aligned = strategy_daily.copy()
    strategy_aligned.index = pd.Index(calendar)
    joint = pd.concat([strategy_aligned.rename("s"), bench_r_global.rename("b")], axis=1)
    joint = joint.dropna(subset=["b"])
    if joint.empty:
        rel_metrics = {"excess_cagr": 0.0, "alpha_daily_mean": 0.0, "information_ratio": 0.0}
    else:
        excess = (joint["s"] - joint["b"]).astype(float)
        ex_mean = float(excess.mean())
        ex_std = float(excess.std(ddof=0))
        ir = 0.0 if ex_std == 0.0 else (ex_mean / ex_std) * math.sqrt(252.0)
        rel_metrics = {
            "excess_cagr": strategy_cagr - bench_cagr,
            "alpha_daily_mean": ex_mean,
            "information_ratio": ir,
        }
    return bench_metrics, rel_metrics, bench_r_global


def _load_regime_on_by_date(
    osakedata_db: str,
    market: str,
    symbol: str,
    calendar: list[pd.Timestamp],
) -> dict[pd.Timestamp, bool]:
    regime_on_by_date: dict[pd.Timestamp, bool] = {}
    if not calendar:
        return regime_on_by_date

    max_date = max(calendar).date().isoformat()
    conn = sqlite3.connect(osakedata_db)
    try:
        rdf = pd.read_sql_query(
            """
            SELECT pvm, close
            FROM osakedata
            WHERE market = ?
              AND osake = ?
              AND pvm <= ?
            ORDER BY pvm
            """,
            conn,
            params=[market, symbol, max_date],
        )
    finally:
        conn.close()

    if rdf.empty:
        for d in calendar:
            regime_on_by_date[d] = False
        return regime_on_by_date

    rdf["pvm"] = pd.to_datetime(rdf["pvm"]).dt.normalize()
    rdf["close"] = rdf["close"].astype(float)
    rdf["sma200"] = rdf["close"].rolling(window=200, min_periods=200).mean()
    close_by_date = rdf.set_index("pvm")["close"]
    sma_by_date = rdf.set_index("pvm")["sma200"]

    for d in calendar:
        close_val = close_by_date.get(d, float("nan"))
        sma_val = sma_by_date.get(d, float("nan"))
        if pd.isna(close_val) or pd.isna(sma_val):
            regime_on_by_date[d] = False
        else:
            regime_on_by_date[d] = bool(float(close_val) >= float(sma_val))
    return regime_on_by_date


def main() -> None:
    args = parse_args()
    cap_enabled = int(args.max_open_positions) > 0

    if args.rank_by_fastpass_score and not args.rc_db:
        print("SUMMARY status=ERROR message=RC_DB_REQUIRED_FOR_RANKING")
        raise SystemExit(2)

    fastpass_score_by_key: dict[tuple[str, str], float] | None = None
    if args.rank_by_fastpass_score:
        try:
            fastpass_score_by_key = _load_fastpass_scores(args.rc_db)
        except sqlite3.Error:
            print("SUMMARY status=ERROR message=RC_EW_SCORE_DAILY_UNAVAILABLE")
            raise SystemExit(2)

    debug_date = (
        pd.to_datetime(args.debug_date).normalize() if args.debug_date is not None else None
    )

    trades_total, trades, kept_trade_df = _read_and_deoverlap_trades(args.trades_csv)
    if not trades:
        out_cols = ["date", "n_open", "portfolio_r"]
        if args.include_equity:
            out_cols.append("equity")
        pd.DataFrame(columns=out_cols).to_csv(args.out_csv, index=False)
        print("date_min=NA")
        print("date_max=NA")
        print("n_days_total=0")
        print("n_days_open=0")
        print("max_n_open=0")
        if args.metrics:
            print("METRICS cagr=0.0")
            print("METRICS ann_vol=0.0")
            print("METRICS sharpe=0.0")
            print("METRICS max_drawdown=0.0")
            print("METRICS final_equity=1.0")
        if args.trade_metrics:
            tmetrics = _compute_trade_metrics(trades_total, kept_trade_df)
            print(f"TRADE_METRICS trades_total={tmetrics['trades_total']}")
            print(
                f"TRADE_METRICS trades_after_no_overlap={tmetrics['trades_after_no_overlap']}"
            )
            print(
                f"TRADE_METRICS trades_dropped_overlap={tmetrics['trades_dropped_overlap']}"
            )
            print(f"TRADE_METRICS win_rate={tmetrics['win_rate']}")
            print(f"TRADE_METRICS avg_trade_return={tmetrics['avg_trade_return']}")
            print(f"TRADE_METRICS median_trade_return={tmetrics['median_trade_return']}")
            print(f"TRADE_METRICS n_exit_STOP_OUT={tmetrics['n_exit_STOP_OUT']}")
            print(f"TRADE_METRICS n_exit_TIME_35D={tmetrics['n_exit_TIME_35D']}")
            print(f"TRADE_METRICS n_exit_OTHER={tmetrics['n_exit_OTHER']}")
        return

    tickers = sorted({t.ticker for t in trades})
    min_entry_date = min(t.entry_date for t in trades)
    max_exit_date = max(t.exit_date for t in trades)
    prices = _load_prices(
        osakedata_db=args.osakedata_db,
        market=args.market,
        tickers=tickers,
        min_entry_date=min_entry_date,
        max_exit_date=max_exit_date,
    )

    out_df, debug_ctx = _build_portfolio_series(
        trades=trades,
        prices=prices,
        include_equity=args.include_equity,
        debug_date=debug_date,
        max_open_positions=int(args.max_open_positions),
        rank_by_fastpass_score=bool(args.rank_by_fastpass_score),
        fastpass_score_by_key=fastpass_score_by_key,
    )
    out_df = out_df.sort_values("date", kind="mergesort")
    out_df.to_csv(args.out_csv, index=False)

    date_min = out_df["date"].min() if not out_df.empty else "NA"
    date_max = out_df["date"].max() if not out_df.empty else "NA"
    n_days_total = int(len(out_df))
    n_days_open = int((out_df["n_open"] > 0).sum()) if not out_df.empty else 0
    max_n_open = int(out_df["n_open"].max()) if not out_df.empty else 0

    print(f"date_min={date_min}")
    print(f"date_max={date_max}")
    print(f"n_days_total={n_days_total}")
    print(f"n_days_open={n_days_open}")
    print(f"max_n_open={max_n_open}")

    report_df = out_df.copy()
    report_df["date_dt"] = pd.to_datetime(report_df["date"]).dt.normalize()
    report_from = (
        pd.to_datetime(args.report_from).normalize() if args.report_from is not None else None
    )
    report_to = pd.to_datetime(args.report_to).normalize() if args.report_to is not None else None

    if report_from is not None or report_to is not None:
        if report_from is not None:
            report_df = report_df[report_df["date_dt"] >= report_from]
        if report_to is not None:
            report_df = report_df[report_df["date_dt"] <= report_to]
        report_df = report_df.sort_values("date", kind="mergesort")
        print(f"REPORT_WINDOW from={args.report_from if args.report_from is not None else 'NONE'}")
        print(f"REPORT_WINDOW to={args.report_to if args.report_to is not None else 'NONE'}")
        print(f"REPORT_WINDOW n_days={len(report_df)}")
        if report_df.empty:
            print("REPORT_WINDOW status=EMPTY")

    if cap_enabled:
        dropped_map = debug_ctx["dropped_entries_count_by_date"]
        if report_df.empty:
            dropped_total = 0
            dropped_days = 0
            dropped_max_day = 0
        else:
            report_days = list(report_df["date_dt"])
            dropped_vals = [int(dropped_map.get(d, 0)) for d in report_days]
            dropped_total = int(sum(dropped_vals))
            dropped_days = int(sum(1 for v in dropped_vals if v > 0))
            dropped_max_day = int(max(dropped_vals)) if dropped_vals else 0
        print(
            "PORTFOLIO_CAP enabled=1 "
            f"max_open_positions={int(args.max_open_positions)} "
            f"rank_by_fastpass_score={1 if args.rank_by_fastpass_score else 0}"
        )
        print(f"PORTFOLIO_CAP dropped_entries_total={dropped_total}")
        print(f"PORTFOLIO_CAP dropped_entries_days={dropped_days}")
        print(f"PORTFOLIO_CAP max_entries_dropped_in_day={dropped_max_day}")

    regime_on_by_date: dict[pd.Timestamp, bool] = {}
    if args.regime_filter_sma200:
        regime_on_by_date = _load_regime_on_by_date(
            osakedata_db=args.osakedata_db,
            market=args.market,
            symbol=args.regime_symbol,
            calendar=debug_ctx["calendar"],
        )

    entries_map = debug_ctx["entries_count_by_date"]
    exits_map = debug_ctx["exits_count_by_date"]
    cost_rate = float(args.cost_bps) / 10000.0
    if not report_df.empty:
        report_df["regime_on"] = report_df["date_dt"].map(
            lambda d: bool(regime_on_by_date.get(d, False))
        ) if args.regime_filter_sma200 else True
        report_df["portfolio_r_effective"] = report_df["portfolio_r"].astype(float)
        report_df["n_open_effective"] = report_df["n_open"].astype(int)
        report_df["entries_effective"] = report_df["date_dt"].map(
            lambda d: int(entries_map.get(d, 0))
        ).astype(int)
        report_df["exits_effective"] = report_df["date_dt"].map(
            lambda d: int(exits_map.get(d, 0))
        ).astype(int)
        if args.regime_filter_sma200:
            regime_off_mask = ~report_df["regime_on"].astype(bool)
            report_df.loc[regime_off_mask, "portfolio_r_effective"] = 0.0
            report_df.loc[regime_off_mask, "n_open_effective"] = 0
            report_df.loc[regime_off_mask, "entries_effective"] = 0
            report_df.loc[regime_off_mask, "exits_effective"] = 0

        events_series = report_df["date_dt"].map(
            lambda d: int(entries_map.get(d, 0)) + int(exits_map.get(d, 0))
        )
        denom_series = report_df["n_open"].astype(float).clip(lower=1.0)
        report_df["cost_daily"] = events_series.astype(float) * cost_rate * (1.0 / denom_series)
        report_df["cost_daily_effective"] = report_df["cost_daily"].astype(float)
        if args.regime_filter_sma200:
            report_df.loc[~report_df["regime_on"].astype(bool), "cost_daily_effective"] = 0.0
        report_df["portfolio_r_net_effective"] = (
            report_df["portfolio_r_effective"].astype(float)
            - report_df["cost_daily_effective"].astype(float)
        )
        report_df["portfolio_r_net"] = report_df["portfolio_r_net_effective"].astype(float)
    else:
        report_df["regime_on"] = pd.Series(dtype=bool)
        report_df["portfolio_r_effective"] = pd.Series(dtype=float)
        report_df["n_open_effective"] = pd.Series(dtype=int)
        report_df["entries_effective"] = pd.Series(dtype=int)
        report_df["exits_effective"] = pd.Series(dtype=int)
        report_df["cost_daily"] = pd.Series(dtype=float)
        report_df["cost_daily_effective"] = pd.Series(dtype=float)
        report_df["portfolio_r_net_effective"] = pd.Series(dtype=float)
        report_df["portfolio_r_net"] = pd.Series(dtype=float)

    if args.regime_filter_sma200:
        n_days_regime = int(len(report_df))
        if report_df.empty:
            n_regime_on = 0
            n_regime_off = 0
        else:
            n_regime_on = int(report_df["regime_on"].astype(bool).sum())
            n_regime_off = int(n_days_regime - n_regime_on)
        print("REGIME_FILTER enabled=1")
        print(f"REGIME_FILTER symbol={args.regime_symbol}")
        print("REGIME_FILTER sma_window=200")
        print(f"REGIME_FILTER n_days={n_days_regime}")
        print(f"REGIME_FILTER n_regime_on={n_regime_on}")
        print(f"REGIME_FILTER n_regime_off={n_regime_off}")

    strategy_metrics_df = pd.DataFrame(
        {"portfolio_r": report_df["portfolio_r_effective"].astype(float)}
    ) if not report_df.empty else pd.DataFrame(columns=["portfolio_r"])
    strategy_metrics = _compute_metrics_from_daily(strategy_metrics_df)
    bench_metrics = {
        "cagr": 0.0,
        "ann_vol": 0.0,
        "sharpe": 0.0,
        "max_drawdown": 0.0,
        "final_equity": 0.0,
    }
    rel_metrics = {"excess_cagr": 0.0, "alpha_daily_mean": 0.0, "information_ratio": 0.0}
    bench_r_report = pd.Series(dtype=float)
    if args.metrics and not report_df.empty:
        print(f"METRICS cagr={strategy_metrics['cagr']}")
        print(f"METRICS ann_vol={strategy_metrics['ann_vol']}")
        print(f"METRICS sharpe={strategy_metrics['sharpe']}")
        print(f"METRICS max_drawdown={strategy_metrics['max_drawdown']}")
        print(f"METRICS final_equity={strategy_metrics['final_equity']}")
    if args.trade_metrics:
        tmetrics = _compute_trade_metrics(trades_total, kept_trade_df)
        print(f"TRADE_METRICS trades_total={tmetrics['trades_total']}")
        print(f"TRADE_METRICS trades_after_no_overlap={tmetrics['trades_after_no_overlap']}")
        print(f"TRADE_METRICS trades_dropped_overlap={tmetrics['trades_dropped_overlap']}")
        print(f"TRADE_METRICS win_rate={tmetrics['win_rate']}")
        print(f"TRADE_METRICS avg_trade_return={tmetrics['avg_trade_return']}")
        print(f"TRADE_METRICS median_trade_return={tmetrics['median_trade_return']}")
        print(f"TRADE_METRICS n_exit_STOP_OUT={tmetrics['n_exit_STOP_OUT']}")
        print(f"TRADE_METRICS n_exit_TIME_35D={tmetrics['n_exit_TIME_35D']}")
        print(f"TRADE_METRICS n_exit_OTHER={tmetrics['n_exit_OTHER']}")
    if args.benchmark is not None and not report_df.empty:
        report_calendar = list(report_df["date_dt"])
        bench_metrics, rel_metrics, bench_r_report = _compute_benchmark_and_relative_metrics(
            osakedata_db=args.osakedata_db,
            market=args.market,
            symbol=args.benchmark,
            calendar=report_calendar,
            strategy_daily=report_df["portfolio_r_effective"].astype(float),
            strategy_cagr=float(strategy_metrics["cagr"]),
        )
        print(f"BENCHMARK symbol={args.benchmark}")
        print(f"BENCHMARK cagr={bench_metrics['cagr']}")
        print(f"BENCHMARK ann_vol={bench_metrics['ann_vol']}")
        print(f"BENCHMARK sharpe={bench_metrics['sharpe']}")
        print(f"BENCHMARK max_drawdown={bench_metrics['max_drawdown']}")
        print(f"BENCHMARK final_equity={bench_metrics['final_equity']}")
        print(f"RELATIVE excess_cagr={rel_metrics['excess_cagr']}")
        print(f"RELATIVE alpha_daily_mean={rel_metrics['alpha_daily_mean']}")
        print(f"RELATIVE information_ratio={rel_metrics['information_ratio']}")
    if args.rolling_window is not None:
        n = int(args.rolling_window)
        print(f"ROLLING window={n}")
        if report_df.empty or len(report_df) < n:
            print("ROLLING status=INSUFFICIENT_DATA")
        else:
            roll_df = pd.DataFrame(
                {"portfolio_r": report_df.tail(n)["portfolio_r_effective"].astype(float)}
            )
            rolling_metrics = _compute_metrics_from_daily(roll_df)
            print(f"ROLLING cagr={rolling_metrics['cagr']}")
            print(f"ROLLING ann_vol={rolling_metrics['ann_vol']}")
            print(f"ROLLING sharpe={rolling_metrics['sharpe']}")
            print(f"ROLLING max_drawdown={rolling_metrics['max_drawdown']}")
            print(f"ROLLING final_equity={rolling_metrics['final_equity']}")
    if args.exposure_turnover:
        if report_df.empty:
            exposure = 0.0
            avg_n_open = 0.0
            max_n_open_report = 0
            entries_total = 0
            exits_total = 0
            avg_entries_per_day = 0.0
            avg_exits_per_day = 0.0
            max_entries_in_day = 0
            max_exits_in_day = 0
        else:
            n_days_report = len(report_df)
            n_days_open_report = int((report_df["n_open_effective"] > 0).sum())
            exposure = n_days_open_report / n_days_report
            avg_n_open = float(report_df["n_open_effective"].mean())
            max_n_open_report = int(report_df["n_open_effective"].max())

            entries_list = list(report_df["entries_effective"].astype(int))
            exits_list = list(report_df["exits_effective"].astype(int))
            entries_total = int(sum(entries_list))
            exits_total = int(sum(exits_list))
            avg_entries_per_day = entries_total / n_days_report
            avg_exits_per_day = exits_total / n_days_report
            max_entries_in_day = int(max(entries_list)) if entries_list else 0
            max_exits_in_day = int(max(exits_list)) if exits_list else 0

        print(f"PORTFOLIO exposure={exposure}")
        print(f"PORTFOLIO avg_n_open={avg_n_open}")
        print(f"PORTFOLIO max_n_open={max_n_open_report}")
        print(f"TURNOVER entries_total={entries_total}")
        print(f"TURNOVER exits_total={exits_total}")
        print(f"TURNOVER avg_entries_per_day={avg_entries_per_day}")
        print(f"TURNOVER avg_exits_per_day={avg_exits_per_day}")
        print(f"TURNOVER max_entries_in_day={max_entries_in_day}")
        print(f"TURNOVER max_exits_in_day={max_exits_in_day}")
    net_metrics = {
        "cagr": 0.0,
        "ann_vol": 0.0,
        "sharpe": 0.0,
        "max_drawdown": 0.0,
        "final_equity": 1.0,
        "total_cost": 0.0,
    }
    if args.net_metrics:
        net_df = pd.DataFrame(
            {
                "portfolio_r_net": report_df["portfolio_r_net_effective"].astype(float),
                "cost_daily": report_df["cost_daily_effective"].astype(float),
            }
        ) if not report_df.empty else pd.DataFrame(columns=["portfolio_r_net", "cost_daily"])
        net_metrics = _compute_net_metrics(net_df)
        print(f"NET_METRICS cagr={net_metrics['cagr']}")
        print(f"NET_METRICS ann_vol={net_metrics['ann_vol']}")
        print(f"NET_METRICS sharpe={net_metrics['sharpe']}")
        print(f"NET_METRICS max_drawdown={net_metrics['max_drawdown']}")
        print(f"NET_METRICS final_equity={net_metrics['final_equity']}")
        print(f"NET_METRICS total_cost={net_metrics['total_cost']}")
    if args.benchmark is not None and args.net_metrics:
        if report_df.empty:
            rel_net_excess_cagr = 0.0
            rel_net_alpha_daily_mean = 0.0
            rel_net_information_ratio = 0.0
        else:
            report_calendar = list(report_df["date_dt"])
            if bench_r_report.empty:
                bench_r_report = pd.Series(index=pd.Index(report_calendar), dtype=float)
                bench_r_report[:] = float("nan")
            strategy_net = report_df["portfolio_r_net_effective"].astype(float).copy()
            strategy_net.index = pd.Index(report_calendar)
            joint_net = pd.concat(
                [strategy_net.rename("s_net"), bench_r_report.rename("b")], axis=1
            )
            joint_net = joint_net.dropna(subset=["b"])
            if joint_net.empty:
                rel_net_excess_cagr = 0.0
                rel_net_alpha_daily_mean = 0.0
                rel_net_information_ratio = 0.0
            else:
                excess_net = (joint_net["s_net"] - joint_net["b"]).astype(float)
                ex_net_mean = float(excess_net.mean())
                ex_net_std = float(excess_net.std(ddof=0))
                rel_net_excess_cagr = float(net_metrics["cagr"]) - float(bench_metrics["cagr"])
                rel_net_alpha_daily_mean = ex_net_mean
                rel_net_information_ratio = (
                    0.0 if ex_net_std == 0.0 else (ex_net_mean / ex_net_std) * math.sqrt(252.0)
                )
        print(f"RELATIVE_NET excess_cagr={rel_net_excess_cagr}")
        print(f"RELATIVE_NET alpha_daily_mean={rel_net_alpha_daily_mean}")
        print(f"RELATIVE_NET information_ratio={rel_net_information_ratio}")
    if args.net_metrics:
        if report_df.empty:
            cagr_delta = 0.0
            final_equity_delta = 0.0
            total_cost = 0.0
        else:
            cagr_delta = float(strategy_metrics["cagr"]) - float(net_metrics["cagr"])
            final_equity_delta = float(strategy_metrics["final_equity"]) - float(
                net_metrics["final_equity"]
            )
            total_cost = float(net_metrics["total_cost"])
        print(f"COST_IMPACT cagr_delta={cagr_delta}")
        print(f"COST_IMPACT final_equity_delta={final_equity_delta}")
        print(f"COST_IMPACT total_cost={total_cost}")
    if debug_date is not None:
        print(f"DEBUG_DATE date={debug_date.date().isoformat()}")
        calendar = debug_ctx["calendar"]
        if debug_date not in calendar:
            print("DEBUG_DATE status=NOT_IN_CALENDAR")
            return
        if cap_enabled:
            snapshot = debug_ctx["snapshot"]
            if snapshot is None:
                print(f"DEBUG_DATE max_open_positions={int(args.max_open_positions)}")
                print("DEBUG_DATE entries_candidates=0")
                print("DEBUG_DATE entries_accepted=0")
                print("DEBUG_DATE entries_dropped=0")
            else:
                print(f"DEBUG_DATE max_open_positions={int(args.max_open_positions)}")
                print(f"DEBUG_DATE entries_candidates={snapshot['entries_candidates']}")
                print(f"DEBUG_DATE entries_accepted={snapshot['entries_accepted']}")
                print(f"DEBUG_DATE entries_dropped={snapshot['entries_dropped']}")
                if args.rank_by_fastpass_score:
                    for ticker, score, accepted in snapshot["cap_ranked_rows"]:
                        score_str = "NA" if score is None else str(score)
                        if accepted:
                            print(f"DEBUG_POS_ACCEPT ticker={ticker} score_fastpass={score_str}")
                        else:
                            print(f"DEBUG_POS_DROP ticker={ticker} score_fastpass={score_str}")
        regime_on_debug = True
        if args.regime_filter_sma200:
            regime_on_debug = bool(regime_on_by_date.get(debug_date, False))
            print(f"DEBUG_DATE regime_symbol={args.regime_symbol}")
            print(f"DEBUG_DATE regime_on={1 if regime_on_debug else 0}")
        snapshot = debug_ctx["snapshot"]
        if snapshot is None or not regime_on_debug:
            print("DEBUG_DATE n_open=0")
            print("DEBUG_DATE portfolio_r=0.0")
            print("DEBUG_DATE status=NO_OPEN_POSITIONS")
            return
        n_open = int(snapshot["n_open"])
        portfolio_r = float(snapshot["portfolio_r"])
        print(f"DEBUG_DATE n_open={n_open}")
        print(f"DEBUG_DATE portfolio_r={portfolio_r}")
        if n_open == 0:
            print("DEBUG_DATE status=NO_OPEN_POSITIONS")
            return
        for pos in snapshot["positions"]:
            if pos["r"] is None:
                r_str = "NA"
            else:
                r_str = str(pos["r"])
            print(
                f"DEBUG_POS ticker={pos['ticker']} r={r_str} "
                f"weight={pos['weight']} contrib={pos['contrib']}"
            )
        print(f"DEBUG_DATE mean_used={snapshot['mean_used']}")


if __name__ == "__main__":
    main()
