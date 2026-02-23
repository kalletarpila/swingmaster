"""Generate deterministic performance report from rc_pipeline_episode."""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import os
import sqlite3
import statistics
import sys
import csv
from typing import Dict, Iterable, List, Optional, Tuple


DEFAULT_DB = "/home/kalle/projects/swingmaster/swingmaster_rc.db"
DEFAULT_REPORT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "perf_reports")
)


class TeeWriter:
    def __init__(self, *writers):
        self._writers = writers

    def write(self, data: str) -> int:
        for writer in self._writers:
            writer.write(data)
        return len(data)

    def flush(self) -> None:
        for writer in self._writers:
            writer.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate performance report from rc_pipeline_episode")
    parser.add_argument("--db", default=DEFAULT_DB, help="RC SQLite database path")
    parser.add_argument("--pipeline-version", default=None, help="Optional pipeline_version filter")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit by computed_at DESC")
    parser.add_argument(
        "--out",
        default=None,
        help="Optional output file path for full report (default: auto-save under perf_reports/)",
    )
    parser.add_argument("--format", choices=["text", "csv"], default="text", help="Output format")
    return parser.parse_args()


def utc_now_z() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def compute_return(num: object, den: object) -> Optional[float]:
    n = to_float(num)
    d = to_float(den)
    if n is None or d is None or d == 0:
        return None
    return (n / d) - 1.0


def summarize(values: Iterable[float]) -> Optional[Dict[str, float]]:
    vals = list(values)
    if not vals:
        return None
    n = len(vals)
    mean_v = statistics.fmean(vals)
    median_v = statistics.median(vals)
    std_v = statistics.pstdev(vals) if n > 1 else 0.0
    min_v = min(vals)
    max_v = max(vals)
    win_rate_v = sum(1 for v in vals if v > 0) / n
    return {
        "n": n,
        "mean": mean_v,
        "median": median_v,
        "std": std_v,
        "min": min_v,
        "max": max_v,
        "win_rate": win_rate_v,
    }


def fmt6(value: object) -> str:
    if value is None:
        return "NA"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.6f}".replace(".", ",")
    return str(value)


def fetch_rows(conn: sqlite3.Connection, pipeline_version: Optional[str], limit: Optional[int]) -> List[sqlite3.Row]:
    where_parts: List[str] = []
    params: List[object] = []

    if pipeline_version is not None:
        where_parts.append("pipeline_version = ?")
        params.append(pipeline_version)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        params.append(limit)

    sql = f"""
        WITH base AS (
            SELECT *
            FROM rc_pipeline_episode
            {where_sql}
            ORDER BY computed_at DESC, episode_id DESC
            {limit_sql}
        )
        SELECT
            episode_id,
            entry_window_exit_state,
            ew_confirm_confirmed,
            close_at_entry,
            close_at_ew_start,
            close_at_ew_exit,
            peak60_growth_pct_close_ew_to_peak,
            post60_peak_days_from_exit,
            post60_peak_sma5,
            post60_growth_pct_close_ew_exit_to_peak
        FROM base
    """
    return conn.execute(sql, params).fetchall()


def fetch_episode_max_levels(
    conn: sqlite3.Connection, pipeline_version: Optional[str], limit: Optional[int]
) -> Dict[str, Optional[int]]:
    cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(rc_ew_score_daily)").fetchall()}
    if "ew_level_rolling" in cols:
        level_expr = "s.ew_level_rolling"
    else:
        level_expr = "s.ew_level_day3"

    where_parts: List[str] = []
    params: List[object] = []

    if pipeline_version is not None:
        where_parts.append("pipeline_version = ?")
        params.append(pipeline_version)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        params.append(limit)

    sql = f"""
        WITH base AS (
            SELECT *
            FROM rc_pipeline_episode
            {where_sql}
            ORDER BY computed_at DESC, episode_id DESC
            {limit_sql}
        )
        SELECT
            b.episode_id,
            MAX({level_expr}) AS episode_max_level
        FROM base b
        LEFT JOIN rc_ew_score_daily s
          ON s.ticker = b.ticker
         AND s.date >= b.entry_window_date
         AND (b.entry_window_exit_date IS NULL OR s.date <= b.entry_window_exit_date)
        GROUP BY b.episode_id
    """
    rows = conn.execute(sql, params).fetchall()
    out: Dict[str, Optional[int]] = {}
    for row in rows:
        level = row["episode_max_level"]
        out[row["episode_id"]] = None if level is None else int(level)
    return out


def fetch_episode_max_fastpass_levels(
    conn: sqlite3.Connection, pipeline_version: Optional[str], limit: Optional[int]
) -> Dict[str, Optional[int]]:
    cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(rc_ew_score_daily)").fetchall()}
    if "ew_level_fastpass" not in cols:
        return {}

    where_parts: List[str] = []
    params: List[object] = []

    if pipeline_version is not None:
        where_parts.append("pipeline_version = ?")
        params.append(pipeline_version)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        params.append(limit)

    sql = f"""
        WITH base AS (
            SELECT *
            FROM rc_pipeline_episode
            {where_sql}
            ORDER BY computed_at DESC, episode_id DESC
            {limit_sql}
        )
        SELECT
            b.episode_id,
            MAX(s.ew_level_fastpass) AS episode_max_fastpass_level
        FROM base b
        LEFT JOIN rc_ew_score_daily s
          ON s.ticker = b.ticker
         AND s.date >= b.entry_window_date
         AND (b.entry_window_exit_date IS NULL OR s.date <= b.entry_window_exit_date)
        GROUP BY b.episode_id
    """
    rows = conn.execute(sql, params).fetchall()
    out: Dict[str, Optional[int]] = {}
    for row in rows:
        level = row["episode_max_fastpass_level"]
        out[row["episode_id"]] = None if level is None else int(level)
    return out


def fetch_single_or_mixed(conn: sqlite3.Connection, sql: str) -> str:
    rows = conn.execute(sql).fetchall()
    values = [row[0] for row in rows if row[0] is not None]
    if not values:
        return "NONE"
    uniq = sorted(set(str(v) for v in values))
    if len(uniq) == 1:
        return uniq[0]
    return "MIXED"


def fetch_ew_rules_used(conn: sqlite3.Connection) -> str:
    cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(rc_ew_score_daily)").fetchall()}

    def _distinct_non_null(col: str) -> List[str]:
        if col not in cols:
            return []
        rows = conn.execute(f"SELECT DISTINCT {col} FROM rc_ew_score_daily WHERE {col} IS NOT NULL").fetchall()
        return sorted(str(row[0]) for row in rows if row[0] is not None and str(row[0]) != "")

    parts: List[str] = []
    legacy = _distinct_non_null("ew_rule")
    rolling = _distinct_non_null("ew_rule_rolling")
    fastpass = _distinct_non_null("ew_rule_fastpass")
    if legacy:
        parts.append(f"legacy={','.join(legacy)}")
    if rolling:
        parts.append(f"rolling={','.join(rolling)}")
    if fastpass:
        parts.append(f"fastpass={','.join(fastpass)}")
    if not parts:
        return "NONE"
    return " | ".join(parts)


def infer_signal_version(policy_version: str) -> str:
    if policy_version == "v3":
        return "v3"
    if policy_version in {"v1", "v2"}:
        return "v2"
    if policy_version in {"NONE", "MIXED"}:
        return policy_version
    return "UNKNOWN"


def main() -> None:
    args = parse_args()
    if args.out is None:
        os.makedirs(DEFAULT_REPORT_DIR, exist_ok=True)
        db_stem = os.path.splitext(os.path.basename(args.db))[0]
        ext = "csv" if args.format == "csv" else "txt"
        args.out = os.path.join(DEFAULT_REPORT_DIR, f"{db_stem}_performance_report.{ext}")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        rows = fetch_rows(conn, args.pipeline_version, args.limit)
        episode_max_levels = fetch_episode_max_levels(conn, args.pipeline_version, args.limit)
        episode_max_fastpass_levels = fetch_episode_max_fastpass_levels(
            conn, args.pipeline_version, args.limit
        )
        policy_id_used = fetch_single_or_mixed(
            conn,
            "SELECT DISTINCT policy_id FROM rc_run",
        )
        policy_version_used = fetch_single_or_mixed(
            conn,
            "SELECT DISTINCT policy_version FROM rc_run",
        )
        ew_rule_used = fetch_ew_rules_used(conn)
    finally:
        conn.close()
    signal_version_used = infer_signal_version(policy_version_used)

    episodes: List[Dict[str, object]] = []
    for row in rows:
        r_entry_to_ew_start = compute_return(row["close_at_ew_start"], row["close_at_entry"])
        r_ew_window = compute_return(row["close_at_ew_exit"], row["close_at_ew_start"])
        peak60_pct = to_float(row["peak60_growth_pct_close_ew_to_peak"])
        r_ew_start_to_peak60 = (peak60_pct / 100.0) if peak60_pct is not None else None
        r_ew_start_to_post60_peak_sma5 = compute_return(row["post60_peak_sma5"], row["close_at_ew_start"])
        post60_peak_days_from_exit = to_float(row["post60_peak_days_from_exit"])
        post60_growth_pct_close_ew_exit_to_peak = to_float(
            row["post60_growth_pct_close_ew_exit_to_peak"]
        )
        episodes.append(
            {
                "episode_id": row["episode_id"],
                "exit_state": row["entry_window_exit_state"],
                "confirmed": row["ew_confirm_confirmed"],
                "r_entry_to_ew_start": r_entry_to_ew_start,
                "r_ew_window": r_ew_window,
                "r_ew_start_to_peak60": r_ew_start_to_peak60,
                "r_ew_start_to_post60_peak_sma5": r_ew_start_to_post60_peak_sma5,
                "post60_peak_days_from_exit": post60_peak_days_from_exit,
                "post60_growth_pct_close_ew_exit_to_peak": post60_growth_pct_close_ew_exit_to_peak,
            }
        )

    n_total = len(episodes)
    n_closed = sum(1 for e in episodes if e["exit_state"] is not None)
    n_open = sum(1 for e in episodes if e["exit_state"] is None)
    n_exit_pass = sum(1 for e in episodes if e["exit_state"] == "PASS")
    n_exit_no_trade = sum(1 for e in episodes if e["exit_state"] == "NO_TRADE")
    n_has_confirm = sum(1 for e in episodes if e["confirmed"] is not None)
    n_confirmed = sum(1 for e in episodes if e["confirmed"] == 1)
    pct_confirmed = (n_confirmed / n_has_confirm) if n_has_confirm > 0 else None

    series_names = [
        "r_entry_to_ew_start",
        "r_ew_window",
        "r_ew_start_to_peak60",
        "r_ew_start_to_post60_peak_sma5",
        "post60_peak_days_from_exit",
        "post60_growth_pct_close_ew_exit_to_peak",
    ]

    overall_stats: Dict[str, Optional[Dict[str, float]]] = {}
    for s in series_names:
        vals = [float(e[s]) for e in episodes if e[s] is not None]
        overall_stats[s] = summarize(vals)

    by_exit: List[Tuple[str, str, Optional[Dict[str, float]]]] = []
    for exit_state in ("PASS", "NO_TRADE"):
        subset = [e for e in episodes if e["exit_state"] == exit_state]
        for s in series_names:
            vals = [float(e[s]) for e in subset if e[s] is not None]
            by_exit.append((exit_state, s, summarize(vals)))

    by_confirm: List[Tuple[int, str, Optional[Dict[str, float]]]] = []
    for confirmed in (0, 1):
        subset = [e for e in episodes if e["confirmed"] == confirmed]
        for s in series_names:
            vals = [float(e[s]) for e in subset if e[s] is not None]
            by_confirm.append((confirmed, s, summarize(vals)))

    for e in episodes:
        e["episode_max_level"] = episode_max_levels.get(str(e["episode_id"]))

    by_ewlevel_early: List[Tuple[int, str, Optional[Dict[str, float]]]] = []
    for ew_level in (0, 1):
        subset = [e for e in episodes if e["episode_max_level"] == ew_level]
        for s in series_names:
            vals = [float(e[s]) for e in subset if e[s] is not None]
            by_ewlevel_early.append((ew_level, s, summarize(vals)))

    by_ewlevel_mature: List[Tuple[int, str, Optional[Dict[str, float]]]] = []
    for ew_level in (2, 3):
        subset = [e for e in episodes if e["episode_max_level"] == ew_level]
        for s in series_names:
            vals = [float(e[s]) for e in subset if e[s] is not None]
            by_ewlevel_mature.append((ew_level, s, summarize(vals)))

    for e in episodes:
        e["episode_max_fastpass_level"] = episode_max_fastpass_levels.get(str(e["episode_id"]))

    by_fastpass_early: List[Tuple[int, str, Optional[Dict[str, float]]]] = []
    for ew_level in (0, 1):
        subset = [e for e in episodes if e["episode_max_fastpass_level"] == ew_level]
        for s in series_names:
            vals = [float(e[s]) for e in subset if e[s] is not None]
            by_fastpass_early.append((ew_level, s, summarize(vals)))

    by_fastpass_mature: List[Tuple[int, str, Optional[Dict[str, float]]]] = []
    for ew_level in (2, 3):
        subset = [e for e in episodes if e["episode_max_fastpass_level"] == ew_level]
        for s in series_names:
            vals = [float(e[s]) for e in subset if e[s] is not None]
            by_fastpass_mature.append((ew_level, s, summarize(vals)))

    out_file = None
    if args.out is not None:
        out_dir = os.path.dirname(os.path.abspath(args.out))
        if not os.path.isdir(out_dir):
            raise SystemExit(f"ERROR: output directory does not exist: {out_dir}")
        try:
            out_file = open(args.out, "w", encoding="utf-8")
        except OSError as exc:
            raise SystemExit(f"ERROR: failed to open output file {args.out}: {exc}")

    ctx = contextlib.nullcontext()
    if out_file is not None:
        ctx = contextlib.redirect_stdout(TeeWriter(sys.stdout, out_file))

    try:
        with ctx:
            if args.format == "csv":
                writer = csv.writer(sys.stdout, delimiter=";")
                writer.writerow(
                    [
                        "row_type",
                        "section",
                        "group",
                        "series_name",
                        "key",
                        "value",
                        "n",
                        "mean",
                        "median",
                        "std",
                        "min",
                        "max",
                        "win_rate",
                    ]
                )
                writer.writerow(["meta", "META", "", "", "db_path", args.db, "", "", "", "", "", "", ""])
                writer.writerow(["meta", "META", "", "", "generated_at_utc", utc_now_z(), "", "", "", "", "", "", ""])
                writer.writerow(["meta", "META", "", "", "pipeline_version_filter", args.pipeline_version if args.pipeline_version is not None else "NONE", "", "", "", "", "", "", ""])
                writer.writerow(["meta", "META", "", "", "policy_used", f"{policy_id_used} ({policy_version_used})", "", "", "", "", "", "", ""])
                writer.writerow(["meta", "META", "", "", "signal_used", signal_version_used, "", "", "", "", "", "", ""])
                writer.writerow(["meta", "META", "", "", "ew_score_rule_used", ew_rule_used, "", "", "", "", "", "", ""])
                writer.writerow(["meta", "META", "", "", "limit", args.limit if args.limit is not None else "NONE", "", "", "", "", "", "", ""])

                writer.writerow(["count", "COUNTS", "", "", "n_total", n_total, "", "", "", "", "", "", ""])
                writer.writerow(["count", "COUNTS", "", "", "n_closed", n_closed, "", "", "", "", "", "", ""])
                writer.writerow(["count", "COUNTS", "", "", "n_open", n_open, "", "", "", "", "", "", ""])
                writer.writerow(["count", "COUNTS", "", "", "n_exit_pass", n_exit_pass, "", "", "", "", "", "", ""])
                writer.writerow(["count", "COUNTS", "", "", "n_exit_no_trade", n_exit_no_trade, "", "", "", "", "", "", ""])
                writer.writerow(["count", "COUNTS", "", "", "n_has_confirm", n_has_confirm, "", "", "", "", "", "", ""])
                writer.writerow(["count", "COUNTS", "", "", "n_confirmed", n_confirmed, "", "", "", "", "", "", ""])
                writer.writerow(["count", "COUNTS", "", "", "pct_confirmed_among_has_confirm", fmt6(pct_confirmed), "", "", "", "", "", "", ""])

                for s in series_names:
                    st = overall_stats[s]
                    if st is None:
                        writer.writerow(["metric", "RETURNS_OVERALL", "", s, "", "", 0, "NA", "NA", "NA", "NA", "NA", "NA"])
                    else:
                        writer.writerow(["metric", "RETURNS_OVERALL", "", s, "", "", st["n"], fmt6(st["mean"]), fmt6(st["median"]), fmt6(st["std"]), fmt6(st["min"]), fmt6(st["max"]), fmt6(st["win_rate"])])

                for exit_state, s, st in by_exit:
                    if st is None:
                        writer.writerow(["metric", "RETURNS_BY_EXIT_STATE", exit_state, s, "", "", 0, "NA", "NA", "NA", "NA", "NA", "NA"])
                    else:
                        writer.writerow(["metric", "RETURNS_BY_EXIT_STATE", exit_state, s, "", "", st["n"], fmt6(st["mean"]), fmt6(st["median"]), fmt6(st["std"]), fmt6(st["min"]), fmt6(st["max"]), fmt6(st["win_rate"])])

                for confirmed, s, st in by_confirm:
                    if st is None:
                        writer.writerow(["metric", "RETURNS_BY_CONFIRM", str(confirmed), s, "", "", 0, "NA", "NA", "NA", "NA", "NA", "NA"])
                    else:
                        writer.writerow(["metric", "RETURNS_BY_CONFIRM", str(confirmed), s, "", "", st["n"], fmt6(st["mean"]), fmt6(st["median"]), fmt6(st["std"]), fmt6(st["min"]), fmt6(st["max"]), fmt6(st["win_rate"])])

                for ew_level, s, st in by_ewlevel_early:
                    if st is None:
                        writer.writerow(["metric", "EARLY_PHASE_BY_MAX_LEVEL", str(ew_level), s, "", "", 0, "NA", "NA", "NA", "NA", "NA", "NA"])
                    else:
                        writer.writerow(["metric", "EARLY_PHASE_BY_MAX_LEVEL", str(ew_level), s, "", "", st["n"], fmt6(st["mean"]), fmt6(st["median"]), fmt6(st["std"]), fmt6(st["min"]), fmt6(st["max"]), fmt6(st["win_rate"])])

                for ew_level, s, st in by_ewlevel_mature:
                    if st is None:
                        writer.writerow(["metric", "MATURE_PHASE_BY_MAX_LEVEL", str(ew_level), s, "", "", 0, "NA", "NA", "NA", "NA", "NA", "NA"])
                    else:
                        writer.writerow(["metric", "MATURE_PHASE_BY_MAX_LEVEL", str(ew_level), s, "", "", st["n"], fmt6(st["mean"]), fmt6(st["median"]), fmt6(st["std"]), fmt6(st["min"]), fmt6(st["max"]), fmt6(st["win_rate"])])

                for ew_level, s, st in by_fastpass_early:
                    if st is None:
                        writer.writerow(["metric", "EARLY_PHASE_BY_MAX_FASTPASS_LEVEL", str(ew_level), s, "", "", 0, "NA", "NA", "NA", "NA", "NA", "NA"])
                    else:
                        writer.writerow(["metric", "EARLY_PHASE_BY_MAX_FASTPASS_LEVEL", str(ew_level), s, "", "", st["n"], fmt6(st["mean"]), fmt6(st["median"]), fmt6(st["std"]), fmt6(st["min"]), fmt6(st["max"]), fmt6(st["win_rate"])])

                for ew_level, s, st in by_fastpass_mature:
                    if st is None:
                        writer.writerow(["metric", "MATURE_PHASE_BY_MAX_FASTPASS_LEVEL", str(ew_level), s, "", "", 0, "NA", "NA", "NA", "NA", "NA", "NA"])
                    else:
                        writer.writerow(["metric", "MATURE_PHASE_BY_MAX_FASTPASS_LEVEL", str(ew_level), s, "", "", st["n"], fmt6(st["mean"]), fmt6(st["median"]), fmt6(st["std"]), fmt6(st["min"]), fmt6(st["max"]), fmt6(st["win_rate"])])
            else:
                print("PERFORMANCE_REPORT")
                print(f"db_path={args.db}")
                print(f"generated_at_utc={utc_now_z()}")
                print(f"pipeline_version_filter={args.pipeline_version if args.pipeline_version is not None else 'NONE'}")
                print(f"policy_used={policy_id_used} ({policy_version_used})")
                print(f"signal_used={signal_version_used}")
                print(f"ew_score_rule_used={ew_rule_used}")
                print(f"limit={args.limit if args.limit is not None else 'NONE'}")

                print("")
                print("COUNTS")
                print(f"n_total={n_total}")
                print(f"n_closed={n_closed}")
                print(f"n_open={n_open}")
                print(f"n_exit_pass={n_exit_pass}")
                print(f"n_exit_no_trade={n_exit_no_trade}")
                print(f"n_has_confirm={n_has_confirm}")
                print(f"n_confirmed={n_confirmed}")
                print(f"pct_confirmed_among_has_confirm={fmt6(pct_confirmed)}")

                print("")
                print("RETURNS_OVERALL")
                print(f"{'series_name':<24} {'n':>8} {'mean':>12} {'median':>12} {'std':>12} {'min':>12} {'max':>12} {'win_rate':>12}")
                for s in series_names:
                    st = overall_stats[s]
                    if st is None:
                        print(f"{s:<24} {0:>8} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12}")
                        continue
                    print(
                        f"{s:<24} {st['n']:>8} {fmt6(st['mean']):>12} {fmt6(st['median']):>12} {fmt6(st['std']):>12} "
                        f"{fmt6(st['min']):>12} {fmt6(st['max']):>12} {fmt6(st['win_rate']):>12}"
                    )

                print("")
                print("RETURNS_BY_EXIT_STATE")
                print(f"{'exit_state':<12} {'series_name':<24} {'n':>8} {'mean':>12} {'median':>12} {'std':>12} {'min':>12} {'max':>12} {'win_rate':>12}")
                for exit_state, s, st in by_exit:
                    if st is None:
                        print(f"{exit_state:<12} {s:<24} {0:>8} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12}")
                        continue
                    print(
                        f"{exit_state:<12} {s:<24} {st['n']:>8} {fmt6(st['mean']):>12} {fmt6(st['median']):>12} {fmt6(st['std']):>12} "
                        f"{fmt6(st['min']):>12} {fmt6(st['max']):>12} {fmt6(st['win_rate']):>12}"
                    )

                print("")
                print("RETURNS_BY_CONFIRM")
                print(f"{'confirmed':<10} {'series_name':<24} {'n':>8} {'mean':>12} {'median':>12} {'std':>12} {'min':>12} {'max':>12} {'win_rate':>12}")
                for confirmed, s, st in by_confirm:
                    if st is None:
                        print(f"{confirmed:<10} {s:<24} {0:>8} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12}")
                        continue
                    print(
                        f"{confirmed:<10} {s:<24} {st['n']:>8} {fmt6(st['mean']):>12} {fmt6(st['median']):>12} {fmt6(st['std']):>12} "
                        f"{fmt6(st['min']):>12} {fmt6(st['max']):>12} {fmt6(st['win_rate']):>12}"
                    )

                print("")
                print("EARLY_PHASE_BY_MAX_LEVEL")
                print(f"{'max_level':<10} {'series_name':<24} {'n':>8} {'mean':>12} {'median':>12} {'std':>12} {'min':>12} {'max':>12} {'win_rate':>12}")
                for ew_level, s, st in by_ewlevel_early:
                    if st is None:
                        print(f"{ew_level:<10} {s:<24} {0:>8} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12}")
                        continue
                    print(
                        f"{ew_level:<10} {s:<24} {st['n']:>8} {fmt6(st['mean']):>12} {fmt6(st['median']):>12} {fmt6(st['std']):>12} "
                        f"{fmt6(st['min']):>12} {fmt6(st['max']):>12} {fmt6(st['win_rate']):>12}"
                    )

                print("")
                print("MATURE_PHASE_BY_MAX_LEVEL")
                print(f"{'max_level':<10} {'series_name':<24} {'n':>8} {'mean':>12} {'median':>12} {'std':>12} {'min':>12} {'max':>12} {'win_rate':>12}")
                for ew_level, s, st in by_ewlevel_mature:
                    if st is None:
                        print(f"{ew_level:<10} {s:<24} {0:>8} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12}")
                        continue
                    print(
                        f"{ew_level:<10} {s:<24} {st['n']:>8} {fmt6(st['mean']):>12} {fmt6(st['median']):>12} {fmt6(st['std']):>12} "
                        f"{fmt6(st['min']):>12} {fmt6(st['max']):>12} {fmt6(st['win_rate']):>12}"
                    )

                print("")
                print("EARLY_PHASE_BY_MAX_FASTPASS_LEVEL")
                print(f"{'max_level':<10} {'series_name':<24} {'n':>8} {'mean':>12} {'median':>12} {'std':>12} {'min':>12} {'max':>12} {'win_rate':>12}")
                for ew_level, s, st in by_fastpass_early:
                    if st is None:
                        print(f"{ew_level:<10} {s:<24} {0:>8} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12}")
                        continue
                    print(
                        f"{ew_level:<10} {s:<24} {st['n']:>8} {fmt6(st['mean']):>12} {fmt6(st['median']):>12} {fmt6(st['std']):>12} "
                        f"{fmt6(st['min']):>12} {fmt6(st['max']):>12} {fmt6(st['win_rate']):>12}"
                    )

                print("")
                print("MATURE_PHASE_BY_MAX_FASTPASS_LEVEL")
                print(f"{'max_level':<10} {'series_name':<24} {'n':>8} {'mean':>12} {'median':>12} {'std':>12} {'min':>12} {'max':>12} {'win_rate':>12}")
                for ew_level, s, st in by_fastpass_mature:
                    if st is None:
                        print(f"{ew_level:<10} {s:<24} {0:>8} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12} {'NA':>12}")
                        continue
                    print(
                        f"{ew_level:<10} {s:<24} {st['n']:>8} {fmt6(st['mean']):>12} {fmt6(st['median']):>12} {fmt6(st['std']):>12} "
                        f"{fmt6(st['min']):>12} {fmt6(st['max']):>12} {fmt6(st['win_rate']):>12}"
                    )
    except OSError as exc:
        if args.out is not None:
            raise SystemExit(f"ERROR: failed to write report to {args.out}: {exc}")
        raise
    finally:
        if out_file is not None:
            out_file.close()


if __name__ == "__main__":
    main()
