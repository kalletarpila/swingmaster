from __future__ import annotations

import json
import math
import sqlite3

from swingmaster.ew_score.model_config import load_model_config
from swingmaster.ew_score.repo import RcEwScoreDailyRepo


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def compute_and_store_ew_scores(
    rc_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    as_of_date: str,
    rule_id: str,
    repo: RcEwScoreDailyRepo | None = None,
    print_rows: bool = False,
) -> int:
    model = load_model_config(rule_id)
    target_repo = repo if repo is not None else RcEwScoreDailyRepo(rc_conn)
    target_repo.ensure_schema()

    ticker_rows = rc_conn.execute(
        """
        SELECT ticker
        FROM rc_state_daily
        WHERE date = ?
          AND state = 'ENTRY_WINDOW'
        ORDER BY ticker
        """,
        (as_of_date,),
    ).fetchall()
    tickers = [row[0] for row in ticker_rows]

    if print_rows:
        print("EW_SCORE_DAILY")
        print("ticker | ew_level_day3 | ew_score_day3 | r_prefix_pct | entry_window_date")

    stored = 0
    for ticker in tickers:
        ep_row = rc_conn.execute(
            """
            SELECT entry_window_date, entry_window_exit_date
            FROM rc_pipeline_episode
            WHERE ticker = ?
              AND entry_window_date <= ?
              AND (entry_window_exit_date IS NULL OR ? <= entry_window_exit_date)
            ORDER BY entry_window_date DESC
            LIMIT 1
            """,
            (ticker, as_of_date, as_of_date),
        ).fetchone()
        if ep_row is None:
            continue

        entry_window_date = ep_row[0]
        entry_window_exit_date = ep_row[1]

        end_date = as_of_date
        if entry_window_exit_date is not None and entry_window_exit_date < end_date:
            end_date = entry_window_exit_date

        px_rows = osakedata_conn.execute(
            """
            SELECT close
            FROM osakedata
            WHERE osake = ?
              AND pvm >= ?
              AND pvm <= ?
            ORDER BY pvm ASC
            LIMIT 4
            """,
            (ticker, entry_window_date, end_date),
        ).fetchall()
        if not px_rows:
            continue

        rn_available = len(px_rows)
        close_day0 = float(px_rows[0][0])
        close_prefix = float(px_rows[-1][0])
        if close_day0 == 0.0:
            continue
        r_prefix_pct = 100.0 * (close_prefix / close_day0 - 1.0)
        ew_score_day3 = _sigmoid(model.beta0 + model.beta1 * r_prefix_pct)
        if rn_available < 4:
            ew_level_day3 = 0
        else:
            ew_level_day3 = 2
            if model.level3_score_threshold is not None and ew_score_day3 >= model.level3_score_threshold:
                ew_level_day3 = 3

        inputs_payload = {
            "as_of_date": as_of_date,
            "beta0": model.beta0,
            "beta1": model.beta1,
            "close_day0": close_day0,
            "close_prefix": close_prefix,
            "entry_window_date": entry_window_date,
            "entry_window_exit_date": entry_window_exit_date,
            "r_prefix_pct": r_prefix_pct,
            "rn_available": rn_available,
            "rule_id": model.rule_id,
        }
        if model.level3_score_threshold is not None:
            inputs_payload["level3_score_threshold"] = model.level3_score_threshold
        inputs_json = json.dumps(inputs_payload, sort_keys=True)

        target_repo.upsert_row(
            ticker=ticker,
            date=as_of_date,
            ew_score_day3=ew_score_day3,
            ew_level_day3=ew_level_day3,
            ew_rule=model.rule_id,
            inputs_json=inputs_json,
        )
        stored += 1

        if print_rows:
            print(
                f"{ticker} | {ew_level_day3} | {ew_score_day3:.6f} | "
                f"{r_prefix_pct:.6f} | {entry_window_date}"
            )

    return stored
