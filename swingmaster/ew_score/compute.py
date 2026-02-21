from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, timedelta

from swingmaster.ew_score.model_config import load_model_config
from swingmaster.ew_score.repo import RcEwScoreDailyRepo

EW_SCORE_FASTPASS_V1_USA_SMALL = "EW_SCORE_FASTPASS_V1_USA_SMALL"
FASTPASS_V1_USA_SMALL_BETA0 = 0.002991128723180779
FASTPASS_V1_USA_SMALL_THRESHOLD = 0.60
FASTPASS_V1_USA_SMALL_BETAS = {
    "r_stab_to_entry_pct": 0.373886,
    "decline_profile_UNKNOWN": -1.216561,
    "decline_profile_SLOW_DRIFT": 0.470619,
    "decline_profile_STRUCTURAL_DOWNTREND": 0.748934,
    "entry_quality_A": 0.915410,
    "entry_quality_B": 0.305710,
    "entry_quality_LEGACY": -1.218129,
}


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

    if rule_id == EW_SCORE_FASTPASS_V1_USA_SMALL:
        if print_rows:
            print("EW_SCORE_FASTPASS_DAILY")
            print("ticker | ew_level_fastpass | ew_score_fastpass | r_stab_to_entry_pct | entry_date")

        stored_fastpass = 0
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

            entry_date = ep_row[0]
            last_stab_row = rc_conn.execute(
                """
                SELECT MAX(date)
                FROM rc_state_daily
                WHERE ticker = ?
                  AND date < ?
                  AND state = 'STABILIZING'
                """,
                (ticker, entry_date),
            ).fetchone()
            if last_stab_row is None or last_stab_row[0] is None:
                continue
            last_stab_date = str(last_stab_row[0])

            close_entry_row = osakedata_conn.execute(
                """
                SELECT close
                FROM osakedata
                WHERE osake = ?
                  AND pvm = ?
                  AND market = 'usa'
                LIMIT 1
                """,
                (ticker, entry_date),
            ).fetchone()
            close_last_stab_row = osakedata_conn.execute(
                """
                SELECT close
                FROM osakedata
                WHERE osake = ?
                  AND pvm = ?
                  AND market = 'usa'
                LIMIT 1
                """,
                (ticker, last_stab_date),
            ).fetchone()
            if close_entry_row is None or close_last_stab_row is None:
                continue

            close_entry = float(close_entry_row[0])
            close_last_stab = float(close_last_stab_row[0])
            if close_last_stab == 0.0:
                continue

            attrs_row = rc_conn.execute(
                """
                SELECT
                  json_extract(state_attrs_json, '$.decline_profile') AS decline_profile,
                  json_extract(state_attrs_json, '$.entry_quality') AS entry_quality
                FROM rc_state_daily
                WHERE ticker = ?
                  AND date = ?
                LIMIT 1
                """,
                (ticker, entry_date),
            ).fetchone()
            decline_profile = "NULL"
            entry_quality = "NULL"
            if attrs_row is not None:
                if attrs_row[0] is not None:
                    decline_profile = str(attrs_row[0])
                if attrs_row[1] is not None:
                    entry_quality = str(attrs_row[1])

            r_stab_to_entry_pct = 100.0 * (close_entry / close_last_stab - 1.0)
            score_raw_z = FASTPASS_V1_USA_SMALL_BETA0
            score_raw_z += FASTPASS_V1_USA_SMALL_BETAS["r_stab_to_entry_pct"] * r_stab_to_entry_pct
            score_raw_z += FASTPASS_V1_USA_SMALL_BETAS.get(
                f"decline_profile_{decline_profile}", 0.0
            )
            score_raw_z += FASTPASS_V1_USA_SMALL_BETAS.get(
                f"entry_quality_{entry_quality}", 0.0
            )
            ew_score_fastpass = _sigmoid(score_raw_z)
            ew_level_fastpass = 1 if ew_score_fastpass >= FASTPASS_V1_USA_SMALL_THRESHOLD else 0

            inputs_payload = {
                "beta0": FASTPASS_V1_USA_SMALL_BETA0,
                "close_entry": close_entry,
                "close_last_stab": close_last_stab,
                "decline_profile": decline_profile,
                "entry_date": entry_date,
                "entry_quality": entry_quality,
                "last_stab_date": last_stab_date,
                "r_stab_to_entry_pct": r_stab_to_entry_pct,
                "rule_id": EW_SCORE_FASTPASS_V1_USA_SMALL,
                "score_raw_z": score_raw_z,
                "threshold": FASTPASS_V1_USA_SMALL_THRESHOLD,
            }
            inputs_json = json.dumps(inputs_payload, sort_keys=True)

            target_repo.upsert_fastpass_row(
                ticker=ticker,
                date=as_of_date,
                ew_score_fastpass=ew_score_fastpass,
                ew_level_fastpass=ew_level_fastpass,
                ew_rule=EW_SCORE_FASTPASS_V1_USA_SMALL,
                inputs_json=inputs_json,
            )
            stored_fastpass += 1

            if print_rows:
                print(
                    f"{ticker} | {ew_level_fastpass} | {ew_score_fastpass:.6f} | "
                    f"{r_stab_to_entry_pct:.6f} | {entry_date}"
                )
        return stored_fastpass

    model = load_model_config(rule_id)

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
            SELECT pvm, close
            FROM osakedata
            WHERE osake = ?
              AND pvm >= ?
              AND pvm <= ?
            ORDER BY pvm ASC
            """,
            (ticker, entry_window_date, end_date),
        ).fetchall()
        if not px_rows:
            continue

        rows_total = len(px_rows)
        pvm_day0 = str(px_rows[0][0])
        pvm_today = str(px_rows[-1][0])
        close_day0 = float(px_rows[0][1])
        close_today = float(px_rows[-1][1])
        if close_day0 == 0.0:
            continue
        r_prefix_pct = 100.0 * (close_today / close_day0 - 1.0)
        ew_score_day3 = _sigmoid(model.beta0 + model.beta1 * r_prefix_pct)
        if rows_total < 4:
            ew_level_day3 = 0
            if model.level3_score_threshold is not None and ew_score_day3 >= model.level3_score_threshold:
                ew_level_day3 = 1
        else:
            ew_level_day3 = 2
            if model.level3_score_threshold is not None and ew_score_day3 >= model.level3_score_threshold:
                ew_level_day3 = 3

        inputs_payload = {
            "as_of_date": as_of_date,
            "beta0": model.beta0,
            "beta1": model.beta1,
            "close_day0": close_day0,
            "close_today": close_today,
            "entry_window_date": entry_window_date,
            "entry_window_exit_date": entry_window_exit_date,
            "pvm_day0": pvm_day0,
            "pvm_today": pvm_today,
            "r_prefix_pct": r_prefix_pct,
            "rows_total": rows_total,
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


def compute_and_store_ew_scores_range(
    rc_conn: sqlite3.Connection,
    osakedata_conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    rule_id: str,
    print_rows: bool = False,
) -> int:
    d0 = date.fromisoformat(date_from)
    d1 = date.fromisoformat(date_to)
    if d1 < d0:
        raise ValueError("date_to must be >= date_from")

    total = 0
    d = d0
    while d <= d1:
        as_of = d.isoformat()
        if print_rows:
            print(f"DATE {as_of}")
        total += compute_and_store_ew_scores(
            rc_conn=rc_conn,
            osakedata_conn=osakedata_conn,
            as_of_date=as_of,
            rule_id=rule_id,
            repo=None,
            print_rows=print_rows,
        )
        d = d + timedelta(days=1)
    return total
