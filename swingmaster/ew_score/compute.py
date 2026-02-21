from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, timedelta
from typing import Any

from swingmaster.ew_score.model_config import load_model_config
from swingmaster.ew_score.repo import RcEwScoreDailyRepo

EW_SCORE_FASTPASS_V1_USA_SMALL = "EW_SCORE_FASTPASS_V1_USA_SMALL"
EW_SCORE_FASTPASS_V1_SE = "EW_SCORE_FASTPASS_V1_SE"
EW_SCORE_ROLLING_V2_FIN = "EW_SCORE_ROLLING_V2_FIN"
EW_SCORE_ROLLING_V2_SE = "EW_SCORE_ROLLING_V2_SE"
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
FASTPASS_V1_SE_BETA0 = -0.36240123039204225
FASTPASS_V1_SE_THRESHOLD = 0.65
FASTPASS_V1_SE_BETAS = {
    "r_stab_to_entry_pct": 0.4235956974235532,
    "downtrend_origin_SLOW": -0.5567538554589132,
    "downtrend_origin_TREND": 0.19435262506683273,
    "downtrend_entry_type_SLOW_SOFT": -0.29027452378914076,
    "downtrend_entry_type_SLOW_STRUCTURAL": -0.2664793316697285,
    "downtrend_entry_type_TREND_SOFT": 0.6865754008841269,
    "downtrend_entry_type_TREND_STRUCTURAL": -0.49222277581728685,
    "decline_profile_SHARP_SELL_OFF": -5.003954179480298,
    "decline_profile_SLOW_DRIFT": 2.290586835702952,
    "decline_profile_STRUCTURAL_DOWNTREND": 1.367367242431005,
    "decline_profile_UNKNOWN": 0.9835988709542381,
    "stabilization_phase_EARLY_REVERSAL": -0.3624012303920593,
    "entry_gate_EARLY_STAB_MA20": 0.1382594006923378,
    "entry_gate_EARLY_STAB_MA20_HL": 0.11815151647220329,
    "entry_gate_LEGACY_ENTRY_SETUP_VALID": -0.6188121475565751,
    "entry_quality_A": 0.11815151647220329,
    "entry_quality_B": 0.1382594006923378,
    "entry_quality_LEGACY": -0.6188121475565969,
}


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _resolve_market_for_ticker(
    osakedata_conn: sqlite3.Connection, ticker: str, as_of_date: str
) -> str | None:
    row = osakedata_conn.execute(
        """
        SELECT market
        FROM osakedata
        WHERE osake = ?
          AND pvm <= ?
        ORDER BY pvm DESC
        LIMIT 1
        """,
        (ticker, as_of_date),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0]).lower()


def _level_from_rows_total(score: float, threshold: float, rows_total: int) -> int:
    if rows_total < 4:
        return 1 if score >= threshold else 0
    return 3 if score >= threshold else 2


def _score_fastpass_v1_se(
    r_stab_to_entry_pct: float,
    downtrend_origin: str,
    downtrend_entry_type: str,
    decline_profile: str,
    stabilization_phase: str,
    entry_gate: str,
    entry_quality: str,
) -> tuple[float, float]:
    z = FASTPASS_V1_SE_BETA0
    z += FASTPASS_V1_SE_BETAS["r_stab_to_entry_pct"] * r_stab_to_entry_pct
    z += FASTPASS_V1_SE_BETAS.get(f"downtrend_origin_{downtrend_origin}", 0.0)
    z += FASTPASS_V1_SE_BETAS.get(f"downtrend_entry_type_{downtrend_entry_type}", 0.0)
    z += FASTPASS_V1_SE_BETAS.get(f"decline_profile_{decline_profile}", 0.0)
    z += FASTPASS_V1_SE_BETAS.get(f"stabilization_phase_{stabilization_phase}", 0.0)
    z += FASTPASS_V1_SE_BETAS.get(f"entry_gate_{entry_gate}", 0.0)
    z += FASTPASS_V1_SE_BETAS.get(f"entry_quality_{entry_quality}", 0.0)
    return z, _sigmoid(z)


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

    if print_rows:
        print("EW_SCORE_DAILY")
        print("ticker | ew_level_day3 | ew_score_day3 | r_prefix_pct | entry_window_date")

    rolling_rule_by_market = {
        "omxh": EW_SCORE_ROLLING_V2_FIN,
        "omxs": EW_SCORE_ROLLING_V2_SE,
    }
    model_cache: dict[str, Any] = {}

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
        market = _resolve_market_for_ticker(osakedata_conn, ticker, as_of_date)
        routed = False

        if market in rolling_rule_by_market:
            rolling_rule = rolling_rule_by_market[market]
            if rolling_rule not in model_cache:
                model_cache[rolling_rule] = load_model_config(rolling_rule)
            rolling_model = model_cache[rolling_rule]

            rolling_px_rows = osakedata_conn.execute(
                """
                SELECT pvm, close
                FROM osakedata
                WHERE osake = ?
                  AND market = ?
                  AND pvm >= ?
                  AND pvm <= ?
                ORDER BY pvm ASC
                """,
                (ticker, market, entry_window_date, as_of_date),
            ).fetchall()
            if rolling_px_rows and rolling_model.level3_score_threshold is not None:
                rows_total = len(rolling_px_rows)
                close_day0 = float(rolling_px_rows[0][1])
                close_today = float(rolling_px_rows[-1][1])
                if close_day0 != 0.0:
                    r_prefix_pct = 100.0 * (close_today / close_day0 - 1.0)
                    score_raw_z = rolling_model.beta0 + rolling_model.beta1 * r_prefix_pct
                    ew_score_rolling = _sigmoid(score_raw_z)
                    ew_level_rolling = _level_from_rows_total(
                        ew_score_rolling,
                        float(rolling_model.level3_score_threshold),
                        rows_total,
                    )
                    rolling_inputs_json = json.dumps(
                        {
                            "as_of_date": as_of_date,
                            "beta0": rolling_model.beta0,
                            "beta1": rolling_model.beta1,
                            "close_day0": close_day0,
                            "close_today": close_today,
                            "entry_date": entry_window_date,
                            "r_prefix_pct": r_prefix_pct,
                            "rows_total": rows_total,
                            "rule_id": rolling_model.rule_id,
                            "score_raw_z": score_raw_z,
                            "threshold": rolling_model.level3_score_threshold,
                        },
                        sort_keys=True,
                    )
                    target_repo.upsert_rolling_row(
                        ticker=ticker,
                        date=as_of_date,
                        ew_score_rolling=ew_score_rolling,
                        ew_level_rolling=ew_level_rolling,
                        ew_rule_rolling=rolling_model.rule_id,
                        inputs_json_rolling=rolling_inputs_json,
                    )
                    stored += 1
                    routed = True

        if market == "usa":
            last_stab_row = rc_conn.execute(
                """
                SELECT MAX(date)
                FROM rc_state_daily
                WHERE ticker = ?
                  AND date < ?
                  AND state = 'STABILIZING'
                """,
                (ticker, entry_window_date),
            ).fetchone()
            if last_stab_row is not None and last_stab_row[0] is not None:
                last_stab_date = str(last_stab_row[0])
                fastpass_px_rows = osakedata_conn.execute(
                    """
                    SELECT pvm, close
                    FROM osakedata
                    WHERE osake = ?
                      AND market = 'usa'
                      AND pvm >= ?
                      AND pvm <= ?
                    ORDER BY pvm ASC
                    """,
                    (ticker, entry_window_date, as_of_date),
                ).fetchall()
                close_last_stab_row = osakedata_conn.execute(
                    """
                    SELECT close
                    FROM osakedata
                    WHERE osake = ?
                      AND market = 'usa'
                      AND pvm = ?
                    LIMIT 1
                    """,
                    (ticker, last_stab_date),
                ).fetchone()
                if fastpass_px_rows and close_last_stab_row is not None:
                    rows_total = len(fastpass_px_rows)
                    close_entry = float(fastpass_px_rows[0][1])
                    close_last_stab = float(close_last_stab_row[0])
                    if close_last_stab != 0.0:
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
                            (ticker, entry_window_date),
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
                        ew_level_fastpass = _level_from_rows_total(
                            ew_score_fastpass,
                            FASTPASS_V1_USA_SMALL_THRESHOLD,
                            rows_total,
                        )

                        fastpass_inputs_json = json.dumps(
                            {
                                "beta0": FASTPASS_V1_USA_SMALL_BETA0,
                                "close_entry": close_entry,
                                "close_last_stab": close_last_stab,
                                "decline_profile": decline_profile,
                                "entry_date": entry_window_date,
                                "entry_quality": entry_quality,
                                "last_stab_date": last_stab_date,
                                "r_stab_to_entry_pct": r_stab_to_entry_pct,
                                "rows_total": rows_total,
                                "rule_id": EW_SCORE_FASTPASS_V1_USA_SMALL,
                                "score_raw_z": score_raw_z,
                                "threshold": FASTPASS_V1_USA_SMALL_THRESHOLD,
                            },
                            sort_keys=True,
                        )
                        target_repo.upsert_fastpass_row(
                            ticker=ticker,
                            date=as_of_date,
                            ew_score_fastpass=ew_score_fastpass,
                            ew_level_fastpass=ew_level_fastpass,
                            ew_rule=EW_SCORE_FASTPASS_V1_USA_SMALL,
                            inputs_json=fastpass_inputs_json,
                        )
                        stored += 1
                        routed = True

        if market == "omxs":
            last_stab_row = rc_conn.execute(
                """
                SELECT MAX(date)
                FROM rc_state_daily
                WHERE ticker = ?
                  AND date < ?
                  AND state = 'STABILIZING'
                """,
                (ticker, entry_window_date),
            ).fetchone()
            if last_stab_row is not None and last_stab_row[0] is not None:
                last_stab_date = str(last_stab_row[0])
                fastpass_px_rows = osakedata_conn.execute(
                    """
                    SELECT pvm, close
                    FROM osakedata
                    WHERE osake = ?
                      AND market = 'omxs'
                      AND pvm >= ?
                      AND pvm <= ?
                    ORDER BY pvm ASC
                    """,
                    (ticker, entry_window_date, as_of_date),
                ).fetchall()
                close_last_stab_row = osakedata_conn.execute(
                    """
                    SELECT close
                    FROM osakedata
                    WHERE osake = ?
                      AND market = 'omxs'
                      AND pvm = ?
                    LIMIT 1
                    """,
                    (ticker, last_stab_date),
                ).fetchone()
                if fastpass_px_rows and close_last_stab_row is not None:
                    rows_total = len(fastpass_px_rows)
                    close_entry = float(fastpass_px_rows[0][1])
                    close_last_stab = float(close_last_stab_row[0])
                    if close_last_stab != 0.0:
                        attrs_row = rc_conn.execute(
                            """
                            SELECT
                              json_extract(state_attrs_json, '$.downtrend_origin') AS downtrend_origin,
                              json_extract(state_attrs_json, '$.downtrend_entry_type') AS downtrend_entry_type,
                              json_extract(state_attrs_json, '$.decline_profile') AS decline_profile,
                              json_extract(state_attrs_json, '$.stabilization_phase') AS stabilization_phase,
                              json_extract(state_attrs_json, '$.entry_gate') AS entry_gate,
                              json_extract(state_attrs_json, '$.entry_quality') AS entry_quality
                            FROM rc_state_daily
                            WHERE ticker = ?
                              AND date = ?
                            LIMIT 1
                            """,
                            (ticker, entry_window_date),
                        ).fetchone()
                        downtrend_origin = "NULL"
                        downtrend_entry_type = "NULL"
                        decline_profile = "NULL"
                        stabilization_phase = "NULL"
                        entry_gate = "NULL"
                        entry_quality = "NULL"
                        if attrs_row is not None:
                            if attrs_row[0] is not None:
                                downtrend_origin = str(attrs_row[0])
                            if attrs_row[1] is not None:
                                downtrend_entry_type = str(attrs_row[1])
                            if attrs_row[2] is not None:
                                decline_profile = str(attrs_row[2])
                            if attrs_row[3] is not None:
                                stabilization_phase = str(attrs_row[3])
                            if attrs_row[4] is not None:
                                entry_gate = str(attrs_row[4])
                            if attrs_row[5] is not None:
                                entry_quality = str(attrs_row[5])

                        r_stab_to_entry_pct = 100.0 * (close_entry / close_last_stab - 1.0)
                        score_raw_z, ew_score_fastpass = _score_fastpass_v1_se(
                            r_stab_to_entry_pct=r_stab_to_entry_pct,
                            downtrend_origin=downtrend_origin,
                            downtrend_entry_type=downtrend_entry_type,
                            decline_profile=decline_profile,
                            stabilization_phase=stabilization_phase,
                            entry_gate=entry_gate,
                            entry_quality=entry_quality,
                        )
                        ew_level_fastpass = _level_from_rows_total(
                            ew_score_fastpass,
                            FASTPASS_V1_SE_THRESHOLD,
                            rows_total,
                        )
                        fastpass_inputs_json = json.dumps(
                            {
                                "beta0": FASTPASS_V1_SE_BETA0,
                                "close_entry": close_entry,
                                "close_last_stab": close_last_stab,
                                "decline_profile": decline_profile,
                                "downtrend_entry_type": downtrend_entry_type,
                                "downtrend_origin": downtrend_origin,
                                "entry_date": entry_window_date,
                                "entry_gate": entry_gate,
                                "entry_quality": entry_quality,
                                "last_stab_date": last_stab_date,
                                "r_stab_to_entry_pct": r_stab_to_entry_pct,
                                "rule_id": EW_SCORE_FASTPASS_V1_SE,
                                "score_raw_z": score_raw_z,
                                "stabilization_phase": stabilization_phase,
                                "threshold": FASTPASS_V1_SE_THRESHOLD,
                            },
                            sort_keys=True,
                        )
                        target_repo.upsert_fastpass_row(
                            ticker=ticker,
                            date=as_of_date,
                            ew_score_fastpass=ew_score_fastpass,
                            ew_level_fastpass=ew_level_fastpass,
                            ew_rule=EW_SCORE_FASTPASS_V1_SE,
                            inputs_json=fastpass_inputs_json,
                        )
                        stored += 1
                        routed = True

        if routed:
            continue

        model = load_model_config(rule_id)

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
