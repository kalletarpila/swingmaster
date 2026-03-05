from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

DUAL_CURRENT_TABLE = "rc_episode_model_dual_inference_current"
UP20_SOURCE_TABLE = "rc_episode_model_inference_rank_meta_v1"
FAIL10_SOURCE_TABLE = "rc_episode_model_full_inference_no_dow_scores_hgb_fail10"
EW_SCORE_TABLE = "rc_ew_score_daily"
TX_SIMU_TABLE = "rc_transactions_simu"

DEFAULT_MODEL_VERSION = "DUAL_META_V1_HGB_FAIL10_FROZEN_TRAIN_2020_2021"
DEFAULT_TRAIN_YEAR_FROM = 2020
DEFAULT_TRAIN_YEAR_TO = 2021
DEFAULT_OSAKEDATA_DB = "/home/kalle/projects/rawcandle/data/osakedata.db"

FEATURE_COLUMNS = [
    "ew_score_fastpass",
    "ew_level_fastpass",
    "exit_state_pass",
    "exit_state_no_trade",
    "buy_true",
    "ew_confirm_above_5",
    "ew_confirm_confirmed",
    "badge_penny_stock",
    "badge_low_volume",
    "badge_bull_div_last_20_days",
    "badge_det_slow_structural",
    "badge_det_slow_soft",
    "badge_det_trend_structural",
    "badge_det_trend_soft",
    "close_at_entry",
    "close_at_ew_start",
    "close_at_ew_exit",
    "days_entry_to_ew_trading",
    "days_in_entry_window_trading",
    "pipe_min_sma3",
    "pipe_max_sma3",
    "pre40_min_sma5",
    "pre40_max_sma5",
    "r_entry_to_ew_start_pct",
    "r_ew_start_to_exit_pct",
    "pipe_range_pct",
    "pre40_range_pct",
    "stock_close_exit_day",
    "stock_volume_exit_day",
    "stock_r_5d_pct",
    "stock_r_10d_pct",
    "stock_r_20d_pct",
    "stock_r_60d_pct",
    "stock_vs_ma20_pct",
    "stock_vs_ma50_pct",
    "stock_vs_ma200_pct",
    "stock_volume_vs_20d",
    "gspc_r_5d_pct",
    "gspc_r_20d_pct",
    "gspc_vs_ma200_pct",
    "ndx_r_5d_pct",
    "ndx_r_20d_pct",
    "ndx_vs_ma200_pct",
]


@dataclass(frozen=True)
class DualProductionSummary:
    rows_scored: int
    rows_with_labels: int
    rows_train_full: int
    rows_train_pass_only: int
    rows_train_fail10: int
    rows_changed_up20_source: int
    rows_changed_fail10_source: int
    rows_changed_dual_current: int
    model_version: str


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _safe_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce")
    den = pd.to_numeric(denominator, errors="coerce")
    out = ((num - den) / den) * 100.0
    valid = den.notna() & (den != 0.0)
    return out.where(valid, 0.0).replace([np.inf, -np.inf], 0.0).fillna(0.0)


def ensure_dual_source_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {UP20_SOURCE_TABLE} (
          episode_id TEXT PRIMARY KEY,
          score_meta_v1_up20_60d_close REAL NOT NULL,
          model_version TEXT NOT NULL,
          computed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {FAIL10_SOURCE_TABLE} (
          episode_id TEXT PRIMARY KEY,
          score_pred REAL NOT NULL,
          model_version TEXT NOT NULL,
          computed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DUAL_CURRENT_TABLE} (
          episode_id TEXT PRIMARY KEY,
          score_up20_meta_v1 REAL NOT NULL,
          score_fail10_60d_close_hgb REAL NOT NULL,
          model_version TEXT NOT NULL,
          computed_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _prepare_episode_map_temp(conn: sqlite3.Connection, frame: pd.DataFrame) -> None:
    conn.execute("DROP TABLE IF EXISTS temp._dual_episode_map")
    conn.execute(
        """
        CREATE TEMP TABLE _dual_episode_map (
          episode_id TEXT PRIMARY KEY,
          ticker TEXT NOT NULL,
          exit_date TEXT NOT NULL
        )
        """
    )
    payload = [
        (str(ep), str(t), str(d))
        for ep, t, d in frame[["episode_id", "ticker", "exit_date_str"]].itertuples(index=False, name=None)
    ]
    conn.executemany(
        "INSERT INTO temp._dual_episode_map (episode_id, ticker, exit_date) VALUES (?, ?, ?)",
        payload,
    )
    conn.commit()


def _enrich_with_fastpass_and_badges(conn: sqlite3.Connection, frame: pd.DataFrame) -> pd.DataFrame:
    has_ew = _table_exists(conn, EW_SCORE_TABLE)
    has_tx = _table_exists(conn, TX_SIMU_TABLE)
    if not has_ew and not has_tx:
        out = frame.copy()
        out["ew_score_fastpass"] = 0.0
        out["ew_level_fastpass"] = 0.0
        out["buy_true"] = 0.0
        out["badge_penny_stock"] = 0.0
        out["badge_low_volume"] = 0.0
        out["badge_bull_div_last_20_days"] = 0.0
        out["badge_det_slow_structural"] = 0.0
        out["badge_det_slow_soft"] = 0.0
        out["badge_det_trend_structural"] = 0.0
        out["badge_det_trend_soft"] = 0.0
        return out

    _prepare_episode_map_temp(conn, frame)
    ew_score_expr = "COALESCE(MAX(es.ew_score_fastpass), 0.0)" if has_ew else "0.0"
    ew_level_expr = "COALESCE(MAX(es.ew_level_fastpass), 0)" if has_ew else "0"
    buy_true_expr = "CASE WHEN COUNT(t.id) > 0 THEN 1 ELSE 0 END" if has_tx else "0"
    badge_penny_expr = "MAX(CASE WHEN t.buy_badges LIKE '%\"PENNY_STOCK\"%' THEN 1 ELSE 0 END)" if has_tx else "0"
    badge_low_expr = "MAX(CASE WHEN t.buy_badges LIKE '%\"LOW_VOLUME\"%' THEN 1 ELSE 0 END)" if has_tx else "0"
    badge_bull_div_expr = (
        "MAX(CASE WHEN t.buy_badges LIKE '%\"BULL_DIV_IN_LAST_20_DAYS\"%' THEN 1 ELSE 0 END)"
        if has_tx
        else "0"
    )
    badge_slow_struct_expr = (
        "MAX(CASE WHEN t.buy_badges LIKE '%\"downtrend_entry_type=SLOW_STRUCTURAL\"%' THEN 1 ELSE 0 END)"
        if has_tx
        else "0"
    )
    badge_slow_soft_expr = (
        "MAX(CASE WHEN t.buy_badges LIKE '%\"downtrend_entry_type=SLOW_SOFT\"%' THEN 1 ELSE 0 END)"
        if has_tx
        else "0"
    )
    badge_trend_struct_expr = (
        "MAX(CASE WHEN t.buy_badges LIKE '%\"downtrend_entry_type=TREND_STRUCTURAL\"%' THEN 1 ELSE 0 END)"
        if has_tx
        else "0"
    )
    badge_trend_soft_expr = (
        "MAX(CASE WHEN t.buy_badges LIKE '%\"downtrend_entry_type=TREND_SOFT\"%' THEN 1 ELSE 0 END)"
        if has_tx
        else "0"
    )
    ew_join = (
        f"LEFT JOIN {EW_SCORE_TABLE} es ON es.ticker=e.ticker AND es.date=e.exit_date"
        if has_ew
        else ""
    )
    tx_join = (
        f"LEFT JOIN {TX_SIMU_TABLE} t ON t.ticker=e.ticker AND t.buy_date=e.exit_date"
        if has_tx
        else ""
    )
    sql = f"""
    SELECT
      e.episode_id,
      {ew_score_expr} AS ew_score_fastpass,
      {ew_level_expr} AS ew_level_fastpass,
      {buy_true_expr} AS buy_true,
      {badge_penny_expr} AS badge_penny_stock,
      {badge_low_expr} AS badge_low_volume,
      {badge_bull_div_expr} AS badge_bull_div_last_20_days,
      {badge_slow_struct_expr} AS badge_det_slow_structural,
      {badge_slow_soft_expr} AS badge_det_slow_soft,
      {badge_trend_struct_expr} AS badge_det_trend_structural,
      {badge_trend_soft_expr} AS badge_det_trend_soft
    FROM temp._dual_episode_map e
    {ew_join}
    {tx_join}
    GROUP BY e.episode_id
    """
    rows = conn.execute(sql).fetchall()
    cols = [
        "episode_id",
        "ew_score_fastpass",
        "ew_level_fastpass",
        "buy_true",
        "badge_penny_stock",
        "badge_low_volume",
        "badge_bull_div_last_20_days",
        "badge_det_slow_structural",
        "badge_det_slow_soft",
        "badge_det_trend_structural",
        "badge_det_trend_soft",
    ]
    feats = pd.DataFrame.from_records(rows, columns=cols)
    out = frame.merge(feats, on="episode_id", how="left")
    for col in cols[1:]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def _attach_osakedata(conn: sqlite3.Connection, osakedata_db_path: str) -> None:
    try:
        conn.execute("DETACH DATABASE osdual")
    except sqlite3.Error:
        pass
    conn.execute("ATTACH DATABASE ? AS osdual", (osakedata_db_path,))


def _enrich_with_market_features(
    conn: sqlite3.Connection,
    frame: pd.DataFrame,
    *,
    osakedata_db_path: str,
) -> pd.DataFrame:
    _prepare_episode_map_temp(conn, frame)
    _attach_osakedata(conn, osakedata_db_path)

    conn.execute("DROP TABLE IF EXISTS temp._dual_symbols")
    conn.execute("CREATE TEMP TABLE _dual_symbols (ticker TEXT PRIMARY KEY)")
    symbols = sorted(set(frame["ticker"].astype(str).tolist()) | {"^GSPC", "^NDX"})
    conn.executemany(
        "INSERT INTO temp._dual_symbols (ticker) VALUES (?)",
        [(s,) for s in symbols],
    )

    conn.execute("DROP TABLE IF EXISTS temp._dual_market_features")
    conn.execute(
        """
        CREATE TEMP TABLE _dual_market_features AS
        SELECT
          o.osake AS ticker,
          o.pvm AS date,
          CAST(o.close AS REAL) AS close,
          CAST(o.volume AS REAL) AS volume,
          LAG(CAST(o.close AS REAL), 5) OVER (PARTITION BY o.osake ORDER BY o.pvm) AS close_lag5,
          LAG(CAST(o.close AS REAL), 10) OVER (PARTITION BY o.osake ORDER BY o.pvm) AS close_lag10,
          LAG(CAST(o.close AS REAL), 20) OVER (PARTITION BY o.osake ORDER BY o.pvm) AS close_lag20,
          LAG(CAST(o.close AS REAL), 60) OVER (PARTITION BY o.osake ORDER BY o.pvm) AS close_lag60,
          AVG(CAST(o.close AS REAL)) OVER (
            PARTITION BY o.osake ORDER BY o.pvm ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
          ) AS ma20,
          AVG(CAST(o.close AS REAL)) OVER (
            PARTITION BY o.osake ORDER BY o.pvm ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
          ) AS ma50,
          AVG(CAST(o.close AS REAL)) OVER (
            PARTITION BY o.osake ORDER BY o.pvm ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
          ) AS ma200,
          AVG(CAST(o.volume AS REAL)) OVER (
            PARTITION BY o.osake ORDER BY o.pvm ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
          ) AS vol_ma20
        FROM osdual.osakedata o
        JOIN temp._dual_symbols s
          ON s.ticker = o.osake
        """
    )

    pct = lambda n, d: f"CASE WHEN {d} IS NULL OR {d}=0 THEN 0.0 ELSE (({n}-{d})/{d})*100.0 END"
    sql = f"""
    SELECT
      e.episode_id,
      COALESCE(st.close, 0.0) AS stock_close_exit_day,
      COALESCE(st.volume, 0.0) AS stock_volume_exit_day,
      {pct('st.close', 'st.close_lag5')} AS stock_r_5d_pct,
      {pct('st.close', 'st.close_lag10')} AS stock_r_10d_pct,
      {pct('st.close', 'st.close_lag20')} AS stock_r_20d_pct,
      {pct('st.close', 'st.close_lag60')} AS stock_r_60d_pct,
      {pct('st.close', 'st.ma20')} AS stock_vs_ma20_pct,
      {pct('st.close', 'st.ma50')} AS stock_vs_ma50_pct,
      {pct('st.close', 'st.ma200')} AS stock_vs_ma200_pct,
      CASE WHEN st.vol_ma20 IS NULL OR st.vol_ma20=0 THEN 0.0 ELSE st.volume/st.vol_ma20 END AS stock_volume_vs_20d,
      {pct('g.close', 'g.close_lag5')} AS gspc_r_5d_pct,
      {pct('g.close', 'g.close_lag20')} AS gspc_r_20d_pct,
      {pct('g.close', 'g.ma200')} AS gspc_vs_ma200_pct,
      {pct('n.close', 'n.close_lag5')} AS ndx_r_5d_pct,
      {pct('n.close', 'n.close_lag20')} AS ndx_r_20d_pct,
      {pct('n.close', 'n.ma200')} AS ndx_vs_ma200_pct
    FROM temp._dual_episode_map e
    LEFT JOIN temp._dual_market_features st
      ON st.ticker = e.ticker AND st.date = e.exit_date
    LEFT JOIN temp._dual_market_features g
      ON g.ticker = '^GSPC' AND g.date = e.exit_date
    LEFT JOIN temp._dual_market_features n
      ON n.ticker = '^NDX' AND n.date = e.exit_date
    """
    rows = conn.execute(sql).fetchall()
    cols = [
        "episode_id",
        "stock_close_exit_day",
        "stock_volume_exit_day",
        "stock_r_5d_pct",
        "stock_r_10d_pct",
        "stock_r_20d_pct",
        "stock_r_60d_pct",
        "stock_vs_ma20_pct",
        "stock_vs_ma50_pct",
        "stock_vs_ma200_pct",
        "stock_volume_vs_20d",
        "gspc_r_5d_pct",
        "gspc_r_20d_pct",
        "gspc_vs_ma200_pct",
        "ndx_r_5d_pct",
        "ndx_r_20d_pct",
        "ndx_vs_ma200_pct",
    ]
    feats = pd.DataFrame.from_records(rows, columns=cols)
    try:
        conn.execute("DETACH DATABASE osdual")
    except sqlite3.Error:
        pass
    out = frame.merge(feats, on="episode_id", how="left")
    for col in cols[1:]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    return out


def _load_episode_frame(conn: sqlite3.Connection, *, osakedata_db_path: str) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT
          episode_id,
          ticker,
          entry_window_exit_date,
          entry_window_exit_state,
          close_at_entry,
          close_at_ew_start,
          close_at_ew_exit,
          days_entry_to_ew_trading,
          days_in_entry_window_trading,
          pipe_min_sma3,
          pipe_max_sma3,
          pre40_min_sma5,
          pre40_max_sma5,
          ew_confirm_above_5,
          ew_confirm_confirmed,
          post60_growth_pct_close_ew_exit_to_peak
        FROM rc_pipeline_episode
        WHERE episode_id IS NOT NULL
          AND entry_window_exit_date IS NOT NULL
        ORDER BY entry_window_exit_date ASC, ticker ASC
        """
    ).fetchall()
    cols = [
        "episode_id",
        "ticker",
        "entry_window_exit_date",
        "entry_window_exit_state",
        "close_at_entry",
        "close_at_ew_start",
        "close_at_ew_exit",
        "days_entry_to_ew_trading",
        "days_in_entry_window_trading",
        "pipe_min_sma3",
        "pipe_max_sma3",
        "pre40_min_sma5",
        "pre40_max_sma5",
        "ew_confirm_above_5",
        "ew_confirm_confirmed",
        "post60_growth_pct_close_ew_exit_to_peak",
    ]
    frame = pd.DataFrame.from_records(rows, columns=cols)
    if frame.empty:
        return frame

    frame["entry_window_exit_date"] = pd.to_datetime(frame["entry_window_exit_date"], errors="coerce")
    frame["exit_date_str"] = frame["entry_window_exit_date"].dt.strftime("%Y-%m-%d")
    frame["year"] = frame["entry_window_exit_date"].dt.year
    frame["exit_state_pass"] = (frame["entry_window_exit_state"] == "PASS").astype(int)
    frame["exit_state_no_trade"] = (frame["entry_window_exit_state"] == "NO_TRADE").astype(int)
    frame["r_entry_to_ew_start_pct"] = _safe_pct(frame["close_at_ew_start"], frame["close_at_entry"])
    frame["r_ew_start_to_exit_pct"] = _safe_pct(frame["close_at_ew_exit"], frame["close_at_ew_start"])
    frame["pipe_range_pct"] = _safe_pct(frame["pipe_max_sma3"], frame["pipe_min_sma3"])
    frame["pre40_range_pct"] = _safe_pct(frame["pre40_max_sma5"], frame["pre40_min_sma5"])
    frame["label_up20"] = (
        pd.to_numeric(frame["post60_growth_pct_close_ew_exit_to_peak"], errors="coerce") >= 20.0
    ).astype(float)
    frame["label_fail10"] = (
        pd.to_numeric(frame["post60_growth_pct_close_ew_exit_to_peak"], errors="coerce") < 10.0
    ).astype(float)

    frame = _enrich_with_fastpass_and_badges(conn, frame)
    frame = _enrich_with_market_features(conn, frame, osakedata_db_path=osakedata_db_path)

    for col in FEATURE_COLUMNS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
    return frame


def _assert_binary_target(values: pd.Series, label_name: str) -> None:
    uniq = sorted(set(values.dropna().astype(int).tolist()))
    if uniq != [0, 1]:
        raise RuntimeError(f"{label_name}_TRAIN_LABELS_NOT_BINARY")


def _write_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: tuple[str, ...],
    rows: Iterable[tuple[object, ...]],
    mode: str,
) -> int:
    if mode == "replace-all":
        conn.execute(f"DELETE FROM {table}")
    payload = list(rows)
    if not payload:
        conn.commit()
        return 0
    before_changes = conn.total_changes
    if mode == "insert-missing":
        placeholders = ", ".join(["?"] * len(columns))
        conn.executemany(
            f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            payload,
        )
    else:
        placeholders = ", ".join(["?"] * len(columns))
        update_assignments = ", ".join(f"{col}=excluded.{col}" for col in columns[1:])
        conn.executemany(
            f"""
            INSERT INTO {table} ({", ".join(columns)}) VALUES ({placeholders})
            ON CONFLICT(episode_id) DO UPDATE SET
              {update_assignments}
            """,
            payload,
        )
    conn.commit()
    return conn.total_changes - before_changes


def compute_and_store_dual_scores_production(
    conn: sqlite3.Connection,
    *,
    osakedata_db_path: str = DEFAULT_OSAKEDATA_DB,
    mode: str = "upsert",
    model_version: str = DEFAULT_MODEL_VERSION,
    computed_at: str | None = None,
    train_year_from: int = DEFAULT_TRAIN_YEAR_FROM,
    train_year_to: int = DEFAULT_TRAIN_YEAR_TO,
) -> DualProductionSummary:
    if mode not in {"upsert", "replace-all", "insert-missing"}:
        raise ValueError("mode must be one of: upsert, replace-all, insert-missing")
    if not osakedata_db_path:
        raise RuntimeError("OSAKEDATA_DB_REQUIRED")

    ensure_dual_source_tables(conn)
    frame = _load_episode_frame(conn, osakedata_db_path=osakedata_db_path)
    if frame.empty:
        return DualProductionSummary(
            rows_scored=0,
            rows_with_labels=0,
            rows_train_full=0,
            rows_train_pass_only=0,
            rows_train_fail10=0,
            rows_changed_up20_source=0,
            rows_changed_fail10_source=0,
            rows_changed_dual_current=0,
            model_version=model_version,
        )

    label_mask = frame["post60_growth_pct_close_ew_exit_to_peak"].notna()
    rows_with_labels = int(label_mask.sum())
    train_mask = label_mask & (frame["year"] >= train_year_from) & (frame["year"] <= train_year_to)

    full_train = frame.loc[train_mask].copy()
    pass_train = frame.loc[train_mask & (frame["entry_window_exit_state"] == "PASS")].copy()
    fail_train = frame.loc[train_mask].copy()
    if len(full_train) < 20:
        raise RuntimeError("UP20_FULL_TRAIN_TOO_SMALL")
    if len(pass_train) < 20:
        raise RuntimeError("UP20_PASS_ONLY_TRAIN_TOO_SMALL")
    if len(fail_train) < 20:
        raise RuntimeError("FAIL10_TRAIN_TOO_SMALL")

    _assert_binary_target(full_train["label_up20"], "UP20_FULL")
    _assert_binary_target(pass_train["label_up20"], "UP20_PASS_ONLY")
    _assert_binary_target(fail_train["label_fail10"], "FAIL10")

    from catboost import CatBoostClassifier

    up_full_model = CatBoostClassifier(
        iterations=500,
        depth=5,
        learning_rate=0.05,
        loss_function="Logloss",
        eval_metric="PRAUC",
        random_seed=42,
        verbose=False,
    )
    up_full_model.fit(full_train[FEATURE_COLUMNS], full_train["label_up20"].astype(int))

    up_pass_model = CatBoostClassifier(
        iterations=500,
        depth=5,
        learning_rate=0.05,
        loss_function="Logloss",
        eval_metric="PRAUC",
        random_seed=42,
        verbose=False,
    )
    up_pass_model.fit(pass_train[FEATURE_COLUMNS], pass_train["label_up20"].astype(int))

    fail_model = HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_depth=3,
        max_iter=300,
        min_samples_leaf=20,
        random_state=42,
    )
    fail_model.fit(fail_train[FEATURE_COLUMNS], fail_train["label_fail10"].astype(int))

    score_full = up_full_model.predict_proba(frame[FEATURE_COLUMNS])[:, 1]
    score_fail10 = fail_model.predict_proba(frame[FEATURE_COLUMNS])[:, 1]
    pass_mask = frame["entry_window_exit_state"] == "PASS"
    score_meta = np.array(score_full, copy=True)
    if pass_mask.any():
        score_meta[pass_mask.to_numpy()] = up_pass_model.predict_proba(frame.loc[pass_mask, FEATURE_COLUMNS])[:, 1]

    ts = computed_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    up20_rows = [
        (str(ep), float(s), model_version, ts)
        for ep, s in zip(frame["episode_id"].tolist(), score_meta.tolist())
    ]
    fail_rows = [
        (str(ep), float(s), model_version, ts)
        for ep, s in zip(frame["episode_id"].tolist(), score_fail10.tolist())
    ]
    dual_rows = [
        (str(ep), float(su), float(sf), model_version, ts)
        for ep, su, sf in zip(frame["episode_id"].tolist(), score_meta.tolist(), score_fail10.tolist())
    ]

    up_changes = _write_rows(
        conn,
        UP20_SOURCE_TABLE,
        ("episode_id", "score_meta_v1_up20_60d_close", "model_version", "computed_at"),
        up20_rows,
        mode,
    )
    fail_changes = _write_rows(
        conn,
        FAIL10_SOURCE_TABLE,
        ("episode_id", "score_pred", "model_version", "computed_at"),
        fail_rows,
        mode,
    )
    dual_changes = _write_rows(
        conn,
        DUAL_CURRENT_TABLE,
        (
            "episode_id",
            "score_up20_meta_v1",
            "score_fail10_60d_close_hgb",
            "model_version",
            "computed_at",
        ),
        dual_rows,
        mode,
    )

    return DualProductionSummary(
        rows_scored=len(frame),
        rows_with_labels=rows_with_labels,
        rows_train_full=len(full_train),
        rows_train_pass_only=len(pass_train),
        rows_train_fail10=len(fail_train),
        rows_changed_up20_source=up_changes,
        rows_changed_fail10_source=fail_changes,
        rows_changed_dual_current=dual_changes,
        model_version=model_version,
    )
