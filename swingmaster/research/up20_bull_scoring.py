from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from swingmaster.episode_exit_features.production import (
    DEFAULT_OSAKEDATA_DB,
    FEATURE_COLUMNS,
    compute_and_store_episode_exit_features,
)
from swingmaster.regime.production import DEFAULT_REGIME_VERSION
from swingmaster.research.up20_bull_evaluation import FEATURE_LIST_FILE_NAME
from swingmaster.research.up20_bull_training import MODEL_ID_HGB

SCORE_TABLE = "rc_episode_model_score"
REGIME_USED = "BULL"
TARGET_NAME = "UP20"
MODEL_FAMILY = "HGB"
FEATURE_VERSION = "EPISODE_EXIT_FEATURES_V1"


@dataclass(frozen=True)
class Up20BullScoreSummary:
    episodes_scanned: int
    episodes_eligible: int
    scores_inserted: int
    scores_updated: int
    scores_skipped: int
    model_id: str
    regime_used: str
    feature_count: int


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table_name: str, col: str) -> bool:
    if not _table_exists(conn, table_name):
        return False
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(r[1]) == col for r in rows)


def _required_schema_check(conn: sqlite3.Connection) -> None:
    required_tables = [
        "rc_pipeline_episode",
        "rc_episode_regime",
        "rc_episode_exit_features",
        SCORE_TABLE,
    ]
    for table in required_tables:
        if not _table_exists(conn, table):
            raise RuntimeError(f"TABLE_MISSING_{table}")
    required_pipeline_cols = ["episode_id", "ticker", "entry_window_date", "entry_window_exit_date"]
    for col in required_pipeline_cols:
        if not _column_exists(conn, "rc_pipeline_episode", col):
            raise RuntimeError(f"COLUMN_MISSING_rc_pipeline_episode_{col}")
    required_regime_cols = ["episode_id", "regime_version", "ew_exit_regime_combined"]
    for col in required_regime_cols:
        if not _column_exists(conn, "rc_episode_regime", col):
            raise RuntimeError(f"COLUMN_MISSING_rc_episode_regime_{col}")
    required_feature_cols = ["episode_id", "as_of_date", *FEATURE_COLUMNS]
    for col in required_feature_cols:
        if not _column_exists(conn, "rc_episode_exit_features", col):
            raise RuntimeError(f"COLUMN_MISSING_rc_episode_exit_features_{col}")
    required_score_cols = ["episode_id", "model_id", "predicted_probability", "scored_at"]
    for col in required_score_cols:
        if not _column_exists(conn, SCORE_TABLE, col):
            raise RuntimeError(f"COLUMN_MISSING_{SCORE_TABLE}_{col}")


def _load_feature_list(model_dir: Path) -> list[str]:
    path = model_dir / FEATURE_LIST_FILE_NAME
    if not path.exists():
        raise RuntimeError("UP20_BULL_FEATURE_LIST_MISSING")
    payload = json.loads(path.read_text(encoding="utf-8"))
    cols = payload.get("feature_columns")
    if not isinstance(cols, list) or not cols or not all(isinstance(c, str) for c in cols):
        raise RuntimeError("UP20_BULL_FEATURE_LIST_INVALID")
    if tuple(cols) != tuple(FEATURE_COLUMNS):
        raise RuntimeError("UP20_BULL_FEATURE_ORDER_MISMATCH")
    return cols


def _load_hgb_model(model_dir: Path) -> Any:
    model_path = model_dir / f"{MODEL_ID_HGB}.joblib"
    if not model_path.exists():
        raise RuntimeError("UP20_BULL_HGB_ARTIFACT_MISSING")
    model = joblib.load(str(model_path))
    if not hasattr(model, "predict_proba"):
        raise RuntimeError("UP20_BULL_HGB_PREDICT_PROBA_MISSING")
    return model


def _sql_date_filter(alias: str, *, date_from: str | None, date_to: str | None) -> tuple[str, list[object]]:
    where: list[str] = []
    params: list[object] = []
    if date_from is not None:
        where.append(f"{alias}.entry_window_exit_date >= ?")
        params.append(date_from)
    if date_to is not None:
        where.append(f"{alias}.entry_window_exit_date <= ?")
        params.append(date_to)
    if not where:
        return "", params
    return " AND " + " AND ".join(where), params


def _count_scanned_episodes(conn: sqlite3.Connection, *, date_from: str | None, date_to: str | None) -> int:
    date_sql, params = _sql_date_filter("p", date_from=date_from, date_to=date_to)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM rc_pipeline_episode p
        WHERE p.entry_window_exit_date IS NOT NULL
        {date_sql}
        """,
        params,
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _load_bull_episodes(
    conn: sqlite3.Connection,
    *,
    regime_version: str,
    date_from: str | None,
    date_to: str | None,
) -> pd.DataFrame:
    date_sql, params = _sql_date_filter("p", date_from=date_from, date_to=date_to)
    rows = conn.execute(
        f"""
        SELECT
          p.episode_id,
          p.ticker,
          p.entry_window_date,
          p.entry_window_exit_date
        FROM rc_pipeline_episode p
        JOIN rc_episode_regime r
          ON r.episode_id = p.episode_id
         AND r.regime_version = ?
        WHERE p.episode_id IS NOT NULL
          AND p.entry_window_exit_date IS NOT NULL
          AND r.ew_exit_regime_combined = 'BULL'
          {date_sql}
        ORDER BY p.entry_window_exit_date ASC, p.ticker ASC
        """,
        [regime_version, *params],
    ).fetchall()
    frame = pd.DataFrame.from_records(
        rows,
        columns=["episode_id", "ticker", "entry_window_date", "entry_window_exit_date"],
    )
    if frame.empty:
        return frame
    frame["entry_window_exit_date"] = pd.to_datetime(frame["entry_window_exit_date"], errors="coerce")
    frame = frame[frame["entry_window_exit_date"].notna()].copy()
    frame["entry_window_exit_date"] = frame["entry_window_exit_date"].dt.strftime("%Y-%m-%d")
    frame["entry_window_date"] = frame["entry_window_date"].astype("string")
    frame["entry_window_date"] = frame["entry_window_date"].where(frame["entry_window_date"].notna(), None)
    return frame.reset_index(drop=True)


def _load_scoring_rows(
    conn: sqlite3.Connection,
    *,
    feature_columns: list[str],
    regime_version: str,
    date_from: str | None,
    date_to: str | None,
) -> pd.DataFrame:
    feature_sql = ", ".join(f"f.{col}" for col in feature_columns)
    date_sql, params = _sql_date_filter("p", date_from=date_from, date_to=date_to)
    rows = conn.execute(
        f"""
        SELECT
          p.episode_id,
          p.ticker,
          p.entry_window_date,
          p.entry_window_exit_date,
          {feature_sql}
        FROM rc_pipeline_episode p
        JOIN rc_episode_regime r
          ON r.episode_id = p.episode_id
         AND r.regime_version = ?
        JOIN rc_episode_exit_features f
          ON f.episode_id = p.episode_id
         AND f.as_of_date = p.entry_window_exit_date
        WHERE p.episode_id IS NOT NULL
          AND p.entry_window_exit_date IS NOT NULL
          AND r.ew_exit_regime_combined = 'BULL'
          {date_sql}
        ORDER BY p.entry_window_exit_date ASC, p.ticker ASC
        """,
        [regime_version, *params],
    ).fetchall()
    frame = pd.DataFrame.from_records(
        rows,
        columns=["episode_id", "ticker", "entry_window_date", "entry_window_exit_date", *feature_columns],
    )
    if frame.empty:
        return frame
    for col in feature_columns:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def _build_missing_features_if_needed(
    conn: sqlite3.Connection,
    *,
    bull_count: int,
    scored_count: int,
    osakedata_db_path: str,
    date_from: str | None,
    date_to: str | None,
    computed_at: str,
) -> None:
    if scored_count >= bull_count:
        return
    compute_and_store_episode_exit_features(
        conn,
        osakedata_db_path=osakedata_db_path,
        mode="insert-missing",
        date_from=date_from,
        date_to=date_to,
        computed_at=computed_at,
    )


def _fetch_existing_episode_ids(
    conn: sqlite3.Connection,
    *,
    model_id: str,
    episode_ids: list[str],
) -> set[str]:
    if not episode_ids:
        return set()
    placeholders = ",".join("?" for _ in episode_ids)
    rows = conn.execute(
        f"""
        SELECT episode_id
        FROM {SCORE_TABLE}
        WHERE model_id=?
          AND episode_id IN ({placeholders})
        """,
        [model_id, *episode_ids],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _upsert_scores(
    conn: sqlite3.Connection,
    *,
    rows: list[tuple[object, ...]],
    model_id: str,
    mode: str,
) -> tuple[int, int]:
    if mode not in {"upsert", "replace-all", "insert-missing"}:
        raise ValueError(f"Unsupported mode: {mode}")
    if mode == "replace-all":
        conn.execute(f"DELETE FROM {SCORE_TABLE} WHERE model_id=?", (model_id,))
    if not rows:
        conn.commit()
        return 0, 0

    inserted = 0
    updated = 0
    if mode == "insert-missing":
        sql = f"""
        INSERT OR IGNORE INTO {SCORE_TABLE} (
          episode_id,
          model_id,
          ticker,
          entry_window_date,
          entry_window_exit_date,
          as_of_date,
          regime_used,
          model_family,
          target_name,
          feature_version,
          regime_version,
          artifact_path,
          predicted_probability,
          scored_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        before = conn.total_changes
        conn.executemany(sql, rows)
        inserted = conn.total_changes - before
        conn.commit()
        return inserted, 0

    episode_ids = [str(r[0]) for r in rows]
    existing = _fetch_existing_episode_ids(conn, model_id=model_id, episode_ids=episode_ids)
    sql = f"""
    INSERT INTO {SCORE_TABLE} (
      episode_id,
      model_id,
      ticker,
      entry_window_date,
      entry_window_exit_date,
      as_of_date,
      regime_used,
      model_family,
      target_name,
      feature_version,
      regime_version,
      artifact_path,
      predicted_probability,
      scored_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(episode_id, model_id) DO UPDATE SET
      ticker=excluded.ticker,
      entry_window_date=excluded.entry_window_date,
      entry_window_exit_date=excluded.entry_window_exit_date,
      as_of_date=excluded.as_of_date,
      regime_used=excluded.regime_used,
      model_family=excluded.model_family,
      target_name=excluded.target_name,
      feature_version=excluded.feature_version,
      regime_version=excluded.regime_version,
      artifact_path=excluded.artifact_path,
      predicted_probability=excluded.predicted_probability,
      scored_at=excluded.scored_at
    """
    conn.executemany(sql, rows)
    conn.commit()
    if mode == "replace-all":
        inserted = len(rows)
        updated = 0
    else:
        inserted = sum(1 for episode_id in episode_ids if episode_id not in existing)
        updated = len(rows) - inserted
    return inserted, updated


def compute_and_store_up20_bull_hgb_scores(
    conn: sqlite3.Connection,
    *,
    model_dir: Path,
    regime_version: str = DEFAULT_REGIME_VERSION,
    mode: str = "upsert",
    date_from: str | None = None,
    date_to: str | None = None,
    osakedata_db_path: str = DEFAULT_OSAKEDATA_DB,
    scored_at: str | None = None,
) -> Up20BullScoreSummary:
    _required_schema_check(conn)
    ts = scored_at or _utc_now_iso()
    feature_columns = _load_feature_list(model_dir)
    for col in feature_columns:
        if not _column_exists(conn, "rc_episode_exit_features", col):
            raise RuntimeError(f"UP20_BULL_REQUIRED_FEATURE_COLUMN_MISSING_{col}")
    model = _load_hgb_model(model_dir)
    model_path = str((model_dir / f"{MODEL_ID_HGB}.joblib").resolve())

    episodes_scanned = _count_scanned_episodes(conn, date_from=date_from, date_to=date_to)
    bull_frame = _load_bull_episodes(
        conn,
        regime_version=regime_version,
        date_from=date_from,
        date_to=date_to,
    )
    bull_count = int(bull_frame.shape[0])

    score_frame = _load_scoring_rows(
        conn,
        feature_columns=feature_columns,
        regime_version=regime_version,
        date_from=date_from,
        date_to=date_to,
    )
    _build_missing_features_if_needed(
        conn,
        bull_count=bull_count,
        scored_count=int(score_frame.shape[0]),
        osakedata_db_path=osakedata_db_path,
        date_from=date_from,
        date_to=date_to,
        computed_at=ts,
    )
    if int(score_frame.shape[0]) < bull_count:
        score_frame = _load_scoring_rows(
            conn,
            feature_columns=feature_columns,
            regime_version=regime_version,
            date_from=date_from,
            date_to=date_to,
        )

    episodes_eligible = int(score_frame.shape[0])
    scores_skipped = max(0, bull_count - episodes_eligible)
    if score_frame.empty:
        return Up20BullScoreSummary(
            episodes_scanned=episodes_scanned,
            episodes_eligible=0,
            scores_inserted=0,
            scores_updated=0,
            scores_skipped=scores_skipped,
            model_id=MODEL_ID_HGB,
            regime_used=REGIME_USED,
            feature_count=len(feature_columns),
        )

    x = score_frame[feature_columns]
    proba = np.asarray(model.predict_proba(x)[:, 1], dtype=float)
    if proba.shape[0] != score_frame.shape[0]:
        raise RuntimeError("UP20_BULL_SCORE_COUNT_MISMATCH")

    payload: list[tuple[object, ...]] = []
    for row, prob in zip(score_frame.itertuples(index=False), proba):
        episode_id = str(getattr(row, "episode_id"))
        ticker = str(getattr(row, "ticker"))
        entry_window_date = getattr(row, "entry_window_date")
        entry_window_exit_date = str(getattr(row, "entry_window_exit_date"))
        payload.append(
            (
                episode_id,
                MODEL_ID_HGB,
                ticker,
                entry_window_date if entry_window_date is not None else None,
                entry_window_exit_date,
                entry_window_exit_date,
                REGIME_USED,
                MODEL_FAMILY,
                TARGET_NAME,
                FEATURE_VERSION,
                regime_version,
                model_path,
                float(prob),
                ts,
            )
        )

    inserted, updated = _upsert_scores(
        conn,
        rows=payload,
        model_id=MODEL_ID_HGB,
        mode=mode,
    )
    return Up20BullScoreSummary(
        episodes_scanned=episodes_scanned,
        episodes_eligible=episodes_eligible,
        scores_inserted=inserted,
        scores_updated=updated,
        scores_skipped=scores_skipped,
        model_id=MODEL_ID_HGB,
        regime_used=REGIME_USED,
        feature_count=len(feature_columns),
    )
