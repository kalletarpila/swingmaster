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
from catboost import CatBoostClassifier

from swingmaster.episode_exit_features.production import (
    DEFAULT_OSAKEDATA_DB,
    FEATURE_COLUMNS,
    compute_and_store_episode_exit_features,
)
from swingmaster.regime.production import DEFAULT_REGIME_VERSION
from swingmaster.research.fail10_sideways_evaluation import EVAL_FILE_NAME, FEATURE_LIST_FILE_NAME
from swingmaster.research.fail10_sideways_training import MODEL_ID_CATBOOST, MODEL_ID_HGB
from swingmaster.research.up20_bull_scoring import SCORE_TABLE

REGIME_USED = "SIDEWAYS"
TARGET_NAME = "FAIL10"
FEATURE_VERSION = "EPISODE_EXIT_FEATURES_V1"


@dataclass(frozen=True)
class Fail10SidewaysScoreSummary:
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


def _load_selected_candidate(model_dir: Path) -> str:
    path = model_dir / EVAL_FILE_NAME
    if not path.exists():
        raise RuntimeError("FAIL10_SIDEWAYS_EVAL_JSON_MISSING")
    payload = json.loads(path.read_text(encoding="utf-8"))
    value = payload.get("evaluation_recommended_candidate")
    if value not in {MODEL_ID_CATBOOST, MODEL_ID_HGB}:
        raise RuntimeError("FAIL10_SIDEWAYS_SELECTED_CANDIDATE_INVALID")
    return str(value)


def _load_feature_list(model_dir: Path) -> list[str]:
    path = model_dir / FEATURE_LIST_FILE_NAME
    if not path.exists():
        raise RuntimeError("FAIL10_SIDEWAYS_FEATURE_LIST_MISSING")
    payload = json.loads(path.read_text(encoding="utf-8"))
    cols = payload.get("feature_columns")
    if not isinstance(cols, list) or not cols or not all(isinstance(c, str) for c in cols):
        raise RuntimeError("FAIL10_SIDEWAYS_FEATURE_LIST_INVALID")
    if tuple(cols) != tuple(FEATURE_COLUMNS):
        raise RuntimeError("FAIL10_SIDEWAYS_FEATURE_ORDER_MISMATCH")
    return cols


def _load_model(model_dir: Path, model_id: str) -> tuple[Any, str, str]:
    if model_id == MODEL_ID_HGB:
        model_path = model_dir / f"{MODEL_ID_HGB}.joblib"
        if not model_path.exists():
            raise RuntimeError("FAIL10_SIDEWAYS_HGB_ARTIFACT_MISSING")
        model = joblib.load(str(model_path))
        family = "HGB"
        return model, family, str(model_path.resolve())
    if model_id == MODEL_ID_CATBOOST:
        model_path = model_dir / f"{MODEL_ID_CATBOOST}.cbm"
        if not model_path.exists():
            raise RuntimeError("FAIL10_SIDEWAYS_CATBOOST_ARTIFACT_MISSING")
        model = CatBoostClassifier()
        model.load_model(str(model_path))
        family = "CATBOOST"
        return model, family, str(model_path.resolve())
    raise RuntimeError("FAIL10_SIDEWAYS_SELECTED_CANDIDATE_INVALID")


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


def _load_sideways_episode_count(
    conn: sqlite3.Connection,
    *,
    regime_version: str,
    date_from: str | None,
    date_to: str | None,
) -> int:
    date_sql, params = _sql_date_filter("p", date_from=date_from, date_to=date_to)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM rc_pipeline_episode p
        JOIN rc_episode_regime r
          ON r.episode_id = p.episode_id
         AND r.regime_version = ?
        WHERE p.entry_window_exit_date IS NOT NULL
          AND r.ew_exit_regime_combined = 'SIDEWAYS'
          {date_sql}
        """,
        [regime_version, *params],
    ).fetchone()
    return int(row[0]) if row is not None else 0


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
          AND r.ew_exit_regime_combined = 'SIDEWAYS'
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
    sideways_count: int,
    scored_count: int,
    osakedata_db_path: str,
    date_from: str | None,
    date_to: str | None,
    computed_at: str,
) -> None:
    if scored_count >= sideways_count:
        return
    compute_and_store_episode_exit_features(
        conn,
        osakedata_db_path=osakedata_db_path,
        mode="insert-missing",
        date_from=date_from,
        date_to=date_to,
        computed_at=computed_at,
    )


def _fetch_existing_episode_ids(conn: sqlite3.Connection, *, model_id: str, episode_ids: list[str]) -> set[str]:
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

    if mode == "insert-missing":
        sql = f"""
        INSERT OR IGNORE INTO {SCORE_TABLE} (
          episode_id, model_id, ticker, entry_window_date, entry_window_exit_date,
          as_of_date, regime_used, model_family, target_name, feature_version,
          regime_version, artifact_path, predicted_probability, scored_at
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
      episode_id, model_id, ticker, entry_window_date, entry_window_exit_date,
      as_of_date, regime_used, model_family, target_name, feature_version,
      regime_version, artifact_path, predicted_probability, scored_at
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
        return len(rows), 0
    inserted = sum(1 for episode_id in episode_ids if episode_id not in existing)
    updated = len(rows) - inserted
    return inserted, updated


def compute_and_store_fail10_sideways_scores(
    conn: sqlite3.Connection,
    *,
    model_dir: Path,
    regime_version: str = DEFAULT_REGIME_VERSION,
    mode: str = "upsert",
    date_from: str | None = None,
    date_to: str | None = None,
    osakedata_db_path: str = DEFAULT_OSAKEDATA_DB,
    scored_at: str | None = None,
) -> Fail10SidewaysScoreSummary:
    _required_schema_check(conn)
    ts = scored_at or _utc_now_iso()
    selected_model_id = _load_selected_candidate(model_dir)
    model, model_family, artifact_path = _load_model(model_dir, selected_model_id)
    feature_columns = _load_feature_list(model_dir)
    for col in feature_columns:
        if not _column_exists(conn, "rc_episode_exit_features", col):
            raise RuntimeError(f"FAIL10_SIDEWAYS_REQUIRED_FEATURE_COLUMN_MISSING_{col}")

    episodes_scanned = _count_scanned_episodes(conn, date_from=date_from, date_to=date_to)
    sideways_count = _load_sideways_episode_count(
        conn,
        regime_version=regime_version,
        date_from=date_from,
        date_to=date_to,
    )
    score_frame = _load_scoring_rows(
        conn,
        feature_columns=feature_columns,
        regime_version=regime_version,
        date_from=date_from,
        date_to=date_to,
    )
    _build_missing_features_if_needed(
        conn,
        sideways_count=sideways_count,
        scored_count=int(score_frame.shape[0]),
        osakedata_db_path=osakedata_db_path,
        date_from=date_from,
        date_to=date_to,
        computed_at=ts,
    )
    if int(score_frame.shape[0]) < sideways_count:
        score_frame = _load_scoring_rows(
            conn,
            feature_columns=feature_columns,
            regime_version=regime_version,
            date_from=date_from,
            date_to=date_to,
        )

    episodes_eligible = int(score_frame.shape[0])
    scores_skipped = max(0, sideways_count - episodes_eligible)
    if score_frame.empty:
        return Fail10SidewaysScoreSummary(
            episodes_scanned=episodes_scanned,
            episodes_eligible=0,
            scores_inserted=0,
            scores_updated=0,
            scores_skipped=scores_skipped,
            model_id=selected_model_id,
            regime_used=REGIME_USED,
            feature_count=len(feature_columns),
        )

    x = score_frame[feature_columns]
    proba = np.asarray(model.predict_proba(x)[:, 1], dtype=float)
    if proba.shape[0] != score_frame.shape[0]:
        raise RuntimeError("FAIL10_SIDEWAYS_SCORE_COUNT_MISMATCH")

    payload: list[tuple[object, ...]] = []
    for row, prob in zip(score_frame.itertuples(index=False), proba):
        exit_date = str(getattr(row, "entry_window_exit_date"))
        payload.append(
            (
                str(getattr(row, "episode_id")),
                selected_model_id,
                str(getattr(row, "ticker")),
                getattr(row, "entry_window_date"),
                exit_date,
                exit_date,
                REGIME_USED,
                model_family,
                TARGET_NAME,
                FEATURE_VERSION,
                regime_version,
                artifact_path,
                float(prob),
                ts,
            )
        )

    inserted, updated = _upsert_scores(
        conn,
        rows=payload,
        model_id=selected_model_id,
        mode=mode,
    )
    return Fail10SidewaysScoreSummary(
        episodes_scanned=episodes_scanned,
        episodes_eligible=episodes_eligible,
        scores_inserted=inserted,
        scores_updated=updated,
        scores_skipped=scores_skipped,
        model_id=selected_model_id,
        regime_used=REGIME_USED,
        feature_count=len(feature_columns),
    )
