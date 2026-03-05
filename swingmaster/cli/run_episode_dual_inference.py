from __future__ import annotations

import argparse
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


DUAL_TABLE = "rc_episode_model_dual_inference_current"
DEFAULT_UP20_SOURCE_TABLE = "rc_episode_model_inference_rank_meta_v1"
DEFAULT_UP20_SOURCE_COLUMN = "score_meta_v1_up20_60d_close"
DEFAULT_FAIL10_SOURCE_TABLE = "rc_episode_model_full_inference_no_dow_scores_hgb_fail10"
DEFAULT_FAIL10_SOURCE_COLUMN = "score_pred"
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate episodic dual-inference current table")
    parser.add_argument("--rc-db", required=True, help="Target RC SQLite database path")
    parser.add_argument(
        "--source-db",
        default=None,
        help="Optional source SQLite database path (defaults to --rc-db)",
    )
    parser.add_argument("--up20-source-table", default=DEFAULT_UP20_SOURCE_TABLE)
    parser.add_argument("--up20-source-column", default=DEFAULT_UP20_SOURCE_COLUMN)
    parser.add_argument("--fail10-source-table", default=DEFAULT_FAIL10_SOURCE_TABLE)
    parser.add_argument("--fail10-source-column", default=DEFAULT_FAIL10_SOURCE_COLUMN)
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace-all", "insert-missing"],
        default="upsert",
    )
    parser.add_argument("--model-version", required=True, help="Model version label for auditability")
    parser.add_argument("--computed-at", default=None, help="Optional ISO8601 timestamp")
    return parser.parse_args()


def summary(**items: Any) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def _assert_identifier(name: str, arg_name: str) -> None:
    if IDENTIFIER_RE.match(name) is None:
        raise ValueError(f"INVALID_IDENTIFIER_{arg_name}")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    if not _table_exists(conn, table_name):
        return False
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row[1]) == column_name for row in rows)


def ensure_dual_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DUAL_TABLE} (
          episode_id TEXT PRIMARY KEY,
          score_up20_meta_v1 REAL NOT NULL,
          score_fail10_60d_close_hgb REAL NOT NULL,
          model_version TEXT NOT NULL,
          computed_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def fetch_source_rows(
    source_conn: sqlite3.Connection,
    up20_table: str,
    up20_column: str,
    fail10_table: str,
    fail10_column: str,
) -> list[tuple[str, float, float]]:
    sql = f"""
    SELECT
      u.episode_id AS episode_id,
      CAST(u.{up20_column} AS REAL) AS score_up20_meta_v1,
      CAST(f.{fail10_column} AS REAL) AS score_fail10_60d_close_hgb
    FROM {up20_table} u
    JOIN {fail10_table} f
      ON f.episode_id = u.episode_id
    WHERE u.episode_id IS NOT NULL
      AND u.{up20_column} IS NOT NULL
      AND f.{fail10_column} IS NOT NULL
    ORDER BY u.episode_id ASC
    """
    rows = source_conn.execute(sql).fetchall()
    out: list[tuple[str, float, float]] = []
    for row in rows:
        out.append((str(row[0]), float(row[1]), float(row[2])))
    return out


def write_dual_rows(
    target_conn: sqlite3.Connection,
    rows: Sequence[tuple[str, float, float]],
    model_version: str,
    computed_at: str,
    mode: str,
) -> int:
    if mode == "replace-all":
        target_conn.execute(f"DELETE FROM {DUAL_TABLE}")

    payload = [
        (episode_id, up20_score, fail10_score, model_version, computed_at)
        for episode_id, up20_score, fail10_score in rows
    ]
    if not payload:
        target_conn.commit()
        return 0

    before_changes = target_conn.total_changes
    if mode == "insert-missing":
        target_conn.executemany(
            f"""
            INSERT OR IGNORE INTO {DUAL_TABLE} (
              episode_id,
              score_up20_meta_v1,
              score_fail10_60d_close_hgb,
              model_version,
              computed_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            payload,
        )
    else:
        target_conn.executemany(
            f"""
            INSERT INTO {DUAL_TABLE} (
              episode_id,
              score_up20_meta_v1,
              score_fail10_60d_close_hgb,
              model_version,
              computed_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(episode_id) DO UPDATE SET
              score_up20_meta_v1 = excluded.score_up20_meta_v1,
              score_fail10_60d_close_hgb = excluded.score_fail10_60d_close_hgb,
              model_version = excluded.model_version,
              computed_at = excluded.computed_at
            """,
            payload,
        )
    target_conn.commit()
    return target_conn.total_changes - before_changes


def main() -> None:
    args = parse_args()
    source_db = args.source_db or args.rc_db
    computed_at = args.computed_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    try:
        _assert_identifier(args.up20_source_table, "UP20_TABLE")
        _assert_identifier(args.up20_source_column, "UP20_COLUMN")
        _assert_identifier(args.fail10_source_table, "FAIL10_TABLE")
        _assert_identifier(args.fail10_source_column, "FAIL10_COLUMN")
    except ValueError as exc:
        summary(status="ERROR", message=str(exc))
        raise SystemExit(2)

    target_conn = sqlite3.connect(str(Path(args.rc_db)))
    source_conn = sqlite3.connect(str(Path(source_db)))
    try:
        ensure_dual_table(target_conn)
        if not _table_exists(source_conn, args.up20_source_table):
            summary(status="ERROR", message="UP20_SOURCE_TABLE_MISSING")
            raise SystemExit(2)
        if not _table_exists(source_conn, args.fail10_source_table):
            summary(status="ERROR", message="FAIL10_SOURCE_TABLE_MISSING")
            raise SystemExit(2)
        if not _column_exists(source_conn, args.up20_source_table, "episode_id"):
            summary(status="ERROR", message="UP20_EPISODE_ID_COLUMN_MISSING")
            raise SystemExit(2)
        if not _column_exists(source_conn, args.fail10_source_table, "episode_id"):
            summary(status="ERROR", message="FAIL10_EPISODE_ID_COLUMN_MISSING")
            raise SystemExit(2)
        if not _column_exists(source_conn, args.up20_source_table, args.up20_source_column):
            summary(status="ERROR", message="UP20_SOURCE_COLUMN_MISSING")
            raise SystemExit(2)
        if not _column_exists(source_conn, args.fail10_source_table, args.fail10_source_column):
            summary(status="ERROR", message="FAIL10_SOURCE_COLUMN_MISSING")
            raise SystemExit(2)

        source_rows = fetch_source_rows(
            source_conn=source_conn,
            up20_table=args.up20_source_table,
            up20_column=args.up20_source_column,
            fail10_table=args.fail10_source_table,
            fail10_column=args.fail10_source_column,
        )
        written_changes = write_dual_rows(
            target_conn=target_conn,
            rows=source_rows,
            model_version=args.model_version,
            computed_at=computed_at,
            mode=args.mode,
        )
    except sqlite3.Error as exc:
        summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    finally:
        source_conn.close()
        target_conn.close()

    summary(status="OK")
    summary(rc_db=args.rc_db)
    summary(source_db=source_db)
    summary(mode=args.mode)
    summary(model_version=args.model_version)
    summary(source_rows=len(source_rows))
    summary(rows_changed=written_changes)


if __name__ == "__main__":
    main()
