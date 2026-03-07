from __future__ import annotations

import argparse
import os
from pathlib import Path

os.environ.setdefault("SQLITE_TMPDIR", "/tmp")
import sqlite3

from swingmaster.episode_exit_features.production import DEFAULT_OSAKEDATA_DB
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.regime.production import DEFAULT_REGIME_VERSION
from swingmaster.research.fail10_bull_scoring import compute_and_store_fail10_bull_scores
from swingmaster.research.fail10_bull_training import MODEL_ID_HGB as FAIL10_MODEL_ID
from swingmaster.research.up20_bull_scoring import SCORE_TABLE, compute_and_store_up20_bull_hgb_scores
from swingmaster.research.up20_bull_training import MODEL_ID_HGB as UP20_MODEL_ID


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BULL model scoring flows (UP20 + FAIL10) and print combined coverage summary."
    )
    parser.add_argument("--rc-db", required=True, help="RC SQLite DB path")
    parser.add_argument(
        "--regime-version",
        default=DEFAULT_REGIME_VERSION,
        help="Regime version used from rc_episode_regime",
    )
    parser.add_argument("--model-dir", required=True, help="Directory containing BULL model artifacts")
    parser.add_argument(
        "--mode",
        choices=["upsert", "replace-all", "insert-missing"],
        default="upsert",
        help="Write mode for rc_episode_model_score",
    )
    parser.add_argument("--date-from", default=None, help="Optional entry_window_exit_date lower bound YYYY-MM-DD")
    parser.add_argument("--date-to", default=None, help="Optional entry_window_exit_date upper bound YYYY-MM-DD")
    parser.add_argument(
        "--osakedata-db",
        default=DEFAULT_OSAKEDATA_DB,
        help="Osakedata SQLite DB path used for building missing rc_episode_exit_features rows",
    )
    parser.add_argument("--scored-at", default=None, help="Optional ISO8601 timestamp")
    parser.add_argument(
        "--up20-min-prob",
        type=float,
        default=None,
        help="Optional minimum UP20 probability threshold (wrapper reporting only)",
    )
    parser.add_argument(
        "--fail10-max-prob",
        type=float,
        default=None,
        help="Optional maximum FAIL10 probability threshold (wrapper reporting only)",
    )
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def _date_sql(date_from: str | None, date_to: str | None) -> tuple[str, list[object]]:
    where: list[str] = []
    params: list[object] = []
    if date_from is not None:
        where.append("entry_window_exit_date >= ?")
        params.append(date_from)
    if date_to is not None:
        where.append("entry_window_exit_date <= ?")
        params.append(date_to)
    if not where:
        return "", params
    return " AND " + " AND ".join(where), params


def _count_scores_for_model(
    conn: sqlite3.Connection,
    *,
    model_id: str,
    date_from: str | None,
    date_to: str | None,
) -> int:
    date_sql, params = _date_sql(date_from, date_to)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {SCORE_TABLE}
        WHERE model_id=?
          {date_sql}
        """,
        [model_id, *params],
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _count_episodes_with_both_scores(
    conn: sqlite3.Connection,
    *,
    up20_model_id: str,
    fail10_model_id: str,
    date_from: str | None,
    date_to: str | None,
) -> int:
    where: list[str] = []
    params: list[object] = [up20_model_id, fail10_model_id]
    if date_from is not None:
        where.append("u.entry_window_exit_date >= ?")
        params.append(date_from)
    if date_to is not None:
        where.append("u.entry_window_exit_date <= ?")
        params.append(date_to)
    where_sql = (" AND " + " AND ".join(where)) if where else ""
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {SCORE_TABLE} u
        JOIN {SCORE_TABLE} f
          ON f.episode_id = u.episode_id
        WHERE u.model_id=?
          AND f.model_id=?
          {where_sql}
        """,
        params,
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _count_threshold_passes(
    conn: sqlite3.Connection,
    *,
    up20_model_id: str,
    fail10_model_id: str,
    date_from: str | None,
    date_to: str | None,
    up20_min_prob: float | None,
    fail10_max_prob: float | None,
) -> tuple[int | None, int | None, int | None]:
    up20_count: int | None = None
    fail10_count: int | None = None
    both_count: int | None = None

    if up20_min_prob is not None:
        where: list[str] = ["model_id=?", "predicted_probability >= ?"]
        params: list[object] = [up20_model_id, up20_min_prob]
        date_sql, date_params = _date_sql(date_from, date_to)
        where_sql = " AND ".join(where) + date_sql
        row = conn.execute(
            f"SELECT COUNT(*) FROM {SCORE_TABLE} WHERE {where_sql}",
            [*params, *date_params],
        ).fetchone()
        up20_count = int(row[0]) if row is not None else 0

    if fail10_max_prob is not None:
        where = ["model_id=?", "predicted_probability <= ?"]
        params = [fail10_model_id, fail10_max_prob]
        date_sql, date_params = _date_sql(date_from, date_to)
        where_sql = " AND ".join(where) + date_sql
        row = conn.execute(
            f"SELECT COUNT(*) FROM {SCORE_TABLE} WHERE {where_sql}",
            [*params, *date_params],
        ).fetchone()
        fail10_count = int(row[0]) if row is not None else 0

    if up20_min_prob is not None and fail10_max_prob is not None:
        where: list[str] = [
            "u.model_id=?",
            "f.model_id=?",
            "u.predicted_probability >= ?",
            "f.predicted_probability <= ?",
        ]
        params = [up20_model_id, fail10_model_id, up20_min_prob, fail10_max_prob]
        if date_from is not None:
            where.append("u.entry_window_exit_date >= ?")
            params.append(date_from)
        if date_to is not None:
            where.append("u.entry_window_exit_date <= ?")
            params.append(date_to)
        row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {SCORE_TABLE} u
            JOIN {SCORE_TABLE} f
              ON f.episode_id = u.episode_id
            WHERE {" AND ".join(where)}
            """,
            params,
        ).fetchone()
        both_count = int(row[0]) if row is not None else 0

    return up20_count, fail10_count, both_count


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(str(Path(args.rc_db)))
    try:
        apply_migrations(conn)
        up20_result = compute_and_store_up20_bull_hgb_scores(
            conn,
            model_dir=Path(args.model_dir),
            regime_version=args.regime_version,
            mode=args.mode,
            date_from=args.date_from,
            date_to=args.date_to,
            osakedata_db_path=args.osakedata_db,
            scored_at=args.scored_at,
        )
        fail10_result = compute_and_store_fail10_bull_scores(
            conn,
            model_dir=Path(args.model_dir),
            regime_version=args.regime_version,
            mode=args.mode,
            date_from=args.date_from,
            date_to=args.date_to,
            osakedata_db_path=args.osakedata_db,
            scored_at=args.scored_at,
        )
        up20_total = _count_scores_for_model(
            conn,
            model_id=UP20_MODEL_ID,
            date_from=args.date_from,
            date_to=args.date_to,
        )
        fail10_total = _count_scores_for_model(
            conn,
            model_id=FAIL10_MODEL_ID,
            date_from=args.date_from,
            date_to=args.date_to,
        )
        both_total = _count_episodes_with_both_scores(
            conn,
            up20_model_id=UP20_MODEL_ID,
            fail10_model_id=FAIL10_MODEL_ID,
            date_from=args.date_from,
            date_to=args.date_to,
        )
        up20_pass_count, fail10_pass_count, both_pass_count = _count_threshold_passes(
            conn,
            up20_model_id=UP20_MODEL_ID,
            fail10_model_id=FAIL10_MODEL_ID,
            date_from=args.date_from,
            date_to=args.date_to,
            up20_min_prob=args.up20_min_prob,
            fail10_max_prob=args.fail10_max_prob,
        )
    except (sqlite3.Error, RuntimeError, OSError, ValueError, FileNotFoundError) as exc:
        _summary(status="ERROR", message=str(exc))
        raise SystemExit(2)
    finally:
        conn.close()

    _summary(status="OK")
    _summary(rc_db=args.rc_db)
    _summary(regime_version=args.regime_version)
    _summary(model_dir=str(Path(args.model_dir).resolve()))
    _summary(mode=args.mode)
    _summary(date_from=args.date_from)
    _summary(date_to=args.date_to)
    _summary(up20_model_id=UP20_MODEL_ID)
    _summary(fail10_model_id=FAIL10_MODEL_ID)
    _summary(up20_scores_inserted=up20_result.scores_inserted)
    _summary(up20_scores_updated=up20_result.scores_updated)
    _summary(fail10_scores_inserted=fail10_result.scores_inserted)
    _summary(fail10_scores_updated=fail10_result.scores_updated)
    _summary(up20_scores_total=up20_total)
    _summary(fail10_scores_total=fail10_total)
    _summary(episodes_with_both_scores=both_total)

    if args.up20_min_prob is not None or args.fail10_max_prob is not None:
        _summary(up20_min_prob=args.up20_min_prob)
        _summary(fail10_max_prob=args.fail10_max_prob)
    if args.up20_min_prob is not None:
        _summary(episodes_passing_up20_threshold=up20_pass_count)
    if args.fail10_max_prob is not None:
        _summary(episodes_passing_fail10_threshold=fail10_pass_count)
    if args.up20_min_prob is not None and args.fail10_max_prob is not None:
        _summary(episodes_passing_both_thresholds=both_pass_count)


if __name__ == "__main__":
    main()

