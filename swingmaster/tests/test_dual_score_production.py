from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.dual_score.production import (
    DUAL_CURRENT_TABLE,
    FAIL10_SOURCE_TABLE,
    UP20_SOURCE_TABLE,
    compute_and_store_dual_scores_production,
)


def _create_pipeline_episode_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE rc_pipeline_episode (
          episode_id TEXT PRIMARY KEY,
          ticker TEXT NOT NULL,
          entry_window_exit_date TEXT,
          entry_window_exit_state TEXT,
          close_at_entry REAL,
          close_at_ew_start REAL,
          close_at_ew_exit REAL,
          days_entry_to_ew_trading INTEGER,
          days_in_entry_window_trading INTEGER,
          pipe_min_sma3 REAL,
          pipe_max_sma3 REAL,
          pre40_min_sma5 REAL,
          pre40_max_sma5 REAL,
          ew_confirm_above_5 INTEGER,
          ew_confirm_confirmed INTEGER,
          post60_growth_pct_close_ew_exit_to_peak REAL
        )
        """
    )


def _seed_episodes(conn: sqlite3.Connection) -> int:
    rows: list[tuple[object, ...]] = []
    idx = 0
    for year in (2020, 2021):
        for day in range(1, 19):
            idx += 1
            state = "PASS" if (idx % 3) != 0 else "NO_TRADE"
            growth = [30.0, 5.0, 15.0, 25.0][idx % 4]
            base = 10.0 + (idx % 7)
            rows.append(
                (
                    f"EP{idx}",
                    f"T{idx:03d}",
                    f"{year}-01-{day:02d}",
                    state,
                    base,
                    base * 1.01,
                    base * 1.03,
                    5 + (idx % 4),
                    4 + (idx % 3),
                    base * 0.92,
                    base * 1.05,
                    base * 0.85,
                    base * 1.08,
                    1 if idx % 2 == 0 else 0,
                    1 if idx % 5 != 0 else 0,
                    growth,
                )
            )
    # One new closed episode without forward label yet; still should be scored.
    rows.append(
        (
            "EP_NEW",
            "TNEW",
            "2026-02-27",
            "PASS",
            12.0,
            12.1,
            12.3,
            6,
            5,
            11.4,
            12.6,
            10.8,
            12.9,
            1,
            1,
            None,
        )
    )
    conn.executemany(
        """
        INSERT INTO rc_pipeline_episode (
          episode_id, ticker, entry_window_exit_date, entry_window_exit_state,
          close_at_entry, close_at_ew_start, close_at_ew_exit,
          days_entry_to_ew_trading, days_in_entry_window_trading,
          pipe_min_sma3, pipe_max_sma3, pre40_min_sma5, pre40_max_sma5,
          ew_confirm_above_5, ew_confirm_confirmed,
          post60_growth_pct_close_ew_exit_to_peak
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def _create_osakedata_db(path: Path, rc_conn: sqlite3.Connection) -> None:
    rows = rc_conn.execute(
        """
        SELECT ticker, entry_window_exit_date
        FROM rc_pipeline_episode
        WHERE entry_window_exit_date IS NOT NULL
        """
    ).fetchall()
    dates = sorted({str(r[1]) for r in rows})
    os_conn = sqlite3.connect(str(path))
    os_conn.execute(
        """
        CREATE TABLE osakedata (
          id INTEGER PRIMARY KEY,
          osake TEXT,
          pvm TEXT,
          open REAL,
          high REAL,
          low REAL,
          close REAL,
          volume INTEGER,
          market TEXT NOT NULL DEFAULT 'usa',
          sector TEXT,
          industry TEXT
        )
        """
    )
    payload: list[tuple[object, ...]] = []
    for idx, (ticker, d) in enumerate(rows, start=1):
        close = 10.0 + (idx % 9)
        payload.append((ticker, d, close * 0.99, close * 1.01, close * 0.98, close, 100000 + idx, "usa"))
    for di, d in enumerate(dates, start=1):
        spx = 3000.0 + di
        ndx = 9000.0 + 2 * di
        payload.append(("^GSPC", d, spx * 0.999, spx * 1.001, spx * 0.998, spx, 1000000 + di, "usa"))
        payload.append(("^NDX", d, ndx * 0.999, ndx * 1.001, ndx * 0.998, ndx, 2000000 + di, "usa"))
    os_conn.executemany(
        """
        INSERT INTO osakedata (osake, pvm, open, high, low, close, volume, market)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    os_conn.commit()
    os_conn.close()


def test_compute_and_store_dual_scores_production_populates_internal_tables(tmp_path: Path) -> None:
    conn = sqlite3.connect(":memory:")
    _create_pipeline_episode_table(conn)
    total_rows = _seed_episodes(conn)
    os_db = tmp_path / "osakedata.db"
    _create_osakedata_db(os_db, conn)

    summary = compute_and_store_dual_scores_production(
        conn,
        osakedata_db_path=str(os_db),
        mode="upsert",
        model_version="DUAL_TEST_V1",
        computed_at="2026-03-06T00:00:00+00:00",
        train_year_from=2020,
        train_year_to=2021,
    )

    assert summary.rows_scored == total_rows
    assert summary.rows_with_labels == total_rows - 1
    assert summary.rows_train_full > 20
    assert summary.rows_train_pass_only > 20
    assert summary.rows_train_fail10 > 20

    n_up20 = conn.execute(f"SELECT COUNT(*) FROM {UP20_SOURCE_TABLE}").fetchone()[0]
    n_fail = conn.execute(f"SELECT COUNT(*) FROM {FAIL10_SOURCE_TABLE}").fetchone()[0]
    n_dual = conn.execute(f"SELECT COUNT(*) FROM {DUAL_CURRENT_TABLE}").fetchone()[0]
    assert n_up20 == total_rows
    assert n_fail == total_rows
    assert n_dual == total_rows

    row = conn.execute(
        f"""
        SELECT score_up20_meta_v1, score_fail10_60d_close_hgb, model_version
        FROM {DUAL_CURRENT_TABLE}
        WHERE episode_id='EP_NEW'
        """
    ).fetchone()
    assert row is not None
    assert 0.0 <= float(row[0]) <= 1.0
    assert 0.0 <= float(row[1]) <= 1.0
    assert row[2] == "DUAL_TEST_V1"


def test_compute_and_store_dual_scores_production_insert_missing_keeps_existing_rows(tmp_path: Path) -> None:
    conn = sqlite3.connect(":memory:")
    _create_pipeline_episode_table(conn)
    _seed_episodes(conn)
    os_db = tmp_path / "osakedata.db"
    _create_osakedata_db(os_db, conn)

    compute_and_store_dual_scores_production(
        conn,
        osakedata_db_path=str(os_db),
        mode="upsert",
        model_version="OLD",
        computed_at="2026-03-06T00:00:00+00:00",
        train_year_from=2020,
        train_year_to=2021,
    )
    before = conn.execute(
        f"SELECT model_version FROM {DUAL_CURRENT_TABLE} WHERE episode_id='EP1'"
    ).fetchone()[0]
    assert before == "OLD"

    summary = compute_and_store_dual_scores_production(
        conn,
        osakedata_db_path=str(os_db),
        mode="insert-missing",
        model_version="NEW",
        computed_at="2026-03-07T00:00:00+00:00",
        train_year_from=2020,
        train_year_to=2021,
    )
    after = conn.execute(
        f"SELECT model_version FROM {DUAL_CURRENT_TABLE} WHERE episode_id='EP1'"
    ).fetchone()[0]
    assert after == "OLD"
    assert summary.rows_changed_dual_current == 0
