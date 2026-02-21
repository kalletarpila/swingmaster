from __future__ import annotations

import sqlite3

from swingmaster.ew_score.repo import (
    RcEwScoreDailyRepo,
    ensure_rc_ew_score_daily_dual_mode_columns,
)


def test_ew_score_repo_schema_and_upsert() -> None:
    conn = sqlite3.connect(":memory:")
    repo = RcEwScoreDailyRepo(conn)

    repo.ensure_schema()

    tbl = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='rc_ew_score_daily'"
    ).fetchone()
    assert tbl is not None
    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(rc_ew_score_daily)").fetchall()
    }
    assert "ew_score_fastpass" in cols
    assert "ew_level_fastpass" in cols

    repo.upsert_row(
        ticker="AAA",
        date="2026-02-19",
        ew_score_day3=0.612345,
        ew_level_day3=2,
        ew_rule="EW_SCORE_DAY3_V1_FIN",
        inputs_json='{"r_ew_day3_pct":1.23}',
    )
    row1 = repo.get_row("AAA", "2026-02-19")
    assert row1 is not None
    assert row1["ticker"] == "AAA"
    assert row1["date"] == "2026-02-19"
    assert row1["ew_score_day3"] == 0.612345
    assert row1["ew_level_day3"] == 2
    assert row1["ew_rule"] == "EW_SCORE_DAY3_V1_FIN"
    assert row1["inputs_json"] == '{"r_ew_day3_pct":1.23}'
    created_at_1 = row1["created_at"]

    repo.upsert_row(
        ticker="AAA",
        date="2026-02-19",
        ew_score_day3=0.712345,
        ew_level_day3=3,
        ew_rule="EW_SCORE_DAY3_V1_FIN",
        inputs_json='{"r_ew_day3_pct":2.34}',
    )
    row2 = repo.get_row("AAA", "2026-02-19")
    assert row2 is not None
    assert row2["ew_score_day3"] == 0.712345
    assert row2["ew_level_day3"] == 3
    assert row2["inputs_json"] == '{"r_ew_day3_pct":2.34}'
    assert row2["created_at"] == created_at_1


def test_ew_score_repo_fastpass_upsert_and_day3_unchanged() -> None:
    conn = sqlite3.connect(":memory:")
    repo = RcEwScoreDailyRepo(conn)
    repo.ensure_schema()

    repo.upsert_row(
        ticker="AAA",
        date="2026-02-19",
        ew_score_day3=0.5,
        ew_level_day3=2,
        ew_rule="EW_SCORE_DAY3_V1_FIN",
        inputs_json='{"legacy":"day3"}',
    )
    before = repo.get_row("AAA", "2026-02-19")
    assert before is not None
    assert before["ew_score_fastpass"] is None
    assert before["ew_level_fastpass"] is None

    repo.upsert_fastpass_row(
        ticker="AAA",
        date="2026-02-19",
        ew_score_fastpass=0.77,
        ew_level_fastpass=1,
        ew_rule="EW_SCORE_FASTPASS_V1_USA_SMALL",
        inputs_json='{"rule_id":"EW_SCORE_FASTPASS_V1_USA_SMALL"}',
    )
    row = repo.get_row("AAA", "2026-02-19")
    assert row is not None
    assert row["ew_score_day3"] == 0.5
    assert row["ew_level_day3"] == 2
    assert row["ew_score_fastpass"] == 0.77
    assert row["ew_level_fastpass"] == 1
    assert row["ew_rule"] == "EW_SCORE_DAY3_V1_FIN"


def test_dual_mode_column_migration_is_idempotent() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE rc_ew_score_daily (
          ticker TEXT NOT NULL,
          date TEXT NOT NULL,
          ew_score_day3 REAL NOT NULL,
          ew_level_day3 INTEGER NOT NULL,
          ew_rule TEXT NOT NULL,
          inputs_json TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          PRIMARY KEY (ticker, date)
        )
        """
    )
    conn.commit()

    ensure_rc_ew_score_daily_dual_mode_columns(conn)
    ensure_rc_ew_score_daily_dual_mode_columns(conn)

    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(rc_ew_score_daily)").fetchall()
    }
    assert "ew_score_rolling" in cols
    assert "ew_level_rolling" in cols
    assert "ew_rule_fastpass" in cols
    assert "ew_rule_rolling" in cols
    assert "inputs_json_fastpass" in cols
    assert "inputs_json_rolling" in cols


def test_fastpass_and_rolling_upserts_do_not_overwrite_other_mode_or_legacy() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE rc_ew_score_daily (
          ticker TEXT NOT NULL,
          date TEXT NOT NULL,
          ew_score_day3 REAL,
          ew_level_day3 INTEGER,
          ew_rule TEXT,
          inputs_json TEXT,
          ew_score_fastpass REAL,
          ew_level_fastpass INTEGER,
          ew_rule_fastpass TEXT,
          inputs_json_fastpass TEXT,
          ew_score_rolling REAL,
          ew_level_rolling INTEGER,
          ew_rule_rolling TEXT,
          inputs_json_rolling TEXT,
          created_at TEXT,
          UNIQUE(ticker, date)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO rc_ew_score_daily (
          ticker, date, ew_rule, inputs_json,
          ew_score_fastpass, ew_level_fastpass, ew_rule_fastpass, inputs_json_fastpass,
          ew_score_rolling, ew_level_rolling, ew_rule_rolling, inputs_json_rolling
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "AAA",
            "2026-02-19",
            "LEGACY_RULE",
            "LEGACY_JSON",
            0.11,
            0,
            "FASTPASS_RULE_OLD",
            "FASTPASS_JSON_OLD",
            0.22,
            2,
            "ROLLING_RULE_OLD",
            "ROLLING_JSON_OLD",
        ),
    )
    conn.commit()

    repo = RcEwScoreDailyRepo(conn)
    repo.upsert_fastpass_row(
        ticker="AAA",
        date="2026-02-19",
        ew_score_fastpass=0.77,
        ew_level_fastpass=1,
        ew_rule="FASTPASS_RULE_NEW",
        inputs_json="FASTPASS_JSON_NEW",
    )
    after_fastpass = conn.execute(
        """
        SELECT
          ew_rule, inputs_json,
          ew_score_fastpass, ew_level_fastpass, ew_rule_fastpass, inputs_json_fastpass,
          ew_score_rolling, ew_level_rolling, ew_rule_rolling, inputs_json_rolling
        FROM rc_ew_score_daily
        WHERE ticker = ? AND date = ?
        """,
        ("AAA", "2026-02-19"),
    ).fetchone()
    assert after_fastpass is not None
    assert after_fastpass[0] == "LEGACY_RULE"
    assert after_fastpass[1] == "LEGACY_JSON"
    assert after_fastpass[2] == 0.77
    assert after_fastpass[3] == 1
    assert after_fastpass[4] == "FASTPASS_RULE_NEW"
    assert after_fastpass[5] == "FASTPASS_JSON_NEW"
    assert after_fastpass[6] == 0.22
    assert after_fastpass[7] == 2
    assert after_fastpass[8] == "ROLLING_RULE_OLD"
    assert after_fastpass[9] == "ROLLING_JSON_OLD"

    repo.upsert_rolling_row(
        ticker="AAA",
        date="2026-02-19",
        ew_score_rolling=0.88,
        ew_level_rolling=3,
        ew_rule_rolling="ROLLING_RULE_NEW",
        inputs_json_rolling="ROLLING_JSON_NEW",
    )
    after_rolling = conn.execute(
        """
        SELECT
          ew_rule, inputs_json,
          ew_score_fastpass, ew_level_fastpass, ew_rule_fastpass, inputs_json_fastpass,
          ew_score_rolling, ew_level_rolling, ew_rule_rolling, inputs_json_rolling
        FROM rc_ew_score_daily
        WHERE ticker = ? AND date = ?
        """,
        ("AAA", "2026-02-19"),
    ).fetchone()
    assert after_rolling is not None
    assert after_rolling[0] == "LEGACY_RULE"
    assert after_rolling[1] == "LEGACY_JSON"
    assert after_rolling[2] == 0.77
    assert after_rolling[3] == 1
    assert after_rolling[4] == "FASTPASS_RULE_NEW"
    assert after_rolling[5] == "FASTPASS_JSON_NEW"
    assert after_rolling[6] == 0.88
    assert after_rolling[7] == 3
    assert after_rolling[8] == "ROLLING_RULE_NEW"
    assert after_rolling[9] == "ROLLING_JSON_NEW"
