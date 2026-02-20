from __future__ import annotations

import sqlite3

from swingmaster.ew_score.repo import RcEwScoreDailyRepo


def test_ew_score_repo_schema_and_upsert() -> None:
    conn = sqlite3.connect(":memory:")
    repo = RcEwScoreDailyRepo(conn)

    repo.ensure_schema()

    tbl = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='rc_ew_score_daily'"
    ).fetchone()
    assert tbl is not None

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
