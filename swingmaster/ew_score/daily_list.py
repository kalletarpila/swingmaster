from __future__ import annotations

import sqlite3
from typing import Any


def fetch_daily_production_rows(
    conn: sqlite3.Connection,
    date: str,
    state: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    sql = """
        SELECT
          s.ticker,
          s.state,
          e.ew_level_day3,
          e.ew_score_day3,
          e.ew_rule
        FROM rc_state_daily s
        LEFT JOIN rc_ew_score_daily e
          ON e.ticker = s.ticker
         AND e.date = s.date
        WHERE s.date = ?
    """
    params: list[Any] = [date]

    if state is not None:
        sql += " AND s.state = ?"
        params.append(state)

    sql += """
        ORDER BY
          CASE WHEN e.ew_score_day3 IS NULL THEN 1 ELSE 0 END,
          e.ew_score_day3 DESC,
          s.ticker ASC
    """

    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description or []]
    return [dict(zip(cols, row)) for row in rows]
