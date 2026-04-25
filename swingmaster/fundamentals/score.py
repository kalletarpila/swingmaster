from __future__ import annotations

import sqlite3
from statistics import mean


FUND_SCORE_RULE_V1 = "FUND_SCORE_RULE_V1"


def load_ttm_rows(conn: sqlite3.Connection, ticker: str | None) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        if ticker is None:
            rows = conn.execute(
                """
                SELECT
                    ticker,
                    as_of_date,
                    revenue_growth_ttm_yoy,
                    ebit_margin_ttm,
                    ebit_margin_trend_4q,
                    fcf_margin_ttm,
                    net_debt_to_ebitda,
                    share_dilution_yoy,
                    lifecycle_class,
                    fundamental_score
                FROM rc_fundamental_ttm
                ORDER BY ticker ASC, as_of_date ASC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    ticker,
                    as_of_date,
                    revenue_growth_ttm_yoy,
                    ebit_margin_ttm,
                    ebit_margin_trend_4q,
                    fcf_margin_ttm,
                    net_debt_to_ebitda,
                    share_dilution_yoy,
                    lifecycle_class,
                    fundamental_score
                FROM rc_fundamental_ttm
                WHERE ticker = ?
                ORDER BY ticker ASC, as_of_date ASC
                """,
                (ticker,),
            ).fetchall()
    finally:
        conn.row_factory = previous_row_factory

    if not rows:
        if ticker is None:
            raise RuntimeError("FUNDAMENTAL_TTM_NOT_FOUND")
        raise RuntimeError(f"FUNDAMENTAL_TTM_NOT_FOUND:{ticker}")
    return rows


def calculate_fundamental_score(row: sqlite3.Row) -> float:
    score_raw = (
        _growth_component(row["revenue_growth_ttm_yoy"])
        + _margin_component(row["ebit_margin_ttm"])
        + _margin_trend_component(row["ebit_margin_trend_4q"])
        + _fcf_component(row["fcf_margin_ttm"])
        + _leverage_component(row["net_debt_to_ebitda"])
        + _dilution_component(row["share_dilution_yoy"])
        + _lifecycle_component(row["lifecycle_class"])
    )
    return float(min(100, max(0, score_raw)))


def _growth_component(value: float | None) -> int:
    if value is None:
        return 8
    if value >= 0.30:
        return 20
    if value >= 0.20:
        return 16
    if value >= 0.10:
        return 12
    if value >= 0:
        return 6
    return 0


def _margin_component(value: float | None) -> int:
    if value is None:
        return 0
    if value >= 0.25:
        return 20
    if value >= 0.15:
        return 16
    if value >= 0.08:
        return 10
    if value >= 0:
        return 5
    return 0


def _margin_trend_component(value: float | None) -> int:
    if value is None:
        return 6
    if value >= 0.05:
        return 15
    if value >= 0.02:
        return 10
    if value >= 0:
        return 6
    return 2


def _fcf_component(value: float | None) -> int:
    if value is None:
        return 0
    if value >= 0.20:
        return 15
    if value >= 0.10:
        return 12
    if value >= 0.05:
        return 8
    if value >= 0:
        return 4
    return 0


def _leverage_component(value: float | None) -> int:
    if value is None:
        return 8
    if value <= 0:
        return 15
    if value <= 1:
        return 12
    if value <= 2:
        return 8
    if value <= 3:
        return 4
    return 0


def _dilution_component(value: float | None) -> int:
    if value is None:
        return 5
    if value <= -0.02:
        return 10
    if value <= 0:
        return 8
    if value <= 0.02:
        return 5
    if value <= 0.05:
        return 2
    return 0


def _lifecycle_component(value: str | None) -> int:
    if value == "STARTUP":
        return -5
    if value == "GROWTH":
        return 2
    if value == "SCALING":
        return 4
    if value == "MATURE":
        return 5
    if value == "DECLINING":
        return -5
    if value == "DISTRESSED":
        return -10
    return 0


def update_scores(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
    dry_run: bool,
) -> tuple[int, float | None, float | None, float | None]:
    score_updates = [
        (calculate_fundamental_score(row), str(row["ticker"]), str(row["as_of_date"]))
        for row in rows
    ]
    scores = [score for score, _, _ in score_updates]
    min_score = min(scores) if scores else None
    max_score = max(scores) if scores else None
    avg_score = round(mean(scores), 4) if scores else None

    if not dry_run:
        conn.executemany(
            """
            UPDATE rc_fundamental_ttm
            SET fundamental_score = ?
            WHERE ticker = ? AND as_of_date = ?
            """,
            score_updates,
        )
        conn.commit()

    return len(rows), min_score, max_score, avg_score


def run_fundamental_scoring(
    conn: sqlite3.Connection,
    ticker: str | None,
    dry_run: bool,
) -> tuple[int, float | None, float | None, float | None]:
    rows = load_ttm_rows(conn, ticker)
    return update_scores(conn, rows, dry_run)
