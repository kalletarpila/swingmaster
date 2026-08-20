from __future__ import annotations

import sqlite3
from collections import Counter
from typing import Any


FUND_LIFECYCLE_RULE_V2 = "FUND_LIFECYCLE_RULE_V2"
FUND_LIFECYCLE_RULE_V3_EBITDA_L2 = "FUND_LIFECYCLE_RULE_V3_EBITDA_L2"
FUND_LIFECYCLE_RULE_V1 = FUND_LIFECYCLE_RULE_V3_EBITDA_L2
LIFECYCLE_CLASSES = (
    "STARTUP",
    "GROWTH",
    "SCALING",
    "MATURE",
    "TRANSITION",
    "DECLINING",
    "DISTRESSED",
    "UNCLASSIFIED",
)


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
                    revenue_ttm,
                    revenue_growth_ttm_yoy,
                    ebit_margin_ttm,
                    ebit_margin_trend_4q,
                    ebitda_margin_ttm,
                    ebitda_margin_trend_4q,
                    fcf_margin_ttm,
                    fundamental_score,
                    lifecycle_class
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
                    revenue_ttm,
                    revenue_growth_ttm_yoy,
                    ebit_margin_ttm,
                    ebit_margin_trend_4q,
                    ebitda_margin_ttm,
                    ebitda_margin_trend_4q,
                    fcf_margin_ttm,
                    fundamental_score,
                    lifecycle_class
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


def classify_lifecycle(row: sqlite3.Row) -> str:
    revenue_growth_ttm_yoy = row["revenue_growth_ttm_yoy"]
    ebitda_margin_ttm = row["ebitda_margin_ttm"]
    ebitda_margin_trend_4q = row["ebitda_margin_trend_4q"]
    fcf_margin_ttm = row["fcf_margin_ttm"]

    if (
        _is_true(ebitda_margin_ttm is not None and ebitda_margin_ttm < -0.30)
        and _is_true(fcf_margin_ttm is not None and fcf_margin_ttm < -0.20)
    ):
        return "DISTRESSED"

    if (
        _is_true(revenue_growth_ttm_yoy is not None and revenue_growth_ttm_yoy > 0.30)
        and _is_true(ebitda_margin_ttm is not None and ebitda_margin_ttm < 0)
        and _is_true(fcf_margin_ttm is not None and fcf_margin_ttm < 0)
    ):
        return "STARTUP"

    if (
        _is_true(revenue_growth_ttm_yoy is not None and revenue_growth_ttm_yoy > 0.20)
        and _is_true(ebitda_margin_ttm is not None and ebitda_margin_ttm < 0.15)
    ):
        return "GROWTH"

    if (
        _is_true(revenue_growth_ttm_yoy is not None and revenue_growth_ttm_yoy > 0.10)
        and _is_true(ebitda_margin_trend_4q is not None and ebitda_margin_trend_4q > 0)
        and _is_true(ebitda_margin_ttm is not None and ebitda_margin_ttm >= 0)
    ):
        return "SCALING"

    mature_growth_ok = (
        revenue_growth_ttm_yoy is None
        or _is_true(revenue_growth_ttm_yoy >= -0.05)
    )
    if (
        _is_true(ebitda_margin_ttm is not None and ebitda_margin_ttm >= 0.25)
        and _is_true(fcf_margin_ttm is not None and fcf_margin_ttm >= 0.05)
        and mature_growth_ok
    ):
        return "MATURE"

    transition_growth_ok = (
        revenue_growth_ttm_yoy is None
        or _is_true(revenue_growth_ttm_yoy >= -0.05)
    )
    transition_margin_trend_ok = (
        ebitda_margin_trend_4q is None
        or _is_true(ebitda_margin_trend_4q >= -0.07)
    )
    if (
        _is_true(ebitda_margin_ttm is not None and ebitda_margin_ttm >= 0)
        and _is_true(ebitda_margin_ttm is not None and ebitda_margin_ttm < 0.25)
        and _is_true(fcf_margin_ttm is not None and fcf_margin_ttm >= 0)
        and transition_growth_ok
        and transition_margin_trend_ok
    ):
        return "TRANSITION"

    if (
        _is_true(revenue_growth_ttm_yoy is not None and revenue_growth_ttm_yoy < -0.05)
        or _is_true(ebitda_margin_trend_4q is not None and ebitda_margin_trend_4q < -0.07)
    ):
        return "DECLINING"

    return "UNCLASSIFIED"


def _is_true(value: bool) -> bool:
    return bool(value)


def update_lifecycle_rows(
    conn: sqlite3.Connection,
    rows: list[sqlite3.Row],
    dry_run: bool,
    as_of_dates: set[str] | None = None,
    skip_unchanged: bool = False,
) -> tuple[int, dict[str, int]]:
    lifecycle_updates = [
        (classify_lifecycle(row), str(row["ticker"]), str(row["as_of_date"]))
        for row in rows
        if as_of_dates is None or str(row["as_of_date"]) in as_of_dates
    ]
    if skip_unchanged:
        lifecycle_updates = [
            update
            for update in lifecycle_updates
            if not any(
                str(row["ticker"]) == update[1]
                and str(row["as_of_date"]) == update[2]
                and row["lifecycle_class"] == update[0]
                for row in rows
            )
        ]
    class_counts = Counter(classification for classification, _, _ in lifecycle_updates)
    for lifecycle_class in LIFECYCLE_CLASSES:
        class_counts.setdefault(lifecycle_class, 0)

    if not dry_run:
        conn.executemany(
            """
            UPDATE rc_fundamental_ttm
            SET lifecycle_class = ?
            WHERE ticker = ? AND as_of_date = ?
            """,
            lifecycle_updates,
        )
        conn.commit()

    return len(lifecycle_updates), dict(class_counts)


def run_lifecycle_classification(
    conn: sqlite3.Connection,
    ticker: str | None,
    dry_run: bool,
    as_of_dates: list[str] | None = None,
    skip_unchanged: bool = False,
) -> tuple[int, dict[str, int]]:
    rows = load_ttm_rows(conn, ticker)
    return update_lifecycle_rows(
        conn,
        rows,
        dry_run,
        as_of_dates=None if as_of_dates is None else set(as_of_dates),
        skip_unchanged=skip_unchanged,
    )
