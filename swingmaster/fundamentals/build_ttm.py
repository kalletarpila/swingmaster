from __future__ import annotations

import sqlite3
from typing import Any


TTM_SUM_FIELDS = (
    "revenue",
    "ebit",
    "free_cashflow",
    "ebitda",
    "gross_profit",
)

TTM_BUSINESS_FIELDS = (
    "latest_period_end_date",
    "revenue_ttm",
    "revenue_growth_ttm_yoy",
    "ebit_ttm",
    "ebit_growth_ttm_yoy",
    "ebit_margin_ttm",
    "ebit_margin_trend_4q",
    "ebitda_ttm",
    "ebitda_margin_ttm",
    "ebitda_margin_trend_4q",
    "gross_margin_trend_4q",
    "fcf_ttm",
    "fcf_margin_ttm",
    "fcf_margin_trend_4q",
    "net_debt",
    "net_debt_to_ebit",
    "net_debt_to_ebitda",
    "share_dilution_yoy",
)


def load_quarterly_rows(conn: sqlite3.Connection, ticker: str) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                ticker,
                period_end_date,
                revenue,
                gross_profit,
                ebit,
                ebitda,
                free_cashflow,
                cash,
                total_debt,
                shares_outstanding
            FROM rc_fundamental_quarterly
            WHERE ticker = ?
            ORDER BY period_end_date ASC
            """,
            (ticker,),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    if len(rows) < 4:
        raise RuntimeError(f"FUNDAMENTAL_TTM_INSUFFICIENT_ROWS:{ticker}")
    return rows


def build_ttm_rows(quarterly_rows: list[sqlite3.Row], run_id: str) -> list[dict[str, Any]]:
    ticker = str(quarterly_rows[0]["ticker"])
    ttm_rows: list[dict[str, Any]] = []

    for index in range(3, len(quarterly_rows)):
        current_4q = quarterly_rows[index - 3 : index + 1]
        previous_4q = quarterly_rows[index - 7 : index - 3] if index >= 7 else None
        current_row = quarterly_rows[index]

        revenue_ttm = _sum_window(current_4q, "revenue")
        ebit_ttm = _sum_window(current_4q, "ebit")
        ebitda_ttm = _sum_window(current_4q, "ebitda")
        fcf_ttm = _sum_window(current_4q, "free_cashflow")
        gross_profit_ttm = _sum_window(current_4q, "gross_profit")

        ebit_margin_ttm = _safe_divide(ebit_ttm, revenue_ttm)
        ebitda_margin_ttm = _safe_divide(ebitda_ttm, revenue_ttm)
        fcf_margin_ttm = _safe_divide(fcf_ttm, revenue_ttm)
        gross_margin_ttm = _safe_divide(gross_profit_ttm, revenue_ttm)

        previous_revenue_ttm = _sum_window(previous_4q, "revenue") if previous_4q is not None else None
        previous_ebit_ttm = _sum_window(previous_4q, "ebit") if previous_4q is not None else None
        previous_ebitda_ttm = _sum_window(previous_4q, "ebitda") if previous_4q is not None else None
        previous_fcf_ttm = _sum_window(previous_4q, "free_cashflow") if previous_4q is not None else None
        previous_gross_profit_ttm = _sum_window(previous_4q, "gross_profit") if previous_4q is not None else None

        revenue_growth_ttm_yoy = _safe_growth(revenue_ttm, previous_revenue_ttm)
        ebit_growth_ttm_yoy = _safe_growth(ebit_ttm, previous_ebit_ttm)

        previous_ebit_margin_ttm = _safe_divide(previous_ebit_ttm, previous_revenue_ttm)
        previous_ebitda_margin_ttm = _safe_divide(previous_ebitda_ttm, previous_revenue_ttm)
        previous_fcf_margin_ttm = _safe_divide(previous_fcf_ttm, previous_revenue_ttm)
        previous_gross_margin_ttm = _safe_divide(previous_gross_profit_ttm, previous_revenue_ttm)

        net_debt = _calculate_net_debt(current_row["total_debt"], current_row["cash"])
        net_debt_to_ebit = _calculate_net_debt_to_ebit(net_debt, ebit_ttm)
        net_debt_to_ebitda = _calculate_net_debt_to_ebitda(net_debt, ebitda_ttm, ebit_ttm)
        share_dilution_yoy = _calculate_share_dilution(quarterly_rows, index)

        latest_period_end_date = str(current_row["period_end_date"])
        ttm_rows.append(
            {
                "ticker": ticker,
                "as_of_date": latest_period_end_date,
                "latest_period_end_date": latest_period_end_date,
                "revenue_ttm": revenue_ttm,
                "revenue_growth_ttm_yoy": revenue_growth_ttm_yoy,
                "ebit_ttm": ebit_ttm,
                "ebit_growth_ttm_yoy": ebit_growth_ttm_yoy,
                "ebit_margin_ttm": ebit_margin_ttm,
                "ebit_margin_trend_4q": _safe_delta(ebit_margin_ttm, previous_ebit_margin_ttm),
                "ebitda_ttm": ebitda_ttm,
                "ebitda_margin_ttm": ebitda_margin_ttm,
                "ebitda_margin_trend_4q": _safe_delta(ebitda_margin_ttm, previous_ebitda_margin_ttm),
                "gross_margin_trend_4q": _safe_delta(gross_margin_ttm, previous_gross_margin_ttm),
                "fcf_ttm": fcf_ttm,
                "fcf_margin_ttm": fcf_margin_ttm,
                "fcf_margin_trend_4q": _safe_delta(fcf_margin_ttm, previous_fcf_margin_ttm),
                "net_debt": net_debt,
                "net_debt_to_ebit": net_debt_to_ebit,
                "net_debt_to_ebitda": net_debt_to_ebitda,
                "share_dilution_yoy": share_dilution_yoy,
                "lifecycle_class": None,
                "fundamental_score": None,
                "run_id": run_id,
            }
        )

    return ttm_rows


def filter_ttm_rows_depending_on_quarter(
    ttm_rows: list[dict[str, Any]],
    quarterly_rows: list[sqlite3.Row],
    target_period_end_date: str | None,
) -> list[dict[str, Any]]:
    if target_period_end_date is None:
        return ttm_rows
    target_periods = {
        str(row["period_end_date"])
        for row in quarterly_rows
        if str(row["period_end_date"]) == target_period_end_date
    }
    if not target_periods:
        return []
    periods_by_as_of = {
        str(quarterly_rows[index]["period_end_date"]): {
            str(row["period_end_date"]) for row in quarterly_rows[index - 3 : index + 1]
        }
        for index in range(3, len(quarterly_rows))
    }
    return [
        row
        for row in ttm_rows
        if target_periods.intersection(periods_by_as_of.get(str(row["as_of_date"]), set()))
    ]


def _sum_window(rows: list[sqlite3.Row] | None, field_name: str) -> float | None:
    if rows is None:
        return None
    values = [row[field_name] for row in rows if row[field_name] is not None]
    if not values:
        return None
    return float(sum(values))


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return float(numerator / denominator)


def _safe_growth(current_value: float | None, previous_value: float | None) -> float | None:
    if current_value is None or previous_value is None or previous_value == 0:
        return None
    return float((current_value - previous_value) / abs(previous_value))


def _safe_delta(current_value: float | None, previous_value: float | None) -> float | None:
    if current_value is None or previous_value is None:
        return None
    return float(current_value - previous_value)


def _calculate_net_debt(total_debt: float | None, cash: float | None) -> float | None:
    if total_debt is None or cash is None:
        return None
    return float(total_debt - cash)


def _calculate_net_debt_to_ebit(net_debt: float | None, ebit_ttm: float | None) -> float | None:
    return _safe_divide(net_debt, ebit_ttm)


def _calculate_net_debt_to_ebitda(
    net_debt: float | None,
    ebitda_ttm: float | None,
    ebit_ttm: float | None,
) -> float | None:
    primary_value = _safe_divide(net_debt, ebitda_ttm)
    if primary_value is not None:
        return primary_value
    return _safe_divide(net_debt, ebit_ttm)


def _calculate_share_dilution(quarterly_rows: list[sqlite3.Row], index: int) -> float | None:
    if index < 4:
        return None
    current_value = quarterly_rows[index]["shares_outstanding"]
    previous_value = quarterly_rows[index - 4]["shares_outstanding"]
    if current_value is None or previous_value is None or previous_value == 0:
        return None
    return float((current_value - previous_value) / abs(previous_value))


def _values_equal(left: Any, right: Any) -> bool:
    return left == right


def _ttm_row_materially_changed(existing: sqlite3.Row | None, row: dict[str, Any]) -> bool:
    if existing is None:
        return True
    return any(not _values_equal(existing[field], row[field]) for field in TTM_BUSINESS_FIELDS)


def _select_ttm_rows_to_write(
    conn: sqlite3.Connection,
    ttm_rows: list[dict[str, Any]],
    *,
    skip_unchanged: bool,
) -> list[dict[str, Any]]:
    if not skip_unchanged:
        return ttm_rows
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows_to_write = []
        for row in ttm_rows:
            existing = conn.execute(
                """
                SELECT *
                FROM rc_fundamental_ttm
                WHERE ticker = ?
                  AND as_of_date = ?
                """,
                (row["ticker"], row["as_of_date"]),
            ).fetchone()
            if _ttm_row_materially_changed(existing, row):
                rows_to_write.append(row)
        return rows_to_write
    finally:
        conn.row_factory = previous_row_factory


def insert_ttm_rows(
    conn: sqlite3.Connection,
    ttm_rows: list[dict[str, Any]],
    *,
    skip_unchanged: bool = False,
) -> int:
    rows_to_write = _select_ttm_rows_to_write(conn, ttm_rows, skip_unchanged=skip_unchanged)
    conn.executemany(
        """
        INSERT OR REPLACE INTO rc_fundamental_ttm (
            ticker,
            as_of_date,
            latest_period_end_date,
            revenue_ttm,
            revenue_growth_ttm_yoy,
            ebit_ttm,
            ebit_growth_ttm_yoy,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            ebitda_ttm,
            ebitda_margin_ttm,
            ebitda_margin_trend_4q,
            gross_margin_trend_4q,
            fcf_ttm,
            fcf_margin_ttm,
            fcf_margin_trend_4q,
            net_debt,
            net_debt_to_ebit,
            net_debt_to_ebitda,
            share_dilution_yoy,
            lifecycle_class,
            fundamental_score,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["as_of_date"],
                row["latest_period_end_date"],
                row["revenue_ttm"],
                row["revenue_growth_ttm_yoy"],
                row["ebit_ttm"],
                row["ebit_growth_ttm_yoy"],
                row["ebit_margin_ttm"],
                row["ebit_margin_trend_4q"],
                row["ebitda_ttm"],
                row["ebitda_margin_ttm"],
                row["ebitda_margin_trend_4q"],
                row["gross_margin_trend_4q"],
                row["fcf_ttm"],
                row["fcf_margin_ttm"],
                row["fcf_margin_trend_4q"],
                row["net_debt"],
                row["net_debt_to_ebit"],
                row["net_debt_to_ebitda"],
                row["share_dilution_yoy"],
                row["lifecycle_class"],
                row["fundamental_score"],
                row["run_id"],
            )
            for row in rows_to_write
        ],
    )
    return len(rows_to_write)


def upsert_ttm_business_rows(
    conn: sqlite3.Connection,
    ttm_rows: list[dict[str, Any]],
    *,
    skip_unchanged: bool = False,
) -> tuple[int, list[str]]:
    rows_to_write = _select_ttm_rows_to_write(conn, ttm_rows, skip_unchanged=skip_unchanged)
    conn.executemany(
        """
        INSERT INTO rc_fundamental_ttm (
            ticker,
            as_of_date,
            latest_period_end_date,
            revenue_ttm,
            revenue_growth_ttm_yoy,
            ebit_ttm,
            ebit_growth_ttm_yoy,
            ebit_margin_ttm,
            ebit_margin_trend_4q,
            ebitda_ttm,
            ebitda_margin_ttm,
            ebitda_margin_trend_4q,
            gross_margin_trend_4q,
            fcf_ttm,
            fcf_margin_ttm,
            fcf_margin_trend_4q,
            net_debt,
            net_debt_to_ebit,
            net_debt_to_ebitda,
            share_dilution_yoy,
            lifecycle_class,
            fundamental_score,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, as_of_date) DO UPDATE SET
            latest_period_end_date = excluded.latest_period_end_date,
            revenue_ttm = excluded.revenue_ttm,
            revenue_growth_ttm_yoy = excluded.revenue_growth_ttm_yoy,
            ebit_ttm = excluded.ebit_ttm,
            ebit_growth_ttm_yoy = excluded.ebit_growth_ttm_yoy,
            ebit_margin_ttm = excluded.ebit_margin_ttm,
            ebit_margin_trend_4q = excluded.ebit_margin_trend_4q,
            ebitda_ttm = excluded.ebitda_ttm,
            ebitda_margin_ttm = excluded.ebitda_margin_ttm,
            ebitda_margin_trend_4q = excluded.ebitda_margin_trend_4q,
            gross_margin_trend_4q = excluded.gross_margin_trend_4q,
            fcf_ttm = excluded.fcf_ttm,
            fcf_margin_ttm = excluded.fcf_margin_ttm,
            fcf_margin_trend_4q = excluded.fcf_margin_trend_4q,
            net_debt = excluded.net_debt,
            net_debt_to_ebit = excluded.net_debt_to_ebit,
            net_debt_to_ebitda = excluded.net_debt_to_ebitda,
            share_dilution_yoy = excluded.share_dilution_yoy,
            run_id = excluded.run_id
        """,
        [
            (
                row["ticker"],
                row["as_of_date"],
                row["latest_period_end_date"],
                row["revenue_ttm"],
                row["revenue_growth_ttm_yoy"],
                row["ebit_ttm"],
                row["ebit_growth_ttm_yoy"],
                row["ebit_margin_ttm"],
                row["ebit_margin_trend_4q"],
                row["ebitda_ttm"],
                row["ebitda_margin_ttm"],
                row["ebitda_margin_trend_4q"],
                row["gross_margin_trend_4q"],
                row["fcf_ttm"],
                row["fcf_margin_ttm"],
                row["fcf_margin_trend_4q"],
                row["net_debt"],
                row["net_debt_to_ebit"],
                row["net_debt_to_ebitda"],
                row["share_dilution_yoy"],
                row["lifecycle_class"],
                row["fundamental_score"],
                row["run_id"],
            )
            for row in rows_to_write
        ],
    )
    return len(rows_to_write), [str(row["as_of_date"]) for row in rows_to_write]


def build_and_insert_ttm_rows(
    conn: sqlite3.Connection,
    ticker: str,
    run_id: str,
    dry_run: bool,
    target_quarter_period_end_date: str | None = None,
    skip_unchanged: bool = False,
) -> tuple[int, int, str | None, str | None]:
    quarterly_rows = load_quarterly_rows(conn, ticker)
    ttm_rows = build_ttm_rows(quarterly_rows, run_id)
    write_rows = filter_ttm_rows_depending_on_quarter(ttm_rows, quarterly_rows, target_quarter_period_end_date)
    quarterly_row_count = len(quarterly_rows)
    ttm_rows_written = len(write_rows)
    first_as_of_date = write_rows[0]["as_of_date"] if write_rows else None
    last_as_of_date = write_rows[-1]["as_of_date"] if write_rows else None

    if dry_run:
        if skip_unchanged:
            ttm_rows_written = len(_select_ttm_rows_to_write(conn, write_rows, skip_unchanged=True))
        return quarterly_row_count, ttm_rows_written, first_as_of_date, last_as_of_date

    if target_quarter_period_end_date is None:
        ttm_rows_written = insert_ttm_rows(conn, write_rows, skip_unchanged=skip_unchanged)
    else:
        ttm_rows_written, _written_dates = upsert_ttm_business_rows(conn, write_rows, skip_unchanged=skip_unchanged)
    conn.commit()
    return quarterly_row_count, ttm_rows_written, first_as_of_date, last_as_of_date
