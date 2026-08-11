from __future__ import annotations

from datetime import date
import sqlite3
from typing import Any


FIELD_MAPPINGS: dict[str, tuple[str, tuple[str, ...]]] = {
    "revenue": (
        "income",
        (
            "Total Revenue",
            "Revenue",
            "Operating Revenue",
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
        ),
    ),
    "gross_profit": ("income", ("Gross Profit",)),
    "operating_income": ("income", ("Operating Income", "EBIT", "OperatingIncomeLoss")),
    "ebit": ("income", ("EBIT", "Operating Income", "OperatingIncomeLoss")),
    "ebitda": ("income", ("EBITDA", "Normalized EBITDA")),
    "net_income": ("income", ("Net Income", "Net Income Common Stockholders")),
    "operating_cashflow": (
        "cashflow",
        (
            "Operating Cash Flow",
            "Total Cash From Operating Activities",
            "Cash Flow From Continuing Operating Activities",
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        ),
    ),
    "capex": (
        "cashflow",
        (
            "Capital Expenditure",
            "Capital Expenditures",
            "Capital Spending",
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PaymentsToAcquireProductiveAssets",
        ),
    ),
    "cash": (
        "balance",
        (
            "Cash And Cash Equivalents",
            "Cash Cash Equivalents And Short Term Investments",
            "Cash And Short Term Investments",
            "CashAndCashEquivalentsAtCarryingValue",
        ),
    ),
    "total_debt": (
        "balance",
        (
            "Total Debt",
            "Long Term Debt And Capital Lease Obligation",
            "Long Term Debt",
        ),
    ),
    "shares_outstanding": (
        "balance",
        (
            "Ordinary Shares Number",
            "Share Issued",
            "Common Stock Shares Outstanding",
            "EntityCommonStockSharesOutstanding",
            "WeightedAverageNumberOfSharesOutstandingBasic",
            "WeightedAverageNumberOfDilutedSharesOutstanding",
        ),
    ),
}


def load_raw_statement_rows(conn: sqlite3.Connection, ticker: str) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                ticker,
                statement_type,
                period_end_date,
                field_name,
                field_value
            FROM rc_fundamental_statement_raw
            WHERE ticker = ?
            ORDER BY period_end_date ASC, statement_type ASC, field_name ASC
            """,
            (ticker,),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    if not rows:
        raise RuntimeError(f"FUNDAMENTAL_RAW_NOT_FOUND:{ticker}")
    return rows


def build_quarterly_rows(raw_rows: list[sqlite3.Row], run_id: str) -> list[dict[str, Any]]:
    raw_lookup: dict[tuple[str, str, str], float | None] = {}
    periods: list[str] = []
    seen_periods: set[str] = set()
    ticker = str(raw_rows[0]["ticker"])

    for row in raw_rows:
        period_end_date = str(row["period_end_date"])
        statement_type = str(row["statement_type"])
        field_name = str(row["field_name"])
        raw_lookup[(statement_type, period_end_date, field_name)] = row["field_value"]
        base_field_name = field_name.split("|", 1)[0]
        raw_lookup.setdefault((statement_type, period_end_date, base_field_name), row["field_value"])
        if period_end_date not in seen_periods:
            seen_periods.add(period_end_date)
            periods.append(period_end_date)

    quarterly_rows: list[dict[str, Any]] = []
    for period_end_date in sorted(periods):
        quarterly_row: dict[str, Any] = {
            "ticker": ticker,
            "period_end_date": period_end_date,
            "revenue": None,
            "gross_profit": None,
            "operating_income": None,
            "ebit": None,
            "ebitda": None,
            "net_income": None,
            "operating_cashflow": None,
            "capex": None,
            "free_cashflow": None,
            "cash": None,
            "total_debt": None,
            "shares_outstanding": None,
            "currency": None,
            "run_id": run_id,
        }
        for normalized_field, (statement_type, candidate_names) in FIELD_MAPPINGS.items():
            quarterly_row[normalized_field] = _resolve_field_value(raw_lookup, statement_type, period_end_date, candidate_names)

        if quarterly_row["total_debt"] is None:
            quarterly_row["total_debt"] = _resolve_total_debt_value(raw_lookup, period_end_date)

        operating_cashflow = quarterly_row["operating_cashflow"]
        capex = quarterly_row["capex"]
        if operating_cashflow is not None and capex is not None:
            quarterly_row["free_cashflow"] = operating_cashflow + capex

        quarterly_rows.append(quarterly_row)
    return quarterly_rows


def _resolve_field_value(
    raw_lookup: dict[tuple[str, str, str], float | None],
    statement_type: str,
    period_end_date: str,
    candidate_names: tuple[str, ...],
) -> float | None:
    for candidate_name in candidate_names:
        raw_key = (statement_type, period_end_date, candidate_name)
        if raw_key in raw_lookup:
            return raw_lookup[raw_key]
    return None


def _resolve_total_debt_value(
    raw_lookup: dict[tuple[str, str, str], float | None],
    period_end_date: str,
) -> float | None:
    current = raw_lookup.get(("balance", period_end_date, "LongTermDebtCurrent"))
    noncurrent = raw_lookup.get(("balance", period_end_date, "LongTermDebtNoncurrent"))
    short_term = raw_lookup.get(("balance", period_end_date, "ShortTermBorrowings"))
    values = [value for value in (current, noncurrent, short_term) if value is not None]
    if not values:
        return None
    return float(sum(values))


QUARTERLY_BUSINESS_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
    "currency",
)


def target_compatible_period(period_end_date: str, target_period_end_date: str, *, tolerance_days: int = 7) -> bool:
    period_date = date.fromisoformat(period_end_date)
    target_date = date.fromisoformat(target_period_end_date)
    return (
        period_date.year == target_date.year
        and _calendar_quarter(period_date) == _calendar_quarter(target_date)
        and abs((period_date - target_date).days) <= tolerance_days
    )


def filter_target_compatible_quarterly_rows(
    quarterly_rows: list[dict[str, Any]],
    target_period_end_date: str | None,
) -> list[dict[str, Any]]:
    if target_period_end_date is None:
        return quarterly_rows
    return [
        row
        for row in quarterly_rows
        if target_compatible_period(str(row["period_end_date"]), target_period_end_date)
    ]


def _calendar_quarter(value: date) -> int:
    return ((value.month - 1) // 3) + 1


def _values_equal(left: Any, right: Any) -> bool:
    return left == right


def _quarterly_row_materially_changed(existing: sqlite3.Row | None, row: dict[str, Any]) -> bool:
    if existing is None:
        return True
    return any(not _values_equal(existing[field], row[field]) for field in QUARTERLY_BUSINESS_FIELDS)


def _preserve_existing_values_for_null_fields(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        merged_rows: list[dict[str, Any]] = []
        for row in rows:
            existing = conn.execute(
                """
                SELECT *
                FROM rc_fundamental_quarterly
                WHERE ticker = ?
                  AND period_end_date = ?
                """,
                (row["ticker"], row["period_end_date"]),
            ).fetchone()
            if existing is None:
                merged_rows.append(row)
                continue
            merged = dict(row)
            for field in QUARTERLY_BUSINESS_FIELDS:
                if merged.get(field) is None and existing[field] is not None:
                    merged[field] = existing[field]
            merged_rows.append(merged)
        return merged_rows
    finally:
        conn.row_factory = previous_row_factory


def insert_quarterly_rows(
    conn: sqlite3.Connection,
    quarterly_rows: list[dict[str, Any]],
    *,
    skip_unchanged: bool = False,
) -> int:
    rows_to_write = quarterly_rows
    if skip_unchanged:
        previous_row_factory = conn.row_factory
        conn.row_factory = sqlite3.Row
        try:
            rows_to_write = []
            for row in quarterly_rows:
                existing = conn.execute(
                    """
                    SELECT *
                    FROM rc_fundamental_quarterly
                    WHERE ticker = ?
                      AND period_end_date = ?
                    """,
                    (row["ticker"], row["period_end_date"]),
                ).fetchone()
                if _quarterly_row_materially_changed(existing, row):
                    rows_to_write.append(row)
        finally:
            conn.row_factory = previous_row_factory

    conn.executemany(
        """
        INSERT OR REPLACE INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            ebit,
            ebitda,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            currency,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["period_end_date"],
                row["revenue"],
                row["gross_profit"],
                row["operating_income"],
                row["ebit"],
                row["ebitda"],
                row["net_income"],
                row["operating_cashflow"],
                row["capex"],
                row["free_cashflow"],
                row["cash"],
                row["total_debt"],
                row["shares_outstanding"],
                row["currency"],
                row["run_id"],
            )
            for row in rows_to_write
        ],
    )
    return len(rows_to_write)


def build_and_insert_quarterly_rows(
    conn: sqlite3.Connection,
    ticker: str,
    run_id: str,
    dry_run: bool,
    target_period_end_date: str | None = None,
    skip_unchanged: bool = False,
) -> tuple[int, int]:
    raw_rows = load_raw_statement_rows(conn, ticker)
    quarterly_rows = build_quarterly_rows(raw_rows, run_id)
    periods_detected = len(quarterly_rows)
    write_rows = filter_target_compatible_quarterly_rows(quarterly_rows, target_period_end_date)
    if target_period_end_date is not None:
        write_rows = _preserve_existing_values_for_null_fields(conn, write_rows)

    if dry_run:
        rows_written = len(write_rows)
        if skip_unchanged:
            previous_row_factory = conn.row_factory
            conn.row_factory = sqlite3.Row
            try:
                rows_written = sum(
                    1
                    for row in write_rows
                    if _quarterly_row_materially_changed(
                        conn.execute(
                            """
                            SELECT *
                            FROM rc_fundamental_quarterly
                            WHERE ticker = ?
                              AND period_end_date = ?
                            """,
                            (row["ticker"], row["period_end_date"]),
                        ).fetchone(),
                        row,
                    )
                )
            finally:
                conn.row_factory = previous_row_factory
        return periods_detected, rows_written

    rows_written = insert_quarterly_rows(conn, write_rows, skip_unchanged=skip_unchanged)
    conn.commit()
    return periods_detected, rows_written
