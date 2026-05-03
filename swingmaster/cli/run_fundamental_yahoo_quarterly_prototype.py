from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_SYMBOL = "NOKIA.HE"
SNAPSHOT_SOURCE = "snapshot"
ORDINARY_SOURCE = "ordinary_shares_number"
ISSUED_MINUS_TREASURY_SOURCE = "issued_minus_treasury"
FIELD_CANDIDATES = {
    "revenue": ("Total Revenue", "Operating Revenue"),
    "gross_profit": ("Gross Profit",),
    "operating_income": ("Operating Income", "Total Operating Income As Reported", "EBIT"),
    "net_income": ("Net Income", "Net Income Common Stockholders", "Net Income Continuous Operations"),
    "operating_cashflow": ("Operating Cash Flow", "Cash Flow From Continuing Operating Activities"),
    "capex": ("Capital Expenditure", "Purchase Of PPE", "Net PPE Purchase And Sale"),
    "free_cashflow": ("Free Cash Flow",),
    "cash": ("Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"),
    "total_debt": ("Total Debt", "Long Term Debt And Capital Lease Obligation"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print Yahoo raw -> normalized quarterly prototype for NOKIA.HE")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Ticker symbol")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def load_latest_yahoo_raw_row(conn: sqlite3.Connection, symbol: str) -> sqlite3.Row:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT *
            FROM rc_fundamental_yahoo_raw
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
    finally:
        conn.row_factory = previous_row_factory
    if row is None:
        raise RuntimeError(f"YAHOO_RAW_NOT_FOUND:{symbol}")
    return row


def load_json(text: str) -> dict[str, Any]:
    return json.loads(text)


def collect_period_end_dates(payloads: list[dict[str, Any]]) -> list[str]:
    periods: set[str] = set()
    for payload in payloads:
        columns = payload.get("columns")
        if isinstance(columns, list):
            periods.update(str(column) for column in columns)
    return sorted(periods)


def extract_statement_series(statement_payload: dict[str, Any], field_name: str) -> dict[str, Any]:
    index = statement_payload.get("index")
    columns = statement_payload.get("columns")
    data = statement_payload.get("data")
    if not isinstance(index, list) or not isinstance(columns, list) or not isinstance(data, list):
        return {}
    try:
        row_idx = index.index(field_name)
    except ValueError:
        return {}
    if row_idx >= len(data):
        return {}
    row_values = data[row_idx]
    if not isinstance(row_values, list):
        return {}
    return {
        str(period_end_date): row_values[col_idx]
        for col_idx, period_end_date in enumerate(columns)
        if col_idx < len(row_values)
    }


def normalize_positive_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if numeric_value <= 0:
        return None
    return numeric_value


def normalize_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def resolve_snapshot_fallback(info_payload: dict[str, Any], fast_info_payload: dict[str, Any]) -> float | None:
    info_value = normalize_positive_number(info_payload.get("sharesOutstanding"))
    if info_value is not None:
        return info_value
    return normalize_positive_number(fast_info_payload.get("shares"))


def resolve_series_value(statement_payload: dict[str, Any], period_end_date: str, candidate_names: tuple[str, ...]) -> float | None:
    for candidate_name in candidate_names:
        series = extract_statement_series(statement_payload, candidate_name)
        value = normalize_number(series.get(period_end_date))
        if value is not None:
            return value
    return None


def build_normalized_rows(raw_row: sqlite3.Row) -> list[dict[str, Any]]:
    info_payload = load_json(str(raw_row["info_json"]))
    fast_info_payload = load_json(str(raw_row["fast_info_json"]))
    income_payload = load_json(str(raw_row["quarterly_income_stmt_json"]))
    balance_payload = load_json(str(raw_row["quarterly_balance_sheet_json"]))
    cashflow_payload = load_json(str(raw_row["quarterly_cashflow_json"]))

    periods = collect_period_end_dates([income_payload, balance_payload, cashflow_payload])
    ordinary_series = extract_statement_series(balance_payload, "Ordinary Shares Number")
    issued_series = extract_statement_series(balance_payload, "Share Issued")
    treasury_series = extract_statement_series(balance_payload, "Treasury Shares Number")
    snapshot_value = resolve_snapshot_fallback(info_payload, fast_info_payload)

    rows: list[dict[str, Any]] = []
    previous_valid_value: float | None = None

    for period_end_date in periods:
        revenue = resolve_series_value(income_payload, period_end_date, FIELD_CANDIDATES["revenue"])
        gross_profit = resolve_series_value(income_payload, period_end_date, FIELD_CANDIDATES["gross_profit"])
        operating_income = resolve_series_value(income_payload, period_end_date, FIELD_CANDIDATES["operating_income"])
        net_income = resolve_series_value(income_payload, period_end_date, FIELD_CANDIDATES["net_income"])
        operating_cashflow = resolve_series_value(cashflow_payload, period_end_date, FIELD_CANDIDATES["operating_cashflow"])
        capex = resolve_series_value(cashflow_payload, period_end_date, FIELD_CANDIDATES["capex"])
        free_cashflow = resolve_series_value(cashflow_payload, period_end_date, FIELD_CANDIDATES["free_cashflow"])
        cash = resolve_series_value(balance_payload, period_end_date, FIELD_CANDIDATES["cash"])
        total_debt = resolve_series_value(balance_payload, period_end_date, FIELD_CANDIDATES["total_debt"])

        if free_cashflow is None and operating_cashflow is not None and capex is not None:
            free_cashflow = operating_cashflow + capex

        shares_outstanding: float | None = None
        shares_source = ""

        ordinary_value = normalize_positive_number(ordinary_series.get(period_end_date))
        if ordinary_value is not None:
            shares_outstanding = ordinary_value
            shares_source = ORDINARY_SOURCE
        else:
            issued_value = normalize_positive_number(issued_series.get(period_end_date))
            treasury_value = normalize_positive_number(treasury_series.get(period_end_date))
            if issued_value is not None and treasury_value is not None:
                issued_minus_treasury = normalize_positive_number(issued_value - treasury_value)
                if issued_minus_treasury is not None:
                    shares_outstanding = issued_minus_treasury
                    shares_source = ISSUED_MINUS_TREASURY_SOURCE
            if shares_outstanding is None and snapshot_value is not None:
                shares_outstanding = snapshot_value
                shares_source = SNAPSHOT_SOURCE

        if shares_outstanding is None:
            shares_quality = "MISSING"
        elif shares_source == SNAPSHOT_SOURCE:
            shares_quality = "REVIEW"
        elif previous_valid_value is None:
            shares_quality = "OK"
        else:
            qoq_change_ratio = abs(shares_outstanding - previous_valid_value) / previous_valid_value
            shares_quality = "OK" if qoq_change_ratio <= 0.25 else "REVIEW"

        rows.append(
            {
                "period_end_date": period_end_date,
                "revenue": revenue,
                "gross_profit": gross_profit,
                "operating_income": operating_income,
                "net_income": net_income,
                "operating_cashflow": operating_cashflow,
                "capex": capex,
                "free_cashflow": free_cashflow,
                "cash": cash,
                "total_debt": total_debt,
                "shares_outstanding": shares_outstanding,
                "shares_source": shares_source,
                "shares_quality": shares_quality,
            }
        )
        if shares_outstanding is not None:
            previous_valid_value = shares_outstanding

    return rows


def row_has_statement_values(row: dict[str, Any]) -> bool:
    return any(
        row[field_name] is not None
        for field_name in (
            "revenue",
            "gross_profit",
            "operating_income",
            "net_income",
            "operating_cashflow",
            "capex",
            "free_cashflow",
            "cash",
            "total_debt",
        )
    )


def should_persist_row(row: dict[str, Any]) -> bool:
    return row_has_statement_values(row)


def format_rows(rows: list[dict[str, Any]]) -> list[str]:
    output = [
        "period_end_date\trevenue\tgross_profit\toperating_income\tnet_income\toperating_cashflow\tcapex\tfree_cashflow\tcash\ttotal_debt\tshares_outstanding\tshares_source\tshares_quality"
    ]
    for row in rows:
        value_map = {
            key: "" if row[key] is None else f"{float(row[key]):.1f}"
            for key in (
                "revenue",
                "gross_profit",
                "operating_income",
                "net_income",
                "operating_cashflow",
                "capex",
                "free_cashflow",
                "cash",
                "total_debt",
                "shares_outstanding",
            )
        }
        output.append(
            "\t".join(
                (
                    str(row["period_end_date"]),
                    value_map["revenue"],
                    value_map["gross_profit"],
                    value_map["operating_income"],
                    value_map["net_income"],
                    value_map["operating_cashflow"],
                    value_map["capex"],
                    value_map["free_cashflow"],
                    value_map["cash"],
                    value_map["total_debt"],
                    value_map["shares_outstanding"],
                    str(row["shares_source"]),
                    str(row["shares_quality"]),
                )
            )
        )
    return output


def run_yahoo_quarterly_prototype(db_path: Path, symbol: str) -> dict[str, Any]:
    with sqlite3.connect(str(db_path)) as conn:
        raw_row = load_latest_yahoo_raw_row(conn, symbol.upper())
    rows = build_normalized_rows(raw_row)
    ok_count = sum(1 for row in rows if row["shares_quality"] == "OK")
    review_count = sum(1 for row in rows if row["shares_quality"] == "REVIEW")
    missing_count = sum(1 for row in rows if row["shares_quality"] == "MISSING")
    return {
        "symbol": symbol.upper(),
        "source_run_id": str(raw_row["run_id"]),
        "periods_total": len(rows),
        "rows_normalized": len(rows),
        "ok_count": ok_count,
        "review_count": review_count,
        "missing_count": missing_count,
        "rows": rows,
    }


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    result = run_yahoo_quarterly_prototype(db_path, args.symbol)
    _summary(symbol=result["symbol"])
    _summary(source_run_id=result["source_run_id"])
    _summary(periods_total=result["periods_total"])
    _summary(rows_normalized=result["rows_normalized"])
    _summary(ok_count=result["ok_count"])
    _summary(review_count=result["review_count"])
    _summary(missing_count=result["missing_count"])
    for line in format_rows(result["rows"]):
        print(line)


if __name__ == "__main__":
    main()
