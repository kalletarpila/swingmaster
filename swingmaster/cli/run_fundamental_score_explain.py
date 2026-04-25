from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.score import FUND_SCORE_RULE_V1, explain_score_components, load_ttm_rows


COMPONENT_ROWS = (
    "growth_component",
    "margin_component",
    "margin_trend_component",
    "fcf_component",
    "leverage_component",
    "dilution_component",
    "lifecycle_component",
    "score_raw",
    "stored_fundamental_score",
    "recomputed_fundamental_score",
)

RAW_FACTOR_ROWS = (
    "revenue_growth_ttm_yoy",
    "ebit_margin_ttm",
    "ebit_margin_trend_4q",
    "fcf_margin_ttm",
    "net_debt_to_ebitda",
    "share_dilution_yoy",
    "lifecycle_class",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explain FUND_SCORE_RULE_V1 score breakdown for TTM rows")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--limit", type=int, default=None, help="Optional latest N rows to include")
    return parser.parse_args()


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def load_rows_for_explain(conn: sqlite3.Connection, ticker: str, limit: int | None) -> list[sqlite3.Row]:
    rows = load_ttm_rows(conn, ticker)
    if limit is not None:
        rows = rows[-limit:]
    return rows


def build_explain_rows(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    explain_rows: list[dict[str, Any]] = []
    for row in rows:
        breakdown = explain_score_components(row)
        stored_score = row["fundamental_score"]
        recomputed_score = breakdown["fundamental_score_recomputed"]
        if stored_score is not None and abs(float(stored_score) - recomputed_score) > 0.000001:
            raise RuntimeError(f"FUNDAMENTAL_SCORE_MISMATCH:{row['ticker']}:{row['as_of_date']}")
        explain_rows.append(
            {
                "ticker": str(row["ticker"]),
                "as_of_date": str(row["as_of_date"]),
                "stored_fundamental_score": stored_score,
                "recomputed_fundamental_score": recomputed_score,
                "revenue_growth_ttm_yoy": row["revenue_growth_ttm_yoy"],
                "ebit_margin_ttm": row["ebit_margin_ttm"],
                "ebit_margin_trend_4q": row["ebit_margin_trend_4q"],
                "fcf_margin_ttm": row["fcf_margin_ttm"],
                "net_debt_to_ebitda": row["net_debt_to_ebitda"],
                "share_dilution_yoy": row["share_dilution_yoy"],
                "lifecycle_class": row["lifecycle_class"],
                **breakdown,
            }
        )
    return explain_rows


def format_explain_output(ticker: str, explain_rows: list[dict[str, Any]]) -> str:
    as_of_dates = [row["as_of_date"] for row in explain_rows]
    lines = [
        "FUNDAMENTAL SCORE EXPLAIN",
        f"ticker={ticker}",
        f"rule_id={FUND_SCORE_RULE_V1}",
        "",
        "SCORE COMPONENTS",
        _format_table(COMPONENT_ROWS, as_of_dates, explain_rows, component_mode=True),
        "",
        "RAW FACTORS",
        _format_table(RAW_FACTOR_ROWS, as_of_dates, explain_rows, component_mode=False),
    ]
    return "\n".join(lines)


def _format_table(
    row_names: tuple[str, ...],
    as_of_dates: list[str],
    explain_rows: list[dict[str, Any]],
    component_mode: bool,
) -> str:
    first_col_width = max(len("metric"), *(len(row_name) for row_name in row_names))
    column_widths = {
        as_of_date: max(
            len(as_of_date),
            *(
                len(_display_value(row[row_name], component_mode))
                for row in explain_rows
                for row_name in row_names
                if row["as_of_date"] == as_of_date
            ),
        )
        for as_of_date in as_of_dates
    }

    header = "metric".ljust(first_col_width)
    for as_of_date in as_of_dates:
        header += " | " + as_of_date.ljust(column_widths[as_of_date])

    rows_out = [header]
    for row_name in row_names:
        line = row_name.ljust(first_col_width)
        for explain_row in explain_rows:
            line += " | " + _display_value(explain_row[row_name], component_mode).ljust(
                column_widths[explain_row["as_of_date"]]
            )
        rows_out.append(line)
    return "\n".join(rows_out)


def _display_value(value: Any, component_mode: bool) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return value
    if component_mode:
        return f"{float(value):.2f}"
    return f"{float(value):.6f}"


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    with sqlite3.connect(str(db_path)) as conn:
        rows = load_rows_for_explain(conn, args.ticker, args.limit)
        explain_rows = build_explain_rows(rows)

    print(format_explain_output(args.ticker, explain_rows))
    first_as_of_date = explain_rows[0]["as_of_date"] if explain_rows else "NULL"
    last_as_of_date = explain_rows[-1]["as_of_date"] if explain_rows else "NULL"
    _summary(rule_id=FUND_SCORE_RULE_V1)
    _summary(ticker=args.ticker)
    _summary(rows_explained=len(explain_rows))
    _summary(first_as_of_date=first_as_of_date)
    _summary(last_as_of_date=last_as_of_date)
    _summary(mismatches=0)
    _summary(status="ok")


if __name__ == "__main__":
    main()
