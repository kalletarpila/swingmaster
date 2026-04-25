from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.build_quarterly import (
    build_and_insert_quarterly_rows,
    build_quarterly_rows,
    load_raw_statement_rows,
)
from swingmaster.fundamentals.build_ttm import build_and_insert_ttm_rows, build_ttm_rows
from swingmaster.fundamentals.fetch_raw_statements import (
    SUPPORTED_STATEMENT_TYPES,
    count_statement_rows,
    fetch_quarterly_statements_raw,
    insert_raw_statement_rows,
    validate_non_empty_statements,
)
from swingmaster.fundamentals.lifecycle import (
    FUND_LIFECYCLE_RULE_V1,
    classify_lifecycle,
    run_lifecycle_classification,
)
from swingmaster.fundamentals.score import (
    FUND_SCORE_RULE_V1,
    calculate_fundamental_score,
    run_fundamental_scoring,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the complete ticker-level fundamentals pipeline")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic base run identifier")
    parser.add_argument("--dry-run", action="store_true", help="Run all supported steps in dry-run mode")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip raw fetch and use existing raw rows")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def derive_child_run_ids(run_id: str) -> dict[str, str]:
    return {
        "raw": f"{run_id}__RAW",
        "quarterly": f"{run_id}__QUARTERLY",
        "ttm": f"{run_id}__TTM",
        "lifecycle": f"{run_id}__LIFECYCLE",
        "score": f"{run_id}__SCORE",
    }


def run_fundamental_pipeline(
    db_path: Path,
    ticker: str,
    run_id: str,
    dry_run: bool,
    skip_fetch: bool,
) -> dict[str, object]:
    child_run_ids = derive_child_run_ids(run_id)
    raw_status = "skipped"
    quarterly_status = "dry-run" if dry_run else "ok"
    ttm_status = "dry-run" if dry_run else "ok"
    lifecycle_status = "dry-run" if dry_run else "ok"
    score_status = "dry-run" if dry_run else "ok"
    statements = None

    if not skip_fetch:
        statements = fetch_quarterly_statements_raw(ticker)
        validate_non_empty_statements(statements)
        if dry_run:
            sum(count_statement_rows(statements[statement_type]) for statement_type in SUPPORTED_STATEMENT_TYPES)
            raw_status = "dry-run"
        else:
            with sqlite3.connect(str(db_path)) as conn:
                for statement_type in SUPPORTED_STATEMENT_TYPES:
                    insert_raw_statement_rows(
                        conn=conn,
                        ticker=ticker,
                        statement_type=statement_type,
                        dataframe=statements[statement_type],
                        run_id=child_run_ids["raw"],
                    )
                conn.commit()
            raw_status = "ok"

    with sqlite3.connect(str(db_path)) as conn:
        if dry_run:
            if skip_fetch:
                raw_rows = load_raw_statement_rows(conn, ticker)
            else:
                assert statements is not None
                raw_rows = _build_raw_rows_from_statements(ticker, statements)
            quarterly_rows = build_quarterly_rows(raw_rows, child_run_ids["quarterly"])
            ttm_rows = build_ttm_rows(quarterly_rows, child_run_ids["ttm"])
            for ttm_row in ttm_rows:
                ttm_row["lifecycle_class"] = classify_lifecycle(ttm_row)
                ttm_row["fundamental_score"] = calculate_fundamental_score(ttm_row)
        else:
            build_and_insert_quarterly_rows(
                conn=conn,
                ticker=ticker,
                run_id=child_run_ids["quarterly"],
                dry_run=False,
            )
            build_and_insert_ttm_rows(
                conn=conn,
                ticker=ticker,
                run_id=child_run_ids["ttm"],
                dry_run=False,
            )
            run_lifecycle_classification(
                conn=conn,
                ticker=ticker,
                dry_run=False,
            )
            run_fundamental_scoring(
                conn=conn,
                ticker=ticker,
                dry_run=False,
            )

    return {
        "ticker": ticker,
        "db_path": str(db_path),
        "run_id": run_id,
        "skip_fetch": "true" if skip_fetch else "false",
        "dry_run": "true" if dry_run else "false",
        "raw_status": raw_status,
        "quarterly_status": quarterly_status,
        "ttm_status": ttm_status,
        "lifecycle_status": lifecycle_status,
        "score_status": score_status,
        "status": "ok",
        "child_run_ids": child_run_ids,
        "lifecycle_rule_id": FUND_LIFECYCLE_RULE_V1,
        "score_rule_id": FUND_SCORE_RULE_V1,
    }


def _build_raw_rows_from_statements(
    ticker: str,
    statements: dict[str, object],
) -> list[dict[str, object]]:
    raw_rows: list[dict[str, object]] = []
    for statement_type in SUPPORTED_STATEMENT_TYPES:
        dataframe = statements[statement_type]
        for field_name in dataframe.index:
            for period_end_date in dataframe.columns:
                raw_rows.append(
                    {
                        "ticker": ticker,
                        "statement_type": str(statement_type),
                        "period_end_date": str(period_end_date),
                        "field_name": str(field_name),
                        "field_value": dataframe.at[field_name, period_end_date],
                    }
                )
    return raw_rows


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    summary = run_fundamental_pipeline(
        db_path=db_path,
        ticker=args.ticker,
        run_id=args.run_id,
        dry_run=args.dry_run,
        skip_fetch=args.skip_fetch,
    )
    _summary(ticker=summary["ticker"])
    _summary(db_path=summary["db_path"])
    _summary(run_id=summary["run_id"])
    _summary(skip_fetch=summary["skip_fetch"])
    _summary(dry_run=summary["dry_run"])
    _summary(raw_status=summary["raw_status"])
    _summary(quarterly_status=summary["quarterly_status"])
    _summary(ttm_status=summary["ttm_status"])
    _summary(lifecycle_status=summary["lifecycle_status"])
    _summary(score_status=summary["score_status"])
    _summary(status=summary["status"])


if __name__ == "__main__":
    main()
