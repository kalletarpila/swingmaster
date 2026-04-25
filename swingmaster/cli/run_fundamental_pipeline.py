from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from swingmaster.cli.run_fundamental_bootstrap_sec_raw import SEC_USER_AGENT, run_sec_raw_bootstrap
from swingmaster.cli.run_fundamental_score_explain import build_explain_rows, format_explain_output, load_rows_for_explain
from swingmaster.cli.run_fundamental_sec_reconstruct_quarterly import run_sec_reconstruct_quarterly
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
    FUND_LIFECYCLE_RULE_V2,
    classify_lifecycle,
    run_lifecycle_classification,
)
from swingmaster.fundamentals.score import FUND_SCORE_RULE_V1, calculate_fundamental_score, run_fundamental_scoring


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the complete ticker-level fundamentals pipeline")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--run-id", required=True, help="Deterministic base run identifier")
    parser.add_argument(
        "--source",
        choices=("sec_edgar", "yfinance"),
        default="sec_edgar",
        help="Raw fundamentals source routing",
    )
    parser.add_argument(
        "--retrieved-at-utc",
        default=None,
        help="Deterministic retrieved timestamp required for SEC fetch path",
    )
    parser.add_argument("--dry-run", action="store_true", help="Run all supported steps in dry-run mode")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip raw fetch and use existing raw rows")
    parser.add_argument("--explain-score", action="store_true", help="Run read-only score explain after score step")
    parser.add_argument("--explain-limit", type=int, default=None, help="Optional latest N TTM rows to explain")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def derive_child_run_ids(run_id: str, source: str) -> dict[str, str]:
    if source == "sec_edgar":
        return {
            "sec_raw": f"{run_id}__SEC_RAW",
            "sec_quarterly_recon": f"{run_id}__SEC_QUARTERLY_RECON",
            "quarterly": f"{run_id}__QUARTERLY",
            "ttm": f"{run_id}__TTM",
            "lifecycle": f"{run_id}__LIFECYCLE",
            "score": f"{run_id}__SCORE",
        }
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
    source: str = "sec_edgar",
    retrieved_at_utc: str | None = None,
    explain_score: bool = False,
    explain_limit: int | None = None,
) -> dict[str, object]:
    normalized_ticker = ticker.upper()
    if dry_run and explain_score:
        raise RuntimeError("FUNDAMENTAL_PIPELINE_EXPLAIN_NOT_ALLOWED_IN_DRY_RUN")
    if source == "sec_edgar" and not skip_fetch and not retrieved_at_utc:
        raise RuntimeError("FUNDAMENTAL_PIPELINE_RETRIEVED_AT_UTC_REQUIRED_FOR_SEC")

    child_run_ids = derive_child_run_ids(run_id, source)
    sec_raw_status = "not-applicable"
    sec_reconstruct_status = "not-applicable"
    raw_status = "not-applicable" if source == "sec_edgar" else "skipped"
    quarterly_status = "dry-run" if dry_run else "ok"
    ttm_status = "dry-run" if dry_run else "ok"
    lifecycle_status = "dry-run" if dry_run else "ok"
    score_status = "dry-run" if dry_run else "ok"
    explain_status = "skipped"
    statements = None

    if source == "sec_edgar":
        if skip_fetch:
            sec_raw_status = "skipped"
            sec_reconstruct_status = "skipped"
        else:
            assert retrieved_at_utc is not None
            run_sec_raw_bootstrap(
                db_path=db_path,
                ticker=normalized_ticker,
                run_id=child_run_ids["sec_raw"],
                retrieved_at_utc=retrieved_at_utc,
                user_agent=SEC_USER_AGENT,
                dry_run=dry_run,
            )
            sec_raw_status = "dry-run" if dry_run else "ok"
            run_sec_reconstruct_quarterly(
                db_path=db_path,
                ticker=normalized_ticker,
                run_id=child_run_ids["sec_quarterly_recon"],
                retrieved_at_utc=retrieved_at_utc,
                dry_run=dry_run,
            )
            sec_reconstruct_status = "dry-run" if dry_run else "ok"
            if dry_run:
                quarterly_status = "skipped"
                ttm_status = "skipped"
                lifecycle_status = "skipped"
                score_status = "skipped"
                return {
                    "ticker": normalized_ticker,
                    "db_path": str(db_path),
                    "run_id": run_id,
                    "source": source,
                    "skip_fetch": "false",
                    "dry_run": "true",
                    "sec_raw_status": sec_raw_status,
                    "sec_reconstruct_status": sec_reconstruct_status,
                    "raw_status": raw_status,
                    "quarterly_status": quarterly_status,
                    "ttm_status": ttm_status,
                    "lifecycle_status": lifecycle_status,
                    "score_status": score_status,
                    "explain_status": explain_status,
                    "status": "ok",
                    "child_run_ids": child_run_ids,
                    "lifecycle_rule_id": FUND_LIFECYCLE_RULE_V2,
                    "score_rule_id": FUND_SCORE_RULE_V1,
                }
    else:
        if not skip_fetch:
            statements = fetch_quarterly_statements_raw(normalized_ticker)
            validate_non_empty_statements(statements)
            if dry_run:
                sum(count_statement_rows(statements[statement_type]) for statement_type in SUPPORTED_STATEMENT_TYPES)
                raw_status = "dry-run"
            else:
                with sqlite3.connect(str(db_path)) as conn:
                    for statement_type in SUPPORTED_STATEMENT_TYPES:
                        insert_raw_statement_rows(
                            conn=conn,
                            ticker=normalized_ticker,
                            statement_type=statement_type,
                            dataframe=statements[statement_type],
                            run_id=child_run_ids["raw"],
                        )
                    conn.commit()
                raw_status = "ok"

    with sqlite3.connect(str(db_path)) as conn:
        if dry_run:
            if source == "yfinance" and skip_fetch:
                raw_rows = load_raw_statement_rows(conn, normalized_ticker)
            elif source == "yfinance":
                assert statements is not None
                raw_rows = _build_raw_rows_from_statements(normalized_ticker, statements)
            else:
                raw_rows = load_raw_statement_rows(conn, normalized_ticker)
            quarterly_rows = build_quarterly_rows(raw_rows, child_run_ids["quarterly"])
            ttm_rows = build_ttm_rows(quarterly_rows, child_run_ids["ttm"])
            for ttm_row in ttm_rows:
                ttm_row["lifecycle_class"] = classify_lifecycle(ttm_row)
                ttm_row["fundamental_score"] = calculate_fundamental_score(ttm_row)
        else:
            build_and_insert_quarterly_rows(
                conn=conn,
                ticker=normalized_ticker,
                run_id=child_run_ids["quarterly"],
                dry_run=False,
            )
            build_and_insert_ttm_rows(
                conn=conn,
                ticker=normalized_ticker,
                run_id=child_run_ids["ttm"],
                dry_run=False,
            )
            run_lifecycle_classification(
                conn=conn,
                ticker=normalized_ticker,
                dry_run=False,
            )
            run_fundamental_scoring(
                conn=conn,
                ticker=normalized_ticker,
                dry_run=False,
            )
            if explain_score:
                explain_rows = build_explain_rows(load_rows_for_explain(conn, normalized_ticker, explain_limit))
                print(format_explain_output(normalized_ticker, explain_rows))
                explain_status = "ok"

    return {
        "ticker": normalized_ticker,
        "db_path": str(db_path),
        "run_id": run_id,
        "source": source,
        "skip_fetch": "true" if skip_fetch else "false",
        "dry_run": "true" if dry_run else "false",
        "sec_raw_status": sec_raw_status,
        "sec_reconstruct_status": sec_reconstruct_status,
        "raw_status": raw_status,
        "quarterly_status": quarterly_status,
        "ttm_status": ttm_status,
        "lifecycle_status": lifecycle_status,
        "score_status": score_status,
        "explain_status": explain_status,
        "status": "ok",
        "child_run_ids": child_run_ids,
        "lifecycle_rule_id": FUND_LIFECYCLE_RULE_V2,
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
        source=args.source,
        retrieved_at_utc=args.retrieved_at_utc,
        dry_run=args.dry_run,
        skip_fetch=args.skip_fetch,
        explain_score=args.explain_score,
        explain_limit=args.explain_limit,
    )
    _summary(ticker=summary["ticker"])
    _summary(db_path=summary["db_path"])
    _summary(run_id=summary["run_id"])
    _summary(source=summary["source"])
    _summary(skip_fetch=summary["skip_fetch"])
    _summary(dry_run=summary["dry_run"])
    _summary(sec_raw_status=summary["sec_raw_status"])
    _summary(sec_reconstruct_status=summary["sec_reconstruct_status"])
    _summary(raw_status=summary["raw_status"])
    _summary(quarterly_status=summary["quarterly_status"])
    _summary(ttm_status=summary["ttm_status"])
    _summary(lifecycle_status=summary["lifecycle_status"])
    _summary(score_status=summary["score_status"])
    _summary(explain_status=summary["explain_status"])
    _summary(status=summary["status"])


if __name__ == "__main__":
    main()
