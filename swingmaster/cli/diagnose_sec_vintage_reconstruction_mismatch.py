from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.build_quarterly import build_quarterly_rows
from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS
from swingmaster.fundamentals.sec_reconstruction_provenance import (
    reconstruct_quarterly_rows_with_provenance,
)


COMPARE_FIELDS = (*REPORTED_FINANCIAL_FIELDS, "currency")
MATCH = "MATCH"
BOTH_NULL = "BOTH_NULL"
LATEST_HAS_VALUE_RECON_NULL = "LATEST_HAS_VALUE_RECON_NULL"
RECON_HAS_VALUE_LATEST_NULL = "RECON_HAS_VALUE_LATEST_NULL"
VALUE_DIFF = "VALUE_DIFF"
CURRENCY_DIFF = "CURRENCY_DIFF"
RECONSTRUCTION_FAILED = "RECONSTRUCTION_FAILED"
RECONSTRUCTION_MISSING_TARGET = "RECONSTRUCTION_MISSING_TARGET"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        result = run_diagnostics(
            fundamentals_db=args.fundamentals_db,
            market=args.market,
            source_run_id=args.source_run_id,
            ticker=args.ticker,
            period_end_date=args.period_end_date,
            sample_limit=args.sample_limit,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_text(result))
    return 0


def run_diagnostics(
    *,
    fundamentals_db: str,
    market: str,
    source_run_id: str | None = None,
    ticker: str | None = None,
    period_end_date: str | None = None,
    sample_limit: int = 20,
) -> dict[str, Any]:
    db_path = Path(fundamentals_db)
    if not db_path.exists():
        raise FileNotFoundError(f"FUNDAMENTALS_DB_NOT_FOUND:{fundamentals_db}")

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        latest_rows = _load_latest_without_vintage_rows(conn, source_run_id, ticker, period_end_date)
        samples: list[dict[str, Any]] = []
        field_status_counts: Counter[str] = Counter()
        mismatched_field_counts: Counter[str] = Counter()
        pattern_counts: Counter[str] = Counter()
        matched_rows = 0
        mismatched_rows = 0
        sec_raw_evidence_count = 0
        yahoo_evidence_count = 0
        enrichment_audit_count = 0
        quarter_state_count = 0

        for latest_row in latest_rows:
            row_result = _diagnose_row(conn, latest_row, market=market)
            sec_raw_evidence_count += 1 if row_result["sec_raw_fact_count"] > 0 else 0
            yahoo_evidence_count += 1 if row_result["yahoo_quarterly_present"] else 0
            enrichment_audit_count += 1 if row_result["enrichment_audit_present"] else 0
            quarter_state_count += 1 if row_result["quarter_state_present"] else 0
            for detail in row_result["field_comparisons"].values():
                field_status_counts[detail["status"]] += 1
            if row_result["mismatched_fields"]:
                mismatched_rows += 1
                for field_name in row_result["mismatched_fields"]:
                    mismatched_field_counts[field_name] += 1
                pattern_counts[",".join(row_result["mismatched_fields"])] += 1
            else:
                matched_rows += 1
                pattern_counts["MATCH_ALL_FIELDS"] += 1
            if len(samples) < sample_limit:
                samples.append(row_result)

        likely_cause = _infer_likely_cause(
            candidate_count=len(latest_rows),
            mismatched_rows=mismatched_rows,
            mismatched_field_counts=mismatched_field_counts,
            yahoo_evidence_count=yahoo_evidence_count,
            enrichment_audit_count=enrichment_audit_count,
        )
        return {
            "summary": {
                "fundamentals_db": str(db_path),
                "market": market,
                "source_run_id": source_run_id,
                "ticker": ticker,
                "period_end_date": period_end_date,
                "candidate_count": len(latest_rows),
                "matched_rows": matched_rows,
                "mismatched_rows": mismatched_rows,
                "sec_raw_evidence_count": sec_raw_evidence_count,
                "yahoo_evidence_count": yahoo_evidence_count,
                "enrichment_audit_count": enrichment_audit_count,
                "quarter_state_count": quarter_state_count,
                "mismatched_field_counts": dict(sorted(mismatched_field_counts.items())),
                "field_status_counts": dict(sorted(field_status_counts.items())),
                "rows_by_mismatch_pattern": dict(pattern_counts.most_common()),
                "likely_cause": likely_cause,
                "recommendation": _recommend(likely_cause),
            },
            "samples": samples,
        }
    finally:
        conn.close()


def _diagnose_row(conn: sqlite3.Connection, latest_row: sqlite3.Row, *, market: str) -> dict[str, Any]:
    ticker = str(latest_row["ticker"]).upper()
    period_end_date = str(latest_row["period_end_date"])
    latest = _row_to_dict(latest_row)
    raw_rows = _load_sec_raw_rows_for_ticker(conn, ticker)
    exact_sec_raw_fact_count = sum(1 for row in raw_rows if str(row["period_end_date"]) == period_end_date)
    reconstructed_row: dict[str, Any] | None = None
    reconstruction_error = None
    if raw_rows:
        try:
            reconstructed_rows, _ = reconstruct_quarterly_rows_with_provenance(
                raw_rows,
                ticker,
                "SEC_DIAGNOSTIC_RECON",
                "1970-01-01T00:00:00Z",
            )
            normalized_rows = build_quarterly_rows(reconstructed_rows, "SEC_DIAGNOSTIC_NORMALIZED")
            reconstructed_row = {
                (str(row["ticker"]).upper(), str(row["period_end_date"])): row
                for row in normalized_rows
            }.get((ticker, period_end_date))
            if reconstructed_row is None:
                reconstruction_error = RECONSTRUCTION_MISSING_TARGET
        except Exception as exc:
            reconstruction_error = f"{RECONSTRUCTION_FAILED}:{exc}"

    field_comparisons: dict[str, dict[str, Any]] = {}
    mismatched_fields: list[str] = []
    for field_name in COMPARE_FIELDS:
        latest_value = latest.get(field_name)
        reconstructed_value = reconstructed_row.get(field_name) if reconstructed_row is not None else None
        status, numeric_diff = _classify_value(field_name, latest_value, reconstructed_value)
        field_comparisons[field_name] = {
            "status": status,
            "latest": latest_value,
            "reconstructed": reconstructed_value,
            "numeric_diff": numeric_diff,
        }
        if status not in (MATCH, BOTH_NULL):
            mismatched_fields.append(field_name)

    if reconstruction_error and not mismatched_fields:
        mismatched_fields.append("reconstruction")

    return {
        "ticker": ticker,
        "period_end_date": period_end_date,
        "latest_run_id": latest.get("run_id"),
        "mismatched_fields": mismatched_fields,
        "field_comparisons": {
            field_name: detail
            for field_name, detail in field_comparisons.items()
            if detail["status"] not in (MATCH, BOTH_NULL)
        },
        "latest_values_for_mismatches": {field: latest.get(field) for field in mismatched_fields if field in COMPARE_FIELDS},
        "reconstructed_values_for_mismatches": {
            field: reconstructed_row.get(field) if reconstructed_row is not None else None
            for field in mismatched_fields
            if field in COMPARE_FIELDS
        },
        "sec_raw_fact_count": exact_sec_raw_fact_count,
        "ticker_sec_raw_fact_count": len(raw_rows),
        "yahoo_quarterly_present": _has_yahoo_quarterly(conn, market, ticker, period_end_date),
        "enrichment_audit_present": _has_enrichment_audit(conn, ticker, period_end_date),
        "quarter_state_present": _has_quarter_state(conn, market, ticker),
        "reconstruction_error": reconstruction_error,
        "suggested_diagnosis": _row_diagnosis(mismatched_fields, reconstruction_error),
    }


def _load_latest_without_vintage_rows(
    conn: sqlite3.Connection,
    source_run_id: str | None,
    ticker: str | None,
    period_end_date: str | None,
) -> list[sqlite3.Row]:
    where_parts = [
        """
        NOT EXISTS (
            SELECT 1
            FROM rc_fundamental_quarterly_vintage v
            WHERE v.ticker = q.ticker
              AND v.period_end_date = q.period_end_date
        )
        """
    ]
    params: list[Any] = []
    if source_run_id:
        where_parts.append("q.run_id = ?")
        params.append(source_run_id)
    if ticker:
        where_parts.append("q.ticker = ?")
        params.append(ticker.upper())
    if period_end_date:
        where_parts.append("q.period_end_date = ?")
        params.append(period_end_date)
    return conn.execute(
        f"""
        SELECT *
        FROM rc_fundamental_quarterly q
        WHERE {" AND ".join(where_parts)}
        ORDER BY ticker ASC, period_end_date ASC
        """,
        params,
    ).fetchall()


def _load_sec_raw_rows_for_ticker(conn: sqlite3.Connection, ticker: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            ticker,
            statement_type,
            period_end_date,
            period_type,
            field_name,
            field_value,
            currency,
            source,
            retrieved_at_utc,
            run_id
        FROM rc_fundamental_statement_raw
        WHERE ticker = ?
          AND source = 'sec_edgar'
          AND period_type = 'sec_fact'
        ORDER BY ticker ASC, statement_type ASC, period_end_date ASC, field_name ASC
        """,
        (ticker,),
    ).fetchall()


def _has_yahoo_quarterly(conn: sqlite3.Connection, market: str, ticker: str, period_end_date: str) -> bool:
    return _exists(
        conn,
        """
        SELECT 1
        FROM rc_fundamental_yahoo_quarterly
        WHERE market = ?
          AND symbol = ?
          AND period_end_date = ?
        LIMIT 1
        """,
        (market, ticker, period_end_date),
    )


def _has_enrichment_audit(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> bool:
    return _exists(
        conn,
        """
        SELECT 1
        FROM rc_fundamental_quarterly_enrichment_audit
        WHERE ticker = ?
          AND period_end_date = ?
        LIMIT 1
        """,
        (ticker, period_end_date),
    )


def _has_quarter_state(conn: sqlite3.Connection, market: str, ticker: str) -> bool:
    return _exists(
        conn,
        """
        SELECT 1
        FROM rc_fundamental_quarter_state
        WHERE market = ?
          AND ticker = ?
        LIMIT 1
        """,
        (market, ticker),
    )


def _exists(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> bool:
    try:
        return conn.execute(sql, params).fetchone() is not None
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            return False
        raise


def _classify_value(field_name: str, latest: Any, reconstructed: Any) -> tuple[str, float | None]:
    latest_null = _is_null(latest)
    reconstructed_null = _is_null(reconstructed)
    if latest_null and reconstructed_null:
        return BOTH_NULL, None
    if not latest_null and reconstructed_null:
        return LATEST_HAS_VALUE_RECON_NULL, None
    if latest_null and not reconstructed_null:
        return RECON_HAS_VALUE_LATEST_NULL, None
    if field_name == "currency":
        return (MATCH, None) if str(latest) == str(reconstructed) else (CURRENCY_DIFF, None)
    if isinstance(latest, (int, float)) or isinstance(reconstructed, (int, float)):
        latest_float = float(latest)
        reconstructed_float = float(reconstructed)
        if latest_float == reconstructed_float:
            return MATCH, 0.0
        return VALUE_DIFF, reconstructed_float - latest_float
    return (MATCH, None) if latest == reconstructed else (VALUE_DIFF, None)


def _is_null(value: Any) -> bool:
    return value is None or value == ""


def _infer_likely_cause(
    *,
    candidate_count: int,
    mismatched_rows: int,
    mismatched_field_counts: Mapping[str, int],
    yahoo_evidence_count: int,
    enrichment_audit_count: int,
) -> str:
    if candidate_count == 0:
        return "UNKNOWN"
    if mismatched_rows == 0:
        return "COMPARISON_POLICY_TOO_STRICT"
    if mismatched_rows == candidate_count:
        if yahoo_evidence_count or enrichment_audit_count:
            return "LATEST_CONTAINS_NON_SEC_VALUES"
        if set(mismatched_field_counts).issubset({"free_cashflow", "total_debt"}):
            return "DERIVED_FIELD_POLICY_DIFF"
        return "DRY_RUN_RECONSTRUCTION_PATH_DIFFERS_FROM_LATEST_WRITER"
    if yahoo_evidence_count or enrichment_audit_count:
        return "SOURCE_EVIDENCE_AMBIGUOUS"
    return "UNKNOWN"


def _recommend(likely_cause: str) -> str:
    if likely_cause == "COMPARISON_POLICY_TOO_STRICT":
        return "FIX_DRY_RUN_COMPARISON_POLICY"
    if likely_cause == "DRY_RUN_RECONSTRUCTION_PATH_DIFFERS_FROM_LATEST_WRITER":
        return "ALIGN_SEC_DRY_RUN_WITH_LATEST_WRITER"
    if likely_cause == "LATEST_CONTAINS_NON_SEC_VALUES":
        return "DO_NOT_BACKFILL_YET_INVESTIGATE_WRITER_PATH"
    if likely_cause == "DERIVED_FIELD_POLICY_DIFF":
        return "READY_AFTER_TARGETED_FIX"
    if likely_cause == "SOURCE_EVIDENCE_AMBIGUOUS":
        return "DO_NOT_BACKFILL_YET_INVESTIGATE_WRITER_PATH"
    return "DO_NOT_BACKFILL_YET_INVESTIGATE_WRITER_PATH"


def _row_diagnosis(mismatched_fields: Sequence[str], reconstruction_error: str | None) -> str:
    if reconstruction_error:
        return "DRY_RUN_RECONSTRUCTION_PATH_DIFFERS_FROM_LATEST_WRITER"
    if not mismatched_fields:
        return "MATCH"
    if set(mismatched_fields).issubset({"free_cashflow", "total_debt"}):
        return "DERIVED_FIELD_POLICY_DIFF"
    return "DRY_RUN_RECONSTRUCTION_PATH_DIFFERS_FROM_LATEST_WRITER"


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _format_text(result: Mapping[str, Any]) -> str:
    summary = result["summary"]
    lines = [
        "SEC vintage reconstruction mismatch diagnostics",
        f"fundamentals_db: {summary['fundamentals_db']}",
        f"market: {summary['market']}",
        f"source_run_id: {summary['source_run_id']}",
        f"candidate_count: {summary['candidate_count']}",
        f"matched_rows: {summary['matched_rows']}",
        f"mismatched_rows: {summary['mismatched_rows']}",
        f"likely_cause: {summary['likely_cause']}",
        f"recommendation: {summary['recommendation']}",
        f"mismatched_field_counts: {summary['mismatched_field_counts']}",
        "samples:",
    ]
    for sample in result["samples"]:
        lines.append(
            f"- {sample['ticker']} {sample['period_end_date']} "
            f"mismatches={','.join(sample['mismatched_fields']) or 'none'} "
            f"sec_raw_facts={sample['sec_raw_fact_count']}"
        )
    return "\n".join(lines)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--source-run-id")
    parser.add_argument("--ticker")
    parser.add_argument("--period-end-date")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--sample-limit", type=int, default=20)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
