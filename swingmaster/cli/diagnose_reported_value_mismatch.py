from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


DEBT_COMPONENT_FIELDS = (
    "LongTermDebtCurrent",
    "LongTermDebtNoncurrent",
    "ShortTermBorrowings",
)
DEBT_RELATED_TOKENS = (
    "Debt",
    "Borrow",
    "Liabil",
    "Cash",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose a reported latest/vintage value mismatch read-only")
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--period-end-date", required=True)
    parser.add_argument("--field", required=True)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--sample-limit", type=int, default=50)
    return parser.parse_args(argv)


def _connect_read_only(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.expanduser().resolve()
    conn = sqlite3.connect(f"file:{resolved.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _rows_to_dicts(rows: list[sqlite3.Row], limit: int) -> list[dict[str, Any]]:
    return [_row_to_dict(row) or {} for row in rows[:limit]]


def _value_equal(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def _non_null_reported_field_count(row: dict[str, Any] | None) -> int:
    if row is None:
        return 0
    fields = (
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
    )
    return sum(1 for field in fields if row.get(field) is not None)


def _latest_row(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> dict[str, Any] | None:
    return _row_to_dict(
        conn.execute(
            """
            SELECT *
            FROM rc_fundamental_quarterly
            WHERE ticker = ?
              AND period_end_date = ?
            """,
            (ticker, period_end_date),
        ).fetchone()
    )


def _vintage_rows(conn: sqlite3.Connection, market: str, ticker: str, period_end_date: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_quarterly_vintage
        WHERE market = ?
          AND ticker = ?
          AND period_end_date = ?
        ORDER BY available_at_utc DESC, statement_vintage_id ASC
        """,
        (market, ticker, period_end_date),
    ).fetchall()
    return _rows_to_dicts(rows, 10_000)


def _field_provenance_rows(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    statement_vintage_id: str,
    field_name: str,
    sample_limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_quarterly_field_provenance
        WHERE market = ?
          AND ticker = ?
          AND period_end_date = ?
          AND statement_vintage_id = ?
          AND field_name = ?
        ORDER BY source_provider ASC, source_row_ref ASC
        """,
        (market, ticker, period_end_date, statement_vintage_id, field_name),
    ).fetchall()
    return _rows_to_dicts(rows, sample_limit)


def _sec_raw_rows(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    period_end_date: str,
    sample_limit: int,
) -> list[dict[str, Any]]:
    conditions = " OR ".join("field_name LIKE ?" for _ in DEBT_RELATED_TOKENS)
    params = [ticker, period_end_date, *[f"%{token}%" for token in DEBT_RELATED_TOKENS]]
    rows = conn.execute(
        f"""
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
          AND period_end_date = ?
          AND ({conditions})
        ORDER BY statement_type ASC, field_name ASC, retrieved_at_utc ASC
        """,
        params,
    ).fetchall()
    return _rows_to_dicts(rows, sample_limit)


def _base_field_name(field_name: object) -> str:
    return str(field_name).split("|", 1)[0]


def _sec_component_summary(sec_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_component: dict[str, float] = {}
    component_rows: dict[str, dict[str, Any]] = {}
    for row in sec_rows:
        base = _base_field_name(row.get("field_name"))
        if base not in DEBT_COMPONENT_FIELDS:
            continue
        if base in by_component:
            continue
        value = row.get("field_value")
        if value is None:
            continue
        by_component[base] = float(value)
        component_rows[base] = row
    component_sum = sum(by_component.values()) if by_component else None
    return {
        "debt_component_values": by_component,
        "debt_component_rows": component_rows,
        "debt_component_sum": component_sum,
    }


def _yahoo_quarterly_rows(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    sample_limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_yahoo_quarterly
        WHERE market = ?
          AND symbol = ?
          AND period_end_date BETWEEN date(?, '-14 day') AND date(?, '+14 day')
        ORDER BY period_end_date ASC, created_at_utc ASC
        """,
        (market, ticker, period_end_date, period_end_date),
    ).fetchall()
    return _rows_to_dicts(rows, sample_limit)


def _yahoo_audit_rows(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    period_end_date: str,
    field_name: str,
    sample_limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_quarterly_enrichment_audit
        WHERE ticker = ?
          AND period_end_date BETWEEN date(?, '-14 day') AND date(?, '+14 day')
          AND (field_name = ? OR field_name IN ('cash', 'total_debt'))
        ORDER BY period_end_date ASC, field_name ASC, created_at_utc ASC
        """,
        (ticker, period_end_date, period_end_date, field_name),
    ).fetchall()
    return _rows_to_dicts(rows, sample_limit)


def _diagnosis(
    *,
    latest_row: dict[str, Any] | None,
    visible_vintage_row: dict[str, Any] | None,
    field_name: str,
    sec_summary: dict[str, Any],
    yahoo_rows: list[dict[str, Any]],
    yahoo_audit_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if latest_row is None or visible_vintage_row is None:
        return {
            "diagnosis_status": "UNKNOWN",
            "recommended_action": "manual_review_missing_latest_or_vintage",
            "confidence": "low",
            "reason": "latest_or_visible_vintage_row_missing",
        }
    latest_value = latest_row.get(field_name)
    vintage_value = visible_vintage_row.get(field_name)
    if _value_equal(latest_value, vintage_value):
        return {
            "diagnosis_status": "NO_MISMATCH",
            "recommended_action": "none",
            "confidence": "high",
            "reason": "latest_and_visible_vintage_values_match",
        }

    component_values = sec_summary.get("debt_component_values") or {}
    component_sum = sec_summary.get("debt_component_sum")
    yahoo_same_field_values = [
        row.get(field_name)
        for row in yahoo_rows
        if row.get(field_name) is not None
    ]
    yahoo_audit_same_field = [row for row in yahoo_audit_rows if row.get("field_name") == field_name]

    if field_name == "total_debt" and component_sum is not None and _value_equal(component_sum, latest_value):
        if any(_value_equal(value, vintage_value) for value in component_values.values()):
            return {
                "diagnosis_status": "DEBT_COMPONENT_POLICY_DIFF",
                "recommended_action": "review_or_create_provider_derived_vintage_for_exact_row",
                "confidence": "high",
                "reason": "latest_total_debt_equals_sec_component_sum_but_visible_vintage_equals_single_debt_component",
            }
        return {
            "diagnosis_status": "VINTAGE_LEGACY_BASELINE_STALE",
            "recommended_action": "create_provider_derived_vintage_for_exact_row_after_review",
            "confidence": "high",
            "reason": "latest_total_debt_equals_sec_component_sum_and_visible_vintage_is_not_current_policy_value",
        }

    if yahoo_same_field_values and not any(_value_equal(value, latest_value) for value in yahoo_same_field_values):
        return {
            "diagnosis_status": "YAHOO_VALUE_CONFLICT",
            "recommended_action": "manual_review_yahoo_conflict_not_proof_without_audit",
            "confidence": "medium",
            "reason": "yahoo_quarterly_has_conflicting_same_field_value",
        }

    if yahoo_audit_same_field:
        return {
            "diagnosis_status": "YAHOO_VALUE_CONFLICT",
            "recommended_action": "manual_review_yahoo_audit_for_field",
            "confidence": "medium",
            "reason": "yahoo_enrichment_audit_exists_for_mismatched_field",
        }

    if not component_values and not yahoo_same_field_values:
        return {
            "diagnosis_status": "LATEST_VALUE_UNSUPPORTED",
            "recommended_action": "investigate_latest_value_source_before_any_vintage_write",
            "confidence": "medium",
            "reason": "latest_value_not_supported_by_sec_debt_components_or_yahoo_quarterly",
        }

    return {
        "diagnosis_status": "UNKNOWN",
        "recommended_action": "manual_review",
        "confidence": "low",
        "reason": "available_evidence_does_not_match_a_specific_policy_case",
    }


def run_diagnostic(
    *,
    fundamentals_db: Path,
    market: str,
    ticker: str,
    period_end_date: str,
    field_name: str,
    sample_limit: int,
) -> dict[str, Any]:
    normalized_market = market.strip().lower()
    normalized_ticker = ticker.strip().upper()
    normalized_field = field_name.strip()
    with _connect_read_only(fundamentals_db) as conn:
        latest = _latest_row(conn, normalized_ticker, period_end_date)
        vintages = _vintage_rows(conn, normalized_market, normalized_ticker, period_end_date)
        visible_vintage = vintages[0] if vintages else None
        provenance = []
        if visible_vintage is not None:
            provenance = _field_provenance_rows(
                conn,
                market=normalized_market,
                ticker=normalized_ticker,
                period_end_date=period_end_date,
                statement_vintage_id=str(visible_vintage["statement_vintage_id"]),
                field_name=normalized_field,
                sample_limit=sample_limit,
            )
        sec_rows = _sec_raw_rows(
            conn,
            ticker=normalized_ticker,
            period_end_date=period_end_date,
            sample_limit=sample_limit,
        )
        yahoo_rows = _yahoo_quarterly_rows(
            conn,
            market=normalized_market,
            ticker=normalized_ticker,
            period_end_date=period_end_date,
            sample_limit=sample_limit,
        )
        audit_rows = _yahoo_audit_rows(
            conn,
            ticker=normalized_ticker,
            period_end_date=period_end_date,
            field_name=normalized_field,
            sample_limit=sample_limit,
        )

    sec_summary = _sec_component_summary(sec_rows)
    diagnosis = _diagnosis(
        latest_row=latest,
        visible_vintage_row=visible_vintage,
        field_name=normalized_field,
        sec_summary=sec_summary,
        yahoo_rows=yahoo_rows,
        yahoo_audit_rows=audit_rows,
    )
    return {
        "market": normalized_market,
        "ticker": normalized_ticker,
        "period_end_date": period_end_date,
        "field_name": normalized_field,
        "latest_row": latest,
        "latest_value": latest.get(normalized_field) if latest else None,
        "latest_run_id": latest.get("run_id") if latest else None,
        "latest_currency": latest.get("currency") if latest else None,
        "latest_non_null_reported_field_count": _non_null_reported_field_count(latest),
        "latest_related_fields": {
            "cash": latest.get("cash") if latest else None,
            "total_debt": latest.get("total_debt") if latest else None,
        },
        "visible_vintage_row": visible_vintage,
        "visible_vintage_value": visible_vintage.get(normalized_field) if visible_vintage else None,
        "visible_vintage_statement_vintage_id": visible_vintage.get("statement_vintage_id") if visible_vintage else None,
        "visible_vintage_source_provider": visible_vintage.get("source_provider") if visible_vintage else None,
        "visible_vintage_run_id": visible_vintage.get("run_id") if visible_vintage else None,
        "visible_vintage_available_at_utc": visible_vintage.get("available_at_utc") if visible_vintage else None,
        "visible_vintage_ingested_at_utc": visible_vintage.get("ingested_at_utc") if visible_vintage else None,
        "visible_vintage_revision_number": visible_vintage.get("revision_number") if visible_vintage else None,
        "visible_vintage_field_provenance_rows": provenance,
        "all_vintage_rows": vintages[:sample_limit],
        "sec_raw_debt_related_rows": sec_rows,
        "sec_debt_component_values": sec_summary["debt_component_values"],
        "sec_debt_component_sum": sec_summary["debt_component_sum"],
        "yahoo_quarterly_rows": yahoo_rows,
        "yahoo_audit_rows": audit_rows,
        **diagnosis,
    }


def _print_text(summary: dict[str, Any]) -> None:
    for key, value in summary.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value, sort_keys=True)
        print(f"SUMMARY {key}={value}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = run_diagnostic(
            fundamentals_db=Path(args.fundamentals_db),
            market=args.market,
            ticker=args.ticker,
            period_end_date=args.period_end_date,
            field_name=args.field,
            sample_limit=args.sample_limit,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(json.dumps({"summary": summary}, indent=2, sort_keys=True))
    else:
        _print_text(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
