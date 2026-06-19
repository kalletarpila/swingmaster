from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote

from swingmaster.cli.dry_run_reported_vintage_backfill import FINANCIAL_FIELDS
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)


CURRENCY_FIELD = "currency"
VALUE_FIELDS = (*FINANCIAL_FIELDS, CURRENCY_FIELD)
SOURCE_PROVIDER = "UNKNOWN_LEGACY"
AVAILABILITY_QUALITY = "LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL"
PROVENANCE_ROLE = "LEGACY_BASELINE"
MERGE_ACTION = "LEGACY_BACKFILL_BASELINE"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify reported vintage legacy backfill read-only")
    parser.add_argument("--fundamentals-db", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--available-at-utc", required=True)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--fail-if-not-ok", action="store_true")
    return parser.parse_args()


def open_readonly_db(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"SQLite database not found: {resolved}")
    conn = sqlite3.connect(f"file:{quote(str(resolved))}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    return conn


def verify_backfill(
    fundamentals_db_path: Path,
    market: str,
    available_at_utc: str,
    sample_size: int = 5,
) -> dict[str, Any]:
    if sample_size < 0:
        raise ValueError("--sample-size must be >= 0")
    market = market.strip().lower()
    with open_readonly_db(fundamentals_db_path) as conn:
        summary = {
            "fundamentals_db": str(fundamentals_db_path.expanduser().resolve()),
            "market": market,
            "available_at_utc": available_at_utc,
            "sample_size": sample_size,
        }
        checks = {
            "coverage": coverage_check(conn, market, available_at_utc),
            "duplicates": duplicate_check(conn, market, available_at_utc),
            "value_parity": value_parity_check(conn, market, available_at_utc),
            "metadata_policy": metadata_policy_check(conn, market, available_at_utc),
            "provenance": provenance_check(conn, market, available_at_utc),
        }
        samples = pit_sample_check(conn, market, available_at_utc, sample_size)
    ok = all(check["ok"] for check in checks.values()) and all(sample["ok"] for sample in samples)
    summary["overall_status"] = "OK" if ok else "NOT_OK"
    return {"summary": summary, "checks": checks, "samples": samples}


def coverage_check(conn: sqlite3.Connection, market: str, available_at_utc: str) -> dict[str, Any]:
    latest_count = _scalar(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly")
    baseline_count = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_vintage
        WHERE market = ?
          AND source_provider = ?
          AND availability_quality = ?
          AND available_at_utc = ?
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc),
    )
    missing_count = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly q
        LEFT JOIN rc_fundamental_quarterly_vintage v
          ON v.market = ?
         AND v.ticker = q.ticker
         AND v.period_end_date = q.period_end_date
         AND v.source_provider = ?
         AND v.availability_quality = ?
         AND v.available_at_utc = ?
        WHERE v.statement_vintage_id IS NULL
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc),
    )
    extra_count = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_vintage v
        LEFT JOIN rc_fundamental_quarterly q
          ON q.ticker = v.ticker
         AND q.period_end_date = v.period_end_date
        WHERE v.market = ?
          AND v.source_provider = ?
          AND v.availability_quality = ?
          AND v.available_at_utc = ?
          AND q.ticker IS NULL
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc),
    )
    ok = latest_count == baseline_count and missing_count == 0 and extra_count == 0
    return {
        "ok": ok,
        "latest_count": latest_count,
        "baseline_vintage_count": baseline_count,
        "missing_latest_rows": missing_count,
        "extra_vintage_rows": extra_count,
    }


def duplicate_check(conn: sqlite3.Connection, market: str, available_at_utc: str) -> dict[str, Any]:
    duplicate_statement_ids = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM (
            SELECT statement_vintage_id
            FROM rc_fundamental_quarterly_vintage
            GROUP BY statement_vintage_id
            HAVING COUNT(*) > 1
        )
        """,
    )
    duplicate_baseline_periods = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM (
            SELECT market, ticker, period_end_date, availability_quality
            FROM rc_fundamental_quarterly_vintage
            WHERE market = ?
              AND source_provider = ?
              AND availability_quality = ?
              AND available_at_utc = ?
            GROUP BY market, ticker, period_end_date, availability_quality
            HAVING COUNT(*) > 1
        )
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc),
    )
    return {
        "ok": duplicate_statement_ids == 0 and duplicate_baseline_periods == 0,
        "duplicate_statement_vintage_ids": duplicate_statement_ids,
        "duplicate_baseline_ticker_periods": duplicate_baseline_periods,
    }


def value_parity_check(conn: sqlite3.Connection, market: str, available_at_utc: str) -> dict[str, Any]:
    mismatches_by_field = {
        field_name: _value_mismatch_count(conn, field_name, market, available_at_utc) for field_name in VALUE_FIELDS
    }
    total_mismatches = sum(mismatches_by_field.values())
    return {
        "ok": total_mismatches == 0,
        "total_mismatches": total_mismatches,
        "mismatches_by_field": mismatches_by_field,
        "comparison": "exact SQLite null-safe comparison using IS NOT",
    }


def metadata_policy_check(conn: sqlite3.Connection, market: str, available_at_utc: str) -> dict[str, Any]:
    total_vintage_rows = _scalar(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage WHERE market = ?", (market,))
    mismatch_count = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_vintage
        WHERE market = ?
          AND (
              source_provider IS NOT ?
           OR availability_quality IS NOT ?
           OR available_at_utc IS NOT ?
           OR revision_number IS NOT 1
           OR is_restated IS NOT 0
           OR supersedes_vintage_id IS NOT NULL
          )
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc),
    )
    return {"ok": mismatch_count == 0, "total_vintage_rows": total_vintage_rows, "metadata_mismatch_rows": mismatch_count}


def provenance_check(conn: sqlite3.Connection, market: str, available_at_utc: str) -> dict[str, Any]:
    total_provenance_rows = _scalar(conn, "SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance WHERE market = ?", (market,))
    expected_provenance_rows = _scalar(
        conn,
        _expected_provenance_sql(market),
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc),
    )
    missing_or_extra_vintage_count = _scalar(
        conn,
        """
        WITH expected AS (
            SELECT
                statement_vintage_id,
                (CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN gross_profit IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN operating_income IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN ebit IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN ebitda IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN net_income IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN operating_cashflow IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN capex IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN free_cashflow IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN cash IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN total_debt IS NOT NULL THEN 1 ELSE 0 END
                 + CASE WHEN shares_outstanding IS NOT NULL THEN 1 ELSE 0 END) AS expected_count
            FROM rc_fundamental_quarterly_vintage
            WHERE market = ?
              AND source_provider = ?
              AND availability_quality = ?
              AND available_at_utc = ?
        ),
        actual AS (
            SELECT statement_vintage_id, COUNT(*) AS actual_count
            FROM rc_fundamental_quarterly_field_provenance
            WHERE market = ?
            GROUP BY statement_vintage_id
        )
        SELECT COUNT(*)
        FROM expected e
        LEFT JOIN actual a
          ON a.statement_vintage_id = e.statement_vintage_id
        WHERE e.expected_count != COALESCE(a.actual_count, 0)
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc, market),
    )
    metadata_mismatch_rows = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_field_provenance
        WHERE market = ?
          AND (
              source_provider IS NOT ?
           OR provenance_role IS NOT ?
           OR merge_action IS NOT ?
          )
        """,
        (market, SOURCE_PROVIDER, PROVENANCE_ROLE, MERGE_ACTION),
    )
    return {
        "ok": (
            total_provenance_rows == expected_provenance_rows
            and missing_or_extra_vintage_count == 0
            and metadata_mismatch_rows == 0
        ),
        "total_provenance_rows": total_provenance_rows,
        "expected_provenance_rows": expected_provenance_rows,
        "vintages_with_provenance_count_mismatch": missing_or_extra_vintage_count,
        "metadata_mismatch_rows": metadata_mismatch_rows,
    }


def pit_sample_check(conn: sqlite3.Connection, market: str, available_at_utc: str, sample_size: int) -> list[dict[str, Any]]:
    if sample_size == 0:
        return []
    before_cutoff = _previous_second(available_at_utc)
    rows = conn.execute(
        """
        SELECT ticker, period_end_date, statement_vintage_id
        FROM rc_fundamental_quarterly_vintage
        WHERE market = ?
          AND source_provider = ?
          AND availability_quality = ?
          AND available_at_utc = ?
        ORDER BY ticker ASC, period_end_date ASC
        LIMIT ?
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc, sample_size),
    ).fetchall()
    latest_rows = conn.execute(
        """
        SELECT ticker, period_end_date, statement_vintage_id
        FROM rc_fundamental_quarterly_vintage
        WHERE market = ?
          AND source_provider = ?
          AND availability_quality = ?
          AND available_at_utc = ?
        ORDER BY ticker DESC, period_end_date DESC
        LIMIT ?
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc, sample_size),
    ).fetchall()
    samples = [*rows, *latest_rows]
    seen: set[tuple[str, str]] = set()
    results: list[dict[str, Any]] = []
    for ticker, period_end_date, statement_vintage_id in samples:
        key = (str(ticker), str(period_end_date))
        if key in seen:
            continue
        seen.add(key)
        at_row = get_pit_quarterly_vintage(conn, str(ticker), str(period_end_date), available_at_utc, market=market)
        before_row = get_pit_quarterly_vintage(conn, str(ticker), str(period_end_date), before_cutoff, market=market)
        provenance_rows = get_quarterly_field_provenance(conn, str(statement_vintage_id))
        results.append(
            {
                "ok": at_row is not None and before_row is None and len(provenance_rows) > 0,
                "ticker": ticker,
                "period_end_date": period_end_date,
                "statement_vintage_id": statement_vintage_id,
                "row_at_cutoff": at_row is not None,
                "row_before_cutoff": before_row is not None,
                "provenance_rows": len(provenance_rows),
            }
        )
    return results


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def render_text(report: dict[str, Any]) -> str:
    lines = [
        f"overall_status: {report['summary']['overall_status']}",
        f"coverage_ok: {report['checks']['coverage']['ok']}",
        f"value_parity_ok: {report['checks']['value_parity']['ok']}",
        f"provenance_ok: {report['checks']['provenance']['ok']}",
        f"metadata_policy_ok: {report['checks']['metadata_policy']['ok']}",
        f"duplicate_check_ok: {report['checks']['duplicates']['ok']}",
        f"sample_count: {len(report['samples'])}",
    ]
    return "\n".join(lines)


def _value_mismatch_count(conn: sqlite3.Connection, field_name: str, market: str, available_at_utc: str) -> int:
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly q
        JOIN rc_fundamental_quarterly_vintage v
          ON v.market = ?
         AND v.ticker = q.ticker
         AND v.period_end_date = q.period_end_date
         AND v.source_provider = ?
         AND v.availability_quality = ?
         AND v.available_at_utc = ?
        WHERE q.{field_name} IS NOT v.{field_name}
        """,
        (market, SOURCE_PROVIDER, AVAILABILITY_QUALITY, available_at_utc),
    )


def _expected_provenance_sql(market: str) -> str:
    del market
    return """
        SELECT
            SUM(
                (CASE WHEN revenue IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN gross_profit IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN operating_income IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN ebit IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN ebitda IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN net_income IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN operating_cashflow IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN capex IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN free_cashflow IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN cash IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN total_debt IS NOT NULL THEN 1 ELSE 0 END)
              + (CASE WHEN shares_outstanding IS NOT NULL THEN 1 ELSE 0 END)
            )
        FROM rc_fundamental_quarterly_vintage
        WHERE market = ?
          AND source_provider = ?
          AND availability_quality = ?
          AND available_at_utc = ?
    """


def _previous_second(timestamp_utc: str) -> str:
    if timestamp_utc == "2026-06-19T00:00:00Z":
        return "2026-06-18T23:59:59Z"
    raise ValueError("Only explicit YYYY-MM-DDT00:00:00Z cutoff sample currently supported")


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    value = conn.execute(sql, params).fetchone()[0]
    return int(value or 0)


def main() -> None:
    args = parse_args()
    try:
        report = verify_backfill(
            fundamentals_db_path=Path(args.fundamentals_db),
            market=args.market,
            available_at_utc=args.available_at_utc,
            sample_size=args.sample_size,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    print(render_json(report) if args.format == "json" else render_text(report))
    if args.fail_if_not_ok and report["summary"]["overall_status"] != "OK":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
