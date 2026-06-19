from __future__ import annotations

import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import apply_reported_vintage_backfill as apply_cli
from swingmaster.cli import dry_run_reported_vintage_backfill as dry_run
from swingmaster.cli import verify_reported_vintage_backfill as verify_cli
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)


AVAILABLE_AT = "2026-06-19T00:00:00Z"
APPLY_TS = "2026-06-19T12:00:00Z"


def test_verify_ok_case_where_latest_vintage_and_provenance_match(tmp_path: Path) -> None:
    db_path = _db_with_applied_backfill(tmp_path, [("AAPL", "2026-03-31")])

    report = verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT, sample_size=1)

    assert report["summary"]["overall_status"] == "OK"
    assert report["checks"]["coverage"]["latest_count"] == 1
    assert report["checks"]["coverage"]["baseline_vintage_count"] == 1
    assert report["checks"]["value_parity"]["total_mismatches"] == 0
    assert report["checks"]["provenance"]["total_provenance_rows"] == len(dry_run.FINANCIAL_FIELDS)


def test_missing_vintage_row_produces_not_ok(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])

    report = verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT)

    assert report["summary"]["overall_status"] == "NOT_OK"
    assert report["checks"]["coverage"]["missing_latest_rows"] == 1


def test_duplicate_vintage_id_produces_not_ok(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31"), ("MSFT", "2026-03-31")])
    with sqlite3.connect(str(db_path)) as conn:
        row_a = _latest_row("AAPL", "2026-03-31")
        row_m = _latest_row("MSFT", "2026-03-31")
        source_hash = "same-source-hash"
        insert_quarterly_vintage_row(conn, _vintage_row(row_a, "same-vintage-id", source_hash))
        insert_quarterly_vintage_row(conn, _vintage_row(row_m, "same-vintage-id", source_hash))
        conn.commit()

    report = verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT)

    assert report["summary"]["overall_status"] == "NOT_OK"
    assert report["checks"]["duplicates"]["duplicate_statement_vintage_ids"] == 1


def test_financial_value_mismatch_produces_not_ok(tmp_path: Path) -> None:
    db_path = _db_with_applied_backfill(tmp_path, [("AAPL", "2026-03-31")])
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("UPDATE rc_fundamental_quarterly_vintage SET revenue = revenue + 1")
        conn.commit()

    report = verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT)

    assert report["summary"]["overall_status"] == "NOT_OK"
    assert report["checks"]["value_parity"]["mismatches_by_field"]["revenue"] == 1


def test_metadata_policy_mismatch_produces_not_ok(tmp_path: Path) -> None:
    db_path = _db_with_applied_backfill(tmp_path, [("AAPL", "2026-03-31")])
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("UPDATE rc_fundamental_quarterly_vintage SET availability_quality = 'BAD'")
        conn.commit()

    report = verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT)

    assert report["summary"]["overall_status"] == "NOT_OK"
    assert report["checks"]["metadata_policy"]["metadata_mismatch_rows"] == 1


def test_provenance_count_mismatch_produces_not_ok(tmp_path: Path) -> None:
    db_path = _db_with_applied_backfill(tmp_path, [("AAPL", "2026-03-31")])
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM rc_fundamental_quarterly_field_provenance WHERE field_name = 'revenue'")
        conn.commit()

    report = verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT)

    assert report["summary"]["overall_status"] == "NOT_OK"
    assert report["checks"]["provenance"]["vintages_with_provenance_count_mismatch"] == 1


def test_pit_sample_behavior_passes_for_live_safe_cutoff_and_before_cutoff(tmp_path: Path) -> None:
    db_path = _db_with_applied_backfill(tmp_path, [("AAPL", "2026-03-31")])

    report = verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT, sample_size=1)

    assert report["samples"][0]["ok"] is True
    assert report["samples"][0]["row_at_cutoff"] is True
    assert report["samples"][0]["row_before_cutoff"] is False
    assert report["samples"][0]["provenance_rows"] == len(dry_run.FINANCIAL_FIELDS)


def test_json_output_contains_summary_checks_and_samples(tmp_path: Path) -> None:
    db_path = _db_with_applied_backfill(tmp_path, [("AAPL", "2026-03-31")])

    report = verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT, sample_size=1)
    parsed = json.loads(verify_cli.render_json(report))

    assert parsed["summary"]["overall_status"] == "OK"
    assert "coverage" in parsed["checks"]
    assert parsed["samples"][0]["ticker"] == "AAPL"


def test_fail_if_not_ok_exits_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])
    monkeypatch.setattr(
        verify_cli,
        "parse_args",
        lambda: Namespace(
            fundamentals_db=str(db_path),
            market="usa",
            available_at_utc=AVAILABLE_AT,
            sample_size=1,
            format="json",
            fail_if_not_ok=True,
        ),
    )

    with pytest.raises(SystemExit) as excinfo:
        verify_cli.main()

    assert excinfo.value.code == 1


def test_verifier_does_not_write_to_db(tmp_path: Path) -> None:
    db_path = _db_with_applied_backfill(tmp_path, [("AAPL", "2026-03-31")])
    before = _counts(db_path)

    verify_cli.verify_backfill(db_path, market="usa", available_at_utc=AVAILABLE_AT, sample_size=1)

    assert _counts(db_path) == before


def _db_with_applied_backfill(tmp_path: Path, rows: list[tuple[str, str]]) -> Path:
    db_path = _db_with_latest_rows(tmp_path, rows)
    expected_vintage_rows = len(rows)
    expected_provenance_rows = len(rows) * len(dry_run.FINANCIAL_FIELDS)
    apply_cli.apply_backfill(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        legacy_availability_policy="live_safe_legacy_baseline",
        legacy_available_at_utc=AVAILABLE_AT,
        legacy_availability_lag_days=None,
        verified_availability_file=None,
        expected_vintage_rows=expected_vintage_rows,
        expected_provenance_rows=expected_provenance_rows,
        confirm_write=True,
        apply_timestamp_utc=APPLY_TS,
    )
    return db_path


def _db_with_latest_rows(tmp_path: Path, rows: list[tuple[str, str]]) -> Path:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        for ticker, period_end_date in rows:
            _insert_latest_row(conn, ticker, period_end_date)
        conn.commit()
    return db_path


def _insert_latest_row(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> None:
    row = _latest_row(ticker, period_end_date)
    conn.execute(
        f"""
        INSERT INTO rc_fundamental_quarterly ({", ".join(dry_run.LATEST_COLUMNS)})
        VALUES ({", ".join("?" for _ in dry_run.LATEST_COLUMNS)})
        """,
        tuple(row[column_name] for column_name in dry_run.LATEST_COLUMNS),
    )


def _latest_row(ticker: str, period_end_date: str) -> dict[str, object]:
    return {
        "ticker": ticker,
        "period_end_date": period_end_date,
        "revenue": 100.0,
        "gross_profit": 45.0,
        "operating_income": 30.0,
        "ebit": 29.0,
        "ebitda": 34.0,
        "net_income": 25.0,
        "operating_cashflow": 35.0,
        "capex": -5.0,
        "free_cashflow": 30.0,
        "cash": 80.0,
        "total_debt": 20.0,
        "shares_outstanding": 1000.0,
        "currency": "USD",
        "run_id": "RUN1",
    }


def _vintage_row(row: dict[str, object], statement_vintage_id: str, source_hash: str) -> dict[str, object]:
    vintage = apply_cli.build_vintage_row(
        row,
        {
            "ticker": row["ticker"],
            "market": "usa",
            "period_end_date": row["period_end_date"],
            "statement_vintage_id": statement_vintage_id,
            "source_hash": source_hash,
            "availability_quality": verify_cli.AVAILABILITY_QUALITY,
            "available_at_utc": AVAILABLE_AT,
        },
        run_id="RUN_DUP",
        apply_timestamp_utc=APPLY_TS,
    )
    return vintage


def _counts(db_path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        return (
            conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0],
        )
