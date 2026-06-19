from __future__ import annotations

import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import dry_run_reported_vintage_backfill as dry_run
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_writer import insert_quarterly_vintage_row


def test_missing_vintage_tables_returns_blocked_missing_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_schema.db"
    _create_latest_only_db(db_path)

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19")

    assert report["summary"]["overall_status"] == "BLOCKED_MISSING_SCHEMA"
    assert "MISSING_TABLE:rc_fundamental_quarterly_vintage" in report["blocked_reasons"]


def test_no_source_rows_returns_no_source_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "no_rows.db"
    run_migration(db_path)

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19")

    assert report["summary"]["overall_status"] == "NO_SOURCE_ROWS"
    assert report["summary"]["planned_vintage_rows"] == 0


def test_candidate_rows_are_generated_from_latest_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "candidates.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19", include_sample_rows=1)

    assert report["summary"]["candidate_rows"] == 1
    assert report["summary"]["planned_vintage_rows"] == 1
    assert report["candidate_samples"][0]["ticker"] == "AAPL"


def test_source_hash_is_deterministic_and_changes_when_value_changes() -> None:
    row = _latest_row("AAPL", "2026-03-31", revenue=100.0)

    first_hash = dry_run.build_legacy_source_hash(row, "usa")
    second_hash = dry_run.build_legacy_source_hash(dict(row), "usa")
    changed_hash = dry_run.build_legacy_source_hash(_latest_row("AAPL", "2026-03-31", revenue=101.0), "usa")

    assert first_hash == second_hash
    assert first_hash != changed_hash


def test_statement_vintage_id_is_deterministic() -> None:
    row = _latest_row("AAPL", "2026-03-31")
    source_hash = dry_run.build_legacy_source_hash(row, "usa")

    first_id = dry_run.build_legacy_statement_vintage_id(row, "usa", source_hash)
    second_id = dry_run.build_legacy_statement_vintage_id(dict(row), "usa", source_hash)

    assert first_id == second_id
    assert first_id.startswith("legacy:usa:AAPL:2026-03-31:")


def test_dry_run_does_not_write_to_vintage_or_provenance_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "no_writes.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19")

    with sqlite3.connect(str(db_path)) as conn:
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
    assert vintage_count == 0
    assert provenance_count == 0


def test_existing_vintage_row_is_detected_and_not_planned(tmp_path: Path) -> None:
    db_path = tmp_path / "existing_vintage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        insert_quarterly_vintage_row(conn, _vintage_row("AAPL", "2026-03-31"))
        conn.commit()

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19")

    assert report["summary"]["planned_vintage_rows"] == 0
    assert report["summary"]["already_has_vintage_rows"] == 1
    assert report["skipped_reasons"] == {"ALREADY_HAS_VINTAGE": 1}


def test_provenance_preview_count_equals_non_null_financial_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "provenance_count.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31", revenue=100.0, gross_profit=None, total_debt=None)
        conn.commit()

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19", include_sample_rows=1)

    assert report["candidate_samples"][0]["planned_field_provenance_count"] == len(dry_run.FINANCIAL_FIELDS) - 2


def test_null_financial_values_are_not_converted_to_zero(tmp_path: Path) -> None:
    db_path = tmp_path / "null_values.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31", revenue=None)
        conn.commit()

    with sqlite3.connect(str(db_path)) as conn:
        row = dry_run.load_latest_quarterly_rows(conn, ["AAPL"], None, "2026-06-19")[0]

    assert row["revenue"] is None
    assert dry_run.count_non_null_financial_fields(row) == len(dry_run.FINANCIAL_FIELDS) - 1


def test_json_output_contains_summary_policy_and_sample_candidates(tmp_path: Path) -> None:
    db_path = tmp_path / "json_output.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19", include_sample_rows=1)
    parsed = json.loads(dry_run.render_json(report))

    assert parsed["summary"]["planned_vintage_rows"] == 1
    assert parsed["policy"]["source_hash_algorithm"] == "sha256"
    assert parsed["candidate_samples"][0]["ticker"] == "AAPL"


def test_tickers_filter_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "tickers.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        _insert_latest_row(conn, "MSFT", "2026-03-31")
        conn.commit()

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19", tickers=["MSFT"], include_sample_rows=1)

    assert report["summary"]["total_latest_rows"] == 1
    assert report["candidate_samples"][0]["ticker"] == "MSFT"


def test_max_rows_limits_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "max_rows.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        _insert_latest_row(conn, "MSFT", "2026-03-31")
        conn.commit()

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19", max_rows=1)

    assert report["summary"]["total_latest_rows"] == 1


def test_fail_if_blocked_returns_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "blocked.db"
    _create_latest_only_db(db_path)
    monkeypatch.setattr(
        dry_run,
        "parse_args",
        lambda: Namespace(
            fundamentals_db=str(db_path),
            market="usa",
            as_of_date="2026-06-19",
            tickers=None,
            max_rows=None,
            format="json",
            include_sample_rows=0,
            fail_if_blocked=True,
        ),
    )

    with pytest.raises(SystemExit) as excinfo:
        dry_run.main()
    assert excinfo.value.code == 1


def test_missing_or_empty_ticker_or_period_is_skipped_with_reason(tmp_path: Path) -> None:
    db_path = tmp_path / "bad_rows.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "", "2026-03-31")
        _insert_latest_row(conn, "MSFT", "")
        conn.commit()

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19")

    assert report["summary"]["planned_vintage_rows"] == 0
    assert report["skipped_reasons"] == {"MISSING_TICKER": 1, "MISSING_PERIOD_END_DATE": 1}


def test_availability_policy_is_explicitly_policy_required(tmp_path: Path) -> None:
    db_path = tmp_path / "availability_policy.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19", include_sample_rows=1)

    assert report["policy"]["availability_policy"]["status"] == "REQUIRES_POLICY_DECISION"
    assert report["candidate_samples"][0]["available_at_utc"] is None
    assert report["candidate_samples"][0]["availability_quality"] == "LEGACY_ESTIMATED"


def _create_latest_only_db(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT,
                period_end_date TEXT,
                revenue REAL,
                gross_profit REAL,
                operating_income REAL,
                ebit REAL,
                ebitda REAL,
                net_income REAL,
                operating_cashflow REAL,
                capex REAL,
                free_cashflow REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL,
                currency TEXT,
                run_id TEXT
            )
            """
        )
        conn.commit()


def _insert_latest_row(conn: sqlite3.Connection, ticker: str, period_end_date: str, **overrides: object) -> None:
    row = _latest_row(ticker, period_end_date, **overrides)
    conn.execute(
        f"""
        INSERT INTO rc_fundamental_quarterly ({", ".join(dry_run.LATEST_COLUMNS)})
        VALUES ({", ".join("?" for _ in dry_run.LATEST_COLUMNS)})
        """,
        tuple(row[column_name] for column_name in dry_run.LATEST_COLUMNS),
    )


def _latest_row(ticker: str, period_end_date: str, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
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
    row.update(overrides)
    return row


def _vintage_row(ticker: str, period_end_date: str) -> dict[str, object]:
    row = _latest_row(ticker, period_end_date)
    source_hash = dry_run.build_legacy_source_hash(row, "usa")
    return {
        "ticker": ticker,
        "market": "usa",
        "period_end_date": period_end_date,
        "statement_vintage_id": dry_run.build_legacy_statement_vintage_id(row, "usa", source_hash),
        "source_provider": "UNKNOWN_LEGACY",
        "source_document_id": None,
        "source_hash": source_hash,
        "revision_number": 1,
        "is_restated": 0,
        "supersedes_vintage_id": None,
        "availability_quality": "LEGACY_ESTIMATED",
        "filed_at_utc": None,
        "available_at_utc": "2026-04-30T00:00:00Z",
        "ingested_at_utc": "2026-06-19T00:00:00Z",
        "provider_observed_at_utc": None,
        "run_id": "RUN1",
        "provider_run_id": None,
        "normalization_run_id": None,
        "enrichment_run_id": None,
        "revenue": row["revenue"],
        "gross_profit": row["gross_profit"],
        "operating_income": row["operating_income"],
        "ebit": row["ebit"],
        "ebitda": row["ebitda"],
        "net_income": row["net_income"],
        "operating_cashflow": row["operating_cashflow"],
        "capex": row["capex"],
        "free_cashflow": row["free_cashflow"],
        "cash": row["cash"],
        "total_debt": row["total_debt"],
        "shares_outstanding": row["shares_outstanding"],
        "currency": row["currency"],
        "created_at_utc": "2026-06-19T00:00:00Z",
        "updated_at_utc": None,
    }
