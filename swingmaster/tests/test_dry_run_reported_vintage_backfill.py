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
            legacy_availability_policy="policy_required",
            legacy_available_at_utc=None,
            legacy_availability_lag_days=None,
            verified_availability_file=None,
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
    assert report["policy"]["selected_policy"] == "policy_required"
    assert report["summary"]["overall_status"] == "DRY_RUN_PARTIAL_POLICY_REQUIRED"
    assert report["candidate_samples"][0]["available_at_utc"] is None
    assert report["candidate_samples"][0]["availability_quality"] == "LEGACY_ESTIMATED"


def test_live_safe_legacy_baseline_requires_available_at_utc() -> None:
    with pytest.raises(ValueError, match="--legacy-available-at-utc is required"):
        dry_run.build_availability_policy_context(policy="live_safe_legacy_baseline")


def test_live_safe_legacy_baseline_sets_available_at_and_quality(tmp_path: Path) -> None:
    db_path = tmp_path / "live_safe.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = dry_run.run_dry_run(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        include_sample_rows=1,
        legacy_availability_policy="live_safe_legacy_baseline",
        legacy_available_at_utc="2026-06-19T00:00:00Z",
    )

    sample = report["candidate_samples"][0]
    assert report["summary"]["overall_status"] == "DRY_RUN_READY"
    assert report["policy"]["selected_policy"] == "live_safe_legacy_baseline"
    assert report["policy"]["legacy_available_at_utc"] == "2026-06-19T00:00:00Z"
    assert sample["available_at_utc"] == "2026-06-19T00:00:00Z"
    assert sample["availability_quality"] == "LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL"
    assert sample["requires_policy_decision"] is False
    assert "HISTORICAL_BACKTESTS_BEFORE_BACKFILL_TIMESTAMP_WILL_NOT_SEE_THESE_VINTAGES" in sample["warnings"]


def test_research_estimated_legacy_requires_lag_days() -> None:
    with pytest.raises(ValueError, match="--legacy-availability-lag-days is required"):
        dry_run.build_availability_policy_context(policy="research_estimated_legacy")


@pytest.mark.parametrize("lag_days", [0, -1])
def test_research_estimated_legacy_rejects_zero_or_negative_lag(lag_days: int) -> None:
    with pytest.raises(ValueError, match="--legacy-availability-lag-days must be >= 1"):
        dry_run.build_availability_policy_context(
            policy="research_estimated_legacy",
            legacy_availability_lag_days=lag_days,
        )


def test_research_estimated_legacy_calculates_available_at_from_period_plus_lag(tmp_path: Path) -> None:
    db_path = tmp_path / "research.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = dry_run.run_dry_run(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        include_sample_rows=1,
        legacy_availability_policy="research_estimated_legacy",
        legacy_availability_lag_days=45,
    )

    sample = report["candidate_samples"][0]
    assert report["summary"]["overall_status"] == "DRY_RUN_READY"
    assert report["policy"]["legacy_availability_lag_days"] == 45
    assert sample["available_at_utc"] == "2026-05-15T00:00:00Z"
    assert sample["availability_quality"] == "LEGACY_ESTIMATED"
    assert "RESEARCH_ESTIMATED_AVAILABILITY_NOT_AUDIT_GRADE" in sample["warnings"]


def test_externally_verified_release_date_requires_local_file() -> None:
    with pytest.raises(ValueError, match="--verified-availability-file is required"):
        dry_run.build_availability_policy_context(policy="externally_verified_release_date")


def test_externally_verified_release_date_uses_local_csv_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "verified.db"
    verified_file = tmp_path / "verified.csv"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "aapl", "2026-03-31")
        conn.commit()
    _write_verified_csv(verified_file, ticker="AAPL", period_end_date="2026-03-31")

    report = dry_run.run_dry_run(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        include_sample_rows=1,
        legacy_availability_policy="externally_verified_release_date",
        verified_availability_file=verified_file,
    )

    sample = report["candidate_samples"][0]
    assert report["summary"]["overall_status"] == "DRY_RUN_READY"
    assert report["policy"]["available_at_is_externally_verified"] is True
    assert report["policy"]["verified_availability_row_count"] == 1
    assert sample["available_at_utc"] == "2026-04-25T13:30:00Z"
    assert sample["availability_quality"] == "EXTERNALLY_VERIFIED"
    assert sample["verification_source"]["source_provider"] == "fixture_verified"
    assert sample["verification_source"]["source_document_id"] == "DOC-AAPL-2026Q1"
    assert sample["verification_source"]["source_hash"] == "verified-source-hash"
    assert sample["verification_source"]["verified_at_utc"] == "2026-06-18T00:00:00Z"
    assert "NO_PROVIDER_DATA_FETCHED" in sample["warnings"]


def test_externally_verified_missing_row_keeps_candidate_policy_incomplete(tmp_path: Path) -> None:
    db_path = tmp_path / "verified_missing.db"
    verified_file = tmp_path / "verified.csv"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()
    _write_verified_csv(verified_file, ticker="MSFT", period_end_date="2026-03-31")

    report = dry_run.run_dry_run(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        include_sample_rows=1,
        legacy_availability_policy="externally_verified_release_date",
        verified_availability_file=verified_file,
    )

    sample = report["candidate_samples"][0]
    assert report["summary"]["overall_status"] == "DRY_RUN_PARTIAL_POLICY_REQUIRED"
    assert sample["available_at_utc"] is None
    assert sample["requires_policy_decision"] is True
    assert "REQUIRES_VERIFIED_AVAILABILITY" in sample["warnings"]


def test_externally_verified_duplicate_rows_fail_clearly(tmp_path: Path) -> None:
    verified_file = tmp_path / "verified.csv"
    _write_verified_csv(verified_file, ticker="AAPL", period_end_date="2026-03-31", duplicate=True)

    with pytest.raises(ValueError, match="duplicate verified availability row"):
        dry_run.load_verified_availability_file(verified_file)


def test_policy_warnings_appear_in_json_output(tmp_path: Path) -> None:
    db_path = tmp_path / "json_policy_warnings.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = dry_run.run_dry_run(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        include_sample_rows=1,
        legacy_availability_policy="research_estimated_legacy",
        legacy_availability_lag_days=60,
    )
    parsed = json.loads(dry_run.render_json(report))

    assert parsed["policy"]["selected_policy"] == "research_estimated_legacy"
    assert "RESEARCH_ESTIMATED_AVAILABILITY_NOT_AUDIT_GRADE" in parsed["candidate_samples"][0]["warnings"]


def test_statement_vintage_id_and_source_hash_are_policy_independent(tmp_path: Path) -> None:
    db_path = tmp_path / "policy_independent_hash.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    policy_required = dry_run.run_dry_run(db_path, market="usa", as_of_date="2026-06-19", include_sample_rows=1)
    live_safe = dry_run.run_dry_run(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        include_sample_rows=1,
        legacy_availability_policy="live_safe_legacy_baseline",
        legacy_available_at_utc="2026-06-19T00:00:00Z",
    )

    assert policy_required["candidate_samples"][0]["source_hash"] == live_safe["candidate_samples"][0]["source_hash"]
    assert (
        policy_required["candidate_samples"][0]["statement_vintage_id"]
        == live_safe["candidate_samples"][0]["statement_vintage_id"]
    )


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


def _write_verified_csv(
    file_path: Path,
    ticker: str,
    period_end_date: str,
    duplicate: bool = False,
) -> None:
    row = (
        "usa,"
        f"{ticker},"
        f"{period_end_date},"
        "2026-04-25T13:30:00Z,"
        "fixture_verified,"
        f"DOC-{ticker}-{period_end_date[:4]}Q1,"
        "verified-source-hash,"
        "2026-06-18T00:00:00Z,"
        "2026-04-25T13:00:00Z,"
        "fixture-ref,"
        "high,"
        "fixture notes"
    )
    rows = [row, row] if duplicate else [row]
    file_path.write_text(
        "\n".join(
            [
                (
                    "market,ticker,period_end_date,available_at_utc,source_provider,"
                    "source_document_id,source_hash,verified_at_utc,filed_at_utc,"
                    "source_url_or_ref,source_confidence,notes"
                ),
                *rows,
            ]
        ),
        encoding="utf-8",
    )
