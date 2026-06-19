from __future__ import annotations

import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import preflight_reported_vintage_backfill as preflight
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_writer import insert_quarterly_vintage_row


def test_missing_vintage_tables_returns_blocked_missing_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_vintage_tables.db"
    _create_latest_only_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31")

    assert report["summary"]["overall_status"] == "BLOCKED_MISSING_SCHEMA"
    assert report["summary"]["missing_vintage_table"] is True
    assert report["summary"]["missing_provenance_table"] is True


def test_no_quarterly_rows_returns_no_source_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "no_source_rows.db"
    run_migration(db_path)

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31")

    assert report["summary"]["overall_status"] == "NO_SOURCE_ROWS"
    assert report["summary"]["latest_quarterly_row_count"] == 0
    assert report["candidates"] == []


def test_latest_quarterly_rows_are_counted_and_listed(tmp_path: Path) -> None:
    db_path = tmp_path / "latest_rows.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        _insert_latest_row(conn, "MSFT", "2026-03-31")
        conn.commit()

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31")

    assert report["summary"]["latest_quarterly_row_count"] == 2
    assert [candidate["ticker"] for candidate in report["candidates"]] == ["AAPL", "MSFT"]


def test_matching_vintage_rows_are_detected(tmp_path: Path) -> None:
    db_path = tmp_path / "matching_vintage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        insert_quarterly_vintage_row(conn, _vintage_row("AAPL", "2026-03-31"))
        conn.commit()

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31")

    candidate = report["candidates"][0]
    assert candidate["matching_vintage_exists"] is True
    assert candidate["eligible_for_backfill"] is False
    assert report["summary"]["already_backfilled_rows"] == 1


def test_unmatched_latest_rows_are_eligible_but_metadata_incomplete(tmp_path: Path) -> None:
    db_path = tmp_path / "metadata_incomplete.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31")

    candidate = report["candidates"][0]
    assert report["summary"]["overall_status"] == "PARTIAL_METADATA_REQUIRED"
    assert candidate["eligible_for_backfill"] is True
    assert candidate["metadata_status"] == "INCOMPLETE_LEGACY_METADATA"
    assert "available_at_utc" in candidate["missing_required_metadata"]


def test_metadata_gap_report_lists_unavailable_pit_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "metadata_gaps.db"
    run_migration(db_path)

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31")
    gaps = {gap["field"]: gap for gap in report["metadata_gaps"]}

    assert gaps["available_at_utc"]["status"] == "UNAVAILABLE"
    assert gaps["filed_at_utc"]["status"] == "UNAVAILABLE"
    assert gaps["statement_vintage_id"]["status"] == "UNAVAILABLE"
    assert gaps["statement_vintage_id"]["proposed_only"] is True


def test_json_output_contains_summary_candidates_and_metadata_gaps(tmp_path: Path) -> None:
    db_path = tmp_path / "json_output.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        conn.commit()

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31")
    parsed = json.loads(preflight.render_json(report))

    assert parsed["summary"]["latest_quarterly_row_count"] == 1
    assert parsed["candidates"][0]["ticker"] == "AAPL"
    assert parsed["metadata_gaps"]


def test_tickers_filter_candidates(tmp_path: Path) -> None:
    db_path = tmp_path / "tickers_filter.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        _insert_latest_row(conn, "MSFT", "2026-03-31")
        conn.commit()

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31", tickers=["msft"])

    assert [candidate["ticker"] for candidate in report["candidates"]] == ["MSFT"]


def test_max_tickers_limits_candidates(tmp_path: Path) -> None:
    db_path = tmp_path / "max_tickers.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest_row(conn, "AAPL", "2026-03-31")
        _insert_latest_row(conn, "MSFT", "2026-03-31")
        _insert_latest_row(conn, "NVDA", "2026-03-31")
        conn.commit()

    report = preflight.run_preflight(db_path, market="usa", as_of_date="2026-12-31", max_tickers=2)

    assert [candidate["ticker"] for candidate in report["candidates"]] == ["AAPL", "MSFT"]


def test_fail_if_blocked_exits_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "fail_if_blocked.db"
    _create_latest_only_db(db_path)
    monkeypatch.setattr(
        preflight,
        "parse_args",
        lambda: Namespace(
            fundamentals_db=str(db_path),
            market="usa",
            as_of_date="2026-12-31",
            tickers=None,
            format="json",
            max_tickers=None,
            fail_if_blocked=True,
        ),
    )

    with pytest.raises(SystemExit) as excinfo:
        preflight.main()

    assert excinfo.value.code == 1


def test_readonly_mode_rejects_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "readonly.db"
    run_migration(db_path)

    with preflight.open_readonly_db(db_path) as conn:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute(
                """
                INSERT INTO rc_fundamental_quarterly (ticker, period_end_date)
                VALUES ('AAPL', '2026-03-31')
                """
            )


def _create_latest_only_db(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
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
                run_id TEXT,
                PRIMARY KEY (ticker, period_end_date)
            )
            """
        )
        conn.commit()


def _insert_latest_row(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            ebit,
            ebitda,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            currency,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            period_end_date,
            100.0,
            45.0,
            30.0,
            29.0,
            34.0,
            25.0,
            35.0,
            -5.0,
            30.0,
            80.0,
            20.0,
            1000.0,
            "USD",
            "RUN1",
        ),
    )


def _vintage_row(ticker: str, period_end_date: str) -> dict[str, object]:
    return {
        "ticker": ticker,
        "market": "usa",
        "period_end_date": period_end_date,
        "statement_vintage_id": f"{ticker}_{period_end_date}_V1",
        "source_provider": "UNKNOWN_LEGACY",
        "source_document_id": None,
        "source_hash": "hash1",
        "revision_number": 1,
        "is_restated": 0,
        "supersedes_vintage_id": None,
        "availability_quality": "LEGACY_ESTIMATED",
        "filed_at_utc": None,
        "available_at_utc": "2026-04-30T00:00:00Z",
        "ingested_at_utc": "2026-04-30T01:00:00Z",
        "provider_observed_at_utc": None,
        "run_id": "RUN1",
        "provider_run_id": None,
        "normalization_run_id": None,
        "enrichment_run_id": None,
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
        "created_at_utc": "2026-04-30T01:00:01Z",
        "updated_at_utc": None,
    }
