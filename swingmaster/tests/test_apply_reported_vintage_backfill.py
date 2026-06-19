from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import apply_reported_vintage_backfill as apply_cli
from swingmaster.cli import dry_run_reported_vintage_backfill as dry_run
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_reader import get_pit_quarterly_vintage
from swingmaster.fundamentals.reported_vintage_writer import insert_quarterly_vintage_row


POLICY = "live_safe_legacy_baseline"
AVAILABLE_AT = "2026-06-19T00:00:00Z"
APPLY_TS = "2026-06-19T12:00:00Z"


def test_apply_requires_confirm_write(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])

    with pytest.raises(ValueError, match="--confirm-write is required"):
        apply_cli.apply_backfill(
            db_path,
            market="usa",
            as_of_date="2026-06-19",
            legacy_availability_policy=POLICY,
            legacy_available_at_utc=AVAILABLE_AT,
            legacy_availability_lag_days=None,
            verified_availability_file=None,
            expected_vintage_rows=1,
            expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS),
            confirm_write=False,
            apply_timestamp_utc=APPLY_TS,
        )


def test_apply_refuses_if_dry_run_status_is_not_ready(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])

    with pytest.raises(RuntimeError, match="DRY_RUN_NOT_READY"):
        _apply(db_path, policy="policy_required", expected_vintage_rows=1, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))


def test_apply_refuses_if_expected_vintage_count_mismatches(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])

    with pytest.raises(RuntimeError, match="EXPECTED_VINTAGE_ROWS_MISMATCH"):
        _apply(db_path, expected_vintage_rows=2, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))


def test_apply_refuses_if_expected_provenance_count_mismatches(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])

    with pytest.raises(RuntimeError, match="EXPECTED_PROVENANCE_ROWS_MISMATCH"):
        _apply(db_path, expected_vintage_rows=1, expected_provenance_rows=999)


def test_apply_inserts_vintage_and_provenance_rows(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])

    summary = _apply(db_path, expected_vintage_rows=1, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))

    assert summary["status"] == "APPLY_COMPLETE"
    assert summary["vintage_rows_written"] == 1
    assert summary["provenance_rows_written"] == len(dry_run.FINANCIAL_FIELDS)
    with sqlite3.connect(str(db_path)) as conn:
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
        vintage = conn.execute(
            """
            SELECT source_provider, availability_quality, available_at_utc, ingested_at_utc, normalization_run_id
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
    assert vintage_count == 1
    assert provenance_count == len(dry_run.FINANCIAL_FIELDS)
    assert vintage == ("UNKNOWN_LEGACY", "LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL", AVAILABLE_AT, APPLY_TS, "RUN1")


def test_apply_uses_plain_insert_and_does_not_replace_existing_vintage(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])
    with sqlite3.connect(str(db_path)) as conn:
        row = _latest_row("AAPL", "2026-03-31")
        source_hash = dry_run.build_legacy_source_hash(row, "usa")
        insert_quarterly_vintage_row(conn, _vintage_row(row, source_hash))
        conn.commit()

    with pytest.raises(RuntimeError, match="EXPECTED_VINTAGE_ROWS_MISMATCH"):
        _apply(db_path, expected_vintage_rows=1, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))

    with sqlite3.connect(str(db_path)) as conn:
        source_provider = conn.execute("SELECT source_provider FROM rc_fundamental_quarterly_vintage").fetchone()[0]
    assert source_provider == "PREEXISTING"


def test_apply_rolls_back_when_provenance_insert_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])

    def _fail_insert(*args: object, **kwargs: object) -> int:
        raise sqlite3.IntegrityError("forced provenance failure")

    monkeypatch.setattr(apply_cli, "insert_quarterly_field_provenance_rows", _fail_insert)

    with pytest.raises(sqlite3.IntegrityError, match="forced provenance failure"):
        _apply(db_path, expected_vintage_rows=1, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))

    with sqlite3.connect(str(db_path)) as conn:
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
    assert vintage_count == 0
    assert provenance_count == 0


def test_apply_leaves_latest_quarterly_rows_unchanged(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])
    before = _latest_rows_snapshot(db_path)

    _apply(db_path, expected_vintage_rows=1, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))

    assert _latest_rows_snapshot(db_path) == before


def test_post_apply_dry_run_becomes_noop_already_has_vintage(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])
    _apply(db_path, expected_vintage_rows=1, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))

    report = dry_run.run_dry_run(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        legacy_availability_policy=POLICY,
        legacy_available_at_utc=AVAILABLE_AT,
    )

    assert report["summary"]["planned_vintage_rows"] == 0
    assert report["summary"]["already_has_vintage_rows"] == 1
    assert report["skipped_reasons"] == {"ALREADY_HAS_VINTAGE": 1}


def test_pit_reader_returns_row_at_or_after_live_safe_timestamp(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])
    _apply(db_path, expected_vintage_rows=1, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))

    with sqlite3.connect(str(db_path)) as conn:
        row = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-06-19T00:00:00Z", market="usa")

    assert row is not None
    assert row["available_at_utc"] == AVAILABLE_AT


def test_pit_reader_returns_none_before_live_safe_timestamp(tmp_path: Path) -> None:
    db_path = _db_with_latest_rows(tmp_path, [("AAPL", "2026-03-31")])
    _apply(db_path, expected_vintage_rows=1, expected_provenance_rows=len(dry_run.FINANCIAL_FIELDS))

    with sqlite3.connect(str(db_path)) as conn:
        row = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-06-18T23:59:59Z", market="usa")

    assert row is None


def test_apply_module_does_not_import_provider_network_modules() -> None:
    module_globals = set(apply_cli.__dict__)

    assert "sec_edgar" not in module_globals
    assert "yfinance" not in module_globals
    assert "finnhub" not in module_globals


def _apply(
    db_path: Path,
    expected_vintage_rows: int,
    expected_provenance_rows: int,
    policy: str = POLICY,
) -> dict[str, object]:
    return apply_cli.apply_backfill(
        db_path,
        market="usa",
        as_of_date="2026-06-19",
        legacy_availability_policy=policy,
        legacy_available_at_utc=AVAILABLE_AT if policy == POLICY else None,
        legacy_availability_lag_days=None,
        verified_availability_file=None,
        expected_vintage_rows=expected_vintage_rows,
        expected_provenance_rows=expected_provenance_rows,
        confirm_write=True,
        apply_timestamp_utc=APPLY_TS,
    )


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


def _vintage_row(row: dict[str, object], source_hash: str) -> dict[str, object]:
    return {
        "ticker": row["ticker"],
        "market": "usa",
        "period_end_date": row["period_end_date"],
        "statement_vintage_id": dry_run.build_legacy_statement_vintage_id(row, "usa", source_hash),
        "source_provider": "PREEXISTING",
        "source_document_id": None,
        "source_hash": source_hash,
        "revision_number": 1,
        "is_restated": 0,
        "supersedes_vintage_id": None,
        "availability_quality": "PREEXISTING",
        "filed_at_utc": None,
        "available_at_utc": AVAILABLE_AT,
        "ingested_at_utc": APPLY_TS,
        "provider_observed_at_utc": None,
        "run_id": "PREEXISTING_RUN",
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
        "created_at_utc": APPLY_TS,
        "updated_at_utc": None,
    }


def _latest_rows_snapshot(db_path: Path) -> list[tuple[object, ...]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            f"""
            SELECT {", ".join(dry_run.LATEST_COLUMNS)}
            FROM rc_fundamental_quarterly
            ORDER BY ticker, period_end_date
            """
        ).fetchall()
