from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_yahoo_fallback_enrich
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)


ENRICH_RUN_ID = "YAHOO_FALLBACK_RUN1"
AVAILABLE_AT_UTC = "2026-05-03T10:30:00Z"
INGESTED_AT_UTC = "2026-05-03T10:31:00Z"
VINTAGE_RUN_ID = "YAHOO_FALLBACK_VINTAGE_RUN1"
NORMALIZATION_RUN_ID = "YAHOO_FALLBACK_NORM_RUN1"
CREATED_AT_UTC = "2026-05-03T10:30:00Z"


def test_default_cli_behavior_updates_latest_and_audit_only(tmp_path: Path) -> None:
    db_path = _db_with_existing_and_yahoo(tmp_path, ticker="AAPL", period="2026-03-31", yahoo_period="2026-03-31")

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="AAPL",
        run_id=ENRICH_RUN_ID,
        dry_run=False,
        replace_audit_for_run=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        latest_row = conn.execute(
            """
            SELECT revenue, cash, total_debt, run_id
            FROM rc_fundamental_quarterly
            WHERE ticker = 'AAPL' AND period_end_date = '2026-03-31'
            """
        ).fetchone()
        audit_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_enrichment_audit").fetchone()[0]
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert summary["fields_filled"] == 2
    assert summary["rows_updated"] == 1
    assert latest_row == (100.0, 80.0, 20.0, "SECQ")
    assert audit_count == 2
    assert vintage_count == 0
    assert provenance_count == 0


@pytest.mark.parametrize(
    ("missing_kwarg", "expected_name"),
    [
        ("vintage_market", "vintage_market"),
        ("vintage_available_at_utc", "vintage_available_at_utc"),
        ("vintage_ingested_at_utc", "vintage_ingested_at_utc"),
        ("vintage_run_id", "vintage_run_id"),
    ],
)
def test_write_vintage_requires_explicit_metadata_flags(
    tmp_path: Path,
    missing_kwarg: str,
    expected_name: str,
) -> None:
    db_path = _db_with_existing_and_yahoo(tmp_path, ticker="AAPL", period="2026-03-31", yahoo_period="2026-03-31")
    kwargs = _vintage_kwargs()
    kwargs[missing_kwarg] = None

    with pytest.raises(
        ValueError,
        match=f"YAHOO_FALLBACK_ENRICH_CLI_VINTAGE_REQUIRED_FIELDS_MISSING:{expected_name}",
    ):
        run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
            db_path=db_path,
            market="usa",
            ticker="AAPL",
            run_id=ENRICH_RUN_ID,
            dry_run=False,
            replace_audit_for_run=False,
            write_vintage=True,
            **kwargs,
        )


def test_exact_date_fallback_fill_writes_one_mixed_vintage_and_provenance(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_fundamental_yahoo_fallback_enrich, "resolve_created_at_utc", lambda: CREATED_AT_UTC)
    db_path = _db_with_existing_and_yahoo(tmp_path, ticker="AAPL", period="2026-03-31", yahoo_period="2026-03-31")

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="AAPL",
        run_id=ENRICH_RUN_ID,
        dry_run=False,
        replace_audit_for_run=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, source_provider, revenue, cash, total_debt, run_id, normalization_run_id
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        before = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:29:59Z", market="usa")
        at_available = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", AVAILABLE_AT_UTC, market="usa")
        after = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-05-03T10:30:01Z", market="usa")
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[0]))

    assert summary["fields_filled"] == 2
    assert vintage_row[0].startswith("yahoo:yahoo_fallback_enrichment:usa:AAPL:2026-03-31:")
    assert vintage_row[1:] == ("yahoo", 100.0, 80.0, 20.0, VINTAGE_RUN_ID, NORMALIZATION_RUN_ID)
    assert before is None
    assert at_available is not None
    assert after is not None

    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["cash"]["source_provider"] == "yahoo"
    assert by_field["cash"]["provenance_role"] == "FALLBACK_REPORTED"
    assert by_field["cash"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert by_field["total_debt"]["source_provider"] == "yahoo"
    assert by_field["total_debt"]["merge_action"] == "YAHOO_FILLED_MISSING"
    assert by_field["revenue"]["source_provider"] == "unknown"
    assert by_field["revenue"]["merge_action"] == "SOURCE_NOT_PROVIDED"


def test_same_quarter_tolerance_fallback_fill_writes_one_mixed_vintage(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_fundamental_yahoo_fallback_enrich, "resolve_created_at_utc", lambda: CREATED_AT_UTC)
    db_path = _db_with_existing_and_yahoo(tmp_path, ticker="LRCX", period="2026-03-29", yahoo_period="2026-03-31")

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="LRCX",
        run_id=ENRICH_RUN_ID,
        dry_run=False,
        replace_audit_for_run=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        audit_row = conn.execute(
            """
            SELECT matched_yahoo_period_end_date, match_method
            FROM rc_fundamental_quarterly_enrichment_audit
            WHERE ticker = 'LRCX'
            LIMIT 1
            """
        ).fetchone()

    assert summary["quarter_aligned_matches"] == 1
    assert vintage_count == 1
    assert audit_row == ("2026-03-31", "SAME_QUARTER_DATE_TOLERANCE")


def test_missing_quarter_insert_writes_yahoo_source_vintage(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_fundamental_yahoo_fallback_enrich, "resolve_created_at_utc", lambda: CREATED_AT_UTC)
    db_path = tmp_path / "fallback_missing_insert_vintage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="AAPL", period_end_date="2026-03-31")
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="AAPL",
        run_id=ENRICH_RUN_ID,
        dry_run=False,
        replace_audit_for_run=False,
        detected_source_period_end_date="2026-03-31",
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, source_provider, revenue, cash, total_debt
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[0]))

    assert summary["rows_inserted"] == 1
    assert vintage_row[0].startswith("yahoo:yahoo_missing_quarter_insert:usa:AAPL:2026-03-31:")
    assert vintage_row[1:] == ("yahoo", 100.0, 80.0, 20.0)
    by_field = {row["field_name"]: row for row in provenance_rows}
    assert by_field["revenue"]["source_provider"] == "yahoo"
    assert by_field["revenue"]["provenance_role"] == "PROVIDER_REPORTED"
    assert by_field["revenue"]["merge_action"] == "YAHOO_INSERTED_MISSING_QUARTER"
    assert by_field["cash"]["merge_action"] == "YAHOO_INSERTED_MISSING_QUARTER"


def test_noop_enrich_creates_no_vintage_or_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_noop_vintage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="AAPL", period_end_date="2026-03-31", revenue=100.0, cash=80.0, total_debt=20.0)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="AAPL", period_end_date="2026-03-31", revenue=100.0, cash=80.0)
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="AAPL",
        run_id=ENRICH_RUN_ID,
        dry_run=False,
        replace_audit_for_run=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0] == 0
    assert summary["fields_filled"] == 0


def test_duplicate_vintage_write_surfaces_integrity_error(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_fundamental_yahoo_fallback_enrich, "resolve_created_at_utc", lambda: CREATED_AT_UTC)
    db_path = _db_with_existing_and_yahoo(tmp_path, ticker="AAPL", period="2026-03-31", yahoo_period="2026-03-31")
    kwargs = dict(
        db_path=db_path,
        market="usa",
        ticker="AAPL",
        run_id=ENRICH_RUN_ID,
        dry_run=False,
        replace_audit_for_run=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(**kwargs)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE rc_fundamental_quarterly
            SET cash = NULL, total_debt = NULL
            WHERE ticker = 'AAPL' AND period_end_date = '2026-03-31'
            """
        )
        conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(**kwargs)


def test_write_vintage_dry_run_writes_nothing(tmp_path: Path) -> None:
    db_path = _db_with_existing_and_yahoo(tmp_path, ticker="AAPL", period="2026-03-31", yahoo_period="2026-03-31")

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="AAPL",
        run_id=ENRICH_RUN_ID,
        dry_run=True,
        replace_audit_for_run=False,
        write_vintage=True,
        **_vintage_kwargs(),
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT cash, total_debt FROM rc_fundamental_quarterly WHERE ticker = 'AAPL'"
        ).fetchone()
        audit_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_enrichment_audit").fetchone()[0]
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
    assert summary["fields_filled"] == 2
    assert row == (None, None)
    assert audit_count == 0
    assert vintage_count == 0


def test_vintage_opt_in_does_not_import_provider_modules() -> None:
    import sys

    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules
    assert "yfinance" not in sys.modules
    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules


def _db_with_existing_and_yahoo(tmp_path: Path, *, ticker: str, period: str, yahoo_period: str) -> Path:
    db_path = tmp_path / f"fallback_{ticker}_{period}_{yahoo_period}.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker=ticker, period_end_date=period, revenue=100.0)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol=ticker, period_end_date=yahoo_period)
        conn.commit()
    return db_path


def _insert_quarterly_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    period_end_date: str,
    revenue: float | None = None,
    cash: float | None = None,
    total_debt: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            cash,
            total_debt,
            run_id
        ) VALUES (?, ?, ?, ?, ?, 'SECQ')
        """,
        (ticker.upper(), period_end_date, revenue, cash, total_debt),
    )


def _insert_yahoo_quarterly_row(
    conn: sqlite3.Connection,
    *,
    market: str,
    symbol: str,
    period_end_date: str,
    revenue: float | None = 100.0,
    cash: float | None = 80.0,
    total_debt: float | None = 20.0,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_yahoo_quarterly (
            market,
            symbol,
            period_end_date,
            revenue,
            cash,
            total_debt,
            source_run_id,
            run_id,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, 'YAHOO_RAW_RUN1', 'YAHOO_QUARTERLY_RUN1', ?)
        """,
        (market, symbol.upper(), period_end_date, revenue, cash, total_debt, CREATED_AT_UTC),
    )


def _vintage_kwargs() -> dict[str, str]:
    return {
        "vintage_market": "usa",
        "vintage_available_at_utc": AVAILABLE_AT_UTC,
        "vintage_ingested_at_utc": INGESTED_AT_UTC,
        "vintage_run_id": VINTAGE_RUN_ID,
        "vintage_normalization_run_id": NORMALIZATION_RUN_ID,
    }
