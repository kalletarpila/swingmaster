from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_yahoo_fallback_enrich
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_quarterly_row(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    period_end_date: str,
    revenue: float | None = None,
    gross_profit: float | None = None,
    operating_income: float | None = None,
    net_income: float | None = None,
    operating_cashflow: float | None = None,
    capex: float | None = None,
    free_cashflow: float | None = None,
    cash: float | None = None,
    total_debt: float | None = None,
    shares_outstanding: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker, period_end_date, revenue, gross_profit, operating_income, net_income,
            operating_cashflow, capex, free_cashflow, cash, total_debt, shares_outstanding, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ticker,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            "SECQ",
        ),
    )


def _insert_yahoo_quarterly_row(
    conn: sqlite3.Connection,
    *,
    market: str,
    symbol: str,
    period_end_date: str,
    revenue: float | None = None,
    gross_profit: float | None = None,
    operating_income: float | None = None,
    net_income: float | None = None,
    operating_cashflow: float | None = None,
    capex: float | None = None,
    free_cashflow: float | None = None,
    cash: float | None = None,
    total_debt: float | None = None,
    shares_outstanding: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_yahoo_quarterly (
            market, symbol, period_end_date, revenue, gross_profit, operating_income, net_income,
            operating_cashflow, capex, free_cashflow, cash, total_debt, shares_outstanding,
            shares_source, shares_quality, source_run_id, run_id, created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            market,
            symbol,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            "yahoo",
            "OK",
            "YRAW",
            "YRUN",
            "2026-05-03T00:00:00+00:00",
        ),
    )


def test_does_not_overwrite_existing_sec_value(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_keep_sec.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="AAPL", period_end_date="2026-03-31", revenue=100.0)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="AAPL", period_end_date="2026-03-31", revenue=999.0)
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="AAPL",
        run_id="ENRICH1",
        dry_run=False,
        replace_audit_for_run=False,
    )

    assert summary["fields_filled"] == 0
    with sqlite3.connect(str(db_path)) as conn:
        revenue = conn.execute(
            "SELECT revenue FROM rc_fundamental_quarterly WHERE ticker='AAPL' AND period_end_date='2026-03-31'"
        ).fetchone()[0]
        audit_count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_quarterly_enrichment_audit WHERE ticker='AAPL' AND field_name='revenue'"
        ).fetchone()[0]
    assert revenue == 100.0
    assert audit_count == 0


def test_fills_missing_sec_value_and_inserts_audit_row(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_fill.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="LRCX", period_end_date="2026-03-29", total_debt=None)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="LRCX", period_end_date="2026-03-29", total_debt=123.0)
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="LRCX",
        run_id="ENRICH2",
        dry_run=False,
        replace_audit_for_run=False,
    )

    assert summary["fields_filled"] == 1
    assert summary["filled_total_debt"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT total_debt
            FROM rc_fundamental_quarterly
            WHERE ticker='LRCX' AND period_end_date='2026-03-29'
            """
        ).fetchone()
        audit_row = conn.execute(
            """
            SELECT field_name, old_value, new_value, primary_source, fallback_source, enrichment_status
            FROM rc_fundamental_quarterly_enrichment_audit
            WHERE ticker='LRCX' AND period_end_date='2026-03-29'
            """
        ).fetchone()
    assert row == (123.0,)
    assert audit_row == ("total_debt", None, 123.0, "sec_edgar", "yahoo", "FILLED_FROM_YAHOO")


def test_exact_period_matching_only(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_exact_period.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="UHAL", period_end_date="2025-12-31", shares_outstanding=None)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="UHAL", period_end_date="2025-12-30", shares_outstanding=10.0)
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="UHAL",
        run_id="ENRICH3",
        dry_run=False,
        replace_audit_for_run=False,
    )

    assert summary["no_match_count"] == 1
    assert summary["fields_filled"] == 0
    with sqlite3.connect(str(db_path)) as conn:
        value = conn.execute(
            "SELECT shares_outstanding FROM rc_fundamental_quarterly WHERE ticker='UHAL' AND period_end_date='2025-12-31'"
        ).fetchone()[0]
    assert value is None


def test_multiple_fields_filled_insert_two_audit_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_multi.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="MSFT", period_end_date="2026-03-31", free_cashflow=None, shares_outstanding=None)
        _insert_yahoo_quarterly_row(
            conn,
            market="usa",
            symbol="MSFT",
            period_end_date="2026-03-31",
            free_cashflow=55.0,
            shares_outstanding=200.0,
        )
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="MSFT",
        run_id="ENRICH4",
        dry_run=False,
        replace_audit_for_run=False,
    )

    assert summary["fields_filled"] == 2
    assert summary["filled_free_cashflow"] == 1
    assert summary["filled_shares_outstanding"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_quarterly_enrichment_audit WHERE ticker='MSFT' AND period_end_date='2026-03-31'"
        ).fetchone()[0]
    assert count == 2


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_dry.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="NVDA", period_end_date="2026-03-31", cash=None)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="NVDA", period_end_date="2026-03-31", cash=500.0)
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="NVDA",
        run_id="ENRICH5",
        dry_run=True,
        replace_audit_for_run=False,
    )

    assert summary["fields_filled"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        cash = conn.execute(
            "SELECT cash FROM rc_fundamental_quarterly WHERE ticker='NVDA' AND period_end_date='2026-03-31'"
        ).fetchone()[0]
        audit_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_enrichment_audit").fetchone()[0]
    assert cash is None
    assert audit_count == 0


def test_missing_yahoo_row_skips_and_increments_no_match(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_no_match.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="GOOG", period_end_date="2026-03-31", cash=None)
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="GOOG",
        run_id="ENRICH6",
        dry_run=False,
        replace_audit_for_run=False,
    )

    assert summary["no_match_count"] == 1
    assert summary["fields_filled"] == 0


def test_ticker_filter_enriches_only_selected_ticker(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_ticker_filter.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="AAPL", period_end_date="2026-03-31", cash=None)
        _insert_quarterly_row(conn, ticker="MSFT", period_end_date="2026-03-31", cash=None)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="AAPL", period_end_date="2026-03-31", cash=100.0)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="MSFT", period_end_date="2026-03-31", cash=200.0)
        conn.commit()

    summary = run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="AAPL",
        run_id="ENRICH7",
        dry_run=False,
        replace_audit_for_run=False,
    )

    assert summary["tickers_processed"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT ticker, cash FROM rc_fundamental_quarterly ORDER BY ticker"
        ).fetchall()
    assert rows == [("AAPL", 100.0), ("MSFT", None)]


def test_replace_audit_for_run_replaces_previous_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_replace_audit.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, ticker="TSLA", period_end_date="2026-03-31", cash=None)
        _insert_yahoo_quarterly_row(conn, market="usa", symbol="TSLA", period_end_date="2026-03-31", cash=300.0)
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly_enrichment_audit (
                ticker, period_end_date, field_name, old_value, new_value, primary_source, fallback_source,
                enrichment_status, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("TSLA", "2026-03-31", "cash", None, 1.0, "sec_edgar", "yahoo", "FILLED_FROM_YAHOO", "ENRICH8", "2026-05-03T00:00:00+00:00"),
        )
        conn.commit()

    run_fundamental_yahoo_fallback_enrich.run_yahoo_fallback_enrich(
        db_path=db_path,
        market="usa",
        ticker="TSLA",
        run_id="ENRICH8",
        dry_run=False,
        replace_audit_for_run=True,
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT field_name, new_value
            FROM rc_fundamental_quarterly_enrichment_audit
            WHERE run_id='ENRICH8'
            ORDER BY id
            """
        ).fetchall()
    assert rows == [("cash", 300.0)]


def test_cli_summary_output(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_yahoo_fallback_enrich,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="usa",
            ticker=None,
            run_id="ENRICHCLI",
            dry_run=True,
            replace_audit_for_run=False,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_yahoo_fallback_enrich,
        "run_yahoo_fallback_enrich",
        lambda **kwargs: {
            "market": "usa",
            "tickers_processed": 2,
            "quarterly_rows_scanned": 3,
            "yahoo_rows_matched": 1,
            "fields_checked": 30,
            "fields_filled": 2,
            "rows_updated": 1,
            "no_match_count": 2,
            "dry_run": "true",
            "run_id": "ENRICHCLI",
            "filled_revenue": 0,
            "filled_gross_profit": 0,
            "filled_operating_income": 0,
            "filled_net_income": 0,
            "filled_operating_cashflow": 0,
            "filled_capex": 0,
            "filled_free_cashflow": 1,
            "filled_cash": 0,
            "filled_total_debt": 0,
            "filled_shares_outstanding": 1,
        },
    )

    run_fundamental_yahoo_fallback_enrich.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY market=usa",
        "SUMMARY tickers_processed=2",
        "SUMMARY quarterly_rows_scanned=3",
        "SUMMARY yahoo_rows_matched=1",
        "SUMMARY fields_checked=30",
        "SUMMARY fields_filled=2",
        "SUMMARY rows_updated=1",
        "SUMMARY no_match_count=2",
        "SUMMARY dry_run=true",
        "SUMMARY run_id=ENRICHCLI",
        "SUMMARY filled_revenue=0",
        "SUMMARY filled_gross_profit=0",
        "SUMMARY filled_operating_income=0",
        "SUMMARY filled_net_income=0",
        "SUMMARY filled_operating_cashflow=0",
        "SUMMARY filled_capex=0",
        "SUMMARY filled_free_cashflow=1",
        "SUMMARY filled_cash=0",
        "SUMMARY filled_total_debt=0",
        "SUMMARY filled_shares_outstanding=1",
    ]
