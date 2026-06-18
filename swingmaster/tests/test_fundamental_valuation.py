from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_valuation
from swingmaster.cli.run_fundamental_migrations import run_migration


def test_exact_ttm_match_uses_zero_staleness(tmp_path: Path) -> None:
    row = _run_single_ticker_case(
        tmp_path,
        ticker="AAA.HE",
        valuation_date="2026-03-31",
        ttm_as_of_date="2026-03-31",
        close=10.0,
        shares_outstanding=10.0,
        cash=0.0,
        total_debt=0.0,
        ebit_ttm=10.0,
        fcf_ttm=7.0,
        ebit_margin_ttm=0.10,
        score=50.0,
    )
    assert row == ("2026-03-31", "2026-03-31", 0, 10.0, 0.07, 0.10, "CHEAP", "OK", 0, 0, "V2")


def test_latest_prior_ttm_fallback_uses_previous_fundamental_without_lookahead(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_fallback.db"
    osakedata_db_path = tmp_path / "osakedata_fallback.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "BBB.HE", "2025-12-31", "2025-12-31", ebit_ttm=10.0, fcf_ttm=5.0, ebit_margin_ttm=0.10, score=50.0)
        _insert_ttm_row(conn, "BBB.HE", "2026-06-30", "2026-06-30", ebit_ttm=999.0, fcf_ttm=999.0, ebit_margin_ttm=0.50, score=99.0)
        _insert_quarterly_row(conn, "BBB.HE", "2025-12-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        _insert_quarterly_row(conn, "BBB.HE", "2026-06-30", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.commit()
    _insert_close(osakedata_db_path, "BBB.HE", "2026-03-31", 10.0, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="BBB.HE",
        run_id="RUN2",
        dry_run=False,
        replace=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT as_of_date, valuation_fundamental_as_of_date, valuation_fundamental_staleness_days,
                   valuation_ev_ebit, valuation_bucket, valuation_status
            FROM rc_fundamental_valuation
            """
        ).fetchone()
    assert row == ("2026-03-31", "2025-12-31", 90, 10.0, "FAIR", "OK")


def test_staleness_between_121_and_240_days_is_stale_fundamentals(tmp_path: Path) -> None:
    row = _run_single_ticker_case(
        tmp_path,
        ticker="CCC.HE",
        valuation_date="2026-08-01",
        ttm_as_of_date="2026-03-31",
        close=10.0,
        shares_outstanding=10.0,
        cash=0.0,
        total_debt=0.0,
        ebit_ttm=10.0,
        fcf_ttm=5.0,
        ebit_margin_ttm=0.10,
        score=50.0,
    )
    assert row == ("2026-08-01", "2026-03-31", 123, 10.0, 0.05, 0.10, "FAIR", "STALE_FUNDAMENTALS", 0, 0, "V2")


def test_staleness_above_240_days_is_too_stale_and_invalid(tmp_path: Path) -> None:
    row = _run_single_ticker_case(
        tmp_path,
        ticker="DDD.HE",
        valuation_date="2026-12-31",
        ttm_as_of_date="2026-03-31",
        close=10.0,
        shares_outstanding=10.0,
        cash=0.0,
        total_debt=0.0,
        ebit_ttm=10.0,
        fcf_ttm=5.0,
        ebit_margin_ttm=0.10,
        score=50.0,
    )
    assert row == ("2026-12-31", "2026-03-31", 275, 10.0, 0.05, 0.10, "INVALID", "TOO_STALE_FUNDAMENTALS", 0, 0, "V2")


def test_ev_ebit_at_or_above_thirty_is_very_expensive(tmp_path: Path) -> None:
    row = _run_single_ticker_case(
        tmp_path,
        ticker="EEE.HE",
        valuation_date="2026-03-31",
        ttm_as_of_date="2026-03-31",
        close=10.0,
        shares_outstanding=30.0,
        cash=0.0,
        total_debt=0.0,
        ebit_ttm=10.0,
        fcf_ttm=5.0,
        ebit_margin_ttm=0.10,
        score=50.0,
    )
    assert row[6] == "VERY_EXPENSIVE"


def test_missing_fcf_is_invalid_missing_fcf(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_missing_fcf.db"
    osakedata_db_path = tmp_path / "osakedata_missing_fcf.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "FFF.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, fcf_ttm=None, ebit_margin_ttm=0.12, score=50.0)
        _insert_quarterly_row(conn, "FFF.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.commit()
    _insert_close(osakedata_db_path, "FFF.HE", "2026-03-31", 10.0, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="FFF.HE",
        run_id="RUN6",
        dry_run=False,
        replace=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute("SELECT valuation_bucket, valuation_status FROM rc_fundamental_valuation").fetchone()
    assert row == ("INVALID", "MISSING_FCF")


def test_missing_total_debt_assumes_zero_and_stays_ok(tmp_path: Path) -> None:
    row = _run_single_ticker_case(
        tmp_path,
        ticker="TD1.HE",
        valuation_date="2026-03-31",
        ttm_as_of_date="2026-03-31",
        close=10.0,
        shares_outstanding=10.0,
        cash=0.0,
        total_debt=None,
        ebit_ttm=10.0,
        fcf_ttm=7.0,
        ebit_margin_ttm=0.10,
        score=50.0,
    )
    assert row == ("2026-03-31", "2026-03-31", 0, 10.0, 0.07, 0.10, "CHEAP", "OK", 1, 0, "V2")


def test_missing_cash_assumes_zero_and_stays_ok(tmp_path: Path) -> None:
    row = _run_single_ticker_case(
        tmp_path,
        ticker="CS1.HE",
        valuation_date="2026-03-31",
        ttm_as_of_date="2026-03-31",
        close=10.0,
        shares_outstanding=10.0,
        cash=None,
        total_debt=0.0,
        ebit_ttm=10.0,
        fcf_ttm=7.0,
        ebit_margin_ttm=0.10,
        score=50.0,
    )
    assert row == ("2026-03-31", "2026-03-31", 0, 10.0, 0.07, 0.10, "CHEAP", "OK", 0, 1, "V2")


def test_missing_both_ev_inputs_assume_zero_and_stay_ok(tmp_path: Path) -> None:
    row = _run_single_ticker_case(
        tmp_path,
        ticker="EV0.HE",
        valuation_date="2026-03-31",
        ttm_as_of_date="2026-03-31",
        close=10.0,
        shares_outstanding=10.0,
        cash=None,
        total_debt=None,
        ebit_ttm=10.0,
        fcf_ttm=7.0,
        ebit_margin_ttm=0.10,
        score=50.0,
    )
    assert row == ("2026-03-31", "2026-03-31", 0, 10.0, 0.07, 0.10, "CHEAP", "OK", 1, 1, "V2")


def test_missing_total_debt_with_invalid_ebit_still_returns_invalid_ebit(tmp_path: Path) -> None:
    row = _run_single_ticker_case(
        tmp_path,
        ticker="BAD.HE",
        valuation_date="2026-03-31",
        ttm_as_of_date="2026-03-31",
        close=10.0,
        shares_outstanding=10.0,
        cash=0.0,
        total_debt=None,
        ebit_ttm=0.0,
        fcf_ttm=7.0,
        ebit_margin_ttm=0.10,
        score=50.0,
    )
    assert row[7] == "INVALID_EBIT"
    assert row[6] == "INVALID"


def test_existing_v1_invalid_statuses_still_work(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_v1_invalids.db"
    osakedata_db_path = tmp_path / "osakedata_v1_invalids.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "GGG.HE", "2026-03-31", "2026-03-31", ebit_ttm=0.0, fcf_ttm=5.0, ebit_margin_ttm=0.10, score=50.0)
        _insert_quarterly_row(conn, "GGG.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        _insert_ttm_row(conn, "HHH.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, fcf_ttm=5.0, ebit_margin_ttm=0.10, score=50.0)
        _insert_quarterly_row(conn, "HHH.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.commit()
    _insert_close(osakedata_db_path, "GGG.HE", "2026-03-31", 10.0, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="GGG.HE",
        run_id="RUN7A",
        dry_run=False,
        replace=False,
    )
    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="HHH.HE",
        run_id="RUN7B",
        dry_run=False,
        replace=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute("SELECT ticker, valuation_status FROM rc_fundamental_valuation ORDER BY ticker").fetchall()
    assert rows == [("GGG.HE", "INVALID_EBIT"), ("HHH.HE", "MISSING_PRICE")]


def test_dry_run_writes_zero_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_dry.db"
    osakedata_db_path = tmp_path / "osakedata_dry.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "III.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, fcf_ttm=7.0, ebit_margin_ttm=0.10, score=50.0)
        _insert_quarterly_row(conn, "III.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.commit()
    _insert_close(osakedata_db_path, "III.HE", "2026-03-31", 10.0, "omxh")

    summary = run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="III.HE",
        run_id="RUN8",
        dry_run=True,
        replace=True,
    )

    assert summary["rows_written"] == 0
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_valuation").fetchone()[0]
    assert count == 0


def test_replace_behavior_remains_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_replace.db"
    osakedata_db_path = tmp_path / "osakedata_replace.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "JJJ.HE", "2025-12-31", "2025-12-31", ebit_ttm=10.0, fcf_ttm=7.0, ebit_margin_ttm=0.10, score=50.0)
        _insert_quarterly_row(conn, "JJJ.HE", "2025-12-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.execute(
            """
            INSERT INTO rc_fundamental_valuation (
                ticker, as_of_date, valuation_bucket, valuation_status, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("JJJ.HE", "2025-09-30", "FAIR", "OK", "OLD2", "2026-01-01T00:00:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_valuation (
                ticker, as_of_date, valuation_bucket, valuation_status, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("JJJ.HE", "2026-03-31", "FAIR", "OK", "OLD", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
    _insert_close(osakedata_db_path, "JJJ.HE", "2026-03-31", 10.0, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="JJJ.HE",
        run_id="RUN9",
        dry_run=False,
        replace=True,
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute("SELECT ticker, as_of_date, run_id FROM rc_fundamental_valuation ORDER BY as_of_date").fetchall()
    assert rows == [("JJJ.HE", "2025-09-30", "OLD2"), ("JJJ.HE", "2026-03-31", "RUN9")]


def test_cli_summary_output(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_cli.db"
    osakedata_db_path = tmp_path / "osakedata_cli.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    monkeypatch.setattr(
        run_fundamental_valuation,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            osakedata_db=str(osakedata_db_path),
            market="omxh",
            as_of_date="2026-03-31",
            ticker=None,
            run_id="RUN10",
            dry_run=True,
            replace=False,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_valuation,
        "run_fundamental_valuation",
        lambda **kwargs: {
            "market": "omxh",
            "as_of_date": "2026-03-31",
            "tickers_processed": 2,
            "rows_written": 0,
            "ok_count": 1,
            "invalid_count": 1,
            "cheap_count": 0,
            "fair_count": 1,
            "expensive_count": 0,
            "very_expensive_count": 0,
            "debt_assumed_zero_count": 0,
            "cash_assumed_zero_count": 0,
            "model_version": "V2",
            "dry_run": "true",
            "replace": "false",
            "run_id": "RUN10",
        },
    )

    run_fundamental_valuation.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY market=omxh",
        "SUMMARY as_of_date=2026-03-31",
        "SUMMARY tickers_processed=2",
        "SUMMARY rows_written=0",
        "SUMMARY ok_count=1",
        "SUMMARY invalid_count=1",
        "SUMMARY cheap_count=0",
        "SUMMARY fair_count=1",
        "SUMMARY expensive_count=0",
        "SUMMARY very_expensive_count=0",
        "SUMMARY debt_assumed_zero_count=0",
        "SUMMARY cash_assumed_zero_count=0",
        "SUMMARY model_version=V2",
        "SUMMARY dry_run=true",
        "SUMMARY replace=false",
        "SUMMARY run_id=RUN10",
    ]


def _run_single_ticker_case(
    tmp_path: Path,
    *,
    ticker: str,
    valuation_date: str,
    ttm_as_of_date: str,
    close: float,
    shares_outstanding: float,
    cash: float,
    total_debt: float,
    ebit_ttm: float,
    fcf_ttm: float | None,
    ebit_margin_ttm: float | None,
    score: float | None,
) -> tuple[str, str, int, float | None, float | None, float | None, str, str, int | None, int | None, str | None]:
    db_path = tmp_path / f"{ticker.replace('.', '_')}.db"
    osakedata_db_path = tmp_path / f"{ticker.replace('.', '_')}_osakedata.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, ticker, ttm_as_of_date, ttm_as_of_date, ebit_ttm=ebit_ttm, fcf_ttm=fcf_ttm, ebit_margin_ttm=ebit_margin_ttm, score=score)
        _insert_quarterly_row(conn, ticker, ttm_as_of_date, cash=cash, total_debt=total_debt, shares_outstanding=shares_outstanding)
        conn.commit()
    _insert_close(osakedata_db_path, ticker, valuation_date, close, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date=valuation_date,
        ticker=ticker,
        run_id="RUN",
        dry_run=False,
        replace=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT as_of_date, valuation_fundamental_as_of_date, valuation_fundamental_staleness_days,
                   valuation_ev_ebit, valuation_fcf_yield, valuation_ebit_margin,
                   valuation_bucket, valuation_status, debt_assumed_zero, cash_assumed_zero, valuation_model_version
            FROM rc_fundamental_valuation
            """
        ).fetchone()


def _create_osakedata_db(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE osakedata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                osake TEXT,
                pvm TEXT,
                close REAL,
                market TEXT
            )
            """
        )
        conn.commit()


def _insert_close(db_path: Path, ticker: str, pvm: str, close: float, market: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO osakedata (osake, pvm, close, market)
            VALUES (?, ?, ?, ?)
            """,
            (ticker, pvm, close, market),
        )
        conn.commit()


def _insert_ttm_row(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    latest_period_end_date: str,
    *,
    ebit_ttm: float | None,
    fcf_ttm: float | None,
    ebit_margin_ttm: float | None,
    score: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_ttm (
            ticker, as_of_date, latest_period_end_date, ebit_ttm, fcf_ttm, ebit_margin_ttm, fundamental_score_lifecycle, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ticker, as_of_date, latest_period_end_date, ebit_ttm, fcf_ttm, ebit_margin_ttm, score, "TTM_RUN"),
    )


def _insert_quarterly_row(
    conn: sqlite3.Connection,
    ticker: str,
    period_end_date: str,
    *,
    cash: float | None,
    total_debt: float | None,
    shares_outstanding: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker, period_end_date, cash, total_debt, shares_outstanding, run_id
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (ticker, period_end_date, cash, total_debt, shares_outstanding, "Q_RUN"),
    )
