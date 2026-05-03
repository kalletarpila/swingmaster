from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_valuation
from swingmaster.cli.run_fundamental_migrations import run_migration


def test_ev_ebit_below_ten_is_cheap(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_cheap.db"
    osakedata_db_path = tmp_path / "osakedata_cheap.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "AAA.HE", "2026-03-31", "2026-03-31", ebit_ttm=20.0, score=50.0)
        _insert_quarterly_row(conn, "AAA.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.commit()
    _insert_close(osakedata_db_path, "AAA.HE", "2026-03-31", 10.0, "omxh")

    summary = run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="AAA.HE",
        run_id="RUN1",
        dry_run=False,
        replace=False,
    )

    assert summary["cheap_count"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT valuation_ev_ebit, valuation_bucket, valuation_status FROM rc_fundamental_valuation"
        ).fetchone()
    assert row == (5.0, "CHEAP", "OK")


def test_ev_ebit_between_ten_and_eighteen_is_fair_for_low_score(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_fair_low.db"
    osakedata_db_path = tmp_path / "osakedata_fair_low.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "BBB.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, score=59.0)
        _insert_quarterly_row(conn, "BBB.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=12.0)
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
            "SELECT valuation_ev_ebit, valuation_bucket FROM rc_fundamental_valuation"
        ).fetchone()
    assert row == (12.0, "FAIR")


def test_ev_ebit_at_or_above_eighteen_is_expensive_for_low_score(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_expensive_low.db"
    osakedata_db_path = tmp_path / "osakedata_expensive_low.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "CCC.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, score=10.0)
        _insert_quarterly_row(conn, "CCC.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=18.0)
        conn.commit()
    _insert_close(osakedata_db_path, "CCC.HE", "2026-03-31", 10.0, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="CCC.HE",
        run_id="RUN3",
        dry_run=False,
        replace=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT valuation_ev_ebit, valuation_bucket FROM rc_fundamental_valuation"
        ).fetchone()
    assert row == (18.0, "EXPENSIVE")


def test_ev_ebit_twenty_is_fair_for_high_score(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_fair_high.db"
    osakedata_db_path = tmp_path / "osakedata_fair_high.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "DDD.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, score=60.0)
        _insert_quarterly_row(conn, "DDD.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=20.0)
        conn.commit()
    _insert_close(osakedata_db_path, "DDD.HE", "2026-03-31", 10.0, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="DDD.HE",
        run_id="RUN4",
        dry_run=False,
        replace=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT valuation_ev_ebit, valuation_bucket FROM rc_fundamental_valuation"
        ).fetchone()
    assert row == (20.0, "FAIR")


def test_ev_ebit_thirty_is_expensive_for_high_score(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_expensive_high.db"
    osakedata_db_path = tmp_path / "osakedata_expensive_high.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "EEE.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, score=80.0)
        _insert_quarterly_row(conn, "EEE.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=30.0)
        conn.commit()
    _insert_close(osakedata_db_path, "EEE.HE", "2026-03-31", 10.0, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="EEE.HE",
        run_id="RUN5",
        dry_run=False,
        replace=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT valuation_ev_ebit, valuation_bucket FROM rc_fundamental_valuation"
        ).fetchone()
    assert row == (30.0, "EXPENSIVE")


def test_non_positive_ebit_is_invalid_ebit(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_invalid_ebit.db"
    osakedata_db_path = tmp_path / "osakedata_invalid_ebit.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "FFF.HE", "2026-03-31", "2026-03-31", ebit_ttm=0.0, score=50.0)
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
        row = conn.execute(
            "SELECT valuation_bucket, valuation_status FROM rc_fundamental_valuation"
        ).fetchone()
    assert row == ("INVALID", "INVALID_EBIT")


def test_missing_price_is_invalid_missing_price(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_missing_price.db"
    osakedata_db_path = tmp_path / "osakedata_missing_price.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "GGG.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, score=50.0)
        _insert_quarterly_row(conn, "GGG.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.commit()

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="GGG.HE",
        run_id="RUN7",
        dry_run=False,
        replace=False,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT valuation_bucket, valuation_status FROM rc_fundamental_valuation"
        ).fetchone()
    assert row == ("INVALID", "MISSING_PRICE")


def test_dry_run_writes_zero_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_dry.db"
    osakedata_db_path = tmp_path / "osakedata_dry.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "HHH.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, score=50.0)
        _insert_quarterly_row(conn, "HHH.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.commit()
    _insert_close(osakedata_db_path, "HHH.HE", "2026-03-31", 10.0, "omxh")

    summary = run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="HHH.HE",
        run_id="RUN8",
        dry_run=True,
        replace=True,
    )

    assert summary["rows_written"] == 0
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_valuation").fetchone()[0]
    assert count == 0


def test_replace_deletes_only_selected_scope(tmp_path: Path) -> None:
    db_path = tmp_path / "valuation_replace.db"
    osakedata_db_path = tmp_path / "osakedata_replace.db"
    run_migration(db_path)
    _create_osakedata_db(osakedata_db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_ttm_row(conn, "III.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, score=50.0)
        _insert_ttm_row(conn, "JJJ.HE", "2026-03-31", "2026-03-31", ebit_ttm=10.0, score=50.0)
        _insert_quarterly_row(conn, "III.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        _insert_quarterly_row(conn, "JJJ.HE", "2026-03-31", cash=0.0, total_debt=0.0, shares_outstanding=10.0)
        conn.execute(
            """
            INSERT INTO rc_fundamental_valuation (
                ticker, as_of_date, valuation_ev_ebit, valuation_bucket, valuation_status, market_cap,
                enterprise_value, close_price, shares_outstanding, cash, total_debt, ebit_ttm,
                fundamental_score_lifecycle, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("III.HE", "2026-03-31", 99.0, "EXPENSIVE", "OK", 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 50.0, "OLD", "2026-01-01T00:00:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_valuation (
                ticker, as_of_date, valuation_ev_ebit, valuation_bucket, valuation_status, market_cap,
                enterprise_value, close_price, shares_outstanding, cash, total_debt, ebit_ttm,
                fundamental_score_lifecycle, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("III.HE", "2025-12-31", 77.0, "EXPENSIVE", "OK", 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 50.0, "OLD2", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
    _insert_close(osakedata_db_path, "III.HE", "2026-03-31", 10.0, "omxh")
    _insert_close(osakedata_db_path, "JJJ.HE", "2026-03-31", 10.0, "omxh")

    run_fundamental_valuation.run_fundamental_valuation(
        db_path=db_path,
        osakedata_db_path=osakedata_db_path,
        market="omxh",
        as_of_date="2026-03-31",
        ticker="III.HE",
        run_id="RUN9",
        dry_run=False,
        replace=True,
    )

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT ticker, as_of_date, run_id FROM rc_fundamental_valuation ORDER BY as_of_date, ticker"
        ).fetchall()
    assert rows == [
        ("III.HE", "2025-12-31", "OLD2"),
        ("III.HE", "2026-03-31", "RUN9"),
    ]


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
        "SUMMARY dry_run=true",
        "SUMMARY replace=false",
        "SUMMARY run_id=RUN10",
    ]


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
    score: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_ttm (
            ticker, as_of_date, latest_period_end_date, ebit_ttm, fundamental_score_lifecycle, run_id
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (ticker, as_of_date, latest_period_end_date, ebit_ttm, score, "TTM_RUN"),
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
