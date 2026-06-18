from __future__ import annotations

import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import preflight_fundamental_ess_readiness as preflight


def test_minimal_complete_ticker_is_ok(tmp_path: Path) -> None:
    db_path = tmp_path / "complete.db"
    _create_core_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_complete_ticker(conn, "AAPL", "usa")
        conn.commit()

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        tickers=["AAPL"],
    )

    assert report["summary"]["overall_status"] == "OK"
    assert report["summary"]["ticker_count_checked"] == 1
    row = report["tickers"][0]
    assert row["ticker"] == "AAPL"
    assert row["reported_status"] == "OK"
    assert row["derived_status"] == "OK"
    assert row["valuation_status"] == "OK"
    assert row["rank_status"] == "OK"
    assert row["quarter_state_status"] == "OK"
    assert row["event_status"] == "NOT_APPLICABLE"
    assert row["overall_ess_readiness"] == "OK"


def test_missing_reported_fundamentals_is_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_reported.db"
    _create_core_db(db_path)

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        tickers=["MSFT"],
    )

    row = report["tickers"][0]
    assert row["reported_status"] == "MISSING"
    assert row["overall_ess_readiness"] == "MISSING"


def test_partial_ticker_has_reported_but_missing_derived_valuation_and_rank(tmp_path: Path) -> None:
    db_path = tmp_path / "partial.db"
    _create_core_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO rc_fundamental_quarterly (ticker, period_end_date) VALUES (?, ?)",
            ("NVDA", "2026-03-31"),
        )
        conn.commit()

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        tickers=["NVDA"],
    )

    row = report["tickers"][0]
    assert row["reported_status"] == "OK"
    assert row["derived_status"] == "MISSING"
    assert row["valuation_status"] == "MISSING"
    assert row["rank_status"] == "MISSING"
    assert row["overall_ess_readiness"] == "PARTIAL"


def test_missing_optional_tables_do_not_crash(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_optional.db"
    _create_core_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_complete_ticker(conn, "AAPL", "usa")
        conn.commit()

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        tickers=["AAPL"],
    )

    row = report["tickers"][0]
    assert row["missing_period_status"] == "NOT_APPLICABLE"
    assert row["reporting_frequency_status"] == "NOT_APPLICABLE"


def test_schema_gap_inspection_detects_missing_pit_columns_and_event_table(tmp_path: Path) -> None:
    db_path = tmp_path / "schema_gaps.db"
    _create_core_db(db_path)

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        tickers=["AAPL"],
    )

    gaps_by_concept = {gap["concept"]: gap for gap in report["schema_gaps"]}
    assert gaps_by_concept["available_at_utc"]["status"] == "MISSING"
    assert gaps_by_concept["statement_vintage_id"]["status"] == "MISSING"
    assert gaps_by_concept["event table"]["status"] == "MISSING"


def test_as_of_filtering_excludes_future_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "future_rows.db"
    _create_core_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO rc_fundamental_quarterly (ticker, period_end_date) VALUES (?, ?)",
            ("AAPL", "2026-06-30"),
        )
        conn.execute(
            "INSERT INTO rc_fundamental_ttm (ticker, as_of_date) VALUES (?, ?)",
            ("AAPL", "2026-06-30"),
        )
        conn.execute(
            "INSERT INTO rc_fundamental_valuation (ticker, as_of_date) VALUES (?, ?)",
            ("AAPL", "2026-06-30"),
        )
        conn.execute(
            "INSERT INTO rc_fundamental_score_percentile (ticker, target_date) VALUES (?, ?)",
            ("AAPL", "2026-06-30"),
        )
        conn.commit()

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        tickers=["AAPL"],
    )

    row = report["tickers"][0]
    assert row["reported_status"] == "MISSING"
    assert row["derived_status"] == "MISSING"
    assert row["valuation_status"] == "MISSING"
    assert row["rank_status"] == "MISSING"


def test_quarter_state_market_mismatch_warns_and_is_not_ok(tmp_path: Path) -> None:
    db_path = tmp_path / "market_mismatch.db"
    _create_core_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_complete_ticker(conn, "AAPL", "omxh")
        conn.commit()

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        tickers=["AAPL"],
    )

    row = report["tickers"][0]
    assert row["quarter_state_status"] == "PARTIAL"
    assert row["overall_ess_readiness"] == "OK"
    assert row["warnings"] == ["QUARTER_STATE_MARKET_MISMATCH:omxh"]
    assert report["summary"]["warning_count"] == 1


def test_json_output_contains_summary_tickers_and_schema_gaps(tmp_path: Path) -> None:
    db_path = tmp_path / "json_output.db"
    _create_core_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_complete_ticker(conn, "AAPL", "usa")
        conn.commit()

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        tickers=["AAPL"],
    )
    parsed = json.loads(preflight.render_json(report))

    assert parsed["summary"]["overall_status"] == "OK"
    assert parsed["tickers"][0]["ticker"] == "AAPL"
    assert parsed["schema_gaps"]


def test_cli_invalid_db_path_exits_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    missing_db = tmp_path / "missing.db"
    monkeypatch.setattr(
        preflight,
        "parse_args",
        lambda: Namespace(
            fundamentals_db=str(missing_db),
            market="usa",
            as_of_date="2026-03-31",
            osakedata_db=None,
            tickers="AAPL",
            format="json",
            fail_if_not_ok=False,
            max_tickers=None,
        ),
    )

    with pytest.raises(SystemExit) as excinfo:
        preflight.main()
    assert excinfo.value.code == 2


def test_fail_if_not_ok_exits_nonzero_for_partial(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fail_if_partial.db"
    _create_core_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO rc_fundamental_quarterly (ticker, period_end_date) VALUES (?, ?)",
            ("NVDA", "2026-03-31"),
        )
        conn.commit()
    monkeypatch.setattr(
        preflight,
        "parse_args",
        lambda: Namespace(
            fundamentals_db=str(db_path),
            market="usa",
            as_of_date="2026-03-31",
            osakedata_db=None,
            tickers="NVDA",
            format="json",
            fail_if_not_ok=True,
            max_tickers=None,
        ),
    )

    with pytest.raises(SystemExit) as excinfo:
        preflight.main()
    assert excinfo.value.code == 1


def test_load_tickers_respects_max_tickers(tmp_path: Path) -> None:
    db_path = tmp_path / "max_tickers.db"
    _create_core_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("INSERT INTO rc_fundamental_quarterly (ticker, period_end_date) VALUES ('BBB', '2026-03-31')")
        conn.execute("INSERT INTO rc_fundamental_quarterly (ticker, period_end_date) VALUES ('AAA', '2026-03-31')")
        conn.commit()

    report = preflight.run_preflight(
        fundamentals_db_path=db_path,
        market="usa",
        as_of_date="2026-03-31",
        max_tickers=1,
    )

    assert [row["ticker"] for row in report["tickers"]] == ["AAA"]


def _create_core_db(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_ttm (
                ticker TEXT NOT NULL,
                as_of_date TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_valuation (
                ticker TEXT NOT NULL,
                as_of_date TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_score_percentile (
                ticker TEXT NOT NULL,
                target_date TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_quarter_state (
                ticker TEXT NOT NULL,
                market TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_statement_raw (
                ticker TEXT NOT NULL,
                field_name TEXT NOT NULL
            );
            """
        )
        conn.commit()


def _insert_complete_ticker(conn: sqlite3.Connection, ticker: str, market: str) -> None:
    conn.execute(
        "INSERT INTO rc_fundamental_quarterly (ticker, period_end_date) VALUES (?, ?)",
        (ticker, "2026-03-31"),
    )
    conn.execute(
        "INSERT INTO rc_fundamental_ttm (ticker, as_of_date) VALUES (?, ?)",
        (ticker, "2026-03-31"),
    )
    conn.execute(
        "INSERT INTO rc_fundamental_valuation (ticker, as_of_date) VALUES (?, ?)",
        (ticker, "2026-03-31"),
    )
    conn.execute(
        "INSERT INTO rc_fundamental_score_percentile (ticker, target_date) VALUES (?, ?)",
        (ticker, "2026-03-31"),
    )
    conn.execute(
        "INSERT INTO rc_fundamental_quarter_state (ticker, market) VALUES (?, ?)",
        (ticker, market),
    )
