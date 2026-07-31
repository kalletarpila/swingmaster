from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from swingmaster.cli.audit_fundamentals_ticker_cleanup import main as audit_cli_main
from swingmaster.fundamentals.ticker_cleanup_audit import (
    audit_ticker_cleanup,
    classify_ticker,
    open_readonly_db,
    repository_root,
)


def test_keeps_delisted_company_with_usable_history() -> None:
    db = _build_db()
    _insert_quarter(db, "OLDCO", "2020-03-31", revenue=100, net_income=10)
    _insert_yahoo_raw(db, "OLDCO", {"quoteType": "EQUITY", "longName": "Oldco Delisted Corp"})

    with open_readonly_db(db) as conn:
        row = classify_ticker(conn, "OLDCO", _metadata(conn, "OLDCO"))

    assert row.classification == "KEEP_DELISTED_HISTORICAL_COMPANY"
    assert row.cleanup_scope == "EXCLUDE_FROM_ACTIVE_UNIVERSE"
    assert row.active_status is False
    assert row.delisted_status is True


def test_keeps_usable_quarterly_history() -> None:
    db = _build_db()
    _insert_quarter(db, "GOOD", "2024-03-31", revenue=100, net_income=7)

    with open_readonly_db(db) as conn:
        row = classify_ticker(conn, "GOOD", {})

    assert row.classification == "KEEP_USABLE_QUARTERLY_HISTORY"
    assert row.usable_quarterly_rows == 1


def test_empty_placeholder_rows_are_removal_candidates() -> None:
    db = _build_db()
    _insert_quarter(db, "EMPTY", "2024-03-31")

    with open_readonly_db(db) as conn:
        row = classify_ticker(conn, "EMPTY", {})

    assert row.classification == "REMOVE_EMPTY_PLACEHOLDER_ONLY"
    assert row.cleanup_scope == "DELETE_ONLY_EMPTY_FUNDAMENTAL_ROWS"


def test_metadata_driven_unsupported_instrument_candidates() -> None:
    db = _build_db()
    _insert_yahoo_raw(db, "ETF1", {"quoteType": "ETF", "longName": "Example ETF"})
    _insert_yahoo_raw(db, "IDX", {"quoteType": "INDEX", "longName": "Example Index"})
    _insert_yahoo_raw(db, "WRT", {"quoteType": "EQUITY", "longName": "Example Warrants"})
    _insert_yahoo_raw(db, "SPAC", {"quoteType": "EQUITY", "longName": "Example Acquisition Corp SPAC"})

    with open_readonly_db(db) as conn:
        assert classify_ticker(conn, "ETF1", _metadata(conn, "ETF1")).classification == "REMOVE_ETF_OR_FUND"
        assert classify_ticker(conn, "IDX", _metadata(conn, "IDX")).classification == "REMOVE_INDEX_OR_BENCHMARK"
        assert classify_ticker(conn, "WRT", _metadata(conn, "WRT")).classification == "REMOVE_WARRANT_RIGHT_UNIT_OR_PREFERRED"
        assert classify_ticker(conn, "SPAC", _metadata(conn, "SPAC")).classification == "REMOVE_UNSUPPORTED_INSTRUMENT"


def test_symbol_pattern_alone_requires_review() -> None:
    db = _build_db()

    with open_readonly_db(db) as conn:
        row = classify_ticker(conn, "ABC.W", {})

    assert row.classification == "UNKNOWN_SECURITY_TYPE_REVIEW"
    assert row.cleanup_scope == "MARK_INACTIVE"


def test_active_dependency_prevents_removal() -> None:
    db = _build_db()
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO rc_fundamental_ttm(ticker, as_of_date) VALUES (?, ?)", ("DEP", "2024-03-31"))

    with open_readonly_db(db) as conn:
        row = classify_ticker(conn, "DEP", {})

    assert row.classification == "KEEP_ACTIVE_DEPENDENCY"


def test_archive_only_dependency_without_unsupported_evidence_requires_review() -> None:
    db = _build_db()
    with sqlite3.connect(db) as conn:
        conn.execute("INSERT INTO rc_fundamental_quarterly_vintage(ticker, period_end_date) VALUES (?, ?)", ("ARCH", "2019-12-31"))

    with open_readonly_db(db) as conn:
        row = classify_ticker(conn, "ARCH", {})

    assert row.classification == "KEEP_MANUAL_REVIEW"
    assert row.cleanup_scope == "MARK_INACTIVE"
    assert row.archive_dependency_count == 1


def test_audit_is_deterministic_and_reports_projected_rows() -> None:
    db = _build_db()
    _insert_quarter(db, "GOOD", "2024-03-31", revenue=100, net_income=7)
    _insert_quarter(db, "EMPTY", "2024-03-31")

    first = audit_ticker_cleanup(db)
    second = audit_ticker_cleanup(db)

    assert first["all_tickers"] == second["all_tickers"]
    assert first["summary"]["category_counts"]["REMOVE_EMPTY_PLACEHOLDER_ONLY"] == 1
    assert first["summary"]["projected_rows_affected_by_table"]["rc_fundamental_quarterly"] == 1
    assert first["database_content_unchanged"] is True


def test_cli_writes_temp_artifacts_and_resumes_from_checkpoint() -> None:
    db = _build_db()
    _insert_quarter(db, "GOOD", "2024-03-31", revenue=100, net_income=7)
    _insert_quarter(db, "EMPTY", "2024-03-31")
    root = _runtime_root() / "cli"
    checkpoint = root / "checkpoint.json"

    assert audit_cli_main(["--fundamentals-db", str(db), "--output-root", str(root), "--checkpoint-json", str(checkpoint)]) == 0
    assert (root / "summary.json").exists()
    assert (root / "all_tickers.csv").exists()
    assert checkpoint.exists()

    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert payload["selected_tickers"] == ["EMPTY", "GOOD"]

    resumed_root = _runtime_root() / "cli-resumed"
    assert audit_cli_main(["--fundamentals-db", str(db), "--output-root", str(resumed_root), "--resume-from-json", str(checkpoint)]) == 0
    resumed = json.loads((resumed_root / "checkpoint.json").read_text(encoding="utf-8"))
    assert resumed["selected_tickers"] == ["EMPTY", "GOOD"]


def test_cli_rejects_runtime_paths_outside_temp() -> None:
    db = _build_db()
    bad_root = repository_root() / "outside-temp-audit"

    try:
        audit_cli_main(["--fundamentals-db", str(db), "--output-root", str(bad_root)])
    except ValueError as exc:
        assert "RUNTIME_PATH_OUTSIDE_TEMP" in str(exc)
    else:
        raise AssertionError("expected outside-temp path rejection")


def _build_db() -> Path:
    path = _runtime_root() / f"{uuid.uuid4().hex}.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT,
                period_end_date TEXT,
                revenue REAL,
                net_income REAL,
                operating_income REAL,
                ebit REAL,
                ebitda REAL,
                operating_cashflow REAL,
                free_cashflow REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL
            );
            CREATE TABLE rc_fundamental_ttm (ticker TEXT, as_of_date TEXT);
            CREATE TABLE rc_fundamental_score_percentile (ticker TEXT, sector TEXT, industry TEXT);
            CREATE TABLE rc_fundamental_valuation (ticker TEXT);
            CREATE TABLE rc_earnings_event (market TEXT, ticker TEXT, announcement_date TEXT);
            CREATE TABLE rc_fundamental_quarter_earnings_match (market TEXT, ticker TEXT, announcement_date TEXT);
            CREATE TABLE rc_fundamental_quarter_state (ticker TEXT, market TEXT);
            CREATE TABLE rc_fundamental_quarterly_enrichment_audit (ticker TEXT, field_name TEXT);
            CREATE TABLE rc_fundamental_statement_raw (ticker TEXT, field_name TEXT);
            CREATE TABLE rc_fundamental_yahoo_raw (
                market TEXT,
                symbol TEXT,
                info_json TEXT,
                fast_info_json TEXT,
                status TEXT,
                loaded_at_utc TEXT
            );
            CREATE TABLE rc_fundamental_yahoo_quarterly (market TEXT, symbol TEXT);
            CREATE TABLE rc_fundamental_quarterly_vintage (ticker TEXT, period_end_date TEXT);
            CREATE TABLE rc_fundamental_quarterly_field_provenance (ticker TEXT);
            """
        )
    return path


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamentals_ticker_cleanup_audit" / "tests"


def _insert_quarter(db: Path, ticker: str, period: str, **values: float) -> None:
    columns = ["ticker", "period_end_date", *values.keys()]
    placeholders = ", ".join("?" for _ in columns)
    with sqlite3.connect(db) as conn:
        conn.execute(
            f"INSERT INTO rc_fundamental_quarterly({', '.join(columns)}) VALUES ({placeholders})",
            [ticker, period, *values.values()],
        )


def _insert_yahoo_raw(db: Path, ticker: str, info: dict[str, object]) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_raw(market, symbol, info_json, fast_info_json, status, loaded_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("usa", ticker, json.dumps(info), "{}", "ok", "2026-07-31T00:00:00Z"),
        )


def _metadata(conn: sqlite3.Connection, ticker: str) -> dict[str, object]:
    row = conn.execute("SELECT info_json, market, status FROM rc_fundamental_yahoo_raw WHERE symbol = ?", (ticker,)).fetchone()
    data = json.loads(row["info_json"])
    data["market"] = row["market"]
    data["source_status"] = row["status"]
    return data
