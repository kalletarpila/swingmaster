from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from swingmaster.cli.audit_historical_fundamental_valuation import main as audit_main
from swingmaster.cli.inspect_historical_fundamental_valuation import main as inspect_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_valuation import build_valuation_row, load_quarterly_ev_inputs, load_ttm_rows
from swingmaster.fundamentals.earnings_events import repository_root
from swingmaster.fundamentals.historical_valuation import (
    HISTORICAL_VALUATION_POLICY,
    PRICE_STATUS_EXACT,
    PRICE_STATUS_NO_PRICE,
    PRICE_STATUS_PREVIOUS,
    STATUS_INVALID_DENOMINATOR,
    STATUS_MISSING_REQUIRED_INPUT,
    STATUS_NO_AVAILABLE_TTM,
    STATUS_NO_PRICE_AVAILABLE,
    STATUS_OK,
    audit_historical_valuation,
    calculate_historical_valuation_as_of,
    select_historical_close_as_of,
)


def test_exact_and_previous_historical_close_selection_and_future_price_excluded() -> None:
    _fundamentals_db, price_db = _build_fixture()
    _insert_close(price_db, "AAPL", "2026-04-26", 99.0)
    _insert_close(price_db, "AAPL", "2026-04-29", 100.0)
    _insert_close(price_db, "AAPL", "2026-05-01", 110.0)

    with sqlite3.connect(price_db) as conn:
        exact = select_historical_close_as_of(conn, ticker="AAPL", market="usa", as_of_date="2026-04-29")
        previous = select_historical_close_as_of(conn, ticker="AAPL", market="usa", as_of_date="2026-04-30")

    assert exact.price_selection_status == PRICE_STATUS_EXACT
    assert exact.selected_price_date == "2026-04-29"
    assert exact.close_price == 100.0
    assert previous.price_selection_status == PRICE_STATUS_PREVIOUS
    assert previous.selected_price_date == "2026-04-29"
    assert previous.close_price == 100.0


def test_no_historical_price_status() -> None:
    _fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(price_db) as conn:
        selected = select_historical_close_as_of(conn, ticker="AAPL", market="usa", as_of_date="2026-04-29")
    assert selected.price_selection_status == PRICE_STATUS_NO_PRICE
    assert selected.close_price is None


def test_latest_available_ttm_switches_only_on_effective_date() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", ebit=10.0, fcf=7.0, margin=0.10, shares=10.0)
        _insert_ttm(conn, "AAPL", "2026-03-31", "2026-04-30", ebit=20.0, fcf=10.0, margin=0.20, shares=10.0)
    _insert_close(price_db, "AAPL", "2026-04-29", 10.0)
    _insert_close(price_db, "AAPL", "2026-04-30", 10.0)

    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        before = calculate_historical_valuation_as_of(fconn, pconn, ticker="AAPL", as_of_date="2026-04-29", market="usa")
        on_date = calculate_historical_valuation_as_of(fconn, pconn, ticker="AAPL", as_of_date="2026-04-30", market="usa")

    assert before.source_ttm_as_of_date == "2025-12-31"
    assert on_date.source_ttm_as_of_date == "2026-03-31"
    assert before.valuation_row is not None and before.valuation_row["valuation_ev_ebit"] == pytest.approx(10.0)
    assert on_date.valuation_row is not None and on_date.valuation_row["valuation_ev_ebit"] == pytest.approx(5.0)


def test_no_available_ttm_does_not_fall_back_to_future_effective_fundamentals() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2026-03-31", "2026-04-30", ebit=20.0, fcf=10.0, margin=0.20, shares=10.0)
    _insert_close(price_db, "AAPL", "2026-04-29", 10.0)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        result = calculate_historical_valuation_as_of(fconn, pconn, ticker="AAPL", as_of_date="2026-04-29", market="usa")
    assert result.valuation_status == STATUS_NO_AVAILABLE_TTM
    assert result.valuation_row is None


def test_existing_valuation_formula_reused_for_historical_row() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", ebit=10.0, fcf=7.0, margin=0.10, shares=10.0)
    _insert_close(price_db, "AAPL", "2026-02-01", 10.0)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        result = calculate_historical_valuation_as_of(fconn, pconn, ticker="AAPL", as_of_date="2026-02-01", market="usa")
        ttm_row = load_ttm_rows(fconn, "2026-02-01", "AAPL", None)[0]
        quarterly = load_quarterly_ev_inputs(fconn, "AAPL", "2025-12-31")
        expected = build_valuation_row("2026-02-01", ttm_row, quarterly, 10.0, "RUN", "2026-01-01T00:00:00Z")
    assert result.valuation_row is not None
    assert result.valuation_row["valuation_ev_ebit"] == expected["valuation_ev_ebit"]
    assert result.valuation_row["valuation_fcf_yield"] == expected["valuation_fcf_yield"]
    assert result.valuation_policy == HISTORICAL_VALUATION_POLICY


def test_historical_shares_cash_and_debt_from_selected_period() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", ebit=10.0, fcf=7.0, margin=0.10, shares=10.0, cash=0.0, debt=0.0)
        _insert_ttm(conn, "AAPL", "2026-03-31", "2026-04-30", ebit=10.0, fcf=7.0, margin=0.10, shares=1000.0, cash=999.0, debt=999.0)
    _insert_close(price_db, "AAPL", "2026-04-29", 10.0)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        result = calculate_historical_valuation_as_of(fconn, pconn, ticker="AAPL", as_of_date="2026-04-29", market="usa")
    assert result.valuation_row is not None
    assert result.valuation_row["shares_outstanding"] == 10.0
    assert result.valuation_row["market_cap"] == 100.0


def test_missing_required_input_and_invalid_denominator_statuses() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "MISS", "2025-12-31", "2026-01-29", ebit=10.0, fcf=None, margin=0.10, shares=10.0)
        _insert_ttm(conn, "BAD", "2025-12-31", "2026-01-29", ebit=0.0, fcf=7.0, margin=0.10, shares=10.0)
    _insert_close(price_db, "MISS", "2026-02-01", 10.0)
    _insert_close(price_db, "BAD", "2026-02-01", 10.0)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        missing = calculate_historical_valuation_as_of(fconn, pconn, ticker="MISS", as_of_date="2026-02-01", market="usa")
        invalid = calculate_historical_valuation_as_of(fconn, pconn, ticker="BAD", as_of_date="2026-02-01", market="usa")
    assert missing.valuation_status == STATUS_MISSING_REQUIRED_INPUT
    assert missing.missing_input_fields == ["fcf_ttm"]
    assert invalid.valuation_status == STATUS_INVALID_DENOMINATOR
    assert invalid.missing_input_fields == ["ebit_ttm"]


def test_no_price_available_has_explicit_status_after_ttm_selected() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", ebit=10.0, fcf=7.0, margin=0.10, shares=10.0)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        result = calculate_historical_valuation_as_of(fconn, pconn, ticker="AAPL", as_of_date="2026-02-01", market="usa")
    assert result.valuation_status == STATUS_NO_PRICE_AVAILABLE
    assert result.missing_input_fields == ["close_price"]
    assert result.valuation_row is not None
    assert result.valuation_row["valuation_status"] == "MISSING_PRICE"


def test_current_selector_and_current_valuation_path_unchanged() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", ebit=10.0, fcf=7.0, margin=0.10, shares=10.0)
        _insert_ttm(conn, "AAPL", "2026-03-31", "2026-04-30", ebit=20.0, fcf=10.0, margin=0.20, shares=10.0)
    _insert_close(price_db, "AAPL", "2026-04-29", 10.0)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        result = calculate_historical_valuation_as_of(
            fconn, pconn, ticker="AAPL", as_of_date="2026-04-29", market="usa", include_current_comparison=True
        )
    assert result.source_ttm_as_of_date == "2025-12-31"
    assert result.current_comparison_row is not None
    assert result.current_comparison_row["valuation_fundamental_as_of_date"] == "2026-03-31"


def test_query_time_no_writes_and_vintage_provenance_unused() -> None:
    fundamentals_db, price_db = _build_fixture(include_vintage=True)
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", ebit=10.0, fcf=7.0, margin=0.10, shares=10.0)
    _insert_close(price_db, "AAPL", "2026-02-01", 10.0)
    before = _counts(fundamentals_db)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        calculate_historical_valuation_as_of(fconn, pconn, ticker="AAPL", as_of_date="2026-02-01", market="usa")
        audit_historical_valuation(fconn, pconn, tickers=["AAPL"], as_of_date="2026-02-01", market="usa")
    assert before == _counts(fundamentals_db)


def test_inspect_cli_json_and_audit_cli_temp_artifacts() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", ebit=10.0, fcf=7.0, margin=0.10, shares=10.0)
    _insert_close(price_db, "AAPL", "2026-02-01", 10.0)
    output_root = _runtime_root() / uuid.uuid4().hex / "audit"
    assert inspect_main(
        [
            "--fundamentals-db",
            str(fundamentals_db),
            "--price-db",
            str(price_db),
            "--ticker",
            "AAPL",
            "--as-of-date",
            "2026-02-01",
            "--json",
        ]
    ) == 0
    assert audit_main(
        [
            "--fundamentals-db",
            str(fundamentals_db),
            "--price-db",
            str(price_db),
            "--ticker",
            "AAPL",
            "--as-of-date",
            "2026-02-01",
            "--output-root",
            str(output_root),
            "--json",
        ]
    ) == 0
    assert json.loads((output_root / "summary.json").read_text(encoding="utf-8"))["ticker_date_rows_evaluated"] == 1
    assert (output_root / "audit_rows.csv").exists()


def test_temp_only_artifact_guard() -> None:
    fundamentals_db, price_db = _build_fixture()
    try:
        audit_main(
            [
                "--fundamentals-db",
                str(fundamentals_db),
                "--price-db",
                str(price_db),
                "--ticker",
                "AAPL",
                "--as-of-date",
                "2026-02-01",
                "--output-root",
                str(repository_root() / "bad-historical-valuation"),
            ]
        )
    except ValueError as exc:
        assert "RUNTIME_PATH_OUTSIDE_TEMP" in str(exc)
    else:
        raise AssertionError("expected temp-only path guard")


def test_currency_policy_is_existing_no_fx_and_percentiles_deferred_by_default() -> None:
    fundamentals_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", ebit=10.0, fcf=7.0, margin=0.10, shares=10.0, currency="EUR")
    _insert_close(price_db, "AAPL", "2026-02-01", 10.0)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        result = calculate_historical_valuation_as_of(fconn, pconn, ticker="AAPL", as_of_date="2026-02-01", market="usa")
    assert result.valuation_status == STATUS_OK
    assert result.historical_percentile is None


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamental_historical_valuation" / "tests"


def _build_fixture(*, include_vintage: bool = False) -> tuple[Path, Path]:
    root = _runtime_root()
    root.mkdir(parents=True, exist_ok=True)
    fundamentals_db = root / f"{uuid.uuid4().hex}.fundamentals.db"
    price_db = root / f"{uuid.uuid4().hex}.price.db"
    run_migration(fundamentals_db)
    with sqlite3.connect(price_db) as conn:
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
        conn.execute(
            """
            CREATE TABLE ticker_meta (
                ticker TEXT PRIMARY KEY,
                market TEXT,
                sector TEXT,
                industry TEXT
            )
            """
        )
    if include_vintage:
        with sqlite3.connect(fundamentals_db) as conn:
            conn.execute("INSERT INTO rc_fundamental_quarterly_vintage(ticker, market, period_end_date, statement_vintage_id, source_provider, source_document_id, source_hash, revision_number, is_restated, availability_quality, available_at_utc, ingested_at_utc, run_id, created_at_utc) VALUES ('AAPL', 'usa', '2025-12-31', 'v1', 'fixture', 'doc', 'hash', 1, 0, 'fixture', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'fixture', '2026-01-01T00:00:00Z')")
            conn.execute("INSERT INTO rc_fundamental_quarterly_field_provenance(ticker, market, period_end_date, statement_vintage_id, field_name, field_value, source_provider, source_table, provenance_role, merge_action, created_at_utc, run_id) VALUES ('AAPL', 'usa', '2025-12-31', 'v1', 'revenue', 100, 'fixture', 'fixture', 'primary', 'insert', '2026-01-01T00:00:00Z', 'fixture')")
    return fundamentals_db, price_db


def _insert_close(db_path: Path, ticker: str, pvm: str, close: float, market: str = "usa") -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO osakedata(osake, pvm, close, market) VALUES (?, ?, ?, ?)", (ticker, pvm, close, market))
        conn.execute("INSERT OR IGNORE INTO ticker_meta(ticker, market, sector, industry) VALUES (?, ?, 'Tech', 'Software')", (ticker, market))


def _insert_ttm(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    effective_date: str,
    *,
    ebit: float | None,
    fcf: float | None,
    margin: float | None,
    shares: float | None,
    cash: float | None = 0.0,
    debt: float | None = 0.0,
    currency: str = "USD",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_ttm(
            ticker, as_of_date, latest_period_end_date, ebit_ttm, fcf_ttm,
            ebit_margin_ttm, fundamental_score_lifecycle,
            effective_trading_date, effective_date_status, effective_date_policy, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, 50.0, ?, 'RESOLVED', 'MAX_COMPONENT_QUARTER_EFFECTIVE_DATE', 'fixture')
        """,
        (ticker, as_of_date, as_of_date, ebit, fcf, margin, effective_date),
    )
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly(
            ticker, period_end_date, cash, total_debt, shares_outstanding, currency, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, 'fixture')
        """,
        (ticker, as_of_date, cash, debt, shares, currency),
    )


def _counts(db: Path) -> dict[str, int]:
    with sqlite3.connect(db) as conn:
        return {
            "ttm": conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0],
            "valuation": conn.execute("SELECT COUNT(*) FROM rc_fundamental_valuation").fetchone()[0],
            "vintage": conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0],
            "provenance": conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0],
        }
