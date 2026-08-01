from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from swingmaster.cli.inspect_historical_state import main as inspect_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import repository_root
from swingmaster.research.historical_fundamental_context import (
    HISTORICAL_STATE_FUNDAMENTAL_POLICY,
    STATUS_NO_AVAILABLE_PERCENTILE,
    STATUS_NO_AVAILABLE_TTM,
    STATUS_PARTIAL,
    STATUS_VALUATION_UNAVAILABLE,
    HistoricalStateFundamentalContextCache,
    audit_historical_state_fundamental_context,
    build_historical_state_fundamental_context,
    build_historical_state_rows,
    enrich_historical_state_rows,
)


def test_enrichment_disabled_by_default_and_output_has_no_context(capsys: pytest.CaptureFixture[str]) -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    assert inspect_main(["--state-db", str(state_db), "--ticker", "AAPL", "--date", "2026-04-29", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert "fundamental_context" not in payload["rows"][0]
    assert payload["rows"][0]["state"] == "ENTRY_WINDOW"
    assert price_db.exists()
    assert fundamentals_db.exists()


def test_disabled_output_is_unchanged_by_unused_fundamentals_args(capsys: pytest.CaptureFixture[str]) -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    args = ["--state-db", str(state_db), "--ticker", "AAPL", "--date", "2026-04-29", "--json"]
    assert inspect_main(args) == 0
    baseline = json.loads(capsys.readouterr().out)
    assert inspect_main(args + ["--fundamentals-db", str(fundamentals_db), "--price-db", str(price_db)]) == 0
    assert json.loads(capsys.readouterr().out) == baseline


def test_context_uses_state_signal_date_and_compact_contract() -> None:
    fundamentals_db, _state_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        context = build_historical_state_fundamental_context(
            fconn,
            pconn,
            ticker="AAPL",
            signal_date="2026-04-29",
            market="omxh",
            include_percentile=False,
            include_valuation=True,
        )
    assert context.fundamental_policy == HISTORICAL_STATE_FUNDAMENTAL_POLICY
    assert context.fundamental_requested_as_of_date == "2026-04-29"
    assert context.fundamental_source_ttm_as_of_date == "2025-12-31"
    assert context.fundamental_effective_trading_date == "2026-01-29"
    assert context.fundamental_score == pytest.approx(70.0)
    assert context.historical_close_price == pytest.approx(10.0)
    assert context.historical_ev_ebit == pytest.approx(4.85)
    assert context.historical_fcf_yield == pytest.approx(0.1)


def test_partial_context_when_score_is_unavailable() -> None:
    fundamentals_db, _state_db, price_db = _build_fixture(include_partial=True)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        context = build_historical_state_fundamental_context(
            fconn,
            pconn,
            ticker="MISS",
            signal_date="2026-04-30",
            market="omxh",
            include_percentile=False,
            include_valuation=False,
        )
    assert context.fundamental_context_status == STATUS_PARTIAL
    assert "MISSING_SCORE" in context.fundamental_warnings


def test_no_available_ttm_state_row_remains_valid() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    with sqlite3.connect(state_db) as sconn, sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        rows = build_historical_state_rows(sconn, tickers=["DGXX"], dates=["2026-04-29"])
        enriched, _timings, _cache = enrich_historical_state_rows(
            rows,
            fconn,
            pconn,
            market="omxh",
            include_percentile=False,
            include_valuation=True,
        )
    assert enriched[0]["state"] == "ENTRY_WINDOW"
    assert enriched[0]["fundamental_context"]["fundamental_context_status"] == STATUS_NO_AVAILABLE_TTM


def test_percentile_unavailable_is_non_blocking() -> None:
    fundamentals_db, _state_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        context = build_historical_state_fundamental_context(
            fconn,
            pconn,
            ticker="AAPL",
            signal_date="2026-04-29",
            market="omxh",
            include_percentile=True,
            include_valuation=False,
        )
    assert context.percentile_status == STATUS_NO_AVAILABLE_PERCENTILE
    assert context.fundamental_context_status == STATUS_PARTIAL
    assert context.fundamental_score == pytest.approx(70.0)


def test_valuation_unavailable_when_price_missing() -> None:
    fundamentals_db, _state_db, price_db = _build_fixture(skip_price_for=["NOPRICE"])
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "NOPRICE", "2025-12-31", "2026-01-29", score=65.0, include_score=True)
    with sqlite3.connect(price_db) as conn:
        conn.execute("INSERT INTO ticker_meta(ticker, market, sector, industry) VALUES ('NOPRICE', 'omxh', 'Tech', 'Software')")
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        context = build_historical_state_fundamental_context(
            fconn,
            pconn,
            ticker="NOPRICE",
            signal_date="2026-04-29",
            market="omxh",
            include_percentile=False,
            include_valuation=True,
        )
    assert context.valuation_status == STATUS_VALUATION_UNAVAILABLE
    assert context.historical_close_price is None


def test_no_future_or_current_fallback() -> None:
    fundamentals_db, _state_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        context = build_historical_state_fundamental_context(
            fconn,
            pconn,
            ticker="AAPL",
            signal_date="2026-04-29",
            market="omxh",
            include_percentile=False,
            include_valuation=False,
        )
    assert context.fundamental_source_ttm_as_of_date == "2025-12-31"
    assert context.fundamental_score == pytest.approx(70.0)


def test_state_reason_transition_signals_and_attrs_are_unchanged() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    with sqlite3.connect(state_db) as sconn, sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        sconn.row_factory = sqlite3.Row
        fconn.row_factory = sqlite3.Row
        pconn.row_factory = sqlite3.Row
        rows = build_historical_state_rows(sconn, tickers=["AAPL"], dates=["2026-04-29"])
        enriched, _timings, _cache = enrich_historical_state_rows(
            rows,
            fconn,
            pconn,
            market="omxh",
            include_percentile=False,
            include_valuation=False,
        )
    for key in ("state", "previous_state", "reason_codes", "transition", "signal_values", "state_attrs"):
        assert enriched[0][key] == rows[0][key]


def test_optional_cli_json_context(capsys: pytest.CaptureFixture[str]) -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    assert inspect_main(
        [
            "--state-db",
            str(state_db),
            "--fundamentals-db",
            str(fundamentals_db),
            "--price-db",
            str(price_db),
            "--ticker",
            "AAPL",
            "--date",
            "2026-04-29",
            "--include-historical-fundamentals",
            "--no-percentile",
            "--json",
        ]
    ) == 0
    row = json.loads(capsys.readouterr().out)["rows"][0]
    assert row["state"] == "ENTRY_WINDOW"
    assert row["fundamental_context"]["fundamental_source_ttm_as_of_date"] == "2025-12-31"


def test_bounded_cache_behavior() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    cache = HistoricalStateFundamentalContextCache(max_entries=4)
    with sqlite3.connect(state_db) as sconn, sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        sconn.row_factory = sqlite3.Row
        fconn.row_factory = sqlite3.Row
        pconn.row_factory = sqlite3.Row
        rows = build_historical_state_rows(sconn, tickers=["AAPL", "AAPL"], dates=["2026-04-29"])
        enrich_historical_state_rows(rows, fconn, pconn, market="omxh", include_percentile=False, include_valuation=False, cache=cache)
    assert cache.miss_count == 1
    assert cache.hit_count == 1


def test_audit_outputs_are_temp_only(capsys: pytest.CaptureFixture[str]) -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    output_root = _runtime_root() / uuid.uuid4().hex / "audit"
    assert inspect_main(
        [
            "--state-db",
            str(state_db),
            "--fundamentals-db",
            str(fundamentals_db),
            "--price-db",
            str(price_db),
            "--ticker",
            "AAPL",
            "--date",
            "2026-04-29",
            "--include-historical-fundamentals",
            "--no-percentile",
            "--audit-output-root",
            str(output_root),
            "--json",
        ]
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["state_difference_count"] == 0
    assert (output_root / "audit.json").exists()
    assert (output_root / "audit_rows.csv").exists()
    assert (output_root / "summary.json").exists()
    assert (output_root / "progress.log").exists()


def test_audit_rejects_non_temp_output_path() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        inspect_main(
            [
                "--state-db",
                str(state_db),
                "--fundamentals-db",
                str(fundamentals_db),
                "--price-db",
                str(price_db),
                "--ticker",
                "AAPL",
                "--date",
                "2026-04-29",
                "--include-historical-fundamentals",
                "--audit-output-root",
                str(repository_root() / "bad-context-output"),
            ]
        )


def test_audit_summary_counts_and_no_database_writes() -> None:
    fundamentals_db, state_db, price_db = _build_fixture(include_partial=True)
    before = _counts(fundamentals_db, state_db, price_db)
    with sqlite3.connect(state_db) as sconn, sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        sconn.row_factory = sqlite3.Row
        fconn.row_factory = sqlite3.Row
        pconn.row_factory = sqlite3.Row
        summary, _rows = audit_historical_state_fundamental_context(
            sconn,
            fconn,
            pconn,
            tickers=["AAPL", "DGXX", "MISS"],
            dates=["2026-04-29"],
            market="omxh",
            include_percentile=False,
            include_valuation=True,
        )
    assert summary.rows_evaluated == 3
    assert summary.state_difference_count == 0
    assert summary.reason_difference_count == 0
    assert summary.transition_difference_count == 0
    assert summary.no_available_ttm_count == 1
    assert _counts(fundamentals_db, state_db, price_db) == before


def test_vintage_and_provenance_are_unused() -> None:
    fundamentals_db, _state_db, price_db = _build_fixture(include_vintage=True)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        context = build_historical_state_fundamental_context(
            fconn,
            pconn,
            ticker="AAPL",
            signal_date="2026-04-29",
            market="omxh",
            include_percentile=False,
            include_valuation=False,
        )
    assert context.fundamental_source_ttm_as_of_date == "2025-12-31"


def _runtime_root() -> Path:
    return repository_root() / "temp" / "historical_state_fundamental_context" / "tests"


def _build_fixture(
    *,
    include_partial: bool = False,
    include_vintage: bool = False,
    skip_price_for: list[str] | None = None,
) -> tuple[Path, Path, Path]:
    skip_price = set(skip_price_for or [])
    root = _runtime_root()
    root.mkdir(parents=True, exist_ok=True)
    fundamentals_db = root / f"{uuid.uuid4().hex}.fundamentals.db"
    state_db = root / f"{uuid.uuid4().hex}.state.db"
    price_db = root / f"{uuid.uuid4().hex}.price.db"
    run_migration(fundamentals_db)
    with sqlite3.connect(state_db) as conn:
        conn.execute(
            """
            CREATE TABLE rc_state_daily (
                ticker TEXT,
                date TEXT,
                state TEXT,
                reasons_json TEXT,
                confidence INTEGER,
                age INTEGER,
                state_attrs_json TEXT,
                run_id TEXT,
                PRIMARY KEY (ticker, date)
            )
            """
        )
        conn.execute("CREATE TABLE rc_transition (ticker TEXT, date TEXT, from_state TEXT, to_state TEXT, reasons_json TEXT, run_id TEXT)")
        conn.execute("CREATE TABLE rc_signal_daily (ticker TEXT, date TEXT, signal_keys_json TEXT, run_id TEXT, PRIMARY KEY (ticker, date))")
        _insert_state(conn, "AAPL", "2026-04-28", "STABILIZING", ["POLICY:STABILIZATION_CONFIRMED"], {"phase": "base"})
        _insert_state(conn, "AAPL", "2026-04-29", "ENTRY_WINDOW", ["POLICY:ENTRY_CONDITIONS_MET"], {"entry_gate": "ready"})
        _insert_state(conn, "DGXX", "2026-04-29", "ENTRY_WINDOW", ["POLICY:ENTRY_CONDITIONS_MET"], {})
        _insert_state(conn, "MISS", "2026-04-29", "STABILIZING", ["POLICY:STABILIZATION_CONFIRMED"], {})
        conn.execute(
            "INSERT INTO rc_transition(ticker, date, from_state, to_state, reasons_json, run_id) VALUES ('AAPL', '2026-04-29', 'STABILIZING', 'ENTRY_WINDOW', ?, 'fixture')",
            (json.dumps(["POLICY:ENTRY_CONDITIONS_MET"]),),
        )
        conn.execute(
            "INSERT INTO rc_signal_daily(ticker, date, signal_keys_json, run_id) VALUES ('AAPL', '2026-04-29', ?, 'fixture')",
            (json.dumps(["ENTRY_SETUP_VALID"]),),
        )
    with sqlite3.connect(price_db) as conn:
        conn.execute("CREATE TABLE osakedata (id INTEGER PRIMARY KEY AUTOINCREMENT, osake TEXT, pvm TEXT, close REAL, market TEXT)")
        conn.execute("CREATE TABLE ticker_meta (ticker TEXT PRIMARY KEY, market TEXT, sector TEXT, industry TEXT)")
        for ticker in ["AAPL", "DGXX", "MISS"]:
            conn.execute("INSERT INTO ticker_meta(ticker, market, sector, industry) VALUES (?, 'omxh', 'Tech', 'Software')", (ticker,))
            if ticker not in skip_price:
                conn.execute("INSERT INTO osakedata(osake, pvm, close, market) VALUES (?, '2026-04-29', 10.0, 'omxh')", (ticker,))
                conn.execute("INSERT INTO osakedata(osake, pvm, close, market) VALUES (?, '2026-04-30', 11.0, 'omxh')", (ticker,))
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", score=70.0, include_score=True)
        _insert_ttm(conn, "AAPL", "2026-03-31", "2026-04-30", score=80.0, include_score=True)
        if include_partial:
            _insert_ttm(conn, "MISS", "2025-12-31", "2026-01-29", score=60.0, include_score=False)
        if include_vintage:
            conn.execute("INSERT INTO rc_fundamental_quarterly_vintage(ticker, market, period_end_date, statement_vintage_id, source_provider, source_document_id, source_hash, revision_number, is_restated, availability_quality, available_at_utc, ingested_at_utc, run_id, created_at_utc) VALUES ('AAPL', 'omxh', '2025-12-31', 'v1', 'fixture', 'doc', 'hash', 1, 0, 'fixture', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'fixture', '2026-01-01T00:00:00Z')")
            conn.execute("INSERT INTO rc_fundamental_quarterly_field_provenance(ticker, market, period_end_date, statement_vintage_id, field_name, field_value, source_provider, source_table, provenance_role, merge_action, created_at_utc, run_id) VALUES ('AAPL', 'omxh', '2025-12-31', 'v1', 'revenue', 100, 'fixture', 'fixture', 'primary', 'insert', '2026-01-01T00:00:00Z', 'fixture')")
    return fundamentals_db, state_db, price_db


def _insert_state(conn: sqlite3.Connection, ticker: str, day: str, state: str, reasons: list[str], attrs: dict[str, str]) -> None:
    conn.execute(
        """
        INSERT INTO rc_state_daily(ticker, date, state, reasons_json, confidence, age, state_attrs_json, run_id)
        VALUES (?, ?, ?, ?, NULL, 1, ?, 'fixture')
        """,
        (ticker, day, state, json.dumps(reasons), json.dumps(attrs)),
    )


def _insert_ttm(conn: sqlite3.Connection, ticker: str, as_of_date: str, effective_date: str, *, score: float, include_score: bool) -> None:
    score_value = score if include_score else None
    component = 10.0 if include_score else None
    rule = "fixture" if include_score else None
    conn.execute(
        """
        INSERT INTO rc_fundamental_ttm(
            ticker, as_of_date, latest_period_end_date, lifecycle_class,
            fundamental_score, fundamental_score_lifecycle,
            growth_component, margin_component, margin_trend_component, fcf_component,
            consistency_component, consistency_component_lifecycle, leverage_component, dilution_component,
            revenue_growth_ttm_yoy, ebit_margin_ttm, ebit_margin_trend_4q,
            fcf_margin_ttm, fcf_margin_trend_4q, net_debt_to_ebitda, share_dilution_yoy,
            ebit_ttm, fcf_ttm,
            effective_trading_date, effective_date_status, effective_date_policy,
            score_effective_trading_date, score_effective_date_status, score_effective_date_policy,
            score_rule_lifecycle, run_id
        ) VALUES (?, ?, ?, 'MATURE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  0.10, 0.20, 0.01, 0.12, 0.01, 1.0, 0.0, 20.0, 10.0,
                  ?, 'RESOLVED', 'MAX_COMPONENT_QUARTER_EFFECTIVE_DATE',
                  ?, 'RESOLVED', 'SOURCE_TTM_EFFECTIVE_DATE', ?, 'fixture')
        """,
        (
            ticker,
            as_of_date,
            as_of_date,
            score_value,
            score_value,
            component,
            component,
            component,
            component,
            component,
            component,
            component,
            component,
            effective_date,
            effective_date,
            rule,
        ),
    )
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly(
            ticker, period_end_date, revenue, operating_income, free_cashflow,
            cash, total_debt, shares_outstanding, currency, run_id
        ) VALUES (?, ?, 100.0, 20.0, 10.0, 5.0, 2.0, 10.0, 'EUR', 'fixture')
        """,
        (ticker, as_of_date),
    )


def _counts(fundamentals_db: Path, state_db: Path, price_db: Path) -> dict[str, int]:
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(state_db) as sconn, sqlite3.connect(price_db) as pconn:
        return {
            "ttm": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0],
            "quarterly": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0],
            "vintage": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0],
            "provenance": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0],
            "state": sconn.execute("SELECT COUNT(*) FROM rc_state_daily").fetchone()[0],
            "transition": sconn.execute("SELECT COUNT(*) FROM rc_transition").fetchone()[0],
            "signal": sconn.execute("SELECT COUNT(*) FROM rc_signal_daily").fetchone()[0],
            "price": pconn.execute("SELECT COUNT(*) FROM osakedata").fetchone()[0],
        }
