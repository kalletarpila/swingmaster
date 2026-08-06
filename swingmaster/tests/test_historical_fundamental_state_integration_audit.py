from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from swingmaster.cli.audit_historical_fundamental_state_usage import main as audit_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.core.domain.enums import ReasonCode, State
from swingmaster.core.domain.models import StateAttrs
from swingmaster.core.engine.evaluator import evaluate_step
from swingmaster.core.policy.rule_policy_v3 import RuleBasedTransitionPolicyV3
from swingmaster.core.signals.enums import SignalKey
from swingmaster.core.signals.models import Signal, SignalSet
from swingmaster.fundamentals.earnings_events import repository_root
from swingmaster.fundamentals.historical_state_integration_audit import (
    STATE_FUNDAMENTAL_POLICY_RECOMMENDATION,
    STATE_READS_FUNDAMENTALS,
    audit_state_fundamental_usage,
    resolve_dates,
    resolve_tickers,
    state_dependency_map,
)


def test_state_core_dependency_map_has_no_fundamental_hard_input() -> None:
    rows = state_dependency_map()
    state_rows = [row for row in rows if row.classification == "STATE_CORE_NO_FUNDAMENTALS"]
    assert state_rows
    assert all(row.fundamental_fields_consumed == "none" for row in state_rows)
    assert all(row.state_decision_impact.startswith("hard") for row in state_rows[:3])
    assert STATE_READS_FUNDAMENTALS is False


def test_state_core_output_does_not_depend_on_fundamental_context() -> None:
    policy = RuleBasedTransitionPolicyV3()
    signals = SignalSet({SignalKey.TREND_STARTED: Signal(SignalKey.TREND_STARTED, True, None, "fixture")})
    before = evaluate_step(State.NO_TRADE, StateAttrs(confidence=None, age=0, status=None), signals, policy)
    after = evaluate_step(State.NO_TRADE, StateAttrs(confidence=None, age=0, status=None), signals, policy)
    assert before.final_state == after.final_state == State.DOWNTREND_EARLY
    assert before.reasons == after.reasons


def test_historical_current_vs_safe_context_difference_state_unchanged() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(state_db) as sconn, sqlite3.connect(price_db) as pconn:
        summary, rows = audit_state_fundamental_usage(
            fconn,
            sconn,
            pconn,
            tickers=["AAPL"],
            dates=["2026-04-29"],
            market="omxh",
        )
    row = rows[0]
    assert row["current_context_ttm_as_of_date"] == "2026-03-31"
    assert row["safe_context_ttm_as_of_date"] == "2025-12-31"
    assert row["fundamental_context_differs"] is True
    assert row["state_would_change_if_context_replaced"] is False
    assert summary.state_classification_difference_count == 0
    assert summary.reason_difference_count == 0
    assert summary.report_context_difference_count == 1


def test_missing_historical_snapshot_does_not_block_state() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(state_db) as sconn, sqlite3.connect(price_db) as pconn:
        summary, rows = audit_state_fundamental_usage(
            fconn,
            sconn,
            pconn,
            tickers=["DGXX"],
            dates=["2026-04-29"],
            market="omxh",
        )
    assert rows[0]["state"] == "ENTRY_WINDOW"
    assert rows[0]["safe_snapshot_status"] == "NO_AVAILABLE_TTM"
    assert rows[0]["state_would_change_if_context_replaced"] is False
    assert summary.rows_with_no_safe_snapshot == 1


def test_partial_snapshot_does_not_block_state_or_reason_codes() -> None:
    fundamentals_db, state_db, price_db = _build_fixture(include_miss_score=True)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(state_db) as sconn, sqlite3.connect(price_db) as pconn:
        summary, rows = audit_state_fundamental_usage(
            fconn,
            sconn,
            pconn,
            tickers=["MISS"],
            dates=["2026-04-30"],
            market="omxh",
        )
    assert rows[0]["state"] == "STABILIZING"
    assert rows[0]["reason_codes"] == "POLICY:STABILIZATION_CONFIRMED"
    assert rows[0]["safe_snapshot_status"] == "PARTIAL"
    assert rows[0]["state_would_change_if_context_replaced"] is False
    assert summary.rows_with_partial_safe_snapshot == 1


def test_no_future_fundamentals_exposed_before_effective_date() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(state_db) as sconn, sqlite3.connect(price_db) as pconn:
        _summary, rows = audit_state_fundamental_usage(
            fconn,
            sconn,
            pconn,
            tickers=["AAPL"],
            dates=["2026-04-29"],
            market="omxh",
        )
    assert rows[0]["safe_context_ttm_as_of_date"] == "2025-12-31"
    assert rows[0]["current_context_ttm_as_of_date"] == "2026-03-31"


def test_earnings_blackout_remains_separate_in_dependency_map() -> None:
    earnings_rows = [row for row in state_dependency_map() if "earnings" in row.module_file]
    assert earnings_rows
    assert earnings_rows[0].classification == "DIAGNOSTIC_ONLY"
    assert earnings_rows[0].state_decision_impact == "none"


def test_resolve_dates_and_tickers_are_deterministic() -> None:
    _fundamentals_db, state_db, _price_db = _build_fixture()
    with sqlite3.connect(state_db) as conn:
        dates = resolve_dates(conn, date=None, date_from="2026-04-29", date_to="2026-04-30")
        tickers = resolve_tickers(conn, tickers=[], tickers_file=None, dates=dates, first_n=2, sample_size=None, random_seed=3)
    assert dates == ["2026-04-29", "2026-04-30"]
    assert tickers == ["AAPL", "DGXX"]


def test_audit_cli_writes_only_temp_artifacts() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    output_root = _runtime_root() / uuid.uuid4().hex / "audit"
    assert audit_main(
        [
            "--fundamentals-db",
            str(fundamentals_db),
            "--state-db",
            str(state_db),
            "--price-db",
            str(price_db),
            "--ticker",
            "AAPL",
            "--date",
            "2026-04-29",
            "--market",
            "omxh",
            "--output-root",
            str(output_root),
            "--json",
        ]
    ) == 0
    summary = json.loads((output_root / "summary.json").read_text(encoding="utf-8"))
    assert summary["recommended_policy"] == STATE_FUNDAMENTAL_POLICY_RECOMMENDATION
    assert (output_root / "audit.json").exists()
    assert (output_root / "audit_rows.csv").exists()
    assert (output_root / "progress.log").exists()


def test_temp_only_artifact_guard() -> None:
    fundamentals_db, state_db, price_db = _build_fixture()
    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        audit_main(
            [
                "--fundamentals-db",
                str(fundamentals_db),
                "--state-db",
                str(state_db),
                "--price-db",
                str(price_db),
                "--ticker",
                "AAPL",
                "--date",
                "2026-04-29",
                "--market",
                "omxh",
                "--output-root",
                str(repository_root() / "bad-state-fundamental-audit"),
            ]
        )


def test_no_database_writes_and_vintage_provenance_unused() -> None:
    fundamentals_db, state_db, price_db = _build_fixture(include_vintage=True)
    before = _counts(fundamentals_db, state_db)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(state_db) as sconn, sqlite3.connect(price_db) as pconn:
        audit_state_fundamental_usage(
            fconn,
            sconn,
            pconn,
            tickers=["AAPL", "DGXX"],
            dates=["2026-04-29", "2026-04-30"],
            market="omxh",
        )
    assert before == _counts(fundamentals_db, state_db)


def _runtime_root() -> Path:
    return repository_root() / "temp" / "historical_fundamental_state_integration_audit" / "tests"


def _build_fixture(*, include_miss_score: bool = False, include_vintage: bool = False) -> tuple[Path, Path, Path]:
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
        _insert_state(conn, "AAPL", "2026-04-29", "ENTRY_WINDOW", ["POLICY:ENTRY_CONDITIONS_MET"])
        _insert_state(conn, "AAPL", "2026-04-30", "PASS", ["POLICY:ENTRY_WINDOW_COMPLETED"])
        _insert_state(conn, "DGXX", "2026-04-29", "ENTRY_WINDOW", ["POLICY:ENTRY_CONDITIONS_MET"])
        _insert_state(conn, "MISS", "2026-04-30", "STABILIZING", ["POLICY:STABILIZATION_CONFIRMED"])
    with sqlite3.connect(price_db) as conn:
        conn.execute("CREATE TABLE osakedata (id INTEGER PRIMARY KEY AUTOINCREMENT, osake TEXT, pvm TEXT, close REAL, market TEXT)")
        conn.execute("CREATE TABLE ticker_meta (ticker TEXT PRIMARY KEY, market TEXT, sector TEXT, industry TEXT)")
        for ticker in ["AAPL", "DGXX", "MISS"]:
            conn.execute("INSERT INTO ticker_meta(ticker, market, sector, industry) VALUES (?, 'omxh', 'Tech', 'Software')", (ticker,))
            conn.execute("INSERT INTO osakedata(osake, pvm, close, market) VALUES (?, '2026-04-29', 10.0, 'omxh')", (ticker,))
            conn.execute("INSERT INTO osakedata(osake, pvm, close, market) VALUES (?, '2026-04-30', 11.0, 'omxh')", (ticker,))
    with sqlite3.connect(fundamentals_db) as conn:
        _insert_ttm(conn, "AAPL", "2025-12-31", "2026-01-29", score=70.0, include_score=True)
        _insert_ttm(conn, "AAPL", "2026-03-31", "2026-04-30", score=80.0, include_score=True)
        if include_miss_score:
            _insert_ttm(conn, "MISS", "2026-03-31", "2026-04-30", score=80.0, include_score=False)
        if include_vintage:
            conn.execute("INSERT INTO rc_fundamental_quarterly_vintage(ticker, market, period_end_date, statement_vintage_id, source_provider, source_document_id, source_hash, revision_number, is_restated, availability_quality, available_at_utc, ingested_at_utc, run_id, created_at_utc) VALUES ('AAPL', 'omxh', '2025-12-31', 'v1', 'fixture', 'doc', 'hash', 1, 0, 'fixture', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'fixture', '2026-01-01T00:00:00Z')")
            conn.execute("INSERT INTO rc_fundamental_quarterly_field_provenance(ticker, market, period_end_date, statement_vintage_id, field_name, field_value, source_provider, source_table, provenance_role, merge_action, created_at_utc, run_id) VALUES ('AAPL', 'omxh', '2025-12-31', 'v1', 'revenue', 100, 'fixture', 'fixture', 'primary', 'insert', '2026-01-01T00:00:00Z', 'fixture')")
    return fundamentals_db, state_db, price_db


def _insert_state(conn: sqlite3.Connection, ticker: str, day: str, state: str, reasons: list[str]) -> None:
    conn.execute(
        """
        INSERT INTO rc_state_daily(ticker, date, state, reasons_json, confidence, age, state_attrs_json, run_id)
        VALUES (?, ?, ?, ?, NULL, 1, '{}', 'fixture')
        """,
        (ticker, day, state, json.dumps(reasons)),
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
            fcf_margin_ttm, fcf_margin_trend_4q, net_debt_to_ebit, share_dilution_yoy,
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


def _counts(fundamentals_db: Path, state_db: Path) -> dict[str, int]:
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(state_db) as sconn:
        return {
            "ttm": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0],
            "quarterly": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0],
            "valuation": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_valuation").fetchone()[0],
            "percentile": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_score_percentile").fetchone()[0],
            "vintage": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0],
            "provenance": fconn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0],
            "state": sconn.execute("SELECT COUNT(*) FROM rc_state_daily").fetchone()[0],
        }
