from __future__ import annotations

import json
import sqlite3
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import pytest

from swingmaster.cli.audit_historical_fundamental_snapshot import main as audit_main
from swingmaster.cli.inspect_historical_fundamental_snapshot import main as inspect_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_ticker_snapshot import build_snapshot_matrix
from swingmaster.fundamentals.earnings_events import repository_root
from swingmaster.fundamentals.historical_snapshot import (
    HISTORICAL_SNAPSHOT_POLICY,
    STATUS_NO_AVAILABLE_TTM,
    STATUS_OK,
    STATUS_PARTIAL,
    audit_historical_fundamental_snapshots,
    build_historical_fundamental_snapshot,
)


def test_basic_historical_snapshot_with_percentile_and_valuation() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        snapshot = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=True,
            include_valuation=True,
        )
    assert snapshot.snapshot_status == STATUS_OK
    assert snapshot.fundamentals_policy == HISTORICAL_SNAPSHOT_POLICY
    assert snapshot.source_ttm_as_of_date == "2026-03-31"
    assert snapshot.source_ttm_effective_trading_date == "2026-04-30"
    assert snapshot.source_score_effective_trading_date == "2026-04-30"
    assert snapshot.selected_price_date == "2026-04-30"
    assert snapshot.score_percentile_population_size is not None and snapshot.score_percentile_population_size >= 50
    assert snapshot.percentiles is not None
    assert snapshot.valuation is not None


def test_historical_ttm_and_score_switch_only_on_effective_date() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        before = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-29",
            market="omxh",
            include_percentiles=False,
            include_valuation=False,
        )
        on_date = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=False,
            include_valuation=False,
        )
    assert before.source_ttm_as_of_date == "2025-12-31"
    assert before.source_score_as_of_date == "2025-12-31"
    assert on_date.source_ttm_as_of_date == "2026-03-31"
    assert on_date.source_score_as_of_date == "2026-03-31"


def test_own_score_unavailable_returns_partial_snapshot() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55, include_aapl_score=False)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        snapshot = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=False,
            include_valuation=False,
        )
    assert snapshot.snapshot_status == STATUS_PARTIAL
    assert "score" in snapshot.missing_components
    assert snapshot.score is None


def test_percentile_unavailable_is_partial_not_failure() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=12)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        snapshot = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=True,
            include_valuation=False,
        )
    assert snapshot.snapshot_status == STATUS_PARTIAL
    assert "percentiles" in snapshot.missing_components
    assert snapshot.percentiles is None


def test_valuation_unavailable_is_partial_not_failure() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55, include_aapl_price=False)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        snapshot = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=False,
            include_valuation=True,
        )
    assert snapshot.snapshot_status == STATUS_PARTIAL
    assert "valuation" in snapshot.missing_components
    assert "price" in snapshot.missing_components
    assert snapshot.valuation is None


def test_no_available_ttm_does_not_use_future_effective_data() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        snapshot = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="FUTR",
            as_of_date="2026-04-29",
            market="omxh",
            include_percentiles=True,
            include_valuation=True,
        )
    assert snapshot.snapshot_status == STATUS_NO_AVAILABLE_TTM
    assert snapshot.ttm is None
    assert snapshot.percentiles is None
    assert snapshot.valuation is None


def test_optional_section_toggles_are_deterministic() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        first = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=False,
            include_valuation=False,
        )
        second = build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=False,
            include_valuation=False,
        )
    assert first == second
    assert first.percentiles is None
    assert first.valuation is None
    assert "PERCENTILES_DISABLED" in first.warnings
    assert "VALUATION_DISABLED" in first.warnings


def test_query_time_no_writes_and_vintage_provenance_unused() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55, include_vintage=True)
    before = _counts(fundamentals_db)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=True,
            include_valuation=True,
        )
        audit_historical_fundamental_snapshots(
            fconn,
            pconn,
            tickers=["AAPL", "PEER001"],
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=True,
            include_valuation=True,
        )
    assert before == _counts(fundamentals_db)


def test_current_snapshot_builder_unchanged() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
    with sqlite3.connect(fundamentals_db) as fconn:
        before = build_snapshot_matrix(fconn, "AAPL", 2, "FUND_SCORE_PERCENTILE_V2_PRE", None)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        build_historical_fundamental_snapshot(
            fconn,
            pconn,
            ticker="AAPL",
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=True,
            include_valuation=True,
        )
    with sqlite3.connect(fundamentals_db) as fconn:
        after = build_snapshot_matrix(fconn, "AAPL", 2, "FUND_SCORE_PERCENTILE_V2_PRE", None)
    assert before == after


def test_inspect_cli_json_and_audit_cli_temp_artifacts() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
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
            "2026-04-30",
            "--market",
            "omxh",
            "--include-percentiles",
            "--include-valuation",
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
            "2026-04-30",
            "--market",
            "omxh",
            "--include-percentiles",
            "--include-valuation",
            "--output-root",
            str(output_root),
            "--json",
        ]
    ) == 0
    assert json.loads((output_root / "summary.json").read_text(encoding="utf-8"))["ticker_date_rows_evaluated"] == 1
    assert (output_root / "audit_rows.csv").exists()
    assert (output_root / "progress.log").exists()


def test_temp_only_runtime_path_guard() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        audit_main(
            [
                "--fundamentals-db",
                str(fundamentals_db),
                "--price-db",
                str(price_db),
                "--ticker",
                "AAPL",
                "--as-of-date",
                "2026-04-30",
                "--market",
                "omxh",
                "--output-root",
                str(repository_root() / "bad-historical-snapshot"),
            ]
        )


def test_representative_boundary_and_missing_tickers() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        for ticker in ["AAPL", "MSFT", "JPM", "XOM", "NVDA", "GIS", "LMT", "BBY", "ARWR"]:
            latest = _latest_effective(fconn, ticker)
            assert latest is not None
            before_date = (date.fromisoformat(latest["effective_trading_date"]) - timedelta(days=1)).isoformat()
            before = build_historical_fundamental_snapshot(
                fconn, pconn, ticker=ticker, as_of_date=before_date, market="omxh", include_percentiles=False, include_valuation=False
            )
            on_date = build_historical_fundamental_snapshot(
                fconn, pconn, ticker=ticker, as_of_date=latest["effective_trading_date"], market="omxh", include_percentiles=False, include_valuation=False
            )
            assert on_date.source_ttm_as_of_date == latest["as_of_date"]
            assert before.source_ttm_as_of_date != on_date.source_ttm_as_of_date
        for ticker in ["DGXX", "AVNS"]:
            missing = build_historical_fundamental_snapshot(
                fconn, pconn, ticker=ticker, as_of_date="2026-04-30", market="omxh", include_percentiles=True, include_valuation=True
            )
            assert missing.snapshot_status == STATUS_NO_AVAILABLE_TTM


def test_audit_counts_and_performance_fixture() -> None:
    fundamentals_db, price_db = _build_fixture(peer_count=55)
    started = time.perf_counter()
    with sqlite3.connect(fundamentals_db) as fconn, sqlite3.connect(price_db) as pconn:
        summary, rows = audit_historical_fundamental_snapshots(
            fconn,
            pconn,
            tickers=["AAPL", "MSFT", "DGXX", "AVNS"],
            as_of_date="2026-04-30",
            market="omxh",
            include_percentiles=True,
            include_valuation=True,
        )
    assert summary.ticker_date_rows_evaluated == 4
    assert summary.ok_snapshots >= 2
    assert summary.no_available_ttm == 2
    assert summary.median_snapshot_query_seconds is not None
    assert rows[0]["safe_snapshot_status"] in {STATUS_OK, STATUS_PARTIAL, STATUS_NO_AVAILABLE_TTM}
    assert time.perf_counter() - started < 5.0


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamental_historical_snapshot" / "tests"


def _build_fixture(
    *,
    peer_count: int,
    include_aapl_score: bool = True,
    include_aapl_price: bool = True,
    include_vintage: bool = False,
) -> tuple[Path, Path]:
    root = _runtime_root()
    root.mkdir(parents=True, exist_ok=True)
    fundamentals_db = root / f"{uuid.uuid4().hex}.fundamentals.db"
    price_db = root / f"{uuid.uuid4().hex}.price.db"
    run_migration(fundamentals_db)
    with sqlite3.connect(price_db) as conn:
        conn.execute("CREATE TABLE osakedata (id INTEGER PRIMARY KEY AUTOINCREMENT, osake TEXT, pvm TEXT, close REAL, market TEXT)")
        conn.execute("CREATE TABLE ticker_meta (ticker TEXT PRIMARY KEY, market TEXT, sector TEXT, industry TEXT)")
    with sqlite3.connect(fundamentals_db) as conn:
        for ticker in ["AAPL", "MSFT", "JPM", "XOM", "NVDA", "GIS", "LMT", "BBY", "ARWR", "FUTR"]:
            _insert_two_periods(conn, ticker, include_score=include_aapl_score or ticker != "AAPL")
        for index in range(peer_count):
            ticker = f"PEER{index:03d}"
            _insert_two_periods(conn, ticker, score_base=35.0 + index)
    for ticker in ["AAPL", "MSFT", "JPM", "XOM", "NVDA", "GIS", "LMT", "BBY", "ARWR", "FUTR", "DGXX", "AVNS"]:
        if include_aapl_price or ticker != "AAPL":
            _insert_close(price_db, ticker, "2026-04-29", 10.0)
            _insert_close(price_db, ticker, "2026-04-30", 11.0)
        _insert_meta(price_db, ticker)
    for index in range(peer_count):
        ticker = f"PEER{index:03d}"
        _insert_close(price_db, ticker, "2026-04-30", 10.0 + index)
        _insert_meta(price_db, ticker)
    if include_vintage:
        with sqlite3.connect(fundamentals_db) as conn:
            conn.execute("INSERT INTO rc_fundamental_quarterly_vintage(ticker, market, period_end_date, statement_vintage_id, source_provider, source_document_id, source_hash, revision_number, is_restated, availability_quality, available_at_utc, ingested_at_utc, run_id, created_at_utc) VALUES ('AAPL', 'omxh', '2025-12-31', 'v1', 'fixture', 'doc', 'hash', 1, 0, 'fixture', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'fixture', '2026-01-01T00:00:00Z')")
            conn.execute("INSERT INTO rc_fundamental_quarterly_field_provenance(ticker, market, period_end_date, statement_vintage_id, field_name, field_value, source_provider, source_table, provenance_role, merge_action, created_at_utc, run_id) VALUES ('AAPL', 'omxh', '2025-12-31', 'v1', 'revenue', 100, 'fixture', 'fixture', 'primary', 'insert', '2026-01-01T00:00:00Z', 'fixture')")
    return fundamentals_db, price_db


def _insert_two_periods(conn: sqlite3.Connection, ticker: str, *, include_score: bool = True, score_base: float = 70.0) -> None:
    if ticker != "FUTR":
        _insert_ttm(conn, ticker, "2025-12-31", "2026-01-29", score=score_base, include_score=include_score, shares=10.0)
    future_effective = "2026-05-15" if ticker == "FUTR" else "2026-04-30"
    _insert_ttm(conn, ticker, "2026-03-31", future_effective, score=score_base + 5.0, include_score=include_score, shares=10.0)


def _insert_ttm(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    effective_date: str,
    *,
    score: float,
    include_score: bool,
    shares: float,
) -> None:
    score_value = score if include_score else None
    component_value = 10.0 if include_score else None
    score_rule = "fixture" if include_score else None
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
            component_value,
            component_value,
            component_value,
            component_value,
            component_value,
            component_value,
            component_value,
            component_value,
            effective_date,
            effective_date,
            score_rule,
        ),
    )
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly(
            ticker, period_end_date, revenue, operating_income, free_cashflow,
            cash, total_debt, shares_outstanding, currency, run_id
        ) VALUES (?, ?, 100.0, 20.0, 10.0, 5.0, 2.0, ?, 'EUR', 'fixture')
        """,
        (ticker, as_of_date, shares),
    )


def _insert_close(db_path: Path, ticker: str, pvm: str, close: float, market: str = "omxh") -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO osakedata(osake, pvm, close, market) VALUES (?, ?, ?, ?)", (ticker, pvm, close, market))


def _insert_meta(db_path: Path, ticker: str, market: str = "omxh") -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT OR IGNORE INTO ticker_meta(ticker, market, sector, industry) VALUES (?, ?, 'Tech', 'Software')", (ticker, market))


def _latest_effective(conn: sqlite3.Connection, ticker: str) -> dict[str, str] | None:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT as_of_date, effective_trading_date
        FROM rc_fundamental_ttm
        WHERE ticker = ?
        ORDER BY effective_trading_date DESC, as_of_date DESC
        LIMIT 1
        """,
        (ticker,),
    ).fetchone()
    return None if row is None else {"as_of_date": str(row["as_of_date"]), "effective_trading_date": str(row["effective_trading_date"])}


def _counts(db: Path) -> dict[str, int]:
    with sqlite3.connect(db) as conn:
        return {
            "ttm": conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0],
            "valuation": conn.execute("SELECT COUNT(*) FROM rc_fundamental_valuation").fetchone()[0],
            "percentile": conn.execute("SELECT COUNT(*) FROM rc_fundamental_score_percentile").fetchone()[0],
            "vintage": conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0],
            "provenance": conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0],
        }
