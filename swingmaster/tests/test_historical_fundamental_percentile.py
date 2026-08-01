from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from swingmaster.cli.audit_historical_fundamental_percentile import main as audit_main
from swingmaster.cli.inspect_historical_fundamental_percentile import main as inspect_main
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import repository_root
from swingmaster.fundamentals.historical_percentile import (
    HISTORICAL_PERCENTILE_POLICY,
    STATUS_NO_AVAILABLE_SCORE,
    STATUS_OK,
    STATUS_UNIVERSE_TOO_SMALL,
    audit_current_vs_historical_percentiles,
    calculate_historical_percentiles_as_of,
    calculate_ticker_historical_percentile_as_of,
    select_peer_scores_as_of,
)
from swingmaster.fundamentals.score_percentile import (
    build_percentile_rows,
    load_latest_percentile_snapshot,
)


def test_select_peer_scores_uses_one_latest_available_row_per_peer() -> None:
    fundamentals_db, osakedata_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
        _insert_meta(osakedata_conn, "AAPL", "Tech", "Hardware")
        _insert_score(fundamentals_conn, "AAPL", "2025-12-31", "2026-02-01", 10.0)
        _insert_score(fundamentals_conn, "AAPL", "2026-03-31", "2026-04-30", 20.0)
        _insert_score(fundamentals_conn, "MSFT", "2026-03-31", "2026-04-29", 30.0)
        rows, counts = select_peer_scores_as_of(fundamentals_conn, osakedata_conn, target_date="2026-04-29", market="usa")

    by_ticker = {row.ticker: row for row in rows}
    assert by_ticker["AAPL"].as_of_date == "2025-12-31"
    assert by_ticker["MSFT"].as_of_date == "2026-03-31"
    assert counts.selected_peer_count == 2


def test_future_and_null_effective_rows_are_excluded() -> None:
    fundamentals_db, osakedata_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
        _insert_meta(osakedata_conn, "AAPL", "Tech", "Hardware")
        _insert_meta(osakedata_conn, "MSFT", "Tech", "Software")
        _insert_meta(osakedata_conn, "NULLX", "Tech", "Software")
        _insert_score(fundamentals_conn, "AAPL", "2025-12-31", "2026-02-01", 10.0)
        _insert_score(fundamentals_conn, "MSFT", "2026-03-31", "2026-04-29", 30.0)
        _insert_score(fundamentals_conn, "NULLX", "2026-03-31", None, 40.0)
        rows, counts = select_peer_scores_as_of(fundamentals_conn, osakedata_conn, target_date="2026-02-15", market="usa")

    assert [row.ticker for row in rows] == ["AAPL"]
    assert counts.excluded_no_available_score_count == 1
    assert counts.excluded_null_effective_score_count == 1


def test_own_score_switches_on_effective_date() -> None:
    fundamentals_db, osakedata_db = _build_fixture(row_count=500)
    with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
        _insert_meta(osakedata_conn, "AAPL", "Tech", "Hardware")
        _insert_score(fundamentals_conn, "AAPL", "2025-12-31", "2026-02-01", 10.0)
        _insert_score(fundamentals_conn, "AAPL", "2026-03-31", "2026-04-30", 1000.0)
        before = calculate_ticker_historical_percentile_as_of(
            fundamentals_conn, osakedata_conn, ticker="AAPL", target_date="2026-04-29", market="usa"
        )
        on_date = calculate_ticker_historical_percentile_as_of(
            fundamentals_conn, osakedata_conn, ticker="AAPL", target_date="2026-04-30", market="usa"
        )

    assert before.status == STATUS_OK
    assert on_date.status == STATUS_OK
    assert before.ticker_score_period == "2025-12-31"
    assert on_date.ticker_score_period == "2026-03-31"


def test_peers_can_use_different_fiscal_periods_and_policy_is_recorded() -> None:
    fundamentals_db, osakedata_db = _build_fixture(row_count=500)
    with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
        rows, counts = calculate_historical_percentiles_as_of(
            fundamentals_conn, osakedata_conn, target_date="2026-04-15", market="usa"
        )

    periods = {row["as_of_date"] for row in rows}
    assert len(periods) > 1
    assert counts.selected_peer_count == 500
    assert rows[0]["population_date_policy"] == HISTORICAL_PERCENTILE_POLICY


def test_sector_filter_preserves_existing_grouping_source() -> None:
    fundamentals_db, osakedata_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
        _insert_meta(osakedata_conn, "AAA", "Tech", "Software")
        _insert_meta(osakedata_conn, "BBB", "Health", "Care")
        _insert_score(fundamentals_conn, "AAA", "2026-03-31", "2026-04-15", 10.0)
        _insert_score(fundamentals_conn, "BBB", "2026-03-31", "2026-04-15", 20.0)
        rows, _counts = select_peer_scores_as_of(
            fundamentals_conn, osakedata_conn, target_date="2026-04-15", market="usa", sector="Tech"
        )

    assert [row.ticker for row in rows] == ["AAA"]
    assert rows[0].sector == "Tech"


def test_metric_direction_and_tie_policy_match_current_builder() -> None:
    fundamentals_db, osakedata_db = _build_fixture(row_count=500)
    with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
        historical_rows, _counts = calculate_historical_percentiles_as_of(
            fundamentals_conn, osakedata_conn, target_date="2026-04-15", market="usa"
        )
        snapshot_rows = load_latest_percentile_snapshot(
            fundamentals_conn, osakedata_conn, target_date="2026-03-31", market="usa"
        )
        current_rows = build_percentile_rows(
            snapshot_rows, "2026-03-31", "RULE", "RUN", "2026-01-01T00:00:00Z", market="usa"
        )

    historical_by_ticker = {row["ticker"]: row for row in historical_rows}
    current_by_ticker = {row["ticker"]: row for row in current_rows}
    assert historical_by_ticker["T0499"]["growth_pct_global"] == current_by_ticker["T0499"]["growth_pct_global"]
    assert historical_by_ticker["T0000"]["leverage_pct_global"] == current_by_ticker["T0000"]["leverage_pct_global"]


def test_unavailable_and_small_population_statuses_are_explicit() -> None:
    fundamentals_db, osakedata_db = _build_fixture()
    with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
        _insert_meta(osakedata_conn, "AAPL", "Tech", "Hardware")
        _insert_score(fundamentals_conn, "AAPL", "2026-03-31", None, 10.0)
        unavailable = calculate_ticker_historical_percentile_as_of(
            fundamentals_conn, osakedata_conn, ticker="AAPL", target_date="2026-04-30", market="omxh"
        )
        _insert_score(fundamentals_conn, "MSFT", "2026-03-31", "2026-04-15", 20.0)
        small = calculate_ticker_historical_percentile_as_of(
            fundamentals_conn, osakedata_conn, ticker="MSFT", target_date="2026-04-30", market="usa"
        )

    assert unavailable.status == STATUS_NO_AVAILABLE_SCORE
    assert small.status == STATUS_UNIVERSE_TOO_SMALL


def test_historical_helpers_do_not_write_database() -> None:
    fundamentals_db, osakedata_db = _build_fixture(row_count=500)
    before = _fundamentals_hash(fundamentals_db)
    with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
        calculate_ticker_historical_percentile_as_of(
            fundamentals_conn, osakedata_conn, ticker="T0001", target_date="2026-04-15", market="usa", include_peers=True
        )
        audit_current_vs_historical_percentiles(
            fundamentals_conn, osakedata_conn, dates=["2026-04-15"], market="usa", sample_size=25
        )
    assert before == _fundamentals_hash(fundamentals_db)


def test_cli_outputs_json_and_audit_writes_only_temp_artifacts() -> None:
    fundamentals_db, osakedata_db = _build_fixture(row_count=500)
    output_root = _runtime_root() / uuid.uuid4().hex / "audit"
    assert inspect_main(
        [
            "--fundamentals-db",
            str(fundamentals_db),
            "--osakedata-db",
            str(osakedata_db),
            "--ticker",
            "T0001",
            "--as-of-date",
            "2026-04-15",
            "--market",
            "usa",
            "--json",
        ]
    ) == 0
    assert audit_main(
        [
            "--fundamentals-db",
            str(fundamentals_db),
            "--osakedata-db",
            str(osakedata_db),
            "--date",
            "2026-04-15",
            "--sample-size",
            "10",
            "--output-root",
            str(output_root),
            "--json",
        ]
    ) == 0
    summary = json.loads((output_root / "summary.json").read_text(encoding="utf-8"))
    assert summary["summary"]["dates_evaluated"] == 1
    assert (output_root / "audit_rows.csv").exists()


def test_temp_only_artifact_guard() -> None:
    fundamentals_db, osakedata_db = _build_fixture(row_count=50)
    try:
        audit_main(
            [
                "--fundamentals-db",
                str(fundamentals_db),
                "--osakedata-db",
                str(osakedata_db),
                "--date",
                "2026-04-15",
                "--output-root",
                str(repository_root() / "bad-percentile-artifacts"),
            ]
        )
    except ValueError as exc:
        assert "RUNTIME_PATH_OUTSIDE_TEMP" in str(exc)
    else:
        raise AssertionError("expected temp-only path guard")


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamental_percentile_effective_date" / "tests"


def _build_fixture(*, row_count: int = 0) -> tuple[Path, Path]:
    root = _runtime_root()
    root.mkdir(parents=True, exist_ok=True)
    fundamentals_db = root / f"{uuid.uuid4().hex}.fundamentals.db"
    osakedata_db = root / f"{uuid.uuid4().hex}.osakedata.db"
    run_migration(fundamentals_db)
    with sqlite3.connect(osakedata_db) as conn:
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
    if row_count:
        with sqlite3.connect(fundamentals_db) as fundamentals_conn, sqlite3.connect(osakedata_db) as osakedata_conn:
            for index in range(row_count):
                ticker = f"T{index:04d}"
                as_of = "2026-03-31" if index % 2 == 0 else "2025-12-31"
                effective = "2026-04-15" if index % 2 == 0 else "2026-02-01"
                _insert_meta(osakedata_conn, ticker, "Tech", "Software")
                _insert_score(fundamentals_conn, ticker, as_of, effective, float(index))
    return fundamentals_db, osakedata_db


def _insert_meta(conn: sqlite3.Connection, ticker: str, sector: str, industry: str) -> None:
    conn.execute(
        "INSERT INTO ticker_meta(ticker, market, sector, industry) VALUES (?, 'usa', ?, ?)",
        (ticker, sector, industry),
    )


def _insert_score(
    conn: sqlite3.Connection,
    ticker: str,
    as_of_date: str,
    effective_date: str | None,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_ttm (
            ticker, as_of_date, latest_period_end_date,
            revenue_growth_ttm_yoy, ebit_margin_ttm, ebit_margin_trend_4q,
            fcf_margin_ttm, net_debt_to_ebitda, share_dilution_yoy,
            consistency_component_lifecycle, lifecycle_class,
            fundamental_score_lifecycle, score_rule_lifecycle,
            score_effective_trading_date, score_effective_date_status,
            score_effective_date_policy, score_effective_date_source_ttm_as_of_date,
            run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'UNCLASSIFIED', ?, 'FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE',
                  ?, ?, 'SOURCE_TTM_EFFECTIVE_DATE', ?, 'fixture')
        """,
        (
            ticker,
            as_of_date,
            as_of_date,
            value,
            value,
            value,
            value,
            1000.0 - value,
            1000.0 - value,
            value,
            value,
            effective_date,
            "RESOLVED" if effective_date is not None else "SOURCE_TTM_EFFECTIVE_DATE_NULL",
            as_of_date if effective_date is not None else None,
        ),
    )


def _fundamentals_hash(db_path: Path) -> tuple[int, int, str | None]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*), COUNT(*), MAX(score_effective_trading_date)
            FROM rc_fundamental_ttm
            """
        ).fetchone()
        percentile_rows = conn.execute("SELECT COUNT(*) FROM rc_fundamental_score_percentile").fetchone()[0]
    return int(row[0]), int(percentile_rows), row[2]
