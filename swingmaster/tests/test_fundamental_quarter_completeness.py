from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from swingmaster.cli.audit_fundamental_quarter_completeness import main as audit_cli_main
from swingmaster.fundamentals.quarter_completeness import (
    assess_quarter_completeness,
    assess_ticker_quarter_history,
    audit_quarter_completeness,
    calendar_status_transition,
    ingestion_status_transition,
    repository_root,
    upsert_quarter_ingestion_status,
)


def test_quarter_basic_complete_uses_canonical_raw_inputs_only() -> None:
    complete = assess_quarter_completeness(_complete_row("GOOD", "2024-03-31", ebitda=None, gross_profit=None, currency=None))
    derived_fcf = assess_quarter_completeness(
        _row("OCF", "2024-03-31", revenue=100, ebit=12, operating_cashflow=11, capex=-3, cash=5, total_debt=9, shares_outstanding=10)
    )
    partial = assess_quarter_completeness(_row("PART", "2024-03-31", revenue=100, ebit=12, free_cashflow=8))
    incomplete = assess_quarter_completeness(_row("BAD", "2024-03-31", revenue=100))
    empty = assess_quarter_completeness(_row("EMPTY", "2024-03-31"))
    invalid = assess_quarter_completeness(_row("INV", "not-a-date", revenue=100))

    assert complete.quarter_basic_complete is True
    assert derived_fcf.quarter_basic_complete is True
    assert partial.quarter_basic_complete is False
    assert incomplete.quarter_basic_complete is False
    assert empty.quarter_basic_complete is False
    assert invalid.quarter_basic_complete is False


def test_individual_quarter_does_not_claim_ttm_or_score_history_complete() -> None:
    row = assess_quarter_completeness(_complete_row("DIFF", "2024-03-31"))

    assert row.quarter_basic_complete is True
    assert row.ttm_input_complete is False
    assert row.score_history_complete is False
    assert row.valuation_input_ready is True
    assert "ebitda" not in row.missing_core_fields

    no_shares = assess_quarter_completeness(_row("NOSH", "2024-03-31", revenue=100, ebit=12, free_cashflow=9))
    assert no_shares.quarter_basic_complete is False
    assert no_shares.ttm_input_complete is False
    assert no_shares.score_history_complete is False
    assert no_shares.valuation_input_ready is False


def test_retry_recommendation_and_ticker_classifications() -> None:
    matched_empty = assess_quarter_completeness(_row("R", "2025-03-31", earnings_event_id=1))
    assert matched_empty.retry_recommendation == "RETRY_YAHOO_AND_SEC"

    latest_bad = [
        assess_quarter_completeness(_complete_row("T", "2023-03-31")),
        assess_quarter_completeness(_complete_row("T", "2023-06-30")),
        assess_quarter_completeness(_complete_row("T", "2023-09-30")),
        assess_quarter_completeness(_row("T", "2023-12-31")),
    ]
    assert assess_ticker_quarter_history(latest_bad)["classification"] == "LATEST_QUARTER_INCOMPLETE"

    old_gap = [
        assess_quarter_completeness(_row("O", "2020-03-31")),
        assess_quarter_completeness(_complete_row("O", "2023-03-31")),
        assess_quarter_completeness(_complete_row("O", "2023-06-30")),
        assess_quarter_completeness(_complete_row("O", "2023-09-30")),
        assess_quarter_completeness(_complete_row("O", "2023-12-31")),
    ]
    assert assess_ticker_quarter_history(old_gap)["classification"] == "RECENT_HISTORY_USABLE_OLD_GAPS"


def test_earnings_relationship_aggregation_and_cli_no_write() -> None:
    db = _build_db()
    _insert_quarter(db, "AAA", "2024-03-31", revenue=100, ebit=10, free_cashflow=8, cash=4, total_debt=9, shares_outstanding=10)
    _insert_quarter(db, "BBB", "2024-03-31")
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO rc_earnings_event(id, market, ticker, announcement_at, announcement_date, announcement_session, is_reported, source, source_observed_at_utc, source_timezone, created_at_utc, updated_at_utc) VALUES (1,'usa','BBB','2024-04-20T12:00:00-04:00','2024-04-20','DURING_MARKET',1,'YAHOO_FINANCE','2024-04-21T00:00:00Z','America/New_York','2024-04-21T00:00:00Z','2024-04-21T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO rc_fundamental_quarter_earnings_match(market, ticker, period_end_date, earnings_event_id, announcement_at, announcement_date, announcement_session, effective_trading_date, effective_date_status, reporting_delay_days, matching_status, matching_confidence, matching_method, candidate_count, availability_policy, matcher_version, created_at_utc, updated_at_utc) VALUES ('usa','BBB','2024-03-31',1,'2024-04-20T12:00:00-04:00','2024-04-20','DURING_MARKET','2024-04-22','RESOLVED',22,'MATCHED','HIGH','nearest',1,'event_effective_date','v1','2024-04-21T00:00:00Z','2024-04-21T00:00:00Z')"
        )

    before = _counts(db)
    payload = audit_quarter_completeness(db)
    assert payload["summary"]["quarter_basic_complete_count"] == 1
    assert payload["summary"]["quarter_basic_incomplete_count"] == 1
    assert payload["summary"]["earnings_relationship"]["quarterly_rows_with_earnings_match_but_incomplete"] == 1
    assert before == _counts(db)

    root = _runtime_root() / f"cli-{uuid.uuid4().hex}"
    assert audit_cli_main(["--fundamentals-db", str(db), "--output-root", str(root)]) == 0
    assert before == _counts(db)
    assert (root / "all_quarters.csv").exists()
    assert (root / "ticker_summary.csv").exists()
    assert (root / "field_completeness.csv").exists()
    assert (root / "retry_candidates.csv").exists()
    assert (root / "latest_quarter_issues.csv").exists()
    assert json.loads((root / "summary.json").read_text(encoding="utf-8"))["database_content_unchanged"] is True


def test_cli_rejects_paths_outside_temp_and_resume_path_must_be_temp() -> None:
    db = _build_db()
    bad_root = repository_root() / "outside-quarter-audit"
    try:
        audit_cli_main(["--fundamentals-db", str(db), "--output-root", str(bad_root)])
    except ValueError as exc:
        assert "RUNTIME_PATH_OUTSIDE_TEMP" in str(exc)
    else:
        raise AssertionError("expected outside-temp path rejection")


def test_future_calendar_and_ingestion_status_transitions() -> None:
    assert calendar_status_transition(estimated_announcement_date="2026-08-06", today="2026-08-06", completed_event_found=False).calendar_status == "DUE_TODAY"
    assert calendar_status_transition(estimated_announcement_date="2026-08-05", today="2026-08-06", completed_event_found=False).calendar_status == "DATE_PASSED_EVENT_NOT_FOUND"
    assert calendar_status_transition(estimated_announcement_date="2026-08-07", today="2026-08-06", completed_event_found=True).calendar_status == "COMPLETED_EVENT_FOUND"
    assert calendar_status_transition(estimated_announcement_date="2026-08-08", previous_estimated_announcement_date="2026-08-07", today="2026-08-06", completed_event_found=False).calendar_status == "DATE_CHANGED"

    complete = assess_quarter_completeness(_complete_row("GOOD", "2024-03-31"))
    assert ingestion_status_transition(published=False, fetched=False, fetch_failed=False, assessment=None).ingestion_status == "NOT_PUBLISHED"
    assert ingestion_status_transition(published=True, fetched=False, fetch_failed=False, assessment=None).ingestion_status == "PUBLISHED_DATA_NOT_FETCHED"
    assert ingestion_status_transition(published=True, fetched=True, fetch_failed=True, assessment=complete).ingestion_status == "FETCH_FAILED"
    assert ingestion_status_transition(published=True, fetched=True, fetch_failed=False, assessment=complete).ingestion_status == "QUARTER_BASIC_COMPLETE"
    assert ingestion_status_transition(published=True, fetched=True, fetch_failed=False, assessment=complete, source_compare_complete=True).ingestion_status == "INGEST_COMPLETE"
    assert ingestion_status_transition(published=True, fetched=True, fetch_failed=False, assessment=complete, source_compare_complete=True, historical=True).ingestion_status == "UNKNOWN_HISTORICAL_INGEST_COMPLETENESS"


def test_history_readiness_and_status_persistence_are_separate() -> None:
    db = _build_db()
    periods = [
        "2023-03-31",
        "2023-06-30",
        "2023-09-30",
        "2023-12-31",
        "2024-03-31",
        "2024-06-30",
        "2024-09-30",
        "2024-12-31",
    ]
    for index, period in enumerate(periods):
        _insert_quarter(
            db,
            "AAA",
            period,
            revenue=100 + index,
            ebit=10 + index,
            free_cashflow=5 + index,
            cash=3,
            total_debt=8,
            shares_outstanding=100 + index,
        )
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_ttm (
                ticker, as_of_date, revenue_growth_ttm_yoy, ebit_margin_ttm,
                ebit_margin_trend_4q, ebitda_margin_ttm, ebitda_margin_trend_4q,
                fcf_margin_ttm, fcf_margin_trend_4q,
                net_debt_to_ebit, share_dilution_yoy
            ) VALUES ('AAA', '2024-12-31', 0.1, 0.2, 0.01, 0.25, 0.04, 0.08, 0.02, 0.4, 0.03)
            """
        )

    payload = audit_quarter_completeness(db)
    latest = [row for row in payload["all_quarters"] if row["period_end_date"] == "2024-12-31"][0]
    fourth = [row for row in payload["all_quarters"] if row["period_end_date"] == "2023-12-31"][0]
    assert latest["quarter_basic_complete"] is True
    assert latest["ttm_input_complete"] is True
    assert latest["score_history_complete"] is True
    assert fourth["quarter_basic_complete"] is True
    assert fourth["ttm_input_complete"] is True
    assert fourth["score_history_complete"] is False

    with sqlite3.connect(db) as conn:
        rows_written = upsert_quarter_ingestion_status(
            conn,
            _assessment_rows_from_payload(payload),
            run_id="TEST_STATUS_BACKFILL",
            assessed_at_utc="2026-08-06T00:00:00Z",
        )
        stored = conn.execute(
            """
            SELECT quarter_basic_complete, ttm_input_complete, score_history_complete
            FROM rc_fundamental_quarter_ingestion_status
            WHERE ticker='AAA' AND period_end_date='2024-12-31'
            """
        ).fetchone()
    assert rows_written == 8
    assert stored == (1, 1, 1)


def _row(ticker: str, period: str, **values: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": ticker,
        "period_end_date": period,
        "revenue": None,
        "gross_profit": None,
        "operating_income": None,
        "ebit": None,
        "ebitda": None,
        "net_income": None,
        "operating_cashflow": None,
        "capex": None,
        "free_cashflow": None,
        "cash": None,
        "total_debt": None,
        "shares_outstanding": None,
        "currency": "USD",
        "run_id": "TEST",
        "earnings_event_id": None,
        "announcement_date": None,
        "effective_trading_date": None,
    }
    row.update(values)
    return row


def _complete_row(ticker: str, period: str, **values: object) -> dict[str, object]:
    row = _row(
        ticker,
        period,
        revenue=100,
        ebit=12,
        free_cashflow=9,
        cash=5,
        total_debt=8,
        shares_outstanding=10,
    )
    row.update(values)
    return row


def _build_db() -> Path:
    path = _runtime_root() / f"{uuid.uuid4().hex}.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                revenue REAL,
                gross_profit REAL,
                operating_income REAL,
                ebit REAL,
                ebitda REAL,
                net_income REAL,
                operating_cashflow REAL,
                capex REAL,
                free_cashflow REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL,
                currency TEXT,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, period_end_date)
            );
            CREATE TABLE rc_fundamental_ttm (
                ticker TEXT,
                as_of_date TEXT,
                revenue_growth_ttm_yoy REAL,
                ebit_margin_ttm REAL,
                ebit_margin_trend_4q REAL,
                ebitda_margin_ttm REAL,
                ebitda_margin_trend_4q REAL,
                fcf_margin_ttm REAL,
                fcf_margin_trend_4q REAL,
                net_debt_to_ebit REAL,
                share_dilution_yoy REAL
            );
            CREATE TABLE rc_fundamental_quarter_ingestion_status (
                id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                earnings_event_id INTEGER,
                announcement_date TEXT,
                effective_trading_date TEXT,
                ingestion_status TEXT NOT NULL,
                basic_status TEXT NOT NULL,
                quarter_basic_complete INTEGER NOT NULL DEFAULT 0,
                ttm_input_complete INTEGER NOT NULL DEFAULT 0,
                score_history_complete INTEGER NOT NULL DEFAULT 0,
                valuation_input_ready INTEGER NOT NULL DEFAULT 0,
                historical_research_ready INTEGER NOT NULL DEFAULT 0,
                available_basic_field_count INTEGER NOT NULL DEFAULT 0,
                missing_basic_fields TEXT NOT NULL DEFAULT '[]',
                missing_core_fields_json TEXT NOT NULL DEFAULT '[]',
                missing_ttm_fields_json TEXT NOT NULL DEFAULT '[]',
                missing_score_fields_json TEXT NOT NULL DEFAULT '[]',
                data_quality_warnings_json TEXT NOT NULL DEFAULT '[]',
                supported_source_field_count INTEGER,
                source_non_null_field_count INTEGER,
                persisted_matching_field_count INTEGER,
                retry_recommendation TEXT NOT NULL,
                last_fetch_status TEXT,
                last_fetch_source TEXT,
                last_source_observed_at_utc TEXT,
                last_checked_at_utc TEXT NOT NULL,
                assessment_policy_version TEXT NOT NULL,
                ingestion_evidence_type TEXT NOT NULL,
                run_id TEXT NOT NULL,
                assessed_at_utc TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                UNIQUE (market, ticker, period_end_date)
            );
            CREATE TABLE rc_earnings_event (
                id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                ticker TEXT NOT NULL,
                announcement_at TEXT NOT NULL,
                announcement_date TEXT NOT NULL,
                announcement_session TEXT NOT NULL,
                is_reported INTEGER NOT NULL,
                reported_eps REAL,
                estimated_eps REAL,
                surprise_pct REAL,
                source TEXT NOT NULL,
                source_observed_at_utc TEXT NOT NULL,
                source_timezone TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            CREATE TABLE rc_fundamental_quarter_earnings_match (
                id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                earnings_event_id INTEGER NOT NULL,
                announcement_at TEXT NOT NULL,
                announcement_date TEXT NOT NULL,
                announcement_session TEXT NOT NULL,
                effective_trading_date TEXT,
                effective_date_status TEXT NOT NULL,
                reporting_delay_days INTEGER NOT NULL,
                matching_status TEXT NOT NULL,
                matching_confidence TEXT NOT NULL,
                matching_method TEXT NOT NULL,
                candidate_count INTEGER NOT NULL,
                availability_policy TEXT NOT NULL,
                matcher_version TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            """
        )
    return path


def _insert_quarter(db: Path, ticker: str, period: str, **values: float) -> None:
    row = _row(ticker, period, **values)
    columns = [
        "ticker",
        "period_end_date",
        "revenue",
        "gross_profit",
        "operating_income",
        "ebit",
        "ebitda",
        "net_income",
        "operating_cashflow",
        "capex",
        "free_cashflow",
        "cash",
        "total_debt",
        "shares_outstanding",
        "currency",
        "run_id",
    ]
    with sqlite3.connect(db) as conn:
        conn.execute(
            f"INSERT INTO rc_fundamental_quarterly({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            [row[column] for column in columns],
        )


def _counts(db: Path) -> dict[str, int]:
    with sqlite3.connect(db) as conn:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("rc_fundamental_quarterly", "rc_fundamental_ttm", "rc_earnings_event", "rc_fundamental_quarter_earnings_match")
        }


def _assessment_rows_from_payload(payload: dict[str, object]) -> list[object]:
    from swingmaster.cli.backfill_fundamental_quarter_ingestion_status import _assessment_rows_from_payload

    return _assessment_rows_from_payload(payload)  # type: ignore[arg-type]


def _runtime_root() -> Path:
    return repository_root() / "temp" / "fundamental_quarter_completeness_audit" / "tests"
