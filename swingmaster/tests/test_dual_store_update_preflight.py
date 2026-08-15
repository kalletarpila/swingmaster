from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import sqlite3

import pytest

from swingmaster.fundamentals.dual_store_update_preflight import (
    InMemoryV2FollowupRepository,
    V2FollowupRecord,
    V2_ENRICH_CORE,
    V2_MAINTENANCE_REQUIRED,
    V2_NOOP_CORE_CURRENT,
    V2_NOOP_SETTLED_INCOMPLETE,
    V2_RETRY_PROVIDER,
    V2_DEFERRED_POLICY_UNSUPPORTED,
    followup_is_due,
    load_validated_source_a_plan,
    merge_work_units,
    normalize_source_a,
    run_dual_store_preflight,
    source_b_work_units,
)
from swingmaster.fundamentals.result_check import PLAN_VERSION, candidate_hash


def _create_legacy_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                revenue REAL,
                ebitda REAL,
                free_cashflow REAL,
                shares_outstanding REAL,
                cash REAL,
                total_debt REAL,
                operating_cashflow REAL,
                capex REAL,
                ebit REAL,
                run_id TEXT,
                PRIMARY KEY (ticker, period_end_date)
            );
            CREATE TABLE rc_fundamental_quarter_ingestion_status (
                ticker TEXT NOT NULL,
                market TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                ingestion_status TEXT,
                quarter_basic_complete INTEGER DEFAULT 0,
                retry_recommendation TEXT,
                source_confirmation_status TEXT,
                missing_core_fields_json TEXT DEFAULT '[]',
                PRIMARY KEY (ticker, market, period_end_date)
            );
            """
        )


def _create_v2_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_v2_company (
                company_id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                ticker TEXT,
                company_profile TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE rc_v2_quarter (
                quarter_id INTEGER PRIMARY KEY,
                company_id INTEGER NOT NULL,
                fiscal_year INTEGER NOT NULL,
                fiscal_period TEXT NOT NULL,
                report_date TEXT NOT NULL,
                UNIQUE (company_id, fiscal_year, fiscal_period, report_date)
            );
            CREATE TABLE rc_v2_fundamental_quarterly (
                quarter_id INTEGER PRIMARY KEY,
                revenue REAL,
                ebitda REAL,
                free_cashflow REAL,
                shares_outstanding REAL,
                cash REAL,
                total_debt REAL,
                operating_cashflow REAL,
                capex REAL,
                ebit REAL
            );
            """
        )


def _insert_legacy_complete(path: Path, ticker: str, period: str) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly
            (ticker, period_end_date, revenue, ebitda, free_cashflow, shares_outstanding, run_id)
            VALUES (?, ?, 1, 2, 3, 4, 'fixture')
            """,
            (ticker, period),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_ingestion_status
            (ticker, market, period_end_date, ingestion_status, quarter_basic_complete, retry_recommendation, source_confirmation_status)
            VALUES (?, 'usa', ?, 'QUARTER_BASIC_COMPLETE', 1, 'NO_ACTION', 'SEC_CONFIRMED')
            """,
            (ticker, period),
        )


def _insert_v2_company(path: Path, ticker: str, *, profile: str = "ORDINARY", company_id: int = 1) -> int:
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO rc_v2_company (company_id, market, ticker, company_profile, active) VALUES (?, 'usa', ?, ?, 1)",
            (company_id, ticker, profile),
        )
    return company_id


def _insert_v2_quarter(
    path: Path,
    company_id: int,
    *,
    quarter_id: int = 10,
    fy: int = 2026,
    fq: str = "Q2",
    report_date: str = "2026-06-30",
    core_complete: bool = False,
    partial_core: bool = True,
) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO rc_v2_quarter (quarter_id, company_id, fiscal_year, fiscal_period, report_date) VALUES (?, ?, ?, ?, ?)",
            (quarter_id, company_id, fy, fq, report_date),
        )
        values = (1, 2, 3, 4) if core_complete else ((1, None, None, None) if partial_core else (None, None, None, None))
        conn.execute(
            """
            INSERT INTO rc_v2_fundamental_quarterly
            (quarter_id, revenue, ebitda, free_cashflow, shares_outstanding)
            VALUES (?, ?, ?, ?, ?)
            """,
            (quarter_id, *values),
        )


def _candidate(ticker: str = "TEST", decision: str = "RETRY_PARTIAL_QUARTER") -> dict[str, object]:
    return {
        "market": "usa",
        "ticker": ticker,
        "decision": decision,
        "planned_action": "PLAN_RETRY_QUARTERLY_FUNDAMENTALS",
        "target_period_end_date": "2026-06-30",
        "canonical_report_date": "2026-06-30",
        "canonical_fiscal_year": 2026,
        "canonical_fiscal_quarter": "Q2",
        "fundamental_fetch_enabled": 1,
        "eligible_for_execution": 1,
        "providers_due": {"yahoo": "DUE_FOR_UPDATE_PROCESSING", "sec": "DUE_FOR_CONFIRMATION_OR_UPDATE_PROCESSING"},
    }


def _write_plan(path: Path, db_path: Path, rows: list[dict[str, object]], *, decision_date: str = "2026-08-15") -> None:
    plan = {
        "plan_version": PLAN_VERSION,
        "check_status": "SUCCESS",
        "fundamentals_db": str(db_path.resolve()),
        "decision_date": decision_date,
        "created_at_utc": "2026-08-15T00:00:00Z",
        "candidate_count": len(rows),
        "candidate_hash": candidate_hash(rows),
        "candidates": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan), encoding="utf-8")


def test_plan_validation_rejects_bad_hash(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.db"
    _create_legacy_db(legacy)
    plan_path = Path("temp/test_dual_store_update_preflight/bad_hash/plan.json")
    rows = [_candidate()]
    _write_plan(plan_path, legacy, rows)
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    payload["candidate_hash"] = "bad"
    plan_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="RESULT_CHECK_PLAN_CANDIDATE_HASH_MISMATCH"):
        load_validated_source_a_plan(plan_path=plan_path, db_path=legacy, execution_decision_date="2026-08-15")


def test_source_a_source_b_merge_preserves_one_work_unit() -> None:
    source_a = normalize_source_a(_candidate())
    source_b = source_b_work_units(
        InMemoryV2FollowupRepository(
            [V2FollowupRecord(market="usa", ticker="TEST", fiscal_year=2026, fiscal_quarter="Q2", report_date="2026-06-30")]
        ),
        as_of=date(2026, 8, 15),
    )

    merged, duplicates = merge_work_units([source_a], source_b)

    assert duplicates == 1
    assert len(merged) == 1
    assert merged[0].source_a is not None
    assert merged[0].source_b is not None


def test_followup_selection_excludes_infinite_loop_inputs() -> None:
    as_of = date(2026, 8, 15)

    assert followup_is_due(V2FollowupRecord(market="usa", ticker="DUE", fiscal_year=2026, fiscal_quarter="Q2"), as_of=as_of)
    assert not followup_is_due(
        V2FollowupRecord(market="usa", ticker="OLD", fiscal_year=2024, fiscal_quarter="Q4"),
        as_of=as_of,
    )
    assert not followup_is_due(
        V2FollowupRecord(market="usa", ticker="DONE", fiscal_year=2026, fiscal_quarter="Q2", resolved=True),
        as_of=as_of,
    )
    assert not followup_is_due(
        V2FollowupRecord(market="usa", ticker="LATER", fiscal_year=2026, fiscal_quarter="Q2", due_at="2026-08-16"),
        as_of=as_of,
    )


def test_run_preflight_classifies_core_complete_and_noop_settled_incomplete(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.db"
    v2 = tmp_path / "v2.db"
    _create_legacy_db(legacy)
    _create_v2_db(v2)
    _insert_legacy_complete(legacy, "DONE", "2026-06-30")
    _insert_legacy_complete(legacy, "SETTLED", "2026-06-30")
    done_company = _insert_v2_company(v2, "DONE", company_id=1)
    settled_company = _insert_v2_company(v2, "SETTLED", company_id=2)
    _insert_v2_quarter(v2, done_company, quarter_id=11, core_complete=True)
    _insert_v2_quarter(v2, settled_company, quarter_id=12, partial_core=True)
    rows = [_candidate("DONE"), {**_candidate("SETTLED"), "providers_due": {}}]
    plan_path = Path("temp/test_dual_store_update_preflight/core_states/plan.json")
    _write_plan(plan_path, legacy, rows)

    result = run_dual_store_preflight(
        plan_path=plan_path,
        legacy_db_path=legacy,
        v2_db_path=v2,
        execution_decision_date="2026-08-15",
    )
    actions = {row.ticker: row.v2_state.v2_action for row in result.work_units}

    assert actions == {"DONE": V2_NOOP_CORE_CURRENT, "SETTLED": V2_NOOP_SETTLED_INCOMPLETE}
    assert result.provider_calls == 0
    assert result.writes == 0


def test_run_preflight_classifies_due_retry_and_provider_enrich(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.db"
    v2 = tmp_path / "v2.db"
    _create_legacy_db(legacy)
    _create_v2_db(v2)
    for index, ticker in enumerate(["ENRICH", "RETRY"], start=1):
        _insert_legacy_complete(legacy, ticker, "2026-06-30")
        company_id = _insert_v2_company(v2, ticker, company_id=index)
        _insert_v2_quarter(v2, company_id, quarter_id=20 + index, partial_core=True)
    rows = [_candidate("ENRICH")]
    plan_path = Path("temp/test_dual_store_update_preflight/source_b_only/plan.json")
    _write_plan(plan_path, legacy, rows)
    repo = InMemoryV2FollowupRepository(
        [V2FollowupRecord(market="usa", ticker="RETRY", fiscal_year=2026, fiscal_quarter="Q2", report_date="2026-06-30")]
    )

    result = run_dual_store_preflight(
        plan_path=plan_path,
        legacy_db_path=legacy,
        v2_db_path=v2,
        execution_decision_date="2026-08-15",
        followup_repository=repo,
    )
    actions = {row.ticker: row.v2_state.v2_action for row in result.work_units}
    retry_row = next(row for row in result.work_units if row.ticker == "RETRY")

    assert actions["ENRICH"] == V2_ENRICH_CORE
    assert actions["RETRY"] == V2_RETRY_PROVIDER
    assert retry_row.source_a_selected is False
    assert retry_row.source_b_selected is True


def test_run_preflight_classifies_company_missing_and_unsupported_profile(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.db"
    v2 = tmp_path / "v2.db"
    _create_legacy_db(legacy)
    _create_v2_db(v2)
    _insert_legacy_complete(legacy, "MISS", "2026-06-30")
    _insert_legacy_complete(legacy, "BANK", "2026-06-30")
    _insert_v2_company(v2, "BANK", profile="BANK", company_id=5)
    rows = [_candidate("MISS"), _candidate("BANK")]
    plan_path = Path("temp/test_dual_store_update_preflight/blockers/plan.json")
    _write_plan(plan_path, legacy, rows)

    result = run_dual_store_preflight(
        plan_path=plan_path,
        legacy_db_path=legacy,
        v2_db_path=v2,
        execution_decision_date="2026-08-15",
    )
    actions = {row.ticker: row.v2_state.v2_action for row in result.work_units}

    assert actions["MISS"] == V2_MAINTENANCE_REQUIRED
    assert actions["BANK"] == V2_DEFERRED_POLICY_UNSUPPORTED
