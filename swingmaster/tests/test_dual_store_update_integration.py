from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from swingmaster.fundamentals.dual_store_update_integration import (
    OVERALL_FAILED,
    OVERALL_PARTIAL,
    OVERALL_SUCCESS,
    STATUS_FAILED,
    STATUS_NOOP,
    STATUS_RETRY,
    STATUS_SUCCESS,
    LegacyComponentResult,
    ensure_v2_followup_schema,
    exit_code_for_overall_status,
    run_integrated_dual_store_update,
)
from swingmaster.fundamentals.dual_store_update_preflight import SQLiteV2FollowupRepository
from swingmaster.fundamentals.result_check import PLAN_VERSION, candidate_hash, merge_v2_followups_into_plan
from swingmaster.fundamentals.selected_v2_work_unit_executor import StaticProviderAdapter, build_simfin_statement_candidates
from swingmaster.fundamentals.selected_v2_work_unit_executor import build_simfin_share_candidate
from swingmaster.tests.test_dual_store_update_preflight import _create_legacy_db, _insert_legacy_complete
from swingmaster.tests.test_selected_v2_work_unit_executor import _connect as _connect_v2
from swingmaster.tests.test_selected_v2_work_unit_executor import _company as _insert_v2_company
from swingmaster.tests.test_selected_v2_work_unit_executor import _quarter as _insert_v2_quarter


def _candidate(ticker: str = "TEST", period: str = "2026-06-30") -> dict[str, object]:
    return {
        "market": "usa",
        "ticker": ticker,
        "decision": "RETRY_PARTIAL_QUARTER",
        "planned_action": "PLAN_RETRY_QUARTERLY_FUNDAMENTALS",
        "target_period_end_date": period,
        "canonical_report_date": period,
        "canonical_fiscal_year": 2026,
        "canonical_fiscal_quarter": "Q2",
        "fundamental_fetch_enabled": 1,
        "eligible_for_execution": 1,
        "providers_due": {"simfin": "DUE_FOR_UPDATE_PROCESSING"},
    }


def _write_plan(path: Path, db_path: Path, rows: list[dict[str, object]], *, decision_date: str = "2026-08-15") -> None:
    payload = {
        "plan_version": PLAN_VERSION,
        "check_status": "SUCCESS",
        "fundamentals_db": str(db_path.resolve()),
        "ohlcv_db": str((path.parent / "ohlcv.db").resolve()),
        "ohlcv_stale_days": 3,
        "decision_date": decision_date,
        "created_at_utc": "2026-08-15T00:00:00Z",
        "candidate_count": len(rows),
        "executable_work_unit_count": len(rows),
        "candidate_hash": candidate_hash(rows),
        "candidates": rows,
        "work_units": [row.get("work_unit", {}) for row in rows],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _safe_plan_path(tmp_path: Path, name: str = "plan.json") -> Path:
    return Path("temp/test_dual_store_update_integration") / tmp_path.name / name


def _legacy_success(_work_unit) -> LegacyComponentResult:
    return LegacyComponentResult(attempted=True, status=STATUS_SUCCESS, writes=1, post_update_lifecycle_status="UPDATED_COMPLETE")


def _legacy_fail(_work_unit) -> LegacyComponentResult:
    return LegacyComponentResult(attempted=True, status=STATUS_FAILED, writes=0, retryable=True, errors=["LEGACY_TRANSIENT"])


def _simfin_adapter(value: float = 100.0) -> StaticProviderAdapter:
    return StaticProviderAdapter(
        "SIMFIN_FIXTURE",
        build_simfin_statement_candidates(
            provider="SIMFIN_API_STATEMENTS",
            fiscal_year=2026,
            fiscal_quarter="Q2",
            report_date="2026-06-30",
            values={"revenue": value, "ebitda": 20.0, "free_cashflow": 8.0},
            source_observation_id="simfin:TEST:2026Q2",
            payload_sha256="hash",
        )
        + build_simfin_share_candidate(
            fiscal_year=2026,
            fiscal_quarter="Q2",
            report_date="2026-06-30",
            shares_outstanding=1000.0,
            source_observation_id="shares:TEST:2026Q2",
            payload_sha256="share-hash",
        ),
    )


def _setup_dual_store(
    tmp_path: Path,
    *,
    ticker: str = "TEST",
    v2_quarter: bool = True,
    core_complete: bool = False,
    legacy_complete: bool = True,
) -> tuple[Path, Path]:
    legacy = tmp_path / "legacy.db"
    v2 = tmp_path / "v2.db"
    _create_legacy_db(legacy)
    if legacy_complete:
        _insert_legacy_complete(legacy, ticker, "2026-06-30")
    with _connect_v2(v2) as conn:
        _insert_v2_company(conn, ticker=ticker, company_id=1, profile="ORDINARY")
        if v2_quarter:
            _insert_v2_quarter(conn, company_id=1, quarter_id=10)
            if core_complete:
                conn.execute(
                    """
                    UPDATE rc_v2_fundamental_quarterly
                    SET revenue=1, ebitda=2, free_cashflow=3, shares_outstanding=4
                    WHERE quarter_id=10
                    """
                )
                conn.commit()
    return legacy, v2


def test_followup_schema_idempotent_and_source_b_due_filter(tmp_path: Path) -> None:
    _, v2 = _setup_dual_store(tmp_path)
    with sqlite3.connect(v2) as conn:
        ensure_v2_followup_schema(conn)
        ensure_v2_followup_schema(conn)
        conn.execute(
            """
            INSERT INTO rc_v2_operational_followup (
                work_unit_key, market, ticker, company_id, fiscal_year, fiscal_quarter, canonical_report_date,
                last_v2_component_status, followup_reason, retry_required, maintenance_required, next_retry_at,
                last_attempt_at, last_run_id, active, created_at, updated_at
            ) VALUES ('usa|TEST|2026|Q2', 'usa', 'TEST', 1, 2026, 'Q2', '2026-06-30',
                      'RETRY', 'NO_DATA', 1, 0, '2026-08-16T00:00:00Z',
                      '2026-08-15T00:00:00Z', 'run-1', 1, '2026-08-15T00:00:00Z', '2026-08-15T00:00:00Z')
            """
        )
        conn.execute(
            """
            INSERT INTO rc_v2_operational_followup (
                work_unit_key, market, ticker, company_id, fiscal_year, fiscal_quarter, canonical_report_date,
                last_v2_component_status, followup_reason, retry_required, maintenance_required,
                last_attempt_at, last_run_id, active, created_at, updated_at
            ) VALUES ('usa|BANK|2026|Q2', 'usa', 'BANK', 2, 2026, 'Q2', '2026-06-30',
                      'BLOCKED', 'UNSUPPORTED', 0, 0,
                      '2026-08-15T00:00:00Z', 'run-1', 1, '2026-08-15T00:00:00Z', '2026-08-15T00:00:00Z')
            """
        )
        conn.commit()
    repo = SQLiteV2FollowupRepository(v2)

    assert repo.list_due_v2_followups(as_of=__import__("datetime").date(2026, 8, 15)) == []
    due = repo.list_due_v2_followups(as_of=__import__("datetime").date(2026, 8, 16))
    assert [row.ticker for row in due] == ["TEST"]


def test_source_b_plan_merge_preserves_source_a_and_adds_due_only() -> None:
    plan = {
        "plan_version": PLAN_VERSION,
        "check_status": "SUCCESS",
        "candidate_count": 1,
        "candidate_hash": candidate_hash([_candidate("TEST")]),
        "candidates": [_candidate("TEST")],
    }
    merged = merge_v2_followups_into_plan(
        plan,
        [
            {
                "work_unit_key": "usa|TEST|2026|Q2",
                "market": "usa",
                "ticker": "TEST",
                "fiscal_year": 2026,
                "fiscal_quarter": "Q2",
                "canonical_report_date": "2026-06-30",
                "followup_reason": "retry",
            },
            {
                "work_unit_key": "usa|NEXT|2026|Q2",
                "market": "usa",
                "ticker": "NEXT",
                "fiscal_year": 2026,
                "fiscal_quarter": "Q2",
                "canonical_report_date": "2026-06-30",
                "followup_reason": "retry",
            },
        ],
    )

    assert merged["candidate_count"] == 2
    assert merged["source_b_v2_followup_merged_count"] == 1
    assert merged["source_b_v2_followup_added_count"] == 1
    assert merged["candidates"][0]["decision"] == "RETRY_PARTIAL_QUARTER"


def test_end_to_end_retry_continuity_and_resolution(tmp_path: Path) -> None:
    legacy, v2 = _setup_dual_store(tmp_path)
    plan = _safe_plan_path(tmp_path)
    _write_plan(plan, legacy, [_candidate()])
    retry_adapter = StaticProviderAdapter("SIMFIN_NO_DATA", [], no_data=True, next_retry_at="2026-08-16T00:00:00Z")

    first = run_integrated_dual_store_update(
        plan_path=plan,
        legacy_db_path=legacy,
        v2_db_path=v2,
        execution_decision_date="2026-08-15",
        run_id="run-1",
        legacy_runner=_legacy_success,
        provider_adapters_by_work_unit={"usa|TEST|2026|Q2": [retry_adapter]},
    )
    assert first.overall_status == OVERALL_PARTIAL
    assert first.exit_code == 2
    assert SQLiteV2FollowupRepository(v2).list_due_v2_followups(as_of=__import__("datetime").date(2026, 8, 15)) == []
    assert [row.ticker for row in SQLiteV2FollowupRepository(v2).list_due_v2_followups(as_of=__import__("datetime").date(2026, 8, 16))] == ["TEST"]

    empty_plan = _safe_plan_path(tmp_path, "empty_plan.json")
    _write_plan(empty_plan, legacy, [], decision_date="2026-08-16")
    second = run_integrated_dual_store_update(
        plan_path=empty_plan,
        legacy_db_path=legacy,
        v2_db_path=v2,
        execution_decision_date="2026-08-16",
        run_id="run-2",
        legacy_runner=_legacy_success,
        provider_adapters_by_work_unit={"usa|TEST|2026|Q2": [_simfin_adapter()]},
    )
    assert second.overall_status == OVERALL_SUCCESS
    assert second.summary()["legacy_noop"] == 1
    assert second.summary()["v2_canonical_writes"] == 4
    assert SQLiteV2FollowupRepository(v2).list_due_v2_followups(as_of=__import__("datetime").date(2026, 8, 16)) == []


def test_integrated_update_uses_provider_adapter_factory(tmp_path: Path) -> None:
    legacy, v2 = _setup_dual_store(tmp_path)
    plan = _safe_plan_path(tmp_path)
    _write_plan(plan, legacy, [_candidate()])

    result = run_integrated_dual_store_update(
        plan_path=plan,
        legacy_db_path=legacy,
        v2_db_path=v2,
        execution_decision_date="2026-08-15",
        run_id="run",
        legacy_runner=_legacy_success,
        provider_adapter_factory=lambda _work_unit: [_simfin_adapter()],
    )

    assert result.overall_status == OVERALL_SUCCESS
    assert result.summary()["v2_canonical_writes"] == 4
    assert result.work_units[0].v2.raw_summary["cache_hits"] == ["SIMFIN_FIXTURE"]


@pytest.mark.parametrize(
    ("legacy", "v2_kwargs", "expected_status", "expected_exit"),
    [
        (_legacy_success, {"v2_quarter": True, "adapter": _simfin_adapter()}, OVERALL_SUCCESS, 0),
        (_legacy_success, {"v2_quarter": True, "core_complete": True, "adapter": None}, OVERALL_SUCCESS, 0),
        (lambda _wu: LegacyComponentResult(False, STATUS_NOOP), {"v2_quarter": True, "adapter": _simfin_adapter()}, OVERALL_SUCCESS, 0),
        (_legacy_success, {"v2_quarter": True, "adapter": StaticProviderAdapter("NO_DATA", [], no_data=True)}, OVERALL_PARTIAL, 2),
        (_legacy_fail, {"v2_quarter": True, "legacy_complete": False, "adapter": _simfin_adapter()}, OVERALL_PARTIAL, 2),
    ],
)
def test_mixed_outcome_matrix_core_cases(tmp_path: Path, legacy, v2_kwargs, expected_status: str, expected_exit: int) -> None:
    legacy_db, v2_db = _setup_dual_store(
        tmp_path,
        v2_quarter=v2_kwargs.get("v2_quarter", True),
        core_complete=v2_kwargs.get("core_complete", False),
        legacy_complete=v2_kwargs.get("legacy_complete", True),
    )
    plan = _safe_plan_path(tmp_path)
    _write_plan(plan, legacy_db, [_candidate()])
    adapter = v2_kwargs.get("adapter")
    result = run_integrated_dual_store_update(
        plan_path=plan,
        legacy_db_path=legacy_db,
        v2_db_path=v2_db,
        execution_decision_date="2026-08-15",
        run_id="run",
        legacy_runner=legacy,
        provider_adapters_by_work_unit={"usa|TEST|2026|Q2": [] if adapter is None else [adapter]},
    )
    assert result.overall_status == expected_status
    assert result.exit_code == expected_exit


def test_followup_persistence_failure_is_not_success(tmp_path: Path) -> None:
    legacy, v2 = _setup_dual_store(tmp_path)
    plan = _safe_plan_path(tmp_path)
    _write_plan(plan, legacy, [_candidate()])

    def bad_persistor(_conn, _work_unit, _v2, _run_id):
        raise RuntimeError("FOLLOWUP_STORE_DOWN")

    result = run_integrated_dual_store_update(
        plan_path=plan,
        legacy_db_path=legacy,
        v2_db_path=v2,
        execution_decision_date="2026-08-15",
        run_id="run",
        legacy_runner=_legacy_success,
        provider_adapters_by_work_unit={"usa|TEST|2026|Q2": [StaticProviderAdapter("NO_DATA", [], no_data=True)]},
        followup_persistor=bad_persistor,
    )

    assert result.overall_status == OVERALL_FAILED
    assert result.exit_code == 1
    assert result.followup_metadata_errors


def test_exit_code_contract() -> None:
    assert exit_code_for_overall_status(OVERALL_SUCCESS) == 0
    assert exit_code_for_overall_status(OVERALL_PARTIAL) == 2
    assert exit_code_for_overall_status(OVERALL_FAILED) == 1
