from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable, Mapping

from swingmaster.fundamentals.dual_store_update_preflight import (
    LEGACY_NOOP,
    SQLiteV2FollowupRepository,
    V2_CREATE_QUARTER_AND_FILL_CORE,
    V2_DEFERRED_POLICY_UNSUPPORTED,
    V2_ENRICH_CORE,
    V2_MAINTENANCE_REQUIRED,
    V2_NOOP_CORE_CURRENT,
    V2_NOOP_SETTLED_INCOMPLETE,
    V2_RETRY_PROVIDER,
    PreflightResult,
    WorkUnitPreflight,
    run_dual_store_preflight,
)
from swingmaster.fundamentals.selected_v2_work_unit_executor import (
    EXECUTABLE_V2_ACTIONS,
    SelectedV2WorkUnitInput,
    SelectedWorkUnitProviderAdapter,
    V2ExecutorResult,
    execute_selected_v2_work_unit,
)


STATUS_SUCCESS = "SUCCESS"
STATUS_NOOP = "NOOP"
STATUS_RETRY = "RETRY"
STATUS_FAILED = "FAILED"
STATUS_BLOCKED = "BLOCKED"
OVERALL_SUCCESS = "SUCCESS"
OVERALL_PARTIAL = "PARTIAL"
OVERALL_FAILED = "FAILED"


@dataclass
class LegacyComponentResult:
    attempted: bool
    status: str
    writes: int = 0
    retryable: bool = False
    errors: list[str] = field(default_factory=list)
    post_update_lifecycle_status: str | None = None
    raw_summary: Mapping[str, Any] | None = None


@dataclass
class V2ComponentResult:
    attempted: bool
    status: str
    canonical_writes: int = 0
    provenance_writes: int = 0
    conflicts: int = 0
    rejections: int = 0
    retry_required: bool = False
    maintenance_required: bool = False
    deferred_reason: str | None = None
    core_complete_after: bool = False
    errors: list[str] = field(default_factory=list)
    raw_summary: Mapping[str, Any] | None = None


@dataclass
class IntegratedWorkUnitResult:
    work_unit_key: str
    market: str
    ticker: str
    fiscal_year: int
    fiscal_quarter: str
    report_date: str
    legacy_preflight_action: str
    v2_preflight_action: str
    legacy_reason: str
    v2_reason: str
    provider_due_summary: Mapping[str, str]
    legacy: LegacyComponentResult
    v2: V2ComponentResult
    overall_status: str
    retry_required: bool
    maintenance_required: bool
    deferred_reason: str | None


@dataclass
class IntegratedUpdateResult:
    overall_status: str
    exit_code: int
    retry_required: bool
    maintenance_required_count: int
    deferred_limitation_count: int
    component_failure_count: int
    provider_calls: int
    preflight: Mapping[str, Any]
    work_units: list[IntegratedWorkUnitResult]
    followup_metadata_errors: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        legacy_counts = _component_counts([row.legacy.status for row in self.work_units])
        v2_counts = _component_counts([row.v2.status for row in self.work_units])
        return {
            "overall_status": self.overall_status,
            "exit_code": self.exit_code,
            "planned_work_units": len(self.work_units),
            "legacy_attempted": sum(1 for row in self.work_units if row.legacy.attempted),
            "legacy_success": legacy_counts.get(STATUS_SUCCESS, 0),
            "legacy_noop": legacy_counts.get(STATUS_NOOP, 0),
            "legacy_retry": legacy_counts.get(STATUS_RETRY, 0),
            "legacy_failed": legacy_counts.get(STATUS_FAILED, 0),
            "v2_attempted": sum(1 for row in self.work_units if row.v2.attempted),
            "v2_success": v2_counts.get(STATUS_SUCCESS, 0),
            "v2_noop": v2_counts.get(STATUS_NOOP, 0),
            "v2_retry": v2_counts.get(STATUS_RETRY, 0),
            "v2_blocked": v2_counts.get(STATUS_BLOCKED, 0),
            "v2_failed": v2_counts.get(STATUS_FAILED, 0),
            "v2_canonical_writes": sum(row.v2.canonical_writes for row in self.work_units),
            "v2_provenance_writes": sum(row.v2.provenance_writes for row in self.work_units),
            "retry_required_count": sum(1 for row in self.work_units if row.retry_required),
            "maintenance_required_count": self.maintenance_required_count,
            "deferred_limitation_count": self.deferred_limitation_count,
            "component_failure_count": self.component_failure_count,
            "provider_calls": self.provider_calls,
            "followup_metadata_error_count": len(self.followup_metadata_errors),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.summary(),
            "preflight": dict(self.preflight),
            "followup_metadata_errors": self.followup_metadata_errors,
            "work_units": [asdict(row) for row in self.work_units],
        }


LegacyRunner = Callable[[WorkUnitPreflight], LegacyComponentResult]
ProviderAdapterFactory = Callable[[WorkUnitPreflight], list[SelectedWorkUnitProviderAdapter]]


def ensure_v2_followup_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rc_v2_operational_followup (
            work_unit_key TEXT PRIMARY KEY,
            market TEXT NOT NULL,
            ticker TEXT NOT NULL,
            company_id INTEGER,
            fiscal_year INTEGER NOT NULL,
            fiscal_quarter TEXT NOT NULL,
            canonical_report_date TEXT NOT NULL,
            last_v2_component_status TEXT NOT NULL,
            followup_reason TEXT,
            retry_required INTEGER NOT NULL DEFAULT 0,
            maintenance_required INTEGER NOT NULL DEFAULT 0,
            deferred_reason TEXT,
            next_retry_at TEXT,
            next_check_at TEXT,
            provider_backoff_reason TEXT,
            last_attempt_at TEXT NOT NULL,
            last_run_id TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            resolved_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rc_v2_operational_followup_due
        ON rc_v2_operational_followup(active, retry_required, fiscal_year, next_retry_at, next_check_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rc_v2_operational_followup_ticker
        ON rc_v2_operational_followup(market, ticker, fiscal_year, fiscal_quarter)
        """
    )


def persist_v2_followup(
    conn: sqlite3.Connection,
    *,
    work_unit: WorkUnitPreflight,
    v2_result: V2ComponentResult,
    run_id: str,
    now_utc: str | None = None,
) -> str:
    now = now_utc or _utc_now()
    ensure_v2_followup_schema(conn)
    if v2_result.retry_required:
        raw = dict(v2_result.raw_summary or {})
        conn.execute(
            """
            INSERT INTO rc_v2_operational_followup (
                work_unit_key, market, ticker, company_id, fiscal_year, fiscal_quarter, canonical_report_date,
                last_v2_component_status, followup_reason, retry_required, maintenance_required, deferred_reason,
                next_retry_at, next_check_at, provider_backoff_reason, last_attempt_at, last_run_id,
                active, resolved_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, NULL, ?, ?, ?, ?, ?, 1, NULL, ?, ?)
            ON CONFLICT(work_unit_key) DO UPDATE SET
                last_v2_component_status=excluded.last_v2_component_status,
                followup_reason=excluded.followup_reason,
                retry_required=1,
                maintenance_required=0,
                deferred_reason=NULL,
                next_retry_at=excluded.next_retry_at,
                next_check_at=excluded.next_check_at,
                provider_backoff_reason=excluded.provider_backoff_reason,
                last_attempt_at=excluded.last_attempt_at,
                last_run_id=excluded.last_run_id,
                active=1,
                resolved_at=NULL,
                updated_at=excluded.updated_at
            """,
            (
                work_unit.work_unit_key,
                work_unit.market,
                work_unit.ticker,
                work_unit.v2_state.company_id,
                work_unit.target_fiscal_year,
                work_unit.target_fiscal_quarter,
                work_unit.target_report_date,
                v2_result.status,
                _followup_reason(v2_result),
                raw.get("next_retry_at"),
                raw.get("next_retry_at"),
                raw.get("retry_reason"),
                now,
                run_id,
                now,
                now,
            ),
        )
        return "ACTIVE_RETRY_UPSERTED"
    if v2_result.maintenance_required or v2_result.deferred_reason:
        return "LIMITATION_NOT_PERSISTED"
    conn.execute(
        """
        UPDATE rc_v2_operational_followup
        SET retry_required=0, active=0, resolved_at=COALESCE(resolved_at, ?),
            last_v2_component_status=?, last_attempt_at=?, last_run_id=?, updated_at=?
        WHERE work_unit_key=? AND active=1
        """,
        (now, v2_result.status, now, run_id, now, work_unit.work_unit_key),
    )
    return "RESOLVED_OR_NOOP"


def run_integrated_dual_store_update(
    *,
    plan_path: Path,
    legacy_db_path: Path,
    v2_db_path: Path,
    execution_decision_date: str | date,
    run_id: str,
    legacy_runner: LegacyRunner,
    provider_adapters_by_work_unit: Mapping[str, list[SelectedWorkUnitProviderAdapter]] | None = None,
    provider_adapter_factory: ProviderAdapterFactory | None = None,
    output_json_path: Path | None = None,
    followup_persistor: Callable[[sqlite3.Connection, WorkUnitPreflight, V2ComponentResult, str], str] | None = None,
) -> IntegratedUpdateResult:
    preflight = run_dual_store_preflight(
        plan_path=plan_path,
        legacy_db_path=legacy_db_path,
        v2_db_path=v2_db_path,
        execution_decision_date=execution_decision_date,
        followup_repository=SQLiteV2FollowupRepository(v2_db_path),
    )
    provider_map = provider_adapters_by_work_unit or {}
    results: list[IntegratedWorkUnitResult] = []
    metadata_errors: list[str] = []
    for work_unit in preflight.work_units:
        legacy = _run_legacy_component(work_unit, legacy_runner)
        provider_adapters = (
            provider_adapter_factory(work_unit)
            if provider_adapter_factory is not None
            else provider_map.get(work_unit.work_unit_key, [])
        )
        v2 = _run_v2_component(v2_db_path, work_unit, run_id=run_id, provider_adapters=provider_adapters)
        if v2.retry_required or v2.status in {STATUS_SUCCESS, STATUS_NOOP, STATUS_BLOCKED}:
            try:
                with sqlite3.connect(v2_db_path) as followup_conn:
                    followup_conn.row_factory = sqlite3.Row
                    persistor = followup_persistor or _default_followup_persistor
                    persistor(followup_conn, work_unit, v2, run_id)
                    followup_conn.commit()
            except Exception as exc:
                metadata_errors.append(f"{work_unit.work_unit_key}:{exc}")
        row_status = _overall_for_components(legacy, v2)
        results.append(
            IntegratedWorkUnitResult(
                work_unit_key=work_unit.work_unit_key,
                market=work_unit.market,
                ticker=work_unit.ticker,
                fiscal_year=work_unit.target_fiscal_year,
                fiscal_quarter=work_unit.target_fiscal_quarter,
                report_date=work_unit.target_report_date,
                legacy_preflight_action=work_unit.legacy_state.legacy_action,
                v2_preflight_action=work_unit.v2_state.v2_action,
                legacy_reason=work_unit.legacy_state.reason,
                v2_reason=work_unit.v2_state.reason,
                provider_due_summary=work_unit.v2_state.provider_due_summary,
                legacy=legacy,
                v2=v2,
                overall_status=row_status,
                retry_required=legacy.retryable or v2.retry_required,
                maintenance_required=v2.maintenance_required,
                deferred_reason=v2.deferred_reason,
            )
        )
    overall = _overall_status(results, metadata_errors)
    result = IntegratedUpdateResult(
        overall_status=overall,
        exit_code=exit_code_for_overall_status(overall),
        retry_required=overall == OVERALL_PARTIAL,
        maintenance_required_count=sum(1 for row in results if row.maintenance_required),
        deferred_limitation_count=sum(1 for row in results if row.deferred_reason),
        component_failure_count=sum(
            int(row.legacy.status == STATUS_FAILED) + int(row.v2.status == STATUS_FAILED) for row in results
        ),
        provider_calls=sum(len(row.v2.raw_summary.get("providers_called", [])) for row in results if row.v2.raw_summary),
        preflight=preflight.summary(),
        work_units=results,
        followup_metadata_errors=metadata_errors,
    )
    if output_json_path is not None:
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        output_json_path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    return result


def exit_code_for_overall_status(status: str) -> int:
    if status == OVERALL_SUCCESS:
        return 0
    if status == OVERALL_PARTIAL:
        return 2
    return 1


def _run_legacy_component(work_unit: WorkUnitPreflight, legacy_runner: LegacyRunner) -> LegacyComponentResult:
    if work_unit.legacy_state.legacy_action == LEGACY_NOOP:
        return LegacyComponentResult(attempted=False, status=STATUS_NOOP, post_update_lifecycle_status="NOOP")
    try:
        return legacy_runner(work_unit)
    except Exception as exc:
        return LegacyComponentResult(attempted=True, status=STATUS_FAILED, retryable=True, errors=[str(exc)])


def _run_v2_component(
    v2_db_path: Path,
    work_unit: WorkUnitPreflight,
    *,
    run_id: str,
    provider_adapters: list[SelectedWorkUnitProviderAdapter],
) -> V2ComponentResult:
    action = work_unit.v2_state.v2_action
    if action in {V2_NOOP_CORE_CURRENT, V2_NOOP_SETTLED_INCOMPLETE}:
        return V2ComponentResult(
            attempted=False,
            status=STATUS_NOOP,
            core_complete_after=action == V2_NOOP_CORE_CURRENT,
            raw_summary={"preflight_action": action, "providers_called": []},
        )
    if action == V2_MAINTENANCE_REQUIRED:
        return V2ComponentResult(
            attempted=False,
            status=STATUS_BLOCKED,
            maintenance_required=True,
            deferred_reason=work_unit.v2_state.deferred_reason,
            raw_summary={"preflight_action": action, "providers_called": []},
        )
    if action == V2_DEFERRED_POLICY_UNSUPPORTED:
        return V2ComponentResult(
            attempted=False,
            status=STATUS_BLOCKED,
            deferred_reason=work_unit.v2_state.deferred_reason,
            raw_summary={"preflight_action": action, "providers_called": []},
        )
    if action not in EXECUTABLE_V2_ACTIONS:
        return V2ComponentResult(attempted=False, status=STATUS_FAILED, errors=[f"V2_ACTION_ROUTE_UNSUPPORTED:{action}"])
    try:
        with sqlite3.connect(v2_db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            executor_result = execute_selected_v2_work_unit(
                conn,
                SelectedV2WorkUnitInput.from_preflight(work_unit, run_id=run_id),
                provider_adapters=provider_adapters,
            )
        return _v2_component_from_executor(executor_result)
    except Exception as exc:
        return V2ComponentResult(attempted=True, status=STATUS_FAILED, retry_required=True, errors=[str(exc)])


def _v2_component_from_executor(result: V2ExecutorResult) -> V2ComponentResult:
    if result.core_complete_after or result.canonical_fields_written:
        status = STATUS_SUCCESS
    elif result.retry_required or result.execution_status == "NO_ELIGIBLE_PROVIDER_WORK":
        status = STATUS_RETRY
    else:
        status = STATUS_NOOP
    return V2ComponentResult(
        attempted=result.attempted,
        status=status,
        canonical_writes=len(result.canonical_fields_written),
        provenance_writes=result.provenance_rows_written,
        conflicts=len(result.conflicts),
        rejections=len(result.rejections),
        retry_required=status == STATUS_RETRY or result.retry_required,
        core_complete_after=result.core_complete_after,
        raw_summary=result.to_dict(),
    )


def _default_followup_persistor(conn: sqlite3.Connection, work_unit: WorkUnitPreflight, v2_result: V2ComponentResult, run_id: str) -> str:
    return persist_v2_followup(conn, work_unit=work_unit, v2_result=v2_result, run_id=run_id)


def _overall_for_components(legacy: LegacyComponentResult, v2: V2ComponentResult) -> str:
    if legacy.status == STATUS_FAILED and v2.status == STATUS_FAILED:
        return OVERALL_FAILED
    if legacy.retryable or v2.retry_required or legacy.status == STATUS_FAILED or v2.status == STATUS_FAILED:
        return OVERALL_PARTIAL
    return OVERALL_SUCCESS


def _overall_status(results: list[IntegratedWorkUnitResult], metadata_errors: list[str]) -> str:
    if metadata_errors:
        return OVERALL_FAILED
    if any(row.overall_status == OVERALL_FAILED for row in results):
        return OVERALL_FAILED
    if any(row.retry_required or row.overall_status == OVERALL_PARTIAL for row in results):
        return OVERALL_PARTIAL
    return OVERALL_SUCCESS


def _component_counts(statuses: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in statuses:
        counts[status] = counts.get(status, 0) + 1
    return counts


def _followup_reason(v2_result: V2ComponentResult) -> str:
    if v2_result.errors:
        return ";".join(v2_result.errors)
    raw = dict(v2_result.raw_summary or {})
    return str(raw.get("retry_reason") or raw.get("execution_status") or v2_result.status)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
