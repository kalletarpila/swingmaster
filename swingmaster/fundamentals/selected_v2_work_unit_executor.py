from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Protocol

from swingmaster.fundamentals.dual_store_update_preflight import (
    CORE_FIELDS,
    OPPORTUNISTIC_FIELDS,
    V2_CREATE_QUARTER_AND_FILL_CORE,
    V2_DEFERRED_POLICY_UNSUPPORTED,
    V2_ENRICH_CORE,
    V2_MAINTENANCE_REQUIRED,
    V2_NOOP_CORE_CURRENT,
    V2_NOOP_SETTLED_INCOMPLETE,
    V2_OUT_OF_OPERATIONAL_SCOPE,
    V2_RETRY_PROVIDER,
    is_at_or_after_operational_floor,
    normalize_fiscal_quarter,
    normalize_market,
    normalize_ticker,
    work_unit_key,
)


EXECUTABLE_V2_ACTIONS = {
    V2_CREATE_QUARTER_AND_FILL_CORE,
    V2_ENRICH_CORE,
    V2_RETRY_PROVIDER,
}
NON_EXECUTABLE_V2_ACTIONS = {
    V2_NOOP_CORE_CURRENT,
    V2_NOOP_SETTLED_INCOMPLETE,
    V2_MAINTENANCE_REQUIRED,
    V2_DEFERRED_POLICY_UNSUPPORTED,
    V2_OUT_OF_OPERATIONAL_SCOPE,
}
SUPPORTED_COMPANY_PROFILES = {"ORDINARY"}
FIELD_WRITE_SET = (*CORE_FIELDS, *OPPORTUNISTIC_FIELDS)
ACTIONABLE_DUE_STATUSES = {
    "DUE_FOR_UPDATE_PROCESSING",
    "DUE_FOR_CONFIRMATION_OR_UPDATE_PROCESSING",
    "DUE_FOR_CONFIRMATION",
    "CACHE_AVAILABLE",
    "PROVIDER_DUE",
    "FOLLOWUP_RETRY_DUE",
}


@dataclass(frozen=True)
class SelectedV2WorkUnitInput:
    work_unit_key: str
    market: str
    ticker: str
    company_id: int
    company_profile: str
    fiscal_year: int
    fiscal_quarter: str
    canonical_report_date: str
    target_period_end_date: str
    identity_evidence: Mapping[str, Any]
    preflight_v2_action: str
    missing_core_fields: tuple[str, ...]
    opportunistic_gaps: tuple[str, ...]
    provider_due_summary: Mapping[str, str]
    run_id: str

    @classmethod
    def from_preflight(cls, row: Any, *, run_id: str) -> "SelectedV2WorkUnitInput":
        v2_state = row.v2_state
        missing_core = tuple(field for field, present in v2_state.core_presence.items() if not present)
        return cls(
            work_unit_key=row.work_unit_key,
            market=row.market,
            ticker=row.ticker,
            company_id=int(v2_state.company_id or 0),
            company_profile=str(v2_state.company_profile or ""),
            fiscal_year=int(row.target_fiscal_year),
            fiscal_quarter=normalize_fiscal_quarter(row.target_fiscal_quarter),
            canonical_report_date=str(row.target_report_date),
            target_period_end_date=str(row.target_period_end_date),
            identity_evidence={
                "source_a_selected": row.source_a_selected,
                "source_b_selected": row.source_b_selected,
                "preflight_reason": v2_state.reason,
            },
            preflight_v2_action=str(v2_state.v2_action),
            missing_core_fields=missing_core,
            opportunistic_gaps=tuple(v2_state.opportunistic_gaps),
            provider_due_summary=dict(v2_state.provider_due_summary),
            run_id=run_id,
        )


@dataclass(frozen=True)
class FieldCandidate:
    field: str
    value: float | int
    provider: str
    provider_field: str
    source_dataset: str
    source_file: str
    source_file_sha256: str
    source_observation_id: str
    transformation: str
    fiscal_year: int
    fiscal_quarter: str
    report_date: str
    quarter_match_mode: str
    quarter_match_evidence: Mapping[str, Any]
    validation_tier: str
    eligible: bool = True
    rejection_reason: str | None = None


@dataclass
class FieldCandidateResult:
    field: str
    candidate_value: float | int | None
    provider: str
    provider_field: str
    source_observation_identity: str
    transformation: str
    quarter_match_mode: str
    quarter_match_evidence: Mapping[str, Any]
    risk_validation_tier: str
    eligible: bool
    rejection_reason: str | None = None
    conflict_reason: str | None = None
    action: str = "NOT_EVALUATED"


@dataclass
class ProviderEvaluation:
    provider: str
    called: bool
    cache_hit: bool
    failure: str | None = None
    candidates: list[FieldCandidate] = field(default_factory=list)
    no_data: bool = False
    next_retry_at: str | None = None


@dataclass
class V2ExecutorResult:
    work_unit_key: str
    attempted: bool
    execution_status: str
    preflight_action: str
    providers_considered: list[str]
    providers_called: list[str]
    cache_hits: list[str]
    provider_failures: list[dict[str, str]]
    canonical_fields_before: dict[str, float | None]
    canonical_fields_written: dict[str, float | int]
    canonical_fields_unchanged: dict[str, float | int]
    conflicts: list[FieldCandidateResult]
    rejections: list[FieldCandidateResult]
    provenance_rows_written: int
    core_before: dict[str, bool]
    core_after: dict[str, bool]
    core_complete_after: bool
    retry_required: bool
    retry_reason: str | None
    next_retry_at: str | None
    maintenance_required: bool
    deferred_reason: str | None
    opportunistic_fields_written: dict[str, float | int]
    quarter_id: int | None = None
    structure_created: bool = False
    fundamental_shell_created: bool = False
    canonical_overwrite_count: int = 0
    unrelated_quarter_canonical_write_count: int = 0
    duplicate_provenance_count_on_replay: int = 0
    candidate_results: list[FieldCandidateResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["conflicts"] = [asdict(row) for row in self.conflicts]
        payload["rejections"] = [asdict(row) for row in self.rejections]
        payload["candidate_results"] = [asdict(row) for row in self.candidate_results]
        return payload


class SelectedWorkUnitProviderAdapter(Protocol):
    provider_name: str
    calls_network: bool

    def evaluate(self, conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput) -> ProviderEvaluation:
        ...


class StaticProviderAdapter:
    def __init__(
        self,
        provider_name: str,
        candidates: list[FieldCandidate] | None = None,
        *,
        calls_network: bool = False,
        cache_hit: bool = True,
        failure: str | None = None,
        no_data: bool = False,
        next_retry_at: str | None = None,
    ) -> None:
        self.provider_name = provider_name
        self.calls_network = calls_network
        self._candidates = candidates or []
        self._cache_hit = cache_hit
        self._failure = failure
        self._no_data = no_data
        self._next_retry_at = next_retry_at

    def evaluate(self, conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput) -> ProviderEvaluation:
        if self._failure:
            raise RuntimeError(self._failure)
        return ProviderEvaluation(
            provider=self.provider_name,
            called=self.calls_network,
            cache_hit=self._cache_hit,
            candidates=list(self._candidates),
            no_data=self._no_data,
            next_retry_at=self._next_retry_at,
        )


class NoopCacheAdapter:
    provider_name = "NOOP_CACHE"
    calls_network = False

    def evaluate(self, conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput) -> ProviderEvaluation:
        return ProviderEvaluation(provider=self.provider_name, called=False, cache_hit=False, no_data=True)


def execute_selected_v2_work_unit(
    conn: sqlite3.Connection,
    work_unit: SelectedV2WorkUnitInput,
    *,
    provider_adapters: list[SelectedWorkUnitProviderAdapter] | None = None,
    now_utc: str | None = None,
) -> V2ExecutorResult:
    now = now_utc or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    _validate_executable_input(work_unit)
    before_counts = _quarter_value_counts(conn, work_unit)
    quarter = _get_exact_quarter(conn, work_unit)
    fundamental = _get_fundamental(conn, int(quarter["quarter_id"])) if quarter is not None else None
    canonical_before = _canonical_fields(fundamental)
    core_before = {field_name: canonical_before.get(field_name) is not None for field_name in CORE_FIELDS}
    missing_core = [field_name for field_name in CORE_FIELDS if not core_before[field_name]]

    considered: list[str] = []
    provider_evaluations: list[ProviderEvaluation] = []
    provider_failures: list[dict[str, str]] = []
    should_consider_providers = bool(missing_core) and _provider_due_allows_core_work(work_unit)
    if should_consider_providers:
        for adapter in provider_adapters or []:
            considered.append(adapter.provider_name)
            try:
                evaluation = adapter.evaluate(conn, work_unit)
            except Exception as exc:
                provider_failures.append({"provider": adapter.provider_name, "error": str(exc)})
                continue
            provider_evaluations.append(evaluation)
            if _candidate_set_fills_all_missing_core(provider_evaluations, work_unit, missing_core):
                break

    candidate_results: list[FieldCandidateResult] = []
    accepted_candidates: list[FieldCandidate] = []
    for evaluation in provider_evaluations:
        for candidate in evaluation.candidates:
            result = _evaluate_candidate(candidate, work_unit)
            candidate_results.append(result)
            if result.eligible:
                accepted_candidates.append(candidate)

    written: dict[str, float | int] = {}
    unchanged: dict[str, float | int] = {}
    opportunistic_written: dict[str, float | int] = {}
    conflicts: list[FieldCandidateResult] = []
    rejections = [row for row in candidate_results if not row.eligible]
    provenance_written = 0
    structure_created = False
    shell_created = False
    quarter_id: int | None
    conn.execute("BEGIN")
    try:
        _ensure_import_run(conn, work_unit, now)
        quarter_id, structure_created = _ensure_selected_quarter(conn, work_unit, now)
        shell_created = _ensure_fundamental_shell(conn, quarter_id, now)
        for candidate in accepted_candidates:
            current = _current_field_value(conn, quarter_id, candidate.field)
            result = _matching_candidate_result(candidate, candidate_results)
            if current is None:
                _write_canonical_field(conn, quarter_id, candidate.field, candidate.value, now)
                if _insert_provenance(conn, quarter_id, candidate, work_unit.run_id, now):
                    provenance_written += 1
                written[candidate.field] = candidate.value
                result.action = "WRITTEN"
                if candidate.field in OPPORTUNISTIC_FIELDS:
                    opportunistic_written[candidate.field] = candidate.value
            elif _same_value(current, candidate.value):
                unchanged[candidate.field] = candidate.value
                result.action = "SAME_VALUE_NOOP"
            else:
                result.conflict_reason = "CANONICAL_NON_NULL_DIFFERENT"
                result.action = "CONFLICT_PRESERVED"
                conflicts.append(result)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    after = _canonical_fields(_get_fundamental(conn, int(quarter_id)) if quarter_id is not None else None)
    after_counts = _quarter_value_counts(conn, work_unit)
    core_after = {field_name: after.get(field_name) is not None for field_name in CORE_FIELDS}
    core_complete_after = all(core_after.values())
    retry_required = not core_complete_after and (bool(provider_failures) or any(item.no_data or item.next_retry_at for item in provider_evaluations))
    status = "SUCCESS_CORE_COMPLETE" if core_complete_after else ("SUCCESS_PARTIAL_RETRY" if retry_required else "SUCCESS_SETTLED_INCOMPLETE")
    if not written and not structure_created and not shell_created and not provider_evaluations and not provider_failures:
        status = "NO_ELIGIBLE_PROVIDER_WORK"
    return V2ExecutorResult(
        work_unit_key=work_unit.work_unit_key,
        attempted=True,
        execution_status=status,
        preflight_action=work_unit.preflight_v2_action,
        providers_considered=considered,
        providers_called=[item.provider for item in provider_evaluations if item.called],
        cache_hits=[item.provider for item in provider_evaluations if item.cache_hit],
        provider_failures=provider_failures,
        canonical_fields_before=canonical_before,
        canonical_fields_written=written,
        canonical_fields_unchanged=unchanged,
        conflicts=conflicts,
        rejections=rejections,
        provenance_rows_written=provenance_written,
        core_before=core_before,
        core_after=core_after,
        core_complete_after=core_complete_after,
        retry_required=retry_required,
        retry_reason=_retry_reason(provider_evaluations, provider_failures, core_complete_after),
        next_retry_at=_next_retry_at(provider_evaluations),
        maintenance_required=False,
        deferred_reason=None,
        opportunistic_fields_written=opportunistic_written,
        quarter_id=quarter_id,
        structure_created=structure_created,
        fundamental_shell_created=shell_created,
        canonical_overwrite_count=0,
        unrelated_quarter_canonical_write_count=max(0, after_counts["non_target_non_null"] - before_counts["non_target_non_null"]),
        duplicate_provenance_count_on_replay=0,
        candidate_results=candidate_results,
    )


def build_simfin_statement_candidates(
    *,
    provider: str,
    fiscal_year: int,
    fiscal_quarter: str,
    report_date: str,
    values: Mapping[str, float | int | None],
    source_observation_id: str,
    payload_sha256: str,
    validation_tier: str = "SAFE_SCOPED",
) -> list[FieldCandidate]:
    candidates: list[FieldCandidate] = []
    for field_name, provider_field, transformation in [
        ("revenue", "Revenue", "none"),
        ("ebitda", "Operating Income + Depreciation & Amortization", "operating_income + depreciation_amortization"),
        ("free_cashflow", "Net Cash from Operating Activities + Change in Fixed Assets & Intangibles", "operating_cashflow + capex"),
        ("cash", "Cash, Cash Equivalents & Short Term Investments", "none"),
        ("total_debt", "Short Term Debt + Long Term Debt", "short_term_debt + long_term_debt"),
        ("operating_cashflow", "Net Cash from Operating Activities", "none"),
        ("capex", "Change in Fixed Assets & Intangibles", "none"),
    ]:
        value = values.get(field_name)
        if value is None:
            continue
        candidates.append(
            field_candidate(
                field=field_name,
                value=value,
                provider=provider,
                provider_field=provider_field,
                source_dataset="simfin_statements",
                source_file="SIMFIN_API_RAW",
                source_file_sha256=payload_sha256,
                source_observation_id=source_observation_id,
                transformation=transformation,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                report_date=report_date,
                validation_tier=validation_tier,
            )
        )
    return candidates


def build_simfin_share_candidate(
    *,
    fiscal_year: int,
    fiscal_quarter: str,
    report_date: str,
    shares_outstanding: float | int | None,
    source_observation_id: str,
    payload_sha256: str,
    validation_tier: str = "SAFE_SCOPED",
) -> list[FieldCandidate]:
    if shares_outstanding is None:
        return []
    return [
        field_candidate(
            field="shares_outstanding",
            value=shares_outstanding,
            provider="SIMFIN_API_SHARES",
            provider_field="Common Shares Outstanding",
            source_dataset="simfin_shares",
            source_file="SIMFIN_API_SHARES_RAW",
            source_file_sha256=payload_sha256,
            source_observation_id=source_observation_id,
            transformation="none",
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            report_date=report_date,
            validation_tier=validation_tier,
        )
    ]


def build_yahoo_field_candidate(
    *,
    field: str,
    value: float | int | None,
    fiscal_year: int,
    fiscal_quarter: str,
    report_date: str,
    provider_field: str,
    source_observation_id: str,
    payload_sha256: str,
    validation_tier: str,
) -> list[FieldCandidate]:
    if value is None:
        return []
    return [
        field_candidate(
            field=field,
            value=value,
            provider="YAHOO",
            provider_field=provider_field,
            source_dataset="legacy_yahoo_raw",
            source_file=f"legacy_yahoo_raw:{source_observation_id}",
            source_file_sha256=payload_sha256,
            source_observation_id=source_observation_id,
            transformation="none",
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            report_date=report_date,
            validation_tier=validation_tier,
        )
    ]


def build_sec_revenue_candidate(
    *,
    revenue: float | int | None,
    fiscal_year: int,
    fiscal_quarter: str,
    report_date: str,
    source_observation_id: str,
    payload_sha256: str,
) -> list[FieldCandidate]:
    if revenue is None:
        return []
    return [
        field_candidate(
            field="revenue",
            value=revenue,
            provider="SEC",
            provider_field="RevenueFromContractWithCustomerExcludingAssessedTax|Revenues",
            source_dataset="sec_reconstructed_quarterly",
            source_file=f"sec_reconstructed:{source_observation_id}",
            source_file_sha256=payload_sha256,
            source_observation_id=source_observation_id,
            transformation="SAFE_SCOPED_RECONSTRUCTED_QUARTERLY_REVENUE",
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            report_date=report_date,
            validation_tier="SAFE_SCOPED",
        )
    ]


def field_candidate(
    *,
    field: str,
    value: float | int,
    provider: str,
    provider_field: str,
    source_dataset: str,
    source_file: str,
    source_file_sha256: str,
    source_observation_id: str,
    transformation: str,
    fiscal_year: int,
    fiscal_quarter: str,
    report_date: str,
    validation_tier: str,
    eligible: bool = True,
    rejection_reason: str | None = None,
) -> FieldCandidate:
    return FieldCandidate(
        field=field,
        value=value,
        provider=provider,
        provider_field=provider_field,
        source_dataset=source_dataset,
        source_file=source_file,
        source_file_sha256=source_file_sha256,
        source_observation_id=source_observation_id,
        transformation=transformation,
        fiscal_year=int(fiscal_year),
        fiscal_quarter=normalize_fiscal_quarter(fiscal_quarter),
        report_date=report_date,
        quarter_match_mode="CANONICAL_FY_FQ_REPORT_DATE",
        quarter_match_evidence={"fiscal_year": fiscal_year, "fiscal_quarter": fiscal_quarter, "report_date": report_date},
        validation_tier=validation_tier,
        eligible=eligible,
        rejection_reason=rejection_reason,
    )


def _validate_executable_input(work_unit: SelectedV2WorkUnitInput) -> None:
    if work_unit.preflight_v2_action not in EXECUTABLE_V2_ACTIONS:
        raise RuntimeError(f"V2_WORK_UNIT_ACTION_NOT_EXECUTABLE:{work_unit.preflight_v2_action}")
    if work_unit.preflight_v2_action in NON_EXECUTABLE_V2_ACTIONS:
        raise RuntimeError(f"V2_WORK_UNIT_ACTION_NOT_EXECUTABLE:{work_unit.preflight_v2_action}")
    if normalize_market(work_unit.market) != "usa":
        raise RuntimeError("V2_WORK_UNIT_MARKET_UNSUPPORTED")
    if work_unit.company_id <= 0:
        raise RuntimeError("V2_WORK_UNIT_COMPANY_REQUIRED")
    if work_unit.company_profile.upper() not in SUPPORTED_COMPANY_PROFILES:
        raise RuntimeError("V2_WORK_UNIT_COMPANY_PROFILE_UNSUPPORTED")
    if not is_at_or_after_operational_floor(work_unit.fiscal_year, work_unit.fiscal_quarter):
        raise RuntimeError("V2_WORK_UNIT_OUT_OF_OPERATIONAL_SCOPE")
    expected_key = work_unit_key(work_unit.market, work_unit.ticker, work_unit.fiscal_year, work_unit.fiscal_quarter)
    if work_unit.work_unit_key != expected_key:
        raise RuntimeError("V2_WORK_UNIT_KEY_MISMATCH")
    if not work_unit.canonical_report_date:
        raise RuntimeError("V2_WORK_UNIT_REPORT_DATE_REQUIRED")


def _provider_due_allows_core_work(work_unit: SelectedV2WorkUnitInput) -> bool:
    if work_unit.preflight_v2_action == V2_RETRY_PROVIDER:
        return True
    return any(str(status) in ACTIONABLE_DUE_STATUSES for status in work_unit.provider_due_summary.values())


def _evaluate_candidate(candidate: FieldCandidate, work_unit: SelectedV2WorkUnitInput) -> FieldCandidateResult:
    eligible = candidate.eligible
    reason = candidate.rejection_reason
    if candidate.field not in FIELD_WRITE_SET:
        eligible = False
        reason = "UNSUPPORTED_FIELD"
    elif candidate.value is None:
        eligible = False
        reason = "NULL_CANDIDATE_VALUE"
    elif candidate.fiscal_year != work_unit.fiscal_year or normalize_fiscal_quarter(candidate.fiscal_quarter) != work_unit.fiscal_quarter:
        eligible = False
        reason = "QUARTER_IDENTITY_MISMATCH"
    elif candidate.report_date != work_unit.canonical_report_date:
        eligible = False
        reason = "REPORT_DATE_MISMATCH"
    elif candidate.field == "ebit" and candidate.provider.upper().startswith("SIMFIN"):
        eligible = False
        reason = "SIMFIN_OPERATING_INCOME_NOT_EBIT"
    return FieldCandidateResult(
        field=candidate.field,
        candidate_value=candidate.value,
        provider=candidate.provider,
        provider_field=candidate.provider_field,
        source_observation_identity=candidate.source_observation_id,
        transformation=candidate.transformation,
        quarter_match_mode=candidate.quarter_match_mode,
        quarter_match_evidence=candidate.quarter_match_evidence,
        risk_validation_tier=candidate.validation_tier,
        eligible=eligible,
        rejection_reason=reason,
        action="ACCEPTED_FOR_NULL_FILL" if eligible else "REJECTED",
    )


def _candidate_set_fills_all_missing_core(
    evaluations: list[ProviderEvaluation], work_unit: SelectedV2WorkUnitInput, missing_core: list[str]
) -> bool:
    fields = set()
    for evaluation in evaluations:
        for candidate in evaluation.candidates:
            if _evaluate_candidate(candidate, work_unit).eligible and candidate.field in missing_core:
                fields.add(candidate.field)
    return set(missing_core).issubset(fields)


def _ensure_selected_quarter(conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput, now: str) -> tuple[int, bool]:
    existing = _get_exact_quarter(conn, work_unit)
    if existing is not None:
        return int(existing["quarter_id"]), False
    ambiguous = conn.execute(
        """
        SELECT quarter_id, report_date FROM rc_v2_quarter
        WHERE company_id=? AND fiscal_year=? AND fiscal_period=? AND report_date<>?
        """,
        (work_unit.company_id, work_unit.fiscal_year, work_unit.fiscal_quarter, work_unit.canonical_report_date),
    ).fetchall()
    if ambiguous:
        raise RuntimeError("V2_WORK_UNIT_AMBIGUOUS_QUARTER_IDENTITY")
    columns = _table_columns(conn, "rc_v2_quarter")
    payload = {
        "company_id": work_unit.company_id,
        "fiscal_year": work_unit.fiscal_year,
        "fiscal_period": work_unit.fiscal_quarter,
        "report_date": work_unit.canonical_report_date,
        "quarter_identity_source": "9H2_SELECTED_WORK_UNIT",
        "has_income": 0,
        "has_balance": 0,
        "has_cashflow": 0,
        "created_at_utc": now,
        "updated_at_utc": now,
    }
    _insert_dynamic(conn, "rc_v2_quarter", payload, columns)
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0]), True


def _ensure_fundamental_shell(conn: sqlite3.Connection, quarter_id: int, now: str) -> bool:
    if _get_fundamental(conn, quarter_id) is not None:
        return False
    columns = _table_columns(conn, "rc_v2_fundamental_quarterly")
    payload = {
        "quarter_id": quarter_id,
        "available_canonical_field_count": 0,
        "has_income": 0,
        "has_balance": 0,
        "has_cashflow": 0,
        "seed_status": "9H2_SELECTED_WORK_UNIT_SHELL",
        "missing_seed_fields_json": "[]",
        "created_at_utc": now,
        "updated_at_utc": now,
    }
    _insert_dynamic(conn, "rc_v2_fundamental_quarterly", payload, columns)
    return True


def _ensure_import_run(conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput, now: str) -> None:
    if "rc_v2_import_run" not in _tables(conn):
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO rc_v2_import_run
        (import_run_id, market, simfin_dir, builder_version, started_at_utc, finished_at_utc)
        VALUES (?, ?, '', '9H2_SELECTED_WORK_UNIT_EXECUTOR', ?, ?)
        """,
        (work_unit.run_id, work_unit.market, now, now),
    )


def _insert_provenance(conn: sqlite3.Connection, quarter_id: int, candidate: FieldCandidate, run_id: str, now: str) -> bool:
    before = conn.total_changes
    source_value = json.dumps(
        {
            "candidate_value": candidate.value,
            "source_observation_id": candidate.source_observation_id,
            "validation_tier": candidate.validation_tier,
            "quarter_match_evidence": candidate.quarter_match_evidence,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
            quarter_id, field_name, provider, provider_field, source_dataset, source_file,
            source_file_sha256, transformation, source_value, import_run_id, created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            quarter_id,
            candidate.field,
            candidate.provider,
            candidate.provider_field,
            candidate.source_dataset,
            candidate.source_file,
            candidate.source_file_sha256,
            candidate.transformation,
            source_value,
            run_id,
            now,
        ),
    )
    return conn.total_changes > before


def _write_canonical_field(conn: sqlite3.Connection, quarter_id: int, field_name: str, value: float | int, now: str) -> None:
    conn.execute(
        f"""
        UPDATE rc_v2_fundamental_quarterly
        SET {field_name}=?, available_canonical_field_count=available_canonical_field_count+1, updated_at_utc=?
        WHERE quarter_id=? AND {field_name} IS NULL
        """,
        (value, now, quarter_id),
    )


def _get_exact_quarter(conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput) -> sqlite3.Row | None:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT quarter_id FROM rc_v2_quarter
        WHERE company_id=? AND fiscal_year=? AND fiscal_period=? AND report_date=?
        """,
        (work_unit.company_id, work_unit.fiscal_year, work_unit.fiscal_quarter, work_unit.canonical_report_date),
    ).fetchone()


def _get_fundamental(conn: sqlite3.Connection, quarter_id: int) -> sqlite3.Row | None:
    conn.row_factory = sqlite3.Row
    return conn.execute("SELECT * FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (quarter_id,)).fetchone()


def _canonical_fields(row: sqlite3.Row | None) -> dict[str, float | None]:
    if row is None:
        return {field_name: None for field_name in FIELD_WRITE_SET}
    return {field_name: row[field_name] for field_name in FIELD_WRITE_SET if field_name in row.keys()}


def _current_field_value(conn: sqlite3.Connection, quarter_id: int, field_name: str) -> float | None:
    return conn.execute(f"SELECT {field_name} FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (quarter_id,)).fetchone()[0]


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}


def _insert_dynamic(conn: sqlite3.Connection, table_name: str, payload: Mapping[str, Any], columns: set[str]) -> None:
    values = {key: value for key, value in payload.items() if key in columns}
    names = list(values.keys())
    placeholders = ", ".join("?" for _ in names)
    conn.execute(
        f"INSERT INTO {table_name} ({', '.join(names)}) VALUES ({placeholders})",
        tuple(values[name] for name in names),
    )


def _same_value(left: Any, right: Any) -> bool:
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def _matching_candidate_result(candidate: FieldCandidate, results: list[FieldCandidateResult]) -> FieldCandidateResult:
    for result in results:
        if (
            result.field == candidate.field
            and result.provider == candidate.provider
            and result.source_observation_identity == candidate.source_observation_id
        ):
            return result
    raise RuntimeError("V2_WORK_UNIT_CANDIDATE_RESULT_MISSING")


def _next_retry_at(evaluations: list[ProviderEvaluation]) -> str | None:
    values = [item.next_retry_at for item in evaluations if item.next_retry_at]
    return min(values) if values else None


def _retry_reason(evaluations: list[ProviderEvaluation], failures: list[dict[str, str]], core_complete_after: bool) -> str | None:
    if core_complete_after:
        return None
    if failures:
        return "PROVIDER_TRANSIENT_FAILURE"
    if any(item.no_data for item in evaluations):
        return "PROVIDER_NO_DATA"
    if any(item.next_retry_at for item in evaluations):
        return "PROVIDER_BACKOFF"
    return None


def _quarter_value_counts(conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput) -> dict[str, int]:
    target = _get_exact_quarter(conn, work_unit)
    target_id = int(target["quarter_id"]) if target is not None else None
    target_non_null = 0
    non_target_non_null = 0
    for row in conn.execute("SELECT * FROM rc_v2_fundamental_quarterly").fetchall():
        count = sum(row[field_name] is not None for field_name in FIELD_WRITE_SET if field_name in row.keys())
        if target_id is not None and int(row["quarter_id"]) == target_id:
            target_non_null += count
        else:
            non_target_non_null += count
    return {"target_non_null": target_non_null, "non_target_non_null": non_target_non_null}
