from __future__ import annotations

import csv
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping, Protocol

from swingmaster.fundamentals.quarter_refresh_decision import (
    DECISION_FETCH_NEW_QUARTER,
    DECISION_REFRESH_SEC_CONFIRMATION,
    DECISION_RETRY_FETCH_FAILED,
    DECISION_RETRY_PARTIAL_QUARTER,
)
from swingmaster.fundamentals.result_check import (
    CHECK_STATUS_SUCCESS,
    EXECUTABLE_DECISIONS,
    PLAN_VERSION,
    validate_candidate_hash,
    validate_temp_path,
)


OPERATIONAL_FLOOR_YEAR = 2025
OPERATIONAL_FLOOR_QUARTER = "Q1"
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "shares_outstanding")
OPPORTUNISTIC_FIELDS = ("cash", "total_debt", "operating_cashflow", "capex", "ebit")
LEGACY_MEANINGFUL_FIELDS = CORE_FIELDS + OPPORTUNISTIC_FIELDS

LEGACY_UPDATE_MISSING_TARGET = "UPDATE_MISSING_TARGET"
LEGACY_RETRY_OR_UPDATE_TARGET = "RETRY_OR_UPDATE_TARGET"
LEGACY_REFRESH_SEC_CONFIRMATION = "REFRESH_SEC_CONFIRMATION"
LEGACY_NOOP = "NOOP"
LEGACY_OUT_OF_OPERATIONAL_SCOPE = "OUT_OF_OPERATIONAL_SCOPE"
LEGACY_BLOCKED = "BLOCKED"

V2_CREATE_QUARTER_AND_FILL_CORE = "CREATE_QUARTER_AND_FILL_CORE"
V2_ENRICH_CORE = "ENRICH_CORE"
V2_RETRY_PROVIDER = "RETRY_PROVIDER"
V2_NOOP_CORE_CURRENT = "NOOP_CORE_CURRENT"
V2_NOOP_SETTLED_INCOMPLETE = "NOOP_SETTLED_INCOMPLETE"
V2_MAINTENANCE_REQUIRED = "MAINTENANCE_REQUIRED"
V2_DEFERRED_POLICY_UNSUPPORTED = "DEFERRED_POLICY_UNSUPPORTED"
V2_OUT_OF_OPERATIONAL_SCOPE = "OUT_OF_OPERATIONAL_SCOPE"

ACTIONABLE_PROVIDER_STATUSES = {
    "DUE_FOR_UPDATE_PROCESSING",
    "DUE_FOR_CONFIRMATION_OR_UPDATE_PROCESSING",
    "DUE_FOR_CONFIRMATION",
    "CACHE_AVAILABLE",
    "FOLLOWUP_RETRY_DUE",
}


@dataclass(frozen=True)
class WorkUnit:
    market: str
    ticker: str
    fiscal_year: int
    fiscal_quarter: str
    report_date: str
    target_period_end_date: str
    source_a: Mapping[str, Any] | None = None
    source_b: Mapping[str, Any] | None = None

    @property
    def key(self) -> str:
        return work_unit_key(self.market, self.ticker, self.fiscal_year, self.fiscal_quarter)


@dataclass(frozen=True)
class V2FollowupRecord:
    market: str
    ticker: str
    fiscal_year: int
    fiscal_quarter: str
    report_date: str = ""
    active: bool = True
    resolved: bool = False
    retry_required: bool = True
    due_at: str = ""
    reason: str = ""
    origin: str = "v2_followup"


@dataclass
class LegacyState:
    market: str
    ticker: str
    target_period_end_date: str
    target_fiscal_year: int
    target_fiscal_quarter: str
    row_present: bool
    meaningful_row_present: bool
    ingestion_status: str | None
    quarter_basic_complete: bool
    source_confirmation_status: str | None
    retry_recommendation: str | None
    missing_basic_fields: list[str]
    latest_present_quarter: str | None
    latest_operationally_complete_quarter: str | None
    latest_sec_confirmed_quarter: str | None
    latest_retry_pending_quarter: str | None
    legacy_action: str
    legacy_blocker: str | None
    reason: str


@dataclass
class V2State:
    market: str
    ticker: str
    company_present: bool
    company_id: int | None
    company_profile: str | None
    target_fiscal_year: int
    target_fiscal_quarter: str
    target_report_date: str
    latest_structure_quarter: str | None
    latest_present_quarter: str | None
    latest_core_complete_quarter: str | None
    target_quarter_present: bool
    target_quarter_id: int | None
    target_fundamental_present: bool
    core_presence: dict[str, bool]
    opportunistic_presence: dict[str, bool]
    provider_due_summary: dict[str, str]
    active_followup_summary: dict[str, Any] | None
    core_complete: bool
    core_update_required: bool
    opportunistic_gaps: list[str]
    v2_action: str
    v2_blocker: str | None
    retry_required: bool
    maintenance_required: bool
    deferred_reason: str | None
    reason: str


@dataclass
class WorkUnitPreflight:
    work_unit_key: str
    market: str
    ticker: str
    target_fiscal_year: int
    target_fiscal_quarter: str
    target_report_date: str
    target_period_end_date: str
    source_a_selected: bool
    source_b_selected: bool
    legacy_state: LegacyState
    v2_state: V2State


@dataclass
class PreflightResult:
    plan_candidate_hash: str
    execution_scope_hash: str
    source_a_count: int
    source_b_due_count: int
    source_b_only_count: int
    source_overlap_count: int
    merged_work_unit_count: int
    duplicate_merge_count: int
    floor_excluded_count: int
    provider_calls: int
    writes: int
    work_units: list[WorkUnitPreflight] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        legacy_counts: dict[str, int] = {}
        v2_counts: dict[str, int] = {}
        provider_buckets: dict[str, int] = {}
        for row in self.work_units:
            legacy_counts[row.legacy_state.legacy_action] = legacy_counts.get(row.legacy_state.legacy_action, 0) + 1
            v2_counts[row.v2_state.v2_action] = v2_counts.get(row.v2_state.v2_action, 0) + 1
            for status in row.v2_state.provider_due_summary.values():
                provider_buckets[status] = provider_buckets.get(status, 0) + 1
        return {
            "plan_candidate_hash": self.plan_candidate_hash,
            "execution_scope_hash": self.execution_scope_hash,
            "source_a_count": self.source_a_count,
            "source_b_due_count": self.source_b_due_count,
            "source_b_only_count": self.source_b_only_count,
            "source_overlap_count": self.source_overlap_count,
            "merged_work_unit_count": self.merged_work_unit_count,
            "duplicate_merge_count": self.duplicate_merge_count,
            "floor_excluded_count": self.floor_excluded_count,
            "provider_calls": self.provider_calls,
            "writes": self.writes,
            "legacy_action_counts": legacy_counts,
            "v2_action_counts": v2_counts,
            "provider_due_buckets": provider_buckets,
        }


class V2FollowupRepository(Protocol):
    def list_due_v2_followups(self, *, as_of: date, floor_year: int = OPERATIONAL_FLOOR_YEAR) -> list[V2FollowupRecord]:
        ...

    def get_v2_followup(self, work_unit_key: str) -> V2FollowupRecord | None:
        ...


class EmptyV2FollowupRepository:
    def list_due_v2_followups(self, *, as_of: date, floor_year: int = OPERATIONAL_FLOOR_YEAR) -> list[V2FollowupRecord]:
        return []

    def get_v2_followup(self, work_unit_key: str) -> V2FollowupRecord | None:
        return None


class InMemoryV2FollowupRepository:
    def __init__(self, rows: list[V2FollowupRecord]) -> None:
        self._rows = rows

    def list_due_v2_followups(self, *, as_of: date, floor_year: int = OPERATIONAL_FLOOR_YEAR) -> list[V2FollowupRecord]:
        return [row for row in self._rows if followup_is_due(row, as_of=as_of, floor_year=floor_year)]

    def get_v2_followup(self, work_unit_key: str) -> V2FollowupRecord | None:
        for row in self._rows:
            if work_unit_key(row.market, row.ticker, row.fiscal_year, row.fiscal_quarter) == work_unit_key:
                return row
        return None


class SQLiteV2FollowupRepository:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def list_due_v2_followups(self, *, as_of: date, floor_year: int = OPERATIONAL_FLOOR_YEAR) -> list[V2FollowupRecord]:
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            if not _table_exists(conn, "rc_v2_operational_followup"):
                return []
            rows = conn.execute(
                """
                SELECT market, ticker, fiscal_year, fiscal_quarter, canonical_report_date,
                       active, resolved_at, retry_required, next_retry_at, next_check_at, followup_reason
                FROM rc_v2_operational_followup
                WHERE active=1
                  AND retry_required=1
                  AND fiscal_year>=?
                  AND (next_retry_at IS NULL OR next_retry_at='' OR substr(next_retry_at, 1, 10)<=?)
                  AND (next_check_at IS NULL OR next_check_at='' OR substr(next_check_at, 1, 10)<=?)
                  AND (maintenance_required IS NULL OR maintenance_required=0)
                  AND (deferred_reason IS NULL OR deferred_reason='')
                  AND (resolved_at IS NULL OR resolved_at='')
                ORDER BY lower(market), upper(ticker), fiscal_year, fiscal_quarter
                """,
                (floor_year, as_of.isoformat(), as_of.isoformat()),
            ).fetchall()
        followups = [
            V2FollowupRecord(
                market=str(row["market"]),
                ticker=str(row["ticker"]),
                fiscal_year=int(row["fiscal_year"]),
                fiscal_quarter=str(row["fiscal_quarter"]),
                report_date=str(row["canonical_report_date"] or ""),
                active=bool(int(row["active"] or 0)),
                resolved=bool(row["resolved_at"]),
                retry_required=bool(int(row["retry_required"] or 0)),
                due_at=str(row["next_retry_at"] or row["next_check_at"] or ""),
                reason=str(row["followup_reason"] or ""),
                origin="sqlite_v2_operational_followup",
            )
            for row in rows
        ]
        return [row for row in followups if followup_is_due(row, as_of=as_of, floor_year=floor_year)]

    def get_v2_followup(self, work_unit_key: str) -> V2FollowupRecord | None:
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            if not _table_exists(conn, "rc_v2_operational_followup"):
                return None
            row = conn.execute(
                """
                SELECT market, ticker, fiscal_year, fiscal_quarter, canonical_report_date,
                       active, resolved_at, retry_required, next_retry_at, next_check_at, followup_reason
                FROM rc_v2_operational_followup
                WHERE work_unit_key=?
                """,
                (work_unit_key,),
            ).fetchone()
        if row is None:
            return None
        return V2FollowupRecord(
            market=str(row["market"]),
            ticker=str(row["ticker"]),
            fiscal_year=int(row["fiscal_year"]),
            fiscal_quarter=str(row["fiscal_quarter"]),
            report_date=str(row["canonical_report_date"] or ""),
            active=bool(int(row["active"] or 0)),
            resolved=bool(row["resolved_at"]),
            retry_required=bool(int(row["retry_required"] or 0)),
            due_at=str(row["next_retry_at"] or row["next_check_at"] or ""),
            reason=str(row["followup_reason"] or ""),
            origin="sqlite_v2_operational_followup",
        )


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def open_readonly_sqlite(path: Path) -> sqlite3.Connection:
    resolved = path.resolve()
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def normalize_market(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_fiscal_quarter(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"1", "2", "3", "4"}:
        return f"Q{text}"
    if text in {"Q1", "Q2", "Q3", "Q4"}:
        return text
    raise ValueError(f"UNSUPPORTED_FISCAL_QUARTER:{value}")


def quarter_sort_key(fiscal_year: int, fiscal_quarter: str) -> tuple[int, int]:
    quarter = normalize_fiscal_quarter(fiscal_quarter)
    return int(fiscal_year), int(quarter[1:])


def quarter_label(fiscal_year: int, fiscal_quarter: str) -> str:
    return f"{int(fiscal_year)}{normalize_fiscal_quarter(fiscal_quarter)}"


def is_at_or_after_operational_floor(fiscal_year: int, fiscal_quarter: str) -> bool:
    return quarter_sort_key(fiscal_year, fiscal_quarter) >= quarter_sort_key(OPERATIONAL_FLOOR_YEAR, OPERATIONAL_FLOOR_QUARTER)


def work_unit_key(market: Any, ticker: Any, fiscal_year: int, fiscal_quarter: str) -> str:
    return "|".join([normalize_market(market), normalize_ticker(ticker), str(int(fiscal_year)), normalize_fiscal_quarter(fiscal_quarter)])


def load_validated_source_a_plan(
    *,
    plan_path: Path,
    db_path: Path,
    execution_decision_date: str | date,
    ticker: str | None = None,
    limit: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    resolved = validate_temp_path(plan_path, must_exist=True)
    plan = json.loads(resolved.read_text(encoding="utf-8"))
    validate_plan(plan, db_path=db_path, execution_decision_date=execution_decision_date)
    rows = [dict(row) for row in plan.get("candidates", [])]
    if ticker is not None:
        rows = [row for row in rows if normalize_ticker(row.get("ticker")) == ticker.strip().upper()]
    rows.sort(key=lambda row: (normalize_ticker(row.get("ticker")), str(row.get("target_period_end_date") or "")))
    if limit is not None:
        rows = rows[:limit]
    return plan, rows


def validate_plan(plan: Mapping[str, Any], *, db_path: Path, execution_decision_date: str | date) -> None:
    if plan.get("plan_version") != PLAN_VERSION:
        raise RuntimeError("INVALID_RESULT_CHECK_PLAN_VERSION")
    if plan.get("check_status") != CHECK_STATUS_SUCCESS:
        raise RuntimeError("RESULT_CHECK_PLAN_NOT_SUCCESS")
    if str(plan.get("fundamentals_db")) != str(db_path.resolve()):
        raise RuntimeError("RESULT_CHECK_PLAN_DB_MISMATCH")
    created_at = str(plan.get("created_at_utc") or "")
    if not created_at:
        raise RuntimeError("RESULT_CHECK_PLAN_CREATED_AT_REQUIRED")
    datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    if date.fromisoformat(str(plan.get("decision_date"))) != _parse_date(execution_decision_date):
        raise RuntimeError("STALE_RESULT_CHECK_PLAN")
    rows = [dict(row) for row in plan.get("candidates", [])]
    if int(plan.get("candidate_count") or 0) != len(rows):
        raise RuntimeError("RESULT_CHECK_PLAN_CANDIDATE_COUNT_MISMATCH")
    if not validate_candidate_hash(plan):
        raise RuntimeError("RESULT_CHECK_PLAN_CANDIDATE_HASH_MISMATCH")
    seen: set[tuple[str, str]] = set()
    for row in rows:
        ticker = normalize_ticker(row.get("ticker"))
        period = str(row.get("target_period_end_date") or "")
        if not ticker or not period:
            raise RuntimeError("RESULT_CHECK_PLAN_TARGET_PERIOD_REQUIRED")
        key = (ticker, period)
        if key in seen:
            raise RuntimeError(f"RESULT_CHECK_PLAN_DUPLICATE_CANDIDATE:{ticker},{period}")
        seen.add(key)
        if normalize_market(row.get("market")) != "usa":
            raise RuntimeError("RESULT_CHECK_PLAN_NON_USA_ROW")
        if str(row.get("decision")) not in EXECUTABLE_DECISIONS:
            raise RuntimeError("RESULT_CHECK_PLAN_NON_EXECUTABLE_DECISION")
        if int(row.get("fundamental_fetch_enabled") or 0) != 1:
            raise RuntimeError("RESULT_CHECK_PLAN_INACTIVE_ROW")
        if int(row.get("eligible_for_execution") or 0) != 1:
            raise RuntimeError("RESULT_CHECK_PLAN_ROW_NOT_EXECUTION_ELIGIBLE")


def normalize_source_a(row: Mapping[str, Any]) -> WorkUnit:
    fiscal_year = int(row.get("canonical_fiscal_year") or 0)
    fiscal_quarter = normalize_fiscal_quarter(row.get("canonical_fiscal_quarter"))
    report_date = str(row.get("canonical_report_date") or row.get("target_period_end_date") or "")
    period = str(row.get("target_period_end_date") or report_date)
    return WorkUnit(
        market=normalize_market(row.get("market")),
        ticker=normalize_ticker(row.get("ticker")),
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        report_date=report_date,
        target_period_end_date=period,
        source_a=dict(row),
    )


def followup_is_due(row: V2FollowupRecord, *, as_of: date, floor_year: int = OPERATIONAL_FLOOR_YEAR) -> bool:
    if not row.active or row.resolved or not row.retry_required:
        return False
    if row.fiscal_year < floor_year or not is_at_or_after_operational_floor(row.fiscal_year, row.fiscal_quarter):
        return False
    if not normalize_market(row.market) or not normalize_ticker(row.ticker):
        return False
    if not row.due_at:
        return True
    return date.fromisoformat(row.due_at[:10]) <= as_of


def source_b_work_units(repo: V2FollowupRepository, *, as_of: date) -> list[WorkUnit]:
    rows = []
    for followup in repo.list_due_v2_followups(as_of=as_of, floor_year=OPERATIONAL_FLOOR_YEAR):
        if not followup_is_due(followup, as_of=as_of):
            continue
        rows.append(
            WorkUnit(
                market=normalize_market(followup.market),
                ticker=normalize_ticker(followup.ticker),
                fiscal_year=followup.fiscal_year,
                fiscal_quarter=normalize_fiscal_quarter(followup.fiscal_quarter),
                report_date=followup.report_date,
                target_period_end_date=followup.report_date,
                source_b=asdict(followup),
            )
        )
    return rows


def merge_work_units(source_a_rows: list[WorkUnit], source_b_rows: list[WorkUnit]) -> tuple[list[WorkUnit], int]:
    merged: dict[str, WorkUnit] = {}
    duplicate_count = 0
    for row in source_a_rows:
        merged[row.key] = row
    for row in source_b_rows:
        existing = merged.get(row.key)
        if existing is None:
            merged[row.key] = row
            continue
        duplicate_count += 1
        merged[row.key] = WorkUnit(
            market=existing.market,
            ticker=existing.ticker,
            fiscal_year=existing.fiscal_year,
            fiscal_quarter=existing.fiscal_quarter,
            report_date=existing.report_date,
            target_period_end_date=existing.target_period_end_date,
            source_a=existing.source_a,
            source_b=row.source_b,
        )
    return sorted(merged.values(), key=lambda row: (row.market, row.ticker, row.fiscal_year, row.fiscal_quarter)), duplicate_count


def execution_scope_hash(rows: list[WorkUnit]) -> str:
    payload = [
        {
            "work_unit_key": row.key,
            "market": row.market,
            "ticker": row.ticker,
            "fiscal_year": row.fiscal_year,
            "fiscal_quarter": row.fiscal_quarter,
            "report_date": row.report_date,
            "source_a_selected": row.source_a is not None,
            "source_b_selected": row.source_b is not None,
        }
        for row in sorted(rows, key=lambda item: item.key)
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()


def inspect_legacy_state(conn: sqlite3.Connection, work_unit: WorkUnit) -> LegacyState:
    target = conn.execute(
        """
        SELECT q.*, s.ingestion_status, s.quarter_basic_complete, s.retry_recommendation,
               s.source_confirmation_status, s.missing_core_fields_json
        FROM rc_fundamental_quarterly q
        LEFT JOIN rc_fundamental_quarter_ingestion_status s
          ON s.ticker=q.ticker AND lower(s.market)=? AND s.period_end_date=q.period_end_date
        WHERE q.ticker=? AND q.period_end_date=?
        """,
        (work_unit.market, work_unit.ticker, work_unit.target_period_end_date),
    ).fetchone()
    status_only = None
    if target is None:
        status_only = conn.execute(
            """
            SELECT ingestion_status, quarter_basic_complete, retry_recommendation,
                   source_confirmation_status, missing_core_fields_json
            FROM rc_fundamental_quarter_ingestion_status
            WHERE ticker=? AND lower(market)=? AND period_end_date=?
            """,
            (work_unit.ticker, work_unit.market, work_unit.target_period_end_date),
        ).fetchone()

    row_present = target is not None
    meaningful = row_present and any(target[field] is not None for field in LEGACY_MEANINGFUL_FIELDS if field in target.keys())
    state_row = target or status_only
    ingestion_status = _row_value(state_row, "ingestion_status")
    retry_recommendation = _row_value(state_row, "retry_recommendation")
    source_confirmation_status = _row_value(state_row, "source_confirmation_status")
    quarter_basic_complete = bool(int(_row_value(state_row, "quarter_basic_complete") or 0))
    missing_basic_fields = _json_list(_row_value(state_row, "missing_core_fields_json"))

    latest_present, latest_complete, latest_confirmed, latest_retry = _legacy_watermarks(conn, work_unit)
    if not is_at_or_after_operational_floor(work_unit.fiscal_year, work_unit.fiscal_quarter):
        action = LEGACY_OUT_OF_OPERATIONAL_SCOPE
        blocker = "PRE_2025_Q1"
        reason = "Target is outside the operational floor."
    elif work_unit.source_a and str(work_unit.source_a.get("decision")) == DECISION_REFRESH_SEC_CONFIRMATION:
        action = LEGACY_REFRESH_SEC_CONFIRMATION
        blocker = None
        reason = "Source A selected a SEC confirmation refresh."
    elif not meaningful:
        action = LEGACY_UPDATE_MISSING_TARGET
        blocker = None
        reason = "Legacy target row is missing or is an empty shell."
    elif _retry_is_pending(retry_recommendation, ingestion_status, source_confirmation_status):
        action = LEGACY_RETRY_OR_UPDATE_TARGET
        blocker = None
        reason = "Legacy target has retryable incomplete ingestion state."
    elif quarter_basic_complete or str(ingestion_status or "") in {"QUARTER_BASIC_COMPLETE", "INGEST_COMPLETE"}:
        action = LEGACY_NOOP
        blocker = None
        reason = "Legacy target is operationally complete."
    else:
        action = LEGACY_RETRY_OR_UPDATE_TARGET
        blocker = None
        reason = "Legacy target is present but not operationally complete."

    return LegacyState(
        market=work_unit.market,
        ticker=work_unit.ticker,
        target_period_end_date=work_unit.target_period_end_date,
        target_fiscal_year=work_unit.fiscal_year,
        target_fiscal_quarter=work_unit.fiscal_quarter,
        row_present=row_present,
        meaningful_row_present=meaningful,
        ingestion_status=ingestion_status,
        quarter_basic_complete=quarter_basic_complete,
        source_confirmation_status=source_confirmation_status,
        retry_recommendation=retry_recommendation,
        missing_basic_fields=missing_basic_fields,
        latest_present_quarter=latest_present,
        latest_operationally_complete_quarter=latest_complete,
        latest_sec_confirmed_quarter=latest_confirmed,
        latest_retry_pending_quarter=latest_retry,
        legacy_action=action,
        legacy_blocker=blocker,
        reason=reason,
    )


def inspect_v2_state(conn: sqlite3.Connection, work_unit: WorkUnit) -> V2State:
    company = _fetch_company(conn, work_unit.market, work_unit.ticker)
    base_provider_due = _provider_due_from_work_unit(conn, work_unit)
    followup = dict(work_unit.source_b) if work_unit.source_b is not None else None
    if followup is not None:
        base_provider_due["followup"] = "FOLLOWUP_RETRY_DUE"

    if not is_at_or_after_operational_floor(work_unit.fiscal_year, work_unit.fiscal_quarter):
        return _blocked_v2_state(work_unit, base_provider_due, followup, V2_OUT_OF_OPERATIONAL_SCOPE, "PRE_2025_Q1", False)
    if company is None:
        return _blocked_v2_state(work_unit, base_provider_due, followup, V2_MAINTENANCE_REQUIRED, "V2_COMPANY_MISSING", True)

    company_id = int(company["company_id"])
    profile = str(company["company_profile"] or "")
    if profile.upper() in {"BANK", "INSURANCE"}:
        state = _blocked_v2_state(work_unit, base_provider_due, followup, V2_DEFERRED_POLICY_UNSUPPORTED, "UNSUPPORTED_COMPANY_PROFILE", False)
        state.company_present = True
        state.company_id = company_id
        state.company_profile = profile
        return state

    watermarks = _v2_watermarks(conn, company_id)
    quarter = _fetch_target_quarter(conn, company_id, work_unit)
    fundamental = _fetch_fundamental(conn, int(quarter["quarter_id"])) if quarter is not None else None
    core_presence = {field_name: bool(fundamental is not None and fundamental[field_name] is not None) for field_name in CORE_FIELDS}
    opportunistic_presence = {
        field_name: bool(fundamental is not None and fundamental[field_name] is not None) for field_name in OPPORTUNISTIC_FIELDS
    }
    core_complete = all(core_presence.values())
    provider_actionable = any(status in ACTIONABLE_PROVIDER_STATUSES for status in base_provider_due.values())
    retry_required = followup is not None
    if quarter is None or fundamental is None:
        action = V2_CREATE_QUARTER_AND_FILL_CORE
        core_update_required = True
        reason = "V2 target structure or fundamental row is missing."
    elif core_complete:
        action = V2_NOOP_CORE_CURRENT
        core_update_required = False
        reason = "V2 CORE is complete for the target quarter."
    elif retry_required:
        action = V2_RETRY_PROVIDER
        core_update_required = True
        reason = "Persisted V2 follow-up is due for retry."
    elif provider_actionable:
        action = V2_ENRICH_CORE
        core_update_required = True
        reason = "V2 CORE is incomplete and a read-only provider/cache due signal exists."
    else:
        action = V2_NOOP_SETTLED_INCOMPLETE
        core_update_required = False
        reason = "V2 CORE is incomplete but no eligible due signal exists."

    return V2State(
        market=work_unit.market,
        ticker=work_unit.ticker,
        company_present=True,
        company_id=company_id,
        company_profile=profile,
        target_fiscal_year=work_unit.fiscal_year,
        target_fiscal_quarter=work_unit.fiscal_quarter,
        target_report_date=work_unit.report_date,
        latest_structure_quarter=watermarks["latest_structure_quarter"],
        latest_present_quarter=watermarks["latest_present_quarter"],
        latest_core_complete_quarter=watermarks["latest_core_complete_quarter"],
        target_quarter_present=quarter is not None,
        target_quarter_id=None if quarter is None else int(quarter["quarter_id"]),
        target_fundamental_present=fundamental is not None,
        core_presence=core_presence,
        opportunistic_presence=opportunistic_presence,
        provider_due_summary=base_provider_due,
        active_followup_summary=followup,
        core_complete=core_complete,
        core_update_required=core_update_required,
        opportunistic_gaps=[field_name for field_name, present in opportunistic_presence.items() if not present],
        v2_action=action,
        v2_blocker=None,
        retry_required=retry_required,
        maintenance_required=False,
        deferred_reason=None,
        reason=reason,
    )


def run_dual_store_preflight(
    *,
    plan_path: Path,
    legacy_db_path: Path,
    v2_db_path: Path,
    execution_decision_date: str | date,
    output_root: Path | None = None,
    ticker: str | None = None,
    limit: int | None = None,
    followup_repository: V2FollowupRepository | None = None,
) -> PreflightResult:
    plan, source_a_plan_rows = load_validated_source_a_plan(
        plan_path=plan_path,
        db_path=legacy_db_path,
        execution_decision_date=execution_decision_date,
        ticker=ticker,
        limit=limit,
    )
    as_of = _parse_date(execution_decision_date)
    source_a_rows = [normalize_source_a(row) for row in source_a_plan_rows]
    repo = followup_repository or EmptyV2FollowupRepository()
    source_b_rows = source_b_work_units(repo, as_of=as_of)
    floor_excluded_count = sum(
        1
        for row in [*source_a_rows, *source_b_rows]
        if not is_at_or_after_operational_floor(row.fiscal_year, row.fiscal_quarter)
    )
    source_a_rows = [row for row in source_a_rows if is_at_or_after_operational_floor(row.fiscal_year, row.fiscal_quarter)]
    source_b_rows = [row for row in source_b_rows if is_at_or_after_operational_floor(row.fiscal_year, row.fiscal_quarter)]
    merged_rows, duplicate_count = merge_work_units(source_a_rows, source_b_rows)
    source_a_keys = {row.key for row in source_a_rows}
    source_b_keys = {row.key for row in source_b_rows}
    with open_readonly_sqlite(legacy_db_path) as legacy_conn, open_readonly_sqlite(v2_db_path) as v2_conn:
        work_units = [
            WorkUnitPreflight(
                work_unit_key=row.key,
                market=row.market,
                ticker=row.ticker,
                target_fiscal_year=row.fiscal_year,
                target_fiscal_quarter=row.fiscal_quarter,
                target_report_date=row.report_date,
                target_period_end_date=row.target_period_end_date,
                source_a_selected=row.source_a is not None,
                source_b_selected=row.source_b is not None,
                legacy_state=inspect_legacy_state(legacy_conn, row),
                v2_state=inspect_v2_state(v2_conn, row),
            )
            for row in merged_rows
        ]
    result = PreflightResult(
        plan_candidate_hash=str(plan["candidate_hash"]),
        execution_scope_hash=execution_scope_hash(merged_rows),
        source_a_count=len(source_a_rows),
        source_b_due_count=len(source_b_rows),
        source_b_only_count=len(source_b_keys - source_a_keys),
        source_overlap_count=len(source_a_keys & source_b_keys),
        merged_work_unit_count=len(merged_rows),
        duplicate_merge_count=duplicate_count,
        floor_excluded_count=floor_excluded_count,
        provider_calls=0,
        writes=0,
        work_units=work_units,
    )
    if output_root is not None:
        write_preflight_artifacts(result, output_root)
    return result


def write_preflight_artifacts(result: PreflightResult, output_root: Path) -> None:
    root = validate_temp_path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    (root / "summary.json").write_text(json.dumps(result.summary(), indent=2, sort_keys=True), encoding="utf-8")
    rows = [_flatten_work_unit(row) for row in result.work_units]
    (root / "work_units.json").write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    with (root / "work_units.csv").open("w", newline="", encoding="utf-8") as fh:
        fieldnames = sorted(rows[0].keys()) if rows else ["work_unit_key"]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    replay_payload = {
        "summary": result.summary(),
        "work_units_sha256": hashlib.sha256(
            json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest(),
    }
    (root / "replay_signature.json").write_text(json.dumps(replay_payload, indent=2, sort_keys=True), encoding="utf-8")


def _blocked_v2_state(
    work_unit: WorkUnit,
    provider_due: dict[str, str],
    followup: dict[str, Any] | None,
    action: str,
    blocker: str,
    maintenance_required: bool,
) -> V2State:
    return V2State(
        market=work_unit.market,
        ticker=work_unit.ticker,
        company_present=False,
        company_id=None,
        company_profile=None,
        target_fiscal_year=work_unit.fiscal_year,
        target_fiscal_quarter=work_unit.fiscal_quarter,
        target_report_date=work_unit.report_date,
        latest_structure_quarter=None,
        latest_present_quarter=None,
        latest_core_complete_quarter=None,
        target_quarter_present=False,
        target_quarter_id=None,
        target_fundamental_present=False,
        core_presence={field_name: False for field_name in CORE_FIELDS},
        opportunistic_presence={field_name: False for field_name in OPPORTUNISTIC_FIELDS},
        provider_due_summary=provider_due,
        active_followup_summary=followup,
        core_complete=False,
        core_update_required=False,
        opportunistic_gaps=list(OPPORTUNISTIC_FIELDS),
        v2_action=action,
        v2_blocker=blocker,
        retry_required=followup is not None,
        maintenance_required=maintenance_required,
        deferred_reason=blocker,
        reason=blocker,
    )


def _legacy_watermarks(conn: sqlite3.Connection, work_unit: WorkUnit) -> tuple[str | None, str | None, str | None, str | None]:
    rows = conn.execute(
        """
        SELECT q.period_end_date, s.ingestion_status, s.quarter_basic_complete, s.retry_recommendation,
               s.source_confirmation_status,
               q.revenue, q.ebitda, q.free_cashflow, q.shares_outstanding,
               q.cash, q.total_debt, q.operating_cashflow, q.capex, q.ebit
        FROM rc_fundamental_quarterly q
        LEFT JOIN rc_fundamental_quarter_ingestion_status s
          ON s.ticker=q.ticker AND lower(s.market)=? AND s.period_end_date=q.period_end_date
        WHERE q.ticker=?
        ORDER BY q.period_end_date
        """,
        (work_unit.market, work_unit.ticker),
    ).fetchall()
    latest_present = latest_complete = latest_confirmed = latest_retry = None
    for row in rows:
        label = str(row["period_end_date"])
        meaningful = any(row[field_name] is not None for field_name in LEGACY_MEANINGFUL_FIELDS if field_name in row.keys())
        if meaningful:
            latest_present = label
        if meaningful and (bool(int(row["quarter_basic_complete"] or 0)) or str(row["ingestion_status"] or "") in {"QUARTER_BASIC_COMPLETE", "INGEST_COMPLETE"}):
            latest_complete = label
        if str(row["source_confirmation_status"] or "") in {"SEC_CONFIRMED", "SEC_CONFIRMED_YAHOO_ENRICHED"}:
            latest_confirmed = label
        if _retry_is_pending(row["retry_recommendation"], row["ingestion_status"], row["source_confirmation_status"]):
            latest_retry = label
    return latest_present, latest_complete, latest_confirmed, latest_retry


def _provider_due_from_work_unit(conn: sqlite3.Connection, work_unit: WorkUnit) -> dict[str, str]:
    providers_due = dict(work_unit.source_a.get("providers_due") or {}) if isinstance(work_unit.source_a, Mapping) else {}
    due: dict[str, str] = {str(provider): str(status) for provider, status in providers_due.items() if status}
    for table_name, provider in [
        ("rc_v2_simfin_api_fetch_state", "simfin_statements"),
        ("rc_v2_simfin_api_shares_fetch_state", "simfin_shares"),
    ]:
        if not _table_exists(conn, table_name):
            due.setdefault(provider, "NO_ELIGIBLE_PROVIDER")
            continue
        row = conn.execute(
            f"SELECT last_status, retry_after_utc FROM {table_name} WHERE lower(market)=? AND ticker=?",
            (work_unit.market, work_unit.ticker),
        ).fetchone()
        if row is None:
            due.setdefault(provider, "NO_DATA_NOT_DUE")
        elif str(row["last_status"]) == "SUCCESS":
            due.setdefault(provider, "CACHE_AVAILABLE")
        elif row["retry_after_utc"] and str(row["retry_after_utc"])[:10] > date.today().isoformat():
            due.setdefault(provider, "BACKOFF_NOT_DUE")
        else:
            due.setdefault(provider, "PROVIDER_DUE")
    return due


def _v2_watermarks(conn: sqlite3.Connection, company_id: int) -> dict[str, str | None]:
    rows = conn.execute(
        """
        SELECT q.fiscal_year, q.fiscal_period, f.revenue, f.ebitda, f.free_cashflow, f.shares_outstanding,
               f.cash, f.total_debt, f.operating_cashflow, f.capex, f.ebit
        FROM rc_v2_quarter q
        LEFT JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
        WHERE q.company_id=? AND q.fiscal_year>=?
        ORDER BY q.fiscal_year, q.fiscal_period
        """,
        (company_id, OPERATIONAL_FLOOR_YEAR),
    ).fetchall()
    latest_structure = latest_present = latest_core = None
    for row in rows:
        label = quarter_label(int(row["fiscal_year"]), str(row["fiscal_period"]))
        latest_structure = label
        if any(row[field_name] is not None for field_name in [*CORE_FIELDS, *OPPORTUNISTIC_FIELDS]):
            latest_present = label
        if all(row[field_name] is not None for field_name in CORE_FIELDS):
            latest_core = label
    return {
        "latest_structure_quarter": latest_structure,
        "latest_present_quarter": latest_present,
        "latest_core_complete_quarter": latest_core,
    }


def _fetch_company(conn: sqlite3.Connection, market: str, ticker: str) -> sqlite3.Row | None:
    if not _table_exists(conn, "rc_v2_company"):
        return None
    return conn.execute(
        "SELECT company_id, company_profile FROM rc_v2_company WHERE lower(market)=? AND upper(ticker)=? AND active=1",
        (market, ticker),
    ).fetchone()


def _fetch_target_quarter(conn: sqlite3.Connection, company_id: int, work_unit: WorkUnit) -> sqlite3.Row | None:
    if not _table_exists(conn, "rc_v2_quarter"):
        return None
    if work_unit.report_date:
        row = conn.execute(
            """
            SELECT quarter_id FROM rc_v2_quarter
            WHERE company_id=? AND fiscal_year=? AND fiscal_period=? AND report_date=?
            """,
            (company_id, work_unit.fiscal_year, work_unit.fiscal_quarter, work_unit.report_date),
        ).fetchone()
        if row is not None:
            return row
    return conn.execute(
        """
        SELECT quarter_id FROM rc_v2_quarter
        WHERE company_id=? AND fiscal_year=? AND fiscal_period=?
        ORDER BY report_date DESC
        LIMIT 1
        """,
        (company_id, work_unit.fiscal_year, work_unit.fiscal_quarter),
    ).fetchone()


def _fetch_fundamental(conn: sqlite3.Connection, quarter_id: int) -> sqlite3.Row | None:
    if not _table_exists(conn, "rc_v2_fundamental_quarterly"):
        return None
    return conn.execute("SELECT * FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (quarter_id,)).fetchone()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone() is not None


def _row_value(row: sqlite3.Row | None, key: str) -> Any:
    if row is None or key not in row.keys():
        return None
    return row[key]


def _json_list(value: Any) -> list[str]:
    if not value:
        return []
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]


def _retry_is_pending(retry_recommendation: Any, ingestion_status: Any, source_confirmation_status: Any) -> bool:
    retry = str(retry_recommendation or "")
    return (
        retry not in {"", "NO_ACTION", "NONE"}
        or str(ingestion_status or "") == "FETCH_FAILED"
        or str(source_confirmation_status or "") == "SEC_CONFIRMATION_FAILED_RETRYABLE"
    )


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _flatten_work_unit(row: WorkUnitPreflight) -> dict[str, Any]:
    return {
        "work_unit_key": row.work_unit_key,
        "market": row.market,
        "ticker": row.ticker,
        "target_fiscal_year": row.target_fiscal_year,
        "target_fiscal_quarter": row.target_fiscal_quarter,
        "target_report_date": row.target_report_date,
        "source_a_selected": int(row.source_a_selected),
        "source_b_selected": int(row.source_b_selected),
        "legacy_action": row.legacy_state.legacy_action,
        "legacy_blocker": row.legacy_state.legacy_blocker or "",
        "legacy_meaningful_row_present": int(row.legacy_state.meaningful_row_present),
        "v2_action": row.v2_state.v2_action,
        "v2_blocker": row.v2_state.v2_blocker or "",
        "v2_core_complete": int(row.v2_state.core_complete),
        "v2_core_update_required": int(row.v2_state.core_update_required),
        "v2_retry_required": int(row.v2_state.retry_required),
        "v2_maintenance_required": int(row.v2_state.maintenance_required),
        "provider_due_summary_json": json.dumps(row.v2_state.provider_due_summary, sort_keys=True),
        "opportunistic_gaps_json": json.dumps(row.v2_state.opportunistic_gaps, sort_keys=True),
    }
