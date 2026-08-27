from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_helpers import normalize_fiscal_quarter, normalize_market, normalize_ticker
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_fiscal_calendar import FiscalCalendarWriteCandidate, validate_canonical_write_candidate


PROVIDERS = {"YAHOO", "LEGACY", "V2", "SEC", "SIMFIN"}
PROVIDER_RESULTS = {"NOT_CHECKED", "ACQUIRED", "PARTIAL", "NO_DATA", "FAILED", "UNSUPPORTED"}
Q_LIFECYCLES = {"RESULT_DETECTED", "ENRICHING", "OPERATIONALLY_SETTLED"}
SEC_CONFIRMATION_STATES = {
    "NOT_APPLICABLE",
    "NOT_YET_EXPECTED",
    "PENDING",
    "CHECKED_NO_EVIDENCE",
    "PARTIAL_EVIDENCE",
    "CONFIRMED",
    "UNSUPPORTED",
    "ERROR_RETRY",
    "NOT_DERIVABLE",
}
ACTION_TYPES = {
    "CHECK_RESULT",
    "FETCH_INITIAL",
    "ENRICH_Q",
    "RETRY_PROVIDER",
    "CHECK_SEC",
    "BACKFILL_HISTORICAL",
    "MANUAL_REVIEW",
}
ACTION_STATUSES = {"ACTIVE", "DEFERRED", "BLOCKED", "RESOLVED", "CANCELLED"}
FUNDAMENTAL_FIELDS = (
    "revenue",
    "ebitda",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
    "ebit",
    "operating_income",
    "operating_cashflow",
    "capex",
    "gross_profit",
    "net_income",
)


@dataclass(frozen=True)
class UniverseCandidate:
    market: str
    ticker: str
    decision: str
    evidence_source: str
    evidence: str


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def configure_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class V3CompanyRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def derive_legacy_authority_universe(
        self,
        legacy_conn: sqlite3.Connection,
        *,
        v2_conn: sqlite3.Connection | None = None,
        market: str = "usa",
    ) -> list[UniverseCandidate]:
        legacy_conn.row_factory = sqlite3.Row
        normalized_market = normalize_market(market)
        legacy_tickers = [
            normalize_ticker(row["ticker"])
            for row in legacy_conn.execute(
                """
                SELECT DISTINCT ticker
                FROM rc_fundamental_quarterly
                WHERE ticker IS NOT NULL AND TRIM(ticker) <> ''
                ORDER BY UPPER(ticker)
                """
            )
        ]
        excluded_by_v2_profile: dict[str, str] = {}
        if v2_conn is not None:
            v2_conn.row_factory = sqlite3.Row
            for row in v2_conn.execute(
                """
                SELECT UPPER(ticker) AS ticker, company_profile
                FROM rc_v2_company
                WHERE market = ? AND ticker IS NOT NULL AND company_profile IN ('BANK', 'INSURANCE')
                """,
                (normalized_market,),
            ):
                excluded_by_v2_profile[normalize_ticker(row["ticker"])] = str(row["company_profile"])

        candidates: list[UniverseCandidate] = []
        for ticker in legacy_tickers:
            profile = excluded_by_v2_profile.get(ticker)
            if profile is not None:
                candidates.append(
                    UniverseCandidate(
                        market=normalized_market,
                        ticker=ticker,
                        decision="EXCLUDE_POSITIVE_PROFILE",
                        evidence_source="V2",
                        evidence=profile,
                    )
                )
            else:
                candidates.append(
                    UniverseCandidate(
                        market=normalized_market,
                        ticker=ticker,
                        decision="INCLUDE",
                        evidence_source="LEGACY",
                        evidence="LEGACY_FUNDAMENTALS_MEMBERSHIP",
                    )
                )
        return candidates

    def admit_company(
        self,
        *,
        market: str,
        ticker: str,
        company_name: str | None = None,
        admission_source: str = "LEGACY",
        admission_evidence: str | None = None,
        active: bool = True,
        now_utc: str | None = None,
    ) -> int:
        now = now_utc or utc_now_text()
        normalized_market = normalize_market(market)
        normalized_ticker = normalize_ticker(ticker)
        self.conn.execute(
            """
            INSERT INTO v3_company (
                market, ticker, company_name, profile, active, admission_source,
                admission_evidence, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, 'ORDINARY', ?, ?, ?, ?, ?)
            ON CONFLICT(market, ticker) DO UPDATE SET
                company_name = COALESCE(excluded.company_name, v3_company.company_name),
                active = excluded.active,
                admission_source = excluded.admission_source,
                admission_evidence = excluded.admission_evidence,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                normalized_market,
                normalized_ticker,
                company_name,
                1 if active else 0,
                admission_source,
                admission_evidence,
                now,
                now,
            ),
        )
        row = self.get_company(market=normalized_market, ticker=normalized_ticker)
        if row is None:
            raise RuntimeError("V3_COMPANY_ADMISSION_FAILED")
        return int(row["company_id"])

    def apply_universe_candidates(
        self,
        candidates: Iterable[UniverseCandidate],
        *,
        migration_run_id: str,
        audit_repo: "V3MigrationAuditRepository | None" = None,
        now_utc: str | None = None,
    ) -> dict[str, int]:
        included = 0
        excluded = 0
        now = now_utc or utc_now_text()
        for candidate in candidates:
            if candidate.decision == "INCLUDE":
                company_id = self.admit_company(
                    market=candidate.market,
                    ticker=candidate.ticker,
                    admission_source=candidate.evidence_source,
                    admission_evidence=candidate.evidence,
                    now_utc=now,
                )
                included += 1
                if audit_repo is not None:
                    audit_repo.record_audit(
                        migration_run_id=migration_run_id,
                        source=candidate.evidence_source,
                        source_key=f"{candidate.market}|{candidate.ticker}",
                        audit_type="UNIVERSE_ADMISSION",
                        decision="INCLUDE",
                        evidence={"evidence": candidate.evidence},
                        company_id=company_id,
                        now_utc=now,
                    )
            else:
                excluded += 1
                if audit_repo is not None:
                    audit_repo.record_audit(
                        migration_run_id=migration_run_id,
                        source=candidate.evidence_source,
                        source_key=f"{candidate.market}|{candidate.ticker}",
                        audit_type="UNIVERSE_ADMISSION",
                        decision=candidate.decision,
                        evidence={"evidence": candidate.evidence},
                        now_utc=now,
                    )
        return {"included": included, "excluded": excluded}

    def get_company(self, *, market: str, ticker: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM v3_company
            WHERE market = ? AND ticker = ?
            """,
            (normalize_market(market), normalize_ticker(ticker)),
        ).fetchone()

    def list_active_companies(self, *, market: str = "usa") -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT *
                FROM v3_company
                WHERE market = ? AND active = 1
                ORDER BY ticker
                """,
                (normalize_market(market),),
            )
        )


class V3QuarterRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def upsert_quarter(
        self,
        *,
        company_id: int,
        fiscal_year: int,
        fiscal_quarter: str,
        period_end_date: str | None = None,
        publish_date: str | None = None,
        market_availability_date: str | None = None,
        q_lifecycle: str = "RESULT_DETECTED",
        sec_confirmation_state: str = "NOT_DERIVABLE",
        now_utc: str | None = None,
        enforce_fiscal_calendar_guard: bool = True,
    ) -> int:
        if q_lifecycle not in Q_LIFECYCLES:
            raise ValueError(f"V3_INVALID_Q_LIFECYCLE:{q_lifecycle}")
        fq = normalize_fiscal_quarter(fiscal_quarter)
        if enforce_fiscal_calendar_guard:
            guard = validate_canonical_write_candidate(
                self.conn,
                FiscalCalendarWriteCandidate(
                    company_id=int(company_id),
                    fiscal_year=int(fiscal_year),
                    fiscal_quarter=fq,
                    period_end_date=period_end_date,
                    publish_date=publish_date,
                    source_context="V3QuarterRepository.upsert_quarter",
                ),
            )
            if not guard.write_permitted:
                raise RuntimeError("V3_FISCAL_CALENDAR_GUARD_REJECTED:" + guard.decision + ":" + ",".join(guard.reason_codes))
        now = now_utc or utc_now_text()
        self.conn.execute(
            """
            INSERT INTO v3_quarter (
                company_id, fiscal_year, fiscal_quarter, period_end_date, publish_date,
                market_availability_date, q_lifecycle, sec_confirmation_state,
                created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_id, fiscal_year, fiscal_quarter) DO UPDATE SET
                period_end_date = COALESCE(excluded.period_end_date, v3_quarter.period_end_date),
                publish_date = COALESCE(excluded.publish_date, v3_quarter.publish_date),
                market_availability_date = COALESCE(excluded.market_availability_date, v3_quarter.market_availability_date),
                q_lifecycle = excluded.q_lifecycle,
                sec_confirmation_state = excluded.sec_confirmation_state,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                int(company_id),
                int(fiscal_year),
                fq,
                period_end_date,
                publish_date,
                market_availability_date,
                q_lifecycle,
                sec_confirmation_state,
                now,
                now,
            ),
        )
        row = self.get_quarter(company_id=company_id, fiscal_year=fiscal_year, fiscal_quarter=fq)
        if row is None:
            raise RuntimeError("V3_QUARTER_UPSERT_FAILED")
        return int(row["quarter_id"])

    def get_quarter(self, *, company_id: int, fiscal_year: int, fiscal_quarter: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM v3_quarter
            WHERE company_id = ? AND fiscal_year = ? AND fiscal_quarter = ?
            """,
            (int(company_id), int(fiscal_year), normalize_fiscal_quarter(fiscal_quarter)),
        ).fetchone()


class V3FundamentalsRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def write_null_preserving_fields(
        self,
        *,
        quarter_id: int,
        values: Mapping[str, Any],
        accepted_source_provider: str,
        accepted_at_utc: str | None = None,
        update_run_id: str | None = None,
        derivation_method: str | None = None,
    ) -> dict[str, Any]:
        provider = _normalize_provider(accepted_source_provider)
        now = accepted_at_utc or utc_now_text()
        cleaned = {field: values[field] for field in FUNDAMENTAL_FIELDS if field in values and values[field] is not None}
        existing = self.get_fundamentals(quarter_id=quarter_id)
        if existing is None:
            insert_values = {field: cleaned.get(field) for field in FUNDAMENTAL_FIELDS}
            self.conn.execute(
                f"""
                INSERT INTO v3_quarter_fundamentals (
                    quarter_id, {", ".join(FUNDAMENTAL_FIELDS)}, accepted_source_provider,
                    accepted_at_utc, update_run_id, derivation_method, created_at_utc, updated_at_utc
                )
                VALUES ({", ".join("?" for _ in range(1 + len(FUNDAMENTAL_FIELDS) + 6))})
                """,
                (
                    int(quarter_id),
                    *(insert_values[field] for field in FUNDAMENTAL_FIELDS),
                    provider,
                    now,
                    update_run_id,
                    derivation_method,
                    now,
                    now,
                ),
            )
            return {"inserted": list(cleaned), "filled": list(cleaned), "preserved": [], "conflicts": []}

        filled: list[str] = []
        preserved: list[str] = []
        conflicts: list[str] = []
        assignments: list[str] = []
        params: list[Any] = []
        for field_name, value in cleaned.items():
            current = existing[field_name]
            if current is None:
                filled.append(field_name)
                assignments.append(f"{field_name} = ?")
                params.append(value)
            elif _values_equal(current, value):
                preserved.append(field_name)
            else:
                conflicts.append(field_name)
        if filled:
            assignments.extend(
                [
                    "accepted_source_provider = ?",
                    "accepted_at_utc = ?",
                    "update_run_id = ?",
                    "derivation_method = ?",
                    "updated_at_utc = ?",
                ]
            )
            params.extend([provider, now, update_run_id, derivation_method, now, int(quarter_id)])
            self.conn.execute(
                f"""
                UPDATE v3_quarter_fundamentals
                SET {", ".join(assignments)}
                WHERE quarter_id = ?
                """,
                tuple(params),
            )
        return {"inserted": [], "filled": filled, "preserved": preserved, "conflicts": conflicts}

    def get_fundamentals(self, *, quarter_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM v3_quarter_fundamentals
            WHERE quarter_id = ?
            """,
            (int(quarter_id),),
        ).fetchone()


class V3ProviderAcquisitionRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def upsert_outcome(
        self,
        *,
        quarter_id: int,
        provider: str,
        acquisition_result: str,
        last_checked_at_utc: str | None = None,
        last_success_at_utc: str | None = None,
        next_retry_at_utc: str | None = None,
        attempt_count: int = 0,
        usable_field_count: int = 0,
        provider_cache_ref: str | None = None,
        last_error_code: str | None = None,
        now_utc: str | None = None,
    ) -> int:
        provider = _normalize_provider(provider)
        if acquisition_result not in PROVIDER_RESULTS:
            raise ValueError(f"V3_INVALID_PROVIDER_ACQUISITION_RESULT:{acquisition_result}")
        now = now_utc or utc_now_text()
        self.conn.execute(
            """
            INSERT INTO v3_provider_q_acquisition (
                quarter_id, provider, acquisition_result, last_checked_at_utc,
                last_success_at_utc, next_retry_at_utc, attempt_count, usable_field_count,
                provider_cache_ref, last_error_code, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(quarter_id, provider) DO UPDATE SET
                acquisition_result = excluded.acquisition_result,
                last_checked_at_utc = excluded.last_checked_at_utc,
                last_success_at_utc = excluded.last_success_at_utc,
                next_retry_at_utc = excluded.next_retry_at_utc,
                attempt_count = excluded.attempt_count,
                usable_field_count = excluded.usable_field_count,
                provider_cache_ref = excluded.provider_cache_ref,
                last_error_code = excluded.last_error_code,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                int(quarter_id),
                provider,
                acquisition_result,
                last_checked_at_utc,
                last_success_at_utc,
                next_retry_at_utc,
                int(attempt_count),
                int(usable_field_count),
                provider_cache_ref,
                last_error_code,
                now,
            ),
        )
        row = self.conn.execute(
            """
            SELECT acquisition_id
            FROM v3_provider_q_acquisition
            WHERE quarter_id = ? AND provider = ?
            """,
            (int(quarter_id), provider),
        ).fetchone()
        return int(row["acquisition_id"])

    def list_due(self, *, as_of_utc: str, provider: str | None = None) -> list[sqlite3.Row]:
        params: list[Any] = [as_of_utc]
        provider_clause = ""
        if provider is not None:
            provider_clause = "AND provider = ?"
            params.append(_normalize_provider(provider))
        return list(
            self.conn.execute(
                f"""
                SELECT *
                FROM v3_provider_q_acquisition
                WHERE next_retry_at_utc IS NOT NULL
                  AND next_retry_at_utc <= ?
                  AND acquisition_result IN ('FAILED', 'NO_DATA', 'PARTIAL')
                  {provider_clause}
                ORDER BY next_retry_at_utc, quarter_id
                """,
                tuple(params),
            )
        )


class V3CalendarRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def upsert_calendar_event(self, *, company_id: int, provider: str, calendar_status: str, provider_event_key: str | None = None, fiscal_year: int | None = None, fiscal_quarter: str | None = None, expected_result_date: str | None = None, source_observed_at_utc: str | None = None, now_utc: str | None = None) -> int:
        now = now_utc or utc_now_text()
        fq = normalize_fiscal_quarter(fiscal_quarter) if fiscal_quarter is not None else None
        event_key = provider_event_key or f"company:{company_id}|provider:{_normalize_provider(provider)}|fy:{fiscal_year}|fq:{fq}|expected:{expected_result_date}"
        self.conn.execute(
            """
            INSERT INTO v3_result_calendar (
                company_id, provider, provider_event_key, fiscal_year, fiscal_quarter,
                expected_result_date, calendar_status, source_observed_at_utc,
                created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_id, provider, provider_event_key) DO UPDATE SET
                fiscal_year = excluded.fiscal_year,
                fiscal_quarter = excluded.fiscal_quarter,
                expected_result_date = excluded.expected_result_date,
                calendar_status = excluded.calendar_status,
                source_observed_at_utc = excluded.source_observed_at_utc,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                int(company_id),
                _normalize_provider(provider),
                event_key,
                fiscal_year,
                fq,
                expected_result_date,
                calendar_status,
                source_observed_at_utc,
                now,
                now,
            ),
        )
        row = self.conn.execute(
            """
            SELECT calendar_id
            FROM v3_result_calendar
            WHERE company_id = ? AND provider = ? AND provider_event_key = ?
            """,
            (int(company_id), _normalize_provider(provider), event_key),
        ).fetchone()
        return int(row["calendar_id"])


class V3OperationalActionRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def upsert_action(self, *, action_type: str, company_id: int, quarter_id: int | None = None, provider: str | None = None, due_at_utc: str | None = None, status: str = "ACTIVE", attempt_count: int = 0, last_error: str | None = None, details: Mapping[str, Any] | None = None, now_utc: str | None = None) -> int:
        if action_type not in ACTION_TYPES:
            raise ValueError(f"V3_INVALID_ACTION_TYPE:{action_type}")
        if status not in ACTION_STATUSES:
            raise ValueError(f"V3_INVALID_ACTION_STATUS:{status}")
        normalized_provider = _normalize_provider(provider) if provider is not None else None
        now = now_utc or utc_now_text()
        existing = self.conn.execute(
            """
            SELECT action_id
            FROM v3_operational_action
            WHERE action_type = ?
              AND company_id = ?
              AND COALESCE(quarter_id, -1) = COALESCE(?, -1)
              AND COALESCE(provider, '') = COALESCE(?, '')
              AND status IN ('ACTIVE', 'DEFERRED', 'BLOCKED')
            """,
            (action_type, int(company_id), quarter_id, normalized_provider),
        ).fetchone()
        details_json = _json_dumps(details) if details is not None else None
        if existing is not None:
            self.conn.execute(
                """
                UPDATE v3_operational_action
                SET due_at_utc = ?, status = ?, attempt_count = ?, last_error = ?,
                    details_json = ?, updated_at_utc = ?
                WHERE action_id = ?
                """,
                (due_at_utc, status, int(attempt_count), last_error, details_json, now, int(existing["action_id"])),
            )
            return int(existing["action_id"])
        self.conn.execute(
            """
            INSERT INTO v3_operational_action (
                action_type, company_id, quarter_id, provider, due_at_utc, status,
                attempt_count, last_error, details_json, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                action_type,
                int(company_id),
                quarter_id,
                normalized_provider,
                due_at_utc,
                status,
                int(attempt_count),
                last_error,
                details_json,
                now,
                now,
            ),
        )
        return int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    def resolve_action(self, *, action_id: int, status: str = "RESOLVED", now_utc: str | None = None) -> None:
        if status not in {"RESOLVED", "CANCELLED"}:
            raise ValueError(f"V3_INVALID_ACTION_RESOLUTION_STATUS:{status}")
        self.conn.execute(
            """
            UPDATE v3_operational_action
            SET status = ?, updated_at_utc = ?
            WHERE action_id = ?
            """,
            (status, now_utc or utc_now_text(), int(action_id)),
        )

    def list_due_actions(self, *, as_of_utc: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                """
                SELECT *
                FROM v3_operational_action
                WHERE status IN ('ACTIVE', 'DEFERRED')
                  AND (due_at_utc IS NULL OR due_at_utc <= ?)
                ORDER BY due_at_utc, action_id
                """,
                (as_of_utc,),
            )
        )


class V3ResolutionIssueRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def create_issue(self, *, issue_type: str, quarter_id: int | None = None, field_name: str | None = None, status: str = "ACTIVE", source_details: Mapping[str, Any] | None = None, unresolved_market: str | None = None, unresolved_ticker: str | None = None, unresolved_fiscal_year: int | None = None, unresolved_fiscal_quarter: str | None = None, now_utc: str | None = None) -> int:
        if status not in ACTION_STATUSES:
            raise ValueError(f"V3_INVALID_ISSUE_STATUS:{status}")
        now = now_utc or utc_now_text()
        self.conn.execute(
            """
            INSERT INTO v3_resolution_issue (
                quarter_id, unresolved_market, unresolved_ticker, unresolved_fiscal_year,
                unresolved_fiscal_quarter, issue_type, field_name, status,
                source_details_json, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                quarter_id,
                normalize_market(unresolved_market) if unresolved_market is not None else None,
                normalize_ticker(unresolved_ticker) if unresolved_ticker is not None else None,
                unresolved_fiscal_year,
                normalize_fiscal_quarter(unresolved_fiscal_quarter) if unresolved_fiscal_quarter is not None else None,
                issue_type,
                field_name,
                status,
                _json_dumps(source_details) if source_details is not None else None,
                now,
                now,
            ),
        )
        return int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    def resolve_issue(self, *, issue_id: int, resolution: str, now_utc: str | None = None) -> None:
        now = now_utc or utc_now_text()
        self.conn.execute(
            """
            UPDATE v3_resolution_issue
            SET status = 'RESOLVED', resolution = ?, resolved_at_utc = ?, updated_at_utc = ?
            WHERE issue_id = ?
            """,
            (resolution, now, now, int(issue_id)),
        )


class V3MigrationAuditRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def record_audit(self, *, migration_run_id: str, source: str, source_key: str, audit_type: str, decision: str, evidence: Mapping[str, Any] | None = None, company_id: int | None = None, quarter_id: int | None = None, now_utc: str | None = None) -> None:
        now = now_utc or utc_now_text()
        self.conn.execute(
            """
            INSERT INTO v3_migration_audit (
                migration_run_id, source, source_key, company_id, quarter_id,
                audit_type, decision, evidence_json, created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(migration_run_id, source, source_key, audit_type) DO UPDATE SET
                company_id = excluded.company_id,
                quarter_id = excluded.quarter_id,
                decision = excluded.decision,
                evidence_json = excluded.evidence_json
            """,
            (
                migration_run_id,
                source,
                source_key,
                company_id,
                quarter_id,
                audit_type,
                decision,
                _json_dumps(evidence) if evidence is not None else None,
                now,
            ),
        )


class V3OutputRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)

    def upsert_ttm(self, *, company_id: int, as_of_quarter_id: int, model_version: str, ttm_ready: bool, output: Mapping[str, Any] | None = None, run_id: str | None = None, now_utc: str | None = None) -> int:
        return self._upsert_output(
            table="v3_ttm",
            id_column="ttm_id",
            key_columns=("company_id", "as_of_quarter_id", "model_version"),
            values=(int(company_id), int(as_of_quarter_id), model_version),
            ready_column="ttm_ready",
            ready=ttm_ready,
            output=output,
            run_id=run_id,
            now_utc=now_utc,
        )

    def upsert_score(self, *, company_id: int, as_of_quarter_id: int, score_model_version: str, score_ready: bool, fundamental_score: float | None = None, output: Mapping[str, Any] | None = None, run_id: str | None = None, now_utc: str | None = None) -> int:
        now = now_utc or utc_now_text()
        self.conn.execute(
            """
            INSERT INTO v3_score (
                company_id, as_of_quarter_id, score_model_version, score_ready,
                fundamental_score, output_json, run_id, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_id, as_of_quarter_id, score_model_version) DO UPDATE SET
                score_ready = excluded.score_ready,
                fundamental_score = excluded.fundamental_score,
                output_json = excluded.output_json,
                run_id = excluded.run_id,
                updated_at_utc = excluded.updated_at_utc
            """,
            (
                int(company_id),
                int(as_of_quarter_id),
                score_model_version,
                1 if score_ready else 0,
                fundamental_score,
                _json_dumps(output) if output is not None else None,
                run_id,
                now,
                now,
            ),
        )
        return _last_or_existing_id(
            self.conn,
            "v3_score",
            "score_id",
            {"company_id": company_id, "as_of_quarter_id": as_of_quarter_id, "score_model_version": score_model_version},
        )

    def upsert_valuation(
        self,
        *,
        company_id: int,
        valuation_date: str,
        model_version: str,
        valuation_ready: bool,
        output: Mapping[str, Any] | None = None,
        run_id: str | None = None,
        now_utc: str | None = None,
        endpoint_ttm_id: int | None = None,
        endpoint_quarter_id: int | None = None,
        endpoint_fiscal_year: int | None = None,
        endpoint_fiscal_quarter: str | None = None,
        endpoint_period_end: str | None = None,
        publish_date: str | None = None,
        valuation_close_price: float | None = None,
        price_source: str = "UNSPECIFIED",
        source_fingerprint: str = "UNSPECIFIED",
    ) -> int:
        columns = {str(row[1]) for row in self.conn.execute("PRAGMA table_info(v3_valuation)").fetchall()}
        if "endpoint_ttm_id" in columns:
            required = {
                "endpoint_ttm_id": endpoint_ttm_id,
                "endpoint_quarter_id": endpoint_quarter_id,
                "endpoint_fiscal_year": endpoint_fiscal_year,
                "endpoint_fiscal_quarter": endpoint_fiscal_quarter,
                "endpoint_period_end": endpoint_period_end,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise ValueError("FUNDAMENTALS_V3_VALUATION_SNAPSHOT_LINEAGE_REQUIRED:" + ",".join(missing))
            now = now_utc or utc_now_text()
            self.conn.execute(
                """
                INSERT INTO v3_valuation (
                    company_id, endpoint_ttm_id, endpoint_quarter_id, endpoint_fiscal_year,
                    endpoint_fiscal_quarter, endpoint_period_end, publish_date, valuation_date,
                    valuation_close_price, price_source, model_version, valuation_ready,
                    valuation_status, source_fingerprint, output_json, run_id,
                    created_at_utc, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_id, endpoint_ttm_id, model_version) DO UPDATE SET
                    valuation_ready = excluded.valuation_ready,
                    valuation_status = excluded.valuation_status,
                    source_fingerprint = excluded.source_fingerprint,
                    output_json = excluded.output_json,
                    run_id = excluded.run_id,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    int(company_id),
                    int(endpoint_ttm_id),
                    int(endpoint_quarter_id),
                    int(endpoint_fiscal_year),
                    endpoint_fiscal_quarter,
                    endpoint_period_end,
                    publish_date,
                    valuation_date,
                    valuation_close_price,
                    price_source,
                    model_version,
                    1 if valuation_ready else 0,
                    "VALID" if valuation_ready else "MISSING_INPUT",
                    source_fingerprint,
                    _json_dumps(output) if output is not None else None,
                    run_id,
                    now,
                    now,
                ),
            )
            return _last_or_existing_id(
                self.conn,
                "v3_valuation",
                "valuation_id",
                {"company_id": company_id, "endpoint_ttm_id": endpoint_ttm_id, "model_version": model_version},
            )
        return self._upsert_output(
            table="v3_valuation",
            id_column="valuation_id",
            key_columns=("company_id", "valuation_date", "model_version"),
            values=(int(company_id), valuation_date, model_version),
            ready_column="valuation_ready",
            ready=valuation_ready,
            output=output,
            run_id=run_id,
            now_utc=now_utc,
        )

    def _upsert_output(self, *, table: str, id_column: str, key_columns: tuple[str, str, str], values: tuple[Any, Any, Any], ready_column: str, ready: bool, output: Mapping[str, Any] | None, run_id: str | None, now_utc: str | None) -> int:
        now = now_utc or utc_now_text()
        self.conn.execute(
            f"""
            INSERT INTO {table} (
                {", ".join(key_columns)}, {ready_column}, output_json, run_id, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT({", ".join(key_columns)}) DO UPDATE SET
                {ready_column} = excluded.{ready_column},
                output_json = excluded.output_json,
                run_id = excluded.run_id,
                updated_at_utc = excluded.updated_at_utc
            """,
            (*values, 1 if ready else 0, _json_dumps(output) if output is not None else None, run_id, now, now),
        )
        return _last_or_existing_id(self.conn, table, id_column, dict(zip(key_columns, values)))


class V3RawCacheRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = configure_connection(sqlite3.connect(str(self.db_path)))
        apply_v3_schema(conn, include_raw_cache=True)
        return conn

    def put_payload(
        self,
        *,
        provider: str,
        provider_symbol: str,
        fetch_run_id: str,
        payload_json: str,
        status: str = "OK",
        error_message: str | None = None,
        observed_at_utc: str | None = None,
    ) -> str:
        provider = _normalize_provider(provider)
        if status not in {"OK", "EMPTY", "ERROR"}:
            raise ValueError(f"V3_INVALID_RAW_CACHE_STATUS:{status}")
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        now = utc_now_text()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO v3_raw_cache_entry (
                    provider, provider_symbol, fetch_run_id, payload_hash,
                    payload_json, status, error_message, observed_at_utc, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    provider,
                    provider_symbol,
                    fetch_run_id,
                    payload_hash,
                    payload_json,
                    status,
                    error_message,
                    observed_at_utc or now,
                    now,
                ),
            )
            conn.commit()
        return payload_hash


def _normalize_provider(value: str) -> str:
    provider = str(value).strip().upper()
    if provider not in PROVIDERS:
        raise ValueError(f"V3_INVALID_PROVIDER:{value}")
    return provider


def _json_dumps(value: Mapping[str, Any] | None) -> str:
    return json.dumps(value or {}, sort_keys=True, separators=(",", ":"))


def _values_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return str(left) == str(right)


def _last_or_existing_id(conn: sqlite3.Connection, table: str, id_column: str, key: Mapping[str, Any]) -> int:
    where = " AND ".join(f"{column} = ?" for column in key)
    row = conn.execute(
        f"""
        SELECT {id_column}
        FROM {table}
        WHERE {where}
        """,
        tuple(key.values()),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"V3_OUTPUT_UPSERT_FAILED:{table}")
    return int(row[id_column])
