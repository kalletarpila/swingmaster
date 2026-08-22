from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from swingmaster.fundamentals.v3_helpers import (
    derive_free_cashflow,
    derive_ordinary_ebitda,
    derive_total_debt,
    make_v3_work_unit_key,
    normalize_fiscal_quarter,
    normalize_market,
    normalize_ticker,
)
from swingmaster.fundamentals.v3_repositories import (
    FUNDAMENTAL_FIELDS,
    PROVIDERS,
    V3CompanyRepository,
    V3MigrationAuditRepository,
    V3QuarterRepository,
    configure_connection,
    utc_now_text,
)


CANONICAL_FIELD_NAMES = FUNDAMENTAL_FIELDS
MIGRATION_SOURCES = {"YAHOO", "V2", "LEGACY"}
ISSUE_TYPES = {
    "DUPLICATE_FISCAL_WORK_UNIT",
    "PERIOD_DATE_CONFLICT",
    "NON_NULL_FIELD_CONFLICT",
    "TRANSITION_PERIOD_VARIANT",
    "FISCAL_MAPPING_CORRECTION",
    "PUBLICATION_DATE_CONFLICT",
    "OTHER_MIGRATION_REVIEW",
}
FIELD_OUTCOMES = (
    "FIELD_INSERTED",
    "FIELD_FILLED_FROM_NULL",
    "FIELD_CONFIRMED_SAME",
    "FIELD_ROUNDING_EQUIVALENT",
    "FIELD_EXPECTED_SEMANTIC_DIFFERENCE",
    "FIELD_CONFLICT",
    "FIELD_SKIPPED_NULL",
    "FIELD_DERIVED",
    "FIELD_REJECTED",
)
DATE_OUTCOMES = (
    "PERIOD_DATE_SET",
    "PERIOD_DATE_CONFIRMED",
    "PERIOD_DATE_SAFE_VARIANT",
    "PERIOD_DATE_CONFLICT",
    "PERIOD_DATE_REQUIRES_RESOLUTION",
    "PUBLISH_DATE_SET",
    "PUBLISH_DATE_CONFIRMED",
    "PUBLISH_DATE_CONFLICT",
    "PUBLISH_DATE_SKIPPED_NULL",
)
HISTORICAL_PERIOD_FLOOR = date(1999, 1, 1)


@dataclass(frozen=True)
class V3CanonicalMigrationCandidate:
    source_system: str
    source_record_id: str
    migration_run_id: str
    market: str
    ticker: str
    fiscal_year: int
    fiscal_quarter: str
    period_end_date: str | None
    values: Mapping[str, Any] = field(default_factory=dict)
    publish_date: str | None = None
    market_availability_date: str | None = None
    raw_evidence_ref: str | None = None
    approved_company_active: bool | None = None
    company_name: str | None = None
    candidate_can_create_quarter: bool = True
    candidate_issue_type: str | None = None
    period_date_policy: str = "CONFLICT"
    field_semantic_differences: tuple[str, ...] = ()
    derivation_inputs: Mapping[str, Any] = field(default_factory=dict)
    value_metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        source = str(self.source_system).strip().upper()
        if source not in MIGRATION_SOURCES:
            raise ValueError(f"V3_CANONICAL_INVALID_SOURCE:{self.source_system}")
        object.__setattr__(self, "source_system", source)
        object.__setattr__(self, "market", normalize_market(self.market))
        object.__setattr__(self, "ticker", normalize_ticker(self.ticker))
        object.__setattr__(self, "fiscal_year", int(self.fiscal_year))
        object.__setattr__(self, "fiscal_quarter", normalize_fiscal_quarter(self.fiscal_quarter))
        if not str(self.source_record_id or "").strip():
            raise ValueError("V3_CANONICAL_SOURCE_RECORD_ID_REQUIRED")
        if not str(self.migration_run_id or "").strip():
            raise ValueError("V3_CANONICAL_MIGRATION_RUN_ID_REQUIRED")
        if self.period_date_policy not in {"CONFLICT", "SAFE_VARIANT", "REQUIRES_RESOLUTION"}:
            raise ValueError(f"V3_CANONICAL_INVALID_PERIOD_DATE_POLICY:{self.period_date_policy}")
        if self.candidate_issue_type is not None and self.candidate_issue_type not in ISSUE_TYPES:
            raise ValueError(f"V3_CANONICAL_INVALID_ISSUE_TYPE:{self.candidate_issue_type}")
        unknown_fields = sorted(set(self.values) - set(CANONICAL_FIELD_NAMES))
        if unknown_fields:
            raise ValueError("V3_CANONICAL_UNKNOWN_FIELDS:" + ",".join(unknown_fields))

    @property
    def work_unit_key(self) -> str:
        return make_v3_work_unit_key(
            market=self.market,
            ticker=self.ticker,
            fiscal_year=self.fiscal_year,
            fiscal_quarter=self.fiscal_quarter,
        )


@dataclass(frozen=True)
class V3FieldPolicy:
    absolute_tolerance: float = 1.0
    relative_tolerance: float = 0.000001


@dataclass(frozen=True)
class V3SourceApplyPolicy:
    source: str
    field_policies: Mapping[str, V3FieldPolicy] = field(default_factory=dict)

    def __post_init__(self) -> None:
        source = str(self.source).strip().upper()
        if source not in MIGRATION_SOURCES:
            raise ValueError(f"V3_CANONICAL_INVALID_POLICY_SOURCE:{self.source}")
        object.__setattr__(self, "source", source)

    def policy_for(self, field_name: str) -> V3FieldPolicy:
        return self.field_policies.get(field_name, V3FieldPolicy())


class V3CanonicalMigrationRunSummary:
    def __init__(self, *, run_id: str, source: str, started_at_utc: str) -> None:
        self.run_id = run_id
        self.source = source
        self.started_at_utc = started_at_utc
        self.completed_at_utc: str | None = None
        self.rows = Counter()
        self.metadata = Counter()
        self.issues = Counter()
        self.companies_seen: set[str] = set()
        self.field_contributions: dict[str, Counter[str]] = {field_name: Counter() for field_name in CANONICAL_FIELD_NAMES}
        self.candidate_results: list[dict[str, Any]] = []
        self.integrity_result: dict[str, Any] = {}

    def add_field(self, field_name: str, outcome: str) -> None:
        self.field_contributions[field_name][outcome] += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source": self.source,
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "companies_seen": len(self.companies_seen),
            "rows": dict(sorted(self.rows.items())),
            "metadata": dict(sorted(self.metadata.items())),
            "field_contributions": {
                field_name: dict(sorted(counter.items()))
                for field_name, counter in sorted(self.field_contributions.items())
            },
            "issues": dict(sorted(self.issues.items())),
            "candidate_results": self.candidate_results,
            "integrity_result": self.integrity_result,
        }


class V3CanonicalMigrationEngine:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = configure_connection(conn)
        self.companies = V3CompanyRepository(self.conn)
        self.quarters = V3QuarterRepository(self.conn)
        self.audit = V3MigrationAuditRepository(self.conn)

    def apply_source_batch(
        self,
        candidates: Iterable[V3CanonicalMigrationCandidate],
        *,
        source: str,
        migration_run_id: str,
        policy: V3SourceApplyPolicy | None = None,
        dry_apply: bool = False,
        now_utc: str | None = None,
    ) -> V3CanonicalMigrationRunSummary:
        normalized_source = _normalize_migration_source(source)
        run_policy = policy or V3SourceApplyPolicy(source=normalized_source)
        if run_policy.source != normalized_source:
            raise ValueError("V3_CANONICAL_POLICY_SOURCE_MISMATCH")
        now = now_utc or utc_now_text()
        summary = V3CanonicalMigrationRunSummary(run_id=migration_run_id, source=normalized_source, started_at_utc=now)
        for candidate in candidates:
            if candidate.source_system != normalized_source or candidate.migration_run_id != migration_run_id:
                raise ValueError("V3_CANONICAL_CANDIDATE_RUN_MISMATCH")
            summary.rows["source_rows_examined"] += 1
            summary.companies_seen.add(f"{candidate.market}|{candidate.ticker}")
            self.conn.execute("SAVEPOINT v3_candidate_apply")
            try:
                result = self._apply_candidate(candidate, policy=run_policy, now_utc=now)
                if dry_apply:
                    self.conn.execute("ROLLBACK TO v3_candidate_apply")
                summary.candidate_results.append(result)
                self._merge_result(summary, result)
                self.conn.execute("RELEASE v3_candidate_apply")
            except Exception:
                self.conn.execute("ROLLBACK TO v3_candidate_apply")
                self.conn.execute("RELEASE v3_candidate_apply")
                raise
        summary.completed_at_utc = now
        summary.integrity_result = self.validate_integrity()
        return summary

    def validate_integrity(self) -> dict[str, Any]:
        quick_check = self.conn.execute("PRAGMA quick_check").fetchone()[0]
        fk_rows = self.conn.execute("PRAGMA foreign_key_check").fetchall()
        duplicate_quarters = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT company_id, fiscal_year, fiscal_quarter
                FROM v3_quarter
                GROUP BY company_id, fiscal_year, fiscal_quarter
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        orphan_fundamentals = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM v3_quarter_fundamentals f
            LEFT JOIN v3_quarter q ON q.quarter_id = f.quarter_id
            WHERE q.quarter_id IS NULL
            """
        ).fetchone()[0]
        return {
            "quick_check": quick_check,
            "foreign_key_check_rows": len(fk_rows),
            "duplicate_company_fy_fq": int(duplicate_quarters),
            "orphan_fundamentals": int(orphan_fundamentals),
        }

    def _apply_candidate(self, candidate: V3CanonicalMigrationCandidate, *, policy: V3SourceApplyPolicy, now_utc: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "source_record_id": candidate.source_record_id,
            "work_unit_key": candidate.work_unit_key,
            "row_outcomes": [],
            "metadata_outcomes": [],
            "field_outcomes": {},
            "issue_ids": [],
            "disposition": "ACCEPTED",
            "company_id": None,
            "quarter_id": None,
        }
        company = self.companies.get_company(market=candidate.market, ticker=candidate.ticker)
        if company is None:
            if candidate.approved_company_active is None:
                return self._reject_before_quarter(
                    candidate,
                    result,
                    issue_type="OTHER_MIGRATION_REVIEW",
                    decision="UNAPPROVED_COMPANY",
                    now_utc=now_utc,
                )
            company_id = self.companies.admit_company(
                market=candidate.market,
                ticker=candidate.ticker,
                company_name=candidate.company_name,
                admission_source=candidate.source_system,
                admission_evidence="PHASE3_APPROVED_BASELINE",
                active=candidate.approved_company_active,
                now_utc=now_utc,
            )
            result["row_outcomes"].append("COMPANY_CREATED")
        else:
            company_id = int(company["company_id"])
            result["row_outcomes"].append("COMPANY_MATCHED")
        result["company_id"] = company_id
        if not candidate.candidate_can_create_quarter:
            return self._reject_before_quarter(
                candidate,
                result,
                issue_type=candidate.candidate_issue_type or "OTHER_MIGRATION_REVIEW",
                decision="CANDIDATE_NOT_CREATABLE",
                now_utc=now_utc,
                company_id=company_id,
            )
        if candidate.period_end_date is None:
            return self._reject_before_quarter(
                candidate,
                result,
                issue_type="OTHER_MIGRATION_REVIEW",
                decision="MISSING_PERIOD_END_DATE",
                now_utc=now_utc,
                company_id=company_id,
            )
        if _parse_iso_date(candidate.period_end_date) < HISTORICAL_PERIOD_FLOOR:
            return self._reject_before_quarter(
                candidate,
                result,
                issue_type="OTHER_MIGRATION_REVIEW",
                decision="PERIOD_BEFORE_1999_FLOOR",
                now_utc=now_utc,
                company_id=company_id,
            )
        quarter = self.quarters.get_quarter(
            company_id=company_id,
            fiscal_year=candidate.fiscal_year,
            fiscal_quarter=candidate.fiscal_quarter,
        )
        if quarter is None:
            quarter_id = self.quarters.upsert_quarter(
                company_id=company_id,
                fiscal_year=candidate.fiscal_year,
                fiscal_quarter=candidate.fiscal_quarter,
                period_end_date=candidate.period_end_date,
                publish_date=candidate.publish_date,
                market_availability_date=candidate.market_availability_date,
                q_lifecycle="RESULT_DETECTED",
                now_utc=now_utc,
            )
            result["row_outcomes"].append("QUARTER_CREATED")
            result["metadata_outcomes"].append("PERIOD_DATE_SET")
            if candidate.publish_date is None:
                result["metadata_outcomes"].append("PUBLISH_DATE_SKIPPED_NULL")
            else:
                result["metadata_outcomes"].append("PUBLISH_DATE_SET")
        else:
            quarter_id = int(quarter["quarter_id"])
            result["row_outcomes"].append("QUARTER_MATCHED")
            result["metadata_outcomes"].extend(
                self._apply_dates(quarter, candidate, quarter_id=quarter_id, result=result, now_utc=now_utc)
            )
        result["quarter_id"] = quarter_id
        values, derived_fields, derivation_notes = _candidate_values_with_derivations(candidate)
        field_outcomes = self._apply_fields(
            candidate,
            quarter_id=quarter_id,
            values=values,
            derived_fields=derived_fields,
            derivation_notes=derivation_notes,
            policy=policy,
            result=result,
            now_utc=now_utc,
        )
        result["field_outcomes"] = field_outcomes
        if "FIELD_CONFLICT" in {outcome for outcomes in field_outcomes.values() for outcome in outcomes}:
            result["row_outcomes"].append("RESOLUTION_REQUIRED")
        self.audit.record_audit(
            migration_run_id=candidate.migration_run_id,
            source=candidate.source_system,
            source_key=candidate.source_record_id,
            audit_type="CANONICAL_APPLY",
            decision=result["disposition"],
            evidence=_result_evidence(result, candidate),
            company_id=company_id,
            quarter_id=quarter_id,
            now_utc=now_utc,
        )
        return result

    def _apply_dates(
        self,
        quarter: sqlite3.Row,
        candidate: V3CanonicalMigrationCandidate,
        *,
        quarter_id: int,
        result: dict[str, Any],
        now_utc: str,
    ) -> list[str]:
        outcomes: list[str] = []
        updates: dict[str, str | None] = {}
        if quarter["period_end_date"] is None and candidate.period_end_date is not None:
            updates["period_end_date"] = candidate.period_end_date
            outcomes.append("PERIOD_DATE_SET")
        elif quarter["period_end_date"] == candidate.period_end_date:
            outcomes.append("PERIOD_DATE_CONFIRMED")
        elif candidate.period_date_policy == "SAFE_VARIANT":
            outcomes.append("PERIOD_DATE_SAFE_VARIANT")
        elif candidate.period_date_policy == "REQUIRES_RESOLUTION":
            outcomes.append("PERIOD_DATE_REQUIRES_RESOLUTION")
            result["issue_ids"].append(
                self._create_issue_once(candidate, "PERIOD_DATE_CONFLICT", quarter_id=quarter_id, field_name="period_end_date", now_utc=now_utc)
            )
        else:
            outcomes.append("PERIOD_DATE_CONFLICT")
            result["issue_ids"].append(
                self._create_issue_once(candidate, "PERIOD_DATE_CONFLICT", quarter_id=quarter_id, field_name="period_end_date", now_utc=now_utc)
            )
        if candidate.publish_date is None:
            outcomes.append("PUBLISH_DATE_SKIPPED_NULL")
        elif quarter["publish_date"] is None:
            updates["publish_date"] = candidate.publish_date
            outcomes.append("PUBLISH_DATE_SET")
        elif quarter["publish_date"] == candidate.publish_date:
            outcomes.append("PUBLISH_DATE_CONFIRMED")
        else:
            outcomes.append("PUBLISH_DATE_CONFLICT")
            result["issue_ids"].append(
                self._create_issue_once(candidate, "PUBLICATION_DATE_CONFLICT", quarter_id=quarter_id, field_name="publish_date", now_utc=now_utc)
            )
        if candidate.market_availability_date is not None and quarter["market_availability_date"] is None:
            updates["market_availability_date"] = candidate.market_availability_date
        if updates:
            assignments = ", ".join(f"{column} = ?" for column in updates)
            self.conn.execute(
                f"UPDATE v3_quarter SET {assignments}, updated_at_utc = ? WHERE quarter_id = ?",
                (*updates.values(), now_utc, quarter_id),
            )
        return outcomes

    def _apply_fields(
        self,
        candidate: V3CanonicalMigrationCandidate,
        *,
        quarter_id: int,
        values: Mapping[str, Any],
        derived_fields: set[str],
        derivation_notes: Mapping[str, str],
        policy: V3SourceApplyPolicy,
        result: dict[str, Any],
        now_utc: str,
    ) -> dict[str, list[str]]:
        existing = self.conn.execute("SELECT * FROM v3_quarter_fundamentals WHERE quarter_id = ?", (quarter_id,)).fetchone()
        if existing is None:
            self.conn.execute(
                f"""
                INSERT INTO v3_quarter_fundamentals (
                    quarter_id, {", ".join(CANONICAL_FIELD_NAMES)}, accepted_source_provider,
                    accepted_at_utc, update_run_id, derivation_method, created_at_utc, updated_at_utc
                )
                VALUES ({", ".join("?" for _ in range(1 + len(CANONICAL_FIELD_NAMES) + 6))})
                """,
                (
                    quarter_id,
                    *(values.get(field_name) for field_name in CANONICAL_FIELD_NAMES),
                    candidate.source_system,
                    now_utc,
                    candidate.migration_run_id,
                    _compact_derivation_method(derivation_notes),
                    now_utc,
                    now_utc,
                ),
            )
            outcomes = {}
            for field_name in CANONICAL_FIELD_NAMES:
                if values.get(field_name) is None:
                    outcomes[field_name] = ["FIELD_SKIPPED_NULL"]
                elif field_name in derived_fields:
                    outcomes[field_name] = ["FIELD_DERIVED", "FIELD_INSERTED"]
                else:
                    outcomes[field_name] = ["FIELD_INSERTED"]
            return outcomes
        outcomes: dict[str, list[str]] = {}
        assignments: list[str] = []
        params: list[Any] = []
        for field_name in CANONICAL_FIELD_NAMES:
            incoming = values.get(field_name)
            if incoming is None:
                outcomes[field_name] = ["FIELD_SKIPPED_NULL"]
                continue
            if field_name in candidate.field_semantic_differences:
                outcomes[field_name] = ["FIELD_EXPECTED_SEMANTIC_DIFFERENCE"]
                continue
            current = existing[field_name]
            if current is None:
                assignments.append(f"{field_name} = ?")
                params.append(incoming)
                outcomes[field_name] = ["FIELD_FILLED_FROM_NULL"]
                if field_name in derived_fields:
                    outcomes[field_name].insert(0, "FIELD_DERIVED")
                continue
            comparison = _compare_numeric(current, incoming, policy.policy_for(field_name))
            if comparison == "same":
                outcomes[field_name] = ["FIELD_CONFIRMED_SAME"]
            elif comparison == "rounding":
                outcomes[field_name] = ["FIELD_ROUNDING_EQUIVALENT"]
            else:
                outcomes[field_name] = ["FIELD_CONFLICT"]
                result["issue_ids"].append(
                    self._create_issue_once(
                        candidate,
                        "NON_NULL_FIELD_CONFLICT",
                        quarter_id=quarter_id,
                        field_name=field_name,
                        details={"existing": current, "incoming": incoming},
                        now_utc=now_utc,
                    )
                )
        if assignments:
            assignments.extend(
                [
                    "accepted_source_provider = ?",
                    "accepted_at_utc = ?",
                    "update_run_id = ?",
                    "derivation_method = ?",
                    "updated_at_utc = ?",
                ]
            )
            params.extend([candidate.source_system, now_utc, candidate.migration_run_id, _compact_derivation_method(derivation_notes), now_utc, quarter_id])
            self.conn.execute(
                f"""
                UPDATE v3_quarter_fundamentals
                SET {", ".join(assignments)}
                WHERE quarter_id = ?
                """,
                tuple(params),
            )
        return outcomes

    def _reject_before_quarter(
        self,
        candidate: V3CanonicalMigrationCandidate,
        result: dict[str, Any],
        *,
        issue_type: str,
        decision: str,
        now_utc: str,
        company_id: int | None = None,
    ) -> dict[str, Any]:
        result["disposition"] = "REJECTED"
        result["row_outcomes"].extend(["CANDIDATE_REJECTED", "RESOLUTION_REQUIRED"])
        issue_id = self._create_issue_once(candidate, issue_type, quarter_id=None, field_name=None, now_utc=now_utc)
        result["issue_ids"].append(issue_id)
        self.audit.record_audit(
            migration_run_id=candidate.migration_run_id,
            source=candidate.source_system,
            source_key=candidate.source_record_id,
            audit_type="CANONICAL_APPLY",
            decision=decision,
            evidence=_result_evidence(result, candidate),
            company_id=company_id,
            quarter_id=None,
            now_utc=now_utc,
        )
        return result

    def _create_issue_once(
        self,
        candidate: V3CanonicalMigrationCandidate,
        issue_type: str,
        *,
        quarter_id: int | None,
        field_name: str | None,
        details: Mapping[str, Any] | None = None,
        now_utc: str,
    ) -> int:
        row = self.conn.execute(
            """
            SELECT issue_id
            FROM v3_resolution_issue
            WHERE status = 'ACTIVE'
              AND issue_type = ?
              AND COALESCE(quarter_id, -1) = COALESCE(?, -1)
              AND COALESCE(unresolved_market, '') = COALESCE(?, '')
              AND COALESCE(unresolved_ticker, '') = COALESCE(?, '')
              AND COALESCE(unresolved_fiscal_year, -1) = COALESCE(?, -1)
              AND COALESCE(unresolved_fiscal_quarter, '') = COALESCE(?, '')
              AND COALESCE(field_name, '') = COALESCE(?, '')
            """,
            (
                issue_type,
                quarter_id,
                candidate.market,
                candidate.ticker,
                candidate.fiscal_year,
                candidate.fiscal_quarter,
                field_name,
            ),
        ).fetchone()
        if row is not None:
            return int(row["issue_id"])
        payload = {
            "source": candidate.source_system,
            "source_record_id": candidate.source_record_id,
            "raw_evidence_ref": candidate.raw_evidence_ref,
            "details": dict(details or {}),
        }
        self.conn.execute(
            """
            INSERT INTO v3_resolution_issue (
                quarter_id, unresolved_market, unresolved_ticker, unresolved_fiscal_year,
                unresolved_fiscal_quarter, issue_type, field_name, status,
                source_details_json, created_at_utc, updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?)
            """,
            (
                quarter_id,
                candidate.market,
                candidate.ticker,
                candidate.fiscal_year,
                candidate.fiscal_quarter,
                issue_type,
                field_name,
                _json_dumps(payload),
                now_utc,
                now_utc,
            ),
        )
        return int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    def _merge_result(self, summary: V3CanonicalMigrationRunSummary, result: Mapping[str, Any]) -> None:
        if result["disposition"] == "REJECTED":
            summary.rows["candidate_rows_rejected"] += 1
        else:
            summary.rows["candidate_rows_accepted"] += 1
        for outcome in result["row_outcomes"]:
            if outcome == "COMPANY_CREATED":
                summary.rows["companies_created"] += 1
            elif outcome == "QUARTER_CREATED":
                summary.rows["canonical_quarters_created"] += 1
            elif outcome == "QUARTER_MATCHED":
                summary.rows["existing_canonical_quarters_matched"] += 1
            elif outcome == "RESOLUTION_REQUIRED":
                summary.rows["resolution_required"] += 1
        for outcome in result["metadata_outcomes"]:
            summary.metadata[outcome] += 1
        for field_name, outcomes in result["field_outcomes"].items():
            for outcome in outcomes:
                summary.add_field(field_name, outcome)
        for issue_id in result["issue_ids"]:
            summary.issues["resolution_issues_created_or_reused"] += 1
            row = self.conn.execute("SELECT issue_type FROM v3_resolution_issue WHERE issue_id = ?", (issue_id,)).fetchone()
            if row is not None:
                summary.issues[str(row["issue_type"])] += 1


def _candidate_values_with_derivations(candidate: V3CanonicalMigrationCandidate) -> tuple[dict[str, Any], set[str], dict[str, str]]:
    values = {field_name: candidate.values.get(field_name) for field_name in CANONICAL_FIELD_NAMES}
    derived: set[str] = set()
    notes: dict[str, str] = {}
    direct_with_check: dict[str, str] = {}
    fcf = derive_free_cashflow(values.get("operating_cashflow"), values.get("capex"))
    _apply_derived_value(values, derived, notes, direct_with_check, "free_cashflow", fcf, "operating_cashflow + capex")
    ebitda = derive_ordinary_ebitda(
        values.get("operating_income"),
        candidate.derivation_inputs.get("depreciation_amortization"),
    )
    _apply_derived_value(values, derived, notes, direct_with_check, "ebitda", ebitda, "operating_income + depreciation_amortization")
    debt = derive_total_debt(candidate.derivation_inputs.get("short_term_debt"), candidate.derivation_inputs.get("long_term_debt"))
    _apply_derived_value(values, derived, notes, direct_with_check, "total_debt", debt, "short_term_debt + long_term_debt")
    for field_name, note in direct_with_check.items():
        notes[field_name] = note
    return values, derived, notes


def _apply_derived_value(
    values: dict[str, Any],
    derived: set[str],
    notes: dict[str, str],
    direct_with_check: dict[str, str],
    field_name: str,
    derived_value: float | None,
    formula: str,
) -> None:
    if derived_value is None:
        return
    if values.get(field_name) is None:
        values[field_name] = derived_value
        derived.add(field_name)
        notes[field_name] = f"DERIVED:{formula}"
    else:
        direct_with_check[field_name] = f"DIRECT_WITH_DERIVED_CHECK:{formula}"


def _compare_numeric(existing: Any, incoming: Any, policy: V3FieldPolicy) -> str:
    left = float(existing)
    right = float(incoming)
    if left == right:
        return "same"
    diff = abs(left - right)
    if diff <= max(policy.absolute_tolerance, max(abs(left), abs(right)) * policy.relative_tolerance):
        return "rounding"
    if math.isclose(left, right, rel_tol=policy.relative_tolerance, abs_tol=policy.absolute_tolerance):
        return "rounding"
    return "conflict"


def _normalize_migration_source(value: str) -> str:
    source = str(value).strip().upper()
    if source not in PROVIDERS or source not in MIGRATION_SOURCES:
        raise ValueError(f"V3_CANONICAL_INVALID_SOURCE:{value}")
    return source


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(str(value))


def _json_dumps(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _compact_derivation_method(notes: Mapping[str, str]) -> str | None:
    if not notes:
        return None
    return _json_dumps(notes)


def _result_evidence(result: Mapping[str, Any], candidate: V3CanonicalMigrationCandidate) -> dict[str, Any]:
    return {
        "work_unit_key": candidate.work_unit_key,
        "row_outcomes": list(result["row_outcomes"]),
        "metadata_outcomes": list(result["metadata_outcomes"]),
        "field_outcomes": result["field_outcomes"],
        "issue_ids": list(result["issue_ids"]),
        "raw_evidence_ref": candidate.raw_evidence_ref,
        "value_metadata": dict(candidate.value_metadata),
    }
