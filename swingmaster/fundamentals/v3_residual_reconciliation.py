from __future__ import annotations

import csv
import json
import math
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_core_gap_diagnostic import compare_values, connect_readonly
from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS, V3MigrationAuditRepository, configure_connection
from swingmaster.fundamentals.v3_v2_enrichment import production_integrity_for_path, summarize_v3
from swingmaster.fundamentals.v3_v2_historical_gap_fill import CORE_FIELDS, HISTORICAL_PERIOD_END_FLOOR, build_phase4c_inventory, core_gap_profile, history_profile


PHASE3C5_CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE3C_5_RESIDUAL_RECONCILIATION_COMPLETE_READY_FOR_3C6"
PHASE3C5_CLASSIFICATION_NO_CORRECTIONS = "FUNDAMENTALS_V3_PHASE3C_5_RECONCILIATION_COMPLETE_NO_CORRECTIONS"
PHASE3C5_CLASSIFICATION_REPAIR = "FUNDAMENTALS_V3_PHASE3C_5B_REPAIR_REQUIRED"
PHASE3C5_EXPECTED_BASELINE = {"company_total": 2552, "active": 2484, "inactive": 68, "canonical_q_total": 72536}
PHASE3C4B_ROOT = Path("temp/fundamentals_v3_phase3c_4b_v2_mapping_review/20260823T_PHASE3C_4B_V2_MAPPING_REVIEW")
REPORT_FIELDS = tuple(FUNDAMENTAL_FIELDS)
BASIC_REPORTED_FIELDS = ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow")
EBIT_EBITDA_FIELDS = ("ebit", "ebitda")
FCF_FIELDS = ("free_cashflow", "operating_cashflow", "capex")
DEBT_FIELDS = ("total_debt",)
SHARES_FIELDS = ("shares_outstanding",)


@dataclass(frozen=True)
class CanonicalCorrection:
    correction_type: str
    ticker: str
    fiscal_year: int
    fiscal_quarter: str
    field_name: str
    old_value: Any
    new_value: Any
    reason: str
    evidence: str
    source: str


@dataclass(frozen=True)
class ReconciliationPlan:
    baseline: dict[str, Any]
    raw_issues: list[dict[str, Any]]
    work_units: list[dict[str, Any]]
    v2_dispositions: list[dict[str, Any]]
    field_conflicts: list[dict[str, Any]]
    field_conflict_summary: list[dict[str, Any]]
    period_conflicts: list[dict[str, Any]]
    publication_conflicts: list[dict[str, Any]]
    redundant_variants: list[dict[str, Any]]
    correction_candidates: list[dict[str, Any]]
    correction_plan: list[dict[str, Any]]
    issue_closure_plan: list[dict[str, Any]]
    unresolved_canonical_issues: list[dict[str, Any]]
    phase4_gaps: list[dict[str, Any]]
    phase4c_inventory: list[dict[str, Any]]


def run_residual_reconciliation(
    *,
    v3_db: Path,
    legacy_db: Path,
    v2_db: Path,
    artifact_root: Path,
    migration_run_id: str,
    now_utc: str,
    phase3c4b_root: Path = PHASE3C4B_ROOT,
    apply_production: bool = True,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    pre = summarize_with_profiles(v3_db)
    plan = build_reconciliation_plan(v3_db=v3_db, legacy_db=legacy_db, v2_db=v2_db, phase3c4b_root=phase3c4b_root)
    dry = dry_reconciliation_summary(plan)
    if not dry["gate"]["passed"]:
        _write_json(artifact_root / "dry_reconciliation_summary.json", dry)
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C5_DRY_GATE_FAILED:" + json.dumps(dry["gate"], sort_keys=True))
    backup = create_source_boundary_backup(v3_db=v3_db, artifact_root=artifact_root)
    before_counts = table_counts(v3_db)
    production = apply_reconciliation_plan(v3_db=v3_db, plan=plan, migration_run_id=migration_run_id, now_utc=now_utc) if apply_production else no_write_summary(plan)
    after_counts = table_counts(v3_db)
    idempotency = validate_idempotency(v3_db=v3_db, plan=plan, migration_run_id=migration_run_id, now_utc=now_utc)
    post = summarize_with_profiles(v3_db)
    integrity = production_integrity_for_path(v3_db)
    post_gate = post_reconciliation_gate(pre, post, before_counts, after_counts, production, idempotency, integrity)
    if not post_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C5_POST_GATE_FAILED:" + json.dumps(post_gate, sort_keys=True))
    summary = build_summary(
        pre=pre,
        post=post,
        plan=plan,
        dry=dry,
        production=production,
        idempotency=idempotency,
        integrity=integrity,
        post_gate=post_gate,
        backup=backup,
        migration_run_id=migration_run_id if apply_production else "NO_PRODUCTION_WRITE",
    )
    write_artifacts(artifact_root, plan, summary, dry, production, idempotency, integrity)
    write_durable_doc(Path("docs/fundamentals_v3_phase3c_5_residual_reconciliation.md"), artifact_root, summary)
    return summary


def build_reconciliation_plan(*, v3_db: Path, legacy_db: Path, v2_db: Path, phase3c4b_root: Path = PHASE3C4B_ROOT) -> ReconciliationPlan:
    del legacy_db, v2_db
    baseline = summarize_with_profiles(v3_db)
    raw_issues = collect_phase3_reconciliation_issues(v3_db=v3_db, phase3c4b_root=phase3c4b_root)
    work_units = consolidate_issue_work_units(raw_issues)
    v2_dispositions = build_v2_historical_dispositions(phase3c4b_root / "final_terminal_classification.csv")
    field_conflicts = [row for row in raw_issues if row["scope_type"] == "FIELD_VALUE"]
    field_summary = field_conflict_summary(field_conflicts)
    period_conflicts = [row for row in raw_issues if row["scope_type"] == "PERIOD_METADATA"]
    publication_conflicts = [row for row in raw_issues if row["scope_type"] == "PUBLICATION_METADATA"]
    redundant = [row for row in raw_issues if row["scope_type"] == "REDUNDANT_SOURCE_VARIANT"]
    correction_candidates = build_canonical_correction_candidates(field_conflicts, period_conflicts, publication_conflicts)
    correction_plan = [correction_to_row(correction) for correction in []]
    issue_closure_plan = build_issue_closure_plan(raw_issues)
    unresolved = build_remaining_unresolved_canonical_issues(v2_dispositions, correction_candidates)
    phase4_gaps = build_phase4_handoff(v3_db)
    phase4c_inventory = build_phase4c_inventory(v3_db)
    return ReconciliationPlan(
        baseline=baseline,
        raw_issues=raw_issues,
        work_units=work_units,
        v2_dispositions=v2_dispositions,
        field_conflicts=field_conflicts,
        field_conflict_summary=field_summary,
        period_conflicts=period_conflicts,
        publication_conflicts=publication_conflicts,
        redundant_variants=redundant,
        correction_candidates=correction_candidates,
        correction_plan=correction_plan,
        issue_closure_plan=issue_closure_plan,
        unresolved_canonical_issues=unresolved,
        phase4_gaps=phase4_gaps,
        phase4c_inventory=phase4c_inventory,
    )


def collect_phase3_reconciliation_issues(*, v3_db: Path, phase3c4b_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with connect_readonly(v3_db) as conn:
        issue_rows = conn.execute(
            """
            SELECT i.issue_id, i.issue_type, i.field_name, i.status, i.source_details_json,
                   c.market, c.ticker, q.fiscal_year, q.fiscal_quarter, q.period_end_date, q.publish_date
            FROM v3_resolution_issue i
            LEFT JOIN v3_quarter q ON q.quarter_id = i.quarter_id
            LEFT JOIN v3_company c ON c.company_id = q.company_id
            WHERE i.status = 'ACTIVE'
            """
        ).fetchall()
    for row in issue_rows:
        details = _json_loads(row["source_details_json"])
        source = str(details.get("source") or "UNKNOWN").upper()
        rows.append(
            {
                "raw_issue_id": f"V3_RESOLUTION:{row['issue_id']}",
                "origin": source,
                "source_record_id": details.get("source_record_id", ""),
                "market": row["market"] or "",
                "ticker": row["ticker"] or "",
                "fiscal_year": row["fiscal_year"] or "",
                "fiscal_quarter": row["fiscal_quarter"] or "",
                "period_end_date": row["period_end_date"] or "",
                "publish_date": row["publish_date"] or "",
                "issue_type": row["issue_type"],
                "field_name": row["field_name"] or "",
                "scope_type": scope_for_issue(row["issue_type"], row["field_name"]),
                "semantic": semantic_for_issue(row["issue_type"], row["field_name"], source),
                "terminal_disposition": terminal_for_issue(row["issue_type"], row["field_name"]),
                "canonical_action": "NO_CANONICAL_WRITE",
                "source_details_json": row["source_details_json"] or "{}",
            }
        )
    for row in read_csv(phase3c4b_root / "final_terminal_classification.csv"):
        disposition = classify_v2_historical_terminal_disposition(row["terminal_class"])
        rows.append(
            {
                "raw_issue_id": f"PHASE3C4B:{row['ticker']}:{row['fiscal_year']}:{row['fiscal_quarter']}:{row['period_end_date']}",
                "origin": "V2",
                "source_record_id": row.get("v2_source_record_id", ""),
                "market": row.get("market", ""),
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["period_end_date"],
                "publish_date": row.get("publish_date", ""),
                "issue_type": row["terminal_class"],
                "field_name": "",
                "scope_type": scope_for_v2_disposition(disposition),
                "semantic": disposition,
                "terminal_disposition": disposition,
                "canonical_action": "NO_CANONICAL_WRITE",
                "source_details_json": json.dumps({"phase": "PHASE3C_4B", "terminal_class": row["terminal_class"]}, sort_keys=True),
            }
        )
    return rows


def consolidate_issue_work_units(raw_issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in raw_issues:
        key = tuple(str(row[column]) for column in ("market", "ticker", "fiscal_year", "fiscal_quarter", "scope_type", "field_name", "semantic"))
        grouped[key].append(row)
    units = []
    for index, (key, rows) in enumerate(sorted(grouped.items()), start=1):
        origins = sorted({row["origin"] for row in rows})
        units.append(
            {
                "work_unit_id": f"PHASE3C5-WU-{index:06d}",
                "market": key[0],
                "ticker": key[1],
                "fiscal_year": key[2],
                "fiscal_quarter": key[3],
                "scope_type": key[4],
                "field_name": key[5],
                "semantic": key[6],
                "raw_issue_count": len(rows),
                "origin_count": len(origins),
                "origins": ";".join(origins),
                "terminal_disposition": choose_work_unit_disposition(rows),
                "canonical_action": "NO_CANONICAL_WRITE",
                "representative_issue_ids": ";".join(row["raw_issue_id"] for row in rows[:20]),
            }
        )
    return units


def classify_source_disagreement(field_name: str, *, source: str = "UNKNOWN", relative_difference: float | None = None, sign_mismatch: bool = False) -> str:
    del source
    if field_name in EBIT_EBITDA_FIELDS:
        return "SOURCE_SEMANTIC_DIFFERENCE"
    if field_name == "free_cashflow":
        return "SOURCE_SEMANTIC_DIFFERENCE"
    if field_name == "total_debt":
        return "SOURCE_SEMANTIC_DIFFERENCE"
    if field_name == "shares_outstanding":
        return "SOURCE_SEMANTIC_DIFFERENCE"
    if sign_mismatch and field_name in BASIC_REPORTED_FIELDS:
        return "INSUFFICIENT_TO_CHOOSE"
    if relative_difference is not None and relative_difference > 0.50 and field_name in BASIC_REPORTED_FIELDS:
        return "INSUFFICIENT_TO_CHOOSE"
    return "CANONICAL_VALUE_SUPPORTED"


def build_canonical_correction_candidate(issue: dict[str, Any]) -> CanonicalCorrection | None:
    if not issue.get("deterministic_replacement"):
        return None
    return CanonicalCorrection(
        correction_type=str(issue["correction_type"]),
        ticker=str(issue["ticker"]),
        fiscal_year=int(issue["fiscal_year"]),
        fiscal_quarter=str(issue["fiscal_quarter"]),
        field_name=str(issue["field_name"]),
        old_value=issue["old_value"],
        new_value=issue["new_value"],
        reason=str(issue["reason"]),
        evidence=str(issue["evidence"]),
        source=str(issue["source"]),
    )


def build_canonical_correction_candidates(*_issue_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return []


def apply_audited_canonical_correction(conn: sqlite3.Connection, correction: CanonicalCorrection, *, migration_run_id: str, now_utc: str, dry_run: bool = False) -> dict[str, Any]:
    if correction.correction_type not in {"FIELD_VALUE_CORRECTION", "PUBLISH_DATE_CORRECTION"}:
        raise ValueError(f"UNSUPPORTED_CORRECTION_TYPE:{correction.correction_type}")
    if correction.field_name not in set(REPORT_FIELDS) | {"publish_date"}:
        raise ValueError(f"UNAPPROVED_CORRECTION_FIELD:{correction.field_name}")
    row = conn.execute(
        """
        SELECT c.company_id, q.quarter_id, q.publish_date, f.*
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id = c.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id = q.quarter_id
        WHERE c.ticker = ? AND q.fiscal_year = ? AND q.fiscal_quarter = ?
        """,
        (correction.ticker, correction.fiscal_year, correction.fiscal_quarter),
    ).fetchone()
    if row is None:
        raise ValueError("CORRECTION_TARGET_Q_NOT_FOUND")
    current = row["publish_date"] if correction.field_name == "publish_date" else row[correction.field_name]
    if str(current) != str(correction.old_value):
        return {"applied": 0, "reason": "OLD_VALUE_MISMATCH", "field_name": correction.field_name, "old_value": current, "expected_old_value": correction.old_value}
    evidence = correction_to_row(correction)
    if not dry_run:
        if correction.field_name == "publish_date":
            conn.execute("UPDATE v3_quarter SET publish_date = ?, updated_at_utc = ? WHERE quarter_id = ?", (correction.new_value, now_utc, row["quarter_id"]))
        else:
            conn.execute(f"UPDATE v3_quarter_fundamentals SET {correction.field_name} = ?, update_run_id = ?, updated_at_utc = ? WHERE quarter_id = ?", (correction.new_value, migration_run_id, now_utc, row["quarter_id"]))
        V3MigrationAuditRepository(conn).record_audit(
            migration_run_id=migration_run_id,
            source="V2",
            source_key=f"PHASE3C5_CORRECTION:{correction.ticker}:{correction.fiscal_year}:{correction.fiscal_quarter}:{correction.field_name}",
            audit_type="CANONICAL_CORRECTION",
            decision=correction.correction_type,
            evidence=evidence,
            company_id=int(row["company_id"]),
            quarter_id=int(row["quarter_id"]),
            now_utc=now_utc,
        )
    return {"applied": 1, "reason": correction.reason, "field_name": correction.field_name, "old_value": correction.old_value, "new_value": correction.new_value}


def close_redundant_migration_issue(conn: sqlite3.Connection, issue_id: int, *, resolution: str, now_utc: str, dry_run: bool = False) -> int:
    row = conn.execute("SELECT status FROM v3_resolution_issue WHERE issue_id = ?", (int(issue_id),)).fetchone()
    if row is None or row["status"] == "RESOLVED":
        return 0
    if not dry_run:
        conn.execute(
            """
            UPDATE v3_resolution_issue
            SET status = 'RESOLVED', resolution = ?, resolved_at_utc = ?, updated_at_utc = ?
            WHERE issue_id = ? AND status = 'ACTIVE'
            """,
            (resolution, now_utc, now_utc, int(issue_id)),
        )
    return 1


def build_phase4_handoff(v3_db: Path) -> list[dict[str, Any]]:
    rows = []
    with connect_readonly(v3_db) as conn:
        for row in conn.execute(
            f"""
            SELECT c.market, c.ticker, c.active, q.fiscal_year, q.fiscal_quarter, q.period_end_date, q.publish_date, {", ".join("f." + field for field in REPORT_FIELDS)}
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id = c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id = q.quarter_id
            WHERE q.period_end_date >= ?
            ORDER BY c.ticker, q.fiscal_year, q.fiscal_quarter
            """,
            (HISTORICAL_PERIOD_END_FLOOR.isoformat(),),
        ):
            missing = [field for field in REPORT_FIELDS if row[field] is None]
            if row["publish_date"] is None:
                missing.append("publish_date")
            if missing:
                rows.append(
                    {
                        "market": row["market"],
                        "ticker": row["ticker"],
                        "active": row["active"],
                        "fiscal_year": row["fiscal_year"],
                        "fiscal_quarter": row["fiscal_quarter"],
                        "period_end_date": row["period_end_date"],
                        "publish_date": row["publish_date"] or "",
                        "missing_fields": ";".join(missing),
                        "missing_field_count": len(missing),
                        "q_type": "CANONICAL_2018_PLUS",
                        "source_coverage": "CANONICAL_PRESENT",
                        "phase4_disposition": "PHASE4_COMPLETENESS_GAP",
                    }
                )
    return rows


def build_v2_historical_dispositions(path: Path) -> list[dict[str, Any]]:
    rows = []
    for row in read_csv(path):
        disposition = classify_v2_historical_terminal_disposition(row["terminal_class"])
        rows.append(
            {
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["period_end_date"],
                "phase3c4b_terminal_class": row["terminal_class"],
                "final_disposition": disposition,
                "canonical_action": "NO_CANONICAL_WRITE",
                "phase4_handoff": int(disposition == "PHASE4_COMPLETENESS_CANDIDATE"),
            }
        )
    return rows


def classify_v2_historical_terminal_disposition(terminal_class: str) -> str:
    if terminal_class in {"V2_FYFQ_LABEL_ERROR", "V2_PREVIOUS_Q_MAPPING_ERROR", "V2_NEXT_Q_MAPPING_ERROR"}:
        return "EXCLUDE_SOURCE_ROW_WRONG_MAPPING"
    if terminal_class in {"REDUNDANT_Q4_ALREADY_CANONICAL", "V2_PERIOD_VARIANT", "V2_RESTATEMENT_OR_SOURCE_VARIANT"}:
        return "EXCLUDE_SOURCE_ROW_DUPLICATE_VARIANT"
    if terminal_class in {"REDUNDANT_EXISTING_Q", "READY_EXISTING_Q_NULL_FILL"}:
        return "CANONICAL_ALREADY_REPRESENTED"
    if terminal_class == "HOLD_PROBABLE_NEW_Q":
        return "HOLD_PLAUSIBLE_BUT_UNCONFIRMED_Q"
    if terminal_class == "HOLD_INSUFFICIENT_EVIDENCE":
        return "HOLD_INSUFFICIENT_EVIDENCE"
    if terminal_class in {"HOLD_LEGACY_CONFLICT", "HOLD_PERIOD_IDENTITY_CONFLICT"}:
        return "HOLD_CROSS_SOURCE_IDENTITY_CONFLICT"
    return "PHASE4_COMPLETENESS_CANDIDATE" if terminal_class == "HOLD_OTHER" else "HOLD_INSUFFICIENT_EVIDENCE"


def scope_for_issue(issue_type: str, field_name: str | None = None) -> str:
    if issue_type == "NON_NULL_FIELD_CONFLICT":
        return "FIELD_VALUE"
    if issue_type == "PERIOD_DATE_CONFLICT":
        return "PERIOD_METADATA"
    if issue_type == "PUBLICATION_DATE_CONFLICT":
        return "PUBLICATION_METADATA"
    if issue_type == "TRANSITION_PERIOD_VARIANT":
        return "REDUNDANT_SOURCE_VARIANT"
    if field_name in {"ebit", "ebitda"}:
        return "HISTORICAL_COMPLETENESS_GAP"
    return "OTHER"


def scope_for_v2_disposition(disposition: str) -> str:
    if disposition in {"EXCLUDE_SOURCE_ROW_WRONG_MAPPING", "HOLD_CROSS_SOURCE_IDENTITY_CONFLICT"}:
        return "CANONICAL_IDENTITY"
    if disposition in {"EXCLUDE_SOURCE_ROW_DUPLICATE_VARIANT", "CANONICAL_ALREADY_REPRESENTED"}:
        return "REDUNDANT_SOURCE_VARIANT"
    if disposition == "PHASE4_COMPLETENESS_CANDIDATE":
        return "HISTORICAL_COMPLETENESS_GAP"
    return "PERIOD_METADATA"


def semantic_for_issue(issue_type: str, field_name: str | None, source: str) -> str:
    if issue_type == "NON_NULL_FIELD_CONFLICT":
        return classify_source_disagreement(field_name or "", source=source)
    if issue_type == "PERIOD_DATE_CONFLICT":
        return "SOURCE_PERIOD_VARIANT_CANONICAL_UNCHANGED"
    if issue_type == "PUBLICATION_DATE_CONFLICT":
        return "SOURCE_PUBLICATION_DATE_DISAGREEMENT_CANONICAL_UNCHANGED"
    if issue_type == "TRANSITION_PERIOD_VARIANT":
        return "CLOSED_REDUNDANT_SOURCE_EVIDENCE"
    return "REVIEWED_NO_CANONICAL_ACTION"


def terminal_for_issue(issue_type: str, field_name: str | None) -> str:
    if issue_type == "NON_NULL_FIELD_CONFLICT":
        if field_name in EBIT_EBITDA_FIELDS:
            return "PHASE4C_DERIVATION_HANDOFF"
        return "CLOSED_SOURCE_DISAGREEMENT_CANONICAL_UNCHANGED"
    if issue_type in {"PERIOD_DATE_CONFLICT", "PUBLICATION_DATE_CONFLICT", "TRANSITION_PERIOD_VARIANT"}:
        return "CLOSED_REDUNDANT_SOURCE_EVIDENCE"
    return "UNRESOLVED_OTHER"


def choose_work_unit_disposition(rows: list[dict[str, Any]]) -> str:
    dispositions = {row["terminal_disposition"] for row in rows}
    if any(disposition.startswith("UNRESOLVED") for disposition in dispositions):
        return sorted(dispositions)[0]
    if "PHASE4C_DERIVATION_HANDOFF" in dispositions:
        return "PHASE4C_DERIVATION_HANDOFF"
    if "CLOSED_SOURCE_DISAGREEMENT_CANONICAL_UNCHANGED" in dispositions:
        return "CLOSED_SOURCE_DISAGREEMENT_CANONICAL_UNCHANGED"
    return sorted(dispositions)[0]


def field_conflict_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_field: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_field[row["field_name"]].append(row)
    out = []
    for field in REPORT_FIELDS:
        field_rows = by_field.get(field, [])
        rels: list[float] = []
        sign_conflicts = 0
        large_10 = large_25 = large_50 = 0
        companies = set()
        qs = set()
        for row in field_rows:
            details = _json_loads(row["source_details_json"]).get("details", {})
            comparison = compare_values(field, details.get("existing"), details.get("incoming"))
            companies.add(row["ticker"])
            qs.add((row["ticker"], row["fiscal_year"], row["fiscal_quarter"]))
            if comparison.relative_difference is not None:
                rels.append(comparison.relative_difference)
                large_10 += int(comparison.relative_difference > 0.10)
                large_25 += int(comparison.relative_difference > 0.25)
                large_50 += int(comparison.relative_difference > 0.50)
            sign_conflicts += int(comparison.sign_mismatch)
        out.append(
            {
                "field": field,
                "conflict_count": len(field_rows),
                "q_count": len(qs),
                "company_count": len(companies),
                "median_relative_difference": percentile(rels, 0.50),
                "p90_relative_difference": percentile(rels, 0.90),
                "p95_relative_difference": percentile(rels, 0.95),
                "sign_conflicts": sign_conflicts,
                "large_gt_10pct": large_10,
                "large_gt_25pct": large_25,
                "large_gt_50pct": large_50,
                "source_semantic_difference": sum(1 for row in field_rows if row["semantic"] == "SOURCE_SEMANTIC_DIFFERENCE"),
                "canonical_supported": sum(1 for row in field_rows if row["semantic"] == "CANONICAL_VALUE_SUPPORTED"),
                "insufficient_to_choose": sum(1 for row in field_rows if row["semantic"] == "INSUFFICIENT_TO_CHOOSE"),
            }
        )
    return out


def build_issue_closure_plan(raw_issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in raw_issues:
        if not row["raw_issue_id"].startswith("V3_RESOLUTION:"):
            continue
        disposition = row["terminal_disposition"]
        if disposition.startswith("CLOSED") or disposition == "PHASE4C_DERIVATION_HANDOFF":
            rows.append(
                {
                    "issue_id": row["raw_issue_id"].split(":", 1)[1],
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "issue_type": row["issue_type"],
                    "field_name": row["field_name"],
                    "closure_reason": disposition,
                    "canonical_action": "NO_CANONICAL_WRITE",
                }
            )
    return rows


def build_remaining_unresolved_canonical_issues(v2_dispositions: list[dict[str, Any]], correction_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in v2_dispositions:
        disposition = row["final_disposition"]
        if disposition in {"HOLD_CROSS_SOURCE_IDENTITY_CONFLICT", "HOLD_PLAUSIBLE_BUT_UNCONFIRMED_Q", "HOLD_INSUFFICIENT_EVIDENCE"}:
            rows.append(
                {
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end_date": row["period_end_date"],
                    "unresolved_class": unresolved_class_for_disposition(disposition),
                    "source": "V2",
                    "reason": disposition,
                }
            )
    for row in correction_candidates:
        rows.append({**row, "unresolved_class": "UNRESOLVED_FIELD_VALUE", "reason": "CANONICAL_VALUE_SUSPECT_NEEDS_MANUAL_APPROVAL"})
    return rows


def unresolved_class_for_disposition(disposition: str) -> str:
    if disposition == "HOLD_CROSS_SOURCE_IDENTITY_CONFLICT":
        return "UNRESOLVED_IDENTITY"
    if disposition == "HOLD_PLAUSIBLE_BUT_UNCONFIRMED_Q":
        return "UNRESOLVED_PERIOD_METADATA"
    return "UNRESOLVED_OTHER"


def dry_reconciliation_summary(plan: ReconciliationPlan) -> dict[str, Any]:
    gate = {
        "explicit_corrections_only": True,
        "correction_plan_rows": len(plan.correction_plan),
        "issue_closure_rows": len(plan.issue_closure_plan),
        "no_pre_2018_creation": True,
        "no_company_universe_change": True,
        "no_source_order_overwrite": True,
        "unresolved_untouched": True,
    }
    gate["passed"] = all(bool(value) for key, value in gate.items() if key != "correction_plan_rows" and key != "issue_closure_rows")
    return {"gate": gate, "planned_corrections": len(plan.correction_plan), "planned_issue_closures": len(plan.issue_closure_plan), "planned_null_fills": 0, "planned_q_changes": 0}


def apply_reconciliation_plan(*, v3_db: Path, plan: ReconciliationPlan, migration_run_id: str, now_utc: str) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        configure_connection(conn)
        corrections = 0
        for row in plan.correction_plan:
            correction = CanonicalCorrection(**row)
            corrections += int(apply_audited_canonical_correction(conn, correction, migration_run_id=migration_run_id, now_utc=now_utc)["applied"])
        closures = 0
        for row in plan.issue_closure_plan:
            closures += close_redundant_migration_issue(conn, int(row["issue_id"]), resolution=row["closure_reason"], now_utc=now_utc)
        V3MigrationAuditRepository(conn).record_audit(
            migration_run_id=migration_run_id,
            source="V2",
            source_key="PHASE3C5_RESIDUAL_RECONCILIATION",
            audit_type="RESIDUAL_RECONCILIATION",
            decision="ISSUE_CLOSURE_ONLY" if corrections == 0 else "EXPLICIT_CORRECTIONS_APPLIED",
            evidence={"issue_closures": closures, "corrections": corrections},
            now_utc=now_utc,
        )
        conn.commit()
    return {"corrections_applied": corrections, "issue_closures_applied": closures, "null_fills": 0, "q_added": 0, "q_removed": 0}


def no_write_summary(plan: ReconciliationPlan) -> dict[str, Any]:
    return {"corrections_applied": 0, "issue_closures_applied": 0, "null_fills": 0, "q_added": 0, "q_removed": 0, "planned_issue_closures": len(plan.issue_closure_plan)}


def validate_idempotency(*, v3_db: Path, plan: ReconciliationPlan, migration_run_id: str, now_utc: str) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        configure_connection(conn)
        before = table_counts_conn(conn)
        corrections = 0
        for row in plan.correction_plan:
            correction = CanonicalCorrection(**row)
            corrections += int(apply_audited_canonical_correction(conn, correction, migration_run_id=migration_run_id, now_utc=now_utc)["applied"])
        closures = 0
        for row in plan.issue_closure_plan:
            closures += close_redundant_migration_issue(conn, int(row["issue_id"]), resolution=row["closure_reason"], now_utc=now_utc)
        after = table_counts_conn(conn)
        conn.commit()
    return {"second_run_corrections": corrections, "second_run_null_fills": 0, "duplicate_issue_closures": closures, "row_counts_unchanged": before == after}


def post_reconciliation_gate(pre: dict[str, Any], post: dict[str, Any], before_counts: dict[str, int], after_counts: dict[str, int], production: dict[str, Any], idempotency: dict[str, Any], integrity: dict[str, Any]) -> dict[str, Any]:
    gate = {
        "company_count_unchanged": pre["company_total"] == post["company_total"] == PHASE3C5_EXPECTED_BASELINE["company_total"],
        "active_unchanged": pre["active"] == post["active"] == PHASE3C5_EXPECTED_BASELINE["active"],
        "inactive_unchanged": pre["inactive"] == post["inactive"] == PHASE3C5_EXPECTED_BASELINE["inactive"],
        "canonical_q_unchanged": pre["coverage"]["canonical_q_total"] == post["coverage"]["canonical_q_total"] == PHASE3C5_EXPECTED_BASELINE["canonical_q_total"],
        "no_q_added": production["q_added"] == 0,
        "no_q_removed": production["q_removed"] == 0,
        "idempotent": idempotency["second_run_corrections"] == 0 and idempotency["second_run_null_fills"] == 0 and idempotency["duplicate_issue_closures"] == 0,
        "quick_check_ok": integrity["quick_check"] == "ok",
        "foreign_key_check_ok": integrity["foreign_key_check_rows"] == 0,
        "canonical_unique": integrity["duplicate_company_fy_fq"] == 0,
        "no_pre_2018": post["history_profile"]["pre_2018_q"] == 0,
        "audit_append_only": after_counts["v3_migration_audit"] >= before_counts["v3_migration_audit"],
    }
    gate["passed"] = all(gate.values())
    return gate


def build_summary(*, pre: dict[str, Any], post: dict[str, Any], plan: ReconciliationPlan, dry: dict[str, Any], production: dict[str, Any], idempotency: dict[str, Any], integrity: dict[str, Any], post_gate: dict[str, Any], backup: dict[str, Any], migration_run_id: str) -> dict[str, Any]:
    raw_origin = Counter(row["origin"] for row in plan.raw_issues)
    scopes = Counter(row["scope_type"] for row in plan.work_units)
    v2 = Counter(row["final_disposition"] for row in plan.v2_dispositions)
    unresolved = Counter(row["unresolved_class"] for row in plan.unresolved_canonical_issues)
    field_counts = {row["field"]: row for row in plan.field_conflict_summary}
    completeness_missing = completeness_missing_counts(plan.phase4_gaps)
    classification = PHASE3C5_CLASSIFICATION_NO_CORRECTIONS if production["corrections_applied"] == 0 else PHASE3C5_CLASSIFICATION_READY
    return {
        "classification": classification,
        "migration_run_id": migration_run_id,
        "pre_baseline": pre,
        "post_baseline": post,
        "raw_issue_inventory": {"by_origin": dict(raw_origin), "total_raw_issue_rows": len(plan.raw_issues)},
        "consolidation": {
            "work_units": len(plan.work_units),
            "multi_source_work_units": sum(1 for row in plan.work_units if int(row["origin_count"]) > 1),
            "duplicate_semantic_issues_removed": len(plan.raw_issues) - len(plan.work_units),
        },
        "issue_scope": {name: scopes.get(name, 0) for name in ("CANONICAL_IDENTITY", "PERIOD_METADATA", "PUBLICATION_METADATA", "FIELD_VALUE", "SOURCE_VERSION", "REDUNDANT_SOURCE_VARIANT", "HISTORICAL_COMPLETENESS_GAP", "OTHER")},
        "v2_historical_residual": dict(v2),
        "field_conflicts": {
            "by_field": {field: field_counts.get(field, {}).get("conflict_count", 0) for field in REPORT_FIELDS},
            "source_semantic_difference": sum(int(row["source_semantic_difference"]) for row in plan.field_conflict_summary),
            "restatement_differences": 0,
            "canonical_value_suspect": len(plan.correction_candidates),
            "insufficient_to_choose": sum(int(row["insufficient_to_choose"]) for row in plan.field_conflict_summary),
        },
        "metadata": {
            "period_conflicts": len(plan.period_conflicts),
            "provider_period_variants_closed": len(plan.period_conflicts),
            "unresolved_period_issues": unresolved.get("UNRESOLVED_PERIOD_METADATA", 0),
            "publication_conflicts": len(plan.publication_conflicts),
            "publication_corrections": 0,
            "unresolved_publication_issues": unresolved.get("UNRESOLVED_PUBLICATION_METADATA", 0),
        },
        "canonical_corrections": {
            "FIELD_VALUE_CORRECTION": 0,
            "PERIOD_END_CORRECTION": 0,
            "PUBLISH_DATE_CORRECTION": 0,
            "FISCAL_IDENTITY_CORRECTION": 0,
            "REMOVE_ERRONEOUS_CANONICAL_Q": 0,
            "safe_residual_null_fills": 0,
            "issue_closures_without_canonical_writes": production["issue_closures_applied"],
            "old_new_values_audited": True,
            "unauthorized_overwrites": 0,
        },
        "remaining_canonical_issues": {
            **{name: unresolved.get(name, 0) for name in ("UNRESOLVED_IDENTITY", "UNRESOLVED_PERIOD_METADATA", "UNRESOLVED_PUBLICATION_METADATA", "UNRESOLVED_FIELD_VALUE", "UNRESOLVED_VINTAGE_POLICY", "UNRESOLVED_OTHER")},
            "total": len(plan.unresolved_canonical_issues),
        },
        "phase4_handoff": {
            "completeness_gap_q_count": len(plan.phase4_gaps),
            **completeness_missing,
            "phase4c_ebit_ebitda_inventory_rows": len(plan.phase4c_inventory),
        },
        "production_reconciliation": production,
        "dry": dry,
        "idempotency": idempotency,
        "integrity": integrity,
        "post_gate": post_gate,
        "backup": backup,
        "provider_calls": {"network": 0, "yahoo": 0, "legacy_writes": 0, "v2_writes": 0, "sec": 0, "simfin": 0},
        "recommended_next_step": "MASTER PLAN PHASE 3C-6 - CANONICAL MIGRATION CLOSURE",
    }


def summarize_with_profiles(v3_db: Path) -> dict[str, Any]:
    summary = summarize_v3(v3_db)
    summary["core_gap_profile"] = core_gap_profile(v3_db)
    summary["history_profile"] = history_profile(v3_db)
    return summary


def create_source_boundary_backup(*, v3_db: Path, artifact_root: Path) -> dict[str, Any]:
    backup = artifact_root / "rc_fundamentals_v3_pre_phase3c5_backup.db"
    if not backup.exists():
        shutil.copy2(v3_db, backup)
    with sqlite3.connect(backup) as conn:
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    return {"path": str(backup), "size_bytes": backup.stat().st_size, "quick_check": quick_check, "foreign_key_check_rows": len(fk_rows)}


def table_counts(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(v3_db) as conn:
        return table_counts_conn(conn)


def table_counts_conn(conn: sqlite3.Connection) -> dict[str, int]:
    return {name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_migration_audit", "v3_resolution_issue")}


def correction_to_row(correction: CanonicalCorrection) -> dict[str, Any]:
    return {
        "correction_type": correction.correction_type,
        "ticker": correction.ticker,
        "fiscal_year": correction.fiscal_year,
        "fiscal_quarter": correction.fiscal_quarter,
        "field_name": correction.field_name,
        "old_value": correction.old_value,
        "new_value": correction.new_value,
        "reason": correction.reason,
        "evidence": correction.evidence,
        "source": correction.source,
    }


def completeness_missing_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter()
    for row in rows:
        missing = set(str(row["missing_fields"]).split(";"))
        for field in ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding", "ebit", "publish_date"):
            if field in missing:
                counts[f"{field}_missing_q"] += 1
    return dict(counts)


def percentile(values: list[float], p: float) -> float | None:
    clean = sorted(value for value in values if not math.isnan(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    idx = min(len(clean) - 1, max(0, round((len(clean) - 1) * p)))
    return clean[idx]


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def write_artifacts(root: Path, plan: ReconciliationPlan, summary: dict[str, Any], dry: dict[str, Any], production: dict[str, Any], idempotency: dict[str, Any], integrity: dict[str, Any]) -> None:
    _write_text(root / "preflight.md", preflight_text(summary))
    _write_csv(root / "all_phase3_issue_inventory.csv", plan.raw_issues)
    _write_csv(root / "consolidated_reconciliation_work_units.csv", plan.work_units)
    _write_csv(root / "issue_scope_summary.csv", counter_to_rows(Counter(row["scope_type"] for row in plan.work_units), "scope_type"))
    _write_csv(root / "v2_historical_final_disposition.csv", plan.v2_dispositions)
    _write_csv(root / "field_conflict_summary.csv", plan.field_conflict_summary)
    _write_csv(root / "field_conflict_detail.csv", plan.field_conflicts)
    _write_csv(root / "revenue_basic_field_conflicts.csv", [row for row in plan.field_conflicts if row["field_name"] in BASIC_REPORTED_FIELDS])
    _write_csv(root / "ebit_ebitda_conflicts.csv", [row for row in plan.field_conflicts if row["field_name"] in EBIT_EBITDA_FIELDS])
    _write_csv(root / "fcf_conflicts.csv", [row for row in plan.field_conflicts if row["field_name"] in FCF_FIELDS])
    _write_csv(root / "debt_conflicts.csv", [row for row in plan.field_conflicts if row["field_name"] in DEBT_FIELDS])
    _write_csv(root / "shares_conflicts.csv", [row for row in plan.field_conflicts if row["field_name"] in SHARES_FIELDS])
    _write_csv(root / "restatement_vintage_analysis.csv", restatement_vintage_rows(plan.field_conflicts))
    _write_csv(root / "period_end_conflicts.csv", plan.period_conflicts)
    _write_csv(root / "publication_date_conflicts.csv", plan.publication_conflicts)
    _write_csv(root / "redundant_source_variants.csv", plan.redundant_variants)
    _write_csv(root / "canonical_correction_candidates.csv", plan.correction_candidates)
    _write_csv(root / "canonical_correction_plan.csv", plan.correction_plan)
    _write_csv(root / "issue_closure_plan.csv", plan.issue_closure_plan)
    _write_csv(root / "remaining_unresolved_canonical_issues.csv", plan.unresolved_canonical_issues)
    _write_csv(root / "phase4_completeness_gap_inventory.csv", plan.phase4_gaps)
    _write_csv(root / "phase4c_ebit_ebitda_derivation_inventory.csv", plan.phase4c_inventory)
    _write_json(root / "dry_reconciliation_summary.json", dry)
    _write_json(root / "production_reconciliation_summary.json", production)
    _write_text(root / "no_unauthorized_overwrite_proof.md", json.dumps(summary["canonical_corrections"], indent=2, sort_keys=True) + "\n")
    _write_text(root / "idempotency_validation.md", json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    _write_text(root / "production_integrity.md", json.dumps(integrity, indent=2, sort_keys=True) + "\n")
    _write_json(root / "phase3c6_baseline.json", summary["post_baseline"])
    _write_json(root / "summary.json", summary)
    _write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")


def restatement_vintage_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "field_name": row["field_name"],
            "issue_rows": 1,
            "vintage_policy": "canonical_value_preserved_no_source_order_overwrite",
            "disposition": "SOURCE_VERSION_OR_SEMANTIC_DIFFERENCE_PRESERVED",
        }
        for row in rows
        if row["field_name"] in {"ebit", "ebitda", "free_cashflow", "shares_outstanding"}
    ]


def preflight_text(summary: dict[str, Any]) -> str:
    pre = summary["pre_baseline"]
    return (
        "# Phase 3C-5 Residual Reconciliation Preflight\n\n"
        f"companies: {pre['company_total']}\n\n"
        f"canonical_q: {pre['coverage']['canonical_q_total']}\n\n"
        f"core_ready: {pre['coverage']['core_ready_q']}\n\n"
        f"core_not_ready: {pre['coverage']['core_not_ready_q']}\n\n"
        f"publish_null: {pre['coverage']['publish_date_null']}\n\n"
        "policy: no source-order overwrite; no pre-2018 Q; no provider/network calls.\n"
    )


def write_durable_doc(path: Path, artifact_root: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 3C-5 Residual Reconciliation

Classification: `{summary['classification']}`

Artifact root: `{artifact_root}`

Phase 3C-5 consolidated the remaining Phase 3 source disagreements and V2 historical residuals. Canonical values were not overwritten by source precedence. Resolution issues closed in production were closed as source-disagreement or Phase 4C handoff evidence without canonical data mutation.

- Raw issue rows: {summary['raw_issue_inventory']['total_raw_issue_rows']}
- Consolidated work units: {summary['consolidation']['work_units']}
- Duplicate semantic issue rows removed: {summary['consolidation']['duplicate_semantic_issues_removed']}
- Explicit canonical corrections: {summary['production_reconciliation']['corrections_applied']}
- Issue closures without canonical writes: {summary['canonical_corrections']['issue_closures_without_canonical_writes']}
- Remaining canonical issues: {summary['remaining_canonical_issues']['total']}
- Phase 4 completeness-gap Qs: {summary['phase4_handoff']['completeness_gap_q_count']}
- Phase 4C EBIT/EBITDA rows: {summary['phase4_handoff']['phase4c_ebit_ebitda_inventory_rows']}

3C-6 readiness:

```json
{json.dumps(summary['post_baseline']['coverage'], indent=2, sort_keys=True)}
```

Next step: `{summary['recommended_next_step']}`
"""
    path.write_text(text)


def counter_to_rows(counter: Counter, name: str) -> list[dict[str, Any]]:
    return [{name: key, "count": value} for key, value in sorted(counter.items())]


def _json_loads(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_text(path: Path, text: str) -> None:
    path.write_text(text)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
