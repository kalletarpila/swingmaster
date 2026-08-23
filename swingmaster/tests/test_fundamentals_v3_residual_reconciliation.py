from __future__ import annotations

import sqlite3
import json
from pathlib import Path

from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, V3FundamentalsRepository, V3MigrationAuditRepository, V3QuarterRepository
from swingmaster.fundamentals.v3_residual_reconciliation import (
    CanonicalCorrection,
    apply_audited_canonical_correction,
    build_phase4_handoff,
    build_remaining_unresolved_canonical_issues,
    choose_work_unit_disposition,
    classify_source_disagreement,
    classify_v2_historical_terminal_disposition,
    close_redundant_migration_issue,
    consolidate_issue_work_units,
    dry_reconciliation_summary,
    scope_for_issue,
    scope_for_v2_disposition,
    semantic_for_issue,
)
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-23T00:00:00Z"


def test_duplicate_semantic_issue_consolidation() -> None:
    rows = [_issue("1"), _issue("2")]

    units = consolidate_issue_work_units(rows)

    assert len(units) == 1
    assert units[0]["raw_issue_count"] == 2


def test_wrong_v2_mapping_closes_without_canonical_change() -> None:
    assert classify_v2_historical_terminal_disposition("V2_FYFQ_LABEL_ERROR") == "EXCLUDE_SOURCE_ROW_WRONG_MAPPING"


def test_redundant_period_variant_closes() -> None:
    assert classify_v2_historical_terminal_disposition("V2_PERIOD_VARIANT") == "EXCLUDE_SOURCE_ROW_DUPLICATE_VARIANT"


def test_source_semantic_difference_is_not_canonical_defect() -> None:
    assert classify_source_disagreement("ebitda") == "SOURCE_SEMANTIC_DIFFERENCE"


def test_restatement_policy_classification() -> None:
    assert choose_work_unit_disposition([_issue("1", terminal_disposition="CLOSED_SOURCE_DISAGREEMENT_CANONICAL_UNCHANGED")]) == "CLOSED_SOURCE_DISAGREEMENT_CANONICAL_UNCHANGED"


def test_revenue_conflict_canonical_supported_case() -> None:
    assert classify_source_disagreement("revenue", relative_difference=0.03) == "CANONICAL_VALUE_SUPPORTED"


def test_canonical_value_suspect_case() -> None:
    assert classify_source_disagreement("revenue", relative_difference=0.75) == "INSUFFICIENT_TO_CHOOSE"


def test_ebit_ebitda_derivation_gap_handed_to_phase4c() -> None:
    assert scope_for_issue("NON_NULL_FIELD_CONFLICT", "ebit") == "FIELD_VALUE"
    assert semantic_for_issue("NON_NULL_FIELD_CONFLICT", "ebitda", "YAHOO") == "SOURCE_SEMANTIC_DIFFERENCE"


def test_fcf_semantic_conflict() -> None:
    assert classify_source_disagreement("free_cashflow") == "SOURCE_SEMANTIC_DIFFERENCE"


def test_debt_semantic_conflict() -> None:
    assert classify_source_disagreement("total_debt") == "SOURCE_SEMANTIC_DIFFERENCE"


def test_shares_weighted_average_conflict() -> None:
    assert classify_source_disagreement("shares_outstanding") == "SOURCE_SEMANTIC_DIFFERENCE"


def test_publication_conflict_scope() -> None:
    assert scope_for_issue("PUBLICATION_DATE_CONFLICT", "publish_date") == "PUBLICATION_METADATA"


def test_explicit_audited_field_correction(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    correction = CanonicalCorrection("FIELD_VALUE_CORRECTION", "AAA", 2020, "Q2", "revenue", 100.0, 110.0, "official correction", "fixture", "LEGACY")

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        result = apply_audited_canonical_correction(conn, correction, migration_run_id="RUN", now_utc=NOW)
        value = conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0]
        audit = conn.execute("SELECT decision FROM v3_migration_audit WHERE audit_type='CANONICAL_CORRECTION'").fetchone()[0]

    assert result["applied"] == 1
    assert value == 110.0
    assert audit == "FIELD_VALUE_CORRECTION"


def test_explicit_audited_publish_correction(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    correction = CanonicalCorrection("PUBLISH_DATE_CORRECTION", "AAA", 2020, "Q2", "publish_date", "2020-08-01", "2020-08-02", "official release", "fixture", "LEGACY")

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        result = apply_audited_canonical_correction(conn, correction, migration_run_id="RUN", now_utc=NOW)
        value = conn.execute("SELECT publish_date FROM v3_quarter").fetchone()[0]

    assert result["applied"] == 1
    assert value == "2020-08-02"


def test_normal_enrichment_cannot_overwrite(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    correction = CanonicalCorrection("FIELD_VALUE_CORRECTION", "AAA", 2020, "Q2", "revenue", 999.0, 110.0, "bad old", "fixture", "LEGACY")

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        result = apply_audited_canonical_correction(conn, correction, migration_run_id="RUN", now_utc=NOW)
        value = conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0]

    assert result["applied"] == 0
    assert value == 100.0


def test_correction_primitive_can_change_only_approved_field(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    correction = CanonicalCorrection("FIELD_VALUE_CORRECTION", "AAA", 2020, "Q2", "company_id", 1, 2, "bad", "fixture", "LEGACY")

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        try:
            apply_audited_canonical_correction(conn, correction, migration_run_id="RUN", now_utc=NOW)
        except ValueError as exc:
            assert str(exc) == "UNAPPROVED_CORRECTION_FIELD:company_id"
        else:
            raise AssertionError("unapproved field was corrected")


def test_old_new_value_audited(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    correction = CanonicalCorrection("FIELD_VALUE_CORRECTION", "AAA", 2020, "Q2", "cash", 50.0, 55.0, "cash evidence", "fixture", "LEGACY")

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        apply_audited_canonical_correction(conn, correction, migration_run_id="RUN", now_utc=NOW)
        evidence = conn.execute("SELECT evidence_json FROM v3_migration_audit WHERE audit_type='CANONICAL_CORRECTION'").fetchone()[0]

    payload = json.loads(evidence)
    assert payload["old_value"] == 50.0
    assert payload["new_value"] == 55.0


def test_no_duplicate_fyfq_scope() -> None:
    assert scope_for_v2_disposition("HOLD_CROSS_SOURCE_IDENTITY_CONFLICT") == "CANONICAL_IDENTITY"


def test_no_pre_2018_q_scope() -> None:
    assert scope_for_v2_disposition("PHASE4_COMPLETENESS_CANDIDATE") == "HISTORICAL_COMPLETENESS_GAP"


def test_completeness_gap_separated_from_reconciliation() -> None:
    assert scope_for_v2_disposition("PHASE4_COMPLETENESS_CANDIDATE") != "FIELD_VALUE"


def test_unresolved_canonical_issue_inventory() -> None:
    rows = build_remaining_unresolved_canonical_issues([{"ticker": "AAA", "fiscal_year": 2020, "fiscal_quarter": "Q2", "period_end_date": "2020-06-30", "final_disposition": "HOLD_CROSS_SOURCE_IDENTITY_CONFLICT"}], [])

    assert rows[0]["unresolved_class"] == "UNRESOLVED_IDENTITY"


def test_issue_closure_idempotency(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        issue_id = conn.execute("SELECT issue_id FROM v3_resolution_issue").fetchone()[0]
        assert close_redundant_migration_issue(conn, issue_id, resolution="CLOSED", now_utc=NOW) == 1
        assert close_redundant_migration_issue(conn, issue_id, resolution="CLOSED", now_utc=NOW) == 0


def test_correction_idempotency(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    correction = CanonicalCorrection("FIELD_VALUE_CORRECTION", "AAA", 2020, "Q2", "revenue", 100.0, 110.0, "official correction", "fixture", "LEGACY")

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        first = apply_audited_canonical_correction(conn, correction, migration_run_id="RUN", now_utc=NOW)
        second = apply_audited_canonical_correction(conn, correction, migration_run_id="RUN", now_utc=NOW)

    assert first["applied"] == 1
    assert second["applied"] == 0


def test_phase4a_handoff(tmp_path: Path) -> None:
    db = _seed(tmp_path, ebitda=None)

    rows = build_phase4_handoff(db)

    assert rows
    assert rows[0]["phase4_disposition"] == "PHASE4_COMPLETENESS_GAP"


def test_phase4c_handoff() -> None:
    assert semantic_for_issue("NON_NULL_FIELD_CONFLICT", "ebit", "LEGACY") == "SOURCE_SEMANTIC_DIFFERENCE"


def test_integrity_dry_gate() -> None:
    plan = type("Plan", (), {"correction_plan": [], "issue_closure_plan": []})()

    assert dry_reconciliation_summary(plan)["gate"]["passed"]


def _issue(raw_id: str, **overrides):
    row = {
        "raw_issue_id": raw_id,
        "origin": "YAHOO",
        "market": "usa",
        "ticker": "AAA",
        "fiscal_year": 2020,
        "fiscal_quarter": "Q2",
        "scope_type": "FIELD_VALUE",
        "field_name": "revenue",
        "semantic": "CANONICAL_VALUE_SUPPORTED",
        "terminal_disposition": "CLOSED_SOURCE_DISAGREEMENT_CANONICAL_UNCHANGED",
        "canonical_action": "NO_CANONICAL_WRITE",
    }
    row.update(overrides)
    return row


def _seed(tmp_path: Path, **fundamental_overrides) -> Path:
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        apply_v3_schema(conn)
        company_id = V3CompanyRepository(conn).admit_company(market="usa", ticker="AAA", now_utc=NOW)
        quarter_id = V3QuarterRepository(conn).upsert_quarter(company_id=company_id, fiscal_year=2020, fiscal_quarter="Q2", period_end_date="2020-06-30", publish_date="2020-08-01", now_utc=NOW)
        values = {"revenue": 100.0, "cash": 50.0, "ebitda": 10.0}
        values.update(fundamental_overrides)
        V3FundamentalsRepository(conn).write_null_preserving_fields(quarter_id=quarter_id, values=values, accepted_source_provider="YAHOO", accepted_at_utc=NOW, update_run_id="SEED")
        issue_id = conn.execute(
            """
            INSERT INTO v3_resolution_issue (quarter_id, issue_type, field_name, status, source_details_json, created_at_utc, updated_at_utc)
            VALUES (?, 'NON_NULL_FIELD_CONFLICT', 'revenue', 'ACTIVE', '{}', ?, ?)
            """,
            (quarter_id, NOW, NOW),
        ).lastrowid
        V3MigrationAuditRepository(conn).record_audit(migration_run_id="SEED", source="YAHOO", source_key=f"issue:{issue_id}", audit_type="SEED", decision="ACCEPTED", now_utc=NOW)
        conn.commit()
    return db
