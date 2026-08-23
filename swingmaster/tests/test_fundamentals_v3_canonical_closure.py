from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_closure import (
    canonical_identity_integrity,
    canonical_sequence_integrity,
    core_readiness_signatures,
    final_closure_gate,
    logical_fingerprint,
    no_unauthorized_overwrite_proof,
    phase4a_baseline,
    phase4b_missing_field_recovery_inventory,
    q4_policy_integrity,
    residual_1256_reclassification,
    source_contribution_summary,
)
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, V3FundamentalsRepository, V3MigrationAuditRepository, V3QuarterRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-23T00:00:00Z"


def test_final_company_universe_count(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM v3_company").fetchone()[0] == 1


def test_active_inactive_reconciliation(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    with sqlite3.connect(db) as conn:
        row = conn.execute("SELECT SUM(active=1), SUM(active=0) FROM v3_company").fetchone()
    assert row == (1, 0)


def test_no_pre_2018_q(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    checks = canonical_identity_integrity(db)
    assert _check(checks, "PRE_2018_Q") == 0


def test_unique_company_fyfq(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    assert _check(canonical_identity_integrity(db), "DUPLICATE_COMPANY_FY_FQ") == 0


def test_sequence_integrity(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    assert canonical_sequence_integrity(db) == []


def test_q4_policy_integrity(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    assert q4_policy_integrity(db) == []


def test_no_instant_field_subtraction(tmp_path: Path) -> None:
    db = _seed(tmp_path, fiscal_quarter="Q4", derivation_method="cash_subtraction")
    assert q4_policy_integrity(db)[0]["violation"] == "cash_subtraction"


def test_no_unauthorized_v2_overwrite(tmp_path: Path) -> None:
    assert no_unauthorized_overwrite_proof(_seed(tmp_path))["v2_automatic_overwrites"] == 0


def test_no_unauthorized_legacy_overwrite(tmp_path: Path) -> None:
    assert no_unauthorized_overwrite_proof(_seed(tmp_path))["legacy_automatic_overwrites"] == 0


def test_source_contribution_accounting(tmp_path: Path) -> None:
    summary, fields = source_contribution_summary(_seed(tmp_path))
    assert any(row["source_decision"] == "YAHOO:ACCEPTED" for row in summary)
    assert isinstance(fields, list)


def test_residual_semantic_classification(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    (root / "remaining_unresolved_canonical_issues.csv").write_text("ticker,fiscal_year,fiscal_quarter,period_end_date,reason\nAAA,2020,Q2,2020-06-30,HOLD_INSUFFICIENT_EVIDENCE\n")
    rows = residual_1256_reclassification(root)
    assert rows[0]["reclassified_as"] == "EXCLUDED_UNCONFIRMED_SOURCE_CANDIDATE"


def test_active_canonical_issue_vs_excluded_source_candidate_distinction(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    (root / "remaining_unresolved_canonical_issues.csv").write_text("ticker,fiscal_year,fiscal_quarter,period_end_date,reason\nAAA,2020,Q2,2020-06-30,HOLD_CROSS_SOURCE_IDENTITY_CONFLICT\n")
    rows = residual_1256_reclassification(root)
    assert rows[0]["active_v3_resolution_issue"] == 0
    assert rows[0]["canonical_defect"] == 0


def test_phase4_completeness_gap_classification(tmp_path: Path) -> None:
    rows = phase4a_baseline(_seed(tmp_path, revenue=None))
    assert rows[0]["historical_gap_context"] == "PHASE4_COMPLETENESS_GAP"


def test_phase4c_inventory_generation(tmp_path: Path) -> None:
    db = _seed(tmp_path, ebit=None, ebitda=None)
    rows = core_readiness_signatures(db)
    assert rows


def test_logical_fingerprint_determinism(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    source, _fields = source_contribution_summary(db)
    assert logical_fingerprint(db, source) == logical_fingerprint(db, source)


def test_closure_does_not_mutate_canonical_financial_values(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    before = _value(db)
    phase4b_missing_field_recovery_inventory(phase4a_baseline(db))
    assert _value(db) == before


def test_quick_check(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_fk_check(tmp_path: Path) -> None:
    db = _seed(tmp_path)
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_final_closure_gate_passes_for_clean_inputs() -> None:
    baseline = {"company_total": 2552, "active": 2484, "inactive": 68, "coverage": {"canonical_q_total": 72536}}
    universe = [
        {"status": "APPROVED_UNIVERSE_RECONCILES", "count": 2552},
        {"status": "ARBITRARY_YAHOO_V2_ONLY_ADMITTED", "count": 0},
    ]
    identity = [{"check": "PRE_2018_Q", "violations": 0}]
    issue_status = [{"status": "ACTIVE_BLOCKING", "count": 0}]
    integrity = {"quick_check": "ok", "foreign_key_check_rows": 0}
    assert final_closure_gate(baseline, universe, identity, [], [], [], issue_status, integrity)["passed"]


def _check(rows: list[dict], name: str) -> int:
    return int(next(row for row in rows if row["check"] == name)["violations"])


def _value(db: Path) -> float:
    with sqlite3.connect(db) as conn:
        return float(conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0])


def _seed(tmp_path: Path, **overrides) -> Path:
    db = tmp_path / "v3.db"
    fq = overrides.pop("fiscal_quarter", "Q2")
    derivation = overrides.pop("derivation_method", None)
    values = {
        "revenue": 100.0,
        "ebitda": 10.0,
        "free_cashflow": 5.0,
        "cash": 50.0,
        "total_debt": 20.0,
        "shares_outstanding": 1000.0,
        "ebit": 9.0,
    }
    values.update(overrides)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        apply_v3_schema(conn)
        company_id = V3CompanyRepository(conn).admit_company(market="usa", ticker="AAA", admission_source="LEGACY_AUTHORITY", now_utc=NOW)
        quarter_id = V3QuarterRepository(conn).upsert_quarter(company_id=company_id, fiscal_year=2020, fiscal_quarter=fq, period_end_date="2020-06-30", publish_date="2020-08-01", now_utc=NOW)
        V3FundamentalsRepository(conn).write_null_preserving_fields(quarter_id=quarter_id, values=values, accepted_source_provider="YAHOO", accepted_at_utc=NOW, update_run_id="SEED", derivation_method=derivation)
        V3MigrationAuditRepository(conn).record_audit(migration_run_id="SEED", source="YAHOO", source_key="AAA|2020|Q2", audit_type="CANONICAL_APPLY", decision="ACCEPTED", evidence={"field_outcomes": {"revenue": ["FIELD_INSERTED"]}}, company_id=company_id, quarter_id=quarter_id, now_utc=NOW)
        conn.commit()
    return db
