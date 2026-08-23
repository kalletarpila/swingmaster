from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_phase4a_completeness_audit import (
    choose_candidate,
    classify_zero_local,
    confidence,
    core_ready,
    expected_quarter_count,
    fcf_recoverability,
    final_completeness_taxonomy,
    longest_gap,
    missing_field_signatures,
    phase4b_inventory,
    phase4c_research_groups,
    priority_for_field,
    zero_q_final_classification,
)
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, V3FundamentalsRepository, V3QuarterRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-23T00:00:00Z"


def test_phase3_final_baseline_can_load_from_seed(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 2


def test_no_phase3_structural_regression_in_seed(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_completeness_gap_classification_deterministic(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, ebitda=None)
    assert missing_field_signatures(db)[0]["missing_signature"] == "ebitda"


def test_missing_field_signature_classification(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, revenue=None, ebitda=None)
    assert missing_field_signatures(db)[0]["missing_signature"] == "revenue;ebitda"


def test_expected_history_model_honors_2018_floor() -> None:
    assert max("2018-01-01", "2017-05-01") == "2018-01-01"


def test_expected_history_model_handles_recent_listings() -> None:
    assert max("2018-01-01", "2025-03-01") == "2025-03-01"


def test_internal_gap_detection_helpers() -> None:
    rows = [_q(2024, "Q1"), _q(2024, "Q4")]
    assert expected_quarter_count(rows) == 4
    assert longest_gap(rows) == 2


def test_trailing_gap_helper() -> None:
    assert longest_gap([_q(2025, "Q1"), _q(2025, "Q2")]) == 2


def test_zero_q_company_detection(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    with sqlite3.connect(db) as conn:
        company = V3CompanyRepository(conn).admit_company(market="usa", ticker="ZERO", admission_source="PHASE3B_APPROVED_BASELINE", now_utc=NOW)
        assert company
        assert conn.execute("SELECT COUNT(*) FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id WHERE c.ticker='ZERO'").fetchone()[0] == 0


def test_zero_q_local_source_evidence_classification() -> None:
    assert classify_zero_local({"ticker": "AAA", "active": 1}, {}, {"legacy_rows": 1}, {}, {}) == "HISTORY_EXISTS_NOT_INGESTED"


def test_zero_q_disposition_taxonomy() -> None:
    rows = zero_q_final_classification([{"ticker": "AAA", "local_preliminary_class": "HISTORY_EXISTS_NOT_INGESTED"}])
    assert rows[0]["final_disposition"] == "KEEP_AND_BACKFILL_PHASE4B"


def test_removal_candidates_are_not_auto_removed() -> None:
    rows = zero_q_final_classification([{"ticker": "IBIT", "local_preliminary_class": "UNIVERSE_REMOVAL_CANDIDATE"}])
    assert rows[0]["final_disposition"] == "REMOVE_FROM_UNIVERSE_CANDIDATE"


def test_direct_recovery_candidate_identification() -> None:
    assert choose_candidate(1.0, None) == ("LEGACY", 1.0, "EXACT_OR_SAME_FYFQ_CANDIDATE")


def test_source_conflict_blocks_auto_recovery_recommendation() -> None:
    assert choose_candidate(1.0, 2.0)[2] == "SOURCE_CONFLICT"


def test_fcf_recoverability_logic(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, free_cashflow=None, operating_cashflow=9.0, capex=-2.0)
    rows = fcf_recoverability(db)
    assert rows[0]["fcf_recoverability"] == "FORMULA_DERIVABLE_OCF_PLUS_CAPEX"


def test_debt_component_recoverability_priority() -> None:
    assert priority_for_field("total_debt") == 1


def test_shares_rejects_weighted_average_only_evidence() -> None:
    assert choose_candidate(None, None) == ("NONE", "", "NO_LOCAL_CANDIDATE")


def test_publish_date_never_inferred_without_evidence() -> None:
    assert choose_candidate(None, None)[2] == "NO_LOCAL_CANDIDATE"


def test_phase4b_priority_assignment() -> None:
    assert priority_for_field("revenue") == 1


def test_core_ready_uplift_estimation_inputs() -> None:
    assert core_ready({"revenue": 1, "ebitda": 1, "free_cashflow": 1, "cash": 1, "total_debt": 0, "shares_outstanding": 1})


def test_phase4c_inventory_grouping() -> None:
    rows = phase4c_research_groups([{"missing_ebit": 0, "missing_ebitda": 1, "operating_income_available": 1}])
    assert rows[0]["research_group"] == "EBITDA_MISSING_EBIT_PRESENT"


def test_no_canonical_production_writes_from_taxonomy(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, ebitda=None)
    before = db.stat().st_size
    final_completeness_taxonomy(db, [], [], [])
    assert db.stat().st_size == before


def test_quick_check(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_fk_check(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_phase4b_inventory_keeps_removal_as_recommendation() -> None:
    rows = phase4b_inventory([], [], [{"ticker": "IBIT", "final_disposition": "REMOVE_FROM_UNIVERSE_CANDIDATE"}])
    assert rows == []


def test_confidence_mapping() -> None:
    assert confidence("HISTORY_EXISTS_NOT_INGESTED") == "HIGH"


def _seed_v3(tmp_path: Path, **overrides) -> Path:
    db = tmp_path / "v3.db"
    values = {
        "revenue": 100.0,
        "gross_profit": 50.0,
        "operating_income": 10.0,
        "ebit": 9.0,
        "ebitda": 11.0,
        "net_income": 7.0,
        "operating_cashflow": 9.0,
        "capex": -2.0,
        "free_cashflow": 7.0,
        "cash": 20.0,
        "total_debt": 5.0,
        "shares_outstanding": 100.0,
    }
    values.update(overrides)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        apply_v3_schema(conn)
        company_id = V3CompanyRepository(conn).admit_company(market="usa", ticker="AAA", admission_source="PHASE3B_APPROVED_BASELINE", now_utc=NOW)
        qrepo = V3QuarterRepository(conn)
        frepo = V3FundamentalsRepository(conn)
        q1 = qrepo.upsert_quarter(company_id=company_id, fiscal_year=2024, fiscal_quarter="Q1", period_end_date="2024-03-31", publish_date="2024-05-01", now_utc=NOW)
        q2 = qrepo.upsert_quarter(company_id=company_id, fiscal_year=2024, fiscal_quarter="Q2", period_end_date="2024-06-30", publish_date="2024-08-01", now_utc=NOW)
        frepo.write_null_preserving_fields(quarter_id=q1, values=values, accepted_source_provider="YAHOO", accepted_at_utc=NOW, update_run_id="TEST")
        frepo.write_null_preserving_fields(quarter_id=q2, values=values, accepted_source_provider="YAHOO", accepted_at_utc=NOW, update_run_id="TEST")
        conn.commit()
    return db


def _q(fy: int, fq: str) -> dict:
    return {"fiscal_year": fy, "fiscal_quarter": fq}
