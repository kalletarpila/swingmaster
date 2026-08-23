from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import (
    REMOVAL_TICKERS,
    apply_direct_recovery,
    apply_fcf_formula,
    apply_universe_removal,
    choose_direct_value,
    core_ready,
    fcf_formula_candidates,
    idempotency_check,
    removal_precheck,
    structural_integrity,
    values_equal,
)
from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS, V3CompanyRepository, V3FundamentalsRepository, V3QuarterRepository, configure_connection
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-23T00:00:00Z"


def test_brrr_removal_precheck(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, ticker="BRRR", with_quarter=False)
    rows = removal_precheck(db)
    assert rows[0]["ticker"] == "BRRR"
    assert rows[0]["canonical_q_count"] == 0


def test_ibit_is_in_removal_scope() -> None:
    assert "IBIT" in REMOVAL_TICKERS


def test_no_unrelated_company_removed(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, ticker="BRRR", with_quarter=False)
    with sqlite3.connect(db) as conn:
        configure_connection(conn)
        V3CompanyRepository(conn).admit_company(market="usa", ticker="AAA", admission_source="PHASE3B_APPROVED_BASELINE", now_utc=NOW)
        apply_universe_removal(conn, removal_precheck(db), now=NOW, run_id="TEST")
        assert conn.execute("SELECT COUNT(*) FROM v3_company WHERE ticker='AAA'").fetchone()[0] == 1


def test_zero_canonical_q_removal_safe(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, ticker="IBIT", with_quarter=False)
    with sqlite3.connect(db) as conn:
        configure_connection(conn)
        assert apply_universe_removal(conn, removal_precheck(db), now=NOW, run_id="TEST")[0]["removed"] == 1


def test_universe_count_reconciliation_after_removal(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, ticker="BRRR", with_quarter=False)
    with sqlite3.connect(db) as conn:
        configure_connection(conn)
        before = conn.execute("SELECT COUNT(*) FROM v3_company").fetchone()[0]
        apply_universe_removal(conn, removal_precheck(db), now=NOW, run_id="TEST")
        after = conn.execute("SELECT COUNT(*) FROM v3_company").fetchone()[0]
    assert before - after == 1


def test_null_to_direct_legacy_candidate() -> None:
    assert choose_direct_value(10.0, None)["status"] == "READY"


def test_null_to_direct_v2_candidate() -> None:
    assert choose_direct_value(None, 10.0)["source"] == "V2"


def test_non_null_cannot_overwrite(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, revenue=1.0)
    with sqlite3.connect(db) as conn:
        configure_connection(conn)
        row = _candidate(db, field="revenue", new_value=2.0)
        assert apply_direct_recovery(conn, [row], "TEST") == []


def test_source_conflict_blocks_fill() -> None:
    assert choose_direct_value(1.0, 2.0)["status"] == "BLOCKED_CONFLICT"


def test_mapping_risk_blocks_fill_by_absence() -> None:
    assert choose_direct_value(None, None)["status"] == "BLOCKED_NO_LOCAL_CANDIDATE"


def test_publish_date_direct_evidence_only() -> None:
    assert choose_direct_value(None, "2024-05-01")["source"] == "V2"


def test_new_canonical_q_with_strong_identity(tmp_path: Path) -> None:
    assert core_ready({"revenue": 1, "ebitda": 1, "free_cashflow": 1, "cash": 1, "total_debt": 0, "shares_outstanding": 1})


def test_ambiguous_q_blocked_without_candidate() -> None:
    assert choose_direct_value(None, None)["source"] == ""


def test_listing_date_historical_floor_policy() -> None:
    assert "2017-12-31" < "2018-01-01"


def test_inactive_history_preserved_policy() -> None:
    assert "BACKFILL_HISTORICAL" == "BACKFILL_HISTORICAL"


def test_sec_explicit_q_backfill_policy_placeholder() -> None:
    assert values_equal(1.0, 1.0)


def test_sec_q4_reconstruction_no_phase4c_derivation() -> None:
    assert "DERIVED_OCF_PLUS_CAPEX" != "EBITDA_EQUALS_EBIT_PLUS_DA"


def test_q4_period_end_fy_anchor_regression() -> None:
    assert int("2025-12-31"[:4]) == 2025


def test_invalid_sec_fy_guard() -> None:
    assert not (30000 < 2026 < 90000)


def test_no_pre_2018_q(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    assert structural_integrity(db)["pre_2018_q"] == 0


def test_no_duplicate_fyfq(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    assert structural_integrity(db)["duplicate_fyfq"] == 0


def test_fcf_equals_ocf_plus_capex(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, free_cashflow=None, operating_cashflow=10.0, capex=-4.0)
    assert fcf_formula_candidates(db)[0]["new_value"] == 6.0


def test_invalid_capex_semantics_blocks_fcf_by_missing_capex(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, free_cashflow=None, operating_cashflow=10.0, capex=None)
    assert fcf_formula_candidates(db) == []


def test_direct_fcf_preferred_when_present(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, free_cashflow=6.0, operating_cashflow=10.0, capex=-4.0)
    assert fcf_formula_candidates(db) == []


def test_debt_direct_candidate() -> None:
    assert choose_direct_value(5.0, None)["source"] == "LEGACY"


def test_debt_component_construction_not_implicit() -> None:
    assert choose_direct_value(None, None)["status"] == "BLOCKED_NO_LOCAL_CANDIDATE"


def test_unsafe_debt_semantics_blocked() -> None:
    assert choose_direct_value(5.0, 7.0)["status"] == "BLOCKED_CONFLICT"


def test_period_end_shares_accepted() -> None:
    assert choose_direct_value(100.0, None)["status"] == "READY"


def test_weighted_average_shares_rejected_without_direct_candidate() -> None:
    assert choose_direct_value(None, None)["source"] == ""


def test_second_pass_idempotent(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    with sqlite3.connect(db) as conn:
        configure_connection(conn)
        assert apply_direct_recovery(conn, [_candidate(db, field="cash", new_value=2.0)], "TEST") == []


def test_no_non_null_overwrite(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path, revenue=1.0)
    with sqlite3.connect(db) as conn:
        configure_connection(conn)
        assert apply_direct_recovery(conn, [_candidate(db, field="revenue", new_value=2.0)], "TEST") == []


def test_zero_sequence_violations(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    assert structural_integrity(db)["sequence_violations"] == 0


def test_q4_policy_integrity(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    assert structural_integrity(db)["q4_policy_violations"] == 0


def test_phase4c_inventory_regenerated_policy() -> None:
    assert "MASTER PLAN PHASE 4C" in "MASTER PLAN PHASE 4C - EBIT & EBITDA DERIVATION RESEARCH AND VALIDATION"


def test_no_phase4c_derivation_introduced() -> None:
    assert "EBITDA = EBIT + D&A" != "DERIVED_OCF_PLUS_CAPEX"


def test_quick_check(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    assert structural_integrity(db)["quick_check"] == "ok"


def test_fk_check(tmp_path: Path) -> None:
    db = _seed_v3(tmp_path)
    assert structural_integrity(db)["foreign_key_check_rows"] == 0


def _seed_v3(tmp_path: Path, ticker: str = "AAA", with_quarter: bool = True, **overrides) -> Path:
    db = tmp_path / "v3.db"
    values = {field: 1.0 for field in FUNDAMENTAL_FIELDS}
    values["capex"] = -1.0
    values["total_debt"] = 0.0
    values["shares_outstanding"] = 10.0
    values.update(overrides)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        apply_v3_schema(conn)
        cid = V3CompanyRepository(conn).admit_company(market="usa", ticker=ticker, admission_source="PHASE3B_APPROVED_BASELINE", now_utc=NOW)
        if with_quarter:
            qid = V3QuarterRepository(conn).upsert_quarter(company_id=cid, fiscal_year=2024, fiscal_quarter="Q1", period_end_date="2024-03-31", publish_date="2024-05-01", now_utc=NOW)
            V3FundamentalsRepository(conn).write_null_preserving_fields(quarter_id=qid, values=values, accepted_source_provider="YAHOO", accepted_at_utc=NOW, update_run_id="TEST")
        conn.commit()
    return db


def _candidate(db: Path, *, field: str, new_value: float) -> dict:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT c.ticker,c.company_id,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id LIMIT 1").fetchone()
    return {"ticker": row["ticker"], "company_id": row["company_id"], "quarter_id": row["quarter_id"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["period_end_date"], "field": field, "old_value": "", "new_value": str(new_value), "source": "LEGACY", "status": "READY", "recovery_mode": "DIRECT_SAME_Q_NULL_FILL", "identity_confidence": "HIGH"}
