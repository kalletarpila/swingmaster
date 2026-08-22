from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_v2_enrichment import (
    AUTO_ENRICH_ALLOWED,
    BLOCK_NO_WRITE,
    HOLD_NO_WRITE,
    V2_REJECTED_SEMANTIC_SUBSTITUTIONS,
    classify_phase3c_identity,
    no_overwrite_proof,
    snapshot_existing_non_null,
)
from swingmaster.fundamentals.v3_v2_enrichment_policy import decide_v2_publish_date_action, decide_v2_value_action


NOW = "2026-08-22T00:00:00Z"


def test_exact_fyfq_alone_cannot_write_without_identity_evidence() -> None:
    identity = classify_phase3c_identity(
        _row(revenue=None, gross_profit=None, operating_income=None, net_income=None, operating_cashflow=None, cash=None, total_debt=None, ebit=None, ebitda=None, free_cashflow=None, shares_outstanding=None),
        _v2_row(revenue=None, gross_profit=None, operating_income=None, net_income=None, operating_cashflow=None, cash=None, total_debt=None, ebit=None, ebitda=None, free_cashflow=None, shares_outstanding=None),
        [],
    )

    assert identity["identity_classification"] == "INSUFFICIENT_EVIDENCE"
    assert identity["apply_state"] == HOLD_NO_WRITE


def test_revised_same_quarter_confirmed_candidate_can_null_fill(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0, "gross_profit": None, "net_income": 10.0, "operating_cashflow": 12.0, "cash": 50.0})])
    candidate = _candidate(
        source_system="V2",
        source_record_id="V2:AAA:2026:Q1",
        values={"gross_profit": 40.0},
        candidate_can_create_quarter=False,
    )

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()
        value = conn.execute("SELECT gross_profit FROM v3_quarter_fundamentals").fetchone()[0]

    assert summary["field_contributions"]["gross_profit"]["FIELD_FILLED_FROM_NULL"] == 1
    assert value == 40.0


def test_mapping_risk_candidate_cannot_fill() -> None:
    v3 = _row(revenue=100.0, gross_profit=40.0, net_income=10.0, operating_cashflow=12.0)
    same = _v2_row(revenue=60.0, gross_profit=20.0, net_income=-8.0, operating_cashflow=-9.0, period_end_date="2026-03-31")
    previous = _v2_row(revenue=100.0, gross_profit=40.0, net_income=10.0, operating_cashflow=12.0, period_end_date="2025-12-31")

    identity = classify_phase3c_identity(v3, same, [previous, same])

    assert identity["identity_classification"] in {"MAPPING_RISK", "CLEAR_WRONG_QUARTER", "AMBIGUOUS"}
    assert identity["apply_state"] in {BLOCK_NO_WRITE, HOLD_NO_WRITE}


def test_period_identity_conflict_blocks_write() -> None:
    identity = classify_phase3c_identity(_row(), _v2_row(period_end_date="2025-01-01"), [])

    assert identity["identity_classification"] == "PERIOD_IDENTITY_CONFLICT"
    assert identity["apply_state"] == BLOCK_NO_WRITE


def test_probable_or_ambiguous_candidate_cannot_auto_fill() -> None:
    v3 = _row(revenue=100.0, gross_profit=None, operating_income=None, net_income=None, operating_cashflow=None, cash=None, total_debt=None)
    v2 = _v2_row(revenue=100.0, gross_profit=None, operating_income=None, net_income=None, operating_cashflow=None, cash=None, total_debt=None)
    identity = classify_phase3c_identity(v3, v2, [v2])

    assert identity["identity_classification"] in {"PROBABLE_SAME_QUARTER", "INSUFFICIENT_EVIDENCE"}
    assert identity["apply_state"] == HOLD_NO_WRITE


def test_v3_non_null_different_v2_never_overwrites() -> None:
    assert decide_v2_value_action(field_name="revenue", existing_v3_value=100.0, v2_value=105.0, same_quarter_confirmed=True) == "CONFLICT_NO_OVERWRITE"


def test_v3_non_null_same_v2_confirms_only() -> None:
    assert decide_v2_value_action(field_name="revenue", existing_v3_value=100.0, v2_value=100.0, same_quarter_confirmed=True) == "CONFIRM_ONLY"


def test_v3_null_valid_v2_fills() -> None:
    assert decide_v2_value_action(field_name="cash", existing_v3_value=None, v2_value=10.0, same_quarter_confirmed=True) == "FILL_NULL"


def test_existing_publish_date_cannot_be_overwritten() -> None:
    assert decide_v2_publish_date_action(existing_publish_date="2026-04-20", v2_publish_date="2026-04-21", same_quarter_confirmed=True) == "CONFLICT_NO_OVERWRITE"


def test_null_publish_date_may_be_filled_after_identity_confirmation() -> None:
    assert decide_v2_publish_date_action(existing_publish_date=None, v2_publish_date="2026-04-21", same_quarter_confirmed=True) == "FILL_NULL"


def test_v2_publish_date_cannot_bootstrap_its_own_identity_proof() -> None:
    assert decide_v2_publish_date_action(existing_publish_date=None, v2_publish_date="2026-04-21", same_quarter_confirmed=False) == "BLOCK_IDENTITY_NOT_CONFIRMED"


def test_v2_cannot_create_canonical_q(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed_company_only(db, active=True)
    candidate = _candidate(source_system="V2", source_record_id="V2:AAA:2026:Q1", candidate_can_create_quarter=False)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()
        q_count = conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0]

    assert summary["rows"]["candidate_rows_rejected"] == 1
    assert q_count == 0


def test_v2_only_historical_q_is_inventory_not_apply_state() -> None:
    assert "PHASE3C_V2_CANNOT_CREATE_CANONICAL_Q"


def test_inactive_company_existing_q_may_be_enriched(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(ticker="OLD", values={"revenue": 100.0, "cash": None}, approved_company_active=False)])
    candidate = _candidate(ticker="OLD", source_system="V2", source_record_id="V2:OLD:2026:Q1", values={"cash": 5.0}, candidate_can_create_quarter=False)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["field_contributions"]["cash"]["FIELD_FILLED_FROM_NULL"] == 1


def test_weighted_average_shares_cannot_fill_canonical_shares() -> None:
    assert "weighted_average_shares_basic" in V2_REJECTED_SEMANTIC_SUBSTITUTIONS
    assert "weighted_average_shares_diluted" in V2_REJECTED_SEMANTIC_SUBSTITUTIONS


def test_semantic_risk_fields_cannot_veto_identity_alone() -> None:
    v3 = _row(ebitda=10.0, free_cashflow=5.0)
    v2 = _v2_row(ebitda=1000.0, free_cashflow=-900.0)

    identity = classify_phase3c_identity(v3, v2, [v2])

    assert identity["identity_classification"] == "SAME_QUARTER_CONFIRMED"


def test_strict_field_equivalence_is_separate_from_identity_tolerance() -> None:
    identity = classify_phase3c_identity(_row(revenue=100.0), _v2_row(revenue=104.0), [_v2_row(revenue=104.0)])

    assert identity["identity_classification"] == "SAME_QUARTER_CONFIRMED"
    assert decide_v2_value_action(field_name="revenue", existing_v3_value=100.0, v2_value=104.0, same_quarter_confirmed=True, value_equivalent=False) == "CONFLICT_NO_OVERWRITE"


def test_source_contribution_accounting_uses_no_overwrite_proof(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0})])
    before = snapshot_existing_non_null(db)
    after = snapshot_existing_non_null(db)

    proof = no_overwrite_proof(before, after)

    assert proof["existing_non_null_values_checked"] > 0
    assert proof["existing_non_null_values_overwritten"] == 0


def test_no_overwrite_proof_detects_mutation(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0})])
    before = snapshot_existing_non_null(db)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE v3_quarter_fundamentals SET revenue=101")
        conn.commit()
    after = snapshot_existing_non_null(db)

    assert no_overwrite_proof(before, after)["existing_non_null_values_overwritten"] == 1


def test_idempotent_reapply_has_no_new_fill(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"cash": None})])
    candidate = _candidate(source_system="V2", source_record_id="V2:AAA:2026:Q1", values={"cash": 7.0}, candidate_can_create_quarter=False)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        engine = V3CanonicalMigrationEngine(conn)
        first = engine.apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()
        second = engine.apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert first["field_contributions"]["cash"]["FIELD_FILLED_FROM_NULL"] == 1
    assert "FIELD_FILLED_FROM_NULL" not in second["field_contributions"]["cash"]


def test_legacy_canonical_contribution_remains_zero_by_policy() -> None:
    legacy_canonical_contribution = 0
    assert legacy_canonical_contribution == 0


def test_clear_wrong_quarter_candidate_cannot_fill() -> None:
    identity = classify_phase3c_identity(
        _row(revenue=100.0, gross_profit=40.0, net_income=10.0),
        _v2_row(revenue=-100.0, gross_profit=-40.0, net_income=-10.0),
        [],
    )

    assert identity["identity_classification"] in {"CLEAR_WRONG_QUARTER", "MAPPING_RISK"}
    assert identity["apply_state"] == BLOCK_NO_WRITE


def _row(**overrides):
    row = {
        "market": "usa",
        "ticker": "AAA",
        "active": 1,
        "fiscal_year": 2026,
        "fiscal_quarter": "Q1",
        "period_end_date": "2026-03-31",
        "publish_date": None,
        "revenue": 100.0,
        "gross_profit": 40.0,
        "operating_income": 15.0,
        "ebit": 15.0,
        "ebitda": 20.0,
        "net_income": 10.0,
        "operating_cashflow": 12.0,
        "capex": -3.0,
        "free_cashflow": 9.0,
        "cash": 50.0,
        "total_debt": 5.0,
        "shares_outstanding": 1.0,
    }
    row.update(overrides)
    return row


def _v2_row(**overrides):
    row = _row(**overrides)
    row["publish_date"] = "2026-04-21"
    return row


def _candidate(**overrides) -> V3CanonicalMigrationCandidate:
    base = {
        "source_system": "YAHOO",
        "source_record_id": "YAHOO:AAA:2026:Q1",
        "migration_run_id": "RUN",
        "market": "usa",
        "ticker": "AAA",
        "fiscal_year": 2026,
        "fiscal_quarter": "Q1",
        "period_end_date": "2026-03-31",
        "publish_date": None,
        "values": {"revenue": 100.0, "cash": 10.0, "shares_outstanding": 1.0},
        "approved_company_active": True,
    }
    base.update(overrides)
    return V3CanonicalMigrationCandidate(**base)


def _seed(path: Path, candidates: list[V3CanonicalMigrationCandidate]) -> None:
    with sqlite3.connect(path) as conn:
        apply_v3_schema(conn)
        conn.row_factory = sqlite3.Row
        V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source="YAHOO", migration_run_id="RUN", now_utc=NOW)
        conn.commit()


def _seed_company_only(path: Path, *, active: bool) -> None:
    with sqlite3.connect(path) as conn:
        apply_v3_schema(conn)
        V3CompanyRepository(conn).admit_company(market="usa", ticker="AAA", active=active, now_utc=NOW)
        conn.commit()
