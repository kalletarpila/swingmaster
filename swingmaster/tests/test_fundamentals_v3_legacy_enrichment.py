from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine
from swingmaster.fundamentals.v3_legacy_enrichment import (
    AUTO_ENRICH_ALLOWED,
    BLOCK_NO_WRITE,
    HOLD_NO_WRITE,
    LEGACY_CAN_CREATE_NEW_CANONICAL_Q,
    LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE,
    classify_legacy_identity,
    counterfactual_summary,
    no_overwrite_proof,
    snapshot_existing_non_null,
    v2_counterfactual,
)
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_v2_enrichment_policy import V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE


NOW = "2026-08-22T00:00:00Z"


def test_legacy_exact_period_candidate_alone_does_not_prove_identity_if_contradictory() -> None:
    identity = classify_legacy_identity(_row(revenue=100.0, gross_profit=40.0), _legacy(revenue=-100.0, gross_profit=-40.0))

    assert identity["apply_state"] == BLOCK_NO_WRITE


def test_legacy_confirmed_same_quarter_can_fill_null(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0, "gross_profit": None, "net_income": 10.0, "operating_cashflow": 12.0, "cash": 50.0})])
    candidate = _candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:2026-03-31", values={"gross_profit": 40.0}, candidate_can_create_quarter=False)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()
        value = conn.execute("SELECT gross_profit FROM v3_quarter_fundamentals").fetchone()[0]

    assert summary["field_contributions"]["gross_profit"]["FIELD_FILLED_FROM_NULL"] == 1
    assert value == 40.0


def test_legacy_cannot_overwrite_non_null_yahoo_or_v2_values(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0, "cash": None})])
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        engine = V3CanonicalMigrationEngine(conn)
        engine.apply_source_batch([_candidate(source_system="V2", source_record_id="V2:AAA:2026:Q1", values={"cash": 5.0}, candidate_can_create_quarter=False)], source="V2", migration_run_id="RUN", now_utc=NOW)
        engine.apply_source_batch([_candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:2026-03-31", values={"revenue": 101.0, "cash": 6.0}, candidate_can_create_quarter=False)], source="LEGACY", migration_run_id="RUN", now_utc=NOW)
        row = conn.execute("SELECT revenue, cash FROM v3_quarter_fundamentals").fetchone()

    assert row["revenue"] == 100.0
    assert row["cash"] == 5.0


def test_legacy_same_value_confirms_only_and_different_value_conflicts_only(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0, "cash": 5.0})])

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
            [_candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:2026-03-31", values={"revenue": 100.0, "cash": 7.0}, candidate_can_create_quarter=False)],
            source="LEGACY",
            migration_run_id="RUN",
            now_utc=NOW,
        ).to_dict()

    assert summary["field_contributions"]["revenue"]["FIELD_CONFIRMED_SAME"] == 1
    assert summary["field_contributions"]["cash"]["FIELD_CONFLICT"] == 1


def test_legacy_cannot_create_new_q(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed_company_only(db)
    candidate = _candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:2026-03-31", candidate_can_create_quarter=False)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["rows"]["candidate_rows_rejected"] == 1
    assert LEGACY_CAN_CREATE_NEW_CANONICAL_Q is False


def test_publish_null_may_be_filled_but_existing_publish_not_overwritten(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(publish_date=None)])
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        engine = V3CanonicalMigrationEngine(conn)
        engine.apply_source_batch([_candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:P1", publish_date="2026-04-20", values={}, candidate_can_create_quarter=False)], source="LEGACY", migration_run_id="RUN", now_utc=NOW)
        engine.apply_source_batch([_candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:P2", publish_date="2026-04-21", values={}, candidate_can_create_quarter=False)], source="LEGACY", migration_run_id="RUN", now_utc=NOW)
        publish_date = conn.execute("SELECT publish_date FROM v3_quarter").fetchone()[0]

    assert publish_date == "2026-04-20"


def test_inactive_company_existing_q_may_be_enriched(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(ticker="OLD", values={"cash": None}, approved_company_active=False)])
    candidate = _candidate(ticker="OLD", source_system="LEGACY", source_record_id="LEGACY:OLD:2026-03-31", values={"cash": 5.0}, candidate_can_create_quarter=False)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["field_contributions"]["cash"]["FIELD_FILLED_FROM_NULL"] == 1


def test_semantic_risk_field_cannot_veto_identity_alone() -> None:
    identity = classify_legacy_identity(_row(ebitda=1_000_000.0, free_cashflow=500_000.0), _legacy(ebitda=-1_000_000.0, free_cashflow=-900_000.0))

    assert identity["apply_state"] == AUTO_ENRICH_ALLOWED
    assert identity["semantic_risk_conflicts"] >= 1


def test_v2_fill_counterfactual_same_and_different_value() -> None:
    v3_by_key = {
        ("AAA", 2026, "Q1"): _row(revenue=100.0, cash=5.0),
        ("BBB", 2026, "Q1"): _row(ticker="BBB", revenue=100.0, cash=5.0),
    }
    legacy = {
        ("AAA", "2026-03-31"): _legacy(revenue=100.0),
        ("BBB", "2026-03-31"): _legacy(ticker="BBB", revenue=120.0),
    }
    rows = v2_counterfactual(
        [
            {"ticker": "AAA", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "field": "revenue"},
            {"ticker": "BBB", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "field": "revenue"},
        ],
        v3_by_key,
        legacy,
    )

    assert counterfactual_summary(rows)["LEGACY_AVAILABLE_SAME_VALUE"] == 1
    assert counterfactual_summary(rows)["LEGACY_AVAILABLE_DIFFERENT_VALUE"] == 1


def test_no_overwrite_proof_and_policy_constants(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0})])
    before = snapshot_existing_non_null(db)
    after = snapshot_existing_non_null(db)

    assert no_overwrite_proof(before, after)["existing_non_null_values_overwritten"] == 0
    assert LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE is False
    assert V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE is False


def test_idempotent_reapply_and_no_company_activity_change(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"cash": None})])
    candidate = _candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:2026-03-31", values={"cash": 7.0}, candidate_can_create_quarter=False)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        engine = V3CanonicalMigrationEngine(conn)
        first = engine.apply_source_batch([candidate], source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()
        second = engine.apply_source_batch([candidate], source="LEGACY", migration_run_id="RUN", now_utc=NOW).to_dict()
        companies = conn.execute("SELECT COUNT(*) AS n, SUM(active=1) AS active FROM v3_company").fetchone()

    assert first["field_contributions"]["cash"]["FIELD_FILLED_FROM_NULL"] == 1
    assert "FIELD_FILLED_FROM_NULL" not in second["field_contributions"]["cash"]
    assert companies["n"] == 1
    assert companies["active"] == 1


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


def _legacy(**overrides):
    row = _row(**overrides)
    row["period_end_date"] = overrides.get("period_end_date", "2026-03-31")
    row["publish_date"] = overrides.get("publish_date")
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


def _seed_company_only(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        apply_v3_schema(conn)
        V3CompanyRepository(conn).admit_company(market="usa", ticker="AAA", active=True, now_utc=NOW)
        conn.commit()
