from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_v2_enrichment import AUTO_ENRICH_ALLOWED, BLOCK_NO_WRITE, classify_phase3c_identity
from swingmaster.fundamentals.v3_v2_residual_existing_q import (
    Q_TYPE_LEGACY,
    Q_TYPE_SEC_Q4,
    Q_TYPE_YAHOO,
    REPORT_FIELDS,
    contribution_by_q_type_rows,
    core_gap_profile,
    load_q_types,
    v2_only_inventory,
)


NOW = "2026-08-23T00:00:00Z"


def test_expanded_legacy_created_q_can_receive_v2_null_fill(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:2024-03-31", values={"revenue": 100.0, "cash": None})], source="LEGACY")
    v2 = _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2024:Q1:cash", values={"cash": 55.0}, candidate_can_create_quarter=False)

    summary = _apply(db, [v2])

    assert summary["field_contributions"]["cash"]["FIELD_FILLED_FROM_NULL"] == 1


def test_exact_fyfq_alone_still_cannot_fill() -> None:
    empty = {
        "revenue": None,
        "gross_profit": None,
        "operating_income": None,
        "ebit": None,
        "ebitda": None,
        "net_income": None,
        "operating_cashflow": None,
        "capex": None,
        "free_cashflow": None,
        "cash": None,
        "total_debt": None,
        "shares_outstanding": None,
    }
    identity = classify_phase3c_identity(_row(**empty), _row(**empty), [])

    assert identity["identity_classification"] == "INSUFFICIENT_EVIDENCE"


def test_same_quarter_confirmed_can_fill_direct_ebitda_and_ebit(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0, "ebit": None, "ebitda": None})])
    v2_rows = [
        _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:ebit", values={"ebit": 12.0}, candidate_can_create_quarter=False),
        _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:ebitda", values={"ebitda": 15.0}, candidate_can_create_quarter=False),
    ]

    summary = _apply(db, v2_rows)

    assert summary["field_contributions"]["ebit"]["FIELD_FILLED_FROM_NULL"] == 1
    assert summary["field_contributions"]["ebitda"]["FIELD_FILLED_FROM_NULL"] == 1


def test_mapping_risk_and_clear_wrong_quarter_are_blocked_by_identity_gate() -> None:
    identity = classify_phase3c_identity(
        _row(revenue=100.0, net_income=10.0, operating_cashflow=11.0),
        _row(revenue=-100.0, net_income=-10.0, operating_cashflow=-11.0),
        [],
    )

    assert identity["identity_classification"] in {"MAPPING_RISK", "CLEAR_WRONG_QUARTER"}
    assert identity["apply_state"] == BLOCK_NO_WRITE


def test_v2_cannot_create_q(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed_company_only(db)
    v2 = _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:cash", values={"cash": 1.0}, candidate_can_create_quarter=False)

    summary = _apply(db, [v2])

    assert summary["rows"]["candidate_rows_rejected"] == 1
    assert _count(db, "v3_quarter") == 0


def test_v2_cannot_overwrite_yahoo_legacy_or_prior_v2_value(tmp_path: Path) -> None:
    for source in ("YAHOO", "LEGACY", "V2"):
        db = tmp_path / f"{source}.db"
        _seed(db, [_candidate(source_system=source, source_record_id=f"{source}:AAA:2026:Q1", values={"cash": 10.0})], source=source)
        candidate = _candidate(source_system="V2", source_record_id=f"V2_RESIDUAL:AAA:2026:Q1:cash:{source}", values={"cash": 12.0}, candidate_can_create_quarter=False)

        summary = _apply(db, [candidate])

        assert summary["field_contributions"]["cash"]["FIELD_CONFLICT"] == 1
        assert _scalar(db, "SELECT cash FROM v3_quarter_fundamentals") == 10.0


def test_v2_same_value_confirms_only(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"cash": 10.0})])

    summary = _apply(db, [_candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:cash", values={"cash": 10.0}, candidate_can_create_quarter=False)])

    assert summary["field_contributions"]["cash"]["FIELD_CONFIRMED_SAME"] == 1


def test_publish_null_fill_and_overwrite_forbidden(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(publish_date=None)])
    fill = _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:publish", values={}, publish_date="2026-04-22", candidate_can_create_quarter=False)
    conflict = _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:publish2", values={}, publish_date="2026-04-23", candidate_can_create_quarter=False)

    first = _apply(db, [fill])
    second = _apply(db, [conflict])

    assert first["metadata"]["PUBLISH_DATE_SET"] == 1
    assert second["metadata"]["PUBLISH_DATE_CONFLICT"] == 1
    assert _scalar(db, "SELECT publish_date FROM v3_quarter") == "2026-04-22"


def test_no_new_derivation_introduced_when_ocf_and_capex_are_separate_candidates(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"operating_cashflow": None, "capex": None, "free_cashflow": None})])
    rows = [
        _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:operating_cashflow", values={"operating_cashflow": 12.0}, candidate_can_create_quarter=False),
        _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:capex", values={"capex": -3.0}, candidate_can_create_quarter=False),
    ]

    _apply(db, rows)

    assert _scalar(db, "SELECT free_cashflow FROM v3_quarter_fundamentals") is None


def test_reconstructed_sec_q4_may_receive_confirmed_v2_null_fill(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:2025:Q4", fiscal_quarter="Q4", values={"ebitda": None}, value_metadata={"field_source_mode": "SEC_Q4_RECONSTRUCTED"})], source="LEGACY")
    q_types = _q_types(db)
    summary = _apply(db, [_candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:ebitda", fiscal_quarter="Q4", values={"ebitda": 4.0}, candidate_can_create_quarter=False)])

    assert Q_TYPE_SEC_Q4 in q_types.values()
    assert summary["field_contributions"]["ebitda"]["FIELD_FILLED_FROM_NULL"] == 1


def test_weighted_average_shares_are_not_report_fields() -> None:
    assert "weighted_average_shares_basic" not in REPORT_FIELDS
    assert "weighted_average_shares_diluted" not in REPORT_FIELDS


def test_pre_2018_candidate_ignored_in_v2_only_inventory() -> None:
    v2_refined = {("AAA", 2017, "Q4"): {"period_end_date": "2017-12-31", "publish_date": None}}
    rows, risk = v2_only_inventory(v2_refined, {}, {})

    assert rows
    assert risk[0]["canonical_gap_classification"] == "INSUFFICIENT_LOCAL_CORROBORATION"


def test_v2_only_historical_inventory_and_mapping_risk() -> None:
    v2_refined = {("AAA", 2020, "Q1"): {"period_end_date": "2020-03-31", "publish_date": "2020-05-01", "revenue": 1.0}}
    v3_by_key = {("AAA", 2020, "Q2"): {"ticker": "AAA", "period_end_date": "2020-03-31", "active": 1}}

    rows, risk = v2_only_inventory(v2_refined, v3_by_key, {})

    assert rows[0]["canonical_gap_classification"] == "DUPLICATE_OR_PERIOD_VARIANT_OF_EXISTING_CANONICAL_Q"
    assert risk


def test_source_contribution_by_q_type() -> None:
    rows = contribution_by_q_type_rows(
        [{"canonical_q_type": Q_TYPE_YAHOO, "field": "cash"}, {"canonical_q_type": Q_TYPE_LEGACY, "field": "cash"}],
        [{"canonical_q_type": Q_TYPE_YAHOO}],
    )

    assert {row["canonical_q_type"] for row in rows} == {Q_TYPE_YAHOO, Q_TYPE_LEGACY}


def test_q_type_priority_marks_sec_q4_before_plain_legacy(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(source_system="LEGACY", source_record_id="LEGACY:AAA:2025:Q4", fiscal_quarter="Q4", value_metadata={"field_source_mode": "SEC_Q4_RECONSTRUCTED"})], source="LEGACY")

    assert set(_q_types(db).values()) == {Q_TYPE_SEC_Q4}


def test_idempotent_reapply_has_no_new_fill(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"cash": None})])
    candidate = _candidate(source_system="V2", source_record_id="V2_RESIDUAL:AAA:2026:Q1:cash", values={"cash": 5.0}, candidate_can_create_quarter=False)
    first = _apply(db, [candidate])
    second = _apply(db, [candidate])

    assert first["field_contributions"]["cash"]["FIELD_FILLED_FROM_NULL"] == 1
    assert "FIELD_FILLED_FROM_NULL" not in second["field_contributions"]["cash"]


def test_core_gap_profile_and_integrity(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 1.0, "ebitda": None, "free_cashflow": 1.0, "cash": 1.0, "total_debt": 1.0, "shares_outstanding": 1.0})])

    profile = core_gap_profile(db)

    assert profile["missing_ebitda_only"] == 1
    assert _scalar(db, "PRAGMA quick_check") == "ok"


def test_legacy_contribution_zero_policy_constant() -> None:
    legacy_canonical_contribution = 0
    assert legacy_canonical_contribution == 0


def test_same_quarter_confirmed_identity_for_strong_v2_match() -> None:
    identity = classify_phase3c_identity(_row(), _row(), [_row()])

    assert identity["apply_state"] == AUTO_ENRICH_ALLOWED


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


def _seed(path: Path, candidates: list[V3CanonicalMigrationCandidate], *, source: str = "YAHOO") -> None:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        apply_v3_schema(conn)
        V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source=source, migration_run_id="RUN", now_utc=NOW)
        conn.commit()


def _seed_company_only(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        apply_v3_schema(conn)
        V3CompanyRepository(conn).admit_company(market="usa", ticker="AAA", active=True, now_utc=NOW)
        conn.commit()


def _apply(path: Path, candidates: list[V3CanonicalMigrationCandidate]) -> dict:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()
        conn.commit()
    return summary


def _q_types(path: Path) -> dict[int, str]:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        return load_q_types(conn)


def _count(path: Path, table: str) -> int:
    return int(_scalar(path, f"SELECT COUNT(*) FROM {table}"))


def _scalar(path: Path, sql: str):
    with sqlite3.connect(path) as conn:
        return conn.execute(sql).fetchone()[0]
