from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_v2_historical_gap_fill import (
    build_new_q_candidate,
    calibrate_new_q_gate,
    classify_v2_only_history_candidate,
    compare_neighbor_sequence,
    corroborate_with_legacy,
    evaluate_period_cadence,
    final_new_q_classification,
    variant_collision,
)


NOW = "2026-08-23T00:00:00Z"


def test_v2_only_row_is_not_automatically_missing_q() -> None:
    result = _classify(legacy=None)

    assert result["final"]["final_classification"] != "STRONG_NEW_Q_CONFIRMED"


def test_between_two_neighbors_strong_case() -> None:
    result = _classify()

    assert result["neighbor"]["neighbor_sequence_class"] == "BETWEEN_TWO_CONFIRMED_NEIGHBORS"
    assert result["final"]["final_classification"] == "STRONG_NEW_Q_CONFIRMED"


def test_one_neighbor_case_is_probable_not_auto_create() -> None:
    result = _classify(next_row=None)

    assert result["neighbor"]["neighbor_sequence_class"] == "ADJACENT_TO_ONE_CONFIRMED_NEIGHBOR"
    assert result["final"]["final_classification"] == "PROBABLE_NEW_Q"


def test_period_cadence_exact_and_safe_variant() -> None:
    exact = compare_neighbor_sequence([_q(2020, "Q1", "2020-03-31"), _q(2020, "Q3", "2020-09-30")], 2020, "Q2", "2020-06-30")
    safe = compare_neighbor_sequence([_q(2020, "Q1", "2020-03-31"), _q(2020, "Q3", "2020-10-15")], 2020, "Q2", "2020-07-05")

    assert evaluate_period_cadence(exact)["period_cadence_class"] == "EXACT_EXPECTED"
    assert evaluate_period_cadence(safe)["period_cadence_class"] in {"SAFE_VARIANT", "PLAUSIBLE"}


def test_material_period_mismatch_blocks() -> None:
    result = _classify(period_end="2020-08-25")

    assert result["final"]["final_classification"] == "PERIOD_IDENTITY_CONFLICT"


def test_legacy_strong_support_and_conflict() -> None:
    assert corroborate_with_legacy(_v2(), _legacy())["legacy_corroboration_class"] == "LEGACY_STRONG_SUPPORT"
    assert corroborate_with_legacy(_v2(), _legacy(revenue=-110000.0, net_income=-80000.0, operating_cashflow=-120000.0))["legacy_corroboration_class"] == "LEGACY_CONFLICT"


def test_same_period_different_fyfq_blocks() -> None:
    collision = variant_collision("AAA", "2020-06-30", 2020, "Q2", {("AAA", "2020-06-30"): _q(2020, "Q3", "2020-06-30")})

    assert collision["variant_collision_class"] == "SAME_PERIOD_DIFFERENT_FYFQ"


def test_duplicate_existing_q_variant_blocks() -> None:
    collision = variant_collision("AAA", "2020-06-30", 2020, "Q2", {("AAA", "2020-06-30"): _q(2020, "Q2", "2020-06-30")})

    assert collision["variant_collision_class"] == "DUPLICATE_EXISTING_Q"


def test_explicit_v2_q4_without_between_neighbors_does_not_duplicate_sec_q4() -> None:
    final = final_new_q_classification(
        {"neighbor_sequence_class": "ADJACENT_TO_ONE_CONFIRMED_NEIGHBOR"},
        {"period_cadence_class": "PLAUSIBLE"},
        {"legacy_corroboration_class": "LEGACY_STRONG_SUPPORT"},
        {"variant_collision_class": "NO_CANONICAL_PERIOD_COLLISION"},
        {"adjacent_fingerprint_class": "MISSING_SLOT_FINGERPRINT_SUPPORT"},
        "Q4",
    )

    assert final == "POSSIBLE_WRONG_V2_MAPPING"


def test_adjacent_q_lookalike_blocks() -> None:
    previous_like = _v2(revenue=900000.0, gross_profit=300000.0, operating_income=90000.0, net_income=60000.0, operating_cashflow=80000.0)
    result = _classify(v2=previous_like, legacy={**previous_like})

    assert result["final"]["final_classification"] == "POSSIBLE_WRONG_V2_MAPPING"


def test_mapping_and_clear_wrong_classes_do_not_auto_create() -> None:
    assert final_new_q_classification(
        {"neighbor_sequence_class": "BETWEEN_TWO_CONFIRMED_NEIGHBORS"},
        {"period_cadence_class": "EXACT_EXPECTED"},
        {"legacy_corroboration_class": "LEGACY_CONFLICT"},
        {"variant_collision_class": "NO_CANONICAL_PERIOD_COLLISION"},
        {"adjacent_fingerprint_class": "MISSING_SLOT_FINGERPRINT_SUPPORT"},
        "Q2",
    ) == "LEGACY_CONFLICT"


def test_hidden_q_calibration_correct_recovery() -> None:
    rows = [
        _q(2020, "Q1", "2020-03-31", revenue=900000.0, gross_profit=300000.0, operating_income=90000.0, net_income=60000.0, operating_cashflow=80000.0),
        _q(2020, "Q2", "2020-06-30"),
        _q(2020, "Q3", "2020-09-30", revenue=1300000.0, gross_profit=500000.0, operating_income=150000.0, net_income=110000.0, operating_cashflow=140000.0),
    ]
    v2 = {("AAA", 2020, "Q2"): _v2()}
    legacy = {("AAA", "2020-06-30"): _legacy()}

    _, hidden = calibrate_new_q_gate(v3_rows=rows, v2_rows=v2, legacy_by_period=legacy)

    q2 = next(row for row in hidden if row["fiscal_quarter"] == "Q2")
    assert q2["correctly_recovered"] == 1
    assert q2["false_extra_q_created"] == 0


def test_strong_new_q_can_create(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(fiscal_quarter="Q1", period_end_date="2020-03-31"), _candidate(fiscal_quarter="Q3", period_end_date="2020-09-30")])
    row = {"market": "usa", "ticker": "AAA", "fiscal_year": 2020, "fiscal_quarter": "Q2", "period_end_date": "2020-06-30", "publish_date": "2020-08-01", "final_classification": "STRONG_NEW_Q_CONFIRMED"}
    candidate = build_new_q_candidate(row, _v2(), "RUN")

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["rows"]["canonical_quarters_created"] == 1


def test_pre_2018_cannot_create(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [])
    candidate = _candidate(source_system="V2", source_record_id="V2:OLD", fiscal_year=2017, period_end_date="2017-12-31", candidate_can_create_quarter=True)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()

    assert summary["rows"]["candidate_rows_rejected"] == 1


def test_shares_debt_direct_ebit_ebitda_allowed_without_derivation() -> None:
    candidate = build_new_q_candidate({"market": "usa", "ticker": "AAA", "fiscal_year": 2020, "fiscal_quarter": "Q2", "period_end_date": "2020-06-30", "publish_date": None, "final_classification": "STRONG_NEW_Q_CONFIRMED"}, _v2(total_debt=5.0, shares_outstanding=1.0, ebit=12.0, ebitda=15.0), "RUN")

    assert candidate.values["total_debt"] == 5.0
    assert candidate.values["shares_outstanding"] == 1.0
    assert candidate.values["ebit"] == 12.0
    assert candidate.values["ebitda"] == 15.0
    assert not candidate.derivation_inputs


def test_no_new_fcf_derivation_when_fcf_missing() -> None:
    candidate = build_new_q_candidate({"market": "usa", "ticker": "AAA", "fiscal_year": 2020, "fiscal_quarter": "Q2", "period_end_date": "2020-06-30", "publish_date": None, "final_classification": "STRONG_NEW_Q_CONFIRMED"}, _v2(free_cashflow=None, operating_cashflow=12.0, capex=-3.0), "RUN")

    assert "operating_cashflow" in candidate.values
    assert "capex" not in candidate.values
    assert "free_cashflow" not in candidate.values


def test_no_overwrite_and_publish_handling(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    _seed(db, [_candidate(values={"revenue": 100.0}, publish_date="2020-05-01")])
    candidate = _candidate(source_system="V2", source_record_id="V2:AAA", values={"revenue": 1010.0}, publish_date="2020-05-02", candidate_can_create_quarter=False)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch([candidate], source="V2", migration_run_id="RUN", now_utc=NOW).to_dict()
        revenue = conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0]

    assert summary["field_contributions"]["revenue"]["FIELD_CONFLICT"] == 1
    assert revenue == 100.0


def _classify(**overrides):
    previous = overrides.get("previous", _q(2020, "Q1", "2020-03-31", revenue=900000.0, gross_profit=300000.0, operating_income=90000.0, net_income=60000.0, operating_cashflow=80000.0))
    next_row = overrides.get("next_row", _q(2020, "Q3", "2020-09-30", revenue=1300000.0, gross_profit=500000.0, operating_income=150000.0, net_income=110000.0, operating_cashflow=140000.0))
    rows = [row for row in (previous, next_row) if row is not None]
    period_end = overrides.get("period_end", "2020-06-30")
    v2 = overrides.get("v2", _v2(period_end_date=period_end))
    legacy = overrides.get("legacy", _legacy())
    return classify_v2_only_history_candidate(
        v2=v2,
        base={"market": "usa", "ticker": "AAA", "company_id": 1, "active": 1, "v2_source_record_id": "V2:AAA:2020:Q2", "fiscal_year": 2020, "fiscal_quarter": "Q2", "period_end_date": period_end, "publish_date": None, "available_fields": "revenue"},
        v3_by_ticker={"AAA": rows},
        v3_by_ticker_period={},
        legacy_by_period={("AAA", period_end): legacy} if legacy is not None else {},
    )


def _q(fiscal_year: int, fiscal_quarter: str, period_end_date: str, **overrides):
    row = _v2(period_end_date=period_end_date)
    row.update({"market": "usa", "ticker": "AAA", "active": 1, "fiscal_year": fiscal_year, "fiscal_quarter": fiscal_quarter, "period_end_date": period_end_date})
    row.update(overrides)
    return row


def _v2(**overrides):
    row = {
        "market": "usa",
        "ticker": "AAA",
        "fiscal_year": 2020,
        "fiscal_quarter": "Q2",
        "period_end_date": "2020-06-30",
        "publish_date": "2020-08-01",
        "revenue": 1100000.0,
        "gross_profit": 400000.0,
        "operating_income": 120000.0,
        "ebit": 12.0,
        "ebitda": 15.0,
        "net_income": 80000.0,
        "operating_cashflow": 120000.0,
        "capex": -3.0,
        "free_cashflow": 9.0,
        "cash": 50.0,
        "total_debt": 5.0,
        "shares_outstanding": 1.0,
    }
    row.update(overrides)
    return row


def _legacy(**overrides):
    row = _v2()
    row.update(overrides)
    return row


def _candidate(**overrides) -> V3CanonicalMigrationCandidate:
    base = {
        "source_system": "YAHOO",
        "source_record_id": "YAHOO:AAA:2020:Q1",
        "migration_run_id": "RUN",
        "market": "usa",
        "ticker": "AAA",
        "fiscal_year": 2020,
        "fiscal_quarter": "Q1",
        "period_end_date": "2020-03-31",
        "publish_date": None,
        "values": {"revenue": 100.0, "cash": 10.0, "shares_outstanding": 1.0},
        "approved_company_active": True,
    }
    base.update(overrides)
    return V3CanonicalMigrationCandidate(**base)


def _seed(path: Path, candidates: list[V3CanonicalMigrationCandidate]) -> None:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        apply_v3_schema(conn)
        if candidates:
            V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source="YAHOO", migration_run_id="RUN", now_utc=NOW)
        else:
            conn.execute(
                "INSERT INTO v3_company (market, ticker, profile, active, admission_source, created_at_utc, updated_at_utc) VALUES ('usa', 'AAA', 'ORDINARY', 1, 'TEST', ?, ?)",
                (NOW, NOW),
            )
        conn.commit()
