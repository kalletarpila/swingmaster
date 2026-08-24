from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase6b_score_lifecycle_calibration_design as p6b


def test_0_15_defines_16_reachable_values() -> None:
    assert p6b.full_scale_values(0, 15) == list(range(16))


def test_no_sparse_scale_for_scalar_component() -> None:
    result = p6b.validate_full_scale({0, 5, 10, 15}, min_score=0, max_score=15)
    assert result["valid"] is False
    assert 1 in result["dead_values"]


def test_score_0_reachable() -> None:
    assert 0 in p6b.full_scale_values(0, 15)


def test_score_max_reachable() -> None:
    assert 15 in p6b.full_scale_values(0, 15)


def test_full_scale_validator_detects_dead_values() -> None:
    assert p6b.validate_full_scale({0, 1, 3}, min_score=0, max_score=3)["dead_values"] == [2]


def test_negative_ebit_margin_floor() -> None:
    result = p6b.classify_positive_only_good(-0.01)
    assert result.state == "BAD_ECONOMIC_VALUE"
    assert result.score_floor == 0


def test_positive_ebit_margin_enters_positive_domain() -> None:
    assert p6b.classify_positive_only_good(0.01).state == "POSITIVE_SCORE_DOMAIN"


def test_negative_net_debt_not_bad() -> None:
    assert p6b.classify_net_debt(-10.0).state == "NET_CASH_FAVORABLE"


def test_ev_ebit_negative_denominator_not_meaningful() -> None:
    assert p6b.classify_multiple_denominator(-5.0).state == "NOT_MEANINGFUL"


def test_missing_is_not_zero() -> None:
    assert p6b.classify_positive_only_good(None).state == "MISSING_DATA"


def test_bad_is_not_missing() -> None:
    assert p6b.classify_positive_only_good(-1.0).state != "MISSING_DATA"


def test_not_meaningful_is_not_missing() -> None:
    assert p6b.classify_multiple_denominator(-1.0).state != "MISSING_DATA"


def test_negative_and_deteriorating_ebit() -> None:
    assert p6b.classify_signed_transition(-5.0, -7.0) == "NEGATIVE_AND_DETERIORATING"


def test_negative_but_improving_ebit() -> None:
    assert p6b.classify_signed_transition(-5.0, -2.0) == "NEGATIVE_BUT_IMPROVING"


def test_crossing_positive() -> None:
    assert p6b.classify_signed_transition(-1.0, 2.0) == "CROSSING_TO_POSITIVE"


def test_positive_growth() -> None:
    assert p6b.classify_signed_transition(2.0, 4.0) == "POSITIVE_AND_GROWING"


def test_no_naive_percent_growth_across_zero() -> None:
    assert p6b.classify_signed_transition(-1.0, 1.0) == "CROSSING_TO_POSITIVE"


def test_2021_2025_only_calibration() -> None:
    assert p6b.calibration_windows()["calibration"] == {"start": "2021-01-01", "end": "2025-12-31", "fit_thresholds": 1}


def test_2026_excluded_from_fitting() -> None:
    assert p6b.calibration_windows()["oos"]["fit_thresholds"] == 0


def test_2020_excluded_from_fitting() -> None:
    assert p6b.calibration_windows()["stress"]["fit_thresholds"] == 0


def test_per_year_distribution_requirement() -> None:
    assert "2021, 2022, 2023, 2024, 2025" in p6b.distribution_requirements_md()


def test_lifecycle_separate_from_score() -> None:
    assert "not an attractiveness score" in p6b.lifecycle_states_md()


def test_transition_matrix_contract() -> None:
    assert "state_t -> state_t+1 transition matrix" in p6b.lifecycle_transition_design_md()


def test_hysteresis_contract() -> None:
    assert "two consecutive quarters" in p6b.lifecycle_hysteresis_design_md()


def test_missing_feature_handling() -> None:
    rows = p6b.lifecycle_feature_design()
    assert all(row["missing_policy"] for row in rows)


def test_score_writes_zero(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    summary = p6b.run_phase6b_design(v3_db=db, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert summary["production_writes"]["score"] == 0


def test_lifecycle_writes_zero(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    summary = p6b.run_phase6b_design(v3_db=db, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert summary["production_writes"]["lifecycle"] == 0


def test_valuation_writes_zero(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    summary = p6b.run_phase6b_design(v3_db=db, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert summary["production_writes"]["valuation"] == 0


def test_ttm_writes_zero(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    summary = p6b.run_phase6b_design(v3_db=db, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert summary["production_writes"]["ttm"] == 0


def test_canonical_writes_zero(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    summary = p6b.run_phase6b_design(v3_db=db, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert summary["production_writes"]["canonical"] == 0


def test_existing_score_inventory_flags_sparse_scales() -> None:
    assert any(row["scale_status"] == "SPARSE_SCALE_REQUIRES_RECALIBRATION" for row in p6b.existing_score_component_inventory())


def test_economic_domain_policy_for_every_primary_component() -> None:
    policies = {row["metric_id"] for row in p6b.economic_domain_policies()}
    components = {row["metric_id"] for row in p6b.proposed_score_components() if row["role"].startswith("PRIMARY")}
    assert components.issubset(policies)


def fixture_db(tmp_path: Path) -> Path:
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY);
            INSERT INTO v3_company VALUES (1);
            INSERT INTO v3_quarter VALUES (10);
            INSERT INTO v3_quarter_fundamentals VALUES (10);
            INSERT INTO v3_ttm VALUES (100);
            """
        )
    return db
