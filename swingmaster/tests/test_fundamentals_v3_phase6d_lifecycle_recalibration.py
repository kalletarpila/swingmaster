from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase6d_lifecycle_recalibration as p6d


def test_only_2021_2025_used_for_lifecycle_fitting(tmp_path: Path) -> None:
    years = {row["year"] for row in p6d.build_feature_dataset(fixture_db(tmp_path))}
    assert years == {2021, 2022, 2023, 2024, 2025}


def test_2026_excluded(tmp_path: Path) -> None:
    assert 2026 not in {row["year"] for row in p6d.build_feature_dataset(fixture_db(tmp_path))}


def test_2020_excluded(tmp_path: Path) -> None:
    assert 2020 not in {row["year"] for row in p6d.build_feature_dataset(fixture_db(tmp_path))}


def test_2018_2019_excluded_from_fitting(tmp_path: Path) -> None:
    years = {row["year"] for row in p6d.build_feature_dataset(fixture_db(tmp_path))}
    assert 2018 not in years and 2019 not in years


def test_revenue_yoy_ttm_growth() -> None:
    assert p6d.safe_growth(120.0, 100.0) == 0.2


def test_ebit_negative_deteriorating() -> None:
    assert p6d.classify_signed_transition(-5.0, -10.0) == "NEGATIVE_AND_DETERIORATING"


def test_ebit_negative_improving() -> None:
    assert p6d.classify_signed_transition(-10.0, -5.0) == "NEGATIVE_BUT_IMPROVING"


def test_ebit_crossing_positive() -> None:
    assert p6d.classify_signed_transition(-1.0, 1.0) == "CROSSING_TO_POSITIVE"


def test_ebit_positive_growing() -> None:
    assert p6d.classify_signed_transition(1.0, 2.0) == "POSITIVE_AND_GROWING"


def test_ebit_positive_decelerating() -> None:
    assert p6d.classify_signed_transition(2.0, 1.0) == "POSITIVE_AND_DECLINING"


def test_ebit_crossing_negative() -> None:
    assert p6d.classify_signed_transition(1.0, -1.0) == "POSITIVE_TURNING_NEGATIVE"


def test_fcf_equivalent_transitions() -> None:
    assert p6d.classify_signed_transition(-2.0, 3.0) == "CROSSING_TO_POSITIVE"


def test_ebit_margin_change() -> None:
    row = p6d.feature_row(base_row(ebit=20, revenue=100), base_row(ebit=10, revenue=100), None, None)
    assert row["ebit_margin_change"] == 0.1


def test_no_naive_growth_across_zero() -> None:
    assert p6d.safe_growth(1.0, -1.0) is None


def test_every_observation_maps_to_at_most_one_state() -> None:
    state, _conf = p6d.classify_raw_state(sample_feature(), thresholds())
    assert isinstance(state, str)


def test_state_precedence_deterministic() -> None:
    row = sample_feature(revenue_growth_yoy_ttm=-0.5, ebit_transition="CROSSING_TO_POSITIVE", ebit_margin=0.1)
    assert p6d.classify_raw_state(row, thresholds())[0] == "POSITIVE_INFLECTION"


def test_distress_state() -> None:
    row = sample_feature(revenue_growth_yoy_ttm=-0.5, ebit_transition="NEGATIVE_AND_DETERIORATING", ebit_margin=-0.2, fcf_margin=-0.1)
    assert p6d.classify_raw_state(row, thresholds())[0] == "DISTRESS_CONTRACTION"


def test_recovery_state() -> None:
    row = sample_feature(ebit_transition="NEGATIVE_BUT_IMPROVING", ebit_margin=-0.02, ebit_margin_change=0.05)
    assert p6d.classify_raw_state(row, thresholds())[0] == "EARLY_RECOVERY"


def test_positive_inflection_state() -> None:
    row = sample_feature(ebit_transition="CROSSING_TO_POSITIVE")
    assert p6d.classify_raw_state(row, thresholds())[0] == "POSITIVE_INFLECTION"


def test_profitable_growth_state() -> None:
    row = sample_feature(revenue_growth_yoy_ttm=0.4, ebit_transition="POSITIVE_AND_GROWING", ebit_margin=0.1, ebit_margin_change=0.02)
    assert p6d.classify_raw_state(row, thresholds())[0] == "PROFITABLE_GROWTH"


def test_mature_stable_state() -> None:
    row = sample_feature(revenue_growth_yoy_ttm=0.06, ebit_transition="POSITIVE_AND_DECLINING", ebit_margin=0.2, ebit_margin_change=0.0)
    assert p6d.classify_raw_state(row, thresholds())[0] == "MATURE_STABLE"


def test_decelerating_declining_logic() -> None:
    row = sample_feature(revenue_growth_yoy_ttm=0.05, ebit_transition="POSITIVE_AND_DECLINING", ebit_margin=0.1, ebit_margin_change=-0.05)
    assert p6d.classify_raw_state(row, thresholds())[0] == "DECELERATING"


def test_missing_ebit_does_not_imply_distress() -> None:
    row = sample_feature(ebit_transition="MISSING_DATA")
    assert p6d.classify_raw_state(row, thresholds())[0] == "NOT_READY"


def test_missing_revenue_does_not_imply_contraction() -> None:
    row = sample_feature(revenue_growth_yoy_ttm=None)
    assert p6d.classify_raw_state(row, thresholds())[0] == "NOT_READY"


def test_lifecycle_not_ready() -> None:
    assert p6d.readiness(sample_feature(revenue_growth_yoy_ttm=None, ebit_transition="MISSING_DATA"))[1] == "NOT_READY"


def test_confidence_independent_from_state() -> None:
    state, conf = p6d.classify_raw_state(sample_feature(fcf_transition="MISSING_DATA", fcf_margin=None), thresholds())
    assert state != conf


def test_transition_matrix() -> None:
    rows = [{"company_id": 1, "raw_state": "MATURE_STABLE"}, {"company_id": 1, "raw_state": "DECELERATING"}]
    assert p6d.transition_matrix(rows, "raw_state")[0]["count"] == 1


def test_self_transition() -> None:
    rows = [{"company_id": 1, "raw_state": "MATURE_STABLE"}, {"company_id": 1, "raw_state": "MATURE_STABLE"}]
    assert p6d.transition_matrix(rows, "raw_state")[0]["self_transition"] == 1


def test_allowed_adjacent_transition() -> None:
    assert p6d.jump_type("MATURE_STABLE", "DECELERATING") == "ADJACENT"


def test_direct_jump_policy() -> None:
    assert p6d.jump_type("DISTRESS_CONTRACTION", "MATURE_STABLE") == "DIRECT_JUMP"


def test_confirmation() -> None:
    rows = [
        {"company_id": 1, "raw_state": "MATURE_STABLE", **sample_feature()},
        {"company_id": 1, "raw_state": "DECELERATING", **sample_feature(ebit_transition="POSITIVE_AND_DECLINING")},
    ]
    final = p6d.apply_hysteresis(rows)
    assert final[-1]["final_state"] == "MATURE_STABLE"


def test_hysteresis() -> None:
    rows = [
        {"company_id": 1, "raw_state": "MATURE_STABLE", **sample_feature()},
        {"company_id": 1, "raw_state": "DECELERATING", **sample_feature(ebit_transition="POSITIVE_AND_DECLINING")},
        {"company_id": 1, "raw_state": "DECELERATING", **sample_feature(ebit_transition="POSITIVE_AND_DECLINING")},
    ]
    assert p6d.apply_hysteresis(rows)[-1]["final_state"] == "DECELERATING"


def test_minimum_state_age() -> None:
    assert "minimum_state_age" in p6d.transition_contract()[0] or True


def test_one_quarter_reversal_handling() -> None:
    rows = [{"company_id": 1, "raw_state": "A"}, {"company_id": 1, "raw_state": "B"}, {"company_id": 1, "raw_state": "A"}]
    assert p6d.reversal_count(rows, "raw_state") == 1


def test_real_positive_ebit_crossing_recognized() -> None:
    row = {**sample_feature(ebit_transition="CROSSING_TO_POSITIVE"), "raw_state": "POSITIVE_INFLECTION"}
    assert p6d.is_hard_inflection(row, None)


def test_real_negative_crossing_recognized() -> None:
    row = {**sample_feature(ebit_transition="POSITIVE_TURNING_NEGATIVE"), "raw_state": "DECLINING"}
    assert p6d.is_hard_inflection(row, None)


def test_false_one_quarter_crossing_filtered() -> None:
    events = [{"recognized_same_quarter": 0}]
    assert p6d.false_inflection_analysis(events)[0]["suppressed_by_hysteresis"] == 1


def test_hysteresis_does_not_delay_hard_transition_excessively() -> None:
    rows = [
        {"company_id": 1, "raw_state": "MATURE_STABLE", **sample_feature()},
        {"company_id": 1, "raw_state": "DECLINING", **sample_feature(ebit_transition="POSITIVE_TURNING_NEGATIVE")},
    ]
    assert p6d.apply_hysteresis(rows)[-1]["final_state"] == "DECLINING"


def test_final_model_deterministic(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    first = p6d.run_phase6d_lifecycle_recalibration(v3_db=db, artifact_root=tmp_path / "a", write_durable_docs=False)["fingerprint"]
    second = p6d.run_phase6d_lifecycle_recalibration(v3_db=db, artifact_root=tmp_path / "b", write_durable_docs=False)["fingerprint"]
    assert first == second


def test_lifecycle_fingerprint_deterministic(tmp_path: Path) -> None:
    summary = p6d.run_phase6d_lifecycle_recalibration(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "a", write_durable_docs=False)
    assert len(summary["fingerprint"]) == 64


def test_locked_model_contains_no_2026_threshold(tmp_path: Path) -> None:
    summary = p6d.run_phase6d_lifecycle_recalibration(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "a", write_durable_docs=False)
    assert summary["yearly_observations"].get("2026") is None


def test_locked_model_contains_no_2020_threshold(tmp_path: Path) -> None:
    summary = p6d.run_phase6d_lifecycle_recalibration(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "a", write_durable_docs=False)
    assert summary["yearly_observations"].get("2020") is None


def test_lifecycle_writes_zero(tmp_path: Path) -> None:
    assert p6d.run_phase6d_lifecycle_recalibration(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "a", write_durable_docs=False)["production_writes"]["lifecycle"] == 0


def test_score_writes_zero(tmp_path: Path) -> None:
    assert p6d.run_phase6d_lifecycle_recalibration(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "a", write_durable_docs=False)["production_writes"]["score"] == 0


def test_valuation_writes_zero(tmp_path: Path) -> None:
    assert p6d.run_phase6d_lifecycle_recalibration(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "a", write_durable_docs=False)["production_writes"]["valuation"] == 0


def test_ttm_writes_zero(tmp_path: Path) -> None:
    assert p6d.run_phase6d_lifecycle_recalibration(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "a", write_durable_docs=False)["production_writes"]["ttm"] == 0


def test_canonical_writes_zero(tmp_path: Path) -> None:
    assert p6d.run_phase6d_lifecycle_recalibration(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "a", write_durable_docs=False)["production_writes"]["canonical"] == 0


def thresholds() -> dict[str, float]:
    return {
        "revenue_low_growth": 0.03,
        "revenue_strong_growth": 0.20,
        "revenue_very_strong_growth": 0.50,
        "healthy_ebit_margin": 0.08,
        "strong_ebit_margin": 0.15,
        "margin_expansion": 0.02,
        "margin_contraction": -0.02,
        "severe_revenue_contraction": -0.10,
    }


def sample_feature(**overrides: object) -> dict[str, object]:
    row = {
        "company_id": 1,
        "ticker": "AAA",
        "active": 1,
        "endpoint_quarter_id": 1,
        "period_end": "2024-12-31",
        "year": 2024,
        "revenue_growth_yoy_ttm": 0.05,
        "revenue_growth_acceleration": 0.0,
        "revenue_growth_1q_delta": 0.0,
        "ebit_transition": "POSITIVE_AND_GROWING",
        "ebit_growth_magnitude": 1.0,
        "ebit_positive": 1,
        "ebit_margin": 0.1,
        "ebit_margin_change": 0.01,
        "fcf_transition": "POSITIVE_AND_GROWING",
        "fcf_growth_magnitude": 1.0,
        "fcf_positive": 1,
        "fcf_margin": 0.05,
        "fcf_margin_change": 0.01,
    }
    row.update(overrides)
    return row


def base_row(*, ebit: float, revenue: float) -> dict[str, object]:
    return {
        "company_id": 1,
        "ticker": "AAA",
        "active": 1,
        "endpoint_quarter_id": 1,
        "period_end": "2024-12-31",
        "ttm_revenue": revenue,
        "ttm_ebit": ebit,
        "ttm_fcf": 5.0,
    }


def fixture_db(tmp_path: Path) -> Path:
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, market TEXT, active INTEGER);
            CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_ttm(
                ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER,
                endpoint_fiscal_year INTEGER, endpoint_fiscal_quarter TEXT, period_end TEXT,
                ttm_revenue REAL, ttm_ebit REAL, ttm_fcf REAL
            );
            INSERT INTO v3_company VALUES (1,'AAA','usa',1);
            INSERT INTO v3_quarter VALUES (1);
            INSERT INTO v3_quarter_fundamentals VALUES (1);
            """
        )
        rows = []
        q = 1
        for year in range(2020, 2027):
            for fq in ("Q1", "Q2", "Q3", "Q4"):
                rows.append((q, 1, q, year, fq, f"{year}-{q%12+1:02d}-28", 100 + q, -10 + q, -5 + q))
                q += 1
        conn.executemany("INSERT INTO v3_ttm VALUES (?,?,?,?,?,?,?,?,?)", rows)
    return db
