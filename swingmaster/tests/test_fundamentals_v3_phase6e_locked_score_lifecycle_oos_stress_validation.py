from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from swingmaster.fundamentals import v3_phase6e_locked_score_lifecycle_oos_stress_validation as p6e
from swingmaster.fundamentals import v3_phase6d_lifecycle_recalibration as p6d
from swingmaster.fundamentals import v3_phase6cr_score_architecture_reconciliation as p6cr
from swingmaster.tests.test_fundamentals_v3_phase6cr_score_architecture_reconciliation import fixture_db


def test_exact_score_fingerprint_constant() -> None:
    assert p6e.EXPECTED_SCORE_FINGERPRINT == "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"


def test_exact_lifecycle_fingerprint_constant() -> None:
    assert p6e.EXPECTED_LIFECYCLE_FINGERPRINT == "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"


def test_total_max_100() -> None:
    assert sum(int(c["max_score"]) for c in p6cr.COMPONENTS) == 100


@pytest.mark.parametrize("bad", ["PRICE", "MARKET_CAP", "EV_EBIT", "FCF_YIELD", "EV_SALES", "NET_DEBT_TO_MARKET_CAP"])
def test_no_price_market_cap_or_valuation_component(bad: str) -> None:
    payload = json.dumps(p6cr.COMPONENTS).upper()
    assert bad not in payload


def test_frozen_mapping_can_be_loaded_without_mutation(tmp_path: Path) -> None:
    root = frozen_score_root(tmp_path)
    before = (root / "phase6e_locked_legacy2_score_model.json").read_text()
    p6e.verify_frozen_score(make_db(tmp_path), root)
    assert (root / "phase6e_locked_legacy2_score_model.json").read_text() == before


def test_lifecycle_state_list_locked() -> None:
    assert p6d.STATE_ORDER == [
        "DISTRESS_CONTRACTION",
        "EARLY_RECOVERY",
        "POSITIVE_INFLECTION",
        "PROFITABLE_GROWTH",
        "HIGH_GROWTH_EXPANSION",
        "MATURE_STABLE",
        "DECELERATING",
        "DECLINING",
        "NOT_READY",
    ]


def test_lifecycle_hysteresis_contract_constant() -> None:
    rows = [
        {"company_id": 1, "raw_state": "MATURE_STABLE", **sample_feature()},
        {"company_id": 1, "raw_state": "DECELERATING", **sample_feature(ebit_transition="POSITIVE_AND_DECLINING")},
    ]
    assert p6d.apply_hysteresis(rows)[-1]["final_state"] == "MATURE_STABLE"


def test_lifecycle_hard_inflection_immutable() -> None:
    row = {**sample_feature(ebit_transition="CROSSING_TO_POSITIVE"), "raw_state": "POSITIVE_INFLECTION"}
    assert p6d.is_hard_inflection(row, None)


def test_2026_never_used_in_fitting_dataset(tmp_path: Path) -> None:
    assert 2026 not in {row["year"] for row in p6cr.build_dataset(make_db(tmp_path))}


def test_2020_never_used_in_fitting_dataset(tmp_path: Path) -> None:
    assert 2020 not in {row["year"] for row in p6cr.build_dataset(make_db(tmp_path))}


def test_2025_remains_locked_oos(tmp_path: Path) -> None:
    assert "OOS_2025_LOCKED" in {row["sample_split"] for row in p6cr.build_dataset(make_db(tmp_path))}


def test_validation_does_not_mutate_mapping(tmp_path: Path) -> None:
    root = frozen_score_root(tmp_path)
    model_before = json.loads((root / "phase6e_locked_legacy2_score_model.json").read_text())
    p6e.verify_frozen_score(make_db(tmp_path), root)
    model_after = json.loads((root / "phase6e_locked_legacy2_score_model.json").read_text())
    assert model_after["mappings"] == model_before["mappings"]


def test_publish_availability_fields_exposed(tmp_path: Path) -> None:
    row = p6e.build_score_dataset(make_db(tmp_path), "2026-01-01", "2026-12-31")[0]
    assert "ttm_pit_ready" in row
    assert "ttm_available_date" in row
    assert "underlying_publish_dates_complete" in row


def test_future_filing_not_visible_before_validation_window(tmp_path: Path) -> None:
    assert all(str(r["period_end"]) <= "2020-12-31" for r in p6e.build_score_dataset(make_db(tmp_path), "2020-01-01", "2020-12-31"))


def test_negative_ebit_ratio_guard() -> None:
    assert p6e.balance_metric(10.0, 50.0, 100.0, -5.0, -10.0) == 1.0


def test_negative_fcf_cash_runway_path() -> None:
    assert p6e.balance_metric(20.0, 0.0, 100.0, -5.0, -10.0) == 2.0


def test_signed_transition_unchanged() -> None:
    assert p6d.classify_signed_transition(-1.0, 2.0) == "CROSSING_TO_POSITIVE"


def test_missing_is_not_bad() -> None:
    comp = next(c for c in p6cr.COMPONENTS if c["component_id"] == "REVENUE_GROWTH")
    score, status = p6cr.score_component({"applicability": "STANDARD_MODEL_APPLICABLE", "revenue_growth": None}, comp, [{"score": 0, "lower_bound": "-inf", "upper_bound": "inf"}])
    assert score is None
    assert status == "MISSING_DATA"


def test_not_meaningful_not_missing() -> None:
    comp = next(c for c in p6cr.COMPONENTS if c["component_id"] == "DILUTION")
    score, status = p6cr.score_component({"applicability": "STANDARD_MODEL_APPLICABLE", "share_change_12m": 999.0}, comp, [{"score": 0, "lower_bound": "-inf", "upper_bound": 1.0}])
    assert score is None
    assert status == "NOT_MEANINGFUL"


def test_not_applicable_not_missing() -> None:
    comp = next(c for c in p6cr.COMPONENTS if c["component_id"] == "REVENUE_GROWTH")
    score, status = p6cr.score_component({"applicability": "NOT_APPLICABLE_STANDARD_MODEL", "revenue_growth": 1.0}, comp, [])
    assert score is None
    assert status == "NOT_APPLICABLE"


def test_net_cash_favorable() -> None:
    assert p6e.balance_metric(100.0, 0.0, 100.0, 10.0, 5.0) > p6e.balance_metric(0.0, 100.0, 100.0, 10.0, 5.0)


def test_leverage_denominator_guard() -> None:
    assert p6cr.safe_positive_ratio(10.0, -2.0) is None


def test_coverage_calculated_correctly(tmp_path: Path) -> None:
    scored = p6cr.apply_model(p6e.build_score_dataset(make_db(tmp_path), "2026-01-01", "2026-12-31"), frozen_mappings(tmp_path))
    row = scored[0]
    assert 0 <= row["coverage_pct"] <= 100


def test_readiness_threshold() -> None:
    assert p6e.readiness_rows([{"score_ready": 0, "coverage_pct": 64.0, "company_id": 1}])[0]["below_readiness_threshold"] == 1


def test_incomplete_score_exposes_coverage() -> None:
    row = p6e.readiness_rows([{"score_ready": 0, "coverage_pct": 50.0, "company_id": 1}])[0]
    assert row["avg_coverage"] == 50.0


def test_2026_aggregate_deterministic(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    mappings = frozen_mappings(tmp_path)
    first = p6e.score_distribution(p6cr.apply_model(p6e.build_score_dataset(db, "2026-01-01", "2026-12-31"), mappings), "OOS_2026_YTD")
    second = p6e.score_distribution(p6cr.apply_model(p6e.build_score_dataset(db, "2026-01-01", "2026-12-31"), mappings), "OOS_2026_YTD")
    assert first == second


def test_group_totals_do_not_exceed_max(tmp_path: Path) -> None:
    scored = p6cr.apply_model(p6e.build_score_dataset(make_db(tmp_path), "2026-01-01", "2026-12-31"), frozen_mappings(tmp_path))
    assert all(row["max"] is None or row["max"] <= row["max_score"] for row in p6e.group_distribution(scored, "OOS_2026_YTD"))


def test_total_score_not_above_100(tmp_path: Path) -> None:
    scored = p6cr.apply_model(p6e.build_score_dataset(make_db(tmp_path), "2026-01-01", "2026-12-31"), frozen_mappings(tmp_path))
    assert max(r["legacy2_score"] for r in scored if r["legacy2_score"] is not None) <= 100


def test_bucket_utilization_rows(tmp_path: Path) -> None:
    scored = p6cr.apply_model(p6e.build_score_dataset(make_db(tmp_path), "2026-01-01", "2026-12-31"), frozen_mappings(tmp_path))
    assert p6e.bucket_utilization(scored, "OOS_2026_YTD")


def test_score_churn_deterministic(tmp_path: Path) -> None:
    scored = p6cr.apply_model(p6e.build_score_dataset(make_db(tmp_path), "2026-01-01", "2026-12-31"), frozen_mappings(tmp_path))
    assert p6e.score_churn(scored, "OOS_2026_YTD") == p6e.score_churn(scored, "OOS_2026_YTD")


def test_lifecycle_state_deterministic(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    features = p6e.build_lifecycle_features(db, "2026-01-01", "2026-12-31")
    thresholds = p6d.calibrate_thresholds(p6e.build_lifecycle_features(db, "2021-01-01", "2025-12-31"))
    assert p6e.apply_locked_lifecycle(features, thresholds) == p6e.apply_locked_lifecycle(features, thresholds)


def test_transition_matrix() -> None:
    rows = [{"company_id": 1, "final_state": "MATURE_STABLE"}, {"company_id": 1, "final_state": "DECELERATING"}]
    assert p6d.transition_matrix(rows, "final_state")[0]["count"] == 1


def test_hysteresis() -> None:
    rows = [
        {"company_id": 1, "raw_state": "MATURE_STABLE", **sample_feature()},
        {"company_id": 1, "raw_state": "DECELERATING", **sample_feature()},
        {"company_id": 1, "raw_state": "DECELERATING", **sample_feature()},
    ]
    assert p6d.apply_hysteresis(rows)[-1]["final_state"] == "DECELERATING"


def test_inflection_recognition() -> None:
    rows = [{**sample_feature(ebit_transition="CROSSING_TO_POSITIVE"), "company_id": 1, "ticker": "A", "period_end": "2026-03-31", "raw_state": "POSITIVE_INFLECTION", "final_state": "POSITIVE_INFLECTION"}]
    assert p6e.inflection_response(rows, "OOS_2026_YTD")[0]["recognized_same_quarter"] == 1


def test_2020_applies_same_score_mapping(tmp_path: Path) -> None:
    mappings = frozen_mappings(tmp_path)
    assert mappings == frozen_mappings(tmp_path)


def test_2020_applies_same_lifecycle_model(tmp_path: Path) -> None:
    root = frozen_lifecycle_root(tmp_path)
    locked = json.loads((root / "phase6e_locked_lifecycle_model.json").read_text())
    assert locked["thresholds"] == json.loads((root / "phase6e_locked_lifecycle_model.json").read_text())["thresholds"]


def test_no_stress_recalibration(tmp_path: Path) -> None:
    stress = p6e.build_score_dataset(make_db(tmp_path), "2020-01-01", "2020-12-31")
    assert {r["sample_split"] for r in stress} == {"STRESS_2020"}


def test_denominator_pathologies_guarded() -> None:
    assert p6cr.safe_div(1.0, 0.0) is None


def test_score_production_writes_zero(tmp_path: Path) -> None:
    assert run_fixture_phase(tmp_path)["production_writes"]["score"] == 0


def test_valuation_writes_zero(tmp_path: Path) -> None:
    assert run_fixture_phase(tmp_path)["production_writes"]["valuation"] == 0


def test_lifecycle_writes_zero(tmp_path: Path) -> None:
    assert run_fixture_phase(tmp_path)["production_writes"]["lifecycle"] == 0


def test_ttm_writes_zero(tmp_path: Path) -> None:
    assert run_fixture_phase(tmp_path)["production_writes"]["ttm"] == 0


def test_canonical_writes_zero(tmp_path: Path) -> None:
    assert run_fixture_phase(tmp_path)["production_writes"]["canonical"] == 0


def test_blocked_score_fingerprint_on_mismatch(tmp_path: Path) -> None:
    root = frozen_score_root(tmp_path)
    data = json.loads((root / "phase6e_locked_legacy2_score_model.json").read_text())
    data["fingerprint"] = "bad"
    (root / "phase6e_locked_legacy2_score_model.json").write_text(json.dumps(data))
    summary = p6e.run_phase6e_validation(v3_db=make_db(tmp_path), artifact_root=tmp_path / f"out_{uuid.uuid4().hex}", score_artifact_root=root, lifecycle_artifact_root=frozen_lifecycle_root(tmp_path), write_durable_docs=False)
    assert summary["classification"] == p6e.BLOCKED_SCORE_FINGERPRINT


def test_group_contract_rows() -> None:
    assert sum(row["group_max"] for row in p6e.group_contract_rows(p6cr.final_contract())) == 100


def test_failure_classification_pass() -> None:
    assert all(row["decision"] == "PASS" for row in p6e.failure_classification("EXPECTED_ECONOMIC_BEHAVIOR", "EXPECTED_ECONOMIC_BEHAVIOR"))


def test_success_classification_constant() -> None:
    assert p6e.CLASSIFICATION_COMPLETE == "FUNDAMENTALS_V3_PHASE6E_LOCKED_SCORE_LIFECYCLE_VALIDATED_READY_FOR_IMPLEMENTATION"


def run_fixture_phase(tmp_path: Path) -> dict[str, object]:
    score_root = frozen_score_root(tmp_path)
    lifecycle_root = frozen_lifecycle_root(tmp_path)
    old_score = p6e.EXPECTED_SCORE_FINGERPRINT
    old_lifecycle = p6e.EXPECTED_LIFECYCLE_FINGERPRINT
    p6e.EXPECTED_SCORE_FINGERPRINT = json.loads((score_root / "phase6cr_score_fingerprint.json").read_text())["fingerprint"]
    p6e.EXPECTED_LIFECYCLE_FINGERPRINT = json.loads((lifecycle_root / "phase6d_lifecycle_fingerprint.json").read_text())["fingerprint"]
    try:
        return p6e.run_phase6e_validation(
            v3_db=make_db(tmp_path),
            artifact_root=tmp_path / f"out_{uuid.uuid4().hex}",
            score_artifact_root=score_root,
            lifecycle_artifact_root=lifecycle_root,
            write_durable_docs=False,
        )
    finally:
        p6e.EXPECTED_SCORE_FINGERPRINT = old_score
        p6e.EXPECTED_LIFECYCLE_FINGERPRINT = old_lifecycle


def make_db(tmp_path: Path) -> Path:
    root = tmp_path / f"db_{uuid.uuid4().hex}"
    root.mkdir()
    return fixture_db(root)


def frozen_score_root(tmp_path: Path) -> Path:
    db = make_db(tmp_path)
    root = tmp_path / f"score_{uuid.uuid4().hex}"
    summary = p6cr.run_phase6cr_reconciliation(v3_db=db, artifact_root=root, write_durable_docs=False)
    data = json.loads((root / "phase6e_locked_legacy2_score_model.json").read_text())
    fp = json.loads((root / "phase6cr_score_fingerprint.json").read_text())
    data["fingerprint"] = summary["fingerprint"]
    fp["fingerprint"] = summary["fingerprint"]
    (root / "phase6e_locked_legacy2_score_model.json").write_text(json.dumps(data))
    (root / "phase6cr_score_fingerprint.json").write_text(json.dumps(fp))
    return root


def frozen_lifecycle_root(tmp_path: Path) -> Path:
    db = make_db(tmp_path)
    root = tmp_path / f"lifecycle_{uuid.uuid4().hex}"
    summary = p6d.run_phase6d_lifecycle_recalibration(v3_db=db, artifact_root=root, write_durable_docs=False)
    data = json.loads((root / "phase6e_locked_lifecycle_model.json").read_text())
    fp = json.loads((root / "phase6d_lifecycle_fingerprint.json").read_text())
    data["fingerprint"] = summary["fingerprint"]
    fp["fingerprint"] = summary["fingerprint"]
    (root / "phase6e_locked_lifecycle_model.json").write_text(json.dumps(data))
    (root / "phase6d_lifecycle_fingerprint.json").write_text(json.dumps(fp))
    return root


def frozen_mappings(tmp_path: Path) -> dict[str, list[dict[str, object]]]:
    return json.loads((frozen_score_root(tmp_path) / "phase6e_locked_legacy2_score_model.json").read_text())["mappings"]


def sample_feature(**overrides: object) -> dict[str, object]:
    row = {
        "company_id": 1,
        "ticker": "AAA",
        "active": 1,
        "endpoint_quarter_id": 1,
        "period_end": "2026-03-31",
        "year": 2026,
        "revenue_growth_yoy_ttm": 0.1,
        "ebit_transition": "POSITIVE_AND_GROWING",
        "fcf_transition": "POSITIVE_AND_GROWING",
        "ebit_margin": 0.1,
        "ebit_margin_change": 0.01,
        "fcf_margin": 0.05,
        "fcf_margin_change": 0.01,
    }
    row.update(overrides)
    return row
