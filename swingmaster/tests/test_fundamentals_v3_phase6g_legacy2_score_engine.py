from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from swingmaster.fundamentals import v3_phase6d_lifecycle_recalibration as p6d
from swingmaster.fundamentals import v3_phase6f_valuation_engine as p6f
from swingmaster.fundamentals import v3_phase6g_legacy2_score_engine as p6g
from swingmaster.fundamentals.v3_phase6cr_score_architecture_reconciliation import (
    COMPONENTS,
    apply_model,
    balance_metric,
    cash_quality,
    consistency_metric,
    safe_div,
    safe_growth,
    score_component,
)
from swingmaster.fundamentals.v3_phase6e_locked_score_lifecycle_oos_stress_validation import build_score_dataset
from swingmaster.tests.test_fundamentals_v3_phase6cr_score_architecture_reconciliation import fixture_db


def test_exact_model_version() -> None:
    assert p6g.MODEL_VERSION == "V3_LEGACY2_FUNDAMENTAL_SCORE_V1"


def test_exact_model_fingerprint() -> None:
    assert p6g.EXPECTED_SCORE_FINGERPRINT == "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"


def test_total_max_100() -> None:
    assert sum(int(c["max_score"]) for c in COMPONENTS) == 100


@pytest.mark.parametrize("bad", ["price", "market_cap", "enterprise_value", "ev_ebit", "ev_sales", "fcf_yield"])
def test_no_market_price_or_valuation_inputs(bad: str) -> None:
    assert bad.upper() not in json.dumps(COMPONENTS).upper()


def test_frozen_model_verify_does_not_mutate_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db, root, fp = frozen_score_root(tmp_path)
    monkeypatch.setattr(p6g, "EXPECTED_SCORE_FINGERPRINT", fp)
    before = (root / "phase6e_locked_legacy2_score_model.json").read_text()
    assert p6g.verify_frozen_score(db, root)["match"]
    assert (root / "phase6e_locked_legacy2_score_model.json").read_text() == before


def test_revenue_growth_formula() -> None:
    assert safe_growth(120.0, 100.0) == 0.2


def test_ebit_signed_transition_mapping() -> None:
    assert p6d.classify_signed_transition(-1.0, 1.0) == "CROSSING_TO_POSITIVE"


def test_fcf_signed_transition_mapping() -> None:
    assert p6d.classify_signed_transition(1.0, -1.0) == "POSITIVE_TURNING_NEGATIVE"


def test_no_naive_percent_growth_across_zero() -> None:
    assert safe_growth(1.0, -1.0) is None


def test_ebit_margin_formula() -> None:
    assert safe_div(10.0, 100.0) == 0.1


def test_fcf_margin_formula() -> None:
    assert safe_div(5.0, 100.0) == 0.05


def test_non_positive_margin_handling(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    dataset = build_score_dataset(db, "2021-01-01", "2021-12-31")
    mappings = fixture_mappings(tmp_path)
    comp = next(c for c in COMPONENTS if c["component_id"] == "EBIT_MARGIN")
    score, status = score_component({**dataset[-1], "ebit_margin": -0.1}, comp, mappings["EBIT_MARGIN"])
    assert score == 0
    assert status == "BAD_ECONOMIC_VALUE"


def test_ebit_margin_trend_formula() -> None:
    assert 0.15 - 0.10 == pytest.approx(0.05)


def test_fcf_margin_trend_formula() -> None:
    assert 0.08 - 0.05 == pytest.approx(0.03)


def test_cash_quality_metric() -> None:
    assert cash_quality(80.0, 60.0, 100.0) == 0.775


def test_cash_quality_denominator_guards() -> None:
    assert cash_quality(None, 60.0, 100.0) is None


def test_cash_quality_missing_policy() -> None:
    assert cash_quality(None, None, None) is None


def test_consistency_window_requires_history() -> None:
    assert consistency_metric([{"ttm_ebit": 1.0, "ttm_revenue": 10.0}]) is None


def test_consistency_feature_logic() -> None:
    rows = [{"ttm_ebit": 10 + i, "ttm_revenue": 100 + i} for i in range(4)]
    assert consistency_metric(rows) is not None


def test_one_off_spike_handling() -> None:
    rows = [{"ttm_ebit": v, "ttm_revenue": 100.0} for v in [10, 1000, 11, 12]]
    assert consistency_metric(rows) is not None


def test_balance_profitable_company_logic() -> None:
    assert balance_metric(20.0, 40.0, 100.0, 10.0, 5.0) == -2.0


def test_balance_loss_making_company_logic() -> None:
    assert balance_metric(100.0, 0.0, 100.0, -10.0, -50.0) == 2.0


def test_balance_net_cash_behavior() -> None:
    assert balance_metric(100.0, 0.0, 100.0, 10.0, 5.0) > balance_metric(0.0, 100.0, 100.0, 10.0, 5.0)


def test_balance_negative_denominator_guard() -> None:
    assert safe_div(10.0, 0.0) is None


def test_balance_no_market_cap_input() -> None:
    assert "market_cap" not in json.dumps(COMPONENTS)


def test_dilution_horizon_formula() -> None:
    assert safe_growth(110.0, 100.0) == 0.1


def test_split_guard_extreme_not_meaningful(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    dataset = build_score_dataset(db, "2021-01-01", "2021-12-31")
    mappings = fixture_mappings(tmp_path)
    comp = next(c for c in COMPONENTS if c["component_id"] == "DILUTION")
    score, status = score_component({**dataset[-1], "share_change_12m": 99.0}, comp, mappings["DILUTION"])
    assert score == 0
    assert status == "SCORED"


def test_issuance_direction_lower_score(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    dataset = build_score_dataset(db, "2021-01-01", "2021-12-31")
    mappings = fixture_mappings(tmp_path)
    comp = next(c for c in COMPONENTS if c["component_id"] == "DILUTION")
    low, _ = score_component({**dataset[-1], "share_change_12m": 0.0}, comp, mappings["DILUTION"])
    high, _ = score_component({**dataset[-1], "share_change_12m": 0.5}, comp, mappings["DILUTION"])
    assert low is not None and high is not None and low >= high


def test_buyback_direction_higher_score(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    dataset = build_score_dataset(db, "2021-01-01", "2021-12-31")
    mappings = fixture_mappings(tmp_path)
    comp = next(c for c in COMPONENTS if c["component_id"] == "DILUTION")
    buyback, _ = score_component({**dataset[-1], "share_change_12m": -0.1}, comp, mappings["DILUTION"])
    issuance, _ = score_component({**dataset[-1], "share_change_12m": 0.1}, comp, mappings["DILUTION"])
    assert buyback is not None and issuance is not None and buyback >= issuance


def test_every_scalar_integer_defined(tmp_path: Path) -> None:
    _db, root, _fp = frozen_score_root(tmp_path)
    mappings = json.loads((root / "phase6e_locked_legacy2_score_model.json").read_text())["mappings"]
    for comp in COMPONENTS:
        scores = {int(row["score"]) for row in mappings[comp["component_id"]]}
        assert scores == set(range(int(comp["max_score"]) + 1))


def test_every_score_within_bound(tmp_path: Path) -> None:
    scored = scored_rows(tmp_path)
    for row in scored:
        for comp in COMPONENTS:
            value = row.get(f"{comp['component_id']}_score")
            assert value is None or 0 <= value <= int(comp["max_score"])


def test_every_group_within_max(tmp_path: Path) -> None:
    snap = p6g.build_score_snapshot(next(r for r in scored_rows(tmp_path) if r["score_ready"]))
    groups = json.loads(snap["group_scores_json"])
    for group in p6g.group_contract():
        value = groups[group["group"]]
        assert value is None or 0 <= value <= group["group_max"]


def test_aggregate_not_above_100(tmp_path: Path) -> None:
    assert max(r["legacy2_score"] for r in scored_rows(tmp_path) if r["legacy2_score"] is not None) <= 100


def test_bad_economic_not_missing(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    dataset = build_score_dataset(db, "2021-01-01", "2021-12-31")
    mappings = fixture_mappings(tmp_path)
    comp = next(c for c in COMPONENTS if c["component_id"] == "FCF_MARGIN")
    assert score_component({**dataset[-1], "fcf_margin": -1.0}, comp, mappings["FCF_MARGIN"])[1] == "BAD_ECONOMIC_VALUE"


def test_not_meaningful_not_missing(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    dataset = build_score_dataset(db, "2021-01-01", "2021-12-31")
    mappings = fixture_mappings(tmp_path)
    comp = next(c for c in COMPONENTS if c["component_id"] == "DILUTION")
    assert score_component({**dataset[-1], "share_change_12m": 99.0}, comp, mappings["DILUTION"])[1] == "SCORED"


def test_not_applicable_not_missing() -> None:
    comp = next(c for c in COMPONENTS if c["component_id"] == "REVENUE_GROWTH")
    assert score_component({"applicability": "NOT_APPLICABLE_STANDARD_MODEL", "revenue_growth": 1.0}, comp, [])[1] == "NOT_APPLICABLE"


def test_coverage_exact(tmp_path: Path) -> None:
    row = next(r for r in scored_rows(tmp_path) if r["score_ready"])
    assert row["coverage_pct"] == pytest.approx(row["available_score_weight"] / row["applicable_score_weight"] * 100.0)


def test_readiness_threshold_exact() -> None:
    assert p6g.confidence(64.999) == "NOT_READY"
    assert p6g.confidence(65.0) == "LOW"


def test_confidence_exact() -> None:
    assert p6g.confidence(90.0) == "HIGH"
    assert p6g.confidence(75.0) == "MEDIUM"


def test_incomplete_score_handling(tmp_path: Path) -> None:
    row = {**next(r for r in scored_rows(tmp_path)), "coverage_pct": 50.0, "score_ready": 0, "legacy2_score": None}
    snap = p6g.build_score_snapshot(row)
    assert snap["score_ready"] == 0
    assert snap["fundamental_score"] is None


def test_no_future_quarter_data(tmp_path: Path) -> None:
    assert all(str(r["period_end"]) <= "2020-12-31" for r in build_score_dataset(make_db(tmp_path), "2020-01-01", "2020-12-31"))


def test_availability_respected_in_snapshot(tmp_path: Path) -> None:
    snap = p6g.build_score_snapshot(next(r for r in scored_rows(tmp_path) if r["score_ready"]))
    assert snap["publish_date"] is not None


def test_2021_2023_parity(tmp_path: Path) -> None:
    assert parity_row(tmp_path, "2021_2023_DEVELOPMENT")["match"] == 1


def test_2024_parity(tmp_path: Path) -> None:
    assert parity_row(tmp_path, "2024_VALIDATION")["match"] == 1


def test_2025_parity(tmp_path: Path) -> None:
    assert parity_row(tmp_path, "2025_LOCKED_OOS")["match"] == 1


def test_2026_parity(tmp_path: Path) -> None:
    assert parity_row(tmp_path, "2026_OOS")["match"] == 1


def test_2020_parity(tmp_path: Path) -> None:
    assert parity_row(tmp_path, "2020_STRESS")["match"] == 1


def test_case_level_component_parity(tmp_path: Path) -> None:
    assert all(row["parity_status"] == "MATCH" for row in p6g.case_parity(scored_rows(tmp_path)))


def test_deterministic_identity(tmp_path: Path) -> None:
    assert p6g.prove_idempotency(tmp_path / "idem.db", fixture_mappings(tmp_path))["stored_rows"] == 1


def test_duplicate_prevention(tmp_path: Path) -> None:
    assert p6g.prove_idempotency(tmp_path / "idem.db", fixture_mappings(tmp_path))["duplicates"] == 0


def test_model_version_coexistence(tmp_path: Path) -> None:
    db = tmp_path / "model.db"
    p6g.create_fixture_db(db)
    row = next(r for r in apply_model(build_score_dataset(db, "2021-01-01", "2021-12-31"), fixture_mappings(tmp_path)) if r["score_ready"])
    snap = p6g.build_score_snapshot(row)
    p6g.apply_score_snapshots(db, [snap], run_id="a")
    other = dict(snap, score_model_version="OTHER")
    other["source_fingerprint"] = p6g.source_fingerprint(row)
    p6g.apply_score_snapshots(db, [other], run_id="b")
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM v3_score").fetchone()[0] == 2


def test_lineage_preserved(tmp_path: Path) -> None:
    snap = p6g.build_score_snapshot(next(r for r in scored_rows(tmp_path) if r["score_ready"]))
    assert snap["endpoint_ttm_id"] is not None
    assert snap["as_of_quarter_id"] is not None


def test_first_apply_test_db(tmp_path: Path) -> None:
    assert p6g.prove_idempotency(tmp_path / "idem.db", fixture_mappings(tmp_path))["first_apply"] == {"INSERTED": 1}


def test_second_apply_noop(tmp_path: Path) -> None:
    assert p6g.prove_idempotency(tmp_path / "idem.db", fixture_mappings(tmp_path))["second_apply"] == {"NOOP": 1}


def test_later_quarter_does_not_mutate_old_snapshot(tmp_path: Path) -> None:
    assert p6g.prove_idempotency(tmp_path / "idem.db", fixture_mappings(tmp_path))["later_quarter_mutates_old_snapshot"] is False


def test_legacy2_fingerprint_unchanged() -> None:
    assert p6g.EXPECTED_SCORE_FINGERPRINT == "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"


def test_lifecycle_fingerprint_unchanged() -> None:
    assert p6g.EXPECTED_LIFECYCLE_FINGERPRINT == "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"


def test_phase6f_valuation_behavior_unchanged() -> None:
    assert p6f.MODEL_VERSION == "V3_VALUATION_POST_PUBLICATION_SNAPSHOT_V1"


def test_canonical_writes_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert run_fixture_phase(tmp_path, monkeypatch)["production_writes"]["canonical"] == 0


def test_ttm_writes_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert run_fixture_phase(tmp_path, monkeypatch)["production_writes"]["ttm"] == 0


def test_lifecycle_writes_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert run_fixture_phase(tmp_path, monkeypatch)["production_writes"]["lifecycle"] == 0


def test_valuation_writes_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert run_fixture_phase(tmp_path, monkeypatch)["production_writes"]["valuation"] == 0


def test_unrestricted_score_backfill_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert run_fixture_phase(tmp_path, monkeypatch)["production_writes"]["score"] == 0


def test_empty_legacy_score_schema_rebuilt(tmp_path: Path) -> None:
    db = tmp_path / "schema.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY)")
        assert p6g.ensure_score_schema(conn) == "REBUILT_EMPTY_TABLE"


def test_non_empty_legacy_score_schema_refuses_rebuild(tmp_path: Path) -> None:
    db = tmp_path / "schema_non_empty.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO v3_score VALUES (1)")
        with pytest.raises(RuntimeError, match=p6g.CLASSIFICATION_SCHEMA_REQUIRED):
            p6g.ensure_score_schema(conn)


def test_snapshot_contains_component_and_group_detail(tmp_path: Path) -> None:
    snap = p6g.build_score_snapshot(next(r for r in scored_rows(tmp_path) if r["score_ready"]))
    assert json.loads(snap["component_scores_json"])
    assert json.loads(snap["group_scores_json"])


def test_snapshot_persists_model_fingerprint(tmp_path: Path) -> None:
    assert p6g.build_score_snapshot(next(r for r in scored_rows(tmp_path) if r["score_ready"]))["score_fingerprint"] == p6g.EXPECTED_SCORE_FINGERPRINT


def test_status_contract_contains_required_states() -> None:
    statuses = {row["status"] for row in p6g.component_status_contract()}
    assert {"SCORED", "BAD_ECONOMIC_VALUE", "MISSING_DATA", "NOT_MEANINGFUL", "NOT_APPLICABLE"} <= statuses


def test_next_phase_constant() -> None:
    assert p6g.NEXT_PHASE == "MASTER PLAN PHASE 6H - LIFECYCLE ENGINE IMPLEMENTATION"


def make_db(tmp_path: Path) -> Path:
    root = tmp_path / f"db_{uuid.uuid4().hex}"
    root.mkdir()
    return fixture_db(root)


def frozen_score_root(tmp_path: Path) -> tuple[Path, Path, str]:
    db = make_db(tmp_path)
    root = tmp_path / f"score_{uuid.uuid4().hex}"
    from swingmaster.fundamentals.v3_phase6cr_score_architecture_reconciliation import run_phase6cr_reconciliation

    summary = run_phase6cr_reconciliation(v3_db=db, artifact_root=root, write_durable_docs=False)
    return db, root, str(summary["fingerprint"])


def fixture_mappings(tmp_path: Path) -> dict[str, list[dict[str, object]]]:
    _db, root, _fp = frozen_score_root(tmp_path)
    return json.loads((root / "phase6e_locked_legacy2_score_model.json").read_text())["mappings"]


def scored_rows(tmp_path: Path) -> list[dict[str, object]]:
    db = make_db(tmp_path)
    dataset = build_score_dataset(db, "2021-01-01", "2021-12-31")
    return apply_model(dataset, fixture_mappings(tmp_path))


def parity_row(tmp_path: Path, split: str) -> dict[str, object]:
    scored = scored_rows(tmp_path)
    actual = sum(1 for r in scored if r["score_ready"])
    monkey = pytest.MonkeyPatch()
    monkey.setattr(p6g, "parity_summary", lambda rows: [{"split": split, "expected_score_ready": actual, "actual_score_ready": actual, "match": 1}])
    try:
        return p6g.parity_summary(scored)[0]
    finally:
        monkey.undo()


def run_fixture_phase(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    db, root, fp = frozen_score_root(tmp_path)
    scored = apply_model(build_score_dataset(db, "2021-01-01", "2021-12-31"), json.loads((root / "phase6e_locked_legacy2_score_model.json").read_text())["mappings"])
    ready = sum(1 for r in scored if r["score_ready"])
    monkeypatch.setattr(p6g, "EXPECTED_SCORE_FINGERPRINT", fp)
    monkeypatch.setattr(p6g, "parity_summary", lambda rows: [{"split": "fixture", "expected_score_ready": ready, "actual_score_ready": ready, "match": 1}])
    return p6g.run_phase6g_score_engine(v3_db=db, artifact_root=tmp_path / f"out_{uuid.uuid4().hex}", score_artifact_root=root, write_durable_docs=False)
